package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"objcli/cmd"
	"github.com/zhangyf/objstore"
	"taskobserver"
)

var (
	action = flag.String("action", "", "操作类型: cp")

	srcType = flag.String("src-type", "", "源存储类型: s3 | cos（list 模式可省略）")
	dstType = flag.String("dst-type", "", "目标存储类型: s3 | cos")

	s3AccessKey = flag.String("s3-ak", "", "AWS Access Key ID")
	s3SecretKey = flag.String("s3-sk", "", "AWS Secret Access Key")

	cosSecretID  = flag.String("cos-id", "", "腾讯云 SecretId")
	cosSecretKey = flag.String("cos-sk", "", "腾讯云 SecretKey")

	srcBucket = flag.String("src-bucket", "", "源 Bucket（list 模式可省略）")
	srcRegion = flag.String("src-region", "", "源 Region（list 模式可省略）")
	dstBucket = flag.String("dst-bucket", "", "目标 Bucket")
	dstRegion = flag.String("dst-region", "", "目标 Region")

	srcKey        = flag.String("src-key", "", "源对象 Key（单文件拷贝）")
	dstKey        = flag.String("dst-key", "", "目标对象 Key（单文件，默认与源 Key 相同）")
	srcPrefix     = flag.String("src-prefix", "", "源前缀（前缀批量拷贝），例: a/b/c/")
	dstPrefix     = flag.String("dst-prefix", "", "目标前缀（前缀/列表拷贝），例: d/e/f/")
	keyListSource = flag.String("key-list", "", "对象 URL 列表文件，本地路径或 HTTP/HTTPS URL")

	chunkMB           = flag.Int("chunk", 128, "分块大小 MB，cos→cos 建议 512")
	concurrency       = flag.Int("concurrency", 5, "单文件分块并发数")
	objectConcurrency = flag.Int("obj-concurrency", 3, "多文件并发数（前缀/列表模式）")

	// taskobserver 可选配置
	obsBucket    = flag.String("obs-bucket", "", "taskobserver: COS 桶名 [env: TASKOBS_BUCKET]")
	obsRegion    = flag.String("obs-region", "", "taskobserver: COS 地域 [env: TASKOBS_REGION]")
	obsSecretID  = flag.String("obs-secret-id", "", "taskobserver: COS SecretId [env: TASKOBS_SECRET_ID]")
	obsSecretKey = flag.String("obs-secret-key", "", "taskobserver: COS SecretKey [env: TASKOBS_SECRET_KEY]")
	obsBaseURL   = flag.String("obs-base-url", "", "taskobserver: 自定义域名 [env: TASKOBS_BASE_URL]")
	obsTask      = flag.String("obs-task", "", "taskobserver: 任务名称 [env: TASKOBS_TASK]")
)

func main() {
	flag.Usage = usage
	flag.Parse()

	if *action == "" {
		usage()
		os.Exit(0)
	}

	ctx := context.Background()
	switch strings.ToLower(*action) {
	case "cp":
		runCopy(ctx)
	default:
		log.Fatalf("未知 action: %s，当前支持: cp", *action)
	}
}

func runCopy(ctx context.Context) {
	resolvedS3AK := envOr(*s3AccessKey, "AWS_ACCESS_KEY_ID")
	resolvedS3SK := envOr(*s3SecretKey, "AWS_SECRET_ACCESS_KEY")
	resolvedCOSID := envOr(*cosSecretID, "TENCENT_SECRET_ID")
	resolvedCOSSK := envOr(*cosSecretKey, "TENCENT_SECRET_KEY")

	isList := *keyListSource != ""

	// 解析并校验目标存储类型
	mustSet("dst-type", *dstType)
	mustSet("dst-bucket", *dstBucket)
	mustSet("dst-region", *dstRegion)
	dstST := objstore.ProviderType(strings.ToLower(*dstType))
	if dstST != objstore.ProviderCOS && dstST != objstore.ProviderS3 {
		log.Fatalf("不支持的存储类型: %q，支持: cos, s3", dstST)
	}

	// 解析并校验源存储类型（list 模式可跳过）
	var srcST objstore.ProviderType
	if !isList {
		mustSet("src-type", *srcType)
		mustSet("src-bucket", *srcBucket)
		mustSet("src-region", *srcRegion)
		srcST = objstore.ProviderType(strings.ToLower(*srcType))
		if srcST != objstore.ProviderCOS && srcST != objstore.ProviderS3 {
			log.Fatalf("不支持的存储类型: %q，支持: cos, s3", srcST)
		}
	}

	// 构建目标 Storage
	dstStorage := buildStorage(ctx, dstST, *dstBucket, *dstRegion, resolvedCOSID, resolvedCOSSK, resolvedS3AK, resolvedS3SK)

	// 构建源 Storage（list 模式为 nil，引擎内部动态创建）
	var srcStorage objstore.Store
	if !isList {
		srcStorage = buildStorage(ctx, srcST, *srcBucket, *srcRegion, resolvedCOSID, resolvedCOSSK, resolvedS3AK, resolvedS3SK)
	}

	cfg := cmd.CopyConfig{
		SrcKey:            *srcKey,
		SrcPrefix:         *srcPrefix,
		KeyListSource:     *keyListSource,
		DstKey:            *dstKey,
		DstPrefix:         *dstPrefix,
		ChunkMB:           *chunkMB,
		ChunkConcurrency:  *concurrency,
		ObjectConcurrency: *objectConcurrency,
	}

	engine := cmd.NewEngine(srcStorage, dstStorage, cfg).
		WithCreds(objstore.ProviderCOS, resolvedCOSID, resolvedCOSSK).
		WithCreds(objstore.ProviderS3, resolvedS3AK, resolvedS3SK)

	if err := engine.CheckMemory(); err != nil {
		log.Fatalf("%v", err)
	}

	// 初始化 taskobserver（配置项全部可选，未配置则不开启监控）
	obsCfg := taskobserver.Config{
		Bucket:      envOr(*obsBucket, "TASKOBS_BUCKET"),
		Region:      envOr(*obsRegion, "TASKOBS_REGION"),
		SecretID:    envOr(*obsSecretID, "TASKOBS_SECRET_ID"),
		SecretKey:   envOr(*obsSecretKey, "TASKOBS_SECRET_KEY"),
		BaseURL:     envOr(*obsBaseURL, "TASKOBS_BASE_URL"),
		TaskName:    envOr(*obsTask, "TASKOBS_TASK"),
		Interval:    5 * time.Second,
		ExtraWriter: os.Stderr,
	}
	var obs *taskobserver.Observer
	if obsCfg.Bucket != "" && obsCfg.SecretID != "" {
		if obsCfg.TaskName == "" {
			// 自动生成任务名
			obsCfg.TaskName = fmt.Sprintf("%s→%s %s", strings.ToUpper(*srcType), strings.ToUpper(*dstType), *srcBucket)
		}
		var obsErr error
		obs, obsErr = taskobserver.NewWithError(obsCfg)
		if obsErr != nil {
			log.Printf("[taskobserver] 初始化失败，将跳过监控: %v", obsErr)
			obs = nil
		} else {
			log.SetOutput(obs.Writer())
			log.SetFlags(0)
			obs.Start(func() (int, int) {
				done, total := engine.BytesProgress()
				return int(done >> 20), int(total >> 20) // 转为 MB 单位避免 int 溢出
			})
			log.Printf("[taskobserver] Overview : %s", obs.OverviewURL())
			log.Printf("[taskobserver] Task page: %s", obs.TaskURL())
		}
	}

	runErr := engine.Run(ctx)

	if obs != nil {
		if runErr != nil {
			obs.Fail(runErr)
		} else {
			obs.Done()
		}
	}
	if runErr != nil {
		log.Fatalf("失败: %v", runErr)
	}
}

// buildStorage 根据 provider 字符串构建对应的 Store 实例
func buildStorage(_ context.Context, provider objstore.ProviderType,
	bucket, region, cosID, cosSK, s3AK, s3SK string) objstore.Store {
	switch provider {
	case objstore.ProviderCOS:
		mustSet("cos-id (or TENCENT_SECRET_ID)", cosID)
		mustSet("cos-sk (or TENCENT_SECRET_KEY)", cosSK)
		s, err := objstore.New(objstore.Config{Provider: objstore.ProviderCOS, Bucket: bucket, Region: region, SecretID: cosID, SecretKey: cosSK})
		if err != nil {
			log.Fatalf("初始化 COS 失败: %v", err)
		}
		return s
	case objstore.ProviderS3:
		mustSet("s3-ak (or AWS_ACCESS_KEY_ID)", s3AK)
		mustSet("s3-sk (or AWS_SECRET_ACCESS_KEY)", s3SK)
		s, err := objstore.New(objstore.Config{Provider: objstore.ProviderS3, Bucket: bucket, Region: region, SecretID: s3AK, SecretKey: s3SK})
		if err != nil {
			log.Fatalf("初始化 S3 失败: %v", err)
		}
		return s
	default:
		log.Fatalf("不支持的存储类型: %s", provider)
		return nil
	}
}

func mustSet(name, val string) {
	if val == "" {
		log.Fatalf("缺少必填参数: -%s", name)
	}
}

func envOr(flagVal, envKey string) string {
	if flagVal != "" {
		return flagVal
	}
	return os.Getenv(envKey)
}

func usage() {
	fmt.Print(`
objcli - 对象存储间数据复制工具
支持 AWS S3 / 腾讯云 COS 之间的单文件、前缀批量、对象列表三种拷贝模式，流式传输不落盘。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【Action】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  cp    对象拷贝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【参数说明】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  通用:
    -action            操作类型（必填）: cp
    -src-type          源存储类型: s3 | cos（list 模式可省略）
    -dst-type          目标存储类型（必填）: s3 | cos

  源/目标:
    -src-bucket        源 Bucket（list 模式可省略）
    -src-region        源 Region（list 模式可省略）
    -dst-bucket        目标 Bucket（必填）
    -dst-region        目标 Region（必填）

  拷贝模式（三选一）:
    -src-key           源对象 Key，单文件拷贝
    -src-prefix        源前缀，前缀批量拷贝，例: a/b/c/
    -key-list          对象 URL 列表，列表拷贝
                       支持本地文件路径: /path/to/keys.txt
                       支持远程 URL:    https://example.com/keys.txt
                       每行一个完整对象 URL，空行和 # 开头行自动跳过

  目标 Key/前缀:
    -dst-key           目标对象 Key（单文件，默认与源 Key 相同）
    -dst-prefix        目标前缀（前缀/列表拷贝），例: d/e/f/

  AWS S3 凭证（命令行优先，未填则读环境变量）:
    -s3-ak             AWS Access Key ID          [env: AWS_ACCESS_KEY_ID]
    -s3-sk             AWS Secret Access Key      [env: AWS_SECRET_ACCESS_KEY]

  腾讯云 COS 凭证（命令行优先，未填则读环境变量）:
    -cos-id            腾讯云 SecretId            [env: TENCENT_SECRET_ID]
    -cos-sk            腾讯云 SecretKey           [env: TENCENT_SECRET_KEY]

  性能:
    -chunk             分块大小 MB               默认: 128，cos→cos 建议 512
    -concurrency       单文件分块并发数           默认: 5
    -obj-concurrency   多文件并发数（前缀/列表）   默认: 3

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【使用示例】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # 1. S3 → COS 单文件
  objcli -action cp -src-type s3 -dst-type cos \
    -src-bucket my-s3-bucket -src-region ap-southeast-1 -src-key path/to/file.zip \
    -dst-bucket my-cos-bucket -dst-region ap-beijing

  # 2. COS → COS 前缀批量
  objcli -action cp -src-type cos -dst-type cos \
    -src-bucket src-bkt -src-region ap-singapore -src-prefix a/b/c/ \
    -dst-bucket dst-bkt -dst-region ap-beijing   -dst-prefix d/e/f/ \
    -chunk 512 -obj-concurrency 3

  # 3. 对象 URL 列表拷贝（无需指定源桶）
  objcli -action cp -dst-type cos \
    -dst-bucket my-cos-bucket -dst-region ap-nanjing -dst-prefix list_dir/ \
    -key-list https://bucket.cos.ap-nanjing.myqcloud.com/obj_list.log

  # 列表文件格式:
  #   https://bucket.cos.ap-singapore.myqcloud.com/path/file1.zip
  #   https://bucket.s3.ap-southeast-1.amazonaws.com/path/file2.zip
  #   # 注释行，跳过

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【环境变量】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  AWS_ACCESS_KEY_ID        AWS Access Key ID
  AWS_SECRET_ACCESS_KEY    AWS Secret Access Key
  TENCENT_SECRET_ID        腾讯云 SecretId
  TENCENT_SECRET_KEY       腾讯云 SecretKey

`)
}
