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

	// prefix 模式特定参数
	recursive      = flag.Bool("r", false, "prefix 模式：递归处理目录下的所有对象")
	force          = flag.Bool("f", false, "prefix 模式：强制确认，跳过用户确认环节")

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
		Recursive:         *recursive,
		Force:             *force,
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
objcli - 对象存储间数据复制与删除工具
支持 AWS S3 / 腾讯云 COS 之间的单文件、前缀批量、对象列表三种模式，流式传输不落盘。

🚀 【新格式】支持 <type>://<bucket>.<endpoint>/<object_path> 格式，支持通配符 *
例如：cos://bucket.cos.ap-beijing.myqcloud.com/testa* 匹配 testa 开头的所有对象

============================================
【目录结构】
============================================
  1. objcli cp <源位置> <目标位置>           # 拷贝对象
  2. objcli cp -key-list <列表文件> <目标位置>  # 列表拷贝
  3. objcli rm <目标位置>                   # 删除对象
  4. objcli rm -del-key-list <列表文件>       # 列表删除

============================================
【URL 格式说明】
============================================
  格式：<类型>://<bucket>.<endpoint>/<路径>

  COS 格式（支持两种）:
    cos://bucket.cos.ap-beijing.myqcloud.com/path/to/file.zip
    https://bucket.cos.ap-beijing.myqcloud.com/path/to/file.zip

  S3 格式（支持三种）:
    s3://bucket.s3.ap-southeast-1.amazonaws.com/path/to/file.zip
    https://bucket.s3.ap-southeast-1.amazonaws.com/path/to/file.zip
    https://s3.ap-southeast-1.amazonaws.com/bucket/path/to/file.zip

  通配符模式（前缀批量匹配）:
    s3://bucket.s3.ap-southeast-1.amazonaws.com/images/*.jpg
    cos://bucket.cos.ap-beijing.myqcloud.com/logs/2024-*/
    cos://bucket.cos.ap-beijing.myqcloud.com/testa*  # 匹配 testa 开头的

============================================
【拷贝参数（action=cp）】
============================================
  必需:
    -action cp

  可选:
    -key-list          对象 URL 列表文件（列表拷贝模式）
                       支持本地文件路径: /path/to/keys.txt
                       支持远程 URL:    https://example.com/keys.txt
                       每行一个完整对象 URL，空行和 # 开头行自动跳过

  性能:
    -chunk             分块大小 MB               默认: 128，cos→cos 建议 512
    -concurrency       单文件分块并发数           默认: 5
    -obj-concurrency   多文件并发数（前缀/列表）   默认: 3

============================================
【删除参数（action=rm）】
============================================
  必需:
    -action rm

  可选:
    -del-key-list      待删除的对象 URL 列表文件（列表删除模式）
                       支持本地文件路径: /path/to/keys.txt
                       支持远程 URL:    https://example.com/keys.txt
                       每行一个完整对象 URL，空行和 # 开头行自动跳过

  性能:
    -delete-concurrency 删除并发数                默认: 3
    -url-decode        是否对列表中的对象名进行 URL decode（仅列表模式有效）  默认: false

============================================
【凭证配置】
============================================
  环境变量优先（命令行参数可覆盖）：

  AWS S3:
    AWS_ACCESS_KEY_ID          [可覆盖: -s3-ak]
    AWS_SECRET_ACCESS_KEY      [可覆盖: -s3-sk]

  腾讯云 COS:
    TENCENT_SECRET_ID          [可覆盖: -cos-id]
    TENCENT_SECRET_KEY         [可覆盖: -cos-sk]

============================================
【taskobserver 监控（可选）】
============================================
  -obs-bucket          COS 桶名                    [env: TASKOBS_BUCKET]
  -obs-region          COS 地域                    [env: TASKOBS_REGION]
  -obs-secret-id       COS SecretId               [env: TASKOBS_SECRET_ID]
  -obs-secret-key      COS SecretKey              [env: TASKOBS_SECRET_KEY]
  -obs-base-url        自定义域名                  [env: TASKOBS_BASE_URL]
  -obs-task            任务名称                    [env: TASKOBS_TASK]

============================================
【自动传参示例】
============================================
  # 1. 单文件拷贝（S3 → COS）
  objcli cp s3://bucket.s3.ap-southeast-1.amazonaws.com/path/file.zip \
            cos://bucket.cos.ap-beijing.myqcloud.com/new_path/file.zip

  # 2. 前缀批量拷贝（带通配符）
  objcli cp s3://bucket.s3.ap-southeast-1.amazonaws.com/images/*.jpg \
            cos://bucket.cos.ap-nanjing.myqcloud.com/photos/

  # 3. 前缀批量拷贝（目录）
  objcli cp cos://bucket.cos.ap-singapore.myqcloud.com/logs/2024-*/ \
            cos://bucket.cos.ap-beijing.myqcloud.com/archive/

  # 4. 列表拷贝（跨云拷贝）
  objcli cp -key-list urls.txt cos://bucket.cos.ap-nanjing.myqcloud.com/migrated/

  # 5. 单文件删除
  objcli rm cos://bucket.cos.ap-beijing.myqcloud.com/expired/file.zip

  # 6. 前缀批量删除（带通配符）
  objcli rm cos://bucket.cos.ap-beijing.myqcloud.com/temp_*

  # 7. 列表删除
  objcli rm -del-key-list delete_urls.txt

============================================
【迁移矩阵】
============================================
  ✅ COS → COS  （支持服务端复制）
  ✅ COS → S3   （流式传输）
  ✅ S3 → COS   （流式传输）
  ✅ S3 → S3    （不支持 S3 服务端跨区复制）

`)
}
