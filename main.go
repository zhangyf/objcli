package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"objcopy/cmd"
)

var (
	action = flag.String("action", "", "操作类型: cp")

	// 拷贝方向
	srcType = flag.String("src-type", "", "源存储类型: s3 | cos")
	dstType = flag.String("dst-type", "", "目标存储类型: s3 | cos")

	// S3 凭证
	s3AccessKey = flag.String("s3-ak", "", "AWS Access Key ID")
	s3SecretKey = flag.String("s3-sk", "", "AWS Secret Access Key")

	// COS 凭证
	cosSecretID  = flag.String("cos-id", "", "腾讯云 SecretId")
	cosSecretKey = flag.String("cos-sk", "", "腾讯云 SecretKey")

	// 源/目标 bucket & region（通用，不带类型前缀）
	srcBucket = flag.String("src-bucket", "", "源 Bucket")
	srcRegion = flag.String("src-region", "", "源 Region（不填则按 src-type 使用默认值）")
	dstBucket = flag.String("dst-bucket", "", "目标 Bucket")
	dstRegion = flag.String("dst-region", "", "目标 Region（不填则按 dst-type 使用默认值）")

	// 单文件拷贝
	srcKey = flag.String("src-key", "", "源对象 Key（单文件拷贝）")
	dstKey = flag.String("dst-key", "", "目标对象 Key（单文件，默认与源 Key 相同）")

	// 前缀批量拷贝
	srcPrefix = flag.String("src-prefix", "", "源前缀（前缀批量拷贝），例: a/b/c/")
	dstPrefix = flag.String("dst-prefix", "", "目标前缀（前缀/列表拷贝），例: d/e/f/")

	// 对象列表拷贝
	keyListSource = flag.String("key-list", "", "对象 Key 列表文件，本地路径或 HTTP/HTTPS URL（列表拷贝）")

	// 性能参数
	chunkMB           = flag.Int("chunk", 128, "分块大小 MB，cos→cos 建议 512，其他建议 32~128")
	concurrency       = flag.Int("concurrency", 5, "单文件分块并发数")
	objectConcurrency = flag.Int("obj-concurrency", 3, "多文件并发数（前缀/列表模式）")
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

	// 内存安全预检
	if err := cmd.CheckMemorySafety(*chunkMB, *concurrency, *objectConcurrency); err != nil {
		log.Fatalf("%v", err)
	}

	src := strings.ToLower(*srcType)
	dst := strings.ToLower(*dstType)
	srcReg := *srcRegion
	dstReg := *dstRegion

	// 先判断拷贝模式：单文件 > 前缀 > 列表
	copyMode := ""
	switch {
	case *srcKey != "":
		copyMode = "single"
	case *srcPrefix != "":
		copyMode = "prefix"
	case *keyListSource != "":
		copyMode = "list"
	default:
		log.Fatal("请指定拷贝模式：-src-key（单文件）、-src-prefix（前缀批量）或 -key-list（对象列表）")
	}

	mustSet("dst-type", dst)
	mustSet("dst-bucket", *dstBucket)
	mustSet("dst-region", dstReg)

	// list 模式不需要 src 相关参数
	if copyMode != "list" {
		mustSet("src-type", src)
		mustSet("src-bucket", *srcBucket)
		mustSet("src-region", srcReg)
	}

	var err error

	// list 模式：不需要 src 类型，直接进入统一入口
	if copyMode == "list" {
		err = cmd.CopyFromURLList(ctx, cmd.CopyFromURLListConfig{
			DstType:           dst,
			DstBucket:         *dstBucket,
			DstRegion:         dstReg,
			DstPrefix:         *dstPrefix,
			COSSecretID:       resolvedCOSID,
			COSSecretKey:      resolvedCOSSK,
			S3AccessKey:       resolvedS3AK,
			S3SecretKey:       resolvedS3SK,
			KeyListSource:     *keyListSource,
			ChunkMB:           *chunkMB,
			ChunkConcurrency:  *concurrency,
			ObjectConcurrency: *objectConcurrency,
		})
		if err != nil {
			log.Fatalf("失败: %v", err)
		}
		return
	}

	switch src + "-to-" + dst {
	case "s3-to-cos":
		mustSet("s3-ak (or AWS_ACCESS_KEY_ID)", resolvedS3AK)
		mustSet("s3-sk (or AWS_SECRET_ACCESS_KEY)", resolvedS3SK)
		mustSet("cos-id (or TENCENT_SECRET_ID)", resolvedCOSID)
		mustSet("cos-sk (or TENCENT_SECRET_KEY)", resolvedCOSSK)
		switch copyMode {
		case "single":
			err = cmd.S3ToCOS(ctx, cmd.S3ToCOSConfig{
				S3AccessKey:  resolvedS3AK,
				S3SecretKey:  resolvedS3SK,
				S3Region:     srcReg,
				S3Bucket:     *srcBucket,
				S3Key:        *srcKey,
				COSSecretID:  resolvedCOSID,
				COSSecretKey: resolvedCOSSK,
				DstBucket:    *dstBucket,
				DstRegion:    dstReg,
				DstKey:       *dstKey,
				ChunkMB:      *chunkMB,
				Concurrency:  *concurrency,
			})
		case "prefix":
			err = cmd.S3ToCOSPrefix(ctx, cmd.S3ToCOSPrefixConfig{
				S3AccessKey:       resolvedS3AK,
				S3SecretKey:       resolvedS3SK,
				S3Region:          srcReg,
				S3Bucket:          *srcBucket,
				SrcPrefix:         *srcPrefix,
				COSSecretID:       resolvedCOSID,
				COSSecretKey:      resolvedCOSSK,
				DstBucket:         *dstBucket,
				DstRegion:         dstReg,
				DstPrefix:         *dstPrefix,
				ChunkMB:           *chunkMB,
				ChunkConcurrency:  *concurrency,
				ObjectConcurrency: *objectConcurrency,
			})
	case "list":
			err = cmd.CopyFromURLList(ctx, cmd.CopyFromURLListConfig{
				DstType:           "cos",
				DstBucket:         *dstBucket,
				DstRegion:         dstReg,
				DstPrefix:         *dstPrefix,
				COSSecretID:       resolvedCOSID,
				COSSecretKey:      resolvedCOSSK,
				S3AccessKey:       resolvedS3AK,
				S3SecretKey:       resolvedS3SK,
				KeyListSource:     *keyListSource,
				ChunkMB:           *chunkMB,
				ChunkConcurrency:  *concurrency,
				ObjectConcurrency: *objectConcurrency,
			})
		}

	case "cos-to-cos":
		mustSet("cos-id (or TENCENT_SECRET_ID)", resolvedCOSID)
		mustSet("cos-sk (or TENCENT_SECRET_KEY)", resolvedCOSSK)
		switch copyMode {
		case "single":
			err = cmd.COSToCOS(ctx, cmd.COSToCOSConfig{
				COSSecretID:  resolvedCOSID,
				COSSecretKey: resolvedCOSSK,
				SrcBucket:    *srcBucket,
				SrcRegion:    srcReg,
				SrcKey:       *srcKey,
				DstBucket:    *dstBucket,
				DstRegion:    dstReg,
				DstKey:       *dstKey,
				ChunkMB:      *chunkMB,
				Concurrency:  *concurrency,
			})
		case "prefix":
			err = cmd.COSToCOSPrefix(ctx, cmd.COSToCOSPrefixConfig{
				COSSecretID:       resolvedCOSID,
				COSSecretKey:      resolvedCOSSK,
				SrcBucket:         *srcBucket,
				SrcRegion:         srcReg,
				SrcPrefix:         *srcPrefix,
				DstBucket:         *dstBucket,
				DstRegion:         dstReg,
				DstPrefix:         *dstPrefix,
				ChunkMB:           *chunkMB,
				ChunkConcurrency:  *concurrency,
				ObjectConcurrency: *objectConcurrency,
			})
		case "list":
			err = cmd.CopyFromURLList(ctx, cmd.CopyFromURLListConfig{
				DstType:           "cos",
				DstBucket:         *dstBucket,
				DstRegion:         dstReg,
				DstPrefix:         *dstPrefix,
				COSSecretID:       resolvedCOSID,
				COSSecretKey:      resolvedCOSSK,
				S3AccessKey:       resolvedS3AK,
				S3SecretKey:       resolvedS3SK,
				KeyListSource:     *keyListSource,
				ChunkMB:           *chunkMB,
				ChunkConcurrency:  *concurrency,
				ObjectConcurrency: *objectConcurrency,
			})
		}

	case "cos-to-s3":
		mustSet("cos-id (or TENCENT_SECRET_ID)", resolvedCOSID)
		mustSet("cos-sk (or TENCENT_SECRET_KEY)", resolvedCOSSK)
		mustSet("s3-ak (or AWS_ACCESS_KEY_ID)", resolvedS3AK)
		mustSet("s3-sk (or AWS_SECRET_ACCESS_KEY)", resolvedS3SK)
		switch copyMode {
		case "single":
			err = cmd.COSToS3(ctx, cmd.COSToS3Config{
				COSSecretID:  resolvedCOSID,
				COSSecretKey: resolvedCOSSK,
				SrcBucket:    *srcBucket,
				SrcRegion:    srcReg,
				SrcKey:       *srcKey,
				S3AccessKey:  resolvedS3AK,
				S3SecretKey:  resolvedS3SK,
				S3Region:     dstReg,
				S3Bucket:     *dstBucket,
				S3Key:        *dstKey,
				ChunkMB:      *chunkMB,
				Concurrency:  *concurrency,
			})
		case "prefix":
			err = cmd.COSToS3Prefix(ctx, cmd.COSToS3PrefixConfig{
				COSSecretID:       resolvedCOSID,
				COSSecretKey:      resolvedCOSSK,
				SrcBucket:         *srcBucket,
				SrcRegion:         srcReg,
				SrcPrefix:         *srcPrefix,
				S3AccessKey:       resolvedS3AK,
				S3SecretKey:       resolvedS3SK,
				S3Region:          dstReg,
				S3Bucket:          *dstBucket,
				DstPrefix:         *dstPrefix,
				ChunkMB:           *chunkMB,
				ChunkConcurrency:  *concurrency,
				ObjectConcurrency: *objectConcurrency,
			})
		case "list":
			err = cmd.CopyFromURLList(ctx, cmd.CopyFromURLListConfig{
				DstType:           "s3",
				DstBucket:         *dstBucket,
				DstRegion:         dstReg,
				DstPrefix:         *dstPrefix,
				COSSecretID:       resolvedCOSID,
				COSSecretKey:      resolvedCOSSK,
				S3AccessKey:       resolvedS3AK,
				S3SecretKey:       resolvedS3SK,
				KeyListSource:     *keyListSource,
				ChunkMB:           *chunkMB,
				ChunkConcurrency:  *concurrency,
				ObjectConcurrency: *objectConcurrency,
			})
		}

	default:
		log.Fatalf("不支持的拷贝方向: %s → %s，支持: s3→cos | cos→cos | cos→s3", src, dst)
	}

	if err != nil {
		log.Fatalf("失败: %v", err)
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
    -src-type          源存储类型（必填）: s3 | cos
    -dst-type          目标存储类型（必填）: s3 | cos

  源/目标:
    -src-bucket        源 Bucket（必填）
    -src-region        源 Region（必填）
    -dst-bucket        目标 Bucket（必填）
    -dst-region        目标 Region（必填）

  拷贝模式（三选一）:
    -src-key           源对象 Key，单文件拷贝
    -src-prefix        源前缀，前缀批量拷贝，例: a/b/c/
    -key-list          对象 Key 列表，列表拷贝
                       支持本地文件路径: /path/to/keys.txt
                       支持远程 URL:    https://example.com/keys.txt
                       每行一个 Key，空行和 # 开头行自动跳过

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
    -chunk             分块大小 MB               默认: 128
                       建议: cos→cos 用 512，其他用 32~128
    -concurrency       单文件分块并发数           默认: 5
    -obj-concurrency   多文件并发数（前缀/列表）   默认: 3

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【使用示例】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # 1. S3 → COS 单文件（凭证从环境变量读取）
  objcli -action cp -src-type s3 -dst-type cos \
    -src-bucket my-s3-bucket -src-key path/to/file.zip \
    -dst-bucket my-cos-bucket

  # 2. S3 → COS 单文件（凭证显式指定）
  objcli -action cp -src-type s3 -dst-type cos \
    -s3-ak AKIAXXXXXX -s3-sk xxxxxxxx \
    -src-bucket my-s3-bucket -src-region ap-southeast-1 -src-key path/to/file.zip \
    -cos-id AKIDxxxxxx -cos-sk xxxxxxxx \
    -dst-bucket my-cos-bucket -dst-region ap-beijing -dst-key path/to/dst.zip \
    -chunk 128 -concurrency 5

  # 3. COS → COS 单文件（不过本机，腾讯云内部复制）
  objcli -action cp -src-type cos -dst-type cos \
    -src-bucket src-bkt -src-region ap-singapore -src-key path/to/file.zip \
    -dst-bucket dst-bkt -dst-region ap-beijing \
    -chunk 512 -concurrency 5

  # 4. COS → S3 单文件
  objcli -action cp -src-type cos -dst-type s3 \
    -src-bucket my-cos-bucket -src-region ap-singapore -src-key path/to/file.zip \
    -dst-bucket my-s3-bucket -dst-region us-east-1

  # 5. S3 → COS 前缀批量
  #    s3://my-s3-bucket/a/b/c/ → cos://my-cos-bucket/d/e/f/
  #    例: a/b/c/11/22/test.txt → d/e/f/11/22/test.txt
  objcli -action cp -src-type s3 -dst-type cos \
    -src-bucket my-s3-bucket -src-prefix a/b/c/ \
    -dst-bucket my-cos-bucket -dst-prefix d/e/f/ \
    -obj-concurrency 3 -concurrency 5 -chunk 128

  # 6. COS → COS 前缀批量
  objcli -action cp -src-type cos -dst-type cos \
    -src-bucket src-bkt -src-region ap-singapore -src-prefix a/b/c/ \
    -dst-bucket dst-bkt -dst-region ap-beijing   -dst-prefix d/e/f/ \
    -obj-concurrency 3 -chunk 512

  # 7. COS → S3 前缀批量
  objcli -action cp -src-type cos -dst-type s3 \
    -src-bucket my-cos-bucket -src-region ap-singapore -src-prefix a/b/c/ \
    -dst-bucket my-s3-bucket  -dst-region us-east-1    -dst-prefix d/e/f/

  # 8. S3 → COS 对象列表（本地文件）
  objcli -action cp -src-type s3 -dst-type cos \
    -src-bucket my-s3-bucket \
    -dst-bucket my-cos-bucket -dst-prefix backup/ \
    -key-list /path/to/keys.txt

  # 9. COS → COS 对象列表（远程 URL）
  objcli -action cp -src-type cos -dst-type cos \
    -src-bucket src-bkt -src-region ap-singapore \
    -dst-bucket dst-bkt -dst-region ap-beijing -dst-prefix archive/ \
    -key-list https://example.com/migrate-keys.txt

  # 10. COS → S3 对象列表
  objcli -action cp -src-type cos -dst-type s3 \
    -src-bucket my-cos-bucket -src-region ap-singapore \
    -dst-bucket my-s3-bucket  -dst-region us-east-1 -dst-prefix archive/ \
    -key-list /path/to/keys.txt

  # 列表文件格式 (keys.txt):
  #   a/b/c/file1.zip
  #   a/b/c/file2.zip
  #   # 注释行，跳过
  #   d/e/f/file3.zip

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【环境变量】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  AWS_ACCESS_KEY_ID        AWS Access Key ID
  AWS_SECRET_ACCESS_KEY    AWS Secret Access Key
  TENCENT_SECRET_ID        腾讯云 SecretId
  TENCENT_SECRET_KEY       腾讯云 SecretKey

`)
}
