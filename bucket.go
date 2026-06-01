package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"objcli/cmd"

	"github.com/zhangyf/objstore"
)

// ============================================================
// mb <BUCKET-URL>     创建桶
// rb <BUCKET-URL>     删除桶（默认要求空桶）
// ============================================================

var (
	flMbRegion string // 兼容性：覆盖从 URL 解析的 region（极少用）
)

func registerMbFlags(fs *flag.FlagSet) {
	bindCreds(fs)
	bindSrcDstCreds(fs)
	fs.StringVar(&flMbRegion, "region", "", "覆盖 URL 中的 region（可选）")
}

func registerRbFlags(fs *flag.FlagSet) {
	bindCreds(fs)
	bindSrcDstCreds(fs)
	bindRF(fs) // -f 跳过确认
}

// runMakeBucket cmdMB 入口
func runMakeBucket(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet(cmdMB, flag.ContinueOnError)
	registerMbFlags(fs)
	fs.Usage = func() { printMbUsage() }
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	pos := fs.Args()
	if len(pos) != 1 {
		fmt.Fprintln(os.Stderr, "mb <BUCKET-URL>：需要 1 个 URL")
		printMbUsage()
		return exitUsage
	}
	resolveCreds()

	target, err := cmd.ParseObjectString(pos[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "解析目标失败: %v\n", err)
		return exitUsage
	}
	if target.Key != "" || strings.TrimSpace(target.Prefix) != "" {
		fmt.Fprintln(os.Stderr, "mb 仅接受桶根 URL，例如 cos://bucket.region/")
		return exitUsage
	}

	region := target.Region
	if flMbRegion != "" {
		region = flMbRegion
	}

	store, err := buildStorage(target.StorageType, target.Bucket, region)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFail
	}
	admin, ok := store.(objstore.BucketAdmin)
	if !ok {
		fmt.Fprintln(os.Stderr, "当前 provider 不支持桶级别管理")
		return exitFail
	}

	if err := admin.CreateBucket(ctx); err != nil {
		if errors.Is(err, objstore.ErrBucketAlreadyOwnedByYou) {
			fmt.Fprintf(os.Stderr, "桶 %s://%s.%s 已存在且为当前账号所有，无需创建\n",
				target.StorageType, target.Bucket, region)
			if cmd.IsJSON() {
				cmd.EmitJSON(map[string]interface{}{
					"command": "mb", "ok": true, "already_exists": true,
					"provider": string(target.StorageType), "bucket": target.Bucket, "region": region,
				})
			}
			return exitOK
		}
		fmt.Fprintf(os.Stderr, "mb 失败: %v\n", err)
		return exitFail
	}
	fmt.Printf("✅ 已创建桶 %s://%s.%s\n", target.StorageType, target.Bucket, region)
	if cmd.IsJSON() {
		cmd.EmitJSON(map[string]interface{}{
			"command": "mb", "ok": true,
			"provider": string(target.StorageType), "bucket": target.Bucket, "region": region,
		})
	}
	return exitOK
}

// runRemoveBucket cmdRB 入口
func runRemoveBucket(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet(cmdRB, flag.ContinueOnError)
	registerRbFlags(fs)
	fs.Usage = func() { printRbUsage() }
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	pos := fs.Args()
	if len(pos) != 1 {
		fmt.Fprintln(os.Stderr, "rb <BUCKET-URL>：需要 1 个 URL")
		printRbUsage()
		return exitUsage
	}
	resolveCreds()

	target, err := cmd.ParseObjectString(pos[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "解析目标失败: %v\n", err)
		return exitUsage
	}
	if target.Key != "" || strings.TrimSpace(target.Prefix) != "" {
		fmt.Fprintln(os.Stderr, "rb 仅接受桶根 URL，例如 cos://bucket.region/")
		return exitUsage
	}

	store, err := buildStorage(target.StorageType, target.Bucket, target.Region)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFail
	}
	admin, ok := store.(objstore.BucketAdmin)
	if !ok {
		fmt.Fprintln(os.Stderr, "当前 provider 不支持桶级别管理")
		return exitFail
	}

	// 二次确认：rb 是危险操作
	if !flForce {
		fmt.Fprintf(os.Stderr,
			"⚠️  即将删除桶 %s://%s.%s（要求桶为空）。继续? [y/N] ",
			target.StorageType, target.Bucket, target.Region)
		var ans string
		fmt.Scanln(&ans)
		if !strings.EqualFold(ans, "y") {
			fmt.Fprintln(os.Stderr, "已取消")
			return exitUsage
		}
	}

	if err := admin.DeleteBucket(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "rb 失败: %v\n", err)
		return exitFail
	}
	fmt.Printf("✅ 已删除桶 %s://%s.%s\n", target.StorageType, target.Bucket, target.Region)
	if cmd.IsJSON() {
		cmd.EmitJSON(map[string]interface{}{
			"command": "rb", "ok": true,
			"provider": string(target.StorageType), "bucket": target.Bucket, "region": target.Region,
		})
	}
	return exitOK
}

func printMbUsage() {
	fmt.Print(`objcli mb - 创建桶（make bucket）

用法:
  objcli mb <BUCKET-URL>

示例:
  objcli mb cos://my-new-bucket.ap-beijing/
  objcli mb s3://my-new-bucket.ap-northeast-1/

说明:
- 仅接受桶根 URL（不能带 key 或前缀）
- 如果桶已存在且为当前账号所有，会输出提示后正常退出（exit 0）
- 如果桶被别人占用，会按底层 SDK 报错失败
`)
}

func printRbUsage() {
	fmt.Print(`objcli rb - 删除桶（remove bucket）

用法:
  objcli rb <BUCKET-URL> [-f]

选项:
  -f      跳过 [y/N] 确认

示例:
  objcli rb cos://my-empty-bucket.ap-beijing/
  objcli rb s3://my-empty-bucket.us-east-1/ -f

说明:
- 仅接受桶根 URL
- 要求桶为空，否则底层 SDK 会报错（请先 objcli rm -r）
- 默认会询问 [y/N]，加 -f 跳过
`)
}
