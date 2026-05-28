package main

import (
	"context"
	"errors"
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

// Linux 同名指令的退出码约定：
//   cp/rm/ls 成功         → 0
//   cp/rm    部分或全部失败 → 1
//   ls       找不到对象/前缀 → 2
//   通用错误（缺参数、URL 解析失败等）→ 2
const (
	exitOK    = 0
	exitFail  = 1
	exitUsage = 2
)

// 子命令名
const (
	cmdCP      = "cp"
	cmdRM      = "rm"
	cmdLS      = "ls"
	cmdSYNC    = "sync"
	cmdPRESIGN = "presign"
)

// ---------- 全局选项（被各子命令复用） ----------

// 凭证（也可走环境变量）
var (
	flS3AK    string
	flS3SK    string
	flCOSID   string
	flCOSSK   string
)

// 拷贝/删除/列举共用
var (
	flRecursive bool
	flForce     bool
)

// cp 专用
var (
	flChunkMB           int
	flChunkConcurrency  int
	flObjectConcurrency int
	flKeyList           string
)

// rm 专用
var (
	flDelConcurrency int
	flURLDecode      bool
)

// taskobserver（cp 专用）
var (
	flObsBucket    string
	flObsRegion    string
	flObsSecretID  string
	flObsSecretKey string
	flObsBaseURL   string
	flObsTask      string
)

func main() {
	if len(os.Args) < 2 {
		printRootUsage()
		os.Exit(exitUsage)
	}

	sub := os.Args[1]
	rest := splitFlagsAndPositional(os.Args[2:])

	ctx := context.Background()
	switch sub {
	case cmdCP:
		os.Exit(runCopy(ctx, rest))
	case cmdRM:
		os.Exit(runRemove(ctx, rest))
	case cmdLS:
		os.Exit(runList(ctx, rest))
	case cmdSYNC:
		os.Exit(runSync(ctx, rest))
	case cmdPRESIGN:
		os.Exit(runPresign(ctx, rest))
	case "-h", "--help", "help":
		printRootUsage()
		os.Exit(exitOK)
	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n", sub)
		printRootUsage()
		os.Exit(exitUsage)
	}
}

// ============================================================
// cp <SRC> <DST>
// ============================================================

func runCopy(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("cp", flag.ContinueOnError)
	bindCreds(fs)
	bindRF(fs)
	fs.IntVar(&flChunkMB, "chunk", 128, "分块大小 MB（cos→cos 建议 512）")
	fs.IntVar(&flChunkConcurrency, "concurrency", 5, "单文件分块并发数")
	fs.IntVar(&flObjectConcurrency, "obj-concurrency", 3, "多文件并发数（前缀/列表模式）")
	fs.StringVar(&flKeyList, "key-list", "", "对象 URL 列表文件（本地路径或 HTTP/HTTPS）")
	bindObs(fs)
	fs.Usage = func() { printCopyUsage() }
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	pos := fs.Args()

	resolveCreds()

	// 判断是 list 模式还是普通模式
	if flKeyList != "" {
		// list 模式：只需 1 个位置参数（DST）
		if len(pos) != 1 {
			fmt.Fprintln(os.Stderr, "cp -key-list <DST>：需要 1 个目标 URL")
			printCopyUsage()
			return exitUsage
		}
		dst, err := cmd.ParseObjectString(pos[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "解析目标失败: %v\n", err)
			return exitUsage
		}
		return doCopy(ctx, nil, dst, true)
	}

	// 普通模式：需要 2 个位置参数（SRC + DST）
	if len(pos) != 2 {
		fmt.Fprintln(os.Stderr, "cp <SRC> <DST>：需要 2 个路径")
		printCopyUsage()
		return exitUsage
	}

	srcIsCloud := isCloudURL(pos[0])
	dstIsCloud := isCloudURL(pos[1])

	// 本地↔云：走独立分支
	if !srcIsCloud || !dstIsCloud {
		if !srcIsCloud && !dstIsCloud {
			fmt.Fprintln(os.Stderr, "cp 不支持本地 → 本地，请用系统 cp 命令")
			return exitUsage
		}
		if srcIsCloud {
			// 云 → 本地
			src, err := cmd.ParseObjectString(pos[0])
			if err != nil {
				fmt.Fprintf(os.Stderr, "解析源失败: %v\n", err)
				return exitUsage
			}
			return doDownload(ctx, src, pos[1])
		}
		// 本地 → 云
		dst, err := cmd.ParseObjectString(pos[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "解析目标失败: %v\n", err)
			return exitUsage
		}
		return doUpload(ctx, pos[0], dst)
	}

	// 云↔云走原有逻辑
	src, err := cmd.ParseObjectString(pos[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "解析源失败: %v\n", err)
		return exitUsage
	}
	dst, err := cmd.ParseObjectString(pos[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "解析目标失败: %v\n", err)
		return exitUsage
	}
	return doCopy(ctx, src, dst, false)
}

// isCloudURL 判断路径是否为云存储 URL
func isCloudURL(p string) bool {
	low := strings.ToLower(p)
	return strings.HasPrefix(low, "cos://") || strings.HasPrefix(low, "s3://") ||
		strings.HasPrefix(low, "https://") || strings.HasPrefix(low, "http://")
}

// ============================================================
// 本地 → 云（upload）
// ============================================================

func doUpload(ctx context.Context, localPath string, dst *cmd.ObjectString) int {
	store, err := buildStorage(dst.StorageType, dst.Bucket, dst.Region)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFail
	}
	cfg := cmd.LocalConfig{
		LocalPath:         localPath,
		ChunkMB:           flChunkMB,
		ChunkConcurrency:  flChunkConcurrency,
		ObjectConcurrency: flObjectConcurrency,
		Recursive:         flRecursive,
		Force:             flForce,
	}
	if dst.IsPrefix {
		cfg.DstPrefix = dst.Key
	} else {
		cfg.DstKey = dst.Key
	}
	engine := cmd.NewLocalEngine(store, cfg)
	if err := engine.Upload(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "上传失败: %v\n", err)
		return exitFail
	}
	return exitOK
}

// ============================================================
// 云 → 本地（download）
// ============================================================

func doDownload(ctx context.Context, src *cmd.ObjectString, localPath string) int {
	store, err := buildStorage(src.StorageType, src.Bucket, src.Region)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFail
	}
	cfg := cmd.LocalConfig{
		LocalPath:         localPath,
		ChunkMB:           flChunkMB,
		ChunkConcurrency:  flChunkConcurrency,
		ObjectConcurrency: flObjectConcurrency,
		Recursive:         flRecursive,
		Force:             flForce,
	}
	if src.IsPrefix {
		cfg.SrcPrefix = src.Prefix
	} else {
		cfg.SrcKey = src.Key
	}
	engine := cmd.NewLocalEngine(store, cfg)
	if err := engine.Download(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "下载失败: %v\n", err)
		return exitFail
	}
	return exitOK
}

func doCopy(ctx context.Context, src, dst *cmd.ObjectString, isList bool) int {
	// 构建目标 storage
	dstStorage, err := buildStorage(dst.StorageType, dst.Bucket, dst.Region)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFail
	}

	var srcStorage objstore.Store
	if !isList {
		srcStorage, err = buildStorage(src.StorageType, src.Bucket, src.Region)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return exitFail
		}
	}

	cfg := cmd.CopyConfig{
		ChunkMB:           flChunkMB,
		ChunkConcurrency:  flChunkConcurrency,
		ObjectConcurrency: flObjectConcurrency,
		Recursive:         flRecursive,
		Force:             flForce,
	}
	if isList {
		cfg.KeyListSource = flKeyList
		cfg.DstPrefix = dst.Key // 整个 key 作为目标前缀（list 模式）
	} else {
		if src.IsPrefix {
			cfg.SrcPrefix = src.Prefix
			cfg.DstPrefix = dst.Key
		} else {
			cfg.SrcKey = src.Key
			if dst.Key == "" || strings.HasSuffix(dst.Key, "/") {
				// dst 是前缀 → 自动拼上 src 文件名
				cfg.DstKey = dst.Key + lastSegment(src.Key)
			} else {
				cfg.DstKey = dst.Key
			}
		}
	}

	engine := cmd.NewEngine(srcStorage, dstStorage, cfg).
		WithCreds(objstore.ProviderCOS, flCOSID, flCOSSK).
		WithCreds(objstore.ProviderS3, flS3AK, flS3SK)

	if err := engine.CheckMemory(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFail
	}

	// taskobserver
	obsCfg := taskobserver.Config{
		Bucket:      envOr(flObsBucket, "TASKOBS_BUCKET"),
		Region:      envOr(flObsRegion, "TASKOBS_REGION"),
		SecretID:    envOr(flObsSecretID, "TASKOBS_SECRET_ID"),
		SecretKey:   envOr(flObsSecretKey, "TASKOBS_SECRET_KEY"),
		BaseURL:     envOr(flObsBaseURL, "TASKOBS_BASE_URL"),
		TaskName:    envOr(flObsTask, "TASKOBS_TASK"),
		Interval:    5 * time.Second,
		ExtraWriter: os.Stderr,
	}
	var obs *taskobserver.Observer
	if obsCfg.Bucket != "" && obsCfg.SecretID != "" {
		if obsCfg.TaskName == "" {
			if isList {
				obsCfg.TaskName = "list→" + dst.Raw
			} else {
				obsCfg.TaskName = src.Raw + " → " + dst.Raw
			}
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
				return int(done >> 20), int(total >> 20)
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
		fmt.Fprintf(os.Stderr, "失败: %v\n", runErr)
		return exitFail
	}
	return exitOK
}

// ============================================================
// rm <TARGET>
// ============================================================

func runRemove(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("rm", flag.ContinueOnError)
	bindCreds(fs)
	bindRF(fs)
	fs.IntVar(&flDelConcurrency, "delete-concurrency", 3, "并发删除数")
	fs.BoolVar(&flURLDecode, "url-decode", false, "列表模式下对 key 做 URL decode")
	fs.StringVar(&flKeyList, "key-list", "", "对象 URL 列表文件（无需提供 TARGET）")
	fs.Usage = func() { printRemoveUsage() }
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	pos := fs.Args()

	resolveCreds()

	// list 模式
	if flKeyList != "" {
		if len(pos) != 0 {
			fmt.Fprintln(os.Stderr, "rm -key-list <FILE>：不应再传位置参数")
			return exitUsage
		}
		cfg := cmd.DeleteConfig{
			KeyList:     flKeyList,
			Concurrency: flDelConcurrency,
			URLDecode:   flURLDecode,
			Recursive:   flRecursive,
			Force:       flForce,
		}
		engine := cmd.NewDeleteEngine(nil, cfg)
		if err := engine.Run(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "rm 失败: %v\n", err)
			return exitFail
		}
		return exitOK
	}

	// 单 / 前缀模式
	if len(pos) != 1 {
		fmt.Fprintln(os.Stderr, "rm <TARGET>：需要 1 个 URL")
		printRemoveUsage()
		return exitUsage
	}
	target, err := cmd.ParseObjectString(pos[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "解析目标失败: %v\n", err)
		return exitUsage
	}

	storage, err := buildStorage(target.StorageType, target.Bucket, target.Region)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFail
	}

	cfg := cmd.DeleteConfig{
		Concurrency: flDelConcurrency,
		URLDecode:   flURLDecode,
		Recursive:   flRecursive,
		Force:       flForce,
	}
	if target.IsPrefix {
		cfg.Prefix = target.Prefix
	} else {
		cfg.Key = target.Key
	}

	engine := cmd.NewDeleteEngine(storage, cfg)
	if err := engine.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "rm 失败: %v\n", err)
		return exitFail
	}
	return exitOK
}

// ============================================================
// ls <TARGET>
// ============================================================

func runList(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("ls", flag.ContinueOnError)
	bindCreds(fs)
	bindRF(fs)
	fs.Usage = func() { printListUsage() }
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	pos := fs.Args()

	resolveCreds()

	if len(pos) != 1 {
		fmt.Fprintln(os.Stderr, "ls <TARGET>：需要 1 个 URL")
		printListUsage()
		return exitUsage
	}
	target, err := cmd.ParseObjectString(pos[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "解析目标失败: %v\n", err)
		return exitUsage
	}

	storage, err := buildStorage(target.StorageType, target.Bucket, target.Region)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFail
	}

	// 用 Prefix（去掉 *），如果是单 key 则等于 Key
	prefix := target.Prefix
	if !target.IsPrefix && target.Key != "" {
		prefix = target.Key
	}

	cfg := cmd.ListConfig{
		Prefix:    prefix,
		Recursive: flRecursive,
	}
	engine := cmd.NewListEngine(storage, cfg)
	err = engine.Run(ctx)
	if err == nil {
		return exitOK
	}
	if errors.Is(err, cmd.ErrNoSuchObject) {
		fmt.Fprintf(os.Stderr, "ls: 未找到任何对象（prefix=%q）\n", prefix)
		return exitUsage // exit 2，对齐 ls 的 ENOENT
	}
	fmt.Fprintf(os.Stderr, "ls 失败: %v\n", err)
	return exitFail
}

// ============================================================
// sync <SRC> <DST>
// ============================================================

var (
	flSyncDelete bool
	flSyncDryRun bool
)

func runSync(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	bindCreds(fs)
	fs.BoolVar(&flRecursive, "r", true, "递归（sync 默认 true）")
	fs.BoolVar(&flForce, "f", false, "跳过确认")
	fs.IntVar(&flChunkMB, "chunk", 128, "分块大小 MB")
	fs.IntVar(&flChunkConcurrency, "concurrency", 5, "单文件分块并发数")
	fs.IntVar(&flObjectConcurrency, "obj-concurrency", 3, "多文件并发数")
	fs.BoolVar(&flSyncDelete, "delete", false, "删除目标中多余的对象")
	fs.BoolVar(&flSyncDryRun, "dry-run", false, "仅打印计划，不执行")
	fs.Usage = func() { printSyncUsage() }
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	pos := fs.Args()
	if len(pos) != 2 {
		fmt.Fprintln(os.Stderr, "sync <SRC> <DST>：需要 2 个路径")
		printSyncUsage()
		return exitUsage
	}
	resolveCreds()

	src, err := buildSyncSide(pos[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "解析源失败: %v\n", err)
		return exitUsage
	}
	dst, err := buildSyncSide(pos[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "解析目标失败: %v\n", err)
		return exitUsage
	}
	if src.IsLocal && dst.IsLocal {
		fmt.Fprintln(os.Stderr, "sync 不支持本地↔本地")
		return exitUsage
	}

	cfg := cmd.SyncConfig{
		Recursive:         flRecursive,
		Delete:            flSyncDelete,
		DryRun:            flSyncDryRun,
		ChunkMB:           flChunkMB,
		ChunkConcurrency:  flChunkConcurrency,
		ObjectConcurrency: flObjectConcurrency,
	}
	engine := cmd.NewSyncEngine(src, dst, cfg)
	if err := engine.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "sync 失败: %v\n", err)
		return exitFail
	}
	return exitOK
}

func buildSyncSide(p string) (cmd.SyncSide, error) {
	if !isCloudURL(p) {
		return cmd.SyncSide{IsLocal: true, Local: p}, nil
	}
	obj, err := cmd.ParseObjectString(p)
	if err != nil {
		return cmd.SyncSide{}, err
	}
	store, err := buildStorage(obj.StorageType, obj.Bucket, obj.Region)
	if err != nil {
		return cmd.SyncSide{}, err
	}
	prefix := obj.Prefix
	if !obj.IsPrefix {
		prefix = obj.Key
	}
	return cmd.SyncSide{Store: store, Prefix: prefix}, nil
}

// ============================================================
// presign <TARGET>
// ============================================================

var (
	flPresignMethod  string
	flPresignExpires int
)

func runPresign(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("presign", flag.ContinueOnError)
	bindCreds(fs)
	fs.StringVar(&flPresignMethod, "method", "GET", "GET | PUT")
	fs.IntVar(&flPresignExpires, "expires", 3600, "有效期，秒")
	fs.Usage = func() { printPresignUsage() }
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	pos := fs.Args()
	if len(pos) != 1 {
		fmt.Fprintln(os.Stderr, "presign <TARGET>：需要 1 个 URL")
		printPresignUsage()
		return exitUsage
	}
	resolveCreds()

	target, err := cmd.ParseObjectString(pos[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "解析目标失败: %v\n", err)
		return exitUsage
	}
	if target.IsPrefix || target.Key == "" {
		fmt.Fprintln(os.Stderr, "presign 必须使用单对象 URL（不能是前缀）")
		return exitUsage
	}
	store, err := buildStorage(target.StorageType, target.Bucket, target.Region)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFail
	}
	u, err := cmd.Presign(ctx, store, cmd.PresignConfig{
		Key:     target.Key,
		Method:  flPresignMethod,
		Expires: time.Duration(flPresignExpires) * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "presign 失败: %v\n", err)
		return exitFail
	}
	fmt.Println(u)
	return exitOK
}

// ============================================================
// helpers
// ============================================================

func bindCreds(fs *flag.FlagSet) {
	fs.StringVar(&flS3AK, "s3-ak", "", "AWS Access Key ID（缺省读 AWS_ACCESS_KEY_ID）")
	fs.StringVar(&flS3SK, "s3-sk", "", "AWS Secret Access Key（缺省读 AWS_SECRET_ACCESS_KEY）")
	fs.StringVar(&flCOSID, "cos-id", "", "腾讯云 SecretId（缺省读 TENCENT_SECRET_ID）")
	fs.StringVar(&flCOSSK, "cos-sk", "", "腾讯云 SecretKey（缺省读 TENCENT_SECRET_KEY）")
}

func bindRF(fs *flag.FlagSet) {
	fs.BoolVar(&flRecursive, "r", false, "递归（前缀/glob 模式）")
	fs.BoolVar(&flForce, "f", false, "前缀/glob 模式：跳过用户确认")
}

func bindObs(fs *flag.FlagSet) {
	fs.StringVar(&flObsBucket, "obs-bucket", "", "taskobserver: COS 桶名 [TASKOBS_BUCKET]")
	fs.StringVar(&flObsRegion, "obs-region", "", "taskobserver: COS 地域 [TASKOBS_REGION]")
	fs.StringVar(&flObsSecretID, "obs-secret-id", "", "taskobserver: COS SecretId [TASKOBS_SECRET_ID]")
	fs.StringVar(&flObsSecretKey, "obs-secret-key", "", "taskobserver: COS SecretKey [TASKOBS_SECRET_KEY]")
	fs.StringVar(&flObsBaseURL, "obs-base-url", "", "taskobserver: 自定义域名 [TASKOBS_BASE_URL]")
	fs.StringVar(&flObsTask, "obs-task", "", "taskobserver: 任务名称 [TASKOBS_TASK]")
}

func resolveCreds() {
	flS3AK = envOr(flS3AK, "AWS_ACCESS_KEY_ID")
	flS3SK = envOr(flS3SK, "AWS_SECRET_ACCESS_KEY")
	flCOSID = envOr(flCOSID, "TENCENT_SECRET_ID")
	flCOSSK = envOr(flCOSSK, "TENCENT_SECRET_KEY")
}

func buildStorage(provider objstore.ProviderType, bucket, region string) (objstore.Store, error) {
	switch provider {
	case objstore.ProviderCOS:
		if flCOSID == "" {
			return nil, fmt.Errorf("缺少 COS 凭证：-cos-id 或 TENCENT_SECRET_ID")
		}
		if flCOSSK == "" {
			return nil, fmt.Errorf("缺少 COS 凭证：-cos-sk 或 TENCENT_SECRET_KEY")
		}
		return objstore.New(objstore.Config{
			Provider: objstore.ProviderCOS, Bucket: bucket, Region: region,
			SecretID: flCOSID, SecretKey: flCOSSK,
		})
	case objstore.ProviderS3:
		if flS3AK == "" {
			return nil, fmt.Errorf("缺少 S3 凭证：-s3-ak 或 AWS_ACCESS_KEY_ID")
		}
		if flS3SK == "" {
			return nil, fmt.Errorf("缺少 S3 凭证：-s3-sk 或 AWS_SECRET_ACCESS_KEY")
		}
		return objstore.New(objstore.Config{
			Provider: objstore.ProviderS3, Bucket: bucket, Region: region,
			SecretID: flS3AK, SecretKey: flS3SK,
		})
	}
	return nil, fmt.Errorf("不支持的存储类型: %s", provider)
}

func envOr(flagVal, envKey string) string {
	if flagVal != "" {
		return flagVal
	}
	return os.Getenv(envKey)
}

func lastSegment(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}

// splitFlagsAndPositional 重排参数：flag 在前、位置参数在后。
// Go 标准 flag 包遇到第一个非 flag 就停止解析；这里手动把 flag 全部前置，
// 让用户可以像 Linux cp/rm/ls 一样把 flag 放在任意位置。
func splitFlagsAndPositional(args []string) []string {
	// 带值的 flag（吞后一个参数）
	valueFlags := map[string]bool{
		"-s3-ak": true, "-s3-sk": true,
		"-cos-id": true, "-cos-sk": true,
		"-chunk": true, "-concurrency": true, "-obj-concurrency": true,
		"-key-list": true, "-delete-concurrency": true,
		"-obs-bucket": true, "-obs-region": true,
		"-obs-secret-id": true, "-obs-secret-key": true,
		"-obs-base-url": true, "-obs-task": true,
		"-method": true, "-expires": true,
	}

	var flags, positional []string
	i := 0
	for i < len(args) {
		a := args[i]
		normalized := a
		if strings.HasPrefix(a, "--") {
			normalized = a[1:]
		}
		if strings.HasPrefix(a, "-") && strings.Contains(a, "=") {
			flags = append(flags, a)
			i++
			continue
		}
		if strings.HasPrefix(a, "-") {
			flags = append(flags, a)
			if valueFlags[normalized] && i+1 < len(args) {
				flags = append(flags, args[i+1])
				i += 2
				continue
			}
			i++
			continue
		}
		positional = append(positional, a)
		i++
	}
	return append(flags, positional...)
}

// ============================================================
// usage
// ============================================================

func printRootUsage() {
	fmt.Print(`objcli - 对象存储统一 CLI
支持 AWS S3 与腾讯云 COS。

用法:
  objcli cp      <SRC>    <DST>     [选项]   # 拷贝（云↔云、本地↔云）
  objcli sync    <SRC>    <DST>     [选项]   # 增量同步
  objcli rm      <TARGET>           [选项]   # 删除
  objcli ls      <TARGET>           [选项]   # 列举
  objcli presign <TARGET>           [选项]   # 预签名 URL

URL 格式:
  cos://<bucket>.<region>/<key-or-prefix>
  s3://<bucket>.<region>/<key-or-prefix>
  /local/path                              （本地路径，仅 cp / sync 可用）

  以 "/" 结尾或包含 "*" 视为前缀；空 key 等价于桶根。
  例:
    cos://mybucket.ap-beijing/data/file.zip   单文件
    cos://mybucket.ap-beijing/data/           前缀
    cos://mybucket.ap-beijing/data/*          通配符
    cos://mybucket.ap-beijing/                整个桶
    /local/file.zip                           本地文件
    /local/dir/                               本地目录

退出码（对齐 Linux cp/rm/ls）:
  0  成功
  1  操作过程出错（部分或全部失败）
  2  参数错误 / ls 找不到对象或前缀

凭证（命令行 > 环境变量）:
  S3 :  -s3-ak / -s3-sk      或 AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
  COS:  -cos-id / -cos-sk    或 TENCENT_SECRET_ID / TENCENT_SECRET_KEY

详细用法：
  objcli cp -h
  objcli rm -h
  objcli ls -h
  objcli sync -h
  objcli presign -h
`)
}

func printCopyUsage() {
	fmt.Print(`objcli cp - 拷贝对象

用法:
  objcli cp <SRC> <DST> [选项]
  objcli cp -key-list <FILE> <DST> [选项]

路径类型组合:
  云 → 云:    cp cos://b.r/k cos://b2.r2/k
  本地 → 云:  cp /local/file.zip cos://b.r/key.zip
  云 → 本地:  cp cos://b.r/key.zip /local/    (末尾 / 表示目录)
  本地 → 本地: 不支持，请用系统 cp

模式:
  单文件:        SRC=cos://b.r/key            DST=cos://b2.r2/key  或  cos://b2.r2/dir/
  前缀批量:      SRC=cos://b.r/dir/   或 .../dir/*    DST=cos://b2.r2/newdir/   配 -r [-f]
  URL 列表:      -key-list FILE                          DST=cos://b2.r2/dir/
  本地上传:      SRC=/local/file.zip          DST=cos://b.r/dir/
  本地下载:      SRC=cos://b.r/key             DST=/local/dir/

选项:
  -r                  递归（前缀模式）
  -f                  前缀模式：跳过确认
  -chunk INT          分块大小 MB（默认 128）
  -concurrency INT    单文件分块并发（默认 5）
  -obj-concurrency INT 多文件并发（默认 3）
  -key-list FILE      对象 URL 列表（本地路径或 HTTP/HTTPS URL）
  -s3-ak / -s3-sk / -cos-id / -cos-sk

taskobserver（可选）:
  -obs-bucket / -obs-region / -obs-secret-id / -obs-secret-key
  -obs-base-url / -obs-task
  对应环境变量 TASKOBS_*

示例:
  objcli cp s3://my-s3.us-east-1/file.zip cos://my-cos.ap-beijing/file.zip
  objcli cp /tmp/data.tar.gz cos://b.ap-beijing/backup/
  objcli cp cos://b.ap-beijing/data/ /tmp/data/ -r -f
  objcli cp 'cos://src.ap-singapore/data/*' cos://dst.ap-beijing/backup/ -r -f -chunk 512
  objcli cp -key-list /tmp/list.txt cos://dst.ap-nanjing/import/
`)
}

func printSyncUsage() {
	fmt.Print(`objcli sync - 增量同步

用法:
  objcli sync <SRC> <DST> [选项]

路径类型组合（与 cp 一致，允许本地与云互为一端）。
同步逻辑：
  - 以 ETag/size 判断是否需要复制
  - 默认不删除。-delete 后才会删除目标中多余的对象
  - -dry-run 仅打印计划

选项:
  -r              递归（默认 true）
  -delete         删除目标多余对象
  -dry-run        仅打印计划不执行
  -chunk / -concurrency / -obj-concurrency  同 cp
  -s3-ak / -s3-sk / -cos-id / -cos-sk

示例:
  objcli sync /local/dir/ cos://b.r/backup/ -delete
  objcli sync cos://b1.r1/data/ cos://b2.r2/data/ -dry-run
  objcli sync cos://b.r/logs/ /local/logs/ -delete
`)
}

func printPresignUsage() {
	fmt.Print(`objcli presign - 生成预签名 URL

用法:
  objcli presign <TARGET> [选项]

选项:
  -method GET|PUT  默认 GET
  -expires INT     有效期，秒（默认 3600）
  -s3-ak / -s3-sk / -cos-id / -cos-sk

示例:
  objcli presign cos://my-bucket.ap-beijing/path/file.zip
  objcli presign cos://my-bucket.ap-beijing/upload/x.bin -method PUT -expires 600
`)
}

func printRemoveUsage() {
	fmt.Print(`objcli rm - 删除对象

用法:
  objcli rm <TARGET> [选项]
  objcli rm -key-list <FILE> [选项]

模式:
  单文件:    TARGET = cos://b.r/key
  前缀:      TARGET = cos://b.r/dir/   或 .../dir/*    配 -r [-f]
  URL 列表:  -key-list FILE（无需 TARGET）

选项:
  -r                       递归（前缀模式）
  -f                       前缀模式：跳过确认
  -delete-concurrency INT  并发删除数（默认 3）
  -url-decode              列表模式对 key 做 URL decode
  -key-list FILE           对象 URL 列表
  -s3-ak / -s3-sk / -cos-id / -cos-sk

示例:
  objcli rm cos://my-bucket.ap-beijing/path/file.zip
  objcli rm 'cos://my-bucket.ap-beijing/tmp/*' -r -f
  objcli rm -key-list /tmp/del-list.txt
`)
}

func printListUsage() {
	fmt.Print(`objcli ls - 列举对象

用法:
  objcli ls <TARGET> [选项]

TARGET:
  cos://b.r/         整桶
  cos://b.r/dir/     某前缀
  cos://b.r/dir/*    带通配符（与 -r 等价于递归）

选项:
  -r              递归列举（默认仅当前层）
  -s3-ak / -s3-sk / -cos-id / -cos-sk

输出列:
  TYPE  SIZE  LAST-MODIFIED  ETAG  OBJECT
  （OBJECT 形如 cos://bucket/key）

示例:
  objcli ls cos://my-bucket.ap-beijing/logs/ -r
  objcli ls s3://my-s3.us-east-1/data/2026/
`)
}