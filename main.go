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
	cmdMV      = "mv"
	cmdSYNC    = "sync"
	cmdPRESIGN = "presign"
	cmdRESUME  = "resume"
)

// ---------- 全局选项（被各子命令复用） ----------

// 凭证（也可走环境变量）
var (
	flS3AK         string
	flS3SK         string
	flS3Endpoint   string
	flAWSProfile   string // -aws-profile NAME：AK/SK 未依时走该 profile（代替 default chain）
	flCOSID   string
	flCOSSK   string
)

// 跨账号 / 跨 endpoint 拷贝（仅 cp）：src / dst 分别指定凭证
var (
	flSrcS3AK       string
	flSrcS3SK       string
	flSrcS3Endpoint string
	flSrcAWSProfile string
	flSrcRegion     string // 覆盖 src URL 中的 region（可选）

	flDstS3AK       string
	flDstS3SK       string
	flDstS3Endpoint string
	flDstAWSProfile string
	flDstRegion     string

	flSrcCOSID string
	flSrcCOSSK string
	flDstCOSID string
	flDstCOSSK string
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

// 对象属性（cp / mv / sync 共用）
var (
	flContentType  string
	flCacheControl string
	flMetadata     cmd.StringSliceFlag // -metadata key=value，可重复
	flStorageClass string
	flACL          string
	flTag          cmd.StringSliceFlag // -tag key=value，可重复
	flSSE          string              // -sse: 服务端加密类型
	flSSEKMSKey    string              // -sse-kms-key: KMS CMK ID/ARN/Alias（仅在 sse=*kms* 时生效）
	flChunkSet     bool                // 用户是否显式设了 -chunk（实现中根据该标记决定是否走自适应）
	flDryRun       bool
)

// rm 专用
var (
	flDelConcurrency int
	flURLDecode      bool
)

// cp 专用：强制本机中转
var flForceClientCopy bool

// 重试 / 限速（cp / mv / sync 共用）
var (
	flRetries     int
	flRetryBaseMS int
	flBandwidth   string
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
	rawRest := os.Args[2:]

	// 全局预扫：-o json / --output json
	rawRest = extractOutputFlag(rawRest)

	rest := splitFlagsAndPositional(rawRest)

	ctx := context.Background()
	switch sub {
	case cmdCP:
		os.Exit(runCopy(ctx, rest))
	case cmdRM:
		os.Exit(runRemove(ctx, rest))
	case cmdLS:
		os.Exit(runList(ctx, rest))
	case cmdMV:
		os.Exit(runMove(ctx, rest))
	case cmdSYNC:
		os.Exit(runSync(ctx, rest))
	case cmdPRESIGN:
		os.Exit(runPresign(ctx, rest))
	case cmdRESUME:
		// resume 子命令不需要重排：子命令名需保留位置
		os.Exit(runResume(ctx, rawRest))
	case cmdVERSION, "-v", "--version":
		printVersion()
		os.Exit(exitOK)
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

// registerCpFlags 返回 cp 子命令的 flagset，供 runCopy 和 collectBoolFlags 复用。
func registerCpFlags(fs *flag.FlagSet) {
	bindCreds(fs)
	bindSrcDstCreds(fs)
	bindRF(fs)
	bindFilter(fs)
	bindPutOpts(fs)
	fs.IntVar(&flChunkMB, "chunk", 0, "分块大小 MB，0=根据总大小自适应（<5GB→8 / 5-50GB→32 / 50-500GB→128 / >500GB→512）")
	fs.IntVar(&flChunkConcurrency, "concurrency", 5, "单文件分块并发数")
	fs.IntVar(&flObjectConcurrency, "obj-concurrency", 3, "多文件并发数（前缀/列表模式）")
	fs.StringVar(&flKeyList, "key-list", "", "对象 URL 列表文件（本地路径或 HTTP/HTTPS）")
	fs.BoolVar(&flDryRun, "dry-run", false, "仅打印将要执行的动作，不真正上传/拷贝/下载")
	fs.BoolVar(&flForceClientCopy, "force-client-copy", false, "同 provider 跨账号拷贝时强制走本机中转（跨 endpoint/跨账号需要，issue #13）")
	bindReliability(fs)
	bindObs(fs)
}

func runCopy(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("cp", flag.ContinueOnError)
	registerCpFlags(fs)
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
	if flDstRegion != "" {
		dst.Region = flDstRegion
	}
	store, err := buildStorageSide(dst.StorageType, dst.Bucket, dst.Region, "dst")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFail
	}
	putOpts, err := buildPutOptions(string(dst.StorageType))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitUsage
	}
	cfg := cmd.LocalConfig{
		LocalPath:         localPath,
		ChunkMB:           flChunkMB,
		ChunkConcurrency:  flChunkConcurrency,
		ObjectConcurrency: flObjectConcurrency,
		Recursive:         flRecursive,
		Force:             flForce,
		Filter:            buildFilter(),
		PutOptions:        putOpts,
		DryRun:            flDryRun,
	}
	if dst.IsPrefix {
		cfg.DstPrefix = dst.Key
	} else {
		cfg.DstKey = dst.Key
	}
	engine := cmd.NewLocalEngine(store, cfg)
	if err := engine.Upload(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "上传失败: %v\n", err)
		if cmd.IsJSON() {
			cmd.EmitJSON(map[string]interface{}{"command": "cp", "ok": false, "error": err.Error()})
		}
		return exitFail
	}
	if cmd.IsJSON() {
		cmd.EmitJSON(map[string]interface{}{"command": "cp", "ok": true, "src": localPath, "dst": dst.Raw})
	}
	return exitOK
}

// ============================================================
// 云 → 本地（download）
// ============================================================

func doDownload(ctx context.Context, src *cmd.ObjectString, localPath string) int {
	if flSrcRegion != "" {
		src.Region = flSrcRegion
	}
	store, err := buildStorageSide(src.StorageType, src.Bucket, src.Region, "src")
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
		Filter:            buildFilter(),
		DryRun:            flDryRun,
	}
	if src.IsPrefix {
		cfg.SrcPrefix = src.Prefix
	} else {
		cfg.SrcKey = src.Key
	}
	engine := cmd.NewLocalEngine(store, cfg)
	if err := engine.Download(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "下载失败: %v\n", err)
		if cmd.IsJSON() {
			cmd.EmitJSON(map[string]interface{}{"command": "cp", "ok": false, "error": err.Error()})
		}
		return exitFail
	}
	if cmd.IsJSON() {
		cmd.EmitJSON(map[string]interface{}{"command": "cp", "ok": true, "src": src.Raw, "dst": localPath})
	}
	return exitOK
}

func doCopy(ctx context.Context, src, dst *cmd.ObjectString, isList bool) int {
	if src != nil && flSrcRegion != "" {
		src.Region = flSrcRegion
	}
	if dst != nil && flDstRegion != "" {
		dst.Region = flDstRegion
	}
	// 构建目标 storage
	dstStorage, err := buildStorageSide(dst.StorageType, dst.Bucket, dst.Region, "dst")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFail
	}

	var srcStorage objstore.Store
	if !isList {
		srcStorage, err = buildStorageSide(src.StorageType, src.Bucket, src.Region, "src")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return exitFail
		}
	}

	putOpts, err := buildPutOptions(string(dst.StorageType))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitUsage
	}
	cfg := cmd.CopyConfig{
		ChunkMB:           flChunkMB,
		ChunkConcurrency:  flChunkConcurrency,
		ObjectConcurrency: flObjectConcurrency,
		Recursive:         flRecursive,
		Force:             flForce,
		Filter:            buildFilter(),
		PutOptions:        putOpts,
		DryRun:            flDryRun,
		ForceClientCopy:   flForceClientCopy || hasDifferentSideCreds(),
	}
	if err := applyReliability(&cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitUsage
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

	// keylist 模式：src 从 URL 列表逐行解析 → 采用 src side 凭证。
	// 非 keylist 模式 srcStorage 已在 buildStorageSide("src") 阶段预构造，这里的凭证不会被用到。
	sAK, sSK, sEP, sProf, sCOSID, sCOSSK := credSide("src")
	engine := cmd.NewEngine(srcStorage, dstStorage, cfg).
		WithCredsFull(objstore.ProviderCOS, cmd.Creds{AK: sCOSID, SK: sCOSSK}).
		WithCredsFull(objstore.ProviderS3, cmd.Creds{AK: sAK, SK: sSK, Endpoint: envOr(sEP, "AWS_ENDPOINT_URL"), Profile: sProf})
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
		if cmd.IsJSON() {
			cmd.EmitJSON(map[string]interface{}{"command": "cp", "ok": false, "error": runErr.Error()})
		}
		return exitFail
	}
	if cmd.IsJSON() {
		res := map[string]interface{}{"command": "cp", "ok": true}
		if !isList {
			res["src"] = src.Raw
		}
		res["dst"] = dst.Raw
		cmd.EmitJSON(res)
	}
	return exitOK
}

// ============================================================
// rm <TARGET>
// ============================================================

func registerRmFlags(fs *flag.FlagSet) {
	bindCreds(fs)
	bindRF(fs)
	bindFilter(fs)
	fs.IntVar(&flDelConcurrency, "delete-concurrency", 3, "并发删除数")
	fs.BoolVar(&flURLDecode, "url-decode", false, "列表模式下对 key 做 URL decode")
	fs.StringVar(&flKeyList, "key-list", "", "对象 URL 列表文件（无需提供 TARGET）")
	fs.BoolVar(&flDryRun, "dry-run", false, "仅打印将要删除的对象，不真正删除")
}

func runRemove(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("rm", flag.ContinueOnError)
	registerRmFlags(fs)
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
			Filter:      buildFilter(),
			DryRun:      flDryRun,
		}
		engine := cmd.NewDeleteEngine(nil, cfg)
		if err := engine.Run(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "rm 失败: %v\n", err)
			if cmd.IsJSON() {
				cmd.EmitJSON(map[string]interface{}{"command": "rm", "ok": false, "error": err.Error()})
			}
			return exitFail
		}
		if cmd.IsJSON() {
			cmd.EmitJSON(map[string]interface{}{"command": "rm", "ok": true, "key_list": flKeyList})
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
		Filter:      buildFilter(),
		DryRun:      flDryRun,
	}
	if target.IsPrefix {
		cfg.Prefix = target.Prefix
	} else {
		cfg.Key = target.Key
	}

	engine := cmd.NewDeleteEngine(storage, cfg)
	if err := engine.Run(ctx); err != nil {
		if errors.Is(err, cmd.ErrNoSuchObject) {
			fmt.Fprintf(os.Stderr, "rm: 未找到任何对象（target=%q）\n", target.Raw)
			if cmd.IsJSON() {
				cmd.EmitJSON(map[string]interface{}{"command": "rm", "ok": false, "error": "no such object"})
			}
			return exitUsage // 2，对齐 ls ENOENT
		}
		fmt.Fprintf(os.Stderr, "rm 失败: %v\n", err)
		if cmd.IsJSON() {
			cmd.EmitJSON(map[string]interface{}{"command": "rm", "ok": false, "error": err.Error()})
		}
		return exitFail
	}
	if cmd.IsJSON() {
		cmd.EmitJSON(map[string]interface{}{"command": "rm", "ok": true, "target": target.Raw})
	}
	return exitOK
}

// ============================================================
// ls <TARGET>
// ============================================================

func registerLsFlags(fs *flag.FlagSet) {
	bindCreds(fs)
	bindRF(fs)
	bindFilter(fs)
}

func runList(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("ls", flag.ContinueOnError)
	registerLsFlags(fs)
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
		Filter:    buildFilter(),
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
	flSyncDelete   bool
	flSyncDryRun   bool
	flSyncSizeOnly bool
)

func registerSyncFlags(fs *flag.FlagSet) {
	bindCreds(fs)
	bindFilter(fs)
	bindPutOpts(fs)
	fs.BoolVar(&flRecursive, "r", true, "递归（sync 默认 true）")
	fs.BoolVar(&flForce, "f", false, "跳过确认")
	fs.IntVar(&flChunkMB, "chunk", 0, "分块大小 MB，0=自适应")
	fs.IntVar(&flChunkConcurrency, "concurrency", 5, "单文件分块并发数")
	fs.IntVar(&flObjectConcurrency, "obj-concurrency", 3, "多文件并发数")
	fs.BoolVar(&flSyncDelete, "delete", false, "删除目标中多余的对象")
	fs.BoolVar(&flSyncDryRun, "dry-run", false, "仅打印计划，不执行")
	fs.BoolVar(&flSyncSizeOnly, "size-only", false, "增量判定只比 size 不比 mtime")
	bindReliability(fs)
}

func runSync(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	registerSyncFlags(fs)
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
		SizeOnly:          flSyncSizeOnly,
		ChunkMB:           flChunkMB,
		ChunkConcurrency:  flChunkConcurrency,
		ObjectConcurrency: flObjectConcurrency,
		Filter:            buildFilter(),
	}
	cfg.Retry = cmd.RetryConfig{Attempts: flRetries, BaseDelay: time.Duration(flRetryBaseMS) * time.Millisecond}
	if flBandwidth != "" {
		rate, err := cmd.ParseRate(flBandwidth)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return exitUsage
		}
		cfg.BandwidthBPS = rate
	}
	engine := cmd.NewSyncEngine(src, dst, cfg)
	if err := engine.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "sync 失败: %v\n", err)
		if cmd.IsJSON() {
			cmd.EmitJSON(map[string]interface{}{"command": "sync", "ok": false, "error": err.Error()})
		}
		return exitFail
	}
	if cmd.IsJSON() {
		cmd.EmitJSON(map[string]interface{}{"command": "sync", "ok": true, "src": pos[0], "dst": pos[1]})
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

func registerPresignFlags(fs *flag.FlagSet) {
	bindCreds(fs)
	fs.StringVar(&flPresignMethod, "method", "GET", "GET | PUT")
	fs.IntVar(&flPresignExpires, "expires", 3600, "有效期，秒")
}

func runPresign(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("presign", flag.ContinueOnError)
	registerPresignFlags(fs)
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
		if cmd.IsJSON() {
			cmd.EmitJSON(map[string]interface{}{"command": "presign", "ok": false, "error": err.Error()})
		}
		return exitFail
	}
	if cmd.IsJSON() {
		cmd.EmitJSON(map[string]interface{}{
			"command": "presign",
			"ok":      true,
			"target":  target.Raw,
			"method":  flPresignMethod,
			"expires": flPresignExpires,
			"url":     u,
		})
	} else {
		fmt.Println(u)
	}
	return exitOK
}

// ============================================================
// helpers
// ============================================================

// runMove mv 命令 —— 复用 cp 完成后在源端删除
func registerMvFlags(fs *flag.FlagSet) {
	bindCreds(fs)
	bindSrcDstCreds(fs)
	bindRF(fs)
	bindFilter(fs)
	bindPutOpts(fs)
	fs.IntVar(&flChunkMB, "chunk", 0, "分块大小 MB，0=自适应")
	fs.IntVar(&flChunkConcurrency, "concurrency", 5, "单文件分块并发数")
	fs.IntVar(&flObjectConcurrency, "obj-concurrency", 3, "多文件并发数")
	fs.BoolVar(&flDryRun, "dry-run", false, "仅打印将要执行的动作，不真正拷贝/删除")
	fs.BoolVar(&flForceClientCopy, "force-client-copy", false, "同 provider 跨账号拷贝时强制走本机中转")
	bindReliability(fs)
}

func runMove(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("mv", flag.ContinueOnError)
	registerMvFlags(fs)
	fs.Usage = func() { printMoveUsage() }
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	pos := fs.Args()
	if len(pos) != 2 {
		fmt.Fprintln(os.Stderr, "mv <SRC> <DST>：需要 2 个路径")
		printMoveUsage()
		return exitUsage
	}
	resolveCreds()

	srcIsCloud := isCloudURL(pos[0])
	dstIsCloud := isCloudURL(pos[1])
	if !srcIsCloud && !dstIsCloud {
		fmt.Fprintln(os.Stderr, "mv 不支持本地 → 本地")
		return exitUsage
	}

	// 第一步：复用 runCopy 逻辑
	copyArgs := []string{}
	for _, p := range flExcludes {
		copyArgs = append(copyArgs, "-exclude", p)
	}
	for _, p := range flIncludes {
		copyArgs = append(copyArgs, "-include", p)
	}
	if flRecursive {
		copyArgs = append(copyArgs, "-r")
	}
	if flForce {
		copyArgs = append(copyArgs, "-f")
	}
	copyArgs = append(copyArgs, "-chunk", fmt.Sprintf("%d", flChunkMB))
	copyArgs = append(copyArgs, "-concurrency", fmt.Sprintf("%d", flChunkConcurrency))
	copyArgs = append(copyArgs, "-obj-concurrency", fmt.Sprintf("%d", flObjectConcurrency))
	if flContentType != "" {
		copyArgs = append(copyArgs, "-content-type", flContentType)
	}
	if flCacheControl != "" {
		copyArgs = append(copyArgs, "-cache-control", flCacheControl)
	}
	if flStorageClass != "" {
		copyArgs = append(copyArgs, "-storage-class", flStorageClass)
	}
	if flACL != "" {
		copyArgs = append(copyArgs, "-acl", flACL)
	}
	for _, kv := range flTag {
		copyArgs = append(copyArgs, "-tag", kv)
	}
	if flSSE != "" {
		copyArgs = append(copyArgs, "-sse", flSSE)
	}
	if flSSEKMSKey != "" {
		copyArgs = append(copyArgs, "-sse-kms-key", flSSEKMSKey)
	}
	for _, kv := range flMetadata {
		copyArgs = append(copyArgs, "-metadata", kv)
	}
	if flS3AK != "" {
		copyArgs = append(copyArgs, "-s3-ak", flS3AK, "-s3-sk", flS3SK)
	}
	if flCOSID != "" {
		copyArgs = append(copyArgs, "-cos-id", flCOSID, "-cos-sk", flCOSSK)
	}
	if flS3Endpoint != "" {
		copyArgs = append(copyArgs, "-s3-endpoint", flS3Endpoint)
	}
	if flAWSProfile != "" {
		copyArgs = append(copyArgs, "-aws-profile", flAWSProfile)
	}
	if flForceClientCopy {
		copyArgs = append(copyArgs, "-force-client-copy")
	}
	// 透传 src/dst 分凭证
	for _, kv := range []struct{ flag, val string }{
		{"-src-s3-ak", flSrcS3AK}, {"-src-s3-sk", flSrcS3SK},
		{"-src-s3-endpoint", flSrcS3Endpoint}, {"-src-aws-profile", flSrcAWSProfile},
		{"-src-region", flSrcRegion},
		{"-src-cos-id", flSrcCOSID}, {"-src-cos-sk", flSrcCOSSK},
		{"-dst-s3-ak", flDstS3AK}, {"-dst-s3-sk", flDstS3SK},
		{"-dst-s3-endpoint", flDstS3Endpoint}, {"-dst-aws-profile", flDstAWSProfile},
		{"-dst-region", flDstRegion},
		{"-dst-cos-id", flDstCOSID}, {"-dst-cos-sk", flDstCOSSK},
	} {
		if kv.val != "" {
			copyArgs = append(copyArgs, kv.flag, kv.val)
		}
	}
	copyArgs = append(copyArgs, pos...) // 位置参数放后面

	cmd.LogProgress("[mv] 第一步: 复制 %s → %s", pos[0], pos[1])
	if rc := runCopy(ctx, copyArgs); rc != exitOK {
		fmt.Fprintln(os.Stderr, "mv 失败：复制阶段出错，未删除源端")
		return rc
	}

	// 第二步：仅当源是云时调用 rm
	if !srcIsCloud {
		// 本地 → 云：手动删除本地文件
		cmd.LogProgress("[mv] 第二步: 删除本地源 %s", pos[0])
		if err := os.RemoveAll(pos[0]); err != nil {
			fmt.Fprintf(os.Stderr, "删除本地源失败: %v\n", err)
			return exitFail
		}
		return exitOK
	}

	rmArgs := []string{}
	for _, p := range flExcludes {
		rmArgs = append(rmArgs, "-exclude", p)
	}
	for _, p := range flIncludes {
		rmArgs = append(rmArgs, "-include", p)
	}
	if flRecursive {
		rmArgs = append(rmArgs, "-r")
	}
	rmArgs = append(rmArgs, "-f") // mv 二阶段不再交互确认
	if flCOSID != "" {
		rmArgs = append(rmArgs, "-cos-id", flCOSID, "-cos-sk", flCOSSK)
	}
	if flS3AK != "" {
		rmArgs = append(rmArgs, "-s3-ak", flS3AK, "-s3-sk", flS3SK)
	}
	if flS3Endpoint != "" {
		rmArgs = append(rmArgs, "-s3-endpoint", flS3Endpoint)
	}
	if flAWSProfile != "" {
		rmArgs = append(rmArgs, "-aws-profile", flAWSProfile)
	}
	// rm 只作用于 src 侧：优先使用 src 专用凭证（能覆盖通用 flag）
	if flSrcS3AK != "" {
		rmArgs = append(rmArgs, "-s3-ak", flSrcS3AK, "-s3-sk", flSrcS3SK)
	}
	if flSrcS3Endpoint != "" {
		rmArgs = append(rmArgs, "-s3-endpoint", flSrcS3Endpoint)
	}
	if flSrcAWSProfile != "" {
		rmArgs = append(rmArgs, "-aws-profile", flSrcAWSProfile)
	}
	if flSrcCOSID != "" {
		rmArgs = append(rmArgs, "-cos-id", flSrcCOSID, "-cos-sk", flSrcCOSSK)
	}
	rmArgs = append(rmArgs, pos[0])

	cmd.LogProgress("[mv] 第二步: 删除云端源 %s", pos[0])
	if rc := runRemove(ctx, rmArgs); rc != exitOK {
		fmt.Fprintln(os.Stderr, "mv 完成复制但删除阶段出错，请手动清理源端")
		return rc
	}
	return exitOK
}

// ============================================================
// resume <list|abort> [args...]
// ============================================================

func runResume(ctx context.Context, args []string) int {
	if len(args) < 1 {
		printResumeUsage()
		return exitUsage
	}
	switch args[0] {
	case "list", "ls":
		return runResumeList()
	case "abort":
		return runResumeAbort(ctx, args[1:])
	case "-h", "--help", "help":
		printResumeUsage()
		return exitOK
	default:
		fmt.Fprintf(os.Stderr, "resume 未知子命令: %s\n", args[0])
		printResumeUsage()
		return exitUsage
	}
}

func runResumeList() int {
	states := cmd.ListResumeStates()
	if cmd.IsJSON() {
		cmd.EmitJSON(map[string]interface{}{"resume_states": states, "count": len(states)})
		return exitOK
	}
	if len(states) == 0 {
		fmt.Println("无残留的断点状态")
		return exitOK
	}
	fmt.Printf("%-9s  %-9s  %-30s  %12s  %-20s  %s\n",
		"KIND", "PROVIDER", "BUCKET/KEY", "SIZE", "UPDATED", "UPLOAD-ID-OR-LOCAL")
	for _, s := range states {
		ident := s.UploadID
		if s.ResumeKind() == "download" {
			ident = s.LocalPath
		}
		fmt.Printf("%-9s  %-9s  %-30s  %12d  %-20s  %s\n",
			s.ResumeKind(),
			s.Provider,
			s.Bucket+"/"+s.Key,
			s.TotalSize,
			s.UpdatedAt.Format("2006-01-02 15:04:05"),
			ident,
		)
	}
	return exitOK
}

// resume abort 状态
var (
	flResumeAll      bool
	flResumeAllCloud bool
	flResumeRegion   string
	flResumeURL      string
)

func registerResumeAbortFlags(fs *flag.FlagSet) {
	bindCreds(fs)
	fs.BoolVar(&flResumeAll, "all", false, "丢弃全部本地残留状态")
	fs.BoolVar(&flResumeAllCloud, "all-cloud", false, "扫描云端未完成的 multipart uploads 并批量 abort（需同时提供 -url）")
	fs.StringVar(&flResumeURL, "url", "", "-all-cloud 模式下的扶包 URL，如 cos://my-bucket.ap-beijing/ 或带前缀 cos://b.r/data/")
	fs.StringVar(&flResumeRegion, "region", "", "为丢弃操作指定存储桶的 region（状态中未保存）")
	fs.BoolVar(&flForce, "f", false, "-all-cloud 模式下跳过交互确认")
	fs.BoolVar(&flDryRun, "dry-run", false, "-all-cloud 模式下仅打印列表，不真正 abort")
}

func runResumeAbort(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("resume abort", flag.ContinueOnError)
	registerResumeAbortFlags(fs)
	flResumeAll = false
	flResumeAllCloud = false
	flResumeRegion = ""
	flResumeURL = ""
	flForce = false
	flDryRun = false
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	// 分支 1：-all-cloud 扫描云端孤儿
	if flResumeAllCloud {
		return runResumeAbortAllCloud(ctx)
	}

	all := flResumeAll
	region := flResumeRegion
	resolveCreds()

	targets := cmd.ListResumeStates()
	if !all {
		if fs.NArg() != 1 {
			fmt.Fprintln(os.Stderr, "resume abort <UPLOAD-ID|local-path> | -all")
			return exitUsage
		}
		id := fs.Arg(0)
		filtered := targets[:0]
		for _, s := range targets {
			if s.UploadID == id || (s.UploadID != "" && strings.HasPrefix(s.UploadID, id)) {
				filtered = append(filtered, s)
				continue
			}
			// download 状态按 LocalPath 匹配
			if s.ResumeKind() == "download" && s.LocalPath != "" &&
				(s.LocalPath == id || strings.HasSuffix(s.LocalPath, id)) {
				filtered = append(filtered, s)
			}
		}
		targets = filtered
		if len(targets) == 0 {
			fmt.Fprintf(os.Stderr, "未找到状态 UPLOAD-ID|LOCAL=%s\n", id)
			return exitFail
		}
	}

	if len(targets) == 0 {
		fmt.Println("无可丢弃的状态")
		return exitOK
	}

	abortedOK := 0
	abortedFail := 0
	for _, s := range targets {
		// download 任务：只需删 .part 与 state，云端无需清理
		if s.ResumeKind() == "download" {
			if s.LocalPath != "" {
				_ = os.Remove(s.LocalPath + ".part")
			}
			cmd.DeleteResumeStateByPath(s.StatePath)
			fmt.Printf("  [✓] download %s/%s → %s\n", s.Bucket, s.Key, s.LocalPath)
			abortedOK++
			continue
		}

		provider := objstore.ProviderType(strings.ToLower(s.Provider))
		if provider == "" {
			provider = objstore.ProviderCOS
		}
		store, err := buildStorage(provider, s.Bucket, region)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  [✗] %s/%s: %v\n", s.Bucket, s.Key, err)
			abortedFail++
			continue
		}
		resumer, ok := store.(objstore.MultipartResumer)
		if !ok {
			fmt.Fprintf(os.Stderr, "  [✗] %s/%s: store 不支持 MultipartResumer\n", s.Bucket, s.Key)
			abortedFail++
			continue
		}
		if err := resumer.AbortMultipart(ctx, s.Key, s.UploadID); err != nil {
			fmt.Fprintf(os.Stderr, "  [✗] %s/%s: %v\n", s.Bucket, s.Key, err)
			abortedFail++
			continue
		}
		// 同步删除本地状态文件
		cmd.DeleteResumeStateByPath(s.StatePath)
		fmt.Printf("  [✓] upload %s/%s\n", s.Bucket, s.Key)
		abortedOK++
	}
	fmt.Printf("丢弃完成：成功 %d / 失败 %d\n", abortedOK, abortedFail)
	if abortedFail > 0 {
		return exitFail
	}
	return exitOK
}

// runResumeAbortAllCloud 扫描云端未完成的 multipart uploads 并批量 abort。
func runResumeAbortAllCloud(ctx context.Context) int {
	if flResumeURL == "" {
		fmt.Fprintln(os.Stderr, "-all-cloud 需与 -url cos://b.r/[前缀] 或 -url s3://b.r/[前缀] 一同使用")
		return exitUsage
	}
	resolveCreds()

	target, err := cmd.ParseObjectString(flResumeURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "解析 -url 失败: %v\n", err)
		return exitUsage
	}
	store, err := buildStorage(target.StorageType, target.Bucket, target.Region)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFail
	}
	lister, ok := store.(objstore.MultipartLister)
	if !ok {
		fmt.Fprintln(os.Stderr, "store 不支持 ListMultipartUploads")
		return exitFail
	}
	resumer, ok := store.(objstore.MultipartResumer)
	if !ok {
		fmt.Fprintln(os.Stderr, "store 不支持 AbortMultipart")
		return exitFail
	}

	prefix := target.Prefix
	if !target.IsPrefix {
		prefix = target.Key // 允许传完整前缀不以 / 结尾
	}

	fmt.Printf("🔍 扫描 %s://%s/%s ...\n", target.StorageType, target.Bucket, prefix)
	uploads, err := lister.ListIncompleteUploads(ctx, prefix)
	if err != nil {
		fmt.Fprintf(os.Stderr, "列举失败: %v\n", err)
		return exitFail
	}
	if len(uploads) == 0 {
		fmt.Println("✨ 云端无未完成的 multipart uploads")
		return exitOK
	}

	fmt.Printf("\n发现 %d 个未完成上传：\n", len(uploads))
	for _, u := range uploads {
		ts := ""
		if !u.Initiated.IsZero() {
			ts = u.Initiated.Local().Format("2006-01-02 15:04:05")
		}
		fmt.Printf("  - %s  uploadID=%s  initiated=%s\n", u.Key, u.UploadID, ts)
	}

	if flDryRun {
		fmt.Println("\n[dry-run] 仅列举，未执行 abort。去掉 -dry-run 并加 -f 可真执行。")
		return exitOK
	}

	if !flForce {
		fmt.Printf("\n⚠️  即将 abort 上述 %d 个上传（包括可能正在进行中的任务）。输入 yes 继续：", len(uploads))
		var reply string
		_, _ = fmt.Scanln(&reply)
		if strings.ToLower(strings.TrimSpace(reply)) != "yes" {
			fmt.Println("已取消")
			return exitOK
		}
	}

	ok2, fail2 := 0, 0
	for _, u := range uploads {
		if err := resumer.AbortMultipart(ctx, u.Key, u.UploadID); err != nil {
			fmt.Fprintf(os.Stderr, "  [✗] %s (%s): %v\n", u.Key, u.UploadID, err)
			fail2++
			continue
		}
		fmt.Printf("  [✓] %s (%s)\n", u.Key, u.UploadID)
		ok2++
	}
	fmt.Printf("\n云端清理完成：成功 %d / 失败 %d\n", ok2, fail2)
	if fail2 > 0 {
		return exitFail
	}
	return exitOK
}

func printResumeUsage() {
	fmt.Print(`objcli resume - 断点上传状态管理

用法:
  objcli resume list                       列出所有残留状态
  objcli resume abort <UPLOAD-ID> [-region R]
  objcli resume abort -all [-region R]     丢弃全部

状态文件位置: ~/.objcli/resume/<sha1>.json
丢弃后会调用 cos/s3 AbortMultipartUpload 清理云端残留。
`)
}

// ============================================================
// helpers
// ============================================================

// 全局 filter（exclude/include）
var (
	flExcludes cmd.StringSliceFlag
	flIncludes cmd.StringSliceFlag
)


func bindCreds(fs *flag.FlagSet) {
	fs.StringVar(&flS3AK, "s3-ak", "", "AWS Access Key ID（缺省读 AWS_ACCESS_KEY_ID）")
	fs.StringVar(&flS3SK, "s3-sk", "", "AWS Secret Access Key（缺省读 AWS_SECRET_ACCESS_KEY）")
	fs.StringVar(&flS3Endpoint, "s3-endpoint", "", "S3 兼容 endpoint（如 minio: http://127.0.0.1:9000）")
	fs.StringVar(&flAWSProfile, "aws-profile", "", "AWS profile 名（~/.aws/credentials）。AK/SK 未依时生效；不依 AK/SK 也不依 profile 时走 default credential chain (env/profile/IMDS/STS)")
	fs.StringVar(&flCOSID, "cos-id", "", "腾讯云 SecretId（缺省读 TENCENT_SECRET_ID）")
	fs.StringVar(&flCOSSK, "cos-sk", "", "腾讯云 SecretKey（缺省读 TENCENT_SECRET_KEY）")
}

// bindSrcDstCreds 绑定跨账号拷贝专用凭证（仅 cp）。
// 与 bindCreds 叠加：同一边同时存在 src/dst 专用凭证与通用凭证时，专用优先。
func bindSrcDstCreds(fs *flag.FlagSet) {
	fs.StringVar(&flSrcS3AK, "src-s3-ak", "", "源端 S3 AK（优先于 -s3-ak，issue #13）")
	fs.StringVar(&flSrcS3SK, "src-s3-sk", "", "源端 S3 SK")
	fs.StringVar(&flSrcS3Endpoint, "src-s3-endpoint", "", "源端 S3 endpoint")
	fs.StringVar(&flSrcAWSProfile, "src-aws-profile", "", "源端 AWS profile")
	fs.StringVar(&flSrcRegion, "src-region", "", "源端 region（覆盖 URL 解析出的 region）")

	fs.StringVar(&flDstS3AK, "dst-s3-ak", "", "目标端 S3 AK")
	fs.StringVar(&flDstS3SK, "dst-s3-sk", "", "目标端 S3 SK")
	fs.StringVar(&flDstS3Endpoint, "dst-s3-endpoint", "", "目标端 S3 endpoint")
	fs.StringVar(&flDstAWSProfile, "dst-aws-profile", "", "目标端 AWS profile")
	fs.StringVar(&flDstRegion, "dst-region", "", "目标端 region")

	fs.StringVar(&flSrcCOSID, "src-cos-id", "", "源端 COS SecretId")
	fs.StringVar(&flSrcCOSSK, "src-cos-sk", "", "源端 COS SecretKey")
	fs.StringVar(&flDstCOSID, "dst-cos-id", "", "目标端 COS SecretId")
	fs.StringVar(&flDstCOSSK, "dst-cos-sk", "", "目标端 COS SecretKey")
}

func bindFilter(fs *flag.FlagSet) {
	flExcludes = cmd.StringSliceFlag{}
	flIncludes = cmd.StringSliceFlag{}
	fs.Var(&flExcludes, "exclude", "排除 glob 模式（可多次，顺序应用）")
	fs.Var(&flIncludes, "include", "重新包含 glob 模式（可多次，顺序应用）")
}

// buildFilterFromArgs 按出现顺序重建 filter。
// 由于 flag.Var 不保存出现次序，这里可能丢失交错顺序。
// 实际使用中最常见的是先一批 -exclude 后一批 -include，按这个假设顺序重建。
// 要 100% 对齐 aws 顺序必须自己扫 os.Args，下面 advancedFilterFromArgs 走该路径。
func buildFilter() *cmd.MatchFilter {
	f := cmd.NewMatchFilter()
	for _, p := range flExcludes {
		f.AddExclude(p)
	}
	for _, p := range flIncludes {
		f.AddInclude(p)
	}
	return f
}

// extractOutputFlag 预扫 -o / --output / -output，设置输出模式
func extractOutputFlag(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "-o" || a == "-output" || a == "--output" {
			if i+1 < len(args) {
				switch strings.ToLower(args[i+1]) {
				case "json":
					cmd.SetOutput(cmd.OutputJSON)
				case "text":
					cmd.SetOutput(cmd.OutputText)
				}
				i++
			}
			continue
		}
		if strings.HasPrefix(a, "-o=") || strings.HasPrefix(a, "--output=") || strings.HasPrefix(a, "-output=") {
			parts := strings.SplitN(a, "=", 2)
			switch strings.ToLower(parts[1]) {
			case "json":
				cmd.SetOutput(cmd.OutputJSON)
			case "text":
				cmd.SetOutput(cmd.OutputText)
			}
			continue
		}
		out = append(out, a)
	}
	return out
}

func bindRF(fs *flag.FlagSet) {
	fs.BoolVar(&flRecursive, "r", false, "递归（前缀/glob 模式）")
	fs.BoolVar(&flForce, "f", false, "前缀/glob 模式：跳过用户确认")
}

// bindReliability 绑定重试/限速相关 flag（cp/mv/sync 共用）
func bindReliability(fs *flag.FlagSet) {
	fs.IntVar(&flRetries, "retries", 3, "遇可重试错误时的最大重试次数（1 表示不重试）")
	fs.IntVar(&flRetryBaseMS, "retry-base-ms", 200, "指数退避基础间隔（毫秒），退避间隔=base*2^(n-1)，上限 base*32")
	fs.StringVar(&flBandwidth, "bandwidth", "", "传输限速，例：10MB/s、100KiB/s、1Gbps；空/0=不限速")
}

// applyReliability 把 flag 变量填进 cfg，bandwidth 解析出错返回可报告的 error。
func applyReliability(cfg *cmd.CopyConfig) error {
	cfg.Retry = cmd.RetryConfig{Attempts: flRetries, BaseDelay: time.Duration(flRetryBaseMS) * time.Millisecond}
	if flBandwidth == "" {
		return nil
	}
	rate, err := cmd.ParseRate(flBandwidth)
	if err != nil {
		return err
	}
	cfg.BandwidthBPS = rate
	return nil
}

// bindPutOpts 绑定对象属性相关的 flag（cp / mv / sync 共用）
func bindPutOpts(fs *flag.FlagSet) {
	fs.StringVar(&flContentType, "content-type", "", "对象 Content-Type（空=云端自动推断）")
	fs.StringVar(&flCacheControl, "cache-control", "", "对象 Cache-Control")
	flMetadata = cmd.StringSliceFlag{}
	fs.Var(&flMetadata, "metadata", "用户元数据 key=value，可重复")
	fs.StringVar(&flStorageClass, "storage-class", "", "存储类型：S3 与 COS 枚举不同，详见 README")
	fs.StringVar(&flACL, "acl", "", "canned ACL，S3: private|public-read|... ; COS: private|public-read|public-read-write|default")
	flTag = cmd.StringSliceFlag{}
	fs.Var(&flTag, "tag", "对象 Tag key=value，可重复")
	flSSE = ""
	flSSEKMSKey = ""
	fs.StringVar(&flSSE, "sse", "", "服务端加密：AES256 (S3:SSE-S3/COS:SSE-COS) | aws:kms (S3) | cos/kms (COS)")
	fs.StringVar(&flSSEKMSKey, "sse-kms-key", "", "KMS CMK ID/ARN/Alias，仅在 -sse=aws:kms 或 -sse=cos/kms 时生效；为空走账号 default key")
}

// validStorageClassesS3 / validStorageClassesCOS 按 provider 分别定义合法枚举。
// 来源：aws-sdk-go-v2 types.StorageClass 与腾讯云 COS 存储类型概述 官方文档。
var validStorageClassesS3 = map[string]struct{}{
	"STANDARD":            {},
	"STANDARD_IA":         {},
	"ONEZONE_IA":          {},
	"INTELLIGENT_TIERING": {},
	"GLACIER":             {},
	"GLACIER_IR":          {},
	"DEEP_ARCHIVE":        {},
	"REDUCED_REDUNDANCY":  {},
	"OUTPOSTS":            {},
	"SNOW":                {},
	"EXPRESS_ONEZONE":     {},
	"FSX_ONTAP":           {},
	"FSX_OPENZFS":         {},
}

var validStorageClassesCOS = map[string]struct{}{
	"STANDARD":                {},
	"STANDARD_IA":             {},
	"INTELLIGENT_TIERING":     {},
	"ARCHIVE":                 {},
	"DEEP_ARCHIVE":            {},
	"MAZ_STANDARD":            {},
	"MAZ_STANDARD_IA":         {},
	"MAZ_INTELLIGENT_TIERING": {},
	"MAZ_ARCHIVE":             {},
}

// validStorageClassDesc 返回某 provider 可选值的人读描述。
func validStorageClassDesc(provider string) string {
	switch provider {
	case "s3":
		return "S3 可选：STANDARD | STANDARD_IA | ONEZONE_IA | INTELLIGENT_TIERING | GLACIER | GLACIER_IR | DEEP_ARCHIVE | REDUCED_REDUNDANCY | EXPRESS_ONEZONE | OUTPOSTS | SNOW | FSX_ONTAP | FSX_OPENZFS"
	case "cos":
		return "COS 可选：STANDARD | STANDARD_IA | INTELLIGENT_TIERING | ARCHIVE | DEEP_ARCHIVE | MAZ_STANDARD | MAZ_STANDARD_IA | MAZ_INTELLIGENT_TIERING | MAZ_ARCHIVE"
	default:
		return "unknown provider " + provider
	}
}

// normalizeStorageClass 输入不区分大小写；输出全大写。不合法返回错误。
// provider: "s3" | "cos"
func normalizeStorageClass(s, provider string) (string, error) {
	if s == "" {
		return "", nil
	}
	up := strings.ToUpper(strings.TrimSpace(s))
	var valid map[string]struct{}
	switch provider {
	case "s3":
		valid = validStorageClassesS3
	case "cos":
		valid = validStorageClassesCOS
	default:
		return "", fmt.Errorf("unknown provider %q", provider)
	}
	if _, ok := valid[up]; !ok {
		return "", fmt.Errorf("-storage-class %q 在 %s 不合法。%s", s, strings.ToUpper(provider), validStorageClassDesc(provider))
	}
	return up, nil
}

// validACLs S3 与 COS 各自支持的对象 canned ACL。
// COS 额外提供 "default" 表示继承桶默认 ACL；S3 无该值。
// COS 不支持 authenticated-read / aws-exec-read / bucket-owner-* 系列。
var validACLsS3 = map[string]struct{}{
	"private":                   {},
	"public-read":               {},
	"public-read-write":         {},
	"authenticated-read":        {},
	"aws-exec-read":             {},
	"bucket-owner-read":         {},
	"bucket-owner-full-control": {},
}

var validACLsCOS = map[string]struct{}{
	"default":            {},
	"private":            {},
	"public-read":        {},
	"public-read-write":  {},
}

func validACLDesc(provider string) string {
	switch provider {
	case "s3":
		return "S3 可选：private | public-read | public-read-write | authenticated-read | aws-exec-read | bucket-owner-read | bucket-owner-full-control"
	case "cos":
		return "COS 可选：default | private | public-read | public-read-write"
	default:
		return "unknown provider " + provider
	}
}

// normalizeACL ACL 不区分大小写；输出全小写（S3/COS canned ACL 均为小写连字符）。
func normalizeACL(s, provider string) (string, error) {
	if s == "" {
		return "", nil
	}
	low := strings.ToLower(strings.TrimSpace(s))
	var valid map[string]struct{}
	switch provider {
	case "s3":
		valid = validACLsS3
	case "cos":
		valid = validACLsCOS
	default:
		return "", fmt.Errorf("unknown provider %q", provider)
	}
	if _, ok := valid[low]; !ok {
		return "", fmt.Errorf("-acl %q 在 %s 不合法。%s", s, strings.ToUpper(provider), validACLDesc(provider))
	}
	return low, nil
}

// parseTags 解析 -tag k=v 列表，返回 map。不合法表达式返回错误。
func parseTags(items []string) (map[string]string, error) {
	if len(items) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(items))
	for _, kv := range items {
		eq := strings.Index(kv, "=")
		if eq <= 0 {
			return nil, fmt.Errorf("-tag %q 格式不合法，需为 key=value", kv)
		}
		k := kv[:eq]
		v := kv[eq+1:]
		if k == "" {
			return nil, fmt.Errorf("-tag %q key 为空", kv)
		}
		out[k] = v
	}
	return out, nil
}

// buildPutOptions 组装 PutOptions。根据目标 provider 校验与规范化 storage-class / ACL / SSE。
func buildPutOptions(provider string) (*objstore.PutOptions, error) {
	if flContentType == "" && flCacheControl == "" && flStorageClass == "" &&
		flACL == "" && len(flMetadata) == 0 && len(flTag) == 0 &&
		flSSE == "" && flSSEKMSKey == "" {
		return nil, nil
	}
	sc, err := normalizeStorageClass(flStorageClass, provider)
	if err != nil {
		return nil, err
	}
	acl, err := normalizeACL(flACL, provider)
	if err != nil {
		return nil, err
	}
	tags, err := parseTags(flTag)
	if err != nil {
		return nil, err
	}
	sse, kmsKey, err := normalizeSSE(flSSE, flSSEKMSKey, provider)
	if err != nil {
		return nil, err
	}
	opts := &objstore.PutOptions{
		ContentType:  flContentType,
		CacheControl: flCacheControl,
		StorageClass: sc,
		ACL:          acl,
		Tags:         tags,
		SSE:          sse,
		SSEKMSKeyID:  kmsKey,
	}
	if len(flMetadata) > 0 {
		opts.Metadata = make(map[string]string, len(flMetadata))
		for _, kv := range flMetadata {
			eq := strings.Index(kv, "=")
			if eq <= 0 {
				continue // 跳过不合法项
			}
			opts.Metadata[kv[:eq]] = kv[eq+1:]
		}
	}
	return opts, nil
}


// normalizeSSE 校验与规范化 -sse 与 -sse-kms-key（按 provider）。
// 返回应会写入 PutOptions 的 SSE 与 SSEKMSKeyID（可为空字串）。
//
// S3:
//   sse="" / "AES256" / "aws:kms" / "aws:kms:dsse"
//   sse=cos/kms 报错
// COS:
//   sse="" / "AES256" / "cos/kms"
//   sse=aws:kms 报错
// kmsKey 仅在 sse 含 kms 时才需要；不包含时传了报错。
func normalizeSSE(sseRaw, kmsKey, provider string) (string, string, error) {
	sse := strings.TrimSpace(sseRaw)
	if sse == "" {
		if kmsKey != "" {
			return "", "", fmt.Errorf("-sse-kms-key 需与 -sse=aws:kms 或 -sse=cos/kms 同时使用")
		}
		return "", "", nil
	}
	// 只对 AES256 做大小写容错；kms 依 S3/COS 官方大小写
	if strings.EqualFold(sse, "AES256") {
		sse = "AES256"
	}
	isKMS := false
	switch provider {
	case "s3":
		switch sse {
		case "AES256":
			// SSE-S3
		case "aws:kms", "aws:kms:dsse":
			isKMS = true
		default:
			return "", "", fmt.Errorf("-sse %q 在 S3 不支持；可选 AES256 / aws:kms / aws:kms:dsse", sseRaw)
		}
	case "cos":
		switch sse {
		case "AES256":
			// SSE-COS
		case "cos/kms":
			isKMS = true
		default:
			return "", "", fmt.Errorf("-sse %q 在 COS 不支持；可选 AES256 / cos/kms", sseRaw)
		}
	default:
		return "", "", fmt.Errorf("unknown provider %q", provider)
	}
	if !isKMS && kmsKey != "" {
		return "", "", fmt.Errorf("-sse-kms-key 仅在 -sse=*kms* 时生效，当前 -sse=%q", sseRaw)
	}
	return sse, strings.TrimSpace(kmsKey), nil
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
	flAWSProfile = envOr(flAWSProfile, "AWS_PROFILE")
	flCOSID = envOr(flCOSID, "TENCENT_SECRET_ID")
	flCOSSK = envOr(flCOSSK, "TENCENT_SECRET_KEY")
}

// credSide 返回某一侧（src/dst）的有效凭证：优先使用该侧专用 flag，未设时回退到通用 flag。
// side 为 "src" / "dst" / ""（空表示非 cp 场景，直接走通用 flag）。
func credSide(side string) (s3AK, s3SK, s3Endpoint, awsProfile, cosID, cosSK string) {
	s3AK, s3SK, s3Endpoint, awsProfile = flS3AK, flS3SK, flS3Endpoint, flAWSProfile
	cosID, cosSK = flCOSID, flCOSSK
	switch side {
	case "src":
		if flSrcS3AK != "" || flSrcS3SK != "" {
			s3AK, s3SK = flSrcS3AK, flSrcS3SK
		}
		if flSrcS3Endpoint != "" {
			s3Endpoint = flSrcS3Endpoint
		}
		if flSrcAWSProfile != "" {
			awsProfile = flSrcAWSProfile
		}
		if flSrcCOSID != "" || flSrcCOSSK != "" {
			cosID, cosSK = flSrcCOSID, flSrcCOSSK
		}
	case "dst":
		if flDstS3AK != "" || flDstS3SK != "" {
			s3AK, s3SK = flDstS3AK, flDstS3SK
		}
		if flDstS3Endpoint != "" {
			s3Endpoint = flDstS3Endpoint
		}
		if flDstAWSProfile != "" {
			awsProfile = flDstAWSProfile
		}
		if flDstCOSID != "" || flDstCOSSK != "" {
			cosID, cosSK = flDstCOSID, flDstCOSSK
		}
	}
	return
}

func buildStorage(provider objstore.ProviderType, bucket, region string) (objstore.Store, error) {
	return buildStorageSide(provider, bucket, region, "")
}

// buildStorageSide 按 side（src/dst/""）选凭证。side="" 表示非 cp 场景，只走通用 flag。
func buildStorageSide(provider objstore.ProviderType, bucket, region, side string) (objstore.Store, error) {
	s3AK, s3SK, s3Endpoint, awsProfile, cosID, cosSK := credSide(side)

	switch provider {
	case objstore.ProviderCOS:
		if cosID == "" {
			return nil, errors.New("缺少 COS 凭证：-cos-id 或 TENCENT_SECRET_ID" + sideHint(side, "cos"))
		}
		if cosSK == "" {
			return nil, errors.New("缺少 COS 凭证：-cos-sk 或 TENCENT_SECRET_KEY" + sideHint(side, "cos"))
		}
		return objstore.New(objstore.Config{
			Provider: objstore.ProviderCOS, Bucket: bucket, Region: region,
			SecretID: cosID, SecretKey: cosSK,
		})
	case objstore.ProviderS3:
		// S3 凭证解析优先级：
		//   1) 显式 AK/SK（-s3-ak/-s3-sk 或 -src-/-dst- 覆盖） → 静态
		//   2) AK/SK 未依，指定 -aws-profile / AWS_PROFILE → profile
		//   3) 都不依 → awssdk default chain (env/profile/IMDS/STS)
		if (s3AK == "" && s3SK != "") || (s3AK != "" && s3SK == "") {
			return nil, errors.New("S3 凭证不完整：AK/SK 必须同时提供（或同时为空走 default chain）" + sideHint(side, "s3"))
		}
		return objstore.New(objstore.Config{
			Provider: objstore.ProviderS3, Bucket: bucket, Region: region,
			SecretID: s3AK, SecretKey: s3SK,
			Endpoint: envOr(s3Endpoint, "AWS_ENDPOINT_URL"),
			Profile:  awsProfile,
		})
	}
	return nil, fmt.Errorf("不支持的存储类型: %s", provider)
}

// sideHint 生成报错补充。例： (src) 或  (dst)。
func sideHint(side, _ string) string {
	if side == "" {
		return ""
	}
	return "（" + side + " 侧）"
}

// hasDifferentSideCreds 检测 src/dst 专用凭证是否造成两侧走不同账号。
// 任一边设了专用 AK/SK、或 endpoint、或 profile 不一致，则认为需要本机中转。
// 这里采取保守策略：只要“两侧专用凭证出现不同”就返 true，
// 不严格区分 provider（COS 则只看 cos-id，S3 只看 s3-ak）以免误判。
func hasDifferentSideCreds() bool {
	// S3 两侧凭证不同
	if (flSrcS3AK != "" || flDstS3AK != "") && flSrcS3AK != flDstS3AK {
		return true
	}
	// COS 两侧凭证不同
	if (flSrcCOSID != "" || flDstCOSID != "") && flSrcCOSID != flDstCOSID {
		return true
	}
	if (flSrcS3Endpoint != "" || flDstS3Endpoint != "") && flSrcS3Endpoint != flDstS3Endpoint {
		return true
	}
	if (flSrcAWSProfile != "" || flDstAWSProfile != "") && flSrcAWSProfile != flDstAWSProfile {
		return true
	}
	return false
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
//
// 判定某个 flag 是否吃后一个参数：动态从 allFlagsets() 中探查（避免白名单脱骎）。
func splitFlagsAndPositional(args []string) []string {
	boolFlags := collectBoolFlags()

	var flags, positional []string
	i := 0
	for i < len(args) {
		a := args[i]
		// 非 flag → positional
		if !strings.HasPrefix(a, "-") {
			positional = append(positional, a)
			i++
			continue
		}
		// -flag=value 形式自含值
		if strings.Contains(a, "=") {
			flags = append(flags, a)
			i++
			continue
		}
		// 去掉前导 -- / -
		name := strings.TrimLeft(a, "-")
		flags = append(flags, a)
		// bool flag 不吃后一个 token；未知 flag 默认吃（保守可传递给 flag.Parse 报错）
		if !boolFlags[name] && i+1 < len(args) {
			flags = append(flags, args[i+1])
			i += 2
			continue
		}
		i++
	}
	return append(flags, positional...)
}

// collectBoolFlags 列出所有子命令 flagset 中的 bool flag 名。
// 调用了所有 register*Flags 函数但不 Parse，只可靠反射拿到名字 + IsBoolFlag。
func collectBoolFlags() map[string]bool {
	boolFlags := map[string]bool{}
	registries := []func(*flag.FlagSet){
		registerCpFlags,
		registerMvFlags,
		registerRmFlags,
		registerLsFlags,
		registerSyncFlags,
		registerPresignFlags,
		registerResumeAbortFlags,
	}
	for _, reg := range registries {
		fs := flag.NewFlagSet("_introspect", flag.ContinueOnError)
		reg(fs)
		fs.VisitAll(func(f *flag.Flag) {
			if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
				boolFlags[f.Name] = true
			}
		})
	}
	return boolFlags
}

// ============================================================
// usage
// ============================================================

func printRootUsage() {
	fmt.Print(`objcli - 对象存储统一 CLI
支持 AWS S3 与腾讯云 COS。

用法:
  objcli cp      <SRC>    <DST>     [选项]   # 拷贝（云↔云、本地↔云）
  objcli mv      <SRC>    <DST>     [选项]   # 移动（= cp + 删源）
  objcli sync    <SRC>    <DST>     [选项]   # 增量同步
  objcli rm      <TARGET>           [选项]   # 删除
  objcli ls      <TARGET>           [选项]   # 列举
  objcli presign <TARGET>           [选项]   # 预签名 URL
  objcli version                              # 输出版本信息

全局选项:
  -o text|json    输出格式，默认 text
  -exclude PAT    排除 glob。可多次，与 -include 按顺序生效
  -include PAT    重新包含 glob。可多次

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
  objcli mv -h
  objcli rm -h
  objcli ls -h
  objcli sync -h
  objcli presign -h
`)
}

func printMoveUsage() {
	fmt.Print(`objcli mv - 移动对象（= cp + 删源）

用法:
  objcli mv <SRC> <DST> [选项]

路径类型组合:
  云 → 云:    mv cos://b.r/k cos://b2.r2/k
  本地 → 云:  mv /local/file.zip cos://b.r/key.zip
  云 → 本地:  mv cos://b.r/key.zip /local/

选项：与 cp 一致，额外支持 -exclude / -include
语义：复制成功后才删源；复制失败不会动源

示例：
  objcli mv cos://src.ap-singapore/data/x.zip cos://dst.ap-beijing/x.zip
  objcli mv /tmp/data.tar.gz cos://b.ap-beijing/backup/
  objcli mv cos://b.ap-beijing/data/ /tmp/data/ -r -f
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
  -retries INT        重试次数（默认 3，1=不重试）
  -retry-base-ms INT  退避基础间隔 ms（默认 200，指数增长封顶 base*32）
  -bandwidth RATE     限速，例：10MB/s、100KiB/s、1Gbps；空/0=不限速
  -s3-ak / -s3-sk / -cos-id / -cos-sk
  -s3-endpoint        S3 兼容 endpoint（minio 等）
  -aws-profile NAME   AWS profile（~/.aws/credentials）。AK/SK 未依时生效；
                      三者都不依 → awssdk default chain (env/profile/IMDS/STS) → 适用 EC2/EKS

跨账号 / 跨 endpoint 拷贝（仅云→云 cp/mv，issue #13）:
  -src-s3-ak / -src-s3-sk / -src-s3-endpoint / -src-aws-profile / -src-region
  -dst-s3-ak / -dst-s3-sk / -dst-s3-endpoint / -dst-aws-profile / -dst-region
  -src-cos-id / -src-cos-sk / -dst-cos-id / -dst-cos-sk
  与通用 -s3-ak/... 叠加：该侧专用 flag 优先，未依时回退到通用 flag。

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

  # 跨账号 S3↔S3（issue #13）
  objcli cp s3://srcbkt.us-east-1/k s3://dstbkt.us-east-1/k \
    -src-s3-ak AKID_A -src-s3-sk SK_A -dst-s3-ak AKID_B -dst-s3-sk SK_B

  # EC2/EKS 上走 IAM Role（不依任何 AK/SK，issue #8）
  objcli cp s3://srcbkt.us-east-1/k cos://dst.ap-beijing/k

  # 使用  ~/.aws/credentials 中的 profile（issue #8）
  objcli cp -aws-profile prod s3://srcbkt.us-east-1/k cos://dst.ap-beijing/k
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