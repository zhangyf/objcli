package cmd

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"objcli/progress"
	"github.com/zhangyf/objstore"
)

const maxMemoryBytes = int64(4 * 1024 * 1024 * 1024)

// CopyConfig 拷贝引擎配置
type CopyConfig struct {
	// 拷贝模式（三选一）
	SrcKey        string // 单文件
	SrcPrefix     string // 前缀批量（支持 * 通配符，解析后提取前缀）
	KeyListSource string // 对象 URL 列表

	DstKey    string // 目标 Key（单文件，默认同源）
	DstPrefix string // 目标前缀（前缀/列表模式）

	ChunkMB           int
	ChunkConcurrency  int
	ObjectConcurrency int

	// prefix 模式特定
	Recursive bool         // 是否递归处理目录下的所有对象
	Force     bool         // 是否强制跳过用户确认
	Filter    *MatchFilter // exclude/include 过滤

	PutOptions *objstore.PutOptions // 跨存储拷贝入盘时可选的对象属性

	DryRun bool // 仅打印计划，不真正拷贝

	// ForceClientCopy 强制走本机中转（跨账号同 provider、跨 endpoint 场景）
	// 为 true 时不走 server-side CopyObject/CopyPartFrom，避免服务端 "拉取" 跨账号源权限不足。
	ForceClientCopy bool

	// Retry 控制重试退避；Attempts<=0 退化为不重试。
	Retry RetryConfig

	// BandwidthBPS 限速（字节/秒），<=0 表示不限速。
	// 当不为零时，会在跨厂商拷贝、上传、下载路径中接入带宽限制。
	BandwidthBPS float64
}

// Creds 通用凭证
type Creds struct {
	AK       string
	SK       string
	Endpoint string // S3 兼容 endpoint（可选）
	Profile  string // AWS profile 名（可选，仅 S3 生效）
}

// Engine 拷贝引擎
type Engine struct {
	src    objstore.Store
	dst    objstore.Store
	cfg    CopyConfig
	creds  map[objstore.ProviderType]*Creds
	global *progress.Tracker // 全局进度，由外部注入，可为 nil

	totalBytes int64      // 总字节数（所有对象累加）
	doneBytes  int64      // 已完成字节数
	byteMu     sync.Mutex

	lim *Limiter // 限速器；nil 或 rate<=0 表示不限速
}

func NewEngine(src, dst objstore.Store, cfg CopyConfig) *Engine {
	e := &Engine{src: src, dst: dst, cfg: cfg, creds: make(map[objstore.ProviderType]*Creds)}
	if cfg.BandwidthBPS > 0 {
		e.lim = NewLimiter(cfg.BandwidthBPS)
	}
	return e
}

// WithGlobalTracker 注入全局进度跟踪器，每个对象操作完成后会累加到全局计数器
func (e *Engine) WithGlobalTracker(t *progress.Tracker) *Engine {
	e.global = t
	return e
}

// SetTotalBytes 设置预期总字节数（即将开始前可调用）
func (e *Engine) SetTotalBytes(n int64) {
	e.byteMu.Lock()
	e.totalBytes = n
	e.byteMu.Unlock()
}

// BytesProgress 返回 (doneBytes, totalBytes)
func (e *Engine) BytesProgress() (int64, int64) {
	e.byteMu.Lock()
	defer e.byteMu.Unlock()
	return e.doneBytes, e.totalBytes
}

func (e *Engine) addDone(n int64) {
	e.byteMu.Lock()
	e.doneBytes += n
	e.byteMu.Unlock()
}

// WithCreds 注册某种存储类型的凭证
func (e *Engine) WithCreds(t objstore.ProviderType, ak, sk string) *Engine {
	e.creds[t] = &Creds{AK: ak, SK: sk}
	return e
}

// WithCredsFull 注册凭证 + endpoint + profile（仅 S3 需要这些额外字段）。
func (e *Engine) WithCredsFull(t objstore.ProviderType, c Creds) *Engine {
	cc := c
	e.creds[t] = &cc
	return e
}

// CheckMemory 预估最坏情况内存占用，超限返回错误
func (e *Engine) CheckMemory() error {
	chunk := int64(e.cfg.ChunkMB) * 1024 * 1024
	largeMax := chunk * int64(e.cfg.ChunkConcurrency)
	smallMax := chunk * int64(e.cfg.ObjectConcurrency)
	worst := largeMax + smallMax

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("[内存预估] 最坏情况: %s（大文件: %s + 小文件: %s）| 进程当前: %s",
		progress.HumanSize(worst), progress.HumanSize(largeMax),
		progress.HumanSize(smallMax), progress.HumanSize(int64(m.Sys)))

	if worst > maxMemoryBytes {
		return fmt.Errorf(
			"内存安全检查失败：预估最坏情况 %s，超过上限 %s\n建议减小 -chunk(%dMB) / -concurrency(%d) / -obj-concurrency(%d)",
			progress.HumanSize(worst), progress.HumanSize(maxMemoryBytes),
			e.cfg.ChunkMB, e.cfg.ChunkConcurrency, e.cfg.ObjectConcurrency,
		)
	}
	return nil
}

// Run 执行拷贝，根据配置自动选择模式
func (e *Engine) Run(ctx context.Context) error {
	switch {
	case e.cfg.SrcKey != "":
		return e.runSingle(ctx)
	case e.cfg.SrcPrefix != "":
		return e.runPrefix(ctx)
	case e.cfg.KeyListSource != "":
		return e.runList(ctx)
	default:
		return fmt.Errorf("请指定拷贝模式：-src-key / -src-prefix / -key-list")
	}
}

// runSingle 单文件拷贝
func (e *Engine) runSingle(ctx context.Context) error {
	info, err := e.src.HeadObject(ctx, e.cfg.SrcKey)
	if err != nil {
		return err
	}
	size := info.Size
	dstKey := e.cfg.DstKey
	if dstKey == "" {
		dstKey = e.cfg.SrcKey
	}
	chunkSize := int64(e.cfg.ChunkMB) * 1024 * 1024
	mode := "multipart"
	if size <= chunkSize {
		mode = "put"
	}
	log.Printf("[%s→%s] 文件大小: %s | 模式: %s",
		e.src.Provider(), e.dst.Provider(), progress.HumanSize(size), mode)

	prog := progress.New(size)
	defer prog.Stop()
	start := time.Now()

	if err := e.copyObject(ctx, e.cfg.SrcKey, dstKey, size, chunkSize, prog); err != nil {
		return err
	}
	elapsed := time.Since(start)
	log.Printf("✅ %s://%s/%s → %s://%s/%s | 耗时: %v | 速度: %s/s",
		e.src.Provider(), e.src.BucketName(), e.cfg.SrcKey,
		e.dst.Provider(), e.dst.BucketName(), dstKey,
		elapsed.Round(time.Second), progress.HumanSize(int64(float64(size)/elapsed.Seconds())))
	return nil
}

// filterObjectInfos 根据递归设置过滤对象列表
func filterObjectInfos(objs []objstore.ObjectInfo, prefix string, recursive bool) []objstore.ObjectInfo {
	if recursive {
		return objs
	}
	// 非递归模式：只保留直接在 prefix 下的对象，不包含子目录
	var filtered []objstore.ObjectInfo
	for _, obj := range objs {
		relative := strings.TrimPrefix(obj.Key, prefix)
		if !strings.Contains(relative, "/") {
			filtered = append(filtered, obj)
		}
	}
	return filtered
}

// interactiveConfirmObjs 交互式确认（object 版本）
func (e *Engine) interactiveConfirmObjs(actionType string, objs []objstore.ObjectInfo) []objstore.ObjectInfo {
	var confirmed []objstore.ObjectInfo
	reader := bufio.NewReader(os.Stdin)

	for _, obj := range objs {
		fmt.Printf("%s对象: %s://%s/%s ? [y/N]: ",
			actionType, e.src.Provider(), e.src.BucketName(), obj.Key)

		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(strings.ToLower(input))

		if input == "y" || input == "yes" {
			confirmed = append(confirmed, obj)
			fmt.Println("✅ 确认")
		} else {
			fmt.Println("⏭️  跳过")
		}
	}
	return confirmed
}

// runPrefix 前缀批量拷贝
func (e *Engine) runPrefix(ctx context.Context) error {
	log.Printf("列举 %s://%s/%s* ...", e.src.Provider(), e.src.BucketName(), e.cfg.SrcPrefix)

	opts := objstore.ListOptions{Prefix: e.cfg.SrcPrefix}
	if e.cfg.Recursive {
		opts.Delimiter = "" // 递归列举
	}
	objs, err := e.src.ListObjects(ctx, opts)
	if err != nil {
		return err
	}

	// 非递归模式下已由 ListObjects 处理，但 filterObjectInfos 保持兼容
	objs = filterObjectInfos(objs, e.cfg.SrcPrefix, e.cfg.Recursive)

	// 应用 --exclude / --include
	if e.cfg.Filter != nil && e.cfg.Filter.HasRules() {
		filtered := objs[:0]
		for _, o := range objs {
			rel := strings.TrimPrefix(o.Key, e.cfg.SrcPrefix)
			rel = strings.TrimLeft(rel, "/")
			if e.cfg.Filter.Match(rel) {
				filtered = append(filtered, o)
			}
		}
		objs = filtered
		log.Printf("过滤后 → %d 个", len(objs))
	}

	log.Printf("共 %d 个对象", len(objs))
	if len(objs) == 0 {
		return nil
	}

	// 如果需要交互确认
	if !e.cfg.Force {
		objs = e.interactiveConfirmObjs("拷贝", objs)
		if len(objs) == 0 {
			log.Println("用户取消操作")
			return nil
		}
	}

	start := time.Now()
	errs := e.runBatch(ctx, objs, func(key string) string {
		return e.cfg.DstPrefix + strings.TrimPrefix(key, e.cfg.SrcPrefix)
	})
	return summarizeObjs(objs, errs, start)
}

// runList 对象 URL 列表拷贝
func (e *Engine) runList(ctx context.Context) error {
	lines, err := loadURLList(e.cfg.KeyListSource)
	if err != nil {
		return err
	}
	log.Printf("[list] 来源: %s | 共 %d 条", e.cfg.KeyListSource, len(lines))
	if len(lines) == 0 {
		return nil
	}

	objs := make([]*ObjectString, 0, len(lines))
	for _, line := range lines {
		obj, err := ParseObjectString(line)
		if err != nil {
			return err
		}
		objs = append(objs, obj)
	}

	sem := make(chan struct{}, e.cfg.ObjectConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string
	start := time.Now()

	for _, obj := range objs {
		obj := obj
		dstKey := e.cfg.DstPrefix + obj.Key
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			cred := e.creds[obj.StorageType]
			if cred == nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: 缺少 %s 凭证", obj.Raw, obj.StorageType))
				mu.Unlock()
				return
			}

			var srcStore objstore.Store
			srcStore, buildErr := objstore.New(objstore.Config{
				Provider:  obj.StorageType,
				Bucket:    obj.Bucket,
				Region:    obj.Region,
				SecretID:  cred.AK,
				SecretKey: cred.SK,
				Endpoint:  cred.Endpoint,
				Profile:   cred.Profile,
			})
			if buildErr != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", obj.Raw, buildErr))
				mu.Unlock()
				return
			}

			info, err := srcStore.HeadObject(ctx, obj.Key)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: HeadObject: %v", obj.Raw, err))
				mu.Unlock()
				return
			}
			size := info.Size
			chunkSize := int64(e.cfg.ChunkMB) * 1024 * 1024
			prog := progress.New(size)
			err = e.copyObjectBetween(ctx, srcStore, obj.Key, e.dst, dstKey, size, chunkSize, prog)
			prog.Stop()
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", obj.Raw, err))
				mu.Unlock()
				return
			}
			log.Printf("✅ %s → %s://%s/%s", obj.Raw, e.dst.Provider(), e.dst.BucketName(), dstKey)
		}()
	}
	wg.Wait()
	return summarize(lines, errs, start)
}

// runBatch 批量拷贝一组对象
func (e *Engine) runBatch(ctx context.Context, objs []objstore.ObjectInfo, dstKeyFn func(string) string) []string {
	sem := make(chan struct{}, e.cfg.ObjectConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string
	chunkSize := int64(e.cfg.ChunkMB) * 1024 * 1024

	for _, obj := range objs {
		obj := obj
		key := obj.Key
		size := obj.Size
		dstKey := dstKeyFn(key)
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			prog := progress.New(size)
			err := e.copyObject(ctx, key, dstKey, size, chunkSize, prog)
			prog.Stop()
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", key, err))
				mu.Unlock()
				return
			}
			log.Printf("✅ %s → %s", key, dstKey)
		}()
	}
	wg.Wait()
	return errs
}

// copyObject 拷贝单个对象（src/dst 已固定）
func (e *Engine) copyObject(ctx context.Context, srcKey, dstKey string, size, chunkSize int64, prog *progress.Tracker) error {
	return e.copyObjectBetween(ctx, e.src, srcKey, e.dst, dstKey, size, chunkSize, prog)
}

// copyObjectBetween 在任意两个 Storage 之间拷贝单个对象
func (e *Engine) copyObjectBetween(ctx context.Context,
	src objstore.Store, srcKey string,
	dst objstore.Store, dstKey string,
	size, chunkSize int64,
	prog *progress.Tracker,
) error {
	if e.cfg.DryRun {
		fmt.Printf("[dry-run] copy %s://%s → %s://%s (%d bytes)\n", src.Provider(), srcKey, dst.Provider(), dstKey, size)
		return nil
	}
	// 同厂商则优先走服务端复制（不过本机带宽）
	// 跨厂商（S3↔COS）走后面的本机中转流式路径
	// ForceClientCopy=true 时跨过 server-side（跨账号同 provider 场景，issue #13）
	if !e.cfg.ForceClientCopy && src.Provider() == dst.Provider() {
		if srcSC, ok1 := src.(objstore.ServerCopier); ok1 {
			if dstSC, ok2 := dst.(objstore.ServerCopier); ok2 {
				if size <= chunkSize {
					// 小文件单次服务端复制
					if err := e.retry(ctx, "CopyObject", func(ctx context.Context) error {
						return dstSC.CopyObject(ctx, dstKey, srcSC, srcKey)
					}); err != nil {
						return err
					}
					prog.Add(size)
					e.addDone(size)
					return nil
				}
				// 大文件分块服务端复制。分块重试由 SDK 内部或上层负责，
				// 这里不包一层 retry 以免全量重走。
				return dstSC.CopyPartFrom(ctx, dstKey, srcSC, srcKey, size, chunkSize, e.cfg.ChunkConcurrency, func(n int64) {
					prog.Add(n)
					e.addDone(n)
				})
			}
		}
	}

	// 其他方向：小文件 PutObject，大文件流式 Multipart
	opts := e.cfg.PutOptions
	optUploader, hasOpt := dst.(objstore.OptionalUploader)
	if size <= chunkSize {
		var data []byte
		if err := e.retry(ctx, "GetAll", func(ctx context.Context) error {
			var err error
			data, err = src.GetAll(ctx, srcKey)
			return err
		}); err != nil {
			return err
		}
		// 上传前简单按总字节数计限流（上行限速）
		if e.lim != nil {
			if err := e.lim.Wait(ctx, len(data)); err != nil {
				return err
			}
		}
		prog.Add(size)
		e.addDone(size)
		return e.retry(ctx, "PutObject", func(ctx context.Context) error {
			if opts.HasAny() && hasOpt {
				return optUploader.PutObjectOpt(ctx, dstKey, data, opts)
			}
			return dst.PutObject(ctx, dstKey, data)
		})
	}
	fetchPart := func(_ int, offset, sz int64) ([]byte, error) {
		var data []byte
		if err := e.retry(ctx, "GetRange", func(ctx context.Context) error {
			var err error
			data, err = src.GetRange(ctx, srcKey, offset, offset+sz-1)
			return err
		}); err != nil {
			return nil, err
		}
		if e.lim != nil {
			if err := e.lim.Wait(ctx, len(data)); err != nil {
				return nil, err
			}
		}
		prog.Add(sz)
		e.addDone(sz)
		return data, nil
	}
	if opts.HasAny() && hasOpt {
		return optUploader.MultipartUploadOpt(ctx, dstKey, size, chunkSize, e.cfg.ChunkConcurrency, fetchPart, opts)
	}
	return dst.MultipartUpload(ctx, dstKey, size, chunkSize, e.cfg.ChunkConcurrency, fetchPart)
}

// retry 包装单个 IO 调用。cfg.Retry.Attempts<=1 时退化为直接调用。
func (e *Engine) retry(ctx context.Context, op string, fn func(ctx context.Context) error) error {
	cfg := e.cfg.Retry.Sanitize()
	if cfg.Attempts <= 1 {
		return fn(ctx)
	}
	return Retry(ctx, cfg, op, fn, func(attempt int, err error, sleep time.Duration) {
		log.Printf("⚠️  %s 重试 %d/%d、5s 后重试…错误: %v (sleep=%v)", op, attempt, cfg.Attempts-1, err, sleep)
	})
}

func summarizeObjs(objs []objstore.ObjectInfo, errs []string, startTime time.Time) error {
	elapsed := time.Since(startTime)
	log.Printf("完成 %d 个对象，耗时 %v，失败 %d 个", len(objs)-len(errs), elapsed.Round(time.Second), len(errs))
	for _, e := range errs {
		log.Printf("[FAIL] %s", e)
	}
	if len(errs) > 0 {
		return fmt.Errorf("存在 %d 个失败对象", len(errs))
	}
	return nil
}

func summarize(all []string, errs []string, startTime time.Time) error {
	elapsed := time.Since(startTime)
	log.Printf("完成 %d 个对象，耗时 %v，失败 %d 个", len(all)-len(errs), elapsed.Round(time.Second), len(errs))
	for _, e := range errs {
		log.Printf("[FAIL] %s", e)
	}
	if len(errs) > 0 {
		return fmt.Errorf("存在 %d 个失败对象", len(errs))
	}
	return nil
}
// chunkMBFor 为给定总大小选取合适的分块大小（MB）。
// userVal>0 表示用户显式设过 -chunk，直接采用；否则按总大小梯度选择。
// 策略：<5GB→8 / 5-50GB→32 / 50-500GB→128 / >500GB→512。
func chunkMBFor(totalSize int64, userVal int) int {
	if userVal > 0 {
		return userVal
	}
	const gb = int64(1) << 30
	switch {
	case totalSize < 5*gb:
		return 8
	case totalSize < 50*gb:
		return 32
	case totalSize < 500*gb:
		return 128
	default:
		return 512
	}
}
