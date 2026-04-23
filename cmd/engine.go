package cmd

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"objutil/progress"
	"objutil/storage"
)

const maxMemoryBytes = int64(4 * 1024 * 1024 * 1024)

// CopyConfig 拷贝引擎配置
type CopyConfig struct {
	// 拷贝模式（三选一）
	SrcKey        string // 单文件
	SrcPrefix     string // 前缀批量
	KeyListSource string // 对象 URL 列表

	DstKey    string // 目标 Key（单文件，默认同源）
	DstPrefix string // 目标前缀（前缀/列表模式）

	ChunkMB           int
	ChunkConcurrency  int
	ObjectConcurrency int
}

// Creds 通用凭证
type Creds struct {
	AK string
	SK string
}

// Engine 拷贝引擎
type Engine struct {
	src    storage.Storage
	dst    storage.Storage
	cfg    CopyConfig
	creds  map[storage.StorageType]*Creds
	global *progress.Tracker // 全局进度，由外部注入，可为 nil

	totalBytes int64      // 总字节数（所有对象累加）
	doneBytes  int64      // 已完成字节数
	byteMu     sync.Mutex
}

func NewEngine(src, dst storage.Storage, cfg CopyConfig) *Engine {
	return &Engine{src: src, dst: dst, cfg: cfg, creds: make(map[storage.StorageType]*Creds)}
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
func (e *Engine) WithCreds(t storage.StorageType, ak, sk string) *Engine {
	e.creds[t] = &Creds{AK: ak, SK: sk}
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
	size, err := e.src.HeadObject(ctx, e.cfg.SrcKey)
	if err != nil {
		return err
	}
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
		e.src.Type(), e.dst.Type(), progress.HumanSize(size), mode)

	prog := progress.New(size)
	defer prog.Stop()
	start := time.Now()

	if err := e.copyObject(ctx, e.cfg.SrcKey, dstKey, size, chunkSize, prog); err != nil {
		return err
	}
	elapsed := time.Since(start)
	log.Printf("✅ %s://%s/%s → %s://%s/%s | 耗时: %v | 速度: %s/s",
		e.src.Type(), e.src.BucketName(), e.cfg.SrcKey,
		e.dst.Type(), e.dst.BucketName(), dstKey,
		elapsed.Round(time.Second), progress.HumanSize(int64(float64(size)/elapsed.Seconds())))
	return nil
}

// runPrefix 前缀批量拷贝
func (e *Engine) runPrefix(ctx context.Context) error {
	log.Printf("[%s→%s prefix] 列举 %s://%s/%s ...",
		e.src.Type(), e.dst.Type(), e.src.Type(), e.src.BucketName(), e.cfg.SrcPrefix)
	keys, err := e.src.ListObjects(ctx, e.cfg.SrcPrefix)
	if err != nil {
		return err
	}
	log.Printf("共 %d 个对象", len(keys))
	if len(keys) == 0 {
		return nil
	}
	start := time.Now()
	errs := e.runBatch(ctx, keys, func(key string) string {
		return e.cfg.DstPrefix + strings.TrimPrefix(key, e.cfg.SrcPrefix)
	})
	return summarize(keys, errs, start)
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

	objs := make([]*ObjURL, 0, len(lines))
	for _, line := range lines {
		obj, err := ParseObjURL(line)
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
				errs = append(errs, fmt.Sprintf("%s: 缺少 %s 凭证", obj.RawURL, obj.StorageType))
				mu.Unlock()
				return
			}

			var srcStore storage.Storage
			var buildErr error
			switch obj.StorageType {
			case storage.StorageTypeCOS:
				srcStore = storage.NewCOSStorage(cred.AK, cred.SK, obj.Bucket, obj.Region)
			case storage.StorageTypeS3:
				srcStore, buildErr = storage.NewS3Storage(ctx, cred.AK, cred.SK, obj.Region, obj.Bucket)
				if buildErr != nil {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("%s: %v", obj.RawURL, buildErr))
					mu.Unlock()
					return
				}
			default:
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: 不支持的存储类型 %s", obj.RawURL, obj.StorageType))
				mu.Unlock()
				return
			}

			size, err := srcStore.HeadObject(ctx, obj.Key)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: HeadObject: %v", obj.RawURL, err))
				mu.Unlock()
				return
			}
			chunkSize := int64(e.cfg.ChunkMB) * 1024 * 1024
			prog := progress.New(size)
			err = e.copyObjectBetween(ctx, srcStore, obj.Key, e.dst, dstKey, size, chunkSize, prog)
			prog.Stop()
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", obj.RawURL, err))
				mu.Unlock()
				return
			}
			log.Printf("✅ %s → %s://%s/%s", obj.RawURL, e.dst.Type(), e.dst.BucketName(), dstKey)
		}()
	}
	wg.Wait()
	return summarize(lines, errs, start)
}

// runBatch 批量拷贝一组 key
func (e *Engine) runBatch(ctx context.Context, keys []string, dstKeyFn func(string) string) []string {
	sem := make(chan struct{}, e.cfg.ObjectConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string
	chunkSize := int64(e.cfg.ChunkMB) * 1024 * 1024

	for _, key := range keys {
		key := key
		dstKey := dstKeyFn(key)
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			size, err := e.src.HeadObject(ctx, key)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: HeadObject: %v", key, err))
				mu.Unlock()
				return
			}
			prog := progress.New(size)
			err = e.copyObject(ctx, key, dstKey, size, chunkSize, prog)
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
	src storage.Storage, srcKey string,
	dst storage.Storage, dstKey string,
	size, chunkSize int64,
	prog *progress.Tracker,
) error {
	// COS→COS 优先走 UploadPart-Copy（不过本机）
	if srcCOS, ok1 := src.(*storage.COSStorage); ok1 {
		if dstCOS, ok2 := dst.(*storage.COSStorage); ok2 {
			if size <= chunkSize {
				data, err := src.GetAll(ctx, srcKey)
				if err != nil {
					return err
				}
				prog.Add(size)
				return dst.PutObject(ctx, dstKey, data)
			}
			return dstCOS.CopyPartFrom(ctx, dstKey, srcCOS, srcKey, size, chunkSize, e.cfg.ChunkConcurrency)
		}
	}

	// 其他方向：小文件 PutObject，大文件流式 Multipart
	if size <= chunkSize {
		data, err := src.GetAll(ctx, srcKey)
		if err != nil {
			return err
		}
		prog.Add(size)
		e.addDone(size)
		return dst.PutObject(ctx, dstKey, data)
	}
	return dst.MultipartUpload(ctx, dstKey, size, chunkSize, e.cfg.ChunkConcurrency,
		func(_ int, offset, sz int64) ([]byte, error) {
			data, err := src.GetRange(ctx, srcKey, offset, offset+sz-1)
			if err == nil {
				prog.Add(sz)
				e.addDone(sz)
			}
			return data, err
		},
	)
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
