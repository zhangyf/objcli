package cmd

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/zhangyf/objstore"
)

// DeleteConfig 删除配置
type DeleteConfig struct {
	// 删除模式（三选一）
	Key     string // 单个对象删除
	Prefix  string // 前缀批量删除（支持 * 通配符）
	KeyList string // 对象 URL 列表删除

	// 并发控制
	Concurrency int // 删除并发数

	// 列表模式专用
	URLDecode bool // 是否对列表中的对象名进行 URL decode

	// prefix 模式特定
	Recursive bool         // 是否递归处理目录下的所有对象
	Force     bool         // 是否强制跳过用户确认
	Filter    *MatchFilter // exclude/include 过滤

	DryRun bool // 仅打印将要删除的对象，不真正删除
}

// DeleteEngine 删除引擎
type DeleteEngine struct {
	storage objstore.Store
	cfg     DeleteConfig

	totalObjects int          // 总对象数
	doneObjects  int          // 已完成对象数
	progressMu   sync.Mutex   // 进度锁
}

func NewDeleteEngine(storage objstore.Store, cfg DeleteConfig) *DeleteEngine {
	return &DeleteEngine{storage: storage, cfg: cfg, totalObjects: 0, doneObjects: 0}
}

// Run 执行删除，根据配置自动选择模式
func (e *DeleteEngine) Run(ctx context.Context) error {
	switch {
	case e.cfg.Key != "":
		return e.runSingle(ctx)
	case e.cfg.Prefix != "":
		return e.runPrefix(ctx)
	case e.cfg.KeyList != "":
		return e.runList(ctx)
	default:
		return fmt.Errorf("请指定删除模式：-key / -prefix / -key-list")
	}
}

// runSingle 单对象删除
func (e *DeleteEngine) runSingle(ctx context.Context) error {
	log.Printf("删除对象: %s://%s/%s",
		e.storage.Provider(), e.storage.BucketName(), e.cfg.Key)

	if e.cfg.DryRun {
		fmt.Printf("[dry-run] delete %s://%s/%s\n", e.storage.Provider(), e.storage.BucketName(), e.cfg.Key)
		e.SetTotalObjects(1)
		e.addDoneObject()
		return nil
	}

	start := time.Now()
	err := e.storage.DeleteObject(ctx, e.cfg.Key)
	if err != nil {
		return fmt.Errorf("删除失败: %v", err)
	}

	e.SetTotalObjects(1)
	e.addDoneObject()
	elapsed := time.Since(start)
	log.Printf("✅ 删除成功 | 耗时: %v", elapsed.Round(time.Second))
	return nil
}

// filterObjInfosForDelete 根据递归设置过滤对象列表（删除专用）
func filterObjInfosForDelete(objs []objstore.ObjectInfo, prefix string, recursive bool) []objstore.ObjectInfo {
	if recursive {
		return objs
	}
	var filtered []objstore.ObjectInfo
	for _, obj := range objs {
		relative := strings.TrimPrefix(obj.Key, prefix)
		if !strings.Contains(relative, "/") {
			filtered = append(filtered, obj)
		}
	}
	return filtered
}

// interactiveConfirmObjs 交互式确认（ObjectInfo 版本）
func (e *DeleteEngine) interactiveConfirmObjs(objs []objstore.ObjectInfo) []objstore.ObjectInfo {
	var confirmed []objstore.ObjectInfo
	reader := bufio.NewReader(os.Stdin)

	for _, obj := range objs {
		fmt.Printf("删除对象: %s://%s/%s ? [y/N]: ",
			e.storage.Provider(), e.storage.BucketName(), obj.Key)

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

// runPrefix 前缀批量删除
func (e *DeleteEngine) runPrefix(ctx context.Context) error {
	log.Printf("批量删除: %s://%s/%s*",
		e.storage.Provider(), e.storage.BucketName(), e.cfg.Prefix)

	start := time.Now()

	opts := objstore.ListOptions{Prefix: e.cfg.Prefix}
	if e.cfg.Recursive {
		opts.Delimiter = ""
	}
	objs, err := e.storage.ListObjects(ctx, opts)
	if err != nil {
		return err
	}

	objs = filterObjInfosForDelete(objs, e.cfg.Prefix, e.cfg.Recursive)

	// 应用 --exclude / --include
	if e.cfg.Filter != nil && e.cfg.Filter.HasRules() {
		filtered := objs[:0]
		for _, o := range objs {
			rel := strings.TrimPrefix(o.Key, e.cfg.Prefix)
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
		// 对齐 ls：找不到 prefix 下任何对象 → ErrNoSuchObject。1
		return ErrNoSuchObject
	}

	if !e.cfg.Force {
		objs = e.interactiveConfirmObjs(objs)
		if len(objs) == 0 {
			log.Println("用户取消操作")
			return nil
		}
	}

	e.SetTotalObjects(len(objs))

	errs := e.runBatchDeleteObj(ctx, objs)

	elapsed := time.Since(start)
	log.Printf("完成 %d 个对象，耗时 %v，失败 %d 个",
		len(objs)-len(errs), elapsed.Round(time.Second), len(errs))
	for _, e := range errs {
		log.Printf("[FAIL] %s", e)
	}
	if len(errs) > 0 {
		return fmt.Errorf("存在 %d 个失败对象", len(errs))
	}
	return nil
}

// runList 对象 URL 列表删除
func (e *DeleteEngine) runList(ctx context.Context) error {
	lines, err := loadURLList(e.cfg.KeyList)
	if err != nil {
		return err
	}

	log.Printf("列表删除: 来源: %s | 共 %d 条", e.cfg.KeyList, len(lines))
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

	e.SetTotalObjects(len(objs))

	storageMap := make(map[string]objstore.Store)

	start := time.Now()
	sem := make(chan struct{}, e.cfg.Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string

	for _, obj := range objs {
		obj := obj
		wg.Add(1)
		sem <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			storageKey := obj.Bucket + "@" + obj.Region
			mu.Lock()
			storage, exists := storageMap[storageKey]
			mu.Unlock()

			if !exists {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: 不支持跨存储删除（需要凭证管理）", obj.Raw))
				mu.Unlock()
				return
			}

			key := obj.Key
			if e.cfg.URLDecode {
				decoded, err := url.PathUnescape(key)
				if err == nil {
					key = decoded
				}
			}

			if e.cfg.DryRun {
				fmt.Printf("[dry-run] delete %s://%s/%s\n", storage.Provider(), storage.BucketName(), key)
				e.addDoneObject()
				return
			}

			err := storage.DeleteObject(ctx, key)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", obj.Raw, err))
				mu.Unlock()
				return
			}

			e.addDoneObject()
			log.Printf("✅ %s", obj.Raw)
		}()
	}

	wg.Wait()

	elapsed := time.Since(start)
	log.Printf("完成 %d 个对象，耗时 %v，失败 %d 个",
		len(objs)-len(errs), elapsed.Round(time.Second), len(errs))
	for _, e := range errs {
		log.Printf("[FAIL] %s", e)
	}
	if len(errs) > 0 {
		return fmt.Errorf("存在 %d 个失败对象", len(errs))
	}
	return nil
}

// runBatchDeleteObj 批量删除一组对象（ObjectInfo 版本）
func (e *DeleteEngine) runBatchDeleteObj(ctx context.Context, objs []objstore.ObjectInfo) []string {
	sem := make(chan struct{}, e.cfg.Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string

	for _, obj := range objs {
		key := obj.Key
		wg.Add(1)
		sem <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			if e.cfg.DryRun {
				fmt.Printf("[dry-run] delete %s://%s/%s\n", e.storage.Provider(), e.storage.BucketName(), key)
				e.addDoneObject()
				return
			}

			err := e.storage.DeleteObject(ctx, key)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", key, err))
				mu.Unlock()
				return
			}

			e.addDoneObject()
			log.Printf("✅ %s", key)
		}()
	}

	wg.Wait()
	return errs
}

// SetTotalObjects 设置总对象数
func (e *DeleteEngine) SetTotalObjects(n int) {
	e.progressMu.Lock()
	e.totalObjects = n
	e.progressMu.Unlock()
}

// ObjectsProgress 返回 (doneObjects, totalObjects)
func (e *DeleteEngine) ObjectsProgress() (int, int) {
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	return e.doneObjects, e.totalObjects
}

// addDoneObject 增加已完成对象计数
func (e *DeleteEngine) addDoneObject() {
	e.progressMu.Lock()
	e.doneObjects++
	e.progressMu.Unlock()
}

// HeadObject 检查对象是否存在，返回对象大小
func (e *DeleteEngine) HeadObject(ctx context.Context, key string) (int64, error) {
	info, err := e.storage.HeadObject(ctx, key)
	if err != nil {
		return 0, err
	}
	return info.Size, nil
}