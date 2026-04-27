package cmd

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/zhangyf/objstore"
)

// DeleteConfig 删除配置
type DeleteConfig struct {
	// 删除模式（三选一）
	Key        string // 单个对象删除
	Prefix     string // 前缀批量删除（支持 * 通配符）
	KeyList    string // 对象 URL 列表删除

	// 并发控制
	Concurrency int // 删除并发数

	// 列表模式专用
	URLDecode bool // 是否对列表中的对象名进行 URL decode
}

// DeleteEngine 删除引擎
type DeleteEngine struct {
	storage objstore.Store
	cfg     DeleteConfig
	
	totalObjects int       // 总对象数
	doneObjects  int       // 已完成对象数
	progressMu   sync.Mutex // 进度锁
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

// runPrefix 前缀批量删除
func (e *DeleteEngine) runPrefix(ctx context.Context) error {
	log.Printf("批量删除: %s://%s/%s*", 
		e.storage.Provider(), e.storage.BucketName(), e.cfg.Prefix)
	
	start := time.Now()
	
	// 获取所有对象
	keys, err := e.storage.ListObjects(ctx, e.cfg.Prefix)
	if err != nil {
		return err
	}
	
	log.Printf("共 %d 个对象", len(keys))
	if len(keys) == 0 {
		return nil
	}
	
	// 设置总对象数
	e.SetTotalObjects(len(keys))
	
	// 分批删除
	errs := e.runBatchDelete(ctx, keys)
	
	elapsed := time.Since(start)
	log.Printf("完成 %d 个对象，耗时 %v，失败 %d 个", 
		len(keys)-len(errs), elapsed.Round(time.Second), len(errs))
	
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
	
	// 解析对象
	objs := make([]*ObjectString, 0, len(lines))
	for _, line := range lines {
		obj, err := ParseObjectString(line)
		if err != nil {
			return err
		}
		objs = append(objs, obj)
	}
	
	// 设置总对象数
	e.SetTotalObjects(len(objs))
	
	// 检查是否需要动态创建存储
	storageMap := make(map[string]objstore.Store) // key: bucket@region
	// 这里需要动态创建存储，暂时留空
	
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
			
			// 获取或创建存储实例
			storageKey := obj.Bucket + "@" + obj.Region
			mu.Lock()
			storage, exists := storageMap[storageKey]
			mu.Unlock()
			
			if !exists {
				// 需要动态创建存储（这里需要凭证）
				// 目前还不能处理跨存储删除
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

// runBatchDelete 批量删除一组 key
func (e *DeleteEngine) runBatchDelete(ctx context.Context, keys []string) []string {
	sem := make(chan struct{}, e.cfg.Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string
	
	for _, key := range keys {
		key := key
		wg.Add(1)
		sem <- struct{}{}
		
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			
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

// HeadObject 检查对象是否存在
func (e *DeleteEngine) HeadObject(ctx context.Context, key string) (int64, error) {
	return e.storage.HeadObject(ctx, key)
}
