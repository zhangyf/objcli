package cmd

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	cosClient "objcopy/cos"
	"objcopy/progress"
)

// COSToCOSPrefix 将 COS 源桶指定前缀下所有对象复制到 COS 目标桶的指定前缀下
func COSToCOSPrefix(ctx context.Context, cfg COSToCOSPrefixConfig) error {
	src := cosClient.New(cfg.COSSecretID, cfg.COSSecretKey, cfg.SrcBucket, cfg.SrcRegion)
	dst := cosClient.New(cfg.COSSecretID, cfg.COSSecretKey, cfg.DstBucket, cfg.DstRegion)

	log.Printf("[cos-to-cos prefix] 列举 cos://%s/%s ...", cfg.SrcBucket, cfg.SrcPrefix)
	keys, err := src.ListObjects(ctx, cfg.SrcPrefix)
	if err != nil {
		return err
	}
	log.Printf("共 %d 个对象", len(keys))
	if len(keys) == 0 {
		return nil
	}

	sem := make(chan struct{}, cfg.ObjectConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string
	startTime := time.Now()

	for _, key := range keys {
		key := key
		dstKey := replacePrefix(key, cfg.SrcPrefix, cfg.DstPrefix)

		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			size, err := src.HeadObject(ctx, key)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: HeadObject: %v", key, err))
				mu.Unlock()
				return
			}

			chunkSize := int64(cfg.ChunkMB) * 1024 * 1024
			prog := progress.New(size)

			err = dst.CopyFromCOS(ctx, dstKey, src, key, size, chunkSize, cfg.ChunkConcurrency, prog)
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

	elapsed := time.Since(startTime)
	log.Printf("完成 %d 个对象，耗时 %v，失败 %d 个", len(keys)-len(errs), elapsed.Round(time.Second), len(errs))
	for _, e := range errs {
		log.Printf("[FAIL] %s", e)
	}
	if len(errs) > 0 {
		return fmt.Errorf("存在 %d 个失败对象", len(errs))
	}
	return nil
}

type COSToCOSPrefixConfig struct {
	COSSecretID  string
	COSSecretKey string

	SrcBucket string
	SrcRegion string
	SrcPrefix string

	DstBucket string
	DstRegion string
	DstPrefix string

	ChunkMB           int
	ChunkConcurrency  int
	ObjectConcurrency int
}
