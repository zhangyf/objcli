package cmd

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	cosClient "objcopy/cos"
	"objcopy/progress"
	s3Client "objcopy/s3"
)

// CopyFromURLList 按对象 URL 列表拷贝到目标桶
// 列表每行是一个完整的对象 URL（COS 或 S3），工具自动解析来源
func CopyFromURLList(ctx context.Context, cfg CopyFromURLListConfig) error {
	lines, err := loadURLList(cfg.KeyListSource)
	if err != nil {
		return err
	}
	log.Printf("[list] 列表来源: %s | 共 %d 条", cfg.KeyListSource, len(lines))
	if len(lines) == 0 {
		return nil
	}

	// 预解析所有 URL，提前报错
	objs := make([]*ObjURL, 0, len(lines))
	for _, line := range lines {
		obj, err := ParseObjURL(line)
		if err != nil {
			return err
		}
		objs = append(objs, obj)
	}

	// 初始化目标 client
	var (
		dstCOS *cosClient.Client
		dstS3  *s3Client.Client
	)
	switch cfg.DstType {
	case "cos":
		dstCOS = cosClient.New(cfg.COSSecretID, cfg.COSSecretKey, cfg.DstBucket, cfg.DstRegion)
	case "s3":
		var e error
		dstS3, e = s3Client.New(ctx, cfg.S3AccessKey, cfg.S3SecretKey, cfg.DstRegion, cfg.DstBucket)
		if e != nil {
			return e
		}
	}

	sem := make(chan struct{}, cfg.ObjectConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string
	startTime := time.Now()

	for _, obj := range objs {
		obj := obj
		dstKey := cfg.DstPrefix + obj.Key

		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			var copyErr error

			switch obj.StorageType {
			case "cos":
				srcCOS := cosClient.New(cfg.COSSecretID, cfg.COSSecretKey, obj.Bucket, obj.Region)
				size, err := srcCOS.HeadObject(ctx, obj.Key)
				if err != nil {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("%s: HeadObject: %v", obj.RawURL, err))
					mu.Unlock()
					return
				}
				chunkSize := int64(cfg.ChunkMB) * 1024 * 1024
				prog := progress.New(size)
				switch cfg.DstType {
				case "cos":
					copyErr = dstCOS.CopyFromCOS(ctx, dstKey, srcCOS, obj.Key, size, chunkSize, cfg.ChunkConcurrency, prog)
				case "s3":
					copyErr = dstS3.MultipartUpload(ctx, dstKey, size, chunkSize, cfg.ChunkConcurrency,
						func(_ int, offset, sz int64) ([]byte, error) {
							data, err := srcCOS.GetRange(ctx, obj.Key, offset, offset+sz-1)
							if err == nil {
								prog.Add(sz)
							}
							return data, err
						},
					)
				}
				prog.Stop()

			case "s3":
				srcS3, err := s3Client.New(ctx, cfg.S3AccessKey, cfg.S3SecretKey, obj.Region, obj.Bucket)
				if err != nil {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("%s: 初始化 S3 client: %v", obj.RawURL, err))
					mu.Unlock()
					return
				}
				size, err := srcS3.HeadObject(ctx, obj.Key)
				if err != nil {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("%s: HeadObject: %v", obj.RawURL, err))
					mu.Unlock()
					return
				}
				chunkSize := int64(cfg.ChunkMB) * 1024 * 1024
				prog := progress.New(size)
				switch cfg.DstType {
				case "cos":
					copyErr = dstCOS.MultipartUpload(ctx, dstKey, size, chunkSize, cfg.ChunkConcurrency,
						func(_ int, offset, sz int64) ([]byte, error) {
							data, err := srcS3.GetRange(ctx, obj.Key, offset, offset+sz-1)
							if err == nil {
								prog.Add(sz)
							}
							return data, err
						},
					)
				case "s3":
					copyErr = dstS3.MultipartUpload(ctx, dstKey, size, chunkSize, cfg.ChunkConcurrency,
						func(_ int, offset, sz int64) ([]byte, error) {
							data, err := srcS3.GetRange(ctx, obj.Key, offset, offset+sz-1)
							if err == nil {
								prog.Add(sz)
							}
							return data, err
						},
					)
				}
				prog.Stop()

			default:
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: 不支持的来源类型", obj.RawURL))
				mu.Unlock()
				return
			}

			if copyErr != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", obj.RawURL, copyErr))
				mu.Unlock()
				return
			}
			log.Printf("✅ %s → %s/%s", obj.RawURL, cfg.DstBucket, dstKey)
		}()
	}
	wg.Wait()

	return summarize(lines, errs, startTime)
}

type CopyFromURLListConfig struct {
	// 目标
	DstType   string // "cos" | "s3"
	DstBucket string
	DstRegion string
	DstPrefix string

	// COS 凭证（源或目标为 COS 时使用）
	COSSecretID  string
	COSSecretKey string

	// S3 凭证（源或目标为 S3 时使用）
	S3AccessKey string
	S3SecretKey string

	KeyListSource     string
	ChunkMB           int
	ChunkConcurrency  int
	ObjectConcurrency int
}

// summarize 输出最终统计
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
