package cmd

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	cosClient "objcopy/cos"
	"objcopy/progress"
	s3Client "objcopy/s3"
)

// S3ToCOSPrefix 将 S3 源桶指定前缀下所有对象复制到 COS 目标桶的指定前缀下
func S3ToCOSPrefix(ctx context.Context, cfg S3ToCOSPrefixConfig) error {
	s3, err := s3Client.New(ctx, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region, cfg.S3Bucket)
	if err != nil {
		return err
	}

	log.Printf("[s3-to-cos prefix] 列举 s3://%s/%s ...", cfg.S3Bucket, cfg.SrcPrefix)
	keys, err := s3.ListObjects(ctx, cfg.SrcPrefix)
	if err != nil {
		return err
	}
	log.Printf("共 %d 个对象", len(keys))
	if len(keys) == 0 {
		return nil
	}

	cos := cosClient.New(cfg.COSSecretID, cfg.COSSecretKey, cfg.DstBucket, cfg.DstRegion)

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

			size, err := s3.HeadObject(ctx, key)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: HeadObject: %v", key, err))
				mu.Unlock()
				return
			}

			chunkSize := int64(cfg.ChunkMB) * 1024 * 1024
			prog := progress.New(size)

			err = cos.MultipartUpload(ctx, dstKey, size, chunkSize, cfg.ChunkConcurrency,
				func(partNumber int, offset, sz int64) ([]byte, error) {
					data, err := s3.GetRange(ctx, key, offset, offset+sz-1)
					if err == nil {
						prog.Add(sz)
					}
					return data, err
				},
			)
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

type S3ToCOSPrefixConfig struct {
	S3AccessKey string
	S3SecretKey string
	S3Region    string
	S3Bucket    string
	SrcPrefix   string

	COSSecretID  string
	COSSecretKey string
	DstBucket    string
	DstRegion    string
	DstPrefix    string

	ChunkMB          int
	ChunkConcurrency int // 单文件分块并发
	ObjectConcurrency int // 多文件并发数
}

// replacePrefix 替换 key 的源前缀为目标前缀
func replacePrefix(key, srcPrefix, dstPrefix string) string {
	rel := strings.TrimPrefix(key, srcPrefix)
	return dstPrefix + rel
}
