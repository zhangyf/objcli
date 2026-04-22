package cmd

import (
	"context"
	"log"
	"time"

	cosClient "objcopy/cos"
	"objcopy/progress"
)

// COSToCOS 在 COS 桶之间复制，小文件 PutObject，大文件 UploadPart-Copy
func COSToCOS(ctx context.Context, cfg COSToCOSConfig) error {
	src := cosClient.New(cfg.COSSecretID, cfg.COSSecretKey, cfg.SrcBucket, cfg.SrcRegion)
	dst := cosClient.New(cfg.COSSecretID, cfg.COSSecretKey, cfg.DstBucket, cfg.DstRegion)

	totalSize, err := src.HeadObject(ctx, cfg.SrcKey)
	if err != nil {
		return err
	}
	dstKey := cfg.DstKey
	if dstKey == "" {
		dstKey = cfg.SrcKey
	}
	chunkSize := int64(cfg.ChunkMB) * 1024 * 1024
	mode := "multipart-copy"
	if totalSize <= chunkSize {
		mode = "put"
	}
	log.Printf("[cos-to-cos] 文件大小: %s | 模式: %s | 分块: %s | 并发: %d",
		progress.HumanSize(totalSize), mode, progress.HumanSize(chunkSize), cfg.Concurrency)

	prog := progress.New(totalSize)
	defer prog.Stop()

	startTime := time.Now()
	err = CopyCOSToCOS(ctx, src, cfg.SrcKey, dst, dstKey, totalSize, chunkSize, cfg.Concurrency, prog)
	if err != nil {
		return err
	}
	elapsed := time.Since(startTime)
	log.Printf("✅ 完成！cos://%s/%s → cos://%s/%s | 耗时: %v | 平均速度: %s/s",
		cfg.SrcBucket, cfg.SrcKey, cfg.DstBucket, dstKey,
		elapsed.Round(time.Second), progress.HumanSize(int64(float64(totalSize)/elapsed.Seconds())))
	return nil
}

type COSToCOSConfig struct {
	COSSecretID  string
	COSSecretKey string
	SrcBucket    string
	SrcRegion    string
	SrcKey       string
	DstBucket    string
	DstRegion    string
	DstKey       string
	ChunkMB      int
	Concurrency  int
}
