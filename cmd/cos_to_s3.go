package cmd

import (
	"context"
	"log"
	"time"

	cosClient "objcopy/cos"
	"objcopy/progress"
	s3Client "objcopy/s3"
)

// COSToS3 将 COS 对象迁移到 S3，小文件 PutObject，大文件 Multipart
func COSToS3(ctx context.Context, cfg COSToS3Config) error {
	cos := cosClient.New(cfg.COSSecretID, cfg.COSSecretKey, cfg.SrcBucket, cfg.SrcRegion)

	totalSize, err := cos.HeadObject(ctx, cfg.SrcKey)
	if err != nil {
		return err
	}
	dstKey := cfg.S3Key
	if dstKey == "" {
		dstKey = cfg.SrcKey
	}
	s3, err := s3Client.New(ctx, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region, cfg.S3Bucket)
	if err != nil {
		return err
	}
	chunkSize := int64(cfg.ChunkMB) * 1024 * 1024
	mode := "multipart"
	if totalSize <= chunkSize {
		mode = "put"
	}
	log.Printf("[cos-to-s3] 文件大小: %s | 模式: %s | 分块: %s | 并发: %d",
		progress.HumanSize(totalSize), mode, progress.HumanSize(chunkSize), cfg.Concurrency)

	prog := progress.New(totalSize)
	defer prog.Stop()

	startTime := time.Now()
	err = CopyCOSToS3(ctx, cos, cfg.SrcKey, s3, dstKey, totalSize, chunkSize, cfg.Concurrency, prog)
	if err != nil {
		return err
	}
	elapsed := time.Since(startTime)
	log.Printf("✅ 完成！cos://%s/%s → s3://%s/%s | 耗时: %v | 平均速度: %s/s",
		cfg.SrcBucket, cfg.SrcKey, cfg.S3Bucket, dstKey,
		elapsed.Round(time.Second), progress.HumanSize(int64(float64(totalSize)/elapsed.Seconds())))
	return nil
}

type COSToS3Config struct {
	COSSecretID  string
	COSSecretKey string
	SrcBucket    string
	SrcRegion    string
	SrcKey       string
	S3AccessKey  string
	S3SecretKey  string
	S3Region     string
	S3Bucket     string
	S3Key        string
	ChunkMB      int
	Concurrency  int
}
