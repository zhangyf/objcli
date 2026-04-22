package cmd

import (
	"context"
	"log"
	"time"

	cosClient "objcopy/cos"
	"objcopy/progress"
	s3Client "objcopy/s3"
)

// S3ToCOS 将 S3 对象迁移到 COS，小文件 PutObject，大文件 Multipart
func S3ToCOS(ctx context.Context, cfg S3ToCOSConfig) error {
	s3, err := s3Client.New(ctx, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region, cfg.S3Bucket)
	if err != nil {
		return err
	}
	totalSize, err := s3.HeadObject(ctx, cfg.S3Key)
	if err != nil {
		return err
	}
	dstKey := cfg.DstKey
	if dstKey == "" {
		dstKey = cfg.S3Key
	}
	chunkSize := int64(cfg.ChunkMB) * 1024 * 1024
	mode := "multipart"
	if totalSize <= chunkSize {
		mode = "put"
	}
	log.Printf("[s3-to-cos] 文件大小: %s | 模式: %s | 分块: %s | 并发: %d",
		progress.HumanSize(totalSize), mode, progress.HumanSize(chunkSize), cfg.Concurrency)

	cos := cosClient.New(cfg.COSSecretID, cfg.COSSecretKey, cfg.DstBucket, cfg.DstRegion)
	prog := progress.New(totalSize)
	defer prog.Stop()

	startTime := time.Now()
	err = CopyS3ToCOS(ctx, s3, cfg.S3Key, cos, dstKey, totalSize, chunkSize, cfg.Concurrency, prog)
	if err != nil {
		return err
	}
	elapsed := time.Since(startTime)
	log.Printf("✅ 完成！s3://%s/%s → cos://%s/%s | 耗时: %v | 平均速度: %s/s",
		cfg.S3Bucket, cfg.S3Key, cfg.DstBucket, dstKey,
		elapsed.Round(time.Second), progress.HumanSize(int64(float64(totalSize)/elapsed.Seconds())))
	return nil
}

type S3ToCOSConfig struct {
	S3AccessKey  string
	S3SecretKey  string
	S3Region     string
	S3Bucket     string
	S3Key        string
	COSSecretID  string
	COSSecretKey string
	DstBucket    string
	DstRegion    string
	DstKey       string
	ChunkMB      int
	Concurrency  int
}
