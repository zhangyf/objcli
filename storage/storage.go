package storage

import (
	"context"
	"fmt"
)

// StorageType 存储类型枚举
type StorageType string

const (
	StorageTypeCOS StorageType = "cos"
	StorageTypeS3  StorageType = "s3"
)

// ParseStorageType 解析字符串为 StorageType，无效时返回错误
func ParseStorageType(s string) (StorageType, error) {
	switch StorageType(s) {
	case StorageTypeCOS, StorageTypeS3:
		return StorageType(s), nil
	default:
		return "", fmt.Errorf("不支持的存储类型: %q，支持: cos, s3", s)
	}
}

// Storage 统一的对象存储接口，COS/S3 各自实现
type Storage interface {
	// HeadObject 获取对象大小
	HeadObject(ctx context.Context, key string) (int64, error)

	// GetRange 按字节范围下载
	GetRange(ctx context.Context, key string, start, end int64) ([]byte, error)

	// GetAll 一次性读取整个对象
	GetAll(ctx context.Context, key string) ([]byte, error)

	// PutObject 单次上传（小文件）
	PutObject(ctx context.Context, key string, data []byte) error

	// MultipartUpload 分块上传（大文件）
	// fetchPart 回调按 offset/size 获取分块数据
	MultipartUpload(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
		fetchPart func(partNumber int, offset, size int64) ([]byte, error)) error

	// ListObjects 列出指定前缀下所有对象 Key
	ListObjects(ctx context.Context, prefix string) ([]string, error)

	// Type 返回存储类型
Type() StorageType

	// BucketName 返回 Bucket 名称，用于日志
	BucketName() string
}
