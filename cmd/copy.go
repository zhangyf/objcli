package cmd

import (
	"context"
	"fmt"
	"log"
	"runtime"

	cosClient "objcopy/cos"
	"objcopy/progress"
	s3Client "objcopy/s3"
)

const (
	// 单文件最大内存占用上限：4GB
	maxMemoryBytes = int64(4 * 1024 * 1024 * 1024)
)

// CheckMemorySafety 预估最大内存占用，超限则返回错误
// 大文件：concurrency 个 chunk 同时在内存
// 小文件（size <= chunkSize）：objConcurrency 个完整文件同时在内存
func CheckMemorySafety(chunkMB, concurrency, objConcurrency int) error {
	chunkBytes := int64(chunkMB) * 1024 * 1024

	// 大文件场景：单文件内最多 concurrency 个 chunk
	largeFileMax := chunkBytes * int64(concurrency)

	// 小文件场景：最多 objConcurrency 个文件，每个最大 chunkSize
	smallFileMax := chunkBytes * int64(objConcurrency)

	// 两个场景可能同时发生（大文件 + 小文件混合），取叠加最坏情况
	worstCase := largeFileMax + smallFileMax

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// 系统可用内存估算：用当前已分配作为参考，保守估计剩余 = maxMemoryBytes - 已用
	alreadyUsed := int64(m.Sys)

	log.Printf("[内存预估] 最坏情况内存占用: %s（大文件: %s + 小文件: %s）| 当前进程内存: %s",
		progress.HumanSize(worstCase),
		progress.HumanSize(largeFileMax),
		progress.HumanSize(smallFileMax),
		progress.HumanSize(alreadyUsed),
	)

	if worstCase > maxMemoryBytes {
		return fmt.Errorf(
			"内存安全检查失败：预估最坏情况占用 %s，超过上限 %s\n"+
				"  建议：减小 -chunk（当前 %dMB）、-concurrency（当前 %d）或 -obj-concurrency（当前 %d）",
			progress.HumanSize(worstCase),
			progress.HumanSize(maxMemoryBytes),
			chunkMB, concurrency, objConcurrency,
		)
	}
	return nil
}

// copySingleObject 拷贝单个对象，自动按大小选择 PutObject 或 Multipart
// srcGet: 按 offset/size 获取数据的函数
// srcGetAll: 一次性获取全部数据的函数
func copySingleObject(
	ctx context.Context,
	key string, size int64, chunkSize int64, chunkConcurrency int,
	srcGetAll func() ([]byte, error),
	srcGetRange func(offset, size int64) ([]byte, error),
	prog *progress.Tracker,
	dstPut func(data []byte) error,
	dstMultipart func() error,
) error {
	if size <= chunkSize {
		// 小文件：一次性读取 + PutObject
		data, err := srcGetAll()
		if err != nil {
			return err
		}
		if err := dstPut(data); err != nil {
			return err
		}
		prog.Add(size)
		return nil
	}
	// 大文件：Multipart Upload
	return dstMultipart()
}

// CopyCOSToCOS 拷贝单个对象 COS → COS，自动选择模式
func CopyCOSToCOS(ctx context.Context,
	src *cosClient.Client, srcKey string,
	dst *cosClient.Client, dstKey string,
	size, chunkSize int64, chunkConcurrency int,
	prog *progress.Tracker,
) error {
	return copySingleObject(ctx, srcKey, size, chunkSize, chunkConcurrency,
		func() ([]byte, error) { return src.GetAll(ctx, srcKey) },
		func(offset, sz int64) ([]byte, error) { return src.GetRange(ctx, srcKey, offset, offset+sz-1) },
		prog,
		func(data []byte) error { return dst.PutObject(ctx, dstKey, data) },
		func() error { return dst.CopyFromCOS(ctx, dstKey, src, srcKey, size, chunkSize, chunkConcurrency, prog) },
	)
}

// CopyS3ToCOS 拷贝单个对象 S3 → COS，自动选择模式
func CopyS3ToCOS(ctx context.Context,
	src *s3Client.Client, srcKey string,
	dst *cosClient.Client, dstKey string,
	size, chunkSize int64, chunkConcurrency int,
	prog *progress.Tracker,
) error {
	return copySingleObject(ctx, srcKey, size, chunkSize, chunkConcurrency,
		func() ([]byte, error) { return src.GetAll(ctx, srcKey) },
		func(offset, sz int64) ([]byte, error) { return src.GetRange(ctx, srcKey, offset, offset+sz-1) },
		prog,
		func(data []byte) error { return dst.PutObject(ctx, dstKey, data) },
		func() error {
			return dst.MultipartUpload(ctx, dstKey, size, chunkSize, chunkConcurrency,
				func(_ int, offset, sz int64) ([]byte, error) {
					data, err := src.GetRange(ctx, srcKey, offset, offset+sz-1)
					if err == nil {
						prog.Add(sz)
					}
					return data, err
				},
			)
		},
	)
}

// CopyCOSToS3 拷贝单个对象 COS → S3，自动选择模式
func CopyCOSToS3(ctx context.Context,
	src *cosClient.Client, srcKey string,
	dst *s3Client.Client, dstKey string,
	size, chunkSize int64, chunkConcurrency int,
	prog *progress.Tracker,
) error {
	return copySingleObject(ctx, srcKey, size, chunkSize, chunkConcurrency,
		func() ([]byte, error) { return src.GetAll(ctx, srcKey) },
		func(offset, sz int64) ([]byte, error) { return src.GetRange(ctx, srcKey, offset, offset+sz-1) },
		prog,
		func(data []byte) error { return dst.PutObject(ctx, dstKey, data) },
		func() error {
			return dst.MultipartUpload(ctx, dstKey, size, chunkSize, chunkConcurrency,
				func(_ int, offset, sz int64) ([]byte, error) {
					data, err := src.GetRange(ctx, srcKey, offset, offset+sz-1)
					if err == nil {
						prog.Add(sz)
					}
					return data, err
				},
			)
		},
	)
}
