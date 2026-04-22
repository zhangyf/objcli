package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Client 封装 S3 操作
type Client struct {
	inner  *s3.Client
	Bucket string
	Region string
}

func New(ctx context.Context, accessKey, secretKey, region, bucket string) (*Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("初始化 S3 配置失败: %w", err)
	}
	return &Client{
		inner:  s3.NewFromConfig(cfg),
		Bucket: bucket,
		Region: region,
	}, nil
}

// HeadObject 获取对象大小
func (c *Client) HeadObject(ctx context.Context, key string) (int64, error) {
	resp, err := c.inner.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, err
	}
	return *resp.ContentLength, nil
}

// GetRange 按字节范围下载对象内容
func (c *Client) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	resp, err := c.inner.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// PartResult 单个分块的上传结果
type PartResult struct {
	PartNumber int
	ETag       string
	Err        error
}

// InitUpload 初始化分块上传，返回 uploadID
func (c *Client) InitUpload(ctx context.Context, key string) (string, error) {
	resp, err := c.inner.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("CreateMultipartUpload: %w", err)
	}
	return *resp.UploadId, nil
}

// UploadPart 上传单个分块
func (c *Client) UploadPart(ctx context.Context, key, uploadID string, partNumber int, data []byte) (string, error) {
	resp, err := c.inner.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(c.Bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(int32(partNumber)),
		Body:       bytes.NewReader(data),
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(resp.ETag), nil
}

// AbortUpload 取消分块上传
func (c *Client) AbortUpload(ctx context.Context, key, uploadID string) {
	c.inner.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(c.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
}

// CompleteUpload 完成分块上传
func (c *Client) CompleteUpload(ctx context.Context, key, uploadID string, parts []PartResult) error {
	s3Parts := make([]s3types.CompletedPart, len(parts))
	for i, p := range parts {
		s3Parts[i] = s3types.CompletedPart{
			PartNumber: aws.Int32(int32(p.PartNumber)),
			ETag:       aws.String(p.ETag),
		}
	}
	_, err := c.inner.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(c.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: s3Parts,
		},
	})
	if err != nil {
		return fmt.Errorf("CompleteMultipartUpload: %w", err)
	}
	return nil
}

// PutObject 小文件单次上传
func (c *Client) PutObject(ctx context.Context, key string, data []byte) error {
	_, err := c.inner.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("PutObject %s: %w", key, err)
	}
	return nil
}

// GetAll 一次性读取整个对象
func (c *Client) GetAll(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.inner.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("GetObject %s: %w", key, err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// ListObjects 列出指定前缀下所有对象的 Key（自动处理分页）
func (c *Client) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	var continuationToken *string
	for {
		resp, err := c.inner.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(c.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("ListObjectsV2: %w", err)
		}
		for _, obj := range resp.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		continuationToken = resp.NextContinuationToken
	}
	return keys, nil
}

// MultipartUpload 通用分块上传（通过 fetchPart 回调获取数据）
func (c *Client) MultipartUpload(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
	fetchPart func(partNumber int, offset, size int64) ([]byte, error),
) error {
	totalParts := int((totalSize + chunkSize - 1) / chunkSize)

	uploadID, err := c.InitUpload(ctx, key)
	if err != nil {
		return err
	}
	log.Printf("S3 UploadId: %s", uploadID)

	abort := func() { c.AbortUpload(ctx, key, uploadID) }

	results := runWorkers(ctx, totalParts, concurrency, func(partNumber int) PartResult {
		offset := int64(partNumber-1) * chunkSize
		size := chunkSize
		if offset+size > totalSize {
			size = totalSize - offset
		}
		data, err := fetchPart(partNumber, offset, size)
		if err != nil {
			return PartResult{PartNumber: partNumber, Err: fmt.Errorf("fetch part %d: %w", partNumber, err)}
		}
		etag, err := c.UploadPart(ctx, key, uploadID, partNumber, data)
		if err != nil {
			return PartResult{PartNumber: partNumber, Err: fmt.Errorf("UploadPart %d: %w", partNumber, err)}
		}
		log.Printf("[Part %d/%d] 完成", partNumber, totalParts)
		return PartResult{PartNumber: partNumber, ETag: etag}
	})

	partList, hasErr := collectResults(results)
	if hasErr {
		abort()
		return fmt.Errorf("存在失败分块")
	}

	err = c.CompleteUpload(ctx, key, uploadID, partList)
	if err != nil {
		abort()
		return err
	}
	return nil
}

// ---- 内部工具 ----

func runWorkers(ctx context.Context, totalParts, concurrency int, do func(int) PartResult) []PartResult {
	type job struct{ partNumber int }
	jobs := make(chan job, concurrency*2)
	out := make(chan PartResult, totalParts)

	go func() {
		for i := 1; i <= totalParts; i++ {
			jobs <- job{i}
		}
		close(jobs)
	}()

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				out <- do(j.partNumber)
			}
		}()
	}
	go func() { wg.Wait(); close(out) }()

	var results []PartResult
	for r := range out {
		results = append(results, r)
	}
	return results
}

func collectResults(results []PartResult) ([]PartResult, bool) {
	hasErr := false
	var partList []PartResult
	for _, r := range results {
		if r.Err != nil {
			log.Printf("[ERROR] %v", r.Err)
			hasErr = true
			continue
		}
		partList = append(partList, r)
	}
	sort.Slice(partList, func(i, j int) bool {
		return partList[i].PartNumber < partList[j].PartNumber
	})
	return partList, hasErr
}
