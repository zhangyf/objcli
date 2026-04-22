package cos

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"sync"

	"objcopy/progress"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

// Client 封装 COS 操作
type Client struct {
	inner    *cos.Client
	Bucket   string
	Region   string
}

func New(secretID, secretKey, bucket, region string) *Client {
	u, _ := url.Parse(fmt.Sprintf("https://%s.cos.%s.myqcloud.com", bucket, region))
	inner := cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{
		Timeout: 0,
		Transport: &cos.AuthorizationTransport{
			SecretID:  secretID,
			SecretKey: secretKey,
		},
	})
	return &Client{inner: inner, Bucket: bucket, Region: region}
}

// HeadObject 获取对象大小
func (c *Client) HeadObject(ctx context.Context, key string) (int64, error) {
	resp, err := c.inner.Object.Head(ctx, key, nil)
	if err != nil {
		return 0, err
	}
	return resp.ContentLength, nil
}

// GetRange 按字节范围下载对象内容
func (c *Client) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	resp, err := c.inner.Object.Get(ctx, key, &cos.ObjectGetOptions{
		Range: fmt.Sprintf("bytes=%d-%d", start, end),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// URL 返回该对象的 host/key 形式（用于 CopyPart 的 XCosCopySource）
func (c *Client) URL(key string) string {
	return fmt.Sprintf("%s.cos.%s.myqcloud.com/%s", c.Bucket, c.Region, key)
}

// PartResult 单个分块的上传结果
type PartResult struct {
	PartNumber int
	ETag       string
	Err        error
}

// MultipartUpload 通用分块上传（通过 reader 上传数据流）
func (c *Client) MultipartUpload(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
	fetchPart func(partNumber int, offset, size int64) ([]byte, error),
) error {
	totalParts := int((totalSize + chunkSize - 1) / chunkSize)

	initResp, _, err := c.inner.Object.InitiateMultipartUpload(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("InitiateMultipartUpload: %w", err)
	}
	uploadID := initResp.UploadID
	log.Printf("COS UploadId: %s", uploadID)

	abort := func() { c.inner.Object.AbortMultipartUpload(ctx, key, uploadID) }

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
		resp, err := c.inner.Object.UploadPart(ctx, key, uploadID, partNumber, bytes.NewReader(data), nil)
		if err != nil {
			return PartResult{PartNumber: partNumber, Err: fmt.Errorf("UploadPart %d: %w", partNumber, err)}
		}
		log.Printf("[Part %d/%d] 完成", partNumber, totalParts)
		return PartResult{PartNumber: partNumber, ETag: resp.Header.Get("ETag")}
	})

	partList, hasErr := collectResults(results)
	if hasErr {
		abort()
		return fmt.Errorf("存在失败分块")
	}
	return c.completeUpload(ctx, key, uploadID, partList, abort)
}

// CopyFromCOS 使用 UploadPart-Copy 从另一个 COS 对象复制（不过本机）
func (c *Client) CopyFromCOS(ctx context.Context, dstKey string, src *Client, srcKey string,
	totalSize, chunkSize int64, concurrency int, prog *progress.Tracker,
) error {
	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	srcURL := src.URL(srcKey)

	initResp, _, err := c.inner.Object.InitiateMultipartUpload(ctx, dstKey, nil)
	if err != nil {
		return fmt.Errorf("InitiateMultipartUpload: %w", err)
	}
	uploadID := initResp.UploadID
	log.Printf("COS UploadId: %s", uploadID)

	abort := func() { c.inner.Object.AbortMultipartUpload(ctx, dstKey, uploadID) }

	results := runWorkers(ctx, totalParts, concurrency, func(partNumber int) PartResult {
		start := int64(partNumber-1) * chunkSize
		end := start + chunkSize - 1
		if end >= totalSize {
			end = totalSize - 1
		}
		opt := &cos.ObjectCopyPartOptions{
			XCosCopySource:      srcURL,
			XCosCopySourceRange: fmt.Sprintf("bytes=%d-%d", start, end),
		}
		resp, _, err := c.inner.Object.CopyPart(ctx, dstKey, uploadID, partNumber, srcURL, opt)
		if err != nil {
			return PartResult{PartNumber: partNumber, Err: fmt.Errorf("CopyPart %d: %w", partNumber, err)}
		}
		if prog != nil {
			prog.Add(end - start + 1)
		}
		log.Printf("[Part %d/%d] 完成", partNumber, totalParts)
		return PartResult{PartNumber: partNumber, ETag: resp.ETag}
	})

	partList, hasErr := collectResults(results)
	if hasErr {
		abort()
		return fmt.Errorf("存在失败分块")
	}
	return c.completeUpload(ctx, dstKey, uploadID, partList, abort)
}

// UploadPart 上传单个分块（供上层 cmd 使用）
func (c *Client) UploadPart(ctx context.Context, key, uploadID string, partNumber int, data []byte) (string, error) {
	resp, err := c.inner.Object.UploadPart(ctx, key, uploadID, partNumber, bytes.NewReader(data), nil)
	if err != nil {
		return "", err
	}
	return resp.Header.Get("ETag"), nil
}

// InitUpload 初始化分块上传，返回 uploadID
func (c *Client) InitUpload(ctx context.Context, key string) (string, error) {
	resp, _, err := c.inner.Object.InitiateMultipartUpload(ctx, key, nil)
	if err != nil {
		return "", err
	}
	return resp.UploadID, nil
}

// AbortUpload 取消分块上传
func (c *Client) AbortUpload(ctx context.Context, key, uploadID string) {
	c.inner.Object.AbortMultipartUpload(ctx, key, uploadID)
}

// CompleteUpload 完成分块上传
func (c *Client) CompleteUpload(ctx context.Context, key, uploadID string, parts []cos.Object) error {
	_, _, err := c.inner.Object.CompleteMultipartUpload(ctx, key, uploadID, &cos.CompleteMultipartUploadOptions{
		Parts: parts,
	})
	return err
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

func collectResults(results []PartResult) ([]cos.Object, bool) {
	hasErr := false
	var partList []cos.Object
	for _, r := range results {
		if r.Err != nil {
			log.Printf("[ERROR] %v", r.Err)
			hasErr = true
			continue
		}
		partList = append(partList, cos.Object{PartNumber: r.PartNumber, ETag: r.ETag})
	}
	sort.Slice(partList, func(i, j int) bool {
		return partList[i].PartNumber < partList[j].PartNumber
	})
	return partList, hasErr
}

func (c *Client) completeUpload(ctx context.Context, key, uploadID string, parts []cos.Object, abort func()) error {
	_, _, err := c.inner.Object.CompleteMultipartUpload(ctx, key, uploadID, &cos.CompleteMultipartUploadOptions{
		Parts: parts,
	})
	if err != nil {
		abort()
		return fmt.Errorf("CompleteMultipartUpload: %w", err)
	}
	return nil
}

// PutObject 小文件单次上传
func (c *Client) PutObject(ctx context.Context, key string, data []byte) error {
	_, err := c.inner.Object.Put(ctx, key, bytes.NewReader(data), nil)
	if err != nil {
		return fmt.Errorf("PutObject %s: %w", key, err)
	}
	return nil
}

// GetAll 一次性读取整个对象
func (c *Client) GetAll(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.inner.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("GetObject %s: %w", key, err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// ListObjects 列出指定前缀下所有对象的 Key（自动处理分页）
func (c *Client) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	var marker string
	for {
		resp, _, err := c.inner.Bucket.Get(ctx, &cos.BucketGetOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return nil, fmt.Errorf("Bucket.Get (list): %w", err)
		}
		for _, obj := range resp.Contents {
			keys = append(keys, obj.Key)
		}
		if !resp.IsTruncated {
			break
		}
		marker = resp.NextMarker
	}
	return keys, nil
}
