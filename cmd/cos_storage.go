package cmd

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

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

// COSStorage 腾讯云 COS 的 Storage 实现
type COSStorage struct {
	inner  *cos.Client
	bucket string
	region string
}

func NewCOSStorage(secretID, secretKey, bucket, region string) *COSStorage {
	u, _ := url.Parse(fmt.Sprintf("https://%s.cos.%s.myqcloud.com", bucket, region))
	inner := cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{
		Timeout: 0,
		Transport: &cos.AuthorizationTransport{
			SecretID:  secretID,
			SecretKey: secretKey,
		},
	})
	return &COSStorage{inner: inner, bucket: bucket, region: region}
}

func (c *COSStorage) Type() string       { return "cos" }
func (c *COSStorage) BucketName() string { return c.bucket }

func (c *COSStorage) HeadObject(ctx context.Context, key string) (int64, error) {
	resp, err := c.inner.Object.Head(ctx, key, nil)
	if err != nil {
		return 0, err
	}
	return resp.ContentLength, nil
}

func (c *COSStorage) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	resp, err := c.inner.Object.Get(ctx, key, &cos.ObjectGetOptions{
		Range: fmt.Sprintf("bytes=%d-%d", start, end),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c *COSStorage) GetAll(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.inner.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c *COSStorage) PutObject(ctx context.Context, key string, data []byte) error {
	_, err := c.inner.Object.Put(ctx, key, bytes.NewReader(data), nil)
	return err
}

func (c *COSStorage) MultipartUpload(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
	fetchPart func(partNumber int, offset, size int64) ([]byte, error)) error {

	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	initResp, _, err := c.inner.Object.InitiateMultipartUpload(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("InitiateMultipartUpload: %w", err)
	}
	uploadID := initResp.UploadID
	abort := func() { c.inner.Object.AbortMultipartUpload(ctx, key, uploadID) }

	type partResult struct {
		partNumber int
		etag       string
		err        error
	}

	jobs := make(chan int, concurrency*2)
	results := make(chan partResult, totalParts)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pn := range jobs {
				offset := int64(pn-1) * chunkSize
				size := chunkSize
				if offset+size > totalSize {
					size = totalSize - offset
				}
				data, err := fetchPart(pn, offset, size)
				if err != nil {
					results <- partResult{pn, "", fmt.Errorf("fetch part %d: %w", pn, err)}
					continue
				}
				resp, err := c.inner.Object.UploadPart(ctx, key, uploadID, pn, bytes.NewReader(data), nil)
				if err != nil {
					results <- partResult{pn, "", fmt.Errorf("UploadPart %d: %w", pn, err)}
					continue
				}
				log.Printf("[Part %d/%d] 完成", pn, totalParts)
				results <- partResult{pn, resp.Header.Get("ETag"), nil}
			}
		}()
	}
	go func() {
		for i := 1; i <= totalParts; i++ {
			jobs <- i
		}
		close(jobs)
	}()
	go func() { wg.Wait(); close(results) }()

	var parts []cos.Object
	hasErr := false
	for r := range results {
		if r.err != nil {
			log.Printf("[ERROR] %v", r.err)
			hasErr = true
			continue
		}
		parts = append(parts, cos.Object{PartNumber: r.partNumber, ETag: r.etag})
	}
	if hasErr {
		abort()
		return fmt.Errorf("存在失败分块")
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	_, _, err = c.inner.Object.CompleteMultipartUpload(ctx, key, uploadID, &cos.CompleteMultipartUploadOptions{Parts: parts})
	if err != nil {
		abort()
		return fmt.Errorf("CompleteMultipartUpload: %w", err)
	}
	return nil
}

func (c *COSStorage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	var marker string
	for {
		resp, _, err := c.inner.Bucket.Get(ctx, &cos.BucketGetOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return nil, fmt.Errorf("Bucket.Get: %w", err)
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

// CopyPartFromCOS 使用 UploadPart-Copy 从同云内复制分块（不过本机）
// 当 src 和 dst 都是 COS 时优先使用
func (c *COSStorage) CopyPartFrom(ctx context.Context, dstKey string, src *COSStorage, srcKey string,
	totalSize, chunkSize int64, concurrency int) error {

	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	srcURL := fmt.Sprintf("%s.cos.%s.myqcloud.com/%s", src.bucket, src.region, srcKey)

	initResp, _, err := c.inner.Object.InitiateMultipartUpload(ctx, dstKey, nil)
	if err != nil {
		return fmt.Errorf("InitiateMultipartUpload: %w", err)
	}
	uploadID := initResp.UploadID
	abort := func() { c.inner.Object.AbortMultipartUpload(ctx, dstKey, uploadID) }

	type partResult struct {
		partNumber int
		etag       string
		err        error
	}

	jobs := make(chan int, concurrency*2)
	results := make(chan partResult, totalParts)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pn := range jobs {
				start := int64(pn-1) * chunkSize
				end := start + chunkSize - 1
				if end >= totalSize {
					end = totalSize - 1
				}
				opt := &cos.ObjectCopyPartOptions{
					XCosCopySource:      srcURL,
					XCosCopySourceRange: fmt.Sprintf("bytes=%d-%d", start, end),
				}
				resp, _, err := c.inner.Object.CopyPart(ctx, dstKey, uploadID, pn, srcURL, opt)
				if err != nil {
					results <- partResult{pn, "", fmt.Errorf("CopyPart %d: %w", pn, err)}
					continue
				}
				log.Printf("[Part %d/%d] 完成", pn, totalParts)
				results <- partResult{pn, resp.ETag, nil}
			}
		}()
	}
	go func() {
		for i := 1; i <= totalParts; i++ {
			jobs <- i
		}
		close(jobs)
	}()
	go func() { wg.Wait(); close(results) }()

	var parts []cos.Object
	hasErr := false
	for r := range results {
		if r.err != nil {
			log.Printf("[ERROR] %v", r.err)
			hasErr = true
			continue
		}
		parts = append(parts, cos.Object{PartNumber: r.partNumber, ETag: r.etag})
	}
	if hasErr {
		abort()
		return fmt.Errorf("存在失败分块")
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	_, _, err = c.inner.Object.CompleteMultipartUpload(ctx, dstKey, uploadID, &cos.CompleteMultipartUploadOptions{Parts: parts})
	if err != nil {
		abort()
		return fmt.Errorf("CompleteMultipartUpload: %w", err)
	}
	return nil
}
