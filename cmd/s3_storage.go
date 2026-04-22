package cmd

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

// S3Storage AWS S3 的 Storage 实现
type S3Storage struct {
	inner  *s3.Client
	bucket string
	region string
}

func NewS3Storage(ctx context.Context, accessKey, secretKey, region, bucket string) (*S3Storage, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("初始化 S3 配置失败: %w", err)
	}
	return &S3Storage{inner: s3.NewFromConfig(cfg), bucket: bucket, region: region}, nil
}

func (s *S3Storage) Type() string       { return "s3" }
func (s *S3Storage) BucketName() string { return s.bucket }

func (s *S3Storage) HeadObject(ctx context.Context, key string) (int64, error) {
	resp, err := s.inner.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, err
	}
	return *resp.ContentLength, nil
}

func (s *S3Storage) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	resp, err := s.inner.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (s *S3Storage) GetAll(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.inner.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (s *S3Storage) PutObject(ctx context.Context, key string, data []byte) error {
	_, err := s.inner.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *S3Storage) MultipartUpload(ctx context.Context, key string, totalSize, chunkSize int64, concurrency int,
	fetchPart func(partNumber int, offset, size int64) ([]byte, error)) error {

	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	createResp, err := s.inner.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("CreateMultipartUpload: %w", err)
	}
	uploadID := *createResp.UploadId
	abort := func() {
		s.inner.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
		})
	}

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
				resp, err := s.inner.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:     aws.String(s.bucket),
					Key:        aws.String(key),
					UploadId:   aws.String(uploadID),
					PartNumber: aws.Int32(int32(pn)),
					Body:       bytes.NewReader(data),
				})
				if err != nil {
					results <- partResult{pn, "", fmt.Errorf("UploadPart %d: %w", pn, err)}
					continue
				}
				log.Printf("[Part %d/%d] 完成", pn, totalParts)
				results <- partResult{pn, aws.ToString(resp.ETag), nil}
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

	type pr struct{ pn int; etag string }
	var parts []pr
	hasErr := false
	for r := range results {
		if r.err != nil {
			log.Printf("[ERROR] %v", r.err)
			hasErr = true
			continue
		}
		parts = append(parts, pr{r.partNumber, r.etag})
	}
	if hasErr {
		abort()
		return fmt.Errorf("存在失败分块")
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].pn < parts[j].pn })

	s3Parts := make([]s3types.CompletedPart, len(parts))
	for i, p := range parts {
		s3Parts[i] = s3types.CompletedPart{
			PartNumber: aws.Int32(int32(p.pn)),
			ETag:       aws.String(p.etag),
		}
	}
	_, err = s.inner.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: s3Parts},
	})
	if err != nil {
		abort()
		return fmt.Errorf("CompleteMultipartUpload: %w", err)
	}
	return nil
}

func (s *S3Storage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	var continuationToken *string
	for {
		resp, err := s.inner.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
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
