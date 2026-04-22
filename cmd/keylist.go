package cmd

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"

	"objutil/storage"
)

// ObjURL 解析后的对象地址
type ObjURL struct {
	StorageType storage.StorageType
	Bucket      string
	Region      string
	Key         string
	RawURL      string
}

// 匹配 COS URL: https://<bucket>.cos.<region>.myqcloud.com/<key>
var cosURLRe = regexp.MustCompile(`^https?://([^.]+)\.cos\.([^.]+\.[^.]+)\.myqcloud\.com/(.+)$`)

// 匹配 S3 path-style: https://s3.<region>.amazonaws.com/<bucket>/<key>
var s3PathRe = regexp.MustCompile(`^https?://s3\.([^.]+)\.amazonaws\.com/([^/]+)/(.+)$`)

// 匹配 S3 virtual-hosted-style: https://<bucket>.s3.<region>.amazonaws.com/<key>
var s3VHostRe = regexp.MustCompile(`^https?://([^.]+)\.s3\.([^.]+)\.amazonaws\.com/(.+)$`)

// ParseObjURL 解析对象 URL
func ParseObjURL(raw string) (*ObjURL, error) {
	if m := cosURLRe.FindStringSubmatch(raw); m != nil {
		return &ObjURL{
			StorageType: storage.StorageTypeCOS,
			Bucket:      m[1],
			Region:      m[2],
			Key:         m[3],
			RawURL:      raw,
		}, nil
	}
	if m := s3VHostRe.FindStringSubmatch(raw); m != nil {
		return &ObjURL{
			StorageType: storage.StorageTypeS3,
			Bucket:      m[1],
			Region:      m[2],
			Key:         m[3],
			RawURL:      raw,
		}, nil
	}
	if m := s3PathRe.FindStringSubmatch(raw); m != nil {
		return &ObjURL{
			StorageType: storage.StorageTypeS3,
			Bucket:      m[2],
			Region:      m[1],
			Key:         m[3],
			RawURL:      raw,
		}, nil
	}
	return nil, fmt.Errorf("无法识别的对象 URL: %s", raw)
}

// loadURLList 从本地文件或 HTTP/HTTPS URL 加载对象 URL 列表
// 每行一个 URL，自动跳过空行和 # 注释行
func loadURLList(source string) ([]string, error) {
	var r io.Reader

	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		resp, err := http.Get(source)
		if err != nil {
			return nil, fmt.Errorf("获取远程列表失败: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("远程列表返回 HTTP %d", resp.StatusCode)
		}
		r = resp.Body
	} else {
		f, err := os.Open(source)
		if err != nil {
			return nil, fmt.Errorf("打开本地列表失败: %w", err)
		}
		defer f.Close()
		r = f
	}

	var lines []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取列表失败: %w", err)
	}
	return lines, nil
}
