package cmd

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/zhangyf/objstore"
)

// ObjectString 解析后的对象地址或通配符模式
type ObjectString struct {
	StorageType objstore.ProviderType
	Bucket      string
	Region      string
	Key         string          // 完整 key 或带 * 的模式
	Prefix      string          // 实际用于 ListObjects 的前缀（去掉 * 后）
	IsGlob      bool            // 是否包含通配符 *
	Raw         string          // 原始字符串
}

// 匹配 COS URL: https://<bucket>.cos.<region>.myqcloud.com/<key> 和 https://<bucket>.cos-internal.<region>.tencentcos.cn/<key>
var cosURLRe = regexp.MustCompile(`^https?://([^.]+)\.cos(?:-internal)?\.([^/]+)/(.+)$`)

// 匹配 S3 path-style: https://s3.<region>.amazonaws.com/<bucket>/<key>
var s3PathRe = regexp.MustCompile(`^https?://s3\.([^.]+)\.amazonaws\.com/([^/]+)/(.+)$`)

// 匹配 S3 virtual-hosted-style: https://<bucket>.s3.<region>.amazonaws.com/<key>
var s3VHostRe = regexp.MustCompile(`^https?://([^.]+)\.s3\.([^.]+)\.amazonaws\.com/(.+)$`)

// ParseObjectString 解析对象地址或通配符模式（支持 s3://... 格式）
func ParseObjectString(raw string) (*ObjectString, error) {
	// 先尝试旧格式的 HTTP/HTTPS URL
	if m := cosURLRe.FindStringSubmatch(raw); m != nil {
		domain := m[2]
		if dotIdx := strings.Index(domain, "."); dotIdx > 0 {
			domain = domain[:dotIdx]
		}
		return parseKey(m[3], objstore.ProviderCOS, m[1], domain, raw)
	}
	if m := s3VHostRe.FindStringSubmatch(raw); m != nil {
		return parseKey(m[3], objstore.ProviderS3, m[1], m[2], raw)
	}
	if m := s3PathRe.FindStringSubmatch(raw); m != nil {
		return parseKey(m[3], objstore.ProviderS3, m[2], m[1], raw)
	}
	
	// 尝试新格式：<type>://<bucket>.<endpoint>/<key>
	if strings.Contains(raw, "://") {
		parts := strings.SplitN(raw, "://", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("无效的格式: %s", raw)
		}
		typeStr := strings.ToLower(parts[0])
		rest := parts[1]
		
		// 解析 bucket 和 endpoint
		dotIdx := strings.Index(rest, ".")
		if dotIdx == -1 {
			return nil, fmt.Errorf("缺少 endpoint: %s", raw)
		}
		bucket := rest[:dotIdx]
		rest = rest[dotIdx+1:]
		
		// 解析 region（第一个点前的内容）
		slashIdx := strings.Index(rest, "/")
		if slashIdx == -1 {
			return nil, fmt.Errorf("缺少 key 部分: %s", raw)
		}
		endpoint := rest[:slashIdx]
		key := rest[slashIdx+1:]
		
		// 从 endpoint 提取 region
		region := endpoint
		if dotIdx := strings.Index(endpoint, "."); dotIdx > 0 {
			region = endpoint[:dotIdx]
		}
		
		var provider objstore.ProviderType
		switch typeStr {
		case "cos":
			provider = objstore.ProviderCOS
		case "s3":
			provider = objstore.ProviderS3
		default:
			return nil, fmt.Errorf("不支持的存储类型: %s", typeStr)
		}
		
		return parseKey(key, provider, bucket, region, raw)
	}
	
	return nil, fmt.Errorf("无法识别的格式: %s", raw)
}

// parseKey 解析 key 部分，处理通配符
func parseKey(key string, provider objstore.ProviderType, bucket, region, raw string) (*ObjectString, error) {
	isGlob := strings.Contains(key, "*")
	prefix := key
	if isGlob {
		// 提取 * 之前的部分作为前缀
		starIdx := strings.Index(key, "*")
		if starIdx > 0 {
			prefix = key[:starIdx]
		} else {
			prefix = ""
		}
	}
	
	return &ObjectString{
		StorageType: provider,
		Bucket:      bucket,
		Region:      region,
		Key:         key,
		Prefix:      prefix,
		IsGlob:      isGlob,
		Raw:         raw,
	}, nil
}

// ParseObjURL 兼容旧函数名
func ParseObjURL(raw string) (*ObjectString, error) {
	return ParseObjectString(raw)
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
