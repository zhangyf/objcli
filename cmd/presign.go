package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/zhangyf/objstore"
)

// PresignConfig 预签名 URL 配置
type PresignConfig struct {
	Key     string
	Method  string // GET | PUT
	Expires time.Duration
}

// Presign 生成预签名 URL
func Presign(ctx context.Context, store objstore.Store, cfg PresignConfig) (string, error) {
	method := strings.ToUpper(cfg.Method)
	if method == "" {
		method = "GET"
	}
	if cfg.Expires <= 0 {
		cfg.Expires = time.Hour
	}
	switch method {
	case "GET":
		return store.PresignGetObject(ctx, cfg.Key, cfg.Expires)
	case "PUT":
		return store.PresignPutObject(ctx, cfg.Key, cfg.Expires)
	default:
		return "", fmt.Errorf("不支持的 method: %s（仅支持 GET / PUT）", method)
	}
}