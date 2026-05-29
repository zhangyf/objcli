package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

// RetryConfig 控制对象存储调用的重试退避行为。
//
// Attempts <= 0 时退化为"只跑一次"；BaseDelay <= 0 时使用 200ms 默认值。
type RetryConfig struct {
	Attempts  int           // 最大尝试次数（含首次），1 表示不重试
	BaseDelay time.Duration // 首次退避基准；指数增长上限封到 BaseDelay*32
}

// Default 返回默认重试配置。
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{Attempts: 3, BaseDelay: 200 * time.Millisecond}
}

// Sanitize 把零值/越界值修正成合理默认。
func (c RetryConfig) Sanitize() RetryConfig {
	if c.Attempts <= 0 {
		c.Attempts = 1
	}
	if c.BaseDelay <= 0 {
		c.BaseDelay = 200 * time.Millisecond
	}
	return c
}

// IsRetryableErr 判断错误是否值得重试。
//
// 当前策略：
//   - context.Canceled / context.DeadlineExceeded → 不重试（用户主动 / 全局超时）
//   - net.Error 且 Timeout 或 Temporary → 重试
//   - 错误字符串里含 "connection reset" / "EOF" / "broken pipe" / "i/o timeout" → 重试
//   - 含明显的 5xx HTTP 状态码字眼 → 重试
//   - 其他默认不重试
//
// 该判断保守是为了"不抑制业务错误"：4xx、NoSuchKey 这类不重试。
func IsRetryableErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		if ne.Timeout() {
			return true
		}
		// net.Error.Temporary 已被弃用但许多 SDK 仍实现，用反射式判定
		type temporary interface{ Temporary() bool }
		if t, ok := any(ne).(temporary); ok && t.Temporary() {
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	for _, k := range []string{
		"connection reset",
		"broken pipe",
		"i/o timeout",
		"no such host",
		"tls handshake timeout",
		"http2: server sent goaway",
		"unexpected eof",
		"use of closed network connection",
		"500 ", "502 ", "503 ", "504 ",
		"slowdown",
		"requesttimeout",
		"servicenotavailable",
	} {
		if strings.Contains(msg, k) {
			return true
		}
	}
	return false
}

// Retry 执行 fn 并按指数退避重试可重试错误。
//
// onRetry 可为 nil；非 nil 时在每次重试前回调，便于打印日志/进度。
func Retry(ctx context.Context, cfg RetryConfig, op string,
	fn func(ctx context.Context) error,
	onRetry func(attempt int, err error, sleep time.Duration),
) error {
	cfg = cfg.Sanitize()
	var lastErr error
	for attempt := 1; attempt <= cfg.Attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == cfg.Attempts || !IsRetryableErr(err) {
			break
		}
		// exponential backoff: base * 2^(attempt-1), 封顶 base*32
		shift := attempt - 1
		if shift > 5 {
			shift = 5
		}
		sleep := cfg.BaseDelay << shift
		if onRetry != nil {
			onRetry(attempt, err, sleep)
		}
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("%s: %w", op, lastErr)
}
