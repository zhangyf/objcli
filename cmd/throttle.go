package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Limiter 是一个简单的 token bucket：
//   - capacity 等于 rate（最大允许 1 秒突发）
//   - 没有依赖第三方包；在多 goroutine 间安全
//
// rate <= 0 表示不限速，所有 Wait 会立即返回。
type Limiter struct {
	rate     float64 // bytes per second
	capacity float64

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// NewLimiter 构造限速器。rate 单位 bytes/sec。rate <= 0 表示不限速。
func NewLimiter(rate float64) *Limiter {
	return &Limiter{
		rate:     rate,
		capacity: rate,
		tokens:   rate, // 启动时给一桶
		last:     time.Now(),
	}
}

// Wait 阻塞直到累计获取到 n 字节配额，或 ctx 取消。
// 当 rate <= 0 时立即返回 nil。
func (l *Limiter) Wait(ctx context.Context, n int) error {
	if l == nil || l.rate <= 0 || n <= 0 {
		return nil
	}
	for {
		l.mu.Lock()
		now := time.Now()
		l.tokens += now.Sub(l.last).Seconds() * l.rate
		if l.tokens > l.capacity {
			l.tokens = l.capacity
		}
		l.last = now

		if l.tokens >= float64(n) {
			l.tokens -= float64(n)
			l.mu.Unlock()
			return nil
		}
		// 不够：算出还差多少 token，按 rate 推算等待时长
		need := float64(n) - l.tokens
		sleep := time.Duration(need / l.rate * float64(time.Second))
		l.mu.Unlock()

		// 防止理论上极端值
		if sleep <= 0 {
			sleep = 5 * time.Millisecond
		}
		// 单次最多睡 1 秒，便于响应 ctx
		if sleep > time.Second {
			sleep = time.Second
		}
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// throttledReader 包装 io.Reader，每次 Read 后阻塞限流。
type throttledReader struct {
	r   io.Reader
	lim *Limiter
	ctx context.Context
}

func (tr *throttledReader) Read(p []byte) (int, error) {
	n, err := tr.r.Read(p)
	if n > 0 {
		if werr := tr.lim.Wait(tr.ctx, n); werr != nil {
			return n, werr
		}
	}
	return n, err
}

// ThrottleReader 把 r 包成限速版本。lim 为 nil 或 rate<=0 时直接返回 r。
func ThrottleReader(ctx context.Context, r io.Reader, lim *Limiter) io.Reader {
	if lim == nil || lim.rate <= 0 || r == nil {
		return r
	}
	return &throttledReader{r: r, lim: lim, ctx: ctx}
}

// ParseRate 解析人类可读速率，如 "10MB/s"、"100KiB/s"、"1Gbps"、"500K"。
//
// 支持单位（大小写不敏感）：
//   - 数据率：B/s, K/KB/KiB, M/MB/MiB, G/GB/GiB, T/TB/TiB
//   - 网络率：Kbps, Mbps, Gbps（按 1000 进制，再 /8 转成字节）
//   - 后缀 /s、/sec 可省略；纯数字按 byte/sec 处理
//
// 0 / 空字符串 / "0" → 返回 0（表示不限速）；负数报错。
func ParseRate(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	low := strings.ToLower(s)
	low = strings.TrimSuffix(low, "/s")
	low = strings.TrimSuffix(low, "/sec")
	low = strings.TrimSpace(low)

	// 拆数字 + 单位
	cut := 0
	for cut < len(low) {
		c := low[cut]
		if (c >= '0' && c <= '9') || c == '.' {
			cut++
			continue
		}
		break
	}
	numStr := strings.TrimSpace(low[:cut])
	unit := strings.TrimSpace(low[cut:])

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid rate %q: %w", s, err)
	}
	if num < 0 {
		return 0, errors.New("rate must be >= 0")
	}

	mult := 1.0
	switch unit {
	case "", "b":
		mult = 1
	case "k", "kb":
		mult = 1000
	case "kib":
		mult = 1024
	case "m", "mb":
		mult = 1000 * 1000
	case "mib":
		mult = 1024 * 1024
	case "g", "gb":
		mult = 1000 * 1000 * 1000
	case "gib":
		mult = 1024 * 1024 * 1024
	case "t", "tb":
		mult = 1000 * 1000 * 1000 * 1000
	case "tib":
		mult = 1024 * 1024 * 1024 * 1024
	case "kbps":
		mult = 1000 / 8.0
	case "mbps":
		mult = 1000 * 1000 / 8.0
	case "gbps":
		mult = 1000 * 1000 * 1000 / 8.0
	default:
		return 0, fmt.Errorf("unknown rate unit %q in %q", unit, s)
	}
	return num * mult, nil
}
