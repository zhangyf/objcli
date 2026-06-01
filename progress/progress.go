package progress

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"golang.org/x/term"
)

func HumanSize(n int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	f := float64(n)
	for _, u := range units {
		if f < 1024 {
			return fmt.Sprintf("%.1f %s", f, u)
		}
		f /= 1024
	}
	return fmt.Sprintf("%.1f PB", f)
}

// formatETA 把秒数格式化成 1d2h / 3h4m / 12m34s / 56s 形式。
func formatETA(secs float64) string {
	if secs <= 0 || secs > 365*24*3600 {
		return "--"
	}
	s := int64(secs + 0.5)
	switch {
	case s >= 24*3600:
		d := s / (24 * 3600)
		h := (s % (24 * 3600)) / 3600
		return fmt.Sprintf("%dd%dh", d, h)
	case s >= 3600:
		h := s / 3600
		m := (s % 3600) / 60
		return fmt.Sprintf("%dh%dm", h, m)
	case s >= 60:
		return fmt.Sprintf("%dm%ds", s/60, s%60)
	default:
		return fmt.Sprintf("%ds", s)
	}
}

// Mode 控制进度展示风格。
type Mode int

const (
	// ModeAuto 自动判断：stderr 为 TTY → Inline，否则 Log。
	ModeAuto Mode = iota
	// ModeInline 单行 \r 刷新到 stderr，刷新频率 ~1s。
	ModeInline
	// ModeLog 按 10 秒间隔通过 log.Printf 输出（旧行为）。
	ModeLog
	// ModeQuiet 完全静默。
	ModeQuiet
)

// 全局默认模式（main.go 启动时根据 -q / 是否 TTY 设置）。
var (
	defaultModeMu sync.RWMutex
	defaultMode   = ModeAuto
)

// SetDefaultMode 设置全局默认进度模式（New 未显式传 mode 时使用）。
func SetDefaultMode(m Mode) {
	defaultModeMu.Lock()
	defaultMode = m
	defaultModeMu.Unlock()
}

// resolveMode 把 ModeAuto 解析成 Inline 或 Log（基于 stderr 是否 TTY）。
func resolveMode(m Mode) Mode {
	if m != ModeAuto {
		return m
	}
	if term.IsTerminal(int(os.Stderr.Fd())) {
		return ModeInline
	}
	return ModeLog
}

type Tracker struct {
	total     int64
	uploaded  int64
	mu        sync.Mutex
	startTime time.Time
	done      chan struct{}
	mode      Mode
	out       io.Writer // inline 模式输出位置（默认 stderr）
	prefix    string    // inline 模式前缀，例如对象名
}

// New 用全局默认模式创建 Tracker。
func New(total int64) *Tracker {
	defaultModeMu.RLock()
	m := defaultMode
	defaultModeMu.RUnlock()
	return NewWithMode(total, m)
}

// NewWithMode 显式指定模式创建 Tracker。
func NewWithMode(total int64, m Mode) *Tracker {
	p := &Tracker{
		total:     total,
		startTime: time.Now(),
		done:      make(chan struct{}),
		mode:      resolveMode(m),
		out:       os.Stderr,
	}
	switch p.mode {
	case ModeQuiet:
		// 不开 ticker
	case ModeInline:
		go p.runInline()
	case ModeLog:
		go p.runLog()
	}
	return p
}

// SetPrefix 设置 inline 模式前缀（如 "[s3→cos] file.txt"），其它模式下无效。
func (p *Tracker) SetPrefix(s string) {
	p.mu.Lock()
	p.prefix = s
	p.mu.Unlock()
}

func (p *Tracker) runLog() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.mu.Lock()
			ub := p.uploaded
			st := p.startTime
			p.mu.Unlock()
			elapsed := time.Since(st).Seconds()
			speed := float64(ub) / elapsed
			pct := float64(ub) / float64(p.total) * 100
			eta := "--"
			if speed > 0 && p.total > ub {
				eta = formatETA(float64(p.total-ub) / speed)
			}
			log.Printf("进度: %s / %s (%.1f%%) | 速度: %s/s | 耗时: %.0fs | ETA: %s",
				HumanSize(ub), HumanSize(p.total), pct, HumanSize(int64(speed)), elapsed, eta)
		case <-p.done:
			return
		}
	}
}

func (p *Tracker) runInline() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.renderInline(false)
		case <-p.done:
			// 收尾：输出最后一行 + 换行，避免下一行覆盖
			p.renderInline(true)
			return
		}
	}
}

func (p *Tracker) renderInline(final bool) {
	p.mu.Lock()
	ub := p.uploaded
	st := p.startTime
	prefix := p.prefix
	p.mu.Unlock()
	elapsed := time.Since(st).Seconds()
	speed := float64(ub) / elapsed
	pct := 0.0
	if p.total > 0 {
		pct = float64(ub) / float64(p.total) * 100
	}
	eta := "--"
	if speed > 0 && p.total > ub {
		eta = formatETA(float64(p.total-ub) / speed)
	}
	bar := renderBar(pct, 24)
	line := fmt.Sprintf("%s %s %s/%s (%.1f%%) | %s/s | ETA: %s",
		prefix, bar,
		HumanSize(ub), HumanSize(p.total), pct,
		HumanSize(int64(speed)), eta,
	)
	// 用 \r 刷新；末尾补几个空格清理短于上一次渲染的尾巴
	if final {
		fmt.Fprintf(p.out, "\r%s          \n", line)
	} else {
		fmt.Fprintf(p.out, "\r%s          ", line)
	}
}

func renderBar(pct float64, width int) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := int(pct / 100 * float64(width))
	if filled > width {
		filled = width
	}
	bar := make([]byte, 0, width+2)
	bar = append(bar, '[')
	for i := 0; i < width; i++ {
		if i < filled {
			bar = append(bar, '=')
		} else if i == filled {
			bar = append(bar, '>')
		} else {
			bar = append(bar, ' ')
		}
	}
	bar = append(bar, ']')
	return string(bar)
}

func (p *Tracker) Add(n int64) {
	p.mu.Lock()
	p.uploaded += n
	p.mu.Unlock()
}

// Reset 重置已传输字节数为 0，用于"失败后重试走另一条路径"场景。
func (p *Tracker) Reset() {
	p.mu.Lock()
	p.uploaded = 0
	p.startTime = time.Now()
	p.mu.Unlock()
}

func (p *Tracker) Stop() {
	close(p.done)
}

// Progress 返回当前已上传字节数和总字节数，供外部进度回调使用
func (p *Tracker) Progress() (uploaded, total int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.uploaded, p.total
}
