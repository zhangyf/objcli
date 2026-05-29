package cmd

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseRate(t *testing.T) {
	cases := []struct {
		in   string
		want float64
		err  bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"100", 100, false},
		{"10MB/s", 10_000_000, false},
		{"10mb/s", 10_000_000, false},
		{"10MiB/s", 10 * 1024 * 1024, false},
		{"100KiB/s", 100 * 1024, false},
		{"1Gbps", 1_000_000_000.0 / 8.0, false},
		{"500K", 500_000, false},
		{"500KiB", 500 * 1024, false},
		{"-1MB/s", 0, true},
		{"abc", 0, true},
		{"10XYZ", 0, true},
	}
	for _, c := range cases {
		got, err := ParseRate(c.in)
		if c.err {
			if err == nil {
				t.Errorf("ParseRate(%q) expected error, got %v", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseRate(%q) unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseRate(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestLimiterRate 验证限速近似准确：以 1MB/s 限速读 2MB，应耗时约 1s。
func TestLimiterRate(t *testing.T) {
	rate := 1_000_000.0 // 1MB/s
	lim := NewLimiter(rate)
	// 把启动桶放掉
	if err := lim.Wait(context.Background(), int(rate)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	// 取 2MB
	for i := 0; i < 8; i++ {
		if err := lim.Wait(context.Background(), 250_000); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	// 2MB ÷ 1MB/s = 2s
	if elapsed < 1500*time.Millisecond || elapsed > 2500*time.Millisecond {
		t.Errorf("expected ~2s, got %v", elapsed)
	}
}

func TestLimiterDisabled(t *testing.T) {
	lim := NewLimiter(0)
	start := time.Now()
	if err := lim.Wait(context.Background(), 1<<30); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Errorf("disabled limiter blocked")
	}
}

func TestLimiterCancel(t *testing.T) {
	lim := NewLimiter(1) // 极慢
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := lim.Wait(ctx, 1<<20)
	if err == nil {
		t.Fatal("expected error after cancel")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected canceled, got %v", err)
	}
}

func TestRetryOnce(t *testing.T) {
	var calls int
	err := Retry(context.Background(), DefaultRetryConfig(), "op",
		func(ctx context.Context) error {
			calls++
			return nil
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestRetryOnRetryableError(t *testing.T) {
	var calls int
	cfg := RetryConfig{Attempts: 3, BaseDelay: 1 * time.Millisecond}
	err := Retry(context.Background(), cfg, "op",
		func(ctx context.Context) error {
			calls++
			if calls < 3 {
				return errors.New("connection reset by peer")
			}
			return nil
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestRetryNonRetryableError(t *testing.T) {
	var calls int
	cfg := RetryConfig{Attempts: 5, BaseDelay: 1 * time.Millisecond}
	err := Retry(context.Background(), cfg, "op",
		func(ctx context.Context) error {
			calls++
			return errors.New("NoSuchKey")
		}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Errorf("non-retryable should not retry; got %d calls", calls)
	}
	if !strings.Contains(err.Error(), "op:") {
		t.Errorf("error should be wrapped: %v", err)
	}
}

func TestRetryExhausted(t *testing.T) {
	var calls int
	cfg := RetryConfig{Attempts: 3, BaseDelay: 1 * time.Millisecond}
	err := Retry(context.Background(), cfg, "op",
		func(ctx context.Context) error {
			calls++
			return errors.New("connection reset")
		}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 3 {
		t.Errorf("expected 3 attempts, got %d", calls)
	}
}
