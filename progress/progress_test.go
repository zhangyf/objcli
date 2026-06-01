package progress

import (
	"strings"
	"testing"
)

func TestFormatETA(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{-1, "--"},
		{0, "--"},
		{0.4, "0s"},
		{30, "30s"},
		{60, "1m0s"},
		{125, "2m5s"},
		{3600, "1h0m"},
		{3661, "1h1m"},
		{86400, "1d0h"},
		{86400 + 3600, "1d1h"},
		{400 * 24 * 3600, "--"}, // > 1 year
	}
	for _, tc := range cases {
		got := formatETA(tc.in)
		if got != tc.want {
			t.Errorf("formatETA(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestRenderBar(t *testing.T) {
	cases := []struct {
		pct      float64
		width    int
		wantHas  string
	}{
		{0, 10, "[>         ]"},
		{50, 10, "[=====>    ]"},
		{100, 10, "[==========]"},
		{-1, 10, "[>         ]"},
		{200, 10, "[==========]"},
	}
	for _, tc := range cases {
		got := renderBar(tc.pct, tc.width)
		if !strings.Contains(got, "[") || !strings.Contains(got, "]") {
			t.Errorf("renderBar(%v) = %q (missing brackets)", tc.pct, got)
		}
		if got != tc.wantHas {
			t.Errorf("renderBar(%v, %d) = %q, want %q", tc.pct, tc.width, got, tc.wantHas)
		}
	}
}

func TestQuietModeHasNoTicker(t *testing.T) {
	// ModeQuiet 下创建 Tracker 不应起 goroutine（行为校验通过 Stop 不阻塞）。
	tr := NewWithMode(100, ModeQuiet)
	tr.Add(50)
	tr.Stop()
}

func TestSetDefaultMode_Concurrent(t *testing.T) {
	// 并发改 mode 不应 race（GOMAXPROCS=1 也能跑过）
	for i := 0; i < 100; i++ {
		go SetDefaultMode(ModeAuto)
	}
	SetDefaultMode(ModeLog)
}
