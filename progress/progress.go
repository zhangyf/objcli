package progress

import (
	"fmt"
	"log"
	"sync"
	"time"
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

type Tracker struct {
	total     int64
	uploaded  int64
	mu        sync.Mutex
	startTime time.Time
	done      chan struct{}
}

func New(total int64) *Tracker {
	p := &Tracker{
		total:     total,
		startTime: time.Now(),
		done:      make(chan struct{}),
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.mu.Lock()
				ub := p.uploaded
				p.mu.Unlock()
				elapsed := time.Since(p.startTime).Seconds()
				speed := float64(ub) / elapsed
				pct := float64(ub) / float64(p.total) * 100
				log.Printf("进度: %s / %s (%.1f%%) | 速度: %s/s | 耗时: %.0fs",
					HumanSize(ub), HumanSize(p.total), pct, HumanSize(int64(speed)), elapsed)
			case <-p.done:
				return
			}
		}
	}()
	return p
}

func (p *Tracker) Add(n int64) {
	p.mu.Lock()
	p.uploaded += n
	p.mu.Unlock()
}

func (p *Tracker) Stop() {
	close(p.done)
}
