package cmd

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/zhangyf/objstore"
)

// ListConfig 列举配置
type ListConfig struct {
	Prefix    string       // 列举前缀
	Recursive bool         // 是否递归（递归时 Delimiter=""）
	Long      bool         // 是否长格式输出（默认就是长格式，本字段保留扩展）
	Filter    *MatchFilter // 可选过滤器
}

// LsObjectJSON ls JSON 输出中的单对象结构
type LsObjectJSON struct {
	Provider     string `json:"provider"`
	Bucket       string `json:"bucket"`
	Key          string `json:"key"`
	URL          string `json:"url"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
	ETag         string `json:"etag"`
	StorageClass string `json:"storage_class,omitempty"`
}

// LsResultJSON ls JSON 输出根对象
type LsResultJSON struct {
	Objects []LsObjectJSON `json:"objects"`
	Count   int            `json:"count"`
}

// ListEngine 列举引擎
type ListEngine struct {
	storage objstore.Store
	cfg     ListConfig
}

func NewListEngine(s objstore.Store, cfg ListConfig) *ListEngine {
	return &ListEngine{storage: s, cfg: cfg}
}

// Run 执行列举并打印结果。
// 返回错误时调用方据此决定退出码。
func (e *ListEngine) Run(ctx context.Context) error {
	opts := objstore.ListOptions{Prefix: e.cfg.Prefix}
	if e.cfg.Recursive {
		opts.Delimiter = "" // 递归列举
	}

	objs, err := e.storage.ListObjects(ctx, opts)
	if err != nil {
		return err
	}

	// 应用 过滤器
	if e.cfg.Filter != nil && e.cfg.Filter.HasRules() {
		filtered := objs[:0]
		for _, o := range objs {
			rel := strings.TrimPrefix(o.Key, e.cfg.Prefix)
			rel = strings.TrimLeft(rel, "/")
			if e.cfg.Filter.Match(rel) {
				filtered = append(filtered, o)
			}
		}
		objs = filtered
	}

	// 按 Key 排序，输出更稳定
	sort.Slice(objs, func(i, j int) bool { return objs[i].Key < objs[j].Key })

	if len(objs) == 0 {
		// 与 ls 行为对齐：找不到对象 → ENOENT 风格
		return ErrNoSuchObject
	}

	provider := strings.ToLower(string(e.storage.Provider()))
	bucket := e.storage.BucketName()

	// JSON 输出
	if IsJSON() {
		res := LsResultJSON{Count: len(objs)}
		for _, o := range objs {
			res.Objects = append(res.Objects, LsObjectJSON{
				Provider:     provider,
				Bucket:       bucket,
				Key:          o.Key,
				URL:          fmt.Sprintf("%s://%s/%s", provider, bucket, o.Key),
				Size:         o.Size,
				LastModified: o.LastModified.Format(time.RFC3339),
				ETag:         strings.Trim(o.ETag, `"`),
				StorageClass: o.StorageClass,
			})
		}
		EmitJSON(res)
		return nil
	}

	// 表头
	fmt.Printf("%-6s  %12s  %20s  %-34s  %s\n",
		"TYPE", "SIZE", "LAST-MODIFIED", "ETAG", "OBJECT")

	for _, o := range objs {
		fmt.Printf("%-6s  %12s  %20s  %-34s  %s://%s/%s\n",
			provider,
			humanSize(o.Size),
			formatTime(o.LastModified),
			truncOrPad(strings.Trim(o.ETag, `"`), 34),
			provider, bucket, o.Key,
		)
	}

	fmt.Printf("\n共 %d 个对象\n", len(objs))
	return nil
}

// ErrNoSuchObject 用于 ls 找不到对象时返回，调用方据此返回 exit 2
var ErrNoSuchObject = fmt.Errorf("no such object or prefix")

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("2006-01-02 15:04:05")
}

func humanSize(n int64) string {
	const (
		_      = iota
		KB int64 = 1 << (10 * iota)
		MB
		GB
		TB
	)
	switch {
	case n >= TB:
		return fmt.Sprintf("%.2fTB", float64(n)/float64(TB))
	case n >= GB:
		return fmt.Sprintf("%.2fGB", float64(n)/float64(GB))
	case n >= MB:
		return fmt.Sprintf("%.2fMB", float64(n)/float64(MB))
	case n >= KB:
		return fmt.Sprintf("%.2fKB", float64(n)/float64(KB))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func truncOrPad(s string, w int) string {
	if s == "" {
		s = "-"
	}
	if len(s) > w {
		return s[:w]
	}
	return s
}