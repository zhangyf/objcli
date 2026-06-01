package cmd

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zhangyf/objstore"
)

// LsHeadConfirmThreshold ls -l 这个阈值以上需要用户确认
const LsHeadConfirmThreshold = 100

// ListConfig 列举配置
type ListConfig struct {
	Prefix          string       // 列举前缀
	Recursive       bool         // 是否递归（递归时 Delimiter=""）
	Long            bool         // -l 长格式：对每个对象 head 拿完整元数据
	NoMeta          bool         // --no-meta 与 Long 互斥
	HeadConcurrency int          // Long 模式下并发 head
	Force           bool         // -f 跳过超阈值确认
	Filter          *MatchFilter // 可选过滤器
}

// LsObjectJSON ls JSON 输出中的单对象结构
type LsObjectJSON struct {
	Provider             string            `json:"provider"`
	Bucket               string            `json:"bucket"`
	Key                  string            `json:"key"`
	URL                  string            `json:"url"`
	Size                 int64             `json:"size"`
	LastModified         string            `json:"last_modified"`
	ETag                 string            `json:"etag"`
	StorageClass         string            `json:"storage_class,omitempty"`
	ContentType          string            `json:"content_type,omitempty"`
	ServerSideEncryption string            `json:"server_side_encryption,omitempty"`
	SSEKMSKeyID          string            `json:"sse_kms_key_id,omitempty"`
	VersionID            string            `json:"version_id,omitempty"`
	Metadata             map[string]string `json:"metadata,omitempty"`
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
	objs, singleHead, err := e.collectObjects(ctx)
	if err != nil {
		return err
	}
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
			res.Objects = append(res.Objects, toLsObjectJSON(provider, bucket, o))
		}
		EmitJSON(res)
		return nil
	}

	// 单对象 -l 走详情页风格
	if singleHead && e.cfg.Long && len(objs) == 1 {
		renderSingleDetail(provider, bucket, objs[0])
		return nil
	}

	// 多条表格
	renderTable(provider, bucket, objs, e.cfg.Long)
	return nil
}

// collectObjects 负责拿到对象列表（可能含 -l head 增强）。
// 返回的 singleHead=true 表示本次是“单 key 精准 head”路径（不走 list）。
func (e *ListEngine) collectObjects(ctx context.Context) (objs []objstore.ObjectInfo, singleHead bool, err error) {
	// 不能同时 -l + --no-meta
	if e.cfg.Long && e.cfg.NoMeta {
		return nil, false, fmt.Errorf("-l 与 --no-meta 互斥")
	}

	// -l 且不是以 "/" 结尾且没有 * 通配符→ 尝试单 key head
	if e.cfg.Long && e.cfg.Prefix != "" &&
		!strings.HasSuffix(e.cfg.Prefix, "/") &&
		!strings.ContainsAny(e.cfg.Prefix, "*?") {
		info, headErr := e.storage.HeadObject(ctx, e.cfg.Prefix)
		if headErr == nil {
			return []objstore.ObjectInfo{*info}, true, nil
		}
		// head 失败表示不是单 key 或权限不足，退化到 list 路径（可能是 prefix）
	}

	opts := objstore.ListOptions{Prefix: e.cfg.Prefix}
	if e.cfg.Recursive {
		opts.Delimiter = "" // 递归列举
	}

	objs, err = e.storage.ListObjects(ctx, opts)
	if err != nil {
		return nil, false, err
	}

	// 应用过滤器
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

	// 按 Key 排序
	sort.Slice(objs, func(i, j int) bool { return objs[i].Key < objs[j].Key })

	if len(objs) == 0 {
		return objs, false, nil
	}

	// -l 的多条路径：需要 N+1 次 head
	if e.cfg.Long && !e.cfg.NoMeta {
		if len(objs) > LsHeadConfirmThreshold && !e.cfg.Force {
			fmt.Fprintf(os.Stderr,
				"[ls -l] 将对 %d 个对象并发 HeadObject（concurrency=%d）。继续? [y/N] ",
				len(objs), e.headConcurrency())
			var ans string
			fmt.Scanln(&ans)
			if !strings.EqualFold(ans, "y") {
				return nil, false, fmt.Errorf("用户取消")
			}
		}
		if err := e.augmentWithHead(ctx, objs); err != nil {
			return nil, false, err
		}
	}

	return objs, false, nil
}

func (e *ListEngine) headConcurrency() int {
	if e.cfg.HeadConcurrency > 0 {
		return e.cfg.HeadConcurrency
	}
	return 50
}

// augmentWithHead 并发对每个对象发起 HeadObject，把额外字段填回 objs。
func (e *ListEngine) augmentWithHead(ctx context.Context, objs []objstore.ObjectInfo) error {
	conc := e.headConcurrency()
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i := range objs {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			info, err := e.storage.HeadObject(ctx, objs[idx].Key)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("head %s: %w", objs[idx].Key, err)
				}
				mu.Unlock()
				return
			}
			// 仅覆盖 head 独有的字段（list 中的 size/etag/lastmod 已在）
			objs[idx].ContentType = info.ContentType
			objs[idx].ServerSideEncryption = info.ServerSideEncryption
			objs[idx].SSEKMSKeyID = info.SSEKMSKeyID
			objs[idx].VersionID = info.VersionID
			objs[idx].Metadata = info.Metadata
		}(i)
	}
	wg.Wait()
	return firstErr
}

// toLsObjectJSON 把 ObjectInfo 转成 JSON 输出结构
func toLsObjectJSON(provider, bucket string, o objstore.ObjectInfo) LsObjectJSON {
	return LsObjectJSON{
		Provider:             provider,
		Bucket:               bucket,
		Key:                  o.Key,
		URL:                  fmt.Sprintf("%s://%s/%s", provider, bucket, o.Key),
		Size:                 o.Size,
		LastModified:         o.LastModified.Format(time.RFC3339),
		ETag:                 strings.Trim(o.ETag, `"`),
		StorageClass:         o.StorageClass,
		ContentType:          o.ContentType,
		ServerSideEncryption: o.ServerSideEncryption,
		SSEKMSKeyID:          o.SSEKMSKeyID,
		VersionID:            o.VersionID,
		Metadata:             o.Metadata,
	}
}

// renderSingleDetail 单对象 -l 的“详情页”风格输出
func renderSingleDetail(provider, bucket string, o objstore.ObjectInfo) {
	fmt.Printf("%-24s %s://%s/%s\n", "url:", provider, bucket, o.Key)
	fmt.Printf("%-24s %s (%d)\n", "size:", humanSize(o.Size), o.Size)
	fmt.Printf("%-24s %s\n", "etag:", strings.Trim(o.ETag, `"`))
	fmt.Printf("%-24s %s\n", "last-modified:", formatTime(o.LastModified))
	if o.ContentType != "" {
		fmt.Printf("%-24s %s\n", "content-type:", o.ContentType)
	}
	if o.StorageClass != "" {
		fmt.Printf("%-24s %s\n", "storage-class:", o.StorageClass)
	}
	if o.ServerSideEncryption != "" {
		fmt.Printf("%-24s %s\n", "server-side-encryption:", o.ServerSideEncryption)
	}
	if o.SSEKMSKeyID != "" {
		fmt.Printf("%-24s %s\n", "sse-kms-key-id:", o.SSEKMSKeyID)
	}
	if o.VersionID != "" {
		fmt.Printf("%-24s %s\n", "version-id:", o.VersionID)
	}
	if len(o.Metadata) > 0 {
		fmt.Printf("metadata:\n")
		keys := make([]string, 0, len(o.Metadata))
		for k := range o.Metadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("  %-22s %s\n", k+":", o.Metadata[k])
		}
	}
}

// renderTable 多条表格输出
func renderTable(provider, bucket string, objs []objstore.ObjectInfo, long bool) {
	if long {
		// 长表格：size / last-modified / storage-class / encryption / content-type / etag / object
		fmt.Printf("%-12s  %-19s  %-12s  %-10s  %-24s  %-12s  %s\n",
			"SIZE", "LAST-MODIFIED", "STORAGE", "SSE", "CONTENT-TYPE", "ETAG", "OBJECT")
		for _, o := range objs {
			fmt.Printf("%-12s  %-19s  %-12s  %-10s  %-24s  %-12s  %s://%s/%s\n",
				humanSize(o.Size),
				formatTime(o.LastModified),
				dashIfEmpty(o.StorageClass),
				dashIfEmpty(o.ServerSideEncryption),
				truncOrPad(dashIfEmpty(o.ContentType), 24),
				truncOrPad(strings.Trim(o.ETag, `"`), 12),
				provider, bucket, o.Key,
			)
		}
		fmt.Printf("\n共 %d 个对象\n", len(objs))
		return
	}

	// 默认表格（保持原状）
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
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
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