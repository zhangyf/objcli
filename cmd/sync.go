package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/zhangyf/objstore"
)

// SyncSide 表示 sync 的一端，是云对象、还是本地路径
type SyncSide struct {
	IsLocal bool
	Local   string // 本地路径（IsLocal 时）
	Store   objstore.Store
	Prefix  string
}

// SyncConfig sync 配置
type SyncConfig struct {
	Recursive         bool
	Delete            bool         // 删除目标多余的对象
	DryRun            bool         // 仅打印计划
	SizeOnly          bool         // 增量判定只比 size 不比 mtime
	ChunkMB           int
	ChunkConcurrency  int
	ObjectConcurrency int
	Filter            *MatchFilter // exclude/include 过滤
}

// SyncEngine 同步引擎
type SyncEngine struct {
	src SyncSide
	dst SyncSide
	cfg SyncConfig
}

func NewSyncEngine(src, dst SyncSide, cfg SyncConfig) *SyncEngine {
	if cfg.ChunkMB <= 0 {
		cfg.ChunkMB = 128
	}
	if cfg.ChunkConcurrency <= 0 {
		cfg.ChunkConcurrency = 5
	}
	if cfg.ObjectConcurrency <= 0 {
		cfg.ObjectConcurrency = 3
	}
	return &SyncEngine{src: src, dst: dst, cfg: cfg}
}

// 单个文件的对比信息
type syncEntry struct {
	relKey string // 相对 key（不含前缀）
	size   int64
	etag   string // 仅云对象有
}

// Run 执行同步
func (e *SyncEngine) Run(ctx context.Context) error {
	// 列举两端
	srcMap, err := listSide(ctx, e.src)
	if err != nil {
		return fmt.Errorf("列举源失败: %w", err)
	}
	dstMap, err := listSide(ctx, e.dst)
	if err != nil {
		return fmt.Errorf("列举目标失败: %w", err)
	}

	// 应用 filter。过滤 应用在 src/dst 两边，以保证不会删被过滤掉的目标对象
	if e.cfg.Filter != nil && e.cfg.Filter.HasRules() {
		srcMap = applyFilter(srcMap, e.cfg.Filter)
		dstMap = applyFilter(dstMap, e.cfg.Filter)
	}

	// 计算需要复制 / 删除
	var toCopy []syncEntry
	for k, sv := range srcMap {
		dv, ok := dstMap[k]
		if !ok {
			toCopy = append(toCopy, sv)
			continue
		}
		// 已存在 → 看 size+ETag 是否一致
		if !sameObject(sv, dv, e.cfg.SizeOnly) {
			toCopy = append(toCopy, sv)
		}
	}

	var toDelete []string
	if e.cfg.Delete {
		for k := range dstMap {
			if _, ok := srcMap[k]; !ok {
				toDelete = append(toDelete, k)
			}
		}
	}

	fmt.Printf("源 %d 个 / 目标 %d 个 → 计划复制 %d 个 / 删除 %d 个\n",
		len(srcMap), len(dstMap), len(toCopy), len(toDelete))

	if e.cfg.DryRun {
		for _, c := range toCopy {
			fmt.Printf("  [+] %s (%d bytes)\n", c.relKey, c.size)
		}
		for _, d := range toDelete {
			fmt.Printf("  [-] %s\n", d)
		}
		return nil
	}

	// 执行复制
	if err := e.runCopies(ctx, toCopy); err != nil {
		return err
	}

	// 执行删除
	if e.cfg.Delete && len(toDelete) > 0 {
		if err := e.runDeletes(ctx, toDelete); err != nil {
			return err
		}
	}

	fmt.Printf("✅ 同步完成：复制 %d / 删除 %d\n", len(toCopy), len(toDelete))
	return nil
}

// listSide 列举一端的对象（云或本地），返回 map[相对key] → entry
func listSide(ctx context.Context, side SyncSide) (map[string]syncEntry, error) {
	out := make(map[string]syncEntry)
	if side.IsLocal {
		// 本地遍历
		root := side.Local
		if _, err := os.Stat(root); err != nil {
			if os.IsNotExist(err) {
				// 本地目录不存在 → 视为空，让 sync 创建
				return out, nil
			}
			return nil, err
		}
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			out[rel] = syncEntry{relKey: rel, size: info.Size()}
			return nil
		})
		return out, err
	}

	// 云端列举
	objs, err := side.Store.ListObjects(ctx, objstore.ListOptions{Prefix: side.Prefix, Delimiter: ""})
	if err != nil {
		return nil, err
	}
	for _, o := range objs {
		rel := strings.TrimPrefix(o.Key, side.Prefix)
		rel = strings.TrimLeft(rel, "/")
		if rel == "" {
			continue
		}
		out[rel] = syncEntry{relKey: rel, size: o.Size, etag: strings.Trim(o.ETag, `"`)}
	}
	return out, nil
}

// sameObject 判断两个对象是否一致
//   - sizeOnly=true 只比 size
//   - sizeOnly=false 优先比 ETag（同厂商可靠），ETag 缺失或不一致时退化到 size
func sameObject(a, b syncEntry, sizeOnly bool) bool {
	if sizeOnly {
		return a.size == b.size
	}
	if a.etag != "" && b.etag != "" {
		return a.etag == b.etag
	}
	return a.size == b.size
}

// ============================================================
// 复制
// ============================================================

func (e *SyncEngine) runCopies(ctx context.Context, items []syncEntry) error {
	if len(items) == 0 {
		return nil
	}
	sem := make(chan struct{}, e.cfg.ObjectConcurrency)
	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	var ok int64

	for i := range items {
		it := items[i]
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			if err := e.copyOne(ctx, it); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				fmt.Fprintf(os.Stderr, "  [✗] %s: %v\n", it.relKey, err)
				return
			}
			mu.Lock()
			ok++
			fmt.Printf("  [%d/%d] %s\n", ok, len(items), it.relKey)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return firstErr
}

func (e *SyncEngine) copyOne(ctx context.Context, it syncEntry) error {
	srcKey := joinPrefix(e.src.Prefix, it.relKey)
	dstKey := joinPrefix(e.dst.Prefix, it.relKey)

	// 4 种组合：local→cloud / cloud→local / cloud→cloud / local→local
	switch {
	case e.src.IsLocal && !e.dst.IsLocal:
		// 上传
		localPath := filepath.Join(e.src.Local, filepath.FromSlash(it.relKey))
		st, err := os.Stat(localPath)
		if err != nil {
			return err
		}
		if st.Size() <= int64(localMultipartThresholdMB)*1024*1024 {
			f, err := os.Open(localPath)
			if err != nil {
				return err
			}
			defer f.Close()
			return e.dst.Store.PutObjectStream(ctx, dstKey, f, st.Size())
		}
		chunkSize := int64(e.cfg.ChunkMB) * 1024 * 1024
		return e.dst.Store.MultipartUpload(ctx, dstKey, st.Size(), chunkSize, e.cfg.ChunkConcurrency,
			func(partNumber int, offset, partSize int64) ([]byte, error) {
				f, err := os.Open(localPath)
				if err != nil {
					return nil, err
				}
				defer f.Close()
				if _, err := f.Seek(offset, io.SeekStart); err != nil {
					return nil, err
				}
				buf := make([]byte, partSize)
				if _, err := io.ReadFull(f, buf); err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
					return nil, err
				}
				return buf, nil
			})

	case !e.src.IsLocal && e.dst.IsLocal:
		// 下载
		localPath := filepath.Join(e.dst.Local, filepath.FromSlash(it.relKey))
		if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
			return err
		}
		rc, err := e.src.Store.GetObject(ctx, srcKey)
		if err != nil {
			return err
		}
		defer rc.Close()
		f, err := os.Create(localPath)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(f, rc)
		if err != nil {
			os.Remove(localPath)
		}
		return err

	case !e.src.IsLocal && !e.dst.IsLocal:
		// 云→云：流式 GetObject + PutObjectStream（同 region 时简化处理）
		// 大文件场景应走 Engine 的 Multipart/ServerCopier，这里先简化为流式
		rc, err := e.src.Store.GetObject(ctx, srcKey)
		if err != nil {
			return err
		}
		defer rc.Close()
		return e.dst.Store.PutObjectStream(ctx, dstKey, rc, it.size)

	case e.src.IsLocal && e.dst.IsLocal:
		// 本地→本地：直接复制文件
		srcPath := filepath.Join(e.src.Local, filepath.FromSlash(it.relKey))
		dstPath := filepath.Join(e.dst.Local, filepath.FromSlash(it.relKey))
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			return err
		}
		sf, err := os.Open(srcPath)
		if err != nil {
			return err
		}
		defer sf.Close()
		df, err := os.Create(dstPath)
		if err != nil {
			return err
		}
		defer df.Close()
		_, err = io.Copy(df, sf)
		return err
	}
	return fmt.Errorf("unreachable")
}

// ============================================================
// 删除
// ============================================================

func (e *SyncEngine) runDeletes(ctx context.Context, keys []string) error {
	sem := make(chan struct{}, e.cfg.ObjectConcurrency)
	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	var ok int64

	for i := range keys {
		k := keys[i]
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			fullKey := joinPrefix(e.dst.Prefix, k)
			var err error
			if e.dst.IsLocal {
				err = os.Remove(filepath.Join(e.dst.Local, filepath.FromSlash(k)))
			} else {
				err = e.dst.Store.DeleteObject(ctx, fullKey)
			}
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				fmt.Fprintf(os.Stderr, "  [✗ del] %s: %v\n", fullKey, err)
				return
			}
			mu.Lock()
			ok++
			fmt.Printf("  [del %d/%d] %s\n", ok, len(keys), fullKey)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return firstErr
}

func joinPrefix(prefix, rel string) string {
	if prefix == "" {
		return rel
	}
	return strings.TrimRight(prefix, "/") + "/" + rel
}

func applyFilter(in map[string]syncEntry, f *MatchFilter) map[string]syncEntry {
	out := make(map[string]syncEntry, len(in))
	for k, v := range in {
		if f.Match(k) {
			out[k] = v
		}
	}
	return out
}