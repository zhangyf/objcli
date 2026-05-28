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

// 单文件本地↔云分块阈值
const localMultipartThresholdMB = 64

// LocalConfig 本地↔云传输配置
type LocalConfig struct {
	// 上传：LocalPath → Store + DstKey（或 DstPrefix + 子路径）
	// 下载：Store + SrcKey/SrcPrefix → LocalPath

	LocalPath string // 本地路径（文件或目录）
	SrcKey    string // 单文件下载用
	SrcPrefix string // 前缀下载用
	DstKey    string // 单文件上传用
	DstPrefix string // 前缀上传用

	ChunkMB           int  // 分块大小 MB
	ChunkConcurrency  int  // 单文件分块并发
	ObjectConcurrency int  // 多文件并发
	Recursive         bool // 前缀模式递归
	Force             bool // 跳过确认
}

// LocalEngine 本地↔云传输引擎
type LocalEngine struct {
	store objstore.Store
	cfg   LocalConfig
}

func NewLocalEngine(s objstore.Store, cfg LocalConfig) *LocalEngine {
	if cfg.ChunkMB <= 0 {
		cfg.ChunkMB = 128
	}
	if cfg.ChunkConcurrency <= 0 {
		cfg.ChunkConcurrency = 5
	}
	if cfg.ObjectConcurrency <= 0 {
		cfg.ObjectConcurrency = 3
	}
	return &LocalEngine{store: s, cfg: cfg}
}

// ============================================================
// 上传
// ============================================================

// Upload 本地 → 云
func (e *LocalEngine) Upload(ctx context.Context) error {
	st, err := os.Stat(e.cfg.LocalPath)
	if err != nil {
		return fmt.Errorf("打开本地路径失败: %w", err)
	}

	if !st.IsDir() {
		// 单文件上传
		key := e.cfg.DstKey
		if key == "" || strings.HasSuffix(key, "/") {
			key += filepath.Base(e.cfg.LocalPath)
		}
		fmt.Printf("[upload] %s → %s://%s/%s\n", e.cfg.LocalPath, e.store.Provider(), e.store.BucketName(), key)
		return e.uploadFile(ctx, e.cfg.LocalPath, key, st.Size())
	}

	// 目录上传
	if !e.cfg.Recursive {
		return fmt.Errorf("上传目录需要 -r 参数")
	}

	// 收集本地文件
	type fileEntry struct {
		path string
		key  string
		size int64
	}
	var files []fileEntry
	root := e.cfg.LocalPath
	if !strings.HasSuffix(root, string(os.PathSeparator)) {
		root += string(os.PathSeparator)
	}
	err = filepath.Walk(e.cfg.LocalPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(e.cfg.LocalPath, path)
		// 路径分隔符统一为 /
		key := filepath.ToSlash(rel)
		if e.cfg.DstPrefix != "" {
			key = strings.TrimRight(e.cfg.DstPrefix, "/") + "/" + key
		}
		files = append(files, fileEntry{path: path, key: key, size: info.Size()})
		return nil
	})
	if err != nil {
		return fmt.Errorf("遍历本地目录失败: %w", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("本地目录为空: %s", e.cfg.LocalPath)
	}

	if !e.cfg.Force {
		fmt.Printf("即将上传 %d 个文件到 %s://%s/%s\n继续? [y/N] ",
			len(files), e.store.Provider(), e.store.BucketName(), e.cfg.DstPrefix)
		var ans string
		fmt.Scanln(&ans)
		if !strings.EqualFold(ans, "y") {
			return fmt.Errorf("用户取消")
		}
	}

	// 并发上传
	sem := make(chan struct{}, e.cfg.ObjectConcurrency)
	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	var ok int64

	for i := range files {
		f := files[i]
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			if err := e.uploadFile(ctx, f.path, f.key, f.size); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				fmt.Fprintf(os.Stderr, "  [✗] %s → %s: %v\n", f.path, f.key, err)
				return
			}
			mu.Lock()
			ok++
			fmt.Printf("  [%d/%d] %s → %s\n", ok, len(files), f.path, f.key)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return fmt.Errorf("上传过程出错（首个错误）：%w", firstErr)
	}
	fmt.Printf("✅ 共上传 %d 个文件\n", ok)
	return nil
}

func (e *LocalEngine) uploadFile(ctx context.Context, localPath, key string, size int64) error {
	threshold := int64(localMultipartThresholdMB) * 1024 * 1024

	if size <= threshold {
		// 小文件 → PutObjectStream
		f, err := os.Open(localPath)
		if err != nil {
			return err
		}
		defer f.Close()
		return e.store.PutObjectStream(ctx, key, f, size)
	}

	// 大文件 → MultipartUpload
	chunkSize := int64(e.cfg.ChunkMB) * 1024 * 1024
	return e.store.MultipartUpload(ctx, key, size, chunkSize, e.cfg.ChunkConcurrency,
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
}

// ============================================================
// 下载
// ============================================================

// Download 云 → 本地
func (e *LocalEngine) Download(ctx context.Context) error {
	if e.cfg.SrcKey != "" {
		// 单文件下载
		dst := e.cfg.LocalPath
		// 若目标是目录或以 / 结尾，则拼上文件名
		if isDir, _ := isLocalDir(dst); isDir || strings.HasSuffix(dst, string(os.PathSeparator)) {
			dst = filepath.Join(dst, filepath.Base(e.cfg.SrcKey))
		}
		fmt.Printf("[download] %s://%s/%s → %s\n", e.store.Provider(), e.store.BucketName(), e.cfg.SrcKey, dst)
		return e.downloadFile(ctx, e.cfg.SrcKey, dst)
	}

	// 前缀下载
	if !e.cfg.Recursive {
		return fmt.Errorf("下载目录需要 -r 参数")
	}

	objs, err := e.store.ListObjects(ctx, objstore.ListOptions{Prefix: e.cfg.SrcPrefix, Delimiter: ""})
	if err != nil {
		return fmt.Errorf("列举源前缀失败: %w", err)
	}
	if len(objs) == 0 {
		return fmt.Errorf("源前缀下没有对象: %q", e.cfg.SrcPrefix)
	}

	if !e.cfg.Force {
		fmt.Printf("即将下载 %d 个对象到本地 %s\n继续? [y/N] ", len(objs), e.cfg.LocalPath)
		var ans string
		fmt.Scanln(&ans)
		if !strings.EqualFold(ans, "y") {
			return fmt.Errorf("用户取消")
		}
	}

	if err := os.MkdirAll(e.cfg.LocalPath, 0o755); err != nil {
		return err
	}

	sem := make(chan struct{}, e.cfg.ObjectConcurrency)
	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	var ok int64

	for i := range objs {
		obj := objs[i]
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			rel := strings.TrimPrefix(obj.Key, e.cfg.SrcPrefix)
			rel = strings.TrimLeft(rel, "/")
			localFile := filepath.Join(e.cfg.LocalPath, filepath.FromSlash(rel))

			if err := os.MkdirAll(filepath.Dir(localFile), 0o755); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			if err := e.downloadFile(ctx, obj.Key, localFile); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				fmt.Fprintf(os.Stderr, "  [✗] %s → %s: %v\n", obj.Key, localFile, err)
				return
			}
			mu.Lock()
			ok++
			fmt.Printf("  [%d/%d] %s → %s\n", ok, len(objs), obj.Key, localFile)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return fmt.Errorf("下载过程出错（首个错误）：%w", firstErr)
	}
	fmt.Printf("✅ 共下载 %d 个对象\n", ok)
	return nil
}

func (e *LocalEngine) downloadFile(ctx context.Context, key, localPath string) error {
	rc, err := e.store.GetObject(ctx, key)
	if err != nil {
		return err
	}
	defer rc.Close()

	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return err
	}
	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		os.Remove(localPath) // 失败清理
		return err
	}
	return nil
}

func isLocalDir(p string) (bool, error) {
	st, err := os.Stat(p)
	if err != nil {
		return false, err
	}
	return st.IsDir(), nil
}