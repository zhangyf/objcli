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

	ChunkMB           int          // 分块大小 MB
	ChunkConcurrency  int          // 单文件分块并发
	ObjectConcurrency int          // 多文件并发
	Recursive         bool         // 前缀模式递归
	Force             bool         // 跳过确认
	Filter            *MatchFilter // exclude/include 过滤
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
		// 应用 filter（基于相对路径）
		if e.cfg.Filter != nil && !e.cfg.Filter.Match(key) {
			return nil
		}
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

	// 大文件 → 优先走断点续传（若 store 实现了 MultipartResumer）
	if resumer, ok := e.store.(objstore.MultipartResumer); ok {
		return e.uploadFileResumable(ctx, resumer, localPath, key, size)
	}

	// 后退到原有一次性 MultipartUpload
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

// uploadFileResumable 可断点续传的大文件上传
func (e *LocalEngine) uploadFileResumable(ctx context.Context, resumer objstore.MultipartResumer, localPath, key string, size int64) error {
	chunkSize := int64(e.cfg.ChunkMB) * 1024 * 1024

	// AWS S3 硬性限制：multipart 除最后一段外每段必须 ≥ 5MB
	// COS 最低 1MB，但为保证跨平台一致性，这里统一拒绝 <5MB 的 chunk
	if e.store.Provider() == objstore.ProviderS3 && chunkSize < 5*1024*1024 {
		return fmt.Errorf("S3 multipart 上传 chunk 必须 ≥ 5MB（当前 -chunk=%d）", e.cfg.ChunkMB)
	}

	totalParts := int((size + chunkSize - 1) / chunkSize)

	provider := string(e.store.Provider())
	bucket := e.store.BucketName()
	statePath := ResumeFilePath(provider, bucket, key, localPath)
	state := LoadResumeState(statePath)

	var uploadID string
	doneSet := make(map[int]string) // partNumber → etag

	if state != nil && state.UploadID != "" && state.TotalSize == size && state.ChunkSize == chunkSize {
		// 尝试恢复。验证在服务端是否还存在
		uploaded, err := resumer.ListParts(ctx, key, state.UploadID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[resume] 旧 uploadID=%s 无法使用 (%v)，重新开始\n", state.UploadID, err)
			DeleteResumeState(statePath)
		} else {
			uploadID = state.UploadID
			for _, p := range uploaded {
				doneSet[p.PartNumber] = p.ETag
			}
			if len(doneSet) > 0 {
				LogProgress("[resume] 检测到已传 %d / %d 个分块，跳过重传", len(doneSet), totalParts)
			}
		}
	}

	if uploadID == "" {
		id, err := resumer.InitMultipart(ctx, key)
		if err != nil {
			return err
		}
		uploadID = id
		state = &ResumeState{
			UploadID:  uploadID,
			Provider:  provider,
			Bucket:    bucket,
			Key:       key,
			TotalSize: size,
			ChunkSize: chunkSize,
			PartETags: make(map[int]string),
		}
		if err := SaveResumeState(statePath, state); err != nil {
			fmt.Fprintf(os.Stderr, "[resume] 状态写入失败: %v\n", err)
		}
	} else {
		// 同步 PartETags
		if state.PartETags == nil {
			state.PartETags = make(map[int]string)
		}
		for pn, et := range doneSet {
			state.PartETags[pn] = et
		}
	}

	// 并发上传缺失的分块
	var (
		jobs    = make(chan int, e.cfg.ChunkConcurrency*2)
		wg      sync.WaitGroup
		mu      sync.Mutex
		first   error
	)
	for i := 0; i < e.cfg.ChunkConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pn := range jobs {
				offset := int64(pn-1) * chunkSize
				sz := chunkSize
				if offset+sz > size {
					sz = size - offset
				}
				data, err := readChunk(localPath, offset, sz)
				if err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
					return
				}
				etag, err := resumer.UploadPartN(ctx, key, uploadID, pn, data)
				if err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
					return
				}
				mu.Lock()
				state.PartETags[pn] = etag
				state.DonePartIDs = append(state.DonePartIDs, pn)
				_ = SaveResumeState(statePath, state)
				mu.Unlock()
			}
		}()
	}
	for pn := 1; pn <= totalParts; pn++ {
		if _, ok := doneSet[pn]; ok {
			continue
		}
		jobs <- pn
	}
	close(jobs)
	wg.Wait()

	if first != nil {
		// 保留 state 以供续传
		return fmt.Errorf("上传中断，状态已保存到 %s：%w", statePath, first)
	}

	// 提交
	parts := make([]objstore.UploadedPart, 0, totalParts)
	for pn := 1; pn <= totalParts; pn++ {
		etag := state.PartETags[pn]
		if etag == "" {
			return fmt.Errorf("分块 %d 缺失 ETag，状态可能损坏", pn)
		}
		parts = append(parts, objstore.UploadedPart{PartNumber: pn, ETag: etag})
	}
	if err := resumer.CompleteMultipart(ctx, key, uploadID, parts); err != nil {
		return err
	}
	DeleteResumeState(statePath)
	return nil
}

func readChunk(path string, offset, size int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(f, buf); err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	return buf, nil
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
	// 应用 filter
	if e.cfg.Filter != nil && e.cfg.Filter.HasRules() {
		filtered := objs[:0]
		for _, o := range objs {
			rel := strings.TrimPrefix(o.Key, e.cfg.SrcPrefix)
			rel = strings.TrimLeft(rel, "/")
			if e.cfg.Filter.Match(rel) {
				filtered = append(filtered, o)
			}
		}
		objs = filtered
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
	// 先 head 拿到 size，判断是否走 resume 路径
	info, err := e.store.HeadObject(ctx, key)
	if err != nil {
		return err
	}

	threshold := int64(localMultipartThresholdMB) * 1024 * 1024
	if info.Size <= threshold {
		return e.downloadFileSimple(ctx, key, localPath)
	}
	return e.downloadFileResumable(ctx, key, localPath, info)
}

// downloadFileSimple 小文件一次性下载
func (e *LocalEngine) downloadFileSimple(ctx context.Context, key, localPath string) error {
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

// downloadFileResumable 大文件可续传下载
// 状态文件与 upload 共用 ResumeState，Kind="download"。
// 临时文件路径为 localPath + ".part"，全部下载完成后 rename。
func (e *LocalEngine) downloadFileResumable(ctx context.Context, key, localPath string, info *objstore.ObjectInfo) error {
	chunkSize := int64(e.cfg.ChunkMB) * 1024 * 1024
	if chunkSize <= 0 {
		chunkSize = 64 * 1024 * 1024
	}
	totalParts := int((info.Size + chunkSize - 1) / chunkSize)

	provider := string(e.store.Provider())
	bucket := e.store.BucketName()
	statePath := ResumeFilePath(provider, bucket, key, localPath)
	partPath := localPath + ".part"

	// 加载或初始化状态
	state := LoadResumeState(statePath)
	if state != nil && (state.ResumeKind() != "download" ||
		state.Provider != provider || state.Bucket != bucket || state.Key != key ||
		state.TotalSize != info.Size || state.ObjectETag != info.ETag) {
		// 状态文件与当前任务不匹配（源端已变、不同任务同路径冲突等），丢弃重来
		DeleteResumeState(statePath)
		_ = os.Remove(partPath)
		state = nil
	}
	if state == nil {
		state = &ResumeState{
			Kind:        "download",
			Provider:    provider,
			Bucket:      bucket,
			Key:         key,
			TotalSize:   info.Size,
			ChunkSize:   chunkSize,
			DonePartIDs: nil,
			LocalPath:   localPath,
			ObjectETag:  info.ETag,
		}
	}

	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return err
	}

	// 打开 .part（O_RDWR、不截断）并预分配空间
	f, err := os.OpenFile(partPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(info.Size); err != nil {
		return err
	}

	done := make(map[int]struct{}, len(state.DonePartIDs))
	for _, p := range state.DonePartIDs {
		done[p] = struct{}{}
	}
	if len(done) > 0 {
		fmt.Printf("[resume] 检测到已下载 %d / %d 个分块，跳过重拉\n", len(done), totalParts)
	}

	// 下载剩余分块
	sem := make(chan struct{}, e.cfg.ChunkConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for pn := 1; pn <= totalParts; pn++ {
		if _, ok := done[pn]; ok {
			continue
		}
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		default:
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(pn int) {
			defer wg.Done()
			defer func() { <-sem }()

			offset := int64(pn-1) * chunkSize
			end := offset + chunkSize - 1
			if end >= info.Size {
				end = info.Size - 1
			}
			data, err := e.store.GetRange(ctx, key, offset, end)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("download-part %d: %w", pn, err)
				}
				mu.Unlock()
				return
			}
			if _, err := f.WriteAt(data, offset); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("write-part %d: %w", pn, err)
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			state.DonePartIDs = append(state.DonePartIDs, pn)
			_ = SaveResumeState(statePath, state)
			mu.Unlock()
		}(pn)
	}
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// 全部完成 → rename 到最终路径 + 清理 state
	if err := os.Rename(partPath, localPath); err != nil {
		return err
	}
	DeleteResumeState(statePath)
	return nil
}

func isLocalDir(p string) (bool, error) {
	st, err := os.Stat(p)
	if err != nil {
		return false, err
	}
	return st.IsDir(), nil
}