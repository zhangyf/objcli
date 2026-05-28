package cmd

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ResumeState 描述一个可续传任务的状态（上传 或 下载）
type ResumeState struct {
	Kind        string         `json:"kind,omitempty"`         // "upload" / "download"，空视为 upload（兼容旧状态文件）
	UploadID    string         `json:"upload_id"`              // upload 专用
	Provider    string         `json:"provider"`
	Bucket      string         `json:"bucket"`
	Key         string         `json:"key"`
	TotalSize   int64          `json:"total_size"`
	ChunkSize   int64          `json:"chunk_size"`
	DonePartIDs []int          `json:"done_part_ids"`
	PartETags   map[int]string `json:"part_etags"`
	UpdatedAt   time.Time      `json:"updated_at"`

	// 下载专用
	LocalPath string `json:"local_path,omitempty"` // 本地目标路径（实际写 .part 后缀）
	ObjectETag string `json:"object_etag,omitempty"` // 云端对象 ETag（判定源是否变化）

	// StatePath 本地状态文件路径（运行时填充，不序列化）
	StatePath string `json:"-"`
}

// ResumeKind 返回任务类型，空值兼容为 upload
func (s *ResumeState) ResumeKind() string {
	if s.Kind == "" {
		return "upload"
	}
	return s.Kind
}

// resumeDir 返回断点续传状态文件目录
func resumeDir() string {
	home, _ := os.UserHomeDir()
	if home == "" {
		home = os.TempDir()
	}
	d := filepath.Join(home, ".objcli", "resume")
	_ = os.MkdirAll(d, 0o700)
	return d
}

// resumeKey 根据 provider+bucket+key+localPath 生成唯一 key
func ResumeFilePath(provider, bucket, key, localOrSrc string) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s|%s|%s|%s", provider, bucket, key, localOrSrc)
	id := hex.EncodeToString(h.Sum(nil))[:16]
	return filepath.Join(resumeDir(), id+".json")
}

// LoadResumeState 加载状态文件，不存在则返回 nil
func LoadResumeState(path string) *ResumeState {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var s ResumeState
	if err := json.Unmarshal(data, &s); err != nil {
		return nil
	}
	return &s
}

// SaveResumeState 持久化状态
func SaveResumeState(path string, s *ResumeState) error {
	s.UpdatedAt = time.Now()
	if s.PartETags == nil {
		s.PartETags = make(map[int]string)
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// DeleteResumeState 上传成功后删除
func DeleteResumeState(path string) {
	os.Remove(path)
}

// DeleteResumeStateByPath 别名，语义更明确
func DeleteResumeStateByPath(path string) {
	if path == "" {
		return
	}
	_ = os.Remove(path)
}

// ListResumeStates 列出所有残留的 resume 状态文件
func ListResumeStates() []*ResumeState {
	d := resumeDir()
	entries, err := os.ReadDir(d)
	if err != nil {
		return nil
	}
	var out []*ResumeState
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		path := filepath.Join(d, e.Name())
		s := LoadResumeState(path)
		if s == nil {
			continue
		}
		s.StatePath = path
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out
}