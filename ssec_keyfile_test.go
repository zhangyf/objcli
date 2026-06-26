package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// writeTemp 写一个临时文件并返回路径
func writeTemp(t *testing.T, content []byte) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "ssec.key")
	if err := os.WriteFile(p, content, 0o600); err != nil {
		t.Fatalf("写临时文件失败: %v", err)
	}
	return p
}

func TestParseSSECKeyFile_Raw32(t *testing.T) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	p := writeTemp(t, raw)
	got, err := parseSSECKeyFile(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("32 字节原始密钥解析不一致")
	}
}

func TestParseSSECKeyFile_Base64(t *testing.T) {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(i)
	}
	b64 := base64.StdEncoding.EncodeToString(raw) // 44 字节
	if len(b64) != 44 {
		t.Fatalf("base64 长度应为 44，得到 %d", len(b64))
	}
	// 不带换行
	p := writeTemp(t, []byte(b64))
	got, err := parseSSECKeyFile(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("base64 解析结果不一致")
	}
	// 带尾部换行也应支持
	p2 := writeTemp(t, []byte(b64+"\n"))
	got2, err := parseSSECKeyFile(p2)
	if err != nil {
		t.Fatalf("带换行 base64 报错: %v", err)
	}
	if !bytes.Equal(got2, raw) {
		t.Fatalf("带换行 base64 解析结果不一致")
	}
}

func TestParseSSECKeyFile_Hex(t *testing.T) {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(255 - i)
	}
	h := hex.EncodeToString(raw) // 64 字节
	if len(h) != 64 {
		t.Fatalf("hex 长度应为 64，得到 %d", len(h))
	}
	p := writeTemp(t, []byte(h+"\n  "))
	got, err := parseSSECKeyFile(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("hex 解析结果不一致")
	}
}

func TestParseSSECKeyFile_Errors(t *testing.T) {
	// 文件不存在
	if _, err := parseSSECKeyFile(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("不存在的文件应报错")
	}
	// 长度非法（10 字节，无法解析）
	p := writeTemp(t, []byte("0123456789"))
	if _, err := parseSSECKeyFile(p); err == nil {
		t.Fatal("10 字节非法内容应报错")
	}
	// 31 字节原始（trim 后仍非 32，且非合法 base64/hex）
	p2 := writeTemp(t, bytes.Repeat([]byte{0x01}, 31))
	if _, err := parseSSECKeyFile(p2); err == nil {
		t.Fatal("31 字节应报错")
	}
}

// TestParseSSECKeyFile_Raw32WithSpaceBytes 验证：恰好 32 字节（含可能是空白的字节）
// 应被当作原始密钥直接使用，而不是被 trim。
func TestParseSSECKeyFile_Raw32WithSpaceBytes(t *testing.T) {
	raw := make([]byte, 32)
	raw[0] = ' '  // 首字节是空格
	raw[31] = '\n' // 末字节是换行
	for i := 1; i < 31; i++ {
		raw[i] = byte(i)
	}
	p := writeTemp(t, raw)
	got, err := parseSSECKeyFile(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("恰好 32 字节应原样返回，不应被 trim")
	}
}
