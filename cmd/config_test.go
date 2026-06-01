package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_Basic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	body := `# top comment
[default]
cos_secret_id = abc
cos_secret_key = def
cos_region = ap-beijing

[profile work]
cos_secret_id = xyz
s3_access_key = AKIA1
s3_secret_key = sec1

; 注释
[home]
s3_region = us-east-1
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfigFrom(path)
	if err != nil {
		t.Fatalf("LoadConfigFrom: %v", err)
	}

	if v := c.Get("default", "cos_secret_id"); v != "abc" {
		t.Errorf("default cos_secret_id = %q, want abc", v)
	}
	if v := c.Get("default", "COS_REGION"); v != "ap-beijing" {
		t.Errorf("Get is case-insensitive on key, got %q", v)
	}
	if v := c.Get("work", "cos_secret_id"); v != "xyz" {
		t.Errorf("[profile work] -> work, got %q", v)
	}
	if v := c.Get("work", "s3_access_key"); v != "AKIA1" {
		t.Errorf("work s3_access_key = %q", v)
	}
	if v := c.Get("home", "s3_region"); v != "us-east-1" {
		t.Errorf("home s3_region = %q", v)
	}
	if v := c.Get("none", "anything"); v != "" {
		t.Errorf("missing profile should return empty, got %q", v)
	}
	if !c.HasProfile("default") || !c.HasProfile("work") || !c.HasProfile("home") {
		t.Errorf("HasProfile missing")
	}
	if c.HasProfile("nope") {
		t.Errorf("HasProfile false positive")
	}
}

func TestLoadConfig_Missing(t *testing.T) {
	dir := t.TempDir()
	c, err := LoadConfigFrom(filepath.Join(dir, "nonexistent"))
	if err != nil {
		t.Fatalf("missing file should be ok, got %v", err)
	}
	if v := c.Get("default", "cos_secret_id"); v != "" {
		t.Errorf("missing file Get should be empty, got %q", v)
	}
}

func TestLoadConfig_BadLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte("[default]\ngarbage line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfigFrom(path)
	if err == nil {
		t.Errorf("expected error on bad line")
	}
}

func TestLoadConfig_OrphanKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte("k = v\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfigFrom(path)
	if err == nil {
		t.Errorf("expected error on orphan key (no section)")
	}
}
