package cmd

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Config 持久化配置（~/.objcli/config 简化 INI）
//
// 文件格式（ini-like）:
//
//	[default]
//	cos_secret_id = xxx
//	cos_secret_key = xxx
//	cos_region = ap-beijing
//	s3_access_key = xxx
//	s3_secret_key = xxx
//	s3_region = us-east-1
//	aws_profile = default
//
//	[profile work]
//	cos_secret_id = yyy
//
// 解析时统一转 lower-case key。"profile xxx" / 直接 "xxx" 两种写法都接受。
type Config struct {
	// section name → key → value
	sections map[string]map[string]string
}

// LoadConfig 从默认路径读取
func LoadConfig() (*Config, error) {
	path, err := DefaultConfigPath()
	if err != nil {
		return &Config{sections: map[string]map[string]string{}}, nil
	}
	return LoadConfigFrom(path)
}

// DefaultConfigPath 返回默认配置文件路径
//
//	$OBJCLI_CONFIG_FILE > $HOME/.objcli/config
func DefaultConfigPath() (string, error) {
	if v := os.Getenv("OBJCLI_CONFIG_FILE"); v != "" {
		return v, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".objcli", "config"), nil
}

// LoadConfigFrom 从指定路径读取；文件不存在返回空配置（不报错）
func LoadConfigFrom(path string) (*Config, error) {
	c := &Config{sections: map[string]map[string]string{}}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return c, nil
		}
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	current := ""
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" || strings.HasPrefix(raw, "#") || strings.HasPrefix(raw, ";") {
			continue
		}
		if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
			name := strings.TrimSpace(raw[1 : len(raw)-1])
			// 兼容 awscli 风格 "profile xxx"
			name = strings.TrimPrefix(name, "profile ")
			name = strings.TrimSpace(name)
			if name == "" {
				return nil, fmt.Errorf("%s:%d: 空 section 名", path, lineNo)
			}
			current = name
			if _, ok := c.sections[current]; !ok {
				c.sections[current] = map[string]string{}
			}
			continue
		}
		eq := strings.Index(raw, "=")
		if eq <= 0 {
			return nil, fmt.Errorf("%s:%d: 无效行 %q", path, lineNo, raw)
		}
		if current == "" {
			return nil, fmt.Errorf("%s:%d: 字段位于任何 section 之前", path, lineNo)
		}
		k := strings.ToLower(strings.TrimSpace(raw[:eq]))
		v := strings.TrimSpace(raw[eq+1:])
		c.sections[current][k] = v
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return c, nil
}

// Get 取 key（指定 profile，找不到返回空串）
func (c *Config) Get(profile, key string) string {
	if c == nil {
		return ""
	}
	sec, ok := c.sections[profile]
	if !ok {
		return ""
	}
	return sec[strings.ToLower(key)]
}

// Profiles 返回所有 profile 名（含 default）
func (c *Config) Profiles() []string {
	if c == nil {
		return nil
	}
	out := make([]string, 0, len(c.sections))
	for k := range c.sections {
		out = append(out, k)
	}
	return out
}

// HasProfile 判断是否存在指定 profile
func (c *Config) HasProfile(profile string) bool {
	if c == nil {
		return false
	}
	_, ok := c.sections[profile]
	return ok
}
