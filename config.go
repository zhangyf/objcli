package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"objcli/cmd"
)

// objcli config <subcommand>
//
//	list                    列出 ~/.objcli/config 中的 profile
//	show [profile]          显示某 profile（默认 default）的所有 key=value
//	path                    打印配置文件路径
//	init [profile]          交互式创建 profile（写入文件，已存在的 key 会被覆盖）
func runConfig(ctx context.Context, args []string) int {
	if len(args) == 0 {
		printConfigUsage()
		return exitUsage
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "list":
		return runConfigList(rest)
	case "show":
		return runConfigShow(rest)
	case "path":
		return runConfigPath(rest)
	case "init":
		return runConfigInit(ctx, rest)
	case "-h", "--help", "help":
		printConfigUsage()
		return exitOK
	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		printConfigUsage()
		return exitUsage
	}
}

func runConfigList(args []string) int {
	fs := flag.NewFlagSet("config list", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	c, err := cmd.LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取配置失败: %v\n", err)
		return exitFail
	}
	profiles := c.Profiles()
	if len(profiles) == 0 {
		path, _ := cmd.DefaultConfigPath()
		fmt.Fprintf(os.Stderr, "(空) %s\n", path)
		return exitOK
	}
	for _, p := range profiles {
		fmt.Println(p)
	}
	return exitOK
}

func runConfigShow(args []string) int {
	fs := flag.NewFlagSet("config show", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	profile := "default"
	if len(fs.Args()) > 0 {
		profile = fs.Arg(0)
	}
	c, err := cmd.LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取配置失败: %v\n", err)
		return exitFail
	}
	if !c.HasProfile(profile) {
		fmt.Fprintf(os.Stderr, "profile %q 不存在\n", profile)
		return exitFail
	}
	keys := []string{
		"cos_secret_id", "cos_secret_key", "cos_region",
		"s3_access_key", "s3_secret_key", "s3_region",
		"s3_endpoint", "aws_profile",
	}
	fmt.Printf("[%s]\n", profile)
	for _, k := range keys {
		v := c.Get(profile, k)
		if v == "" {
			continue
		}
		// 敏感字段脱敏
		if strings.Contains(k, "secret") || strings.Contains(k, "access_key") {
			v = maskSecret(v)
		}
		fmt.Printf("  %-16s = %s\n", k, v)
	}
	return exitOK
}

func runConfigPath(args []string) int {
	fs := flag.NewFlagSet("config path", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	path, err := cmd.DefaultConfigPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return exitFail
	}
	fmt.Println(path)
	return exitOK
}

func runConfigInit(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("config init", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	profile := "default"
	if len(fs.Args()) > 0 {
		profile = fs.Arg(0)
	}
	path, err := cmd.DefaultConfigPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return exitFail
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "创建目录失败: %v\n", err)
		return exitFail
	}
	c, err := cmd.LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取已有配置失败: %v\n", err)
		return exitFail
	}
	fmt.Fprintf(os.Stderr, "配置 [%s]（回车跳过保留原值）\n", profile)
	prompt := func(label, current string) string {
		shown := current
		if strings.Contains(strings.ToLower(label), "secret") || strings.Contains(strings.ToLower(label), "access_key") {
			shown = maskSecret(current)
		}
		if shown != "" {
			fmt.Fprintf(os.Stderr, "  %-16s [%s]: ", label, shown)
		} else {
			fmt.Fprintf(os.Stderr, "  %-16s: ", label)
		}
		var s string
		fmt.Scanln(&s)
		s = strings.TrimSpace(s)
		if s == "" {
			return current
		}
		return s
	}

	updates := map[string]string{
		"cos_secret_id":  prompt("cos_secret_id", c.Get(profile, "cos_secret_id")),
		"cos_secret_key": prompt("cos_secret_key", c.Get(profile, "cos_secret_key")),
		"cos_region":     prompt("cos_region", c.Get(profile, "cos_region")),
		"s3_access_key":  prompt("s3_access_key", c.Get(profile, "s3_access_key")),
		"s3_secret_key":  prompt("s3_secret_key", c.Get(profile, "s3_secret_key")),
		"s3_region":      prompt("s3_region", c.Get(profile, "s3_region")),
		"s3_endpoint":    prompt("s3_endpoint", c.Get(profile, "s3_endpoint")),
		"aws_profile":    prompt("aws_profile", c.Get(profile, "aws_profile")),
	}

	if err := writeConfigProfile(path, profile, updates); err != nil {
		fmt.Fprintf(os.Stderr, "写入失败: %v\n", err)
		return exitFail
	}
	fmt.Fprintf(os.Stderr, "✅ 已更新 %s\n", path)
	_ = ctx
	return exitOK
}

// writeConfigProfile 把 profile 的 KV 写回 ini 文件（保留其他 section）
func writeConfigProfile(path, profile string, kv map[string]string) error {
	c, err := cmd.LoadConfig()
	if err != nil {
		return err
	}
	// 用 LoadConfig 读出所有 profile，然后整体重写
	merged := map[string]map[string]string{}
	for _, p := range c.Profiles() {
		merged[p] = map[string]string{}
		// 走 keys 都拷出来
		for _, k := range []string{
			"cos_secret_id", "cos_secret_key", "cos_region",
			"s3_access_key", "s3_secret_key", "s3_region",
			"s3_endpoint", "aws_profile",
		} {
			if v := c.Get(p, k); v != "" {
				merged[p][k] = v
			}
		}
	}
	if _, ok := merged[profile]; !ok {
		merged[profile] = map[string]string{}
	}
	for k, v := range kv {
		if v == "" {
			delete(merged[profile], k)
			continue
		}
		merged[profile][k] = v
	}

	var b strings.Builder
	// default 排前
	order := []string{"default"}
	for p := range merged {
		if p != "default" {
			order = append(order, p)
		}
	}
	for _, p := range order {
		sec, ok := merged[p]
		if !ok || len(sec) == 0 {
			continue
		}
		fmt.Fprintf(&b, "[%s]\n", p)
		// 固定 key 顺序，便于 diff
		for _, k := range []string{
			"cos_secret_id", "cos_secret_key", "cos_region",
			"s3_access_key", "s3_secret_key", "s3_region",
			"s3_endpoint", "aws_profile",
		} {
			if v := sec[k]; v != "" {
				fmt.Fprintf(&b, "%s = %s\n", k, v)
			}
		}
		b.WriteString("\n")
	}
	return os.WriteFile(path, []byte(b.String()), 0o600)
}

func maskSecret(s string) string {
	if len(s) <= 6 {
		return strings.Repeat("*", len(s))
	}
	return s[:3] + strings.Repeat("*", len(s)-6) + s[len(s)-3:]
}

func printConfigUsage() {
	fmt.Print(`objcli config - 管理 ~/.objcli/config 中的持久化配置

用法:
  objcli config list                  列出所有 profile
  objcli config show [profile]        显示某 profile 的 key=value（敏感字段脱敏）
  objcli config path                  打印配置文件路径
  objcli config init [profile]        交互式编辑 profile（默认 default）

支持的 key:
  cos_secret_id      腾讯云 SecretId
  cos_secret_key     腾讯云 SecretKey
  cos_region         默认 COS region（URL 不带 region 时使用）
  s3_access_key      AWS Access Key ID
  s3_secret_key      AWS Secret Access Key
  s3_region          默认 S3 region
  s3_endpoint        S3 兼容 endpoint
  aws_profile        AWS profile 名（~/.aws/credentials）

凭证优先级:
  flag (-cos-id / -s3-ak / ...) > env (TENCENT_SECRET_ID / AWS_ACCESS_KEY_ID / ...) > config 文件

使用 -profile NAME 选择某个 profile（默认 default 或 $OBJCLI_PROFILE）。

示例:
  objcli config init                            # 交互式配置 default
  objcli config init work                       # 配置另一个 profile
  objcli ls -profile work cos://b.region/dir/  # 用 work profile 列举
`)
}
