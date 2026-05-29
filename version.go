package main

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// 通过 -ldflags 注入；未注入时回退读取 debug.ReadBuildInfo()。
var (
	versionTag    = ""
	versionCommit = ""
	versionTime   = ""
)

const cmdVERSION = "version"

// printVersion 输出版本信息。
//
// 优先使用编译期通过 -ldflags 注入的 versionTag / versionCommit / versionTime；
// 若没有注入（例如 go install / go build 直接编译），回退到 runtime/debug.BuildInfo
// 中的 vcs.revision / vcs.time / Main.Version。
func printVersion() {
	tag := versionTag
	commit := versionCommit
	buildTime := versionTime
	modVersion := ""
	modSum := ""

	if bi, ok := debug.ReadBuildInfo(); ok {
		modVersion = bi.Main.Version
		modSum = bi.Main.Sum
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if commit == "" {
					commit = s.Value
				}
			case "vcs.time":
				if buildTime == "" {
					buildTime = s.Value
				}
			case "vcs.modified":
				if s.Value == "true" && commit != "" {
					commit += "-dirty"
				}
			}
		}
	}

	if tag == "" {
		if modVersion != "" && modVersion != "(devel)" {
			tag = modVersion
		} else {
			tag = "dev"
		}
	}
	if commit == "" {
		commit = "unknown"
	}
	if buildTime == "" {
		buildTime = "unknown"
	}

	fmt.Printf("objcli %s\n", tag)
	fmt.Printf("  commit:     %s\n", commit)
	fmt.Printf("  built:      %s\n", buildTime)
	fmt.Printf("  go:         %s\n", runtime.Version())
	fmt.Printf("  os/arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
	if modSum != "" {
		fmt.Printf("  module sum: %s\n", modSum)
	}
}
