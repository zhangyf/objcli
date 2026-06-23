package main

// obsConfig 是 taskobserver 的最小配置，与具体实现解耦。
// 默认构建（无 taskobserver tag）下 startObserver 为 no-op。
type obsConfig struct {
	Bucket    string
	Region    string
	SecretID  string
	SecretKey string
	BaseURL   string
	TaskName  string
}

// obsHandle 是 main 对监控器的最小依赖面。
// 真实实现见 observer_enabled.go（//go:build taskobserver）。
type obsHandle interface {
	Fail(err error)
	Done()
}
