//go:build !taskobserver

package main

// 默认构建：不依赖 taskobserver 库。
// startObserver 始终返回 nil，所有 -obs-* 参数与 TASKOBS_* 环境变量被忽略。
func startObserver(_ obsConfig, _ func() (int, int)) obsHandle {
	return nil
}
