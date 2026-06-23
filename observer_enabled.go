//go:build taskobserver

package main

import (
	"log"
	"os"
	"time"

	"taskobserver"
)

// enabled 构建（go build -tags taskobserver）：真正接入 taskobserver。
// 仅当 Bucket 与 SecretID 均非空时启用，否则返回 nil（no-op）。
func startObserver(cfg obsConfig, progressFn func() (int, int)) obsHandle {
	if cfg.Bucket == "" || cfg.SecretID == "" {
		return nil
	}
	obs, err := taskobserver.NewWithError(taskobserver.Config{
		Bucket:      cfg.Bucket,
		Region:      cfg.Region,
		SecretID:    cfg.SecretID,
		SecretKey:   cfg.SecretKey,
		BaseURL:     cfg.BaseURL,
		TaskName:    cfg.TaskName,
		Interval:    5 * time.Second,
		ExtraWriter: os.Stderr,
	})
	if err != nil {
		log.Printf("[taskobserver] 初始化失败，将跳过监控: %v", err)
		return nil
	}
	log.SetOutput(obs.Writer())
	log.SetFlags(0)
	obs.Start(progressFn)
	log.Printf("[taskobserver] Overview : %s", obs.OverviewURL())
	log.Printf("[taskobserver] Task page: %s", obs.TaskURL())
	return obs
}
