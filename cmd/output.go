package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// OutputMode 控制 CLI 输出格式
type OutputMode string

const (
	OutputText OutputMode = "text"
	OutputJSON OutputMode = "json"
)

var (
	currentOutputMu sync.Mutex
	currentOutput   = OutputText
)

func SetOutput(mode OutputMode) {
	currentOutputMu.Lock()
	defer currentOutputMu.Unlock()
	currentOutput = mode
}

func GetOutput() OutputMode {
	currentOutputMu.Lock()
	defer currentOutputMu.Unlock()
	return currentOutput
}

// IsJSON 是否 JSON 输出
func IsJSON() bool {
	return GetOutput() == OutputJSON
}

// EmitJSON 输出 JSON 到 stdout
func EmitJSON(v interface{}) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// EmitResult 通用结果输出（自动判 text/json）
//
//	JSON 模式：直接输出 result 对象到 stdout
//	文本模式：调用 textFn 打印
func EmitResult(result interface{}, textFn func()) {
	if IsJSON() {
		EmitJSON(result)
		return
	}
	if textFn != nil {
		textFn()
	}
}

// LogProgress 进度日志（始终走 stderr，不污染 JSON 输出）
func LogProgress(format string, args ...interface{}) {
	if IsJSON() {
		// JSON 模式仍然把进度写 stderr，方便调试
		fmt.Fprintf(os.Stderr, format, args...)
		if len(format) == 0 || format[len(format)-1] != '\n' {
			fmt.Fprintln(os.Stderr)
		}
	} else {
		fmt.Printf(format, args...)
		if len(format) == 0 || format[len(format)-1] != '\n' {
			fmt.Println()
		}
	}
}