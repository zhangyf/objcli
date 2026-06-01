package cmd

import (
	"strings"
)

// MatchFilter 实现 aws s3 风格的 --exclude / --include 顺序过滤
//
// 规则（与 aws s3 行为一致）：
//   - 默认所有对象都包含
//   - 按命令行声明顺序处理 patterns
//   - 每个 -exclude p：匹配 p 的对象被标记为排除
//   - 每个 -include p：匹配 p 的对象被标记为包含
//   - 最后一次决策生效
//
// 模式：
//   - * 匹配除 / 之外的任意字符
//   - **          匹配任意路径（含 /）
//   - ?           匹配单字符
//   - 字符串字面量精确匹配
type FilterRule struct {
	Pattern string
	Exclude bool // true = exclude, false = include
}

type MatchFilter struct {
	rules []FilterRule
}

func NewMatchFilter() *MatchFilter {
	return &MatchFilter{}
}

func (f *MatchFilter) AddExclude(pattern string) {
	f.rules = append(f.rules, FilterRule{Pattern: pattern, Exclude: true})
}

func (f *MatchFilter) AddInclude(pattern string) {
	f.rules = append(f.rules, FilterRule{Pattern: pattern, Exclude: false})
}

// HasRules 是否有任何过滤规则（无 → 全部包含）
func (f *MatchFilter) HasRules() bool {
	return len(f.rules) > 0
}

// Match 判断 relKey 是否被包含（true = 包含，false = 排除）
// relKey 应为相对路径（去除前缀后），统一用 / 分隔
func (f *MatchFilter) Match(relKey string) bool {
	if !f.HasRules() {
		return true
	}
	included := true // 默认包含
	for _, r := range f.rules {
		if globMatch(r.Pattern, relKey) {
			if r.Exclude {
				included = false
			} else {
				included = true
			}
		}
	}
	return included
}

// globMatch 跨路径 fnmatch （与 aws s3 cp --exclude/--include 语义对齐）
//   - **       任意（仅为了反向兼容）
//   - *        跨路径匹配任意字符（含 /，与 aws 行为一致）
//   - ?        单字符（含 /）
//   - 其他     字面量
func globMatch(pattern, name string) bool {
	// ** 同义于 *
for strings.Contains(pattern, "**") {
		pattern = strings.ReplaceAll(pattern, "**", "*")
	}
	return matchFNM(pattern, name)
}

// matchFNM 递归的 fnmatch 实现，* 跨路径
func matchFNM(pattern, name string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			// 压缩连续的 *
			for len(pattern) > 0 && pattern[0] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 0 {
				return true
			}
			for i := 0; i <= len(name); i++ {
				if matchFNM(pattern, name[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(name) == 0 {
				return false
			}
			name = name[1:]
			pattern = pattern[1:]
		case '[':
			end := strings.IndexByte(pattern, ']')
			if end < 0 || len(name) == 0 {
				return false
			}
			class := pattern[1:end]
			if !inCharClass(name[0], class) {
				return false
			}
			name = name[1:]
			pattern = pattern[end+1:]
		default:
			if len(name) == 0 || pattern[0] != name[0] {
				return false
			}
			name = name[1:]
			pattern = pattern[1:]
		}
	}
	return len(name) == 0
}

func inCharClass(c byte, class string) bool {
	neg := false
	if len(class) > 0 && (class[0] == '^' || class[0] == '!') {
		neg = true
		class = class[1:]
	}
	matched := false
	for i := 0; i < len(class); i++ {
		if i+2 < len(class) && class[i+1] == '-' {
			if c >= class[i] && c <= class[i+2] {
				matched = true
				break
			}
			i += 2
		} else if class[i] == c {
			matched = true
			break
		}
	}
	if neg {
		return !matched
	}
	return matched
}

// StringSliceFlag 实现 flag.Value，支持 -exclude 多次出现
type StringSliceFlag []string

func (s *StringSliceFlag) String() string {
	if s == nil {
		return ""
	}
	return strings.Join(*s, ",")
}

func (s *StringSliceFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}

// KeyValueListFlag 专门用于 -metadata / -tag 这类“可重复 且 可逗号列表”的 flag。
// 语义：
//   -metadata k1=v1,k2=v2          → ["k1=v1", "k2=v2"]
//   -metadata k1=v1 -metadata k2=v2 → ["k1=v1", "k2=v2"]
// 转义：
//   值中含 , 或 = 时，用 \, 或 \= 转义。
// 存储格式仍为 ["k=v", ...]，后续使用端不需改动。
type KeyValueListFlag []string

func (k *KeyValueListFlag) String() string {
	if k == nil {
		return ""
	}
	return strings.Join(*k, ",")
}

func (k *KeyValueListFlag) Set(v string) error {
	parts := splitKVList(v)
	*k = append(*k, parts...)
	return nil
}

// splitKVList 按未转义逗号拆分；\, / \= 会被还原为 , / =。
func splitKVList(s string) []string {
	var out []string
	var cur strings.Builder
	escape := false
	flush := func() {
		t := strings.TrimSpace(cur.String())
		if t != "" {
			out = append(out, t)
		}
		cur.Reset()
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if escape {
			cur.WriteByte(c)
			escape = false
			continue
		}
		switch c {
		case '\\':
			escape = true
		case ',':
			flush()
		default:
			cur.WriteByte(c)
		}
	}
	flush()
	return out
}