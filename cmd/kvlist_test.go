package cmd

import (
	"reflect"
	"testing"
)

func TestSplitKVList(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"single", "k=v", []string{"k=v"}},
		{"two", "k1=v1,k2=v2", []string{"k1=v1", "k2=v2"}},
		{"three", "a=1,b=2,c=3", []string{"a=1", "b=2", "c=3"}},
		{"trailing-comma", "k=v,", []string{"k=v"}},
		{"leading-comma", ",k=v", []string{"k=v"}},
		{"spaces-trimmed", " k1=v1 , k2=v2 ", []string{"k1=v1", "k2=v2"}},
		{"escape-comma", `k=v\,with\,commas`, []string{"k=v,with,commas"}},
		{"escape-eq", `k=v\=eq`, []string{"k=v=eq"}},
		{"mixed", `k1=v1,k2=v\,with\,commas,k3=v3`, []string{"k1=v1", "k2=v,with,commas", "k3=v3"}},
		{"backslash-not-escape-non-special", `k=v\.txt`, []string{`k=v.txt`}}, // \. → .
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := splitKVList(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("splitKVList(%q) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestKeyValueListFlag_Set(t *testing.T) {
	cases := []struct {
		name  string
		calls []string
		want  []string
	}{
		{"single-call-csv", []string{"k1=v1,k2=v2"}, []string{"k1=v1", "k2=v2"}},
		{"multi-call", []string{"k1=v1", "k2=v2"}, []string{"k1=v1", "k2=v2"}},
		{"mixed", []string{"k1=v1,k2=v2", "k3=v3"}, []string{"k1=v1", "k2=v2", "k3=v3"}},
		{"escape", []string{`note=hello\,world`}, []string{"note=hello,world"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var f KeyValueListFlag
			for _, c := range tc.calls {
				if err := f.Set(c); err != nil {
					t.Fatalf("Set(%q): %v", c, err)
				}
			}
			if !reflect.DeepEqual([]string(f), tc.want) {
				t.Errorf("got %#v, want %#v", []string(f), tc.want)
			}
		})
	}
}

// 兜底：StringSliceFlag 仍然保持 append 全值不分隔（exclude/include 不能误伤）
func TestStringSliceFlag_NoCommaSplit(t *testing.T) {
	var f StringSliceFlag
	_ = f.Set("a,b,c")
	if len(f) != 1 || f[0] != "a,b,c" {
		t.Errorf("StringSliceFlag should NOT split on comma, got %#v", []string(f))
	}
}
