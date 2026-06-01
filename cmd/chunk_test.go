package cmd

import "testing"

const (
	mb = 1024 * 1024
	gb = 1024 * mb
)

func TestResolveChunkSize_Adaptive(t *testing.T) {
	cases := []struct {
		name    string
		chunkMB int
		size    int64
		want    int64
	}{
		{"explicit 16MB ignores size", 16, 100 * gb, 16 * mb},
		{"adaptive small (<5GB) → 8MB", 0, 100 * mb, 8 * mb},
		{"adaptive boundary 4.99GB → 8MB", 0, 5*gb - 1, 8 * mb},
		{"adaptive 5GB → 32MB", 0, 5 * gb, 32 * mb},
		{"adaptive 30GB → 32MB", 0, 30 * gb, 32 * mb},
		{"adaptive 50GB → 128MB", 0, 50 * gb, 128 * mb},
		{"adaptive 200GB → 128MB", 0, 200 * gb, 128 * mb},
		{"adaptive 500GB → 512MB", 0, 500 * gb, 512 * mb},
		{"adaptive 2TB → 512MB", 0, 2 * 1024 * gb, 512 * mb},

		// 关键回归：chunkMB=0 & 极小文件，必须返回非 0 的值
		// （issue #25：以前这里返回 0，导致单文件 cp 走 multipart 分支后 ChunkSize=0 除零 panic）
		{"adaptive 38B → 8MB (issue #25 regression)", 0, 38, 8 * mb},
		{"adaptive 0B → 8MB", 0, 0, 8 * mb},
		{"negative chunkMB falls into adaptive", -1, 100 * mb, 8 * mb},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveChunkSize(tc.chunkMB, tc.size)
			if got != tc.want {
				t.Fatalf("resolveChunkSize(%d, %d) = %d, want %d",
					tc.chunkMB, tc.size, got, tc.want)
			}
			if got <= 0 {
				t.Fatalf("resolveChunkSize must never return <=0, got %d", got)
			}
		})
	}
}
