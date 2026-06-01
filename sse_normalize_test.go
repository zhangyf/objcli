package main

import "testing"

func TestNormalizeSSE(t *testing.T) {
	cases := []struct {
		name      string
		sse       string
		key       string
		provider  string
		wantSSE   string
		wantKey   string
		wantErr   bool
	}{
		// 通用
		{"empty", "", "", "s3", "", "", false},
		{"empty + key error", "", "alias/x", "s3", "", "", true},
		{"unknown provider", "AES256", "", "gcs", "", "", true},

		// S3
		{"s3 sse-s3", "AES256", "", "s3", "AES256", "", false},
		{"s3 sse-s3 case-insensitive", "aes256", "", "s3", "AES256", "", false},
		{"s3 sse-kms with key", "aws:kms", "alias/key1", "s3", "aws:kms", "alias/key1", false},
		{"s3 sse-kms without key (account default)", "aws:kms", "", "s3", "aws:kms", "", false},
		{"s3 sse-kms-dsse", "aws:kms:dsse", "", "s3", "aws:kms:dsse", "", false},
		{"s3 reject cos/kms", "cos/kms", "", "s3", "", "", true},
		{"s3 sse-s3 + key error", "AES256", "alias/x", "s3", "", "", true},

		// COS
		{"cos sse-cos", "AES256", "", "cos", "AES256", "", false},
		{"cos sse-kms with key", "cos/kms", "uuid-1234", "cos", "cos/kms", "uuid-1234", false},
		{"cos sse-kms no key", "cos/kms", "", "cos", "cos/kms", "", false},
		{"cos reject aws:kms", "aws:kms", "", "cos", "", "", true},
		{"cos sse-cos + key error", "AES256", "k", "cos", "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotSSE, gotKey, err := normalizeSSE(tc.sse, tc.key, tc.provider)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got sse=%q key=%q", gotSSE, gotKey)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotSSE != tc.wantSSE {
				t.Fatalf("sse=%q want %q", gotSSE, tc.wantSSE)
			}
			if gotKey != tc.wantKey {
				t.Fatalf("kmsKey=%q want %q", gotKey, tc.wantKey)
			}
		})
	}
}
