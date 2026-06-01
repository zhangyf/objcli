package cmd

import (
	"errors"
	"testing"
)

func TestIsCrossAccountServerCopyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"random network", errors.New("connection reset"), false},
		{"timeout", errors.New("context deadline exceeded"), false},
		{"5xx", errors.New("500 InternalError: server busy"), false},
		{"NotFound", errors.New("NoSuchKey: object missing"), false},

		// 真实 COS 跨账号 CopyObject 报错（含 "Failed to query the state"）
		{"cos failed to query source",
			errors.New(`PUT https://x.cos-internal.ap-tokyo.tencentcos.cn/k: 403 AccessDenied(Message: Failed to query the state of source object, RequestId: ...)`),
			true,
		},

		// 真实 COS 跨账号 CopyObject 报错（仅 Access Denied，不带 source 关键字）
		{"cos plain access denied",
			errors.New(`PUT https://x.cos-internal.ap-tokyo.tencentcos.cn/k: 403 AccessDenied(Message: Access Denied., RequestId: ...)`),
			true,
		},

		// S3 跨账号 CopyObject 典型报错
		{"s3 access denied on copysource",
			errors.New(`api error AccessDenied: Access Denied to source x-amz-copy-source`),
			true,
		},

		// 403 Forbidden 也算
		{"403 Forbidden",
			errors.New(`StatusCode: 403, Forbidden`),
			true,
		},

		// 签名不匹配（跨 endpoint 跨 region 偶发）
		{"signature mismatch",
			errors.New(`SignatureDoesNotMatch: The request signature we calculated does not match`),
			true,
		},

		// 401/Unauthorized
		{"unauthorized",
			errors.New(`Unauthorized: token expired`),
			true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isCrossAccountServerCopyError(tc.err)
			if got != tc.want {
				t.Fatalf("isCrossAccountServerCopyError(%q) = %v, want %v",
					tc.err, got, tc.want)
			}
		})
	}
}
