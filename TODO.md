# objcli TODO

记录待补全/已识别但未实现的能力。优先级按价值密度评估。

> 当前已支持：`cp` / `mv` / `sync` / `rm` / `ls` / `presign` / `resume`
> 双引擎：AWS S3（含 S3 兼容如 MinIO）+ 腾讯云 COS
> 大文件上传支持断点续传

---

## P0 — 真实痛点 / 高价值

### [feat] 退出码不准确
**症状**：上传/续传遇到错误时（如 `EntityTooSmall`、`S3 multipart 上传 chunk < 5MB`），主进程仍然 `exit=0`。

**现状**：`local.go` 里 `firstErr` 已经记录但 `cmdCP` 路径没把它向上 propagate。

**方案**：cp / mv / sync 的命令分支返回 firstErr，main.go 用 errToExit() 统一转换。需对照 ls/rm 已有的退出码处理。

**影响**：脚本 / CI 无法正确判断成败。

---

### [feat] 下载也支持断点续传
**症状**：上传支持 resume 但下载（GetObject → 本地文件）不支持。下载 45 GiB 文件中断要从头来。

**方案**：用 `objstore.GetRange(start, end)` + 本地 `os.OpenFile(O_RDWR)` 按段写。状态文件复用 `~/.objcli/resume/`，类型字段区分 upload/download。

**影响**：大文件下载场景（典型用例：从 S3 拉日志/备份到本地）。

---

## P1 — 常用但目前缺

### [feat] 对象元数据 / Content-Type / Cache-Control / 用户元数据
**现状**：上传时不能指定 `Content-Type`，云端要么自动猜要么默认 application/octet-stream。

**方案**：cp/mv/sync 加 flag：
- `-content-type STR`
- `-cache-control STR`
- `-metadata key=value`（可重复）

**影响**：前端构建产物上传 CDN 必备；归档文件分类必备。

---

### [feat] 存储类型（Storage Class）
**现状**：上传一律默认 Standard。无法走 IA / Archive / DeepArchive。

**方案**：`-storage-class STANDARD|STANDARD_IA|GLACIER|...`，COS / S3 各自映射。

**影响**：归档备份成本优化。

---

### [feat] 服务端加密
**方案**：`-sse AES256` / `-sse aws:kms` / `-sse-kms-key ID`。COS 类似。

**影响**：合规场景必备。

---

### [feat] 进度条 / ETA
**现状**：大文件上传只输出 `[upload]` 一行，中间没任何回显。

**方案**：默认在 stderr 显示一行刷新进度（已传/总量、速度、ETA）。`-q` 禁用。
注意不要污染 stdout（json 输出场景）。

**影响**：用户体验，尤其跨境大文件。

---

### [feat] 单段大小自适应
**现状**：用户得手动 `-chunk` 选合适的分块大小，太小会触发 S3 5MB 限制或者总段数超 10000。

**方案**：
- 默认根据 total size 自动选 chunk：
  - <5GB → 8MB
  - 5-50GB → 32MB
  - 50-500GB → 128MB
  - 500GB-5TB → 512MB
- 用户显式 `-chunk` 时跳过自适应，只做合法性校验。

---

### [feat] AWS Profile / IAM Role / STS 临时凭证支持
**现状**：只支持 `-s3-ak / -s3-sk` 或 `AWS_ACCESS_KEY_ID/SECRET`。
没有 `-profile` / `~/.aws/credentials` / IMDS / AssumeRole。

**方案**：
- 引入 `-aws-profile NAME`
- AK/SK 都不给时自动走 awssdk default chain（profile + IMDS + STS）

**影响**：在 EC2 / EKS 上跑必备。

---

### [feat] HEAD/STAT 单独子命令
**现状**：要看一个对象的 size + etag + lastModified，只能 ls 整个前缀过滤。

**方案**：`objcli stat <URL>` 或 `objcli head <URL>` → 输出 size / etag / lastModified / content-type / metadata。

---

## P2 — 锦上添花

### [feat] mb / rb 桶操作
- `objcli mb cos://newbucket.ap-beijing/`
- `objcli rb cos://emptybucket.ap-beijing/`

只对运维向用户有价值。

### [feat] cat / pipe
- `objcli cat s3://b.r/file.txt` 直接输出到 stdout
- 支持 `-` 表示 stdin/stdout（流式管道）

### [feat] 批量 multipart upload abort
**现状**：`resume abort` 只能按 uploadID。云端可能残留不在本地状态的 incomplete uploads（其他客户端、其他设备遗留）。

**方案**：`objcli resume abort -all-cloud -url cos://b.r/` 用 ListMultipartUploads API 拉全量后挨个 abort。

### [feat] 跨账号 / 跨 endpoint 同 provider 拷贝
**现状**：cp 内部只构造一对 store。跨账号 S3↔S3 时 src 和 dst 用同一组 ak/sk 不行。

**方案**：`-src-s3-ak / -src-s3-sk / -dst-s3-ak / -dst-s3-sk` 分别指定。

### [feat] dry-run 模式
**方案**：`-n` / `--dry-run` 模拟执行，输出会做什么但不真做。
对 sync / rm -r 尤其有价值。

### [feat] sync 的 --delete 和 --size-only
**现状**：sync 是单向覆盖，不会删 dst 多余对象。
**方案**：`--delete` 同 awscli 行为，`--size-only` 只比 size 不比 mtime。

### [feat] 对象级 ACL（公有读/私有）
- `-acl public-read | private | bucket-owner-full-control`
- 静态站点上传场景常用。

### [feat] 对象级 tag
- `-tag key=value`（可多次）

---

## P3 — 边角 / 不重要

- bash/zsh completion 自动补全脚本
- man page
- `objcli version` 子命令（带 git commit + build time）
- `objcli config` 持久化默认 region/credentials（cli 配置文件）
- 限速 `-bandwidth 50MB/s`
- 重试退避策略可调（`-retries N -retry-base-ms M`）

---

## 已知 / 已修

- ~~cmd/rm 没接入 main.go~~ → 已接入
- ~~大文件断点续传~~ → c7e0902
- ~~ETag 引号格式不一致~~ → objstore v0.9.1 / v0.9.2
- ~~S3 chunk<5MB 才在 CompleteMultipart 时报错~~ → 6b9d412 提前校验
- ~~-s3-endpoint flag 缺失~~ → 8c32a61
- ~~跨厂商大文件 cp~~ → 之前不是实现缺失，是 cp 路径里 `src.(ServerCopier)/dst.(ServerCopier)` 双断言都会过（两边都实现了），导致跨厂商依然入 CopyPartFrom 被抳在 cosStore 内部。改为先比较 `src.Provider() == dst.Provider()` 才走服务端复制，跨厂商走 fallback 流式（`MultipartUpload(fetchPart=src.GetRange)`）。实测 AWS S3 sg → COS bj 200MB 五秒 41.7MB/s、MD5 一致 ✅

---

最后更新：2026-05-28
