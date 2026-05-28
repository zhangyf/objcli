# objcli TODO

> 所有待办已迁移到 GitHub Issues：https://github.com/zhangyf/objcli/issues
>
> 此文件仅作总览索引。新需求/bug 直接在 issues 提，不再更新 TODO.md。

---

## P0 — 真实痛点 / 高价值

| # | 标题 |
|---|------|
| [#1](https://github.com/zhangyf/objcli/issues/1) | 退出码不准确：上传/续传失败时仍 exit=0 |
| [#2](https://github.com/zhangyf/objcli/issues/2) | 下载也支持断点续传 |

## P1 — 常用但缺

| # | 标题 |
|---|------|
| [#3](https://github.com/zhangyf/objcli/issues/3) | 对象元数据：Content-Type / Cache-Control / 用户元数据 |
| [#4](https://github.com/zhangyf/objcli/issues/4) | 存储类型 (Storage Class) 设置 |
| [#5](https://github.com/zhangyf/objcli/issues/5) | 服务端加密 (SSE) |
| [#6](https://github.com/zhangyf/objcli/issues/6) | 进度条 / ETA |
| [#7](https://github.com/zhangyf/objcli/issues/7) | chunk 大小自适应 |
| [#8](https://github.com/zhangyf/objcli/issues/8) | AWS Profile / IAM Role / STS 临时凭证支持 |
| [#9](https://github.com/zhangyf/objcli/issues/9) | stat / head 单独子命令 |

## P2 — 锦上添花

| # | 标题 |
|---|------|
| [#10](https://github.com/zhangyf/objcli/issues/10) | mb / rb 桶操作 |
| [#11](https://github.com/zhangyf/objcli/issues/11) | cat / pipe 管道支持 |
| [#12](https://github.com/zhangyf/objcli/issues/12) | 批量 multipart upload abort（云端残留扫描） |
| [#13](https://github.com/zhangyf/objcli/issues/13) | 跨账号 / 跨 endpoint 同 provider 拷贝 |
| [#14](https://github.com/zhangyf/objcli/issues/14) | dry-run 模式 |
| [#15](https://github.com/zhangyf/objcli/issues/15) | sync --delete / --size-only |
| [#16](https://github.com/zhangyf/objcli/issues/16) | 对象级 ACL（公有读/私有） |
| [#17](https://github.com/zhangyf/objcli/issues/17) | 对象级 tag |

## P3 — 边角

| # | 标题 |
|---|------|
| [#18](https://github.com/zhangyf/objcli/issues/18) | bash/zsh completion 自动补全 |
| [#19](https://github.com/zhangyf/objcli/issues/19) | man page |
| [#20](https://github.com/zhangyf/objcli/issues/20) | version 子命令（带 git commit + build time） |
| [#21](https://github.com/zhangyf/objcli/issues/21) | config 配置文件持久化默认 region/credentials |
| [#22](https://github.com/zhangyf/objcli/issues/22) | 限速 -bandwidth 50MB/s |
| [#23](https://github.com/zhangyf/objcli/issues/23) | 重试退避策略可调 |

---

## 已修复（仅作历史归档）

- ~~cmd/rm 没接入 main.go~~ → 已接入
- ~~大文件上传断点续传~~ → c7e0902
- ~~ETag 引号格式不一致~~ → objstore v0.9.1 / v0.9.2
- ~~S3 chunk<5MB 才在 CompleteMultipart 时报错~~ → 6b9d412 提前校验
- ~~-s3-endpoint flag 缺失~~ → 8c32a61
- ~~跨厂商大文件 cp 报 src must also be a COS store~~ → 348a149（双向实测一致：S3 sg → COS bj 200MB/5s/41.7MB/s；COS bj → S3 sg 200MB/4.7s/45.1MB/s）
