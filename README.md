# objcli

对象存储统一 CLI：在 **AWS S3** 与**腾讯云 COS** 之间复制（cp）、移动（mv）、同步（sync）、删除（rm）、列举（ls）、预签名（presign）。
- 本地 ↔ 云、云 ↔ 云都支持
- 流式传输、不落盘、内存安全
- URL 风格命令，对齐 Linux `cp` / `rm` / `ls` 的习惯
- `--exclude` / `--include` glob 过滤（对齐 aws s3）
- `-o json` JSON 输出模式
- 对象属性：`-content-type` / `-cache-control` / `-metadata` / `-storage-class` / `-acl` / `-tag`
- 传输控制：`-chunk` 自适应、`-dry-run` 预览、`-size-only` / `--delete`（sync）

## 安装

```bash
git clone https://github.com/zhangyf/objcli.git
cd objcli
go build -o objcli .          # 默认构建（不含 taskobserver 监控）
```

启用 taskobserver 监控（可选，需本地 `../taskobserver` 源码）：

```bash
make build-obs                # 经 go.work.taskobserver 注入本地 taskobserver
```

## URL 格式

云端路径统一：

```
cos://<bucket>.<region>/<key-or-prefix>
s3://<bucket>.<region>/<key-or-prefix>
```

本地路径为普通文件系统路径（`/local/...` 或相对路径），仅 `cp` / `sync` 可用。

| URL                                              | 含义                       |
| ------------------------------------------------ | -------------------------- |
| `cos://my-bucket.ap-beijing/data/file.zip`       | 单对象                     |
| `cos://my-bucket.ap-beijing/data/`               | 前缀（以 `/` 结尾）        |
| `cos://my-bucket.ap-beijing/data/*`              | 通配符前缀                 |
| `cos://my-bucket.ap-beijing/`                    | 整个桶                     |
| `s3://my-bucket.us-east-1/path/`                 | S3 同理                    |

> 含 `*` 或以 `/` 结尾或 key 为空 → 当作前缀；`-r` 控制是否递归。

### 什么时候需要加引号？

“URL 是否要加引号”取决于 shell 是否会插手。**只要 key 中不包含 shell 特殊字符，不加引号也能跑**：

```bash
# ⚠️ 不含特殊字符 —— 两者等价
objcli cp cos://src.ap-singapore/data/ cos://dst.ap-beijing/backup/ -r -f      # ✅
objcli cp 'cos://src.ap-singapore/data/' cos://dst.ap-beijing/backup/ -r -f    # ✅
```

但如果 URL 里有这些字符，**必须加单引号**避免 shell 预处理：

| 字符   | shell 默认行为               | 例                                       |
| ------ | ----------------------- | ---------------------------------------- |
| `*`    | glob 展开，匹配本地文件 | `cos://b.r/data/*`                       |
| `?`    | 匹配单字符              | `cos://b.r/file?.zip`                    |
| `[ ]`  | 字符类匹配                | `cos://b.r/log[0-9].txt`                 |
| `$`    | 变量替换                | key 含 `$VAR`                           |
| 空格   | 切分参数                | key 中含空格                            |
| `&` `;` `|` | 控制操作符          | 很少出现但遇到会中断命令              |

```bash
# ✅ 安全写法
objcli cp 'cos://src.ap-singapore/data/*' cos://dst.ap-beijing/backup/ -r -f
objcli rm 'cos://my-bucket.ap-beijing/tmp/*' -r -f

# ❌ 危险写法：如果当前目录刚好有 cos:/ 目录，或开了 nullglob/failglob 会出错
objcli cp cos://src.ap-singapore/data/* cos://dst.ap-beijing/backup/ -r -f
```

**经验法则**：有疑问就加引号。

## 凭证

命令行参数优先，未填则从环境变量读取：

| 服务 | 命令行 flag           | 环境变量                 |
| ---- | --------------------- | ------------------------ |
| S3   | `-s3-ak` / `-s3-sk`   | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` |
| COS  | `-cos-id` / `-cos-sk` | `TENCENT_SECRET_ID` / `TENCENT_SECRET_KEY`    |

```bash
export AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=xxx
export TENCENT_SECRET_ID=xxx TENCENT_SECRET_KEY=xxx
```

## Endpoint / 域名

通用参数 **`-endpoint`** 适用于**所有子命令**、**COS / S3 两种存储**：

| 存储 | `-endpoint` 传什么 | 默认（不传） | 环境变量 |
| ---- | ------------------ | ----------- | -------- |
| COS  | **域名后缀**（不含 bucket），如 `cos-internal.ap-tokyo.tencentcos.cn` 走内网/VPC | 公网 `cos.<region>.myqcloud.com` | `COS_ENDPOINT` |
| S3   | **完整 endpoint URL**，如 `http://127.0.0.1:9000`（MinIO 等） | AWS 标准 endpoint | `AWS_ENDPOINT_URL` |

- cp / mv / sync 可分端指定：`-src-endpoint` / `-dst-endpoint`（优先于 `-endpoint`）。
- S3 专用 `-s3-endpoint` 仍保留，优先级：`-s3-endpoint` > `-endpoint` > `AWS_ENDPOINT_URL`。

```bash
# COS 默认走公网
objcli ls cos://my-bucket.ap-beijing/logs/ -r

# COS 走内网域名
objcli ls cos://my-bucket.ap-tokyo/logs/ -r -endpoint cos-internal.ap-tokyo.tencentcos.cn

# S3 兼容 endpoint
objcli ls s3://my-bucket.us-east-1/ -r -endpoint http://127.0.0.1:9000
```

## 退出码（对齐 Linux cp/rm/ls）

| 码 | 含义                                       |
| -- | ------------------------------------------ |
| 0  | 成功                                       |
| 1  | 操作过程中出错（部分或全部失败）           |
| 2  | 参数错误 / `ls` 找不到对象或前缀（ENOENT） |

## ls — 列举

```bash
objcli ls <TARGET> [-r]
```

输出列：

```
TYPE   SIZE   LAST-MODIFIED   ETAG   OBJECT
```

OBJECT 形如 `cos://bucket/key`（不含 region，方便复制粘贴）。

```bash
# 列举某前缀（不递归）
objcli ls cos://my-bucket.ap-beijing/logs/

# 递归列举
objcli ls cos://my-bucket.ap-beijing/logs/ -r

# 整个桶递归
objcli ls cos://my-bucket.ap-beijing/ -r

# 通配符 == 自动当前缀
objcli ls 'cos://my-bucket.ap-beijing/logs/*' -r

# 单对象（精确匹配）
objcli ls cos://my-bucket.ap-beijing/logs/2026-05-27.log
```

## cp — 拷贝

### 云 ↔ 云 单文件

```bash
# S3 → COS
objcli cp s3://my-s3.us-east-1/path/file.zip cos://my-cos.ap-beijing/path/file.zip

# DST 是前缀（以 / 结尾），自动拼源端文件名
objcli cp cos://src.ap-singapore/data/file.zip cos://dst.ap-beijing/backup/

# COS → COS（服务端拷贝，不过本机）
objcli cp cos://src.ap-singapore/x.zip cos://dst.ap-beijing/y.zip

# COS → S3
objcli cp cos://my-cos.ap-beijing/x.zip s3://my-s3.us-east-1/x.zip
```

### 前缀批量

```bash
# 整个目录迁移
objcli cp cos://src.ap-singapore/data/ cos://dst.ap-beijing/backup/ -r -f

# 通配符等价（必须加单引号）
objcli cp 'cos://src.ap-singapore/data/*' cos://dst.ap-beijing/backup/ -r -f

# 大文件多并发
objcli cp cos://src.ap-singapore/2026/ cos://dst.ap-beijing/2026/ \
  -r -f -chunk 512 -concurrency 8 -obj-concurrency 5
```

### URL 列表

列表中每行一个完整对象 URL（HTTPS 形式），自动解析来源，**无需指定 SRC**。

```bash
# 本地列表
objcli cp -key-list /path/to/list.txt cos://dst.ap-nanjing/import/

# 远程列表
objcli cp -key-list https://bucket.cos.ap-nanjing.myqcloud.com/list.log \
          cos://dst.ap-nanjing/import/
```

列表文件格式：

```
# 注释行自动跳过
https://src-bucket.cos.ap-singapore.myqcloud.com/data/file1.zip
https://src-bucket.s3.ap-southeast-1.amazonaws.com/data/file2.zip
https://another.cos.ap-beijing.myqcloud.com/logs/app.log
```

### 本地 ↔ 云

```bash
# 上传单文件
objcli cp /tmp/data.tar.gz cos://b.ap-beijing/backup/

# 上传目录
objcli cp /tmp/logs/ cos://b.ap-beijing/logs/ -r -f

# 下载单文件（DST 以 / 结尾 → 默认拼上原文件名）
objcli cp cos://b.ap-beijing/backup/data.tar.gz /tmp/

# 下载目录
objcli cp cos://b.ap-beijing/logs/ /tmp/logs/ -r -f
```

> 本地路径不带 `cos://` / `s3://` 前缀，**objcli 自动识别**。不支持本地 → 本地（请用系统 `cp`）。

### cp 选项

| 选项                  | 默认  | 说明                                |
| --------------------- | ----- | ----------------------------------- |
| `-r`                  | false | 前缀模式递归                        |
| `-f`                  | false | 前缀模式跳过确认                    |
| `-chunk`              | 0（自适应） | 分块大小 MB。0表示按总大小选取：<5GB→8 / 5-50GB→32 / 50-500GB→128 / >500GB→512 |
| `-concurrency`        | 5     | 单文件分块并发数                    |
| `-obj-concurrency`    | 3     | 多文件并发数（前缀/列表模式）       |
| `-key-list FILE`      |       | 列表文件路径或 URL                  |
| `-dry-run`            | false | 仅打印将要上传/拷贝/下载的动作，不真正执行 |
| `-endpoint` / `-src-endpoint` / `-dst-endpoint` | | 自定义 endpoint，参见上文《Endpoint / 域名》一节 |
| `-content-type` / `-cache-control` / `-metadata` / `-storage-class` / `-acl` / `-tag` | | 参见下文《对象属性》一节 |
| `-sse STR` | | 服务端加密：`AES256`（S3:SSE-S3 / COS:SSE-COS）\| `aws:kms`（S3）\| `cos/kms`（COS）。参见下文《服务端加密（SSE）》 |
| `-sse-kms-key STR` | | KMS CMK ID/ARN/Alias，仅在 `-sse=aws:kms` 或 `-sse=cos/kms` 时生效；为空走账号默认 key |
| `-ssec-key-file FILE` | | SSE-C 客户提供密钥文件，启用后读写/拷贝全程自动带 SSE-C 头。参见下文《服务端加密（SSE）》 |

## 对象属性（cp / mv / sync 共用）

上传/拷贝时可附加对象级元数据与存储选项，跨厂商拷贝（S3↔COS）也会透传。

```bash
objcli cp /tmp/index.html cos://b.ap-beijing/web/index.html \
  -content-type 'text/html; charset=utf-8' \
  -cache-control 'max-age=3600' \
  -metadata owner=lingbo -metadata purpose=test \
  -storage-class STANDARD_IA \
  -acl public-read \
  -tag env=prod -tag team=storage
```

| 选项 | 说明 |
| --- | --- |
| `-content-type STR` | 对象 Content-Type（空=云端自动推断） |
| `-cache-control STR` | HTTP Cache-Control |
| `-metadata KEY=VAL` | 用户自定义元数据（会透过 `x-amz-meta-` / `x-cos-meta-` 头部），**可重复** |
| `-storage-class CLS` | 存储类型（不区分大小写，本地按 provider 校验） |
| `-acl ACL` | canned ACL（不区分大小写，本地按 provider 校验） |
| `-tag KEY=VAL` | 对象级 Tag，**可重复** |

### `-storage-class` 可选枚举

S3 与 COS 枚举不同，objcli 按 provider 分别校验。误填会在上传前被本地拒绝，不会白跑云端。

| Provider | 可选值 |
| --- | --- |
| **S3** | `STANDARD` \| `STANDARD_IA` \| `ONEZONE_IA` \| `INTELLIGENT_TIERING` \| `GLACIER` \| `GLACIER_IR` \| `DEEP_ARCHIVE` \| `REDUCED_REDUNDANCY` \| `EXPRESS_ONEZONE` \| `OUTPOSTS` \| `SNOW` \| `FSX_ONTAP` \| `FSX_OPENZFS` |
| **COS** | `STANDARD` \| `STANDARD_IA` \| `INTELLIGENT_TIERING` \| `ARCHIVE` \| `DEEP_ARCHIVE` \| `MAZ_STANDARD` \| `MAZ_STANDARD_IA` \| `MAZ_INTELLIGENT_TIERING` \| `MAZ_ARCHIVE` |

> 注：MAZ_* 仅适用于多 AZ 桶。在单 AZ 桶上使用会被云端拒绝（`MAZOperationNotSupportOnSAZBucket`）。

### `-acl` 可选枚举

S3 和 COS 的 canned ACL 枚举也不同，objcli 同样按 provider 分别校验。

| Provider | 可选值 |
| --- | --- |
| **S3** (7) | `private` \| `public-read` \| `public-read-write` \| `authenticated-read` \| `aws-exec-read` \| `bucket-owner-read` \| `bucket-owner-full-control` |
| **COS** (4) | `default` \| `private` \| `public-read` \| `public-read-write` |

> 注：S3 桶如果启用了 BucketOwnerEnforced（AWS 2023+ 默认），会拒绝任何对象级 ACL 请求，使用前需在桶设置中关闭该选项。

## 服务端加密（SSE）

objcli 支持三种服务端加密模式，作用于上传 / 拷贝 / 分块的目标对象：

| 模式 | flag | 密钥保管 | 读取要求 | 适用 |
| --- | --- | --- | --- | --- |
| **SSE-S3 / SSE-COS** | `-sse AES256` | 云厂商托管 | 无感知 | 最省心，CDN 可用 |
| **SSE-KMS** | `-sse cos/kms`（COS）/ `-sse aws:kms`（S3），配 `-sse-kms-key`（可空走默认 CMK） | KMS 托管 | 无感知（需 KMS 权限） | 合规、密钥审计 |
| **SSE-C** | `-ssec-key-file FILE` | **你自己保管，云端不存** | **每次读取/拷贝都必须带同一把密钥** | 密钥完全自控 |

### SSE-C 密钥文件格式

`-ssec-key-file` 指向一个文件，内容为 32 字节 AES-256 密钥，支持三种编码，自动识别：

- **32 字节原始**（裸二进制，含空白字节也按原样处理，不 trim）
- **44 字节 base64**
- **64 字节 hex**

```bash
# 生成一把 32 字节随机密钥
head -c 32 /dev/urandom > key.bin
```

> ⚠️ **SSE-C 死规矩**：密钥由你自己保管，COS/S3 **不会存储**。密钥一旦丢失 = 数据**永久不可读**，没有任何找回手段。加密前务必把密钥文件备份到安全位置。

### 典型用法

```bash
# SSE-COS：上传时云厂商托管密钥加密
objcli cp ./data.zip cos://b.ap-beijing/data.zip -sse AES256

# SSE-KMS：用指定 CMK
objcli cp ./data.zip cos://b.ap-beijing/data.zip -sse cos/kms -sse-kms-key <CMK-ID>

# SSE-C：加密上传
objcli cp -ssec-key-file key.bin ./data.zip cos://b.ap-beijing/data.zip

# SSE-C：下载（必须带同一把密钥，否则 HEAD/GET 返回 400）
objcli cp -ssec-key-file key.bin cos://b.ap-beijing/data.zip ./data.zip
```

### 给存量数据重新加密（原地、纯云端、不下本机）

把一批**未加密**的存量对象就地加上 SSE-C：用**同桶同前缀自拷贝**触发服务端 Copy，COS 在云端内部读源对象、写回带 SSE-C 的新对象，数据**全程不经过本机带宽**。

```bash
objcli cp -ssec-key-file key.bin \
  'cos://你的桶.你的region/前缀/' \
  'cos://你的桶.你的region/前缀/' \
  -r -f -obj-concurrency 10
```

- 小文件走服务端 **`CopyObject`**，大文件（>chunk）走服务端 **`CopyPartFrom`**（分块 Copy），均在云端完成。
- 「源明文 → 目标 SSE-C」的非对称加密 Copy 受 COS 支持。
- 实测（OBJSTORE_DEBUG=true 抓请求）：整个过程只有 `HeadObject` + `CopyObject`/`CopyPartFrom`，**无 GetObject/PutObject、无 download/upload**，确认数据不下本机。

> 注意：日志里打印的 `模式: put` / `模式: multipart` 仅按对象大小区分，**不代表是否走服务端 Copy**；实际是否服务端 Copy 以 DEBUG 中出现 `CopyObject`/`CopyPartFrom` 为准。跨账号 / 跨 endpoint 等场景会自动降级为本机中转（此时数据过本机，并打印降级提示）。

## rm — 删除

```bash
# 单文件
objcli rm cos://my-bucket.ap-beijing/path/file.zip

# 前缀批量（带确认）
objcli rm 'cos://my-bucket.ap-beijing/tmp/' -r

# 前缀强制（跳过确认）
objcli rm 'cos://my-bucket.ap-beijing/tmp/*' -r -f

# 列表文件
objcli rm -key-list /path/to/del-list.txt
```

### rm 选项

| 选项                    | 默认  | 说明                          |
| ----------------------- | ----- | ----------------------------- |
| `-r`                    | false | 前缀模式递归                  |
| `-f`                    | false | 跳过确认                      |
| `-delete-concurrency`   | 3     | 并发删除数                    |
| `-url-decode`           | false | 列表模式下对 key 做 URL decode |
| `-key-list FILE`        |       | 列表文件路径或 URL            |
| `-dry-run`              | false | 仅打印将要删除的对象，不真正删除 |


## sync — 增量同步

```bash
objcli sync <SRC> <DST> [-r] [-delete] [-dry-run] [-size-only] [其他选项]
```

- 以 **ETag / size** 判断是否需要复制（ETag 为主，本地↔云回退 size）
- 默认不删除；`-delete` 后才会删除目标中多余的对象
- `-dry-run` 仅打印计划，不执行任何写操作
- `-size-only` 只比对 size，不比 ETag。适用于跨厂商 / multipart 上传后 ETag 不可靠的场景
- 云↔云、本地↔云都支持（本地↔本地不支持）

```bash
# 本地同步到云
objcli sync /local/dir/ cos://b.ap-beijing/backup/ -delete

# 云同步到本地
objcli sync cos://b.ap-beijing/data/ /local/data/ -delete

# 云与云间同步
objcli sync cos://b1.r1/data/ cos://b2.r2/data/

# 查看计划
objcli sync /local/dir/ cos://b.r/dir/ -dry-run

# 只按 size 增量
objcli sync s3://src.us-east-1/ cos://dst.ap-beijing/ -size-only
```

## presign — 预签名 URL

```bash
objcli presign <TARGET> [-method GET|PUT] [-expires SECONDS]
```

- 默认 `-method GET -expires 3600`
- 输出可直接 `curl` / `wget` / `PUT` 上传的 URL

```bash
# 生成 GET URL（1 小时有效）
objcli presign cos://my-bucket.ap-beijing/path/file.zip

# 生成 PUT URL（10 分钟有效）
objcli presign cos://my-bucket.ap-beijing/upload/x.bin -method PUT -expires 600

# 联合 curl 下载
URL=$(objcli presign cos://my-bucket.ap-beijing/file.zip -expires 60)
curl -o file.zip "$URL"

# 联合 curl 上传
URL=$(objcli presign cos://my-bucket.ap-beijing/upload/x.bin -method PUT -expires 600)
curl -X PUT --data-binary @./local.bin "$URL"
```

## mv — 移动

```bash
objcli mv <SRC> <DST> [选项]
```

- 语义：`mv` = `cp` + 复制成功后在源端删除
- 复制失败不会动源（避免丢文件）
- 路径类型组合同 `cp`，不支持本地 → 本地

```bash
# 云 → 云
objcli mv cos://src.ap-singapore/data/x.zip cos://dst.ap-beijing/x.zip

# 本地 → 云（上传完删本地）
objcli mv /tmp/data.tar.gz cos://b.ap-beijing/backup/

# 云 → 本地（下载完删云端）
objcli mv cos://b.ap-beijing/data/ /tmp/data/ -r -f
```

## resume — 断点续传状态管理

大文件上传/下载是分块进行的（multipart upload / download）。objcli 会在 `~/.objcli/resume/` 下记录进度状态，任务中断后重跑同一命令就会从上次位置续传。

但如果你不想续了，状态文件会**一直赖在本地**，云端已上传的分块也会**一直占着存储按量计费**。这时需要 `resume abort` 手动清理。

### `resume list` — 看看本地有哪些未完成任务

```bash
objcli resume list
```

列出你这台机器 `~/.objcli/resume/` 里记录的全部未完成任务（含 uploadID / 当前已传到哪里 / 本地路径等）。

### `resume abort` — 三种用法

#### 用法 1：丢弃单个任务

```bash
objcli resume abort <UPLOAD-ID>
```

适用场景：`resume list` 看到某个任务不想要了，拿它的 uploadID（或下载任务的本地路径）过来丢。会同时在**云端** abort 该上传（或本地删 .part）+ 删除本地状态文件。

#### 用法 2：`-all` — 丢弃本机全部未完成任务

```bash
objcli resume abort -all
```

走遍 `~/.objcli/resume/` 里的所有状态文件，逐个处理：

- 上传任务 → 在云端 abort，并删本地状态文件
- 下载任务 → 删本地 .part 临时文件和状态文件

适用场景：这台机器全面**退出**上下载任务，不打算续了。例如老机器运侜 / 开发调试完清现场。

> ⚠️ 只能处理本地状态文件里记录的任务。其他机器上起的 / 状态文件丢了的 → 看用法 3。

#### 用法 3：`-all-cloud` — 扫云端孤儿清理

```bash
objcli resume abort -all-cloud -url cos://my-bucket.ap-beijing/ [-dry-run] [-f]
```

适用场景：云端上可能存在**本地状态文件跟不到**的未完成 multipart uploads（他们一直在扣错量存储费）。常见来源：

- 别的机器 / 同事在另一台电脑跳上传到一半崩掉了
- 别的工具（aws cli / coscmd / SDK 脚本）起的 multipart，objcli 本地根本不知道
- objcli 状态文件被 `kill -9` 后丢了 / 被误删了
- 换了台电脑，没拷贝 `~/.objcli/resume/`

这个命令**调云端 API**（`ListMultipartUploads`）拿一个桶里**所有**未完成上传的权威列表，不依赖本地状态文件，然后逐个 abort。

```bash
# 步骤 1：先看看多少个孤儿（默认 dry-run 不动云端）
objcli resume abort -all-cloud -url cos://my-bucket.ap-beijing/ -dry-run

# 可以限定前缀，只扫某个子目录
objcli resume abort -all-cloud -url cos://my-bucket.ap-beijing/data/ -dry-run

# 步骤 2：看过列表后真清理（-f 跳过交互确认）
objcli resume abort -all-cloud -url cos://my-bucket.ap-beijing/ -f

# S3 同理
objcli resume abort -all-cloud -url s3://my-bucket.us-east-1/ -f
```

输出示例：

```
🔍 扫描 cos://my-bucket/data/ ...

发现 3 个未完成上传：
  - data/big1.bin  uploadID=17799577...  initiated=2026-05-28 16:42:28
  - data/big2.bin  uploadID=17799577...  initiated=2026-05-28 16:42:28
  - data/big3.bin  uploadID=17799577...  initiated=2026-05-28 16:42:28

  [✓] data/big1.bin (17799577...)
  [✓] data/big2.bin (17799577...)
  [✓] data/big3.bin (17799577...)

云端清理完成：成功 3 / 失败 0
```

> ⚠️ 谨慎：`-all-cloud` 拿到的是云端所有未完成上传，可能包含**别人正在正常跑的任务**。务必先 `-dry-run` 看 initiated 时间戳，确认这些任务都已是孤儿了再 -f 真执行。

### 三种用法一表看不同

| 命令 | 作用范围 | 是否需要本地状态文件 | 适用场景 |
| --- | --- | --- | --- |
| `resume abort <UPLOAD-ID>` | 某个任务 | 是 | 选择性丢弃 |
| `resume abort -all` | 本地状态文件里的全部 | 是 | 本机全面退出 |
| `resume abort -all-cloud -url ...` | 云端一个桶/前缀的全部 | **否** | 跨机器/跨工具的孤儿清理 |

## --exclude / --include 过滤

与 **aws s3** 语义对齐，适用于 `cp` / `mv` / `sync` / `rm` / `ls`：

- 默认全部包含
- `-exclude PAT` 按顺序排除匹配的对象
- `-include PAT` 按顺序重新包含匹配的对象
- 可反复交错使用，最后一次判定生效

```bash
# 只传 *.txt
objcli cp /local/dir/ cos://b.r/dir/ -r -f -exclude '*' -include '*.txt'

# 传除 *.log 以外的全部
objcli sync /local/dir/ cos://b.r/dir/ -exclude '*.log'

# 只删 *.tmp
objcli rm cos://b.r/cache/ -r -f -exclude '*' -include '*.tmp'
```

模式语法（跨路径 fnmatch、与 aws 一致）：

| 符号  | 含义                                |
| ----- | ----------------------------------- |
| `*`   | 任意字符（含 `/`）                  |
| `**`  | 同 `*`，仅为反向兼容保留                |
| `?`   | 单字符（含 `/`）                    |
| `[a-z]` `[^abc]` | 字符类              |

## -o json 输出模式

所有子命令均支持 `-o json` / `--output json`，进度日志走 stderr，最终结果走 stdout JSON。

```bash
objcli ls cos://b.r/dir/ -r -o json
objcli cp ... -o json
objcli presign cos://b.r/file.zip -o json
```

输出示例：

```json
{
  "objects": [
    {
      "provider": "cos",
      "bucket": "my-bucket",
      "key": "data/file.zip",
      "url": "cos://my-bucket/data/file.zip",
      "size": 12345,
      "last_modified": "2026-05-28T03:46:36Z",
      "etag": "...",
      "storage_class": "STANDARD"
    }
  ],
  "count": 1
}
```

## taskobserver（cp 可选监控）

> ⚠️ **可选编译**：taskobserver 监控默认**不编入**二进制。
> - 默认构建（`make build` / `go build`）：所有 `-obs-*` 参数与 `TASKOBS_*` 环境变量被忽略（no-op）。
> - 启用构建（`make build-obs`）：需本地提供 `../taskobserver` 源码（经 `go.work.taskobserver` 注入），才会真正接入监控。

| 命令行              | 环境变量              |
| ------------------- | --------------------- |
| `-obs-bucket`       | `TASKOBS_BUCKET`      |
| `-obs-region`       | `TASKOBS_REGION`      |
| `-obs-secret-id`    | `TASKOBS_SECRET_ID`   |
| `-obs-secret-key`   | `TASKOBS_SECRET_KEY`  |
| `-obs-base-url`     | `TASKOBS_BASE_URL`    |
| `-obs-task`         | `TASKOBS_TASK`        |

任意一组凭证齐全即启用（仅在启用构建下生效），启动时打印 Overview / Task 页面 URL。

## 内存安全

启动时会预估最坏情况内存占用（大文件分块并发 × 多文件并发 × chunk × 2，COS→COS 不算入因为是服务端拷贝）。
超过 4 GiB 上限直接拒绝并给出建议。

## 常见用法速查

```bash
# 1) 把整个目录从 COS Singapore 迁到 COS Beijing
objcli cp cos://src.ap-singapore/ cos://dst.ap-beijing/ -r -f -chunk 512

# 2) 列出某前缀下所有对象（递归）
objcli ls cos://my-bucket.ap-beijing/logs/ -r

# 3) 清空某前缀
objcli rm cos://my-bucket.ap-beijing/tmp/ -r -f

# 4) 按列表批量复制
objcli cp -key-list list.txt cos://dst.ap-nanjing/import/

# 5) S3 → COS 单文件
objcli cp s3://my-s3.us-east-1/file.zip cos://my-cos.ap-beijing/file.zip

# 6) 含通配符 → 加单引号避免 shell 展开
objcli rm 'cos://my-bucket.ap-beijing/tmp/*' -r -f
```

## 子命令帮助

```bash
objcli           # 总览
objcli cp -h     # cp 详细帮助
objcli rm -h     # rm 详细帮助
objcli ls -h     # ls 详细帮助
```
