# objcli

对象存储统一 CLI：在 **AWS S3** 与**腾讯云 COS** 之间复制（cp）、删除（rm）、列举（ls）对象。
- 流式传输、不落盘、内存安全
- URL 风格命令，对齐 Linux `cp` / `rm` / `ls` 的习惯
- 自动选择 PutObject / Multipart / UploadPart-Copy（cos→cos 服务端拷贝，不过本机）

## 安装

```bash
git clone https://github.com/zhangyf/objcli.git
cd objcli
go build -o objcli .
```

## URL 格式

所有源 / 目标都用统一 URL 形式：

```
cos://<bucket>.<region>/<key-or-prefix>
s3://<bucket>.<region>/<key-or-prefix>
```

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

### 单文件

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

### cp 选项

| 选项                  | 默认  | 说明                                |
| --------------------- | ----- | ----------------------------------- |
| `-r`                  | false | 前缀模式递归                        |
| `-f`                  | false | 前缀模式跳过确认                    |
| `-chunk`              | 128   | 分块大小 MB（cos→cos 建议 512）     |
| `-concurrency`        | 5     | 单文件分块并发数                    |
| `-obj-concurrency`    | 3     | 多文件并发数（前缀/列表模式）       |
| `-key-list FILE`      |       | 列表文件路径或 URL                  |

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

## taskobserver（cp 可选监控）

| 命令行              | 环境变量              |
| ------------------- | --------------------- |
| `-obs-bucket`       | `TASKOBS_BUCKET`      |
| `-obs-region`       | `TASKOBS_REGION`      |
| `-obs-secret-id`    | `TASKOBS_SECRET_ID`   |
| `-obs-secret-key`   | `TASKOBS_SECRET_KEY`  |
| `-obs-base-url`     | `TASKOBS_BASE_URL`    |
| `-obs-task`         | `TASKOBS_TASK`        |

任意一组凭证齐全即启用，启动时打印 Overview / Task 页面 URL。

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
