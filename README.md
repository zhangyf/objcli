# objcli

对象存储间数据迁移工具，支持 **AWS S3** 与**腾讯云 COS** 之间的流式拷贝，不落盘，内存安全。

## 特性

- **三种拷贝模式**：单文件、前缀批量、对象 URL 列表
- **三个传输方向**：S3 → COS、COS → COS、COS → S3
- **自动路由**：小文件走 PutObject，大文件走 Multipart Upload，COS→COS 走 UploadPart-Copy（不过本机）
- **流式传输**：按 Range 分块读取，内存中转，不落盘
- **内存安全预估**：启动时预估最坏情况内存占用，超过 4GB 上限自动拒绝并提示调参
- **凭证 fallback**：命令行参数优先，未填则从环境变量读取

## 安装

```bash
git clone https://github.com/zhangyf/objcli.git
cd objcli
go build -o objcli .
```

## 快速开始

### 环境变量（推荐）

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export TENCENT_SECRET_ID=your-secret-id
export TENCENT_SECRET_KEY=your-secret-key
```

### 单文件拷贝

```bash
# S3 → COS
objcli -action cp \
  -src-type s3  -src-bucket my-s3-bucket  -src-region ap-southeast-1 -src-key path/to/file.zip \
  -dst-type cos -dst-bucket my-cos-bucket -dst-region ap-beijing

# COS → COS
objcli -action cp \
  -src-type cos -src-bucket src-bucket -src-region ap-singapore -src-key data/file.zip \
  -dst-type cos -dst-bucket dst-bucket -dst-region ap-beijing   -dst-key backup/file.zip

# COS → S3
objcli -action cp \
  -src-type cos -src-bucket my-cos-bucket -src-region ap-beijing -src-key path/to/file.zip \
  -dst-type s3  -dst-bucket my-s3-bucket  -dst-region us-east-1
```

### 前缀批量拷贝

按前缀列举源桶中所有对象并并发拷贝，相对路径保持不变。

```bash
# COS → COS，将 a/b/c/ 下所有对象迁移到 d/e/f/
objcli -action cp \
  -src-type cos -src-bucket src-bucket -src-region ap-singapore -src-prefix a/b/c/ \
  -dst-type cos -dst-bucket dst-bucket -dst-region ap-beijing   -dst-prefix d/e/f/ \
  -chunk 512 -obj-concurrency 5

# S3 → COS，前缀批量
objcli -action cp \
  -src-type s3  -src-bucket my-s3-bucket  -src-region ap-southeast-1 -src-prefix data/2024/ \
  -dst-type cos -dst-bucket my-cos-bucket -dst-region ap-beijing      -dst-prefix archive/2024/
```

### 对象 URL 列表拷贝

列表中每行为一个完整的对象 URL（COS 或 S3），工具自动解析来源，**无需指定 `-src-*` 参数**。

```bash
# 从本地列表文件
objcli -action cp \
  -dst-type cos -dst-bucket my-cos-bucket -dst-region ap-nanjing -dst-prefix import/ \
  -key-list /path/to/obj_list.txt

# 从远程 URL 读取列表
objcli -action cp \
  -dst-type cos -dst-bucket my-cos-bucket -dst-region ap-nanjing -dst-prefix import/ \
  -key-list https://bucket.cos.ap-nanjing.myqcloud.com/obj_list.log
```

**列表文件格式：**

```
# 注释行，自动跳过
https://src-bucket.cos.ap-singapore.myqcloud.com/data/file1.zip
https://src-bucket.s3.ap-southeast-1.amazonaws.com/data/file2.zip

https://another-bucket.cos.ap-beijing.myqcloud.com/logs/app.log
```

- 每行一个完整的对象 URL
- 空行和 `#` 开头的行自动跳过
- 同一列表中可混合 COS 和 S3 URL

## 参数说明

### 通用参数

| 参数 | 说明 | 必填 |
|------|------|------|
| `-action` | 操作类型，当前只支持 `cp` | ✅ |
| `-src-type` | 源存储类型：`s3` \| `cos` | list 模式可省略 |
| `-dst-type` | 目标存储类型：`s3` \| `cos` | ✅ |

### 桶与 Region

| 参数 | 说明 | 必填 |
|------|------|------|
| `-src-bucket` | 源 Bucket 名称 | list 模式可省略 |
| `-src-region` | 源 Region | list 模式可省略 |
| `-dst-bucket` | 目标 Bucket 名称 | ✅ |
| `-dst-region` | 目标 Region | ✅ |

### 拷贝模式（三选一）

| 参数 | 说明 |
|------|------|
| `-src-key` | 源对象 Key，单文件拷贝 |
| `-src-prefix` | 源前缀，批量拷贝该前缀下所有对象 |
| `-key-list` | 对象 URL 列表，支持本地路径或 HTTP/HTTPS URL |

### 目标路径

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-dst-key` | 目标对象 Key（仅单文件模式） | 与源 Key 相同 |
| `-dst-prefix` | 目标前缀（前缀/列表模式） | 空（保留原始路径） |

### 凭证

凭证优先级：**命令行参数 > 环境变量**

| 参数 | 环境变量 | 说明 |
|------|----------|------|
| `-s3-ak` | `AWS_ACCESS_KEY_ID` | AWS Access Key ID |
| `-s3-sk` | `AWS_SECRET_ACCESS_KEY` | AWS Secret Access Key |
| `-cos-id` | `TENCENT_SECRET_ID` | 腾讯云 SecretId |
| `-cos-sk` | `TENCENT_SECRET_KEY` | 腾讯云 SecretKey |

### 性能调优

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-chunk` | `128` | 分块大小（MB），COS→COS 建议 `512` |
| `-concurrency` | `5` | 单文件内分块并发数 |
| `-obj-concurrency` | `3` | 多文件间并发数（前缀/列表模式） |

> **内存预估**：启动时自动计算最坏情况内存 = `chunk × concurrency`（大文件）+ `chunk × obj-concurrency`（小文件）。超过 4GB 上限则拒绝启动并给出调参建议。

## 项目结构

```
objcli/
├── main.go               # 入口，参数解析与分发
├── storage/              # 存储抽象层
│   ├── storage.go        # Storage 接口定义
│   ├── cos_storage.go    # 腾讯云 COS 实现
│   └── s3_storage.go     # AWS S3 实现
├── cmd/
│   ├── engine.go         # 拷贝引擎（三种模式统一控制流）
│   └── keylist.go        # URL 列表加载与解析
└── progress/
    └── progress.go       # 进度跟踪与速度统计
```

扩展新的云厂商只需在 `storage/` 下新增实现文件（如 `aliyun_storage.go`），实现 `Storage` 接口即可，无需改动其他代码。

## License

MIT
