## MinIO Storage Plugin

**Author:** joker  
**Version:** 0.0.2  
**Type:** tool

### Description
This Dify plugin provides MinIO file operations:

- `upload_file`: upload file to MinIO
- `list_files`: list objects in bucket
- `download_file`: download to Dify, return MinIO URL, or both

### Provider Credentials

- `endpoint`: MinIO endpoint, for example `127.0.0.1:9000` or `https://minio.example.com`
- `access_key`: MinIO Access Key
- `secret_key`: MinIO Secret Key
- `bucket`: target bucket
- `secure`: whether to use HTTPS when endpoint has no scheme
- `region`: optional
- `session_token`: optional
- `public_download_base_url`: optional public base URL for raw file path output

### Operation Guide (EN)

1. Install dependencies:
```bash
pip install -r requirements.txt
```
2. Local debug run:
```bash
python -m main
```
3. Configure Provider credentials in Dify.
4. Use tools:
- `upload_file`
  - `return_download_url=false`: no URL returned
  - `return_download_url=true` and no `download_url_expires_in`: return raw URL
  - `return_download_url=true` with `download_url_expires_in`: return pre-signed URL
- `list_files`
  - `with_download_url=true` enables per-file URL output
  - if `download_url_expires_in` is empty, return raw URL
  - if `download_url_expires_in` is set, return pre-signed URL
- `download_file`
  - `download_mode=dify_file`: download blob to Dify
  - `download_mode=minio_url`: return MinIO URL only
  - `download_mode=both`: return both blob and MinIO URL
5. Expiration setting:
- `download_url_expires_in` unit is seconds
- valid range: `1` to `604800` (7 days)
- empty means raw URL path

---

## MinIO 存储插件

**作者：** joker  
**版本：** 0.0.2  
**类型：** 工具

### 插件说明
该插件为 Dify 提供 MinIO 文件操作能力：

- `upload_file`：上传文件到 MinIO
- `list_files`：列出 Bucket 中对象
- `download_file`：下载到 Dify、返回 MinIO 链接，或两者同时返回

### Provider 配置项

- `endpoint`：MinIO 地址，例如 `127.0.0.1:9000` 或 `https://minio.example.com`
- `access_key`：访问密钥
- `secret_key`：密钥
- `bucket`：目标桶名称
- `secure`：当 endpoint 未带协议时是否使用 HTTPS
- `region`：可选
- `session_token`：可选
- `public_download_base_url`：可选，返回裸路径时使用的公网基础地址

### 操作说明（中文）

1. 安装依赖：
```bash
pip install -r requirements.txt
```
2. 本地调试运行：
```bash
python -m main
```
3. 在 Dify 中配置 Provider 凭据。
4. 工具使用规则：
- `upload_file`
  - `return_download_url=false`：不返回链接
  - `return_download_url=true` 且不填 `download_url_expires_in`：返回裸路径
  - `return_download_url=true` 且填写 `download_url_expires_in`：返回签名链接
- `list_files`
  - `with_download_url=true` 时为每个文件返回下载链接
  - 未设置 `download_url_expires_in` 返回裸路径
  - 设置 `download_url_expires_in` 返回签名链接
- `download_file`
  - `download_mode=dify_file`：下载到 Dify
  - `download_mode=minio_url`：仅返回 MinIO 链接
  - `download_mode=both`：同时返回 Dify 文件和 MinIO 链接
5. 过期时间说明：
- `download_url_expires_in` 单位为秒
- 取值范围 `1` 到 `604800`（7 天）
- 不填写表示返回裸路径
