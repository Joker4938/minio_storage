## minio_storage

**Author:** joker
**Version:** 0.0.1
**Type:** tool

### Description
MinIO Dify plugin with configurable credentials, supporting:

- `upload_file`: upload a Dify file to MinIO
- `list_files`: list objects in bucket
- `download_file`: download object as binary blob

### Provider Credentials

- `endpoint`: MinIO endpoint (`127.0.0.1:9000` or `https://minio.example.com`)
- `access_key`: MinIO access key
- `secret_key`: MinIO secret key
- `bucket`: target bucket name
- `secure`: whether to use HTTPS when endpoint has no scheme
- `region` (optional)
- `session_token` (optional)



## minio 存储

**作者：** joker
**版本：** 0.0.1
**类型：** 工具

### 描述
带有可配置凭证的 MinIO Dify 插件，支持：

- 'upload_file'：将Dify文件上传到MinIO
- “list_files”：在桶中列出对象
- “download_file”：以二进制 blob 下载对象

### 提供者资质

- “端点”：MinIO 端点（“127.0.0.1：9000”或“https://minio.example.com”）
- “access_key”：MinIO 访问密钥
- “secret_key”：MinIO 密钥
- “桶”：目标桶名称
- “安全”：当端点没有方案时是否使用 HTTPS
- “区域”（可选）
- “session_token”（可选）

