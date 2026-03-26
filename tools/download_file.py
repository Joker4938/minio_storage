from collections.abc import Generator
from pathlib import PurePosixPath
from typing import Any

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage
from minio.error import S3Error

from common.minio_client import build_minio_client, get_bucket_name


class DownloadFileTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        try:
            object_name = str(tool_parameters.get("object_name") or "").strip()
            if not object_name:
                raise ValueError("`object_name` is required.")

            client = build_minio_client(self.runtime.credentials)
            bucket_name = get_bucket_name(self.runtime.credentials)

            stat = client.stat_object(bucket_name=bucket_name, object_name=object_name)
            response = client.get_object(bucket_name=bucket_name, object_name=object_name)
            try:
                content = response.read()
            finally:
                response.close()
                response.release_conn()

            filename = PurePosixPath(object_name).name or "download.bin"
            content_type = stat.content_type or "application/octet-stream"

            yield self.create_blob_message(
                content,
                meta={
                    "filename": filename,
                    "mime_type": content_type,
                    "bucket": bucket_name,
                    "object_name": object_name,
                },
            )
            yield self.create_json_message(
                {
                    "bucket": bucket_name,
                    "object_name": object_name,
                    "filename": filename,
                    "size": len(content),
                    "etag": stat.etag,
                    "content_type": content_type,
                    "last_modified": stat.last_modified.isoformat() if stat.last_modified else None,
                }
            )
        except (ValueError, S3Error) as e:
            raise Exception(f"Download failed: {e!s}") from e
