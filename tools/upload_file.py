from collections.abc import Generator
from io import BytesIO
from typing import Any

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage
from dify_plugin.file.file import File
from minio.error import S3Error

from common.minio_client import build_minio_client, get_bucket_name


class UploadFileTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        try:
            file_input = tool_parameters.get("file")
            if not isinstance(file_input, File):
                raise ValueError("Parameter `file` must be a Dify file input.")

            object_name = str(tool_parameters.get("object_name") or "").strip()
            if not object_name:
                object_name = file_input.filename or "uploaded.bin"

            prefix = str(tool_parameters.get("prefix") or "").strip().strip("/")
            if prefix:
                object_name = f"{prefix}/{object_name}"

            content = file_input.blob
            if len(content) == 0:
                raise ValueError("File content is empty.")

            content_type = str(tool_parameters.get("content_type") or "").strip()
            if not content_type:
                content_type = file_input.mime_type or "application/octet-stream"

            client = build_minio_client(self.runtime.credentials)
            bucket_name = get_bucket_name(self.runtime.credentials)
            result = client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=BytesIO(content),
                length=len(content),
                content_type=content_type,
            )

            yield self.create_json_message(
                {
                    "success": True,
                    "bucket": bucket_name,
                    "object_name": object_name,
                    "size": len(content),
                    "content_type": content_type,
                    "etag": result.etag,
                    "version_id": result.version_id,
                }
            )
        except (ValueError, S3Error) as e:
            raise Exception(f"Upload failed: {e!s}") from e
