from collections.abc import Generator
from io import BytesIO
from typing import Any

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage
from dify_plugin.file.file import File
from minio.error import S3Error

from common.minio_client import (
    build_download_url,
    build_minio_client,
    get_bucket_name,
    parse_optional_expires_seconds,
)


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "y"}
    return default


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

            return_download_url = _as_bool(
                tool_parameters.get("return_download_url"), default=False
            )
            expires_seconds = None
            if return_download_url:
                expires_seconds = parse_optional_expires_seconds(
                    tool_parameters.get("download_url_expires_in")
                )

            client = build_minio_client(self.runtime.credentials)
            bucket_name = get_bucket_name(self.runtime.credentials)
            result = client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=BytesIO(content),
                length=len(content),
                content_type=content_type,
            )

            response = {
                "success": True,
                "bucket": bucket_name,
                "object_name": object_name,
                "size": len(content),
                "content_type": content_type,
                "etag": result.etag,
                "version_id": result.version_id,
            }
            if return_download_url:
                response.update(
                    build_download_url(
                        client=client,
                        credentials=self.runtime.credentials,
                        bucket_name=bucket_name,
                        object_name=object_name,
                        expires_seconds=expires_seconds,
                    )
                )

            yield self.create_json_message(
                response
            )
        except (ValueError, S3Error) as e:
            raise Exception(f"Upload failed: {e!s}") from e
