from collections.abc import Generator
from pathlib import PurePosixPath
from typing import Any

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage
from minio.error import S3Error

from common.minio_client import (
    build_download_url,
    build_minio_client,
    get_bucket_name,
    parse_optional_expires_seconds,
)


def _normalize_download_mode(value: Any) -> str:
    mode = str(value or "dify_file").strip().lower()
    valid_modes = {"dify_file", "minio_url", "both"}
    if mode not in valid_modes:
        raise ValueError("`download_mode` must be one of: dify_file, minio_url, both.")
    return mode


class DownloadFileTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        try:
            object_name = str(tool_parameters.get("object_name") or "").strip()
            if not object_name:
                raise ValueError("`object_name` is required.")
            download_mode = _normalize_download_mode(tool_parameters.get("download_mode"))
            expires_seconds = None
            if download_mode in {"minio_url", "both"}:
                expires_seconds = parse_optional_expires_seconds(
                    tool_parameters.get("download_url_expires_in")
                )

            client = build_minio_client(self.runtime.credentials)
            bucket_name = get_bucket_name(self.runtime.credentials)

            stat = client.stat_object(bucket_name=bucket_name, object_name=object_name)
            filename = PurePosixPath(object_name).name or "download.bin"
            content_type = stat.content_type or "application/octet-stream"
            link_info: dict[str, Any] = {}
            if download_mode in {"minio_url", "both"}:
                link_info = build_download_url(
                    client=client,
                    credentials=self.runtime.credentials,
                    bucket_name=bucket_name,
                    object_name=object_name,
                    expires_seconds=expires_seconds,
                )

            if download_mode in {"dify_file", "both"}:
                response = client.get_object(bucket_name=bucket_name, object_name=object_name)
                try:
                    content = response.read()
                finally:
                    response.close()
                    response.release_conn()

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
                    "size": stat.size,
                    "etag": stat.etag,
                    "content_type": content_type,
                    "download_mode": download_mode,
                    "last_modified": stat.last_modified.isoformat() if stat.last_modified else None,
                    **link_info,
                }
            )
        except (ValueError, S3Error) as e:
            raise Exception(f"Download failed: {e!s}") from e
