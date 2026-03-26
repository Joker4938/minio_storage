from collections.abc import Generator
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


class ListFilesTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        try:
            prefix = str(tool_parameters.get("prefix") or "").strip()
            recursive = _as_bool(tool_parameters.get("recursive"), default=True)
            with_download_url = _as_bool(
                tool_parameters.get("with_download_url"), default=False
            )
            expires_seconds = None
            if with_download_url:
                expires_seconds = parse_optional_expires_seconds(
                    tool_parameters.get("download_url_expires_in")
                )

            max_keys_value = tool_parameters.get("max_keys")
            max_keys = int(max_keys_value) if max_keys_value is not None else 100
            if max_keys <= 0:
                raise ValueError("`max_keys` must be greater than 0.")

            client = build_minio_client(self.runtime.credentials)
            bucket_name = get_bucket_name(self.runtime.credentials)
            iterator = client.list_objects(
                bucket_name=bucket_name,
                prefix=prefix or None,
                recursive=recursive,
            )

            objects: list[dict[str, Any]] = []
            truncated = False
            for obj in iterator:
                if len(objects) >= max_keys:
                    truncated = True
                    break
                item = {
                    "name": obj.object_name,
                    "size": obj.size,
                    "etag": obj.etag,
                    "is_dir": obj.is_dir,
                    "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                }
                if with_download_url and not obj.is_dir and obj.object_name:
                    item.update(
                        build_download_url(
                            client=client,
                            credentials=self.runtime.credentials,
                            bucket_name=bucket_name,
                            object_name=obj.object_name,
                            expires_seconds=expires_seconds,
                        )
                    )
                objects.append(item)

            yield self.create_json_message(
                {
                    "bucket": bucket_name,
                    "prefix": prefix,
                    "recursive": recursive,
                    "with_download_url": with_download_url,
                    "download_url_expires_in": expires_seconds,
                    "max_keys": max_keys,
                    "count": len(objects),
                    "truncated": truncated,
                    "objects": objects,
                }
            )
        except (ValueError, S3Error) as e:
            raise Exception(f"List failed: {e!s}") from e
