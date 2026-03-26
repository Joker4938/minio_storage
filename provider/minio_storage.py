from typing import Any

from dify_plugin import ToolProvider
from dify_plugin.errors.tool import ToolProviderCredentialValidationError
from minio.error import S3Error

from common.minio_client import (
    build_download_url,
    build_minio_client,
    get_bucket_name,
)


class MinioStorageProvider(ToolProvider):
    def _validate_credentials(self, credentials: dict[str, Any]) -> None:
        try:
            client = build_minio_client(credentials)
            bucket_name = get_bucket_name(credentials)

            if not client.bucket_exists(bucket_name):
                raise ValueError(f"Bucket '{bucket_name}' does not exist.")

            # Try listing once to validate list permission.
            next(client.list_objects(bucket_name, recursive=False), None)

            # Validate raw download URL format (uses public base URL or endpoint).
            build_download_url(
                client=client,
                credentials=credentials,
                bucket_name=bucket_name,
                object_name="connectivity-check.txt",
                expires_seconds=None,
            )
        except (ValueError, S3Error) as e:
            raise ToolProviderCredentialValidationError(str(e)) from e
        except Exception as e:
            raise ToolProviderCredentialValidationError(
                f"Unable to validate MinIO credentials: {e!s}"
            ) from e
