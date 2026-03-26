from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote, urljoin, urlparse

from minio import Minio


def _required_text(credentials: Mapping[str, Any], key: str, label: str) -> str:
    value = credentials.get(key)
    if value is None:
        raise ValueError(f"Missing required credential: {label}.")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Missing required credential: {label}.")
    return text


def _optional_text(credentials: Mapping[str, Any], key: str) -> str | None:
    value = credentials.get(key)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "y"}
    return default


def _normalize_endpoint(endpoint: str, secure: bool) -> tuple[str, bool]:
    endpoint = endpoint.strip().rstrip("/")
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        parsed = urlparse(endpoint)
        if not parsed.netloc:
            raise ValueError("Invalid endpoint URL.")
        if parsed.path not in ("", "/"):
            raise ValueError("Endpoint must not contain a path.")
        return parsed.netloc, parsed.scheme == "https"

    if "/" in endpoint:
        raise ValueError("Endpoint must be host:port or a full URL without path.")
    return endpoint, secure


def get_bucket_name(credentials: Mapping[str, Any]) -> str:
    return _required_text(credentials, "bucket", "bucket")


def build_minio_client(credentials: Mapping[str, Any]) -> Minio:
    endpoint_input = _required_text(credentials, "endpoint", "endpoint")
    access_key = _required_text(credentials, "access_key", "access_key")
    secret_key = _required_text(credentials, "secret_key", "secret_key")
    secure_flag = _as_bool(credentials.get("secure"), default=False)
    endpoint, secure = _normalize_endpoint(endpoint_input, secure_flag)

    region_raw = credentials.get("region")
    region = str(region_raw).strip() if region_raw is not None else None
    if region == "":
        region = None

    session_token_raw = credentials.get("session_token")
    session_token = str(session_token_raw).strip() if session_token_raw is not None else None
    if session_token == "":
        session_token = None

    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        region=region,
        session_token=session_token,
    )


def _normalize_public_base_url(credentials: Mapping[str, Any]) -> str | None:
    base_url = _optional_text(credentials, "public_download_base_url")
    if not base_url:
        return None

    if base_url.startswith("http://") or base_url.startswith("https://"):
        parsed = urlparse(base_url)
        if not parsed.netloc:
            raise ValueError("`public_download_base_url` is invalid.")
        return base_url.rstrip("/")

    secure_flag = _as_bool(credentials.get("secure"), default=False)
    scheme = "https" if secure_flag else "http"
    return f"{scheme}://{base_url}".rstrip("/")


def _normalize_endpoint_base_url(credentials: Mapping[str, Any]) -> str:
    endpoint_input = _required_text(credentials, "endpoint", "endpoint")
    secure_flag = _as_bool(credentials.get("secure"), default=False)

    if endpoint_input.startswith("http://") or endpoint_input.startswith("https://"):
        parsed = urlparse(endpoint_input)
        if not parsed.netloc:
            raise ValueError("Invalid endpoint URL.")
        return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")

    if "/" in endpoint_input:
        raise ValueError("Endpoint must be host:port or a full URL without path.")
    scheme = "https" if secure_flag else "http"
    return f"{scheme}://{endpoint_input.rstrip('/')}"


def _build_raw_object_url(credentials: Mapping[str, Any], bucket_name: str, object_name: str) -> str:
    base_url = _normalize_public_base_url(credentials) or _normalize_endpoint_base_url(credentials)

    encoded_object_path = "/".join(quote(part, safe="") for part in object_name.split("/"))
    relative_path = f"{quote(bucket_name, safe='')}/{encoded_object_path}"
    return urljoin(f"{base_url}/", relative_path)


def _validate_expires_seconds(expires_seconds: int | None) -> None:
    if expires_seconds is None:
        return
    if expires_seconds <= 0:
        raise ValueError("`download_url_expires_in` must be greater than 0.")
    # AWS Signature V4 and MinIO pre-signed URL max expiration is 7 days.
    if expires_seconds > 604800:
        raise ValueError("`download_url_expires_in` must be <= 604800 seconds (7 days).")


def parse_optional_expires_seconds(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("`download_url_expires_in` must be a number in seconds.")
    if isinstance(value, (int, float)):
        seconds = int(value)
        _validate_expires_seconds(seconds)
        return seconds

    text = str(value).strip()
    if text == "":
        return None
    try:
        seconds = int(float(text))
    except ValueError as e:
        raise ValueError("`download_url_expires_in` must be a number in seconds.") from e
    _validate_expires_seconds(seconds)
    return seconds


def build_download_url(
    client: Minio,
    credentials: Mapping[str, Any],
    bucket_name: str,
    object_name: str,
    expires_seconds: int | None = None,
) -> dict[str, Any]:
    _validate_expires_seconds(expires_seconds)

    if expires_seconds is None:
        raw_url = _build_raw_object_url(credentials, bucket_name, object_name)
        return {
            "download_url": raw_url,
            "download_url_type": "raw",
            "download_url_expires_at": None,
            "download_url_expires_in": None,
        }

    expires = timedelta(seconds=expires_seconds)
    presigned_url = client.presigned_get_object(
        bucket_name=bucket_name,
        object_name=object_name,
        expires=expires,
    )
    return {
        "download_url": presigned_url,
        "download_url_type": "presigned",
        "download_url_expires_at": (datetime.now(timezone.utc) + expires).isoformat(),
        "download_url_expires_in": expires_seconds,
    }
