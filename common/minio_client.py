from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse

from minio import Minio


def _required_text(credentials: Mapping[str, Any], key: str, label: str) -> str:
    value = credentials.get(key)
    if value is None:
        raise ValueError(f"Missing required credential: {label}.")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Missing required credential: {label}.")
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
