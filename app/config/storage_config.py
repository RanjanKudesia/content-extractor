import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class S3StorageConfig:
    bucket_name: str
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str
    session_token: str | None
    signature_version: str
    addressing_style: str
    key_prefix: str


def load_s3_storage_config() -> S3StorageConfig:
    base_dir = Path(__file__).resolve().parents[2]
    load_dotenv(base_dir / ".env")

    bucket_name = _required_env("S3_BUCKET_NAME")
    if bucket_name.lower() in {"bucket", "your-bucket-name", "example"}:
        raise ValueError(
            "S3_BUCKET_NAME is using a placeholder value. Set it to your real bucket name."
        )

    return S3StorageConfig(
        bucket_name=bucket_name,
        endpoint_url=_required_env("S3_ENDPOINT_URL"),
        access_key=_required_env("S3_ACCESS_KEY_ID"),
        secret_key=_required_env("S3_SECRET_ACCESS_KEY"),
        region=os.getenv("S3_REGION", "us-east-1"),
        session_token=os.getenv("S3_SESSION_TOKEN") or None,
        signature_version=os.getenv("S3_SIGNATURE_VERSION", "s3v4"),
        addressing_style=os.getenv("S3_ADDRESSING_STYLE", "path"),
        key_prefix=(os.getenv("S3_KEY_PREFIX", "content-extractor")
                    or "content-extractor").strip("/"),
    )


def _required_env(key: str) -> str:
    value = os.getenv(key)
    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {key}")
    return value.strip()
