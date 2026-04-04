import logging
import mimetypes
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from app.config.storage_config import S3StorageConfig, load_s3_storage_config


class S3UploadError(RuntimeError):
    pass


class S3StorageAdapter:
    def __init__(self, config: S3StorageConfig | None = None) -> None:
        self.logger = logging.getLogger(__name__)
        self.config = config or load_s3_storage_config()

        self.bucket_name = self.config.bucket_name
        self.endpoint_url = self.config.endpoint_url
        self.access_key = self.config.access_key
        self.secret_key = self.config.secret_key
        self.region = self.config.region
        self.session_token = self.config.session_token
        self.signature_version = self.config.signature_version
        self.addressing_style = self.config.addressing_style
        self.key_prefix = self.config.key_prefix

        config = Config(
            signature_version=self.signature_version,
            s3={"addressing_style": self.addressing_style},
        )

        self.client: BaseClient = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            aws_session_token=self.session_token,
            region_name=self.region,
            config=config,
        )
        self.logger.info(
            "S3 adapter initialized",
            extra={"bucket": self.bucket_name, "region": self.region},
        )

    def upload_bytes(self, data: bytes, key: str, content_type: str | None = None) -> str:
        params: dict[str, object] = {
            "Bucket": self.bucket_name,
            "Key": key,
            "Body": data,
        }
        if content_type:
            params["ContentType"] = content_type

        try:
            self.client.put_object(**params)
            self.logger.debug("S3 upload succeeded", extra={"s3_key": key})
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            message = e.response.get("Error", {}).get("Message", str(e))
            self.logger.exception("S3 upload failed", extra={
                                  "s3_key": key, "error_code": code})
            raise S3UploadError(
                f"S3 upload failed ({code}): {message}. "
                f"Check S3 endpoint, bucket name, key permissions, and access keys."
            ) from e
        return key

    def upload_file(self, file_path: Path | str, key: str, content_type: str | None = None) -> str:
        path = Path(file_path)
        guessed_type = content_type or mimetypes.guess_type(str(path))[0]
        return self.upload_bytes(path.read_bytes(), key, guessed_type)

    def delete_key(self, key: str) -> None:
        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=key)
            self.logger.debug("S3 delete succeeded", extra={"s3_key": key})
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            message = e.response.get("Error", {}).get("Message", str(e))
            self.logger.exception("S3 delete failed", extra={
                                  "s3_key": key, "error_code": code})
            raise S3UploadError(
                f"S3 delete failed ({code}): {message}."
            ) from e

    def check_bucket_access(self) -> bool:
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            self.logger.debug("S3 bucket access check succeeded", extra={
                              "bucket": self.bucket_name})
            return True
        except ClientError:
            self.logger.warning("S3 bucket access check failed", extra={
                                "bucket": self.bucket_name})
            return False

    def build_key(self, *parts: str) -> str:
        segments = [self.key_prefix]
        for part in parts:
            cleaned = (part or "").strip("/")
            if cleaned:
                segments.append(cleaned)
        return "/".join(segments)
