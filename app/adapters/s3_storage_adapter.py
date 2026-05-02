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

    def download_bytes(self, key: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            body = response.get("Body")
            if body is None:
                raise S3UploadError("S3 response body missing")
            data = body.read()
            self.logger.debug(
                "S3 download succeeded", extra={"s3_key": key, "size_bytes": len(data)}
            )
            return data
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            message = e.response.get("Error", {}).get("Message", str(e))
            self.logger.exception("S3 download failed", extra={
                                  "s3_key": key, "error_code": code})
            raise S3UploadError(
                f"S3 download failed ({code}): {message}."
            ) from e

    def generate_presigned_download_url(self, key: str, expires_in_seconds: int = 3600) -> str:
        try:
            url = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket_name, "Key": key},
                ExpiresIn=expires_in_seconds,
            )
            self.logger.debug(
                "S3 presigned URL generated",
                extra={"s3_key": key, "expires_in_seconds": expires_in_seconds},
            )
            return url
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            message = e.response.get("Error", {}).get("Message", str(e))
            self.logger.exception("S3 presigned URL generation failed", extra={
                                  "s3_key": key, "error_code": code})
            raise S3UploadError(
                f"S3 presigned URL generation failed ({code}): {message}."
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

    def object_exists(self, key: str) -> bool:
        """Return True if the object exists in the bucket."""
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
                return False
            code = e.response.get("Error", {}).get("Code", "Unknown")
            raise S3UploadError(f"S3 head_object failed ({code}): {str(e)}") from e

    def delete_keys(self, keys: list[str]) -> int:
        """Batch-delete up to 1000 keys per call. Returns the number deleted."""
        if not keys:
            return 0
        deleted = 0
        chunk_size = 1000
        for i in range(0, len(keys), chunk_size):
            chunk = keys[i : i + chunk_size]
            objects = [{"Key": k} for k in chunk]
            try:
                resp = self.client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={"Objects": objects, "Quiet": False},
                )
                deleted += len(resp.get("Deleted", []))
                for err in resp.get("Errors", []):
                    self.logger.warning(
                        "S3 batch delete partial error",
                        extra={"key": err.get("Key"), "code": err.get("Code")},
                    )
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "Unknown")
                self.logger.exception("S3 batch delete failed")
                raise S3UploadError(f"S3 batch delete failed ({code}): {str(e)}") from e
        return deleted

    def build_key(self, *parts: str) -> str:
        segments = [self.key_prefix]
        for part in parts:
            cleaned = (part or "").strip("/")
            if cleaned:
                segments.append(cleaned)
        return "/".join(segments)
