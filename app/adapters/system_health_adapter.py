from datetime import UTC, datetime
import logging
import os

from app.adapters.mongodb_storage_adapter import MongoDbStorageAdapter
from app.adapters.s3_storage_adapter import S3StorageAdapter


class SystemHealthAdapter:
    def __init__(
        self,
        s3_adapter: S3StorageAdapter | None = None,
        mongo_adapter: MongoDbStorageAdapter | None = None,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.s3_adapter = s3_adapter
        self.mongo_adapter = mongo_adapter

    def fetch(self) -> dict:
        dependencies: dict[str, str] = {}

        if self.s3_adapter is not None:
            dependencies["s3"] = "ok" if self.s3_adapter.check_bucket_access(
            ) else "error"

        if self.mongo_adapter is not None:
            dependencies["mongodb"] = "ok" if self.mongo_adapter.check_connection(
            ) else "error"

        overall = "ok"
        if dependencies and any(value != "ok" for value in dependencies.values()):
            overall = "degraded"

        if overall == "degraded":
            self.logger.warning("Health check degraded", extra={
                                "dependencies": dependencies})
        else:
            self.logger.debug("Health check OK", extra={
                              "dependencies": dependencies or None})

        return {
            "status": overall,
            "service": os.getenv("APP_NAME", "content-extractor"),
            "environment": os.getenv("APP_ENV", "development"),
            "timestamp": datetime.now(UTC).isoformat(),
            "dependencies": dependencies or None,
        }
