from datetime import datetime, timezone
import logging
from typing import Any

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from app.config.db_config import MongoDbConfig, load_mongodb_config


class MongoStorageError(RuntimeError):
    pass


class MongoDbStorageAdapter:
    def __init__(self, config: MongoDbConfig | None = None) -> None:
        self.logger = logging.getLogger(__name__)
        self.config = config or load_mongodb_config()

        self.client = MongoClient(
            self.config.mongo_uri,
            serverSelectionTimeoutMS=5000,
        )
        self.collection = self.client[self.config.database_name][self.config.collection_name]
        self.logger.info(
            "MongoDB adapter initialized",
            extra={
                "database": self.config.database_name,
                "collection": self.config.collection_name,
            },
        )

    def save_extraction(self, payload: dict[str, Any]) -> str:
        document = dict(payload)
        document.setdefault("created_at", datetime.now(timezone.utc))
        try:
            result = self.collection.insert_one(document)
        except PyMongoError as e:
            self.logger.exception("MongoDB insert failed")
            raise MongoStorageError(
                "MongoDB insert failed. Check MONGODB_URI format/credentials and network access."
            ) from e
        self.logger.debug("MongoDB insert succeeded", extra={
                          "inserted_id": str(result.inserted_id)})
        return str(result.inserted_id)

    def check_connection(self) -> bool:
        try:
            self.client.admin.command("ping")
            self.logger.debug("MongoDB connection check succeeded")
            return True
        except PyMongoError:
            self.logger.warning("MongoDB connection check failed")
            return False
