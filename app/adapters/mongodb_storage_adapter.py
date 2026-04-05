from datetime import datetime, timezone
import logging
from typing import Any

from bson import ObjectId
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
        db = self.client[self.config.database_name]
        self.uploads_collection = db[self.config.uploads_collection_name]
        self.content_collection = db[self.config.content_collection_name]
        self.logger.info(
            "MongoDB adapter initialized",
            extra={
                "database": self.config.database_name,
                "uploads_collection": self.config.uploads_collection_name,
                "content_collection": self.config.content_collection_name,
            },
        )

    def save_upload(self, payload: dict[str, Any]) -> str:
        document = dict(payload)
        now = datetime.now(timezone.utc)
        document["content_versions"] = []
        document["created_at"] = now
        document["updated_at"] = now
        try:
            result = self.uploads_collection.insert_one(document)
        except PyMongoError as e:
            self.logger.exception("MongoDB insert failed (uploads)")
            raise MongoStorageError(
                "MongoDB insert failed. Check MONGODB_URI format/credentials and network access."
            ) from e
        self.logger.debug("Upload record inserted", extra={
                          "upload_id": str(result.inserted_id)})
        return str(result.inserted_id)

    def save_content(self, payload: dict[str, Any]) -> str:
        document = dict(payload)
        now = datetime.now(timezone.utc)
        document["created_at"] = now
        document["updated_at"] = now
        try:
            result = self.content_collection.insert_one(document)
        except PyMongoError as e:
            self.logger.exception("MongoDB insert failed (content)")
            raise MongoStorageError(
                "MongoDB insert failed. Check MONGODB_URI format/credentials and network access."
            ) from e
        self.logger.debug("Content record inserted", extra={
                          "content_id": str(result.inserted_id)})
        return str(result.inserted_id)

    def add_content_version(self, upload_id: str, content_id: str, version: int) -> None:
        try:
            self.uploads_collection.update_one(
                {"_id": ObjectId(upload_id)},
                {
                    "$push": {"content_versions": {"content_id": content_id, "version": version}},
                    "$set": {"updated_at": datetime.now(timezone.utc)},
                },
            )
        except PyMongoError as e:
            self.logger.exception(
                "MongoDB update failed (add_content_version)")
            raise MongoStorageError("MongoDB update failed.") from e

    def get_content(self, content_id: str, version: int) -> dict[str, Any] | None:
        try:
            object_id = ObjectId(content_id)
        except Exception as e:  # ObjectId throws TypeError/ValueError for invalid IDs
            raise MongoStorageError("Invalid content_id format.") from e

        try:
            found = self.content_collection.find_one(
                {"_id": object_id, "version": version})
        except PyMongoError as e:
            self.logger.exception("MongoDB query failed (get_content)")
            raise MongoStorageError("MongoDB query failed.") from e

        if not found:
            return None

        found["_id"] = str(found["_id"])
        return found

    def check_connection(self) -> bool:
        try:
            self.client.admin.command("ping")
            self.logger.debug("MongoDB connection check succeeded")
            return True
        except PyMongoError:
            self.logger.warning("MongoDB connection check failed")
            return False
