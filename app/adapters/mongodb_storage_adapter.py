from datetime import datetime, timezone
import logging
from typing import Any

from bson import ObjectId
from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.errors import PyMongoError

from app.config.db_config import MongoDbConfig, load_mongodb_config


_UPDATE_PUSH_OP = "$push"
_MONGO_WRITE_FAILED_MSG = "MongoDB write failed."
_MONGO_QUERY_FAILED_MSG = "MongoDB query failed."
_UPLOAD_NOT_FOUND_MSG = "MongoDB update failed: upload record not found."
_INVALID_UPLOAD_ID_MSG = "Invalid upload_id format."


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

    def ensure_indexes(self) -> None:
        """Create indexes required for efficient querying. Safe to call multiple times (idempotent)."""
        from pymongo import ASCENDING, DESCENDING

        self.uploads_collection.create_index(
            [("user_id", ASCENDING), ("created_at", DESCENDING)],
            name="uploads_user_created",
        )
        self.uploads_collection.create_index(
            [("extension", ASCENDING), ("created_at", DESCENDING)],
            name="uploads_ext_created",
        )
        self.content_collection.create_index(
            [("upload_id", ASCENDING), ("version", ASCENDING)],
            name="content_upload_version",
        )
        self.logger.info("MongoDB indexes ensured")

    def save_extraction_bundle(
        self,
        *,
        user_id: str,
        original_filename: str,
        stored_filename: str,
        extension: str,
        extract_media: bool,
        store_media: bool,
        uploaded_file_s3_key: str,
        data_s3_key: str,
    ) -> tuple[str, str]:
        """Persist upload+content+version linkage atomically where supported."""
        now = datetime.now(timezone.utc)
        upload_doc = {
            "user_id": user_id,
            "original_filename": original_filename,
            "stored_filename": stored_filename,
            "extension": extension,
            "extract_media": extract_media,
            "store_media": store_media,
            "uploaded_file_s3_key": uploaded_file_s3_key,
            "content_versions": [],
            "_next_version": 1,
            "created_at": now,
            "updated_at": now,
        }

        try:
            with self.client.start_session() as session:
                return self._save_extraction_bundle_in_transaction(
                    session=session,
                    upload_doc=upload_doc,
                    data_s3_key=data_s3_key,
                    now=now,
                )
        except PyMongoError as exc:
            # Standalone Mongo instances may not support transactions.
            if self._is_transaction_not_supported(exc):
                self.logger.info(
                    "MongoDB transactions unavailable; using compensating-write fallback"
                )
                return self._save_extraction_bundle_without_transaction(
                    upload_doc=upload_doc,
                    data_s3_key=data_s3_key,
                    now=now,
                )

            self.logger.exception("MongoDB bundle persistence failed")
            raise MongoStorageError(_MONGO_WRITE_FAILED_MSG) from exc

    def _save_extraction_bundle_in_transaction(
        self,
        *,
        session: ClientSession,
        upload_doc: dict[str, Any],
        data_s3_key: str,
        now: datetime,
    ) -> tuple[str, str]:
        with session.start_transaction():
            upload_result = self.uploads_collection.insert_one(
                upload_doc, session=session)
            upload_id = str(upload_result.inserted_id)

            content_doc = {
                "upload_id": upload_id,
                "version": 0,
                "data_s3_key": data_s3_key,
                "created_at": now,
                "updated_at": now,
            }
            content_result = self.content_collection.insert_one(
                content_doc, session=session)
            content_id = str(content_result.inserted_id)

            update_result = self.uploads_collection.update_one(
                {"_id": upload_result.inserted_id},
                {
                    _UPDATE_PUSH_OP: {"content_versions": {"content_id": content_id, "version": 0}},
                    "$set": {"updated_at": now},
                },
                session=session,
            )
            if update_result.matched_count != 1:
                raise MongoStorageError(_UPLOAD_NOT_FOUND_MSG)

            return upload_id, content_id

    def _save_extraction_bundle_without_transaction(
        self,
        *,
        upload_doc: dict[str, Any],
        data_s3_key: str,
        now: datetime,
    ) -> tuple[str, str]:
        upload_object_id: ObjectId | None = None
        content_object_id: ObjectId | None = None
        try:
            upload_result = self.uploads_collection.insert_one(upload_doc)
            upload_object_id = upload_result.inserted_id
            upload_id = str(upload_object_id)

            content_doc = {
                "upload_id": upload_id,
                "version": 0,
                "data_s3_key": data_s3_key,
                "created_at": now,
                "updated_at": now,
            }
            content_result = self.content_collection.insert_one(content_doc)
            content_object_id = content_result.inserted_id
            content_id = str(content_object_id)

            update_result = self.uploads_collection.update_one(
                {"_id": upload_object_id},
                {
                    _UPDATE_PUSH_OP: {"content_versions": {"content_id": content_id, "version": 0}},
                    "$set": {"updated_at": now},
                },
            )
            if update_result.matched_count != 1:
                raise MongoStorageError(_UPLOAD_NOT_FOUND_MSG)

            return upload_id, content_id
        except Exception as exc:
            self.logger.exception("MongoDB fallback bundle persistence failed")
            self._best_effort_delete_inserted(
                upload_object_id, content_object_id)
            if isinstance(exc, MongoStorageError):
                raise
            if isinstance(exc, PyMongoError):
                raise MongoStorageError(_MONGO_WRITE_FAILED_MSG) from exc
            raise MongoStorageError(_MONGO_WRITE_FAILED_MSG) from exc

    def _best_effort_delete_inserted(
        self,
        upload_object_id: ObjectId | None,
        content_object_id: ObjectId | None,
    ) -> None:
        if content_object_id is not None:
            try:
                self.content_collection.delete_one({"_id": content_object_id})
            except PyMongoError:
                self.logger.warning(
                    "Failed to rollback inserted content record")
        if upload_object_id is not None:
            try:
                self.uploads_collection.delete_one({"_id": upload_object_id})
            except PyMongoError:
                self.logger.warning(
                    "Failed to rollback inserted upload record")

    @staticmethod
    def _is_transaction_not_supported(exc: Exception) -> bool:
        message = str(exc).lower()
        hints = (
            "transaction numbers are only allowed",
            "transactions are not supported",
            "replica set",
            "not supported",
        )
        return any(hint in message for hint in hints)

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
            raise MongoStorageError(_MONGO_QUERY_FAILED_MSG) from e

        if not found:
            return None

        found["_id"] = str(found["_id"])
        return found

    def get_upload(self, upload_id: str) -> dict[str, Any] | None:
        """Fetch a single upload record by ID."""
        try:
            object_id = ObjectId(upload_id)
        except Exception as e:
            raise MongoStorageError("Invalid upload_id format.") from e
        try:
            found = self.uploads_collection.find_one({"_id": object_id})
        except PyMongoError as e:
            self.logger.exception("MongoDB query failed (get_upload)")
            raise MongoStorageError(_MONGO_QUERY_FAILED_MSG) from e
        if not found:
            return None
        found["_id"] = str(found["_id"])
        for field in ("created_at", "updated_at"):
            if hasattr(found.get(field), "isoformat"):
                found[field] = found[field].isoformat()
        return found

    def get_contents_for_upload(self, upload_id: str) -> list[dict[str, Any]]:
        """Fetch all content records associated with an upload."""
        try:
            ObjectId(upload_id)  # validate format early
        except Exception as e:
            raise MongoStorageError(_INVALID_UPLOAD_ID_MSG) from e
        try:
            docs = list(self.content_collection.find({"upload_id": upload_id}))
        except PyMongoError as e:
            self.logger.exception(
                "MongoDB query failed (get_contents_for_upload)")
            raise MongoStorageError(_MONGO_QUERY_FAILED_MSG) from e
        for doc in docs:
            doc["_id"] = str(doc["_id"])
        return docs

    def delete_upload_bundle(self, upload_id: str) -> tuple[list[str], int]:
        """Delete upload + all content records. Returns (data_s3_keys_for_cleanup, content_record_count)."""
        try:
            object_id = ObjectId(upload_id)
        except Exception as e:
            raise MongoStorageError(_INVALID_UPLOAD_ID_MSG) from e
        try:
            # Collect content S3 keys before any deletes.
            content_docs = list(
                self.content_collection.find(
                    {"upload_id": upload_id}, {"data_s3_key": 1})
            )
            data_s3_keys = [
                doc["data_s3_key"]
                for doc in content_docs
                if isinstance(doc.get("data_s3_key"), str)
            ]
            # Delete upload first so partial failures leave orphaned content
            # (not an orphaned upload pointing at missing content).
            self.uploads_collection.delete_one({"_id": object_id})
            self.content_collection.delete_many({"upload_id": upload_id})
            return data_s3_keys, len(content_docs)
        except PyMongoError as e:
            self.logger.exception(
                "MongoDB delete failed (delete_upload_bundle)")
            raise MongoStorageError("MongoDB delete failed.") from e

    def list_uploads(
        self,
        *,
        user_id: str | None = None,
        extension: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Paginated list of upload records sorted by created_at descending.

        Uses a single $facet aggregation to fetch items + total count in one round-trip.
        """
        query: dict[str, Any] = {}
        if user_id:
            query["user_id"] = user_id
        if extension:
            query["extension"] = extension.lower().lstrip(".")
        try:
            pipeline: list[dict] = [
                {"$match": query},
                {
                    "$facet": {
                        "items": [
                            {"$sort": {"created_at": -1}},
                            {"$skip": offset},
                            {"$limit": limit},
                        ],
                        "total": [{"$count": "n"}],
                    }
                },
            ]
            facet = list(self.uploads_collection.aggregate(pipeline))
            result = facet[0] if facet else {"items": [], "total": []}
            total: int = result["total"][0]["n"] if result["total"] else 0
            items: list[dict[str, Any]] = []
            for doc in result["items"]:
                doc["_id"] = str(doc["_id"])
                for field in ("created_at", "updated_at"):
                    if hasattr(doc.get(field), "isoformat"):
                        doc[field] = doc[field].isoformat()
                items.append(doc)
            return items, total
        except PyMongoError as e:
            self.logger.exception("MongoDB query failed (list_uploads)")
            raise MongoStorageError(_MONGO_QUERY_FAILED_MSG) from e

    def add_new_content_version(
        self,
        upload_id: str,
        data_s3_key: str,
    ) -> tuple[str, int]:
        """Insert new content document and push its version to the upload record.

        Uses an atomic $inc on ``_next_version`` to eliminate race conditions when
        multiple reprocess requests arrive concurrently.

        Returns (content_id, new_version_number).
        """
        try:
            object_id = ObjectId(upload_id)
        except Exception as e:
            raise MongoStorageError(_INVALID_UPLOAD_ID_MSG) from e
        try:
            # Atomically increment the version counter and get the new value.
            updated = self.uploads_collection.find_one_and_update(
                {"_id": object_id},
                {"$inc": {"_next_version": 1}},
                projection={"_next_version": 1},
                return_document=True,  # return document AFTER the increment
            )
            if updated is None:
                raise MongoStorageError("Upload record not found.")
            # If _next_version didn't exist (old document), MongoDB sets it to 1
            # on first $inc which is correct (version 0 was the initial extraction).
            next_version: int = updated.get("_next_version", 1)

            now = datetime.now(timezone.utc)
            content_doc = {
                "upload_id": upload_id,
                "version": next_version,
                "data_s3_key": data_s3_key,
                "created_at": now,
                "updated_at": now,
            }
            content_result = self.content_collection.insert_one(content_doc)
            content_id = str(content_result.inserted_id)

            self.uploads_collection.update_one(
                {"_id": object_id},
                {
                    _UPDATE_PUSH_OP: {
                        "content_versions": {"content_id": content_id, "version": next_version}
                    },
                    "$set": {"updated_at": now},
                },
            )
            return content_id, next_version
        except PyMongoError as e:
            self.logger.exception("MongoDB error in add_new_content_version")
            raise MongoStorageError(_MONGO_WRITE_FAILED_MSG) from e

    def check_connection(self) -> bool:
        try:
            self.client.admin.command("ping")
            self.logger.debug("MongoDB connection check succeeded")
            return True
        except PyMongoError:
            self.logger.warning("MongoDB connection check failed")
            return False
