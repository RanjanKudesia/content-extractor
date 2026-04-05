import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class MongoDbConfig:
    mongo_uri: str
    database_name: str
    uploads_collection_name: str
    content_collection_name: str


def load_mongodb_config() -> MongoDbConfig:
    base_dir = Path(__file__).resolve().parents[2]
    load_dotenv(base_dir / ".env")

    mongo_uri = _required_env("MONGODB_URI")
    if not (mongo_uri.startswith("mongodb://") or mongo_uri.startswith("mongodb+srv://")):
        mongo_uri = f"mongodb://{mongo_uri}"

    return MongoDbConfig(
        mongo_uri=mongo_uri,
        database_name=os.getenv("MONGODB_DATABASE", "content_extractor"),
        uploads_collection_name=os.getenv(
            "MONGODB_UPLOADS_COLLECTION", "uploads"),
        content_collection_name=os.getenv(
            "MONGODB_CONTENT_COLLECTION", "content"),
    )


def _required_env(key: str) -> str:
    value = os.getenv(key)
    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {key}")
    return value.strip()
