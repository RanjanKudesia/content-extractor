"""FastAPI app entrypoint for the Content Extractor service."""

import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi import Request
from pymongo.errors import PyMongoError

from app.api.routes import router as api_router
from app.api.routes import get_storage_adapters, get_http_client
from app.adapters.mongodb_storage_adapter import MongoStorageError
from app.config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Initialize shared resources at startup and clean them up at shutdown."""
    # Eagerly initialize adapters (populates @lru_cache) and ensure indexes.
    _, mongo_adapter = get_storage_adapters()
    try:
        mongo_adapter.ensure_indexes()
    except (MongoStorageError, PyMongoError) as exc:  # pragma: no cover — startup best-effort
        logger.warning(
            "MongoDB index creation failed — service will still start: %s", exc)

    # Initialize the shared httpx client (populates module-level singleton).
    get_http_client()
    logger.info("Content Extractor API started")

    yield

    # Shutdown: close the shared HTTP client gracefully.
    http_client = get_http_client()
    await http_client.aclose()
    get_http_client.cache_clear()
    logger.info("Shared httpx client closed")


app = FastAPI(
    title="Content Extractor API",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "Extract structured JSON content from uploaded documents and retrieve "
        "the extracted payload later.\n\n"
        "Key workflows:\n"
        "1. POST /extract-content to upload a file and run extraction\n"
        "2. GET /content to fetch extracted JSON or a presigned JSON file URL\n\n"
        "Supported inputs: docx, dox, pdf, md, txt, html, htm, pptx, ppt."
    ),
    openapi_tags=[
        {
            "name": "health",
            "description": "Service and dependency readiness endpoints.",
        },
        {
            "name": "content",
            "description": (
                "Upload/extract documents and retrieve extracted content by "
                "content_id/version."
            ),
        },
        {
            "name": "uploads",
            "description": (
                "List, fetch, reprocess, and delete upload records and their "
                "content versions."
            ),
        },
    ],
)
app.include_router(api_router)


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    """Emit request/response logs with latency and request correlation id."""
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    start_time = time.perf_counter()

    logger.info(
        "Request started",
        extra={
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
        },
    )
    try:
        response = await call_next(request)
    except Exception:
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        logger.exception(
            "Request failed",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "duration_ms": duration_ms,
            },
        )
        raise

    duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
    response.headers["X-Request-ID"] = request_id
    logger.info(
        "Request completed",
        extra={
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
        },
    )
    return response


logger.info("Content Extractor API initialized")
