"""HTTP routes for health and content extraction endpoints."""

import asyncio
import ipaddress
import json
import logging
import mimetypes
import socket
from functools import lru_cache
from pathlib import Path
from typing import Annotated
from urllib.parse import urlparse

import httpx
from fastapi import (
    APIRouter,
    File,
    Form,
    HTTPException,
    Path as FastAPIPath,
    Query,
    UploadFile,
    status,
)

from app.adapters.docx_json_extraction_adapter import DocxJsonExtractionAdapter
from app.adapters.html_json_extraction_adapter import HtmlJsonExtractionAdapter
from app.adapters.markdown_json_extraction_adapter import MarkdownJsonExtractionAdapter
from app.adapters.mongodb_storage_adapter import MongoStorageError
from app.adapters.mongodb_storage_adapter import MongoDbStorageAdapter
from app.adapters.pdf_json_extraction_adapter import PdfJsonExtractionAdapter
from app.adapters.ppt_json_extraction_adapter import PptJsonExtractionAdapter
from app.adapters.s3_storage_adapter import S3StorageAdapter, S3UploadError
from app.adapters.text_json_extraction_adapter import TextJsonExtractionAdapter
from app.adapters.system_health_adapter import SystemHealthAdapter
from app.controllers.file_upload_controller import FileUploadController
from app.controllers.health_controller import HealthController
from app.pipelines.docx_extraction_pipeline import DocxExtractionPipeline
from app.pipelines.health_pipeline import HealthPipeline
from app.pipelines.pdf_extraction_pipeline import PdfExtractionPipeline
from app.pipelines.ppt_extraction_pipeline import PptExtractionPipeline
from app.schemas.content_schema import ContentResponse
from app.schemas.file_upload_schema import FileUploadResponse
from app.schemas.health_schema import HealthResponse
from app.schemas.upload_schema import (
    DeleteUploadResponse,
    ExtractFromUrlRequest,
    MediaListResponse,
    ReprocessRequest,
    ReprocessResponse,
    UploadRecord,
    UploadsListResponse,
    UploadVersionsResponse,
    ContentVersionItem,
)

router = APIRouter()
logger = logging.getLogger(__name__)
_PRESIGNED_URL_EXPIRY_SECONDS = 3600
_MAX_FETCH_BYTES = 20 * 1024 * 1024  # 20 MB

# Shared connection-pooled HTTP client — initialized by lifespan, avoids per-request TCP overhead.


@lru_cache(maxsize=1)
def get_http_client() -> httpx.AsyncClient:
    """Return a shared httpx client reused across requests."""
    logger.info("Initializing shared HTTP client")
    return httpx.AsyncClient(
        follow_redirects=True,
        timeout=30.0,
        limits=httpx.Limits(
            max_connections=20,
            max_keepalive_connections=10,
        ),
    )


ALLOWED_EXTENSIONS = {"pdf", "docx", "dox",
                      "md", "txt", "html", "htm", "pptx", "ppt"}


@router.get(
    "/health",
    tags=["health"],
    summary="Service health status",
    description="Returns service status plus dependency checks for S3 and MongoDB.",
)
def get_health() -> HealthResponse:
    """Return service and dependency health information."""
    logger.debug("Health endpoint requested")
    s3_adapter, mongo_adapter = get_storage_adapters()
    adapter = SystemHealthAdapter(
        s3_adapter=s3_adapter,
        mongo_adapter=mongo_adapter,
    )
    pipeline = HealthPipeline(adapter=adapter)
    controller = HealthController(pipeline=pipeline)
    return controller.execute()


def validate_upload_schema(
    file: UploadFile,
    allowed_extensions: set[str] | None = None,
) -> UploadFile:
    """Validate filename presence and extension before extraction."""
    valid_extensions = allowed_extensions or ALLOWED_EXTENSIONS

    if not file.filename:
        logger.warning("Upload rejected: missing filename")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Filename is required.",
        )

    extension = Path(file.filename).suffix.lower().lstrip(".")
    if extension not in valid_extensions:
        logger.warning(
            "Upload rejected: unsupported extension",
            extra={"filename": file.filename, "extension": extension},
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Allowed file types are: {', '.join(sorted(valid_extensions))}.",
        )
    return file


@lru_cache(maxsize=1)
def get_storage_adapters() -> tuple[S3StorageAdapter, MongoDbStorageAdapter]:
    """Create and cache storage adapters for reuse across requests."""
    logger.info("Initializing storage adapters")
    return S3StorageAdapter(), MongoDbStorageAdapter()


@lru_cache(maxsize=1)
def create_file_upload_controller() -> FileUploadController:
    """Create and cache the main file upload controller."""
    try:
        logger.info("Initializing file upload controller")
        s3_adapter, mongo_adapter = get_storage_adapters()
        return FileUploadController(
            docx_json_adapter=DocxJsonExtractionAdapter(
                DocxExtractionPipeline()),
            html_json_adapter=HtmlJsonExtractionAdapter(),
            markdown_json_adapter=MarkdownJsonExtractionAdapter(),
            text_json_adapter=TextJsonExtractionAdapter(),
            ppt_json_adapter=PptJsonExtractionAdapter(PptExtractionPipeline()),
            pdf_json_adapter=PdfJsonExtractionAdapter(PdfExtractionPipeline()),
            s3_adapter=s3_adapter,
            mongo_adapter=mongo_adapter,
        )
    except ValueError as e:
        logger.exception(
            "Controller initialization failed due to invalid storage config")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Storage adapter configuration error: {str(e)}",
        ) from e


@router.post(
    "/extract-content",
    tags=["content"],
    summary="Extract structured content from an uploaded file",
    response_description=(
        "Extraction identifiers and storage references for the newly created "
        "content version."
    ),
    description=(
        "Upload a single file and the service automatically selects the extraction "
        "pipeline by file extension.\n\n"
        "Supported file types: docx, dox, pdf, md, txt, html, htm, pptx, ppt.\n\n"
        "Extraction behavior:\n"
        "- output is always JSON.\n"
        "- extract_media=true includes media objects in the extracted payload.\n"
        "- extract_media=false skips media extraction for every supported extension.\n"
        "- store_media=true uploads extracted media to S3 and rewrites payload media with s3_key.\n"
        "- store_media=false keeps media inline in base64 form inside the extracted payload.\n\n"
        "Storage behavior:\n"
        "- uploaded original file is stored in S3-compatible object storage.\n"
        "- final extraction payload (JSON structure) is persisted in S3, with "
        "only the reference stored in MongoDB.\n"
        "- response includes upload and content identifiers.\n\n"
        "Notes:\n"
        "- PDF extraction uses native PyMuPDF + pdfplumber. Scanned/image-only "
        "PDFs automatically fall back to pdf2docx conversion."
    ),
    responses={
        400: {
            "description": (
                "Invalid input (unsupported extension, bad output_format, "
                "invalid IDs, etc.)."
            )
        },
        404: {"description": "Requested content was not found."},
        502: {"description": "Storage/backend dependency failure (S3 or MongoDB)."},
    },
)
async def extract_content(
    file: Annotated[
        UploadFile,
        File(
            ...,
            description="Document file to extract (docx, dox, pdf, md, txt, html, htm, pptx, ppt).",
        ),
    ],
    user_id: Annotated[
        str,
        Form(
            ...,
            description="Caller/user identifier used for ownership and audit metadata.",
            examples=["user-123"],
        ),
    ],
    store_media: Annotated[
        bool,
        Form(
            description=(
                "If true, upload extracted media to S3 and return s3_key "
                "references; if false, keep media inline as base64."
            ),
            examples=[True],
        ),
    ] = True,
    extract_media: Annotated[
        bool,
        Form(
            description=(
                "If false, skip media extraction entirely; if true, include "
                "media in payload (stored inline or in S3 depending on "
                "store_media)."
            ),
            examples=[True],
        ),
    ] = True,
) -> FileUploadResponse:
    """Extract structured content from an uploaded file."""
    logger.info(
        "Received extraction request",
        extra={"request_filename": file.filename,
               "store_media": store_media,
               "extract_media": extract_media,
               "user_id": user_id},
    )
    validated_file = validate_upload_schema(file)
    controller = create_file_upload_controller()
    return await controller.execute(
        validated_file,
        user_id=user_id,
        store_media=store_media,
        extract_media=extract_media,
    )


@router.get(
    "/content",
    tags=["content"],
    summary="Get extracted content by content_id and version",
    description=(
        "Fetch a specific extracted content version.\n\n"
        "- output_format=json returns parsed extracted JSON in the response body.\n"
        "- output_format=file returns a presigned URL for downloading the "
        "stored extracted JSON file."
    ),
    response_description="Extracted content payload (json mode) or presigned file URL (file mode).",
    responses={
        400: {"description": "Invalid content_id/version/output_format."},
        404: {"description": "Content or requested file output not found."},
        502: {"description": "Dependency/storage read failure while loading content."},
    },
)
def get_content(
    content_id: Annotated[
        str,
        Query(
            ...,
            description=(
                "MongoDB content document identifier returned in "
                "content_versions[].content_id."
            ),
            examples=["69f64331423c9bfe1bf883a1"],
        ),
    ],
    version: Annotated[
        int,
        Query(
            ...,
            ge=0,
            description="Content version number to fetch (usually 0 for current implementation).",
            examples=[0],
        ),
    ],
    output_format: Annotated[
        str,
        Query(
            description="Response mode: json for inline payload, file for presigned URL.",
            pattern="^(json|file)$",
            examples=["json"],
        ),
    ] = "json",
) -> ContentResponse:
    """Fetch extracted content as inline JSON or a presigned file URL."""
    logger.info(
        "Fetching extracted content",
        extra={
            "content_id": content_id,
            "version": version,
            "output_format": output_format,
        },
    )
    s3_adapter, mongo_adapter = get_storage_adapters()
    normalized_output_format = _normalize_output_format(output_format)
    found = _fetch_content_or_raise(
        mongo_adapter, content_id=content_id, version=version)

    extracted_data: dict | None = None
    file_download_url: str | None = None
    file_url_expires_in_seconds: int | None = None

    if normalized_output_format == "json":
        extracted_data = _resolve_json_payload(found, s3_adapter)
    else:
        file_download_url, file_url_expires_in_seconds = _resolve_file_download(
            found,
            s3_adapter,
        )

    return ContentResponse(
        content_id=found["_id"],
        version=int(found.get("version", version)),
        output_format=normalized_output_format,
        data=extracted_data,
        file_download_url=file_download_url,
        file_url_expires_in_seconds=file_url_expires_in_seconds,
        created_at=_as_iso(found.get("created_at")),
        updated_at=_as_iso(found.get("updated_at")),
    )


def _normalize_output_format(output_format: str) -> str:
    normalized = (output_format or "json").strip().lower()
    if normalized not in {"json", "file"}:
        logger.warning(
            "Invalid output_format requested",
            extra={"output_format": output_format},
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="output_format must be either 'json' or 'file'.",
        )
    return normalized


def _fetch_content_or_raise(
    mongo_adapter: MongoDbStorageAdapter, *, content_id: str, version: int
) -> dict:
    logger.debug(
        "Loading content record",
        extra={"content_id": content_id, "version": version},
    )
    try:
        found = mongo_adapter.get_content(
            content_id=content_id, version=version)
    except MongoStorageError as e:
        detail = str(e)
        if "Invalid content_id format" in detail:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=detail,
            ) from e
        logger.exception("MongoDB content lookup failed")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=detail,
        ) from e

    if not found:
        logger.info(
            "Content record not found",
            extra={"content_id": content_id, "version": version},
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Content not found for the provided content_id and version.",
        )
    return found


def _resolve_json_payload(found: dict, s3_adapter: S3StorageAdapter) -> dict:
    data_s3_key = found.get("data_s3_key")
    if isinstance(data_s3_key, str):
        logger.debug("Loading extracted payload from S3",
                     extra={"data_s3_key": data_s3_key})
        try:
            raw_bytes = s3_adapter.download_bytes(data_s3_key)
            loaded = json.loads(raw_bytes)
        except Exception as e:
            logger.exception("Failed to load extracted JSON from S3")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to load extracted JSON from storage: {str(e)}",
            ) from e
        return loaded if isinstance(loaded, dict) else {}

    # Backward compatibility: older documents may embed extracted payload in MongoDB.
    return found.get("data") if isinstance(found.get("data"), dict) else {}


def _resolve_file_download(found: dict, s3_adapter: S3StorageAdapter) -> tuple[str, int]:
    data_s3_key = found.get("data_s3_key")
    if not isinstance(data_s3_key, str):
        logger.info(
            "File download unavailable for content record without S3 key")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File output is unavailable for this content record.",
        )

    try:
        logger.debug(
            "Generating extracted payload presigned URL",
            extra={"data_s3_key": data_s3_key,
                   "expires": _PRESIGNED_URL_EXPIRY_SECONDS},
        )
        url = s3_adapter.generate_presigned_download_url(
            data_s3_key,
            expires_in_seconds=_PRESIGNED_URL_EXPIRY_SECONDS,
        )
    except Exception as e:
        logger.exception("Failed to generate extracted payload presigned URL")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to generate file download URL: {str(e)}",
        ) from e
    return url, _PRESIGNED_URL_EXPIRY_SECONDS


def _as_iso(value: object) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


# ── Shared helpers (used by all routes below) ─────────────────────────────────

def _upload_id_path():
    """Shared path parameter descriptor for upload_id."""
    return FastAPIPath(
        ...,
        description="MongoDB upload record ID.",
        examples=["69f6432f423c9bfe1bf883a0"],
    )


def _content_id_path():
    """Shared path parameter descriptor for content_id."""
    return FastAPIPath(
        ...,
        description="MongoDB content record ID.",
        examples=["69f64331423c9bfe1bf883a1"],
    )


def _fetch_upload_or_raise(mongo_adapter: MongoDbStorageAdapter, upload_id: str) -> dict:
    logger.debug("Loading upload record", extra={"upload_id": upload_id})
    try:
        doc = mongo_adapter.get_upload(upload_id)
    except MongoStorageError as e:
        detail = str(e)
        if "Invalid upload_id" in detail:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=detail) from e
        logger.exception("MongoDB upload lookup failed")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail=detail) from e
    if doc is None:
        logger.info("Upload record not found", extra={"upload_id": upload_id})
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Upload not found for the provided upload_id.",
        )
    return doc


def _upload_doc_to_record(doc: dict) -> UploadRecord:
    raw_versions: list[dict] = doc.get("content_versions") or []
    return UploadRecord(
        upload_id=doc.get("_id") or doc.get("upload_id") or "",
        user_id=doc.get("user_id") or "",
        original_filename=doc.get("original_filename") or "",
        stored_filename=doc.get("stored_filename") or "",
        uploaded_file_s3_key=doc.get("uploaded_file_s3_key") or "",
        extension=doc.get("extension") or "",
        extract_media=bool(doc.get("extract_media", True)),
        store_media=bool(doc.get("store_media", True)),
        content_versions=[ContentVersionItem(**v) for v in raw_versions],
        created_at=str(doc.get("created_at") or ""),
        updated_at=str(doc.get("updated_at") or ""),
    )


def _collect_media_items(payload: dict) -> list[dict]:
    """Extract top-level and run-embedded media items from an extracted payload."""
    items: list[dict] = []
    _gather_top_level_media(payload, items)
    _gather_run_embedded_media(payload, items)
    logger.debug("Collected media items from payload",
                 extra={"media_count": len(items)})
    return items


def _gather_top_level_media(payload: dict, items: list[dict]) -> None:
    top_media = payload.get("media")
    if isinstance(top_media, list):
        items.extend(dict(m) for m in top_media if isinstance(m, dict))


def _gather_run_embedded_media(payload: dict, items: list[dict]) -> None:
    for para in payload.get("paragraphs") or []:
        if not isinstance(para, dict):
            continue
        for run in para.get("runs") or []:
            if not isinstance(run, dict):
                continue
            items.extend(
                dict(em) for em in (run.get("embedded_media") or []) if isinstance(em, dict)
            )


def _validate_url_format(url: str) -> tuple[str, str, str | None]:
    """Validate URL and parse hostname, filename, and content-type hint."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        logger.warning("URL extraction rejected: invalid scheme",
                       extra={"url": url})
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only http and https URLs are supported.",
        )
    hostname = parsed.hostname or ""
    if not hostname:
        logger.warning(
            "URL extraction rejected: missing hostname", extra={"url": url})
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid URL: missing hostname.",
        )
    url_path = parsed.path.rstrip("/")
    filename = Path(url_path).name if url_path else "document"
    if not filename:
        filename = "document"
    content_type_hint, _ = mimetypes.guess_type(filename)
    return hostname, filename, content_type_hint


async def _check_ssrf(hostname: str) -> None:
    """Async DNS resolution with SSRF guard — runs blocking gethostbyname in a thread pool."""
    try:
        loop = asyncio.get_event_loop()
        ip_str = await loop.run_in_executor(None, socket.gethostbyname, hostname)
        addr = ipaddress.ip_address(ip_str)
    except (socket.gaierror, ValueError) as e:
        logger.warning("Hostname resolution failed",
                       extra={"hostname": hostname})
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not resolve hostname '{hostname}'.",
        ) from e
    if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
        logger.warning(
            "URL extraction rejected by SSRF guard",
            extra={"hostname": hostname, "resolved_ip": str(addr)},
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Fetching from private or internal addresses is not allowed.",
        )


# ── Upload management ─────────────────────────────────────────────────────────

@router.get(
    "/uploads",
    tags=["uploads"],
    summary="List upload records (paginated)",
    description=(
        "Return a paginated, newest-first list of upload records. "
        "Filter by user_id and/or file extension."
    ),
    responses={
        400: {"description": "Invalid query parameters."},
        502: {"description": "MongoDB query failure."},
    },
)
def list_uploads(
    user_id: Annotated[
        str | None,
        Query(description="Filter by caller/user identifier.",
              examples=["user-123"]),
    ] = None,
    extension: Annotated[
        str | None,
        Query(
            description="Filter by normalized file extension (e.g. pdf, docx).",
            examples=["pdf"],
        ),
    ] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=100,
              description="Maximum number of records to return.", examples=[20]),
    ] = 20,
    offset: Annotated[
        int,
        Query(
            ge=0, description="Number of records to skip (for pagination).", examples=[0]),
    ] = 0,
) -> UploadsListResponse:
    """List upload records with optional filters and pagination."""
    logger.info(
        "Listing uploads",
        extra={
            "user_id": user_id,
            "extension": extension,
            "limit": limit,
            "offset": offset,
        },
    )
    _, mongo_adapter = get_storage_adapters()
    try:
        items, total = mongo_adapter.list_uploads(
            user_id=user_id, extension=extension, limit=limit, offset=offset
        )
    except MongoStorageError as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail=str(e)) from e

    return UploadsListResponse(
        items=[_upload_doc_to_record(doc) for doc in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/uploads/{upload_id}",
    tags=["uploads"],
    summary="Get a single upload record",
    description="Fetch the full upload record including all linked content version IDs.",
    responses={
        400: {"description": "Invalid upload_id format."},
        404: {"description": "Upload not found."},
        502: {"description": "MongoDB query failure."},
    },
)
def get_upload(
    upload_id: Annotated[str, _upload_id_path()],
) -> UploadRecord:
    """Return a single upload record by ID."""
    logger.info("Fetching upload record", extra={"upload_id": upload_id})
    _, mongo_adapter = get_storage_adapters()
    doc = _fetch_upload_or_raise(mongo_adapter, upload_id)
    return _upload_doc_to_record(doc)


@router.get(
    "/uploads/{upload_id}/versions",
    tags=["uploads"],
    summary="List content versions for an upload",
    description=(
        "Return all content versions (content_id + version number) linked to the given upload. "
        "Use content_id + version with GET /content to fetch extracted payloads."
    ),
    responses={
        400: {"description": "Invalid upload_id format."},
        404: {"description": "Upload not found."},
        502: {"description": "MongoDB query failure."},
    },
)
def get_upload_versions(
    upload_id: Annotated[str, _upload_id_path()],
) -> UploadVersionsResponse:
    """List all content versions for an upload record."""
    logger.info("Fetching upload versions", extra={"upload_id": upload_id})
    _, mongo_adapter = get_storage_adapters()
    doc = _fetch_upload_or_raise(mongo_adapter, upload_id)
    raw_versions: list[dict] = doc.get("content_versions") or []
    versions = [ContentVersionItem(**v) for v in raw_versions]
    return UploadVersionsResponse(
        upload_id=upload_id, versions=versions, total=len(versions)
    )


@router.delete(
    "/uploads/{upload_id}",
    tags=["uploads"],
    summary="Delete an upload and all associated data",
    description=(
        "Permanently delete the upload record, all linked content records from MongoDB, "
        "the original source file from S3, and all extracted JSON payloads from S3.\n\n"
        "**This operation is irreversible.**"
    ),
    responses={
        400: {"description": "Invalid upload_id format."},
        404: {"description": "Upload not found."},
        502: {"description": "Storage failure during deletion."},
    },
)
def delete_upload(
    upload_id: Annotated[str, _upload_id_path()],
) -> DeleteUploadResponse:
    """Delete upload + all content records + all associated S3 objects."""
    logger.info("Deleting upload bundle", extra={"upload_id": upload_id})
    s3_adapter, mongo_adapter = get_storage_adapters()
    doc = _fetch_upload_or_raise(mongo_adapter, upload_id)

    # Collect all S3 keys before touching MongoDB.
    keys_to_delete: list[str] = []
    source_key: str = doc.get("uploaded_file_s3_key") or ""
    if source_key:
        keys_to_delete.append(source_key)

    try:
        data_s3_keys, content_count = mongo_adapter.delete_upload_bundle(
            upload_id)
    except MongoStorageError as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail=str(e)) from e

    keys_to_delete.extend(data_s3_keys)

    deleted_s3 = 0
    if keys_to_delete:
        try:
            deleted_s3 = s3_adapter.delete_keys(keys_to_delete)
        except S3UploadError as e:
            logger.warning(
                "S3 cleanup partially failed after MongoDB delete; manual cleanup may be needed",
                extra={"error": str(e), "keys": keys_to_delete},
            )

    return DeleteUploadResponse(
        upload_id=upload_id,
        deleted_content_records=content_count,
        deleted_s3_keys=deleted_s3,
        message=(
            f"Upload {upload_id} deleted: "
            f"{content_count} content record(s) removed, "
            f"{deleted_s3} S3 object(s) deleted."
        ),
    )


# ── Content media ─────────────────────────────────────────────────────────────

@router.get(
    "/content/{content_id}/media",
    tags=["content"],
    summary="List media items from extracted content",
    description=(
        "Download the stored extracted JSON for a content version and return only the media items, "
        "avoiding the need to fetch the full payload. "
        "Pass include_presigned_urls=true to get short-lived download URLs for S3-stored media."
    ),
    responses={
        400: {"description": "Invalid content_id or version."},
        404: {"description": "Content not found or has no media."},
        502: {"description": "Storage failure while loading content."},
    },
)
def get_content_media(
    content_id: Annotated[str, _content_id_path()],
    version: Annotated[
        int,
        Query(ge=0, description="Content version number.", examples=[0]),
    ] = 0,
    include_presigned_urls: Annotated[
        bool,
        Query(description="If true, add presigned_url to each S3-stored media item."),
    ] = False,
) -> MediaListResponse:
    """Return the media list from an extracted content payload."""
    logger.info(
        "Fetching content media",
        extra={
            "content_id": content_id,
            "version": version,
            "include_presigned_urls": include_presigned_urls,
        },
    )
    s3_adapter, mongo_adapter = get_storage_adapters()
    found = _fetch_content_or_raise(
        mongo_adapter, content_id=content_id, version=version)
    payload = _resolve_json_payload(found, s3_adapter)

    media_items: list[dict] = _collect_media_items(payload)

    if include_presigned_urls:
        for item in media_items:
            s3_key = item.get("s3_key")
            if isinstance(s3_key, str):
                try:
                    item["presigned_url"] = s3_adapter.generate_presigned_download_url(
                        s3_key, expires_in_seconds=_PRESIGNED_URL_EXPIRY_SECONDS
                    )
                except S3UploadError:
                    item["presigned_url"] = None

    return MediaListResponse(
        content_id=content_id,
        version=version,
        total=len(media_items),
        items=media_items,
    )


# ── URL extraction ────────────────────────────────────────────────────────────

@router.post(
    "/extract-content/url",
    tags=["content"],
    summary="Extract content from a remote URL",
    description=(
        "Fetch a document from an HTTP/HTTPS URL and run the same extraction pipeline as "
        "POST /extract-content. The URL must be publicly accessible and resolve to a "
        "supported file type (docx, pdf, md, txt, html, pptx).\n\n"
        "Private/loopback addresses are blocked."
    ),
    response_description="Same as POST /extract-content.",
    responses={
        400: {"description": "Invalid URL, blocked address, unsupported type, or fetch failure."},
        502: {"description": "Storage dependency failure."},
    },
)
async def extract_content_from_url(body: ExtractFromUrlRequest) -> FileUploadResponse:
    """Download a document from a URL and run extraction."""
    raw_url = body.url.strip()
    logger.info(
        "Received URL extraction request",
        extra={
            "url": raw_url,
            "user_id": body.user_id,
            "extract_media": body.extract_media,
            "store_media": body.store_media,
        },
    )
    hostname, filename, content_type_hint = _validate_url_format(raw_url)
    await _check_ssrf(hostname)

    try:
        client = get_http_client()
        resp = await client.get(raw_url, headers={"User-Agent": "ContentExtractor/1.0"})
        resp.raise_for_status()
        raw_content_type: str = resp.headers.get("content-type", "") or ""
        content_type = raw_content_type.split(
            ";")[0].strip() or content_type_hint

        # Enforce size limit without buffering full body upfront.
        chunks: list[bytes] = []
        total = 0
        async for chunk in resp.aiter_bytes(chunk_size=65536):
            total += len(chunk)
            if total > _MAX_FETCH_BYTES:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Remote file exceeds the 20 MB size limit.",
                )
            chunks.append(chunk)
        file_bytes = b"".join(chunks)
        logger.info(
            "Fetched remote file for extraction",
            extra={
                "url": raw_url,
                "filename": filename,
                "size_bytes": len(file_bytes),
                "content_type": content_type,
            },
        )
    except httpx.HTTPStatusError as e:
        logger.warning(
            "URL extraction failed due to HTTP status",
            extra={"url": raw_url, "status_code": e.response.status_code},
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Remote URL returned HTTP {e.response.status_code}.",
        ) from e
    except httpx.RequestError as e:
        logger.warning("URL extraction failed due to request error", extra={
                       "url": raw_url})
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to fetch URL: {str(e)}",
        ) from e

    controller = create_file_upload_controller()
    return controller.execute_from_bytes(
        file_bytes=file_bytes,
        original_filename=filename,
        content_type=content_type or None,
        user_id=body.user_id,
        extract_media=body.extract_media,
        store_media=body.store_media,
    )


# ── Reprocess ─────────────────────────────────────────────────────────────────

@router.post(
    "/content/{content_id}/reprocess",
    tags=["content"],
    summary="Re-extract content from the original uploaded file",
    description=(
        "Re-download the original source file from S3 and run extraction again, "
        "creating a new content version. Useful after pipeline improvements or "
        "when changing extract_media / store_media flags.\n\n"
        "The original upload record and previous versions are preserved."
    ),
    responses={
        400: {"description": "Invalid content_id format."},
        404: {"description": "Content or parent upload not found."},
        502: {"description": "Storage failure."},
    },
)
def reprocess_content(
    content_id: Annotated[str, _content_id_path()],
    body: ReprocessRequest = ReprocessRequest(),
) -> ReprocessResponse:
    """Re-run extraction on the stored source file for the parent upload."""
    logger.info(
        "Received reprocess request",
        extra={
            "content_id": content_id,
            "extract_media": body.extract_media,
            "store_media": body.store_media,
        },
    )
    _, mongo_adapter = get_storage_adapters()
    found = _fetch_content_or_raise(
        mongo_adapter, content_id=content_id, version=0)
    upload_id: str = found.get("upload_id", "")
    if not upload_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Parent upload not found for this content record.",
        )

    controller = create_file_upload_controller()
    new_content_id, new_version = controller.execute_reprocess(
        upload_id=upload_id,
        extract_media=body.extract_media,
        store_media=body.store_media,
    )

    return ReprocessResponse(
        upload_id=upload_id,
        content_id=new_content_id,
        version=new_version,
        extract_media=body.extract_media,
        store_media=body.store_media,
        message=f"Reprocessed upload {upload_id}; new content version {new_version} created.",
    )
