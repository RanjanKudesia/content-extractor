"""HTTP routes for health and content extraction endpoints."""

import logging
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal

from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status

from app.adapters.docx_json_extraction_adapter import DocxJsonExtractionAdapter
from app.adapters.docx_xml_extraction_adapter import DocxXmlExtractionAdapter
from app.adapters.html_json_extraction_adapter import HtmlJsonExtractionAdapter
from app.adapters.markdown_json_extraction_adapter import MarkdownJsonExtractionAdapter
from app.adapters.mongodb_storage_adapter import MongoStorageError
from app.adapters.mongodb_storage_adapter import MongoDbStorageAdapter
from app.adapters.ppt_json_extraction_adapter import PptJsonExtractionAdapter
from app.adapters.s3_storage_adapter import S3StorageAdapter
from app.adapters.text_json_extraction_adapter import TextJsonExtractionAdapter
from app.adapters.system_health_adapter import SystemHealthAdapter
from app.controllers.file_upload_controller import FileUploadController
from app.controllers.health_controller import HealthController
from app.pipelines.docx_extraction_pipeline import DocxExtractionPipeline
from app.pipelines.health_pipeline import HealthPipeline
from app.pipelines.pdf_conversion_pipeline import PdfConversionPipeline
from app.pipelines.ppt_xml_extraction_pipeline import PptXmlExtractionPipeline
from app.schemas.content_schema import ContentResponse
from app.schemas.file_upload_schema import FileUploadResponse
from app.schemas.health_schema import HealthResponse

router = APIRouter()
logger = logging.getLogger(__name__)
ALLOWED_EXTENSIONS = {"pdf", "docx", "dox",
                      "md", "txt", "html", "htm", "pptx", "ppt"}


@router.get("/health", tags=["health"])
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


def validate_upload_schema(file: UploadFile, allowed_extensions: set[str] | None = None) -> UploadFile:
    """Validate filename presence and extension before extraction."""
    valid_extensions = allowed_extensions or ALLOWED_EXTENSIONS

    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Filename is required.",
        )

    extension = Path(file.filename).suffix.lower().lstrip(".")
    if extension not in valid_extensions:
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
            docx_xml_adapter=DocxXmlExtractionAdapter(),
            html_json_adapter=HtmlJsonExtractionAdapter(),
            markdown_json_adapter=MarkdownJsonExtractionAdapter(),
            text_json_adapter=TextJsonExtractionAdapter(),
            ppt_json_adapter=PptJsonExtractionAdapter(
                PptXmlExtractionPipeline()),
            ppt_xml_pipeline=PptXmlExtractionPipeline(),
            pdf_pipeline=PdfConversionPipeline(),
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
    description=(
        "Upload a single file and the service automatically selects the extraction "
        "pipeline by file extension.\n\n"
        "Supported file types: docx, dox, pdf, md, txt, html, htm, pptx, ppt.\n\n"
        "Output format rules:\n"
        "- json: supported for every file type.\n"
        "- xml: supported only for docx and pptx.\n"
        "- requesting xml for any other file type returns HTTP 400.\n\n"
        "Storage behavior:\n"
        "- uploaded original file is stored in S3-compatible object storage.\n"
        "- extracted media assets are uploaded to S3 and `s3_key` is written into the extracted payload.\n"
        "- final extraction payload (json/xml structure) is persisted in MongoDB.\n"
        "- response includes MongoDB record id and output file path.\n\n"
        "Notes:\n"
        "- pdf extraction uses the service's default pdf-to-docx flow before extraction.\n"
        "- output_format defaults to json."
    ),
)
async def extract_content(
    file: Annotated[UploadFile, File(...)],
    user_id: Annotated[str, Form()],
    output_format: Annotated[Literal["json", "xml"], Form()] = "json",
) -> FileUploadResponse:
    """Extract structured content from an uploaded file."""
    logger.info(
        "Received extraction request",
        extra={"request_filename": file.filename,
               "output_format": output_format,
               "user_id": user_id},
    )
    validated_file = validate_upload_schema(file)
    controller = create_file_upload_controller()
    return await controller.execute(
        validated_file,
        user_id=user_id,
        output_format=output_format,
    )


@router.get(
    "/content",
    tags=["content"],
    summary="Get extracted content by content_id and version",
)
def get_content(
    content_id: str,
    version: int,
) -> ContentResponse:
    """Fetch extracted content document by content_id and version."""
    _, mongo_adapter = get_storage_adapters()
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
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=detail,
        ) from e

    if not found:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Content not found for the provided content_id and version.",
        )

    created_at = found.get("created_at")
    updated_at = found.get("updated_at")
    return ContentResponse(
        content_id=found["_id"],
        version=int(found.get("version", version)),
        data=found.get("data") if isinstance(found.get("data"), dict) else {},
        created_at=created_at.isoformat() if hasattr(
            created_at, "isoformat") else str(created_at),
        updated_at=updated_at.isoformat() if hasattr(
            updated_at, "isoformat") else str(updated_at),
    )
