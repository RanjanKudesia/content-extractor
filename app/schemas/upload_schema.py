from typing import Any

from pydantic import BaseModel, Field


class ContentVersionItem(BaseModel):
    content_id: str = Field(description="Content record ID.")
    version: int = Field(description="Version number.", examples=[0])


class UploadRecord(BaseModel):
    upload_id: str = Field(
        description="MongoDB upload record ID.",
        examples=["69f6432f423c9bfe1bf883a0"],
    )
    user_id: str = Field(description="Caller/user identifier.", examples=["user-123"])
    original_filename: str = Field(description="Original client filename.", examples=["report.pdf"])
    stored_filename: str = Field(description="Internal stored filename.")
    uploaded_file_s3_key: str = Field(description="S3 key for source file.")
    extension: str = Field(description="Normalized file extension.", examples=["pdf"])
    extract_media: bool = Field(description="Whether media extraction was enabled.")
    store_media: bool = Field(description="Whether media was stored in S3.")
    content_versions: list[ContentVersionItem] = Field(
        description="All content versions for this upload."
    )
    created_at: str = Field(description="ISO-8601 UTC creation timestamp.")
    updated_at: str = Field(description="ISO-8601 UTC last-updated timestamp.")


class UploadVersionsResponse(BaseModel):
    upload_id: str = Field(description="Upload record ID.")
    versions: list[ContentVersionItem] = Field(description="All content versions.")
    total: int = Field(description="Total number of versions.")


class DeleteUploadResponse(BaseModel):
    upload_id: str = Field(description="Deleted upload record ID.")
    deleted_content_records: int = Field(
        description="Number of content records deleted from MongoDB."
    )
    deleted_s3_keys: int = Field(description="Number of S3 objects deleted.")
    message: str = Field(description="Human-readable summary.")


class MediaListResponse(BaseModel):
    content_id: str = Field(description="Content record ID.")
    version: int = Field(description="Content version number.")
    total: int = Field(description="Total number of media items found.")
    items: list[dict[str, Any]] = Field(
        description="Media item objects from the extracted payload."
    )


class ExtractFromUrlRequest(BaseModel):
    url: str = Field(
        description="HTTP/HTTPS URL of the document to fetch and extract.",
        examples=["https://example.com/report.pdf"],
    )
    user_id: str = Field(description="Caller/user identifier.", examples=["user-123"])
    extract_media: bool = Field(
        default=True, description="Whether to extract media."
    )
    store_media: bool = Field(
        default=True, description="Whether to store media in S3."
    )


class ReprocessRequest(BaseModel):
    extract_media: bool = Field(
        default=True, description="Override extract_media flag for reprocessing."
    )
    store_media: bool = Field(
        default=True, description="Override store_media flag for reprocessing."
    )


class ReprocessResponse(BaseModel):
    upload_id: str = Field(description="Upload record ID.")
    content_id: str = Field(description="Newly created content record ID.")
    version: int = Field(description="New version number assigned.")
    extract_media: bool
    store_media: bool
    message: str = Field(description="Human-readable summary.")


class UploadsListResponse(BaseModel):
    items: list[UploadRecord] = Field(description="Page of upload records.")
    total: int = Field(description="Total matching records (before pagination).")
    limit: int = Field(description="Page size used.")
    offset: int = Field(description="Offset used.")
