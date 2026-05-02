from typing import Any

from pydantic import BaseModel, Field


class ContentResponse(BaseModel):
    content_id: str = Field(
        description="MongoDB content record ID.",
        examples=["69f64331423c9bfe1bf883a1"],
    )
    version: int = Field(
        description="Content version number.",
        examples=[0],
    )
    output_format: str = Field(
        description="Response mode used: json or file.",
        examples=["json"],
    )
    data: dict[str, Any] | None = Field(
        default=None,
        description="Inline extracted payload when output_format=json.",
    )
    file_download_url: str | None = Field(
        default=None,
        description="Presigned URL for the extracted JSON file when output_format=file.",
    )
    file_url_expires_in_seconds: int | None = Field(
        default=None,
        description="Lifetime in seconds for file_download_url.",
        examples=[3600],
    )
    created_at: str = Field(
        description="ISO-8601 UTC timestamp when content version was created.",
    )
    updated_at: str = Field(
        description="ISO-8601 UTC timestamp when content version was last updated.",
    )
