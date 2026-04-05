from typing import Any

from pydantic import BaseModel


class FileUploadResponse(BaseModel):
    upload_id: str
    user_id: str
    original_filename: str
    stored_filename: str
    uploaded_file_s3_key: str
    extension: str
    output_format: str = "json"
    content_versions: list[dict[str, Any]]
    created_at: str
    updated_at: str
