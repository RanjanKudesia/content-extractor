from typing import Any

from pydantic import BaseModel


class FileUploadResponse(BaseModel):
    db_record_id: str
    original_filename: str
    stored_filename: str
    uploaded_file_s3_key: str
    extension: str
    extracted_data: dict[str, Any]
    output_file_path: str
    output_format: str = "json"
    version: int = 0
