from typing import Any

from pydantic import BaseModel


class ContentResponse(BaseModel):
    content_id: str
    version: int
    data: dict[str, Any]
    created_at: str
    updated_at: str
