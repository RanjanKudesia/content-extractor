"""Plain text extraction adapter — delegates to TextExtractionPipeline."""
from typing import Any

from app.pipelines.text_extraction_pipeline import TextExtractionPipeline


class TextJsonExtractionAdapter:
    def __init__(self) -> None:
        self.pipeline = TextExtractionPipeline()

    def run(self, file_bytes: bytes, output_basename: str) -> tuple[dict[str, Any], str]:
        result = self.pipeline.run(file_bytes=file_bytes, include_media=True)
        return result, f"virtual://extracted/{output_basename}.txt.json"
