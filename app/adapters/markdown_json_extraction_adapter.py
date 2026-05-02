"""Markdown extraction adapter — delegates to MarkdownExtractionPipeline."""
from typing import Any

from app.pipelines.markdown_extraction_pipeline import MarkdownExtractionPipeline


class MarkdownJsonExtractionAdapter:
    def __init__(self) -> None:
        self.pipeline = MarkdownExtractionPipeline()

    def run(self, file_bytes: bytes, output_basename: str) -> tuple[dict[str, Any], str]:
        result = self.pipeline.run(file_bytes=file_bytes, include_media=True)
        return result, f"virtual://extracted/{output_basename}.md.json"
