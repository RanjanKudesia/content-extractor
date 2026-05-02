"""HTML extraction adapter — delegates to HtmlExtractionPipeline."""
from typing import Any

from app.pipelines.html_extraction_pipeline import HtmlExtractionPipeline


class HtmlJsonExtractionAdapter:
    def __init__(self) -> None:
        self.pipeline = HtmlExtractionPipeline()

    def run(self, file_bytes: bytes, output_basename: str) -> tuple[dict[str, Any], str]:
        result = self.pipeline.run(file_bytes=file_bytes, include_media=True)
        return result, f"virtual://extracted/{output_basename}.html.json"
