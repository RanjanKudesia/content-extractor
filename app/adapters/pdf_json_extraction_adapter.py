"""PDF extraction adapter — delegates to PdfExtractionPipeline."""
from typing import Any

from app.pipelines.pdf_extraction_pipeline import PdfExtractionPipeline


class PdfJsonExtractionAdapter:
    def __init__(self, pipeline: PdfExtractionPipeline | None = None) -> None:
        self.pipeline = pipeline or PdfExtractionPipeline()

    def run(self, file_bytes: bytes, output_basename: str) -> tuple[dict[str, Any], str]:
        result = self.pipeline.run(file_bytes=file_bytes, include_media=True)
        return result, f"virtual://extracted/{output_basename}.pdf.json"

