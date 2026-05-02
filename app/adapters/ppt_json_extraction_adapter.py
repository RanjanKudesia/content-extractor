"""PowerPoint extraction adapter — delegates to PptExtractionPipeline."""
from typing import Any

from app.pipelines.ppt_extraction_pipeline import PptExtractionPipeline


class PptJsonExtractionAdapter:
    def __init__(self, pipeline: PptExtractionPipeline | None = None) -> None:
        self.pipeline = pipeline or PptExtractionPipeline()

    def run(self, file_bytes: bytes, output_basename: str) -> tuple[dict[str, Any], str]:
        result = self.pipeline.run(
            file_bytes=file_bytes, include_media=True, output_basename=output_basename
        )
        return result, f"virtual://extracted/{output_basename}.pptx.json"
