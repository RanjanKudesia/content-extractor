"""PowerPoint extraction adapter — delegates to PptExtractionPipeline."""
import logging
from typing import Any

from app.pipelines.ppt_extraction_pipeline import PptExtractionPipeline


logger = logging.getLogger(__name__)


class PptJsonExtractionAdapter:
    def __init__(self, pipeline: PptExtractionPipeline | None = None) -> None:
        self.pipeline = pipeline or PptExtractionPipeline()

    def run(
        self,
        file_bytes: bytes,
        output_basename: str,
        include_media: bool = True,
    ) -> tuple[dict[str, Any], str]:
        logger.debug(
            "Starting PPT extraction adapter run",
            extra={
                "output_basename": output_basename,
                "include_media": include_media,
                "size_bytes": len(file_bytes),
            },
        )
        result = self.pipeline.run(
            file_bytes=file_bytes,
            include_media=include_media,
            output_basename=output_basename,
        )
        logger.debug(
            "Completed PPT extraction adapter run",
            extra={"output_basename": output_basename},
        )
        return result, f"virtual://extracted/{output_basename}.pptx.json"
