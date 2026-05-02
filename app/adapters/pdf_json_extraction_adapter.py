"""PDF extraction adapter — delegates to PdfExtractionPipeline."""
import logging
from typing import Any

from app.pipelines.pdf_extraction_pipeline import PdfExtractionPipeline


logger = logging.getLogger(__name__)


class PdfJsonExtractionAdapter:
    """Adapter that extracts structured JSON data from PDF files."""

    def __init__(self, pipeline: PdfExtractionPipeline | None = None) -> None:
        self.pipeline = pipeline or PdfExtractionPipeline()

    def run(
        self,
        file_bytes: bytes,
        output_basename: str,
        include_media: bool = True,
    ) -> tuple[dict[str, Any], str]:
        """Run PDF extraction and return (data_dict, virtual_path)."""
        logger.debug(
            "Starting PDF extraction adapter run",
            extra={
                "output_basename": output_basename,
                "include_media": include_media,
                "size_bytes": len(file_bytes),
            },
        )
        result = self.pipeline.run(
            file_bytes=file_bytes,
            include_media=include_media,
        )
        logger.debug(
            "Completed PDF extraction adapter run",
            extra={"output_basename": output_basename},
        )
        return result, f"virtual://extracted/{output_basename}.pdf.json"
