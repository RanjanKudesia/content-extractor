"""Plain text extraction adapter — delegates to TextExtractionPipeline."""
import logging
from typing import Any

from app.pipelines.text_extraction_pipeline import TextExtractionPipeline


logger = logging.getLogger(__name__)


class TextJsonExtractionAdapter:
    """Adapter that extracts structured JSON data from plain-text files."""

    def __init__(self) -> None:
        self.pipeline = TextExtractionPipeline()

    def run(
        self,
        file_bytes: bytes,
        output_basename: str,
        include_media: bool = True,
    ) -> tuple[dict[str, Any], str]:
        """Run plain-text extraction and return (data_dict, virtual_path)."""
        logger.debug(
            "Starting TXT extraction adapter run",
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
            "Completed TXT extraction adapter run",
            extra={"output_basename": output_basename},
        )
        return result, f"virtual://extracted/{output_basename}.txt.json"
