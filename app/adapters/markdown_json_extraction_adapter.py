"""Markdown extraction adapter — delegates to MarkdownExtractionPipeline."""
import logging
from typing import Any

from app.pipelines.markdown_extraction_pipeline import MarkdownExtractionPipeline


logger = logging.getLogger(__name__)


class MarkdownJsonExtractionAdapter:
    """Adapter that extracts structured JSON data from Markdown files."""

    def __init__(self) -> None:
        self.pipeline = MarkdownExtractionPipeline()

    def run(
        self,
        file_bytes: bytes,
        output_basename: str,
        include_media: bool = True,
    ) -> tuple[dict[str, Any], str]:
        """Run Markdown extraction and return (data_dict, virtual_path)."""
        logger.debug(
            "Starting Markdown extraction adapter run",
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
            "Completed Markdown extraction adapter run",
            extra={"output_basename": output_basename},
        )
        return result, f"virtual://extracted/{output_basename}.md.json"
