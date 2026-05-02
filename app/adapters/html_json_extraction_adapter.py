"""HTML extraction adapter — delegates to HtmlExtractionPipeline."""
import logging
from typing import Any

from app.pipelines.html_extraction_pipeline import HtmlExtractionPipeline


logger = logging.getLogger(__name__)


class HtmlJsonExtractionAdapter:
    """Adapter that extracts structured JSON data from HTML files."""

    def __init__(self) -> None:
        self.pipeline = HtmlExtractionPipeline()

    def run(
        self,
        file_bytes: bytes,
        output_basename: str,
        include_media: bool = True,
    ) -> tuple[dict[str, Any], str]:
        """Run HTML extraction and return (data_dict, virtual_path)."""
        logger.debug(
            "Starting HTML extraction adapter run",
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
            "Completed HTML extraction adapter run",
            extra={"output_basename": output_basename},
        )
        return result, f"virtual://extracted/{output_basename}.html.json"
