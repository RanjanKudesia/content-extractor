import logging
from typing import Any

from app.pipelines.docx_extraction_pipeline import DocxExtractionPipeline


logger = logging.getLogger(__name__)


class DocxJsonExtractionAdapter:
    def __init__(self, pipeline: DocxExtractionPipeline) -> None:
        self.pipeline = pipeline

    def run(
        self,
        file_bytes: bytes,
        output_basename: str,
        include_media: bool = True,
    ) -> tuple[dict[str, Any], str]:
        logger.debug(
            "Starting DOCX extraction adapter run",
            extra={
                "output_basename": output_basename,
                "include_media": include_media,
                "size_bytes": len(file_bytes),
            },
        )
        result = self.pipeline.run(
            file_bytes=file_bytes,
            output_basename=output_basename,
            include_media=include_media,
        )
        logger.debug(
            "Completed DOCX extraction adapter run",
            extra={"output_basename": output_basename},
        )
        return result
