from typing import Any

from app.pipelines.docx_extraction_pipeline import DocxExtractionPipeline


class DocxJsonExtractionAdapter:
    def __init__(self, pipeline: DocxExtractionPipeline) -> None:
        self.pipeline = pipeline

    def run(self, file_bytes: bytes, output_basename: str) -> tuple[dict[str, Any], str]:
        return self.pipeline.run(file_bytes=file_bytes, output_basename=output_basename)
