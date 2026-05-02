import logging
from pathlib import Path
import tempfile
from typing import Any

from pdf2docx import Converter

from app.pipelines.docx_extraction_pipeline import DocxExtractionPipeline


class PdfConversionPipeline:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.docx_pipeline = DocxExtractionPipeline()

    def run(self, file_bytes: bytes, include_media: bool = True) -> dict[str, Any]:
        """Convert a PDF via pdf2docx, then extract the resulting DOCX to JSON."""
        self.logger.info("Starting PDF to DOCX conversion (pdf2docx fallback)")
        with tempfile.TemporaryDirectory(prefix="content-extractor-") as temp_dir:
            temp_path = Path(temp_dir)
            pdf_path = temp_path / "input.pdf"
            docx_path = temp_path / "converted.docx"

            pdf_path.write_bytes(file_bytes)

            converter = Converter(str(pdf_path))
            try:
                converter.convert(str(docx_path))
            finally:
                converter.close()

            if not docx_path.exists() or not docx_path.is_file():
                raise ValueError(
                    "PDF conversion failed: DOCX output was not created.")

            self.logger.info(
                "PDF to DOCX conversion completed, running DOCX extraction")
            docx_bytes = docx_path.read_bytes()

        result, _ = self.docx_pipeline.run(
            file_bytes=docx_bytes,
            output_basename="pdf-converted",
        )
        result.setdefault("metadata", {})
        result["metadata"]["source_type"] = "pdf"
        result["metadata"]["extraction_mode"] = "pdf2docx"
        if not include_media:
            result["media"] = []
        return result
