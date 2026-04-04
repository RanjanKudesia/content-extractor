import logging
from pathlib import Path
import tempfile

from pdf2docx import Converter


class PdfConversionPipeline:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, file_bytes: bytes, output_basename: str) -> tuple[bytes, str]:
        self.logger.info("Starting PDF to DOCX conversion", extra={
                         "output_basename": output_basename})
        with tempfile.TemporaryDirectory(prefix="content-extractor-") as temp_dir:
            temp_path = Path(temp_dir)
            pdf_path = temp_path / f"{output_basename}.pdf"
            docx_path = temp_path / f"{output_basename}.docx"

            pdf_path.write_bytes(file_bytes)

            converter = Converter(str(pdf_path))
            try:
                converter.convert(str(docx_path))
            finally:
                converter.close()

            if not docx_path.exists() or not docx_path.is_file():
                raise ValueError(
                    "PDF conversion failed: DOCX output was not created.")

            self.logger.info("PDF to DOCX conversion completed")
            return docx_path.read_bytes(), f"virtual://converted/{output_basename}.docx"
