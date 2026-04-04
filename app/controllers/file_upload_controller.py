import logging
import re
import uuid
import base64
import hashlib
import mimetypes
from typing import Literal
from pathlib import Path

from fastapi import HTTPException, UploadFile, status

from app.adapters.docx_json_extraction_adapter import DocxJsonExtractionAdapter
from app.adapters.docx_xml_extraction_adapter import DocxXmlExtractionAdapter
from app.adapters.html_json_extraction_adapter import HtmlJsonExtractionAdapter
from app.adapters.markdown_json_extraction_adapter import MarkdownJsonExtractionAdapter
from app.adapters.mongodb_storage_adapter import MongoStorageError
from app.adapters.mongodb_storage_adapter import MongoDbStorageAdapter
from app.adapters.ppt_json_extraction_adapter import PptJsonExtractionAdapter
from app.adapters.s3_storage_adapter import S3UploadError
from app.adapters.s3_storage_adapter import S3StorageAdapter
from app.adapters.text_json_extraction_adapter import TextJsonExtractionAdapter
from app.pipelines.pdf_conversion_pipeline import PdfConversionPipeline
from app.pipelines.ppt_xml_extraction_pipeline import PptXmlExtractionPipeline
from app.schemas.file_upload_schema import FileUploadResponse

MAX_FILE_SIZE_BYTES = 20 * 1024 * 1024
FILENAME_REGEX = re.compile(r"^[A-Za-z0-9 _().-]+$")


class FileUploadController:
    def __init__(
        self,
        docx_json_adapter: DocxJsonExtractionAdapter,
        docx_xml_adapter: DocxXmlExtractionAdapter,
        html_json_adapter: HtmlJsonExtractionAdapter,
        markdown_json_adapter: MarkdownJsonExtractionAdapter,
        text_json_adapter: TextJsonExtractionAdapter,
        ppt_json_adapter: PptJsonExtractionAdapter,
        ppt_xml_pipeline: PptXmlExtractionPipeline,
        pdf_pipeline: PdfConversionPipeline,
        s3_adapter: S3StorageAdapter,
        mongo_adapter: MongoDbStorageAdapter,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.base_dir = Path(__file__).resolve().parents[2]
        self.docx_json_adapter = docx_json_adapter
        self.docx_xml_adapter = docx_xml_adapter
        self.html_json_adapter = html_json_adapter
        self.markdown_json_adapter = markdown_json_adapter
        self.text_json_adapter = text_json_adapter
        self.ppt_json_adapter = ppt_json_adapter
        self.ppt_xml_pipeline = ppt_xml_pipeline
        self.pdf_pipeline = pdf_pipeline
        self.s3_adapter = s3_adapter
        self.mongo_adapter = mongo_adapter

    async def execute(
        self,
        file: UploadFile,
        output_format: Literal["json", "xml"] = "json",
    ) -> FileUploadResponse:
        if not file.filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Filename is required.",
            )

        original_filename = file.filename
        extension = Path(original_filename).suffix.lower().lstrip(".")
        stem = Path(original_filename).stem

        self.logger.info(
            "Starting extraction",
            extra={"original_filename": original_filename,
                   "requested_output_format": output_format},
        )

        if not stem.strip() or not FILENAME_REGEX.fullmatch(stem):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Filename contains unsupported characters.",
            )

        file_bytes = await file.read()
        if not file_bytes:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Uploaded file is empty.",
            )

        if len(file_bytes) > MAX_FILE_SIZE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File size must be less than 20 MB.",
            )

        if extension == "dox":
            normalized_extension = "docx"
        elif extension == "htm":
            normalized_extension = "html"
        elif extension == "ppt":
            normalized_extension = "pptx"
        else:
            normalized_extension = extension

        stored_filename = f"{uuid.uuid4()}.{normalized_extension}"

        if output_format == "xml" and normalized_extension not in {"docx", "pptx"}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="output_format='xml' is supported only for DOCX and PPTX files.",
            )

        extraction_bytes = file_bytes
        if normalized_extension == "pdf":
            try:
                self.logger.info("Converting PDF to DOCX before extraction")
                extraction_bytes, _ = self.pdf_pipeline.run(
                    file_bytes=file_bytes,
                    output_basename=Path(stored_filename).stem,
                )
            except (ValueError, TypeError, OSError) as e:
                self.logger.exception("PDF conversion failed")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to convert PDF to DOCX: {str(e)}",
                ) from e
        elif normalized_extension not in {"docx", "pdf", "md", "txt", "html", "pptx"}:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="Only PDF, DOCX, MD, TXT, HTML, and PPTX extraction are implemented right now.",
            )

        if normalized_extension == "html":
            if output_format != "json":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="HTML extraction currently supports only output_format='json'.",
                )

            extracted_data, output_file_path = self.html_json_adapter.run(
                file_bytes=file_bytes,
                output_basename=Path(stored_filename).stem,
            )
        elif normalized_extension == "md":
            if output_format != "json":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Markdown extraction currently supports only output_format='json'.",
                )

            extracted_data, output_file_path = self.markdown_json_adapter.run(
                file_bytes=file_bytes,
                output_basename=Path(stored_filename).stem,
            )
        elif normalized_extension == "txt":
            if output_format != "json":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="TXT extraction currently supports only output_format='json'.",
                )

            extracted_data, output_file_path = self.text_json_adapter.run(
                file_bytes=file_bytes,
                output_basename=Path(stored_filename).stem,
            )
        elif normalized_extension == "pptx":
            if output_format == "xml":
                extracted_data, output_file_path = self.ppt_xml_pipeline.run(
                    file_bytes=file_bytes,
                    output_basename=Path(stored_filename).stem,
                )
            else:
                extracted_data, output_file_path = self.ppt_json_adapter.run(
                    file_bytes=file_bytes,
                    output_basename=Path(stored_filename).stem,
                )
        elif output_format == "xml":
            extracted_data, output_file_path = self.docx_xml_adapter.run(
                file_bytes=extraction_bytes,
                output_basename=Path(stored_filename).stem,
            )
        else:
            extracted_data, output_file_path = self.docx_json_adapter.run(
                file_bytes=extraction_bytes,
                output_basename=Path(stored_filename).stem,
            )

        extraction_folder = Path(stored_filename).stem
        uploaded_keys: list[str] = []

        def safe_cleanup_uploaded() -> None:
            for key in reversed(uploaded_keys):
                try:
                    self.s3_adapter.delete_key(key)
                except S3UploadError:
                    # Best-effort cleanup; main error path should still surface.
                    continue

        uploaded_file_key = self.s3_adapter.build_key(
            "uploads",
            extraction_folder,
            stored_filename,
        )
        try:
            self.logger.info("Uploading original file to S3",
                             extra={"s3_key": uploaded_file_key})
            self.s3_adapter.upload_bytes(
                data=file_bytes,
                key=uploaded_file_key,
                content_type=file.content_type,
            )
            uploaded_keys.append(uploaded_file_key)
        except S3UploadError as e:
            self.logger.exception("Original file upload to S3 failed")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Original file upload to S3 failed: {str(e)}",
            ) from e

        try:
            rewritten_payload, media_uploaded_keys = self._upload_media_and_rewrite(
                payload=extracted_data,
                extraction_folder=extraction_folder,
            )
            uploaded_keys.extend(media_uploaded_keys)

            self.logger.info(
                "Uploaded extracted media",
                extra={"media_objects_uploaded": len(media_uploaded_keys)},
            )
        except S3UploadError as e:
            safe_cleanup_uploaded()
            self.logger.exception(
                "Media upload to S3 failed; rolled back uploaded objects")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Media upload to S3 failed: {str(e)}",
            ) from e

        try:
            db_record_id = self.mongo_adapter.save_extraction(
                {
                    "original_filename": original_filename,
                    "stored_filename": stored_filename,
                    "extension": normalized_extension,
                    "output_format": output_format,
                    "uploaded_file_s3_key": uploaded_file_key,
                    "output_file_path": output_file_path,
                    "version": 0,
                    "extracted_data": rewritten_payload,
                }
            )
        except MongoStorageError as e:
            safe_cleanup_uploaded()
            self.logger.exception(
                "MongoDB persistence failed; rolled back uploaded objects")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"MongoDB persistence failed: {str(e)}",
            ) from e

        self.logger.info(
            "Extraction completed",
            extra={
                "original_filename": original_filename,
                "extension": normalized_extension,
                "output_format": output_format,
                "db_record_id": db_record_id,
            },
        )

        return FileUploadResponse(
            original_filename=original_filename,
            stored_filename=stored_filename,
            extension=normalized_extension,
            extracted_data=rewritten_payload,
            output_file_path=output_file_path,
            output_format=output_format,
            db_record_id=db_record_id,
            uploaded_file_s3_key=uploaded_file_key,
            version=0,
        )

    def _upload_media_and_rewrite(self, payload: dict, extraction_folder: str) -> tuple[dict, list[str]]:
        upload_cache: dict[str, str] = {}
        uploaded_media_keys: list[str] = []

        def upload_from_base64(encoded: str, file_name: str, content_type: str | None) -> str | None:
            digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
            cache_key = f"b64:{digest}"
            if cache_key in upload_cache:
                return upload_cache[cache_key]

            try:
                data = base64.b64decode(encoded)
            except (ValueError, TypeError):
                self.logger.warning("Invalid base64 media payload skipped")
                return None

            key = self.s3_adapter.build_key(
                "media", extraction_folder, file_name)
            self.s3_adapter.upload_bytes(
                data=data, key=key, content_type=content_type)
            upload_cache[cache_key] = key
            uploaded_media_keys.append(key)
            return key

        def resolve_local_path(local_file_path: str | None) -> Path | None:
            candidates: list[Path] = []

            if local_file_path:
                path = Path(local_file_path)
                if path.is_absolute():
                    candidates.append(path)
                else:
                    candidates.append(self.base_dir / path)

            for candidate in candidates:
                if candidate.exists() and candidate.is_file():
                    return candidate
            return None

        def upload_from_local_path(path: Path, file_name: str, content_type: str | None) -> str:
            cache_key = f"file:{str(path.resolve())}"
            if cache_key in upload_cache:
                return upload_cache[cache_key]

            key = self.s3_adapter.build_key(
                "media", extraction_folder, file_name)
            self.s3_adapter.upload_file(
                path, key=key, content_type=content_type)
            upload_cache[cache_key] = key
            uploaded_media_keys.append(key)
            return key

        def walk(node: object) -> object:
            if isinstance(node, dict):
                content_type = node.get("content_type") if isinstance(
                    node.get("content_type"), str) else None
                file_name = node.get("file_name") if isinstance(
                    node.get("file_name"), str) and node.get("file_name") else None

                if not file_name:
                    target_path = node.get("target_path")
                    if isinstance(target_path, str) and target_path.strip():
                        file_name = Path(target_path).name
                if not file_name:
                    file_name = f"media_{uuid.uuid4().hex}.bin"

                s3_key = None

                b64_data = node.get("base64")
                if isinstance(b64_data, str) and b64_data:
                    s3_key = upload_from_base64(
                        b64_data, file_name, content_type)
                    node.pop("base64", None)

                b64_data_alt = node.get("base64_data")
                if s3_key is None and isinstance(b64_data_alt, str) and b64_data_alt:
                    s3_key = upload_from_base64(
                        b64_data_alt, file_name, content_type)
                    node.pop("base64_data", None)

                if s3_key is None:
                    local_path = resolve_local_path(
                        local_file_path=node.get("local_file_path") if isinstance(
                            node.get("local_file_path"), str) else None,
                    )
                    if local_path is not None:
                        guessed_type = content_type or mimetypes.guess_type(str(local_path))[
                            0]
                        s3_key = upload_from_local_path(
                            local_path, file_name, guessed_type)

                if s3_key is not None:
                    node["s3_key"] = s3_key

                for key, value in node.items():
                    node[key] = walk(value)
                return node

            if isinstance(node, list):
                return [walk(item) for item in node]

            return node

        walked = walk(dict(payload))
        return (walked if isinstance(walked, dict) else payload), uploaded_media_keys
