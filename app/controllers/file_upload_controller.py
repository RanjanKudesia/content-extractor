"""File upload controller for content-extractor service."""
import logging
import re
import uuid
import base64
import hashlib
import mimetypes
from datetime import datetime, timezone
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
from app.adapters.pdf_json_extraction_adapter import PdfJsonExtractionAdapter
from app.pipelines.ppt_xml_extraction_pipeline import PptXmlExtractionPipeline
from app.schemas.file_upload_schema import FileUploadResponse

MAX_FILE_SIZE_BYTES = 20 * 1024 * 1024
FILENAME_REGEX = re.compile(r"^[A-Za-z0-9 _().-]+$")


class _MediaUploader:
    """Uploads media assets to S3 and rewrites payload references in-place."""

    def __init__(
        self,
        s3_adapter: S3StorageAdapter,
        base_dir: Path,
        extraction_folder: str,
        logger: logging.Logger,
    ) -> None:
        self._s3 = s3_adapter
        self._base_dir = base_dir
        self._folder = extraction_folder
        self._logger = logger
        self._cache: dict[str, str] = {}
        self.uploaded_keys: list[str] = []

    def upload_from_base64(
        self, encoded: str, file_name: str, content_type: str | None
    ) -> str | None:
        """Decode a base64 string, upload it to S3, and return the S3 key."""
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        cache_key = f"b64:{digest}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        try:
            data = base64.b64decode(encoded)
        except (ValueError, TypeError):
            self._logger.warning("Invalid base64 media payload skipped")
            return None
        key = self._s3.build_key("uploads", self._folder, "media", file_name)
        self._s3.upload_bytes(data=data, key=key, content_type=content_type)
        self._cache[cache_key] = key
        self.uploaded_keys.append(key)
        return key

    def resolve_local_path(self, local_file_path: str | None) -> Path | None:
        if not local_file_path:
            return None
        path = Path(local_file_path)
        candidate = path if path.is_absolute() else self._base_dir / path
        return candidate if candidate.exists() and candidate.is_file() else None

    def upload_from_local_path(
        self, path: Path, file_name: str, content_type: str | None
    ) -> str:
        """Upload a local file to S3 and return the S3 key."""
        cache_key = f"file:{str(path.resolve())}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        key = self._s3.build_key("uploads", self._folder, "media", file_name)
        self._s3.upload_file(path, key=key, content_type=content_type)
        self._cache[cache_key] = key
        self.uploaded_keys.append(key)
        return key

    def walk(self, node: object) -> object:
        """Recursively rewrite media references in a payload dict/list tree."""
        if isinstance(node, dict):
            return self._walk_dict(node)
        if isinstance(node, list):
            return [self.walk(item) for item in node]
        return node

    def _walk_dict(self, node: dict) -> dict:
        """Upload any media embedded in *node* and rewrite references in-place."""
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

        s3_key = self._try_upload_base64(node, file_name, content_type)
        if s3_key is None:
            s3_key = self._try_upload_local(node, file_name, content_type)

        if s3_key is not None:
            node["s3_key"] = s3_key

        for key, value in node.items():
            node[key] = self.walk(value)
        return node

    def _try_upload_base64(
        self, node: dict, file_name: str, content_type: str | None
    ) -> str | None:
        """Try to upload base64-encoded data from *node*; returns S3 key or None."""
        for field in ("base64", "base64_data"):
            data = node.get(field)
            if isinstance(data, str) and data:
                key = self.upload_from_base64(data, file_name, content_type)
                node.pop(field, None)
                return key
        return None

    def _try_upload_local(
        self, node: dict, file_name: str, content_type: str | None
    ) -> str | None:
        """Try to upload a local file referenced in *node*; returns S3 key or None."""
        raw_path = node.get("local_file_path")
        lpath = self.resolve_local_path(
            raw_path if isinstance(raw_path, str) else None
        )
        if lpath is None:
            return None
        guessed_type = content_type or mimetypes.guess_type(str(lpath))[0]
        return self.upload_from_local_path(lpath, file_name, guessed_type)


class FileUploadController:
    """Orchestrates file upload, extraction, media upload, and persistence."""

    # Extensions that only produce JSON output.
    _JSON_ONLY_EXT: frozenset[str] = frozenset({"html", "md", "txt"})

    def __init__(
        self,
        docx_json_adapter: DocxJsonExtractionAdapter,
        docx_xml_adapter: DocxXmlExtractionAdapter,
        html_json_adapter: HtmlJsonExtractionAdapter,
        markdown_json_adapter: MarkdownJsonExtractionAdapter,
        text_json_adapter: TextJsonExtractionAdapter,
        ppt_json_adapter: PptJsonExtractionAdapter,
        ppt_xml_pipeline: PptXmlExtractionPipeline,
        pdf_json_adapter: PdfJsonExtractionAdapter,
        s3_adapter: S3StorageAdapter,
        mongo_adapter: MongoDbStorageAdapter,
    ) -> None:
        """Initialise the controller with all required adapters and pipelines."""
        self.logger = logging.getLogger(__name__)
        self.base_dir = Path(__file__).resolve().parents[2]
        self.docx_json_adapter = docx_json_adapter
        self.docx_xml_adapter = docx_xml_adapter
        self.html_json_adapter = html_json_adapter
        self.markdown_json_adapter = markdown_json_adapter
        self.text_json_adapter = text_json_adapter
        self.ppt_json_adapter = ppt_json_adapter
        self.ppt_xml_pipeline = ppt_xml_pipeline
        self.pdf_json_adapter = pdf_json_adapter
        self.s3_adapter = s3_adapter
        self.mongo_adapter = mongo_adapter

    async def execute(
        self,
        file: UploadFile,
        user_id: str,
        output_format: Literal["json", "xml"] = "json",
    ) -> FileUploadResponse:
        """Validate, extract, upload, and persist an uploaded file."""
        original_filename, file_bytes, normalized_extension = (
            await self._validate_upload(file)
        )

        stored_filename = f"{uuid.uuid4()}.{normalized_extension}"
        basename = Path(stored_filename).stem

        self._assert_format_supported(output_format, normalized_extension)

        extracted_data = self._run_extraction(
            file_bytes, normalized_extension, output_format, basename
        )

        extraction_folder = basename
        uploaded_keys: list[str] = []

        def safe_cleanup_uploaded() -> None:
            for key in reversed(uploaded_keys):
                try:
                    self.s3_adapter.delete_key(key)
                except S3UploadError:
                    continue

        uploaded_file_key = self.s3_adapter.build_key(
            "uploads", extraction_folder, stored_filename
        )
        try:
            self.logger.info(
                "Uploading original file to S3",
                extra={"s3_key": uploaded_file_key},
            )
            self.s3_adapter.upload_bytes(
                data=file_bytes,
                key=uploaded_file_key,
                content_type=file.content_type,
            )
            uploaded_keys.append(uploaded_file_key)
            self.logger.info(
                "Uploaded original file to S3",
                extra={"s3_key": uploaded_file_key, "size_bytes": len(file_bytes)},
            )
        except S3UploadError as e:
            self.logger.exception("Original file upload to S3 failed")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Original file upload to S3 failed: {str(e)}",
            ) from e

        if normalized_extension in {"md", "txt"}:
            # Text-like extractions do not emit media assets; avoid full payload traversal.
            rewritten_payload = extracted_data
            media_uploaded_keys: list[str] = []
            self.logger.info(
                "Skipped media upload pass for text-like extraction",
                extra={"extension": normalized_extension},
            )
        else:
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
                    "Media upload to S3 failed; rolled back uploaded objects"
                )
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Media upload to S3 failed: {str(e)}",
                ) from e

        try:
            upload_id, content_id = self._persist_to_storage(
                user_id=user_id,
                original_filename=original_filename,
                stored_filename=stored_filename,
                normalized_extension=normalized_extension,
                output_format=output_format,
                uploaded_file_key=uploaded_file_key,
                rewritten_payload=rewritten_payload,
            )
        except MongoStorageError as e:
            safe_cleanup_uploaded()
            self.logger.exception(
                "MongoDB persistence failed; rolled back uploaded objects"
            )
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
                "upload_id": upload_id,
                "content_id": content_id,
            },
        )

        return FileUploadResponse(
            upload_id=upload_id,
            user_id=user_id,
            original_filename=original_filename,
            stored_filename=stored_filename,
            extension=normalized_extension,
            output_format=output_format,
            uploaded_file_s3_key=uploaded_file_key,
            content_versions=[{"content_id": content_id, "version": 0}],
            created_at=self._now_iso(),
            updated_at=self._now_iso(),
        )

    async def _validate_upload(
        self, file: UploadFile
    ) -> tuple[str, bytes, str]:
        """Validate filename, size, and extension; return (filename, bytes, ext)."""
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
            extra={
                "original_filename": original_filename,
                "user_id": "unknown",
            },
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

        _EXT_ALIASES = {"dox": "docx", "htm": "html", "ppt": "pptx"}
        normalized_extension = _EXT_ALIASES.get(extension, extension)
        return original_filename, file_bytes, normalized_extension

    def _assert_format_supported(
        self, output_format: str, normalized_extension: str
    ) -> None:
        """Raise HTTP 400/501 if the format/extension combination is unsupported."""
        if output_format == "xml" and normalized_extension not in {"docx", "pptx"}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="output_format='xml' is supported only for DOCX and PPTX files.",
            )
        if normalized_extension not in {"docx", "pdf", "md", "txt", "html", "pptx"}:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail=(
                    "Only PDF, DOCX, MD, TXT, HTML, and PPTX extraction "
                    "are implemented right now."
                ),
            )

    def _run_extraction(
        self,
        file_bytes: bytes,
        normalized_extension: str,
        output_format: str,
        basename: str,
    ) -> dict:
        """Dispatch to the appropriate extraction adapter and return the result."""
        kwargs: dict = {"file_bytes": file_bytes, "output_basename": basename}

        if normalized_extension in self._JSON_ONLY_EXT:
            adapters = {
                "html": self.html_json_adapter,
                "md": self.markdown_json_adapter,
                "txt": self.text_json_adapter,
            }
            data, _ = adapters[normalized_extension].run(**kwargs)
            return data

        if normalized_extension == "pptx":
            pipeline = (
                self.ppt_xml_pipeline if output_format == "xml"
                else self.ppt_json_adapter
            )
            data, _ = pipeline.run(**kwargs)
            return data

        if normalized_extension == "pdf":
            try:
                data, _ = self.pdf_json_adapter.run(**kwargs)
                return data
            except (ValueError, TypeError, OSError) as e:
                self.logger.exception("PDF extraction failed")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to extract PDF: {str(e)}",
                ) from e

        # DOCX — json or xml
        if output_format == "xml":
            data, _ = self.docx_xml_adapter.run(**kwargs)
        else:
            data, _ = self.docx_json_adapter.run(**kwargs)
        return data

    def _persist_to_storage(
        self,
        *,
        user_id: str,
        original_filename: str,
        stored_filename: str,
        normalized_extension: str,
        output_format: str,
        uploaded_file_key: str,
        rewritten_payload: dict,
    ) -> tuple[str, str]:
        """Upload JSON to S3, save metadata to MongoDB; return (upload_id, content_id)."""
        import json as _json

        # Serialize the full payload and upload to S3 instead of embedding in MongoDB.
        json_bytes = _json.dumps(
            rewritten_payload, default=str).encode("utf-8")
        basename = Path(stored_filename).stem
        data_s3_key = self.s3_adapter.build_key(
            "extracted", basename, f"{basename}.json"
        )
        self.s3_adapter.upload_bytes(
            data=json_bytes,
            key=data_s3_key,
            content_type="application/json",
        )
        self.logger.info(
            "Extracted JSON uploaded to S3",
            extra={"data_s3_key": data_s3_key, "size_bytes": len(json_bytes)},
        )

        upload_id = self.mongo_adapter.save_upload(
            {
                "user_id": user_id,
                "original_filename": original_filename,
                "stored_filename": stored_filename,
                "extension": normalized_extension,
                "output_format": output_format,
                "uploaded_file_s3_key": uploaded_file_key,
            }
        )
        content_id = self.mongo_adapter.save_content(
            {
                "upload_id": upload_id,
                "version": 0,
                "data_s3_key": data_s3_key,
            }
        )
        self.mongo_adapter.add_content_version(
            upload_id, content_id, version=0)
        return upload_id, content_id

    def _upload_media_and_rewrite(
        self, payload: dict, extraction_folder: str
    ) -> tuple[dict, list[str]]:
        """Upload embedded media to S3 and rewrite keys in the payload."""
        uploader = _MediaUploader(
            s3_adapter=self.s3_adapter,
            base_dir=self.base_dir,
            extraction_folder=extraction_folder,
            logger=self.logger,
        )
        walked = uploader.walk(dict(payload))
        return (walked if isinstance(walked, dict) else payload), uploader.uploaded_keys

    @staticmethod
    def _now_iso() -> str:
        """Return the current UTC time as an ISO-8601 string."""
        return datetime.now(timezone.utc).isoformat()
