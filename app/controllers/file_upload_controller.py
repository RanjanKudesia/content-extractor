"""File upload controller for the Content Extractor service."""
import base64
import hashlib
import json
import logging
import mimetypes
import re
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException, UploadFile, status

from app.adapters.docx_json_extraction_adapter import DocxJsonExtractionAdapter
from app.adapters.html_json_extraction_adapter import HtmlJsonExtractionAdapter
from app.adapters.markdown_json_extraction_adapter import MarkdownJsonExtractionAdapter
from app.adapters.mongodb_storage_adapter import MongoStorageError
from app.adapters.mongodb_storage_adapter import MongoDbStorageAdapter
from app.adapters.pdf_json_extraction_adapter import PdfJsonExtractionAdapter
from app.adapters.ppt_json_extraction_adapter import PptJsonExtractionAdapter
from app.adapters.s3_storage_adapter import S3UploadError
from app.adapters.s3_storage_adapter import S3StorageAdapter
from app.adapters.text_json_extraction_adapter import TextJsonExtractionAdapter
from app.schemas.file_upload_schema import FileUploadResponse

MAX_FILE_SIZE_BYTES = 20 * 1024 * 1024
FILENAME_REGEX = re.compile(r"^[A-Za-z0-9 _().-]+$")
_EXTRACTION_COMPLETED_MSG = "Extraction adapter completed"


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
            self._logger.debug("Reusing cached media upload",
                               extra={"file_name": file_name})
            return self._cache[cache_key]
        try:
            data = base64.b64decode(encoded)
        except (ValueError, TypeError):
            self._logger.warning("Invalid base64 media payload skipped")
            return None
        key = self._s3.build_key("uploads", self._folder, "media", file_name)
        self._logger.debug("Uploading base64 media", extra={"s3_key": key})
        self._s3.upload_bytes(data=data, key=key, content_type=content_type)
        self._cache[cache_key] = key
        self.uploaded_keys.append(key)
        return key

    def resolve_local_path(self, local_file_path: str | None) -> Path | None:
        """Resolve a local file path relative to the extraction base directory."""
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
            self._logger.debug(
                "Reusing cached local media upload",
                extra={"local_file_path": str(path), "file_name": file_name},
            )
            return self._cache[cache_key]
        key = self._s3.build_key("uploads", self._folder, "media", file_name)
        self._logger.debug(
            "Uploading local media file",
            extra={"local_file_path": str(path), "s3_key": key},
        )
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

        for key, value in list(node.items()):
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
                if key is None:
                    continue
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


class _MediaBase64Inliner:
    """Ensures extracted media stays inline when object storage is disabled."""

    def __init__(self, base_dir: Path, logger: logging.Logger) -> None:
        self._base_dir = base_dir
        self._logger = logger
        self._cache: dict[str, str] = {}

    def resolve_local_path(self, local_file_path: str | None) -> Path | None:
        """Resolve a local file path relative to the extraction base directory."""
        if not local_file_path:
            return None
        path = Path(local_file_path)
        candidate = path if path.is_absolute() else self._base_dir / path
        return candidate if candidate.exists() and candidate.is_file() else None

    def walk(self, node: object) -> object:
        """Recursively ensure media in *node* is inlined as base64."""
        if isinstance(node, dict):
            return self._walk_dict(node)
        if isinstance(node, list):
            return [self.walk(item) for item in node]
        return node

    def _walk_dict(self, node: dict) -> dict:
        """Inline base64 media into *node* when no inline data is already present."""
        if not self._has_inline_base64(node):
            self._inline_local_file(node)

        for key, value in list(node.items()):
            node[key] = self.walk(value)
        return node

    @staticmethod
    def _has_inline_base64(node: dict) -> bool:
        """Return True when *node* already contains an inline base64 media field."""
        return any(
            isinstance(node.get(field), str) and node.get(field)
            for field in ("base64", "base64_data")
        )

    def _inline_local_file(self, node: dict) -> None:
        """Inline a local media file into *node* as base64 when not already present."""
        raw_path = node.get("local_file_path")
        lpath = self.resolve_local_path(
            raw_path if isinstance(raw_path, str) else None)
        if lpath is None:
            return
        cache_key = str(lpath.resolve())
        encoded = self._cache.get(cache_key)
        if encoded is None:
            try:
                encoded = base64.b64encode(lpath.read_bytes()).decode("ascii")
            except OSError as exc:
                self._logger.warning(
                    "Failed to inline local media as base64",
                    extra={"local_file_path": str(lpath), "error": str(exc)},
                )
                return
            self._cache[cache_key] = encoded
        node["base64"] = encoded
        node["base64_data"] = encoded


class FileUploadController:
    """Orchestrates file upload, extraction, media upload, and persistence."""

    # Extensions that only produce JSON output.
    _JSON_ONLY_EXT: frozenset[str] = frozenset({"html", "md", "txt"})
    # Normalise legacy/alias extensions to canonical forms.
    _EXT_ALIASES: dict[str, str] = {
        "dox": "docx", "htm": "html", "ppt": "pptx"}

    def __init__(
        self,
        docx_json_adapter: DocxJsonExtractionAdapter,
        html_json_adapter: HtmlJsonExtractionAdapter,
        markdown_json_adapter: MarkdownJsonExtractionAdapter,
        text_json_adapter: TextJsonExtractionAdapter,
        ppt_json_adapter: PptJsonExtractionAdapter,
        pdf_json_adapter: PdfJsonExtractionAdapter,
        s3_adapter: S3StorageAdapter,
        mongo_adapter: MongoDbStorageAdapter,
    ) -> None:
        """Initialise the controller with all required adapters and pipelines."""
        self.logger = logging.getLogger(__name__)
        self.base_dir = Path(__file__).resolve().parents[2]
        self.docx_json_adapter = docx_json_adapter
        self.html_json_adapter = html_json_adapter
        self.markdown_json_adapter = markdown_json_adapter
        self.text_json_adapter = text_json_adapter
        self.ppt_json_adapter = ppt_json_adapter
        self.pdf_json_adapter = pdf_json_adapter
        self.s3_adapter = s3_adapter
        self.mongo_adapter = mongo_adapter

    def _cleanup_keys(self, uploaded_keys: list[str]) -> None:
        """Best-effort S3 cleanup for keys accumulated before a failure."""
        if uploaded_keys:
            self.logger.warning(
                "Starting rollback cleanup for uploaded S3 objects",
                extra={"key_count": len(uploaded_keys)},
            )
        for key in reversed(uploaded_keys):
            try:
                self.s3_adapter.delete_key(key)
            except S3UploadError:
                self.logger.warning(
                    "Failed to delete S3 key during rollback", extra={"s3_key": key})
                continue

    async def execute(
        self,
        file: UploadFile,
        user_id: str,
        extract_media: bool = True,
        store_media: bool = True,
    ) -> FileUploadResponse:
        """Validate, extract, upload, and persist an uploaded file."""
        original_filename, file_bytes, normalized_extension = (
            await self._validate_upload(file=file, user_id=user_id)
        )

        stored_filename = f"{uuid.uuid4()}.{normalized_extension}"
        basename = Path(stored_filename).stem

        extracted_data = self._run_extraction(
            file_bytes,
            normalized_extension,
            basename,
            extract_media=extract_media,
        )

        extraction_folder = basename
        uploaded_keys: list[str] = []

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
                extra={"s3_key": uploaded_file_key,
                       "size_bytes": len(file_bytes)},
            )
        except S3UploadError as e:
            self.logger.exception("Original file upload to S3 failed")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Original file upload to S3 failed: {str(e)}",
            ) from e

        rewritten_payload, media_uploaded_keys = self._prepare_payload(
            extracted_data=extracted_data,
            extraction_folder=extraction_folder,
            normalized_extension=normalized_extension,
            extract_media=extract_media,
            store_media=store_media,
            on_error=lambda: self._cleanup_keys(uploaded_keys),
        )
        uploaded_keys.extend(media_uploaded_keys)

        try:
            data_s3_key = self._upload_extracted_payload_to_s3(
                stored_filename=stored_filename,
                rewritten_payload=rewritten_payload,
            )
            uploaded_keys.append(data_s3_key)
        except S3UploadError as e:
            self._cleanup_keys(uploaded_keys)
            self.logger.exception(
                "Extracted JSON upload to S3 failed; rolled back uploaded objects"
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Extracted JSON upload to S3 failed: {str(e)}",
            ) from e

        try:
            upload_id, content_id = self._persist_to_storage(
                user_id=user_id,
                original_filename=original_filename,
                stored_filename=stored_filename,
                normalized_extension=normalized_extension,
                uploaded_file_key=uploaded_file_key,
                data_s3_key=data_s3_key,
                extract_media=extract_media,
                store_media=store_media,
            )
        except MongoStorageError as e:
            self._cleanup_keys(uploaded_keys)
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
                "extract_media": extract_media,
                "store_media": store_media,
                "upload_id": upload_id,
                "content_id": content_id,
            },
        )

        now_iso = self._now_iso()
        return FileUploadResponse(
            upload_id=upload_id,
            user_id=user_id,
            original_filename=original_filename,
            stored_filename=stored_filename,
            extension=normalized_extension,
            extract_media=extract_media,
            store_media=store_media,
            uploaded_file_s3_key=uploaded_file_key,
            content_versions=[{"content_id": content_id, "version": 0}],
            created_at=now_iso,
            updated_at=now_iso,
        )

    def execute_from_bytes(
        self,
        *,
        file_bytes: bytes,
        original_filename: str,
        content_type: str | None,
        user_id: str,
        extract_media: bool = True,
        store_media: bool = True,
    ) -> FileUploadResponse:
        """Extract content from an already-read byte buffer (e.g. fetched from URL)."""
        self.logger.info(
            "Starting extraction from byte buffer",
            extra={
                "original_filename": original_filename,
                "user_id": user_id,
                "extract_media": extract_media,
                "store_media": store_media,
                "size_bytes": len(file_bytes),
            },
        )
        if not file_bytes:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Fetched file is empty.",
            )
        if len(file_bytes) > MAX_FILE_SIZE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File size must be less than 20 MB.",
            )

        extension = Path(original_filename).suffix.lower().lstrip(".")
        stem = Path(original_filename).stem
        if not stem.strip() or not FILENAME_REGEX.fullmatch(stem):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Filename contains unsupported characters.",
            )
        _EXT_ALIASES = self._EXT_ALIASES  # pylint: disable=invalid-name
        normalized_extension = _EXT_ALIASES.get(extension, extension)

        stored_filename = f"{uuid.uuid4()}.{normalized_extension}"
        basename = Path(stored_filename).stem
        extracted_data = self._run_extraction(
            file_bytes,
            normalized_extension,
            basename,
            extract_media=extract_media,
        )

        extraction_folder = basename
        uploaded_keys: list[str] = []

        def safe_cleanup() -> None:
            self._cleanup_keys(uploaded_keys)

        uploaded_file_key = self.s3_adapter.build_key(
            "uploads", extraction_folder, stored_filename
        )
        try:
            self.s3_adapter.upload_bytes(
                data=file_bytes,
                key=uploaded_file_key,
                content_type=content_type,
            )
            uploaded_keys.append(uploaded_file_key)
        except S3UploadError as e:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Original file upload to S3 failed: {str(e)}",
            ) from e

        rewritten_payload, media_uploaded_keys = self._prepare_payload(
            extracted_data=extracted_data,
            extraction_folder=extraction_folder,
            normalized_extension=normalized_extension,
            extract_media=extract_media,
            store_media=store_media,
            on_error=safe_cleanup,
        )
        uploaded_keys.extend(media_uploaded_keys)

        try:
            data_s3_key = self._upload_extracted_payload_to_s3(
                stored_filename=stored_filename, rewritten_payload=rewritten_payload
            )
            uploaded_keys.append(data_s3_key)
        except S3UploadError as e:
            safe_cleanup()
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Extracted JSON upload to S3 failed: {str(e)}",
            ) from e

        try:
            upload_id, content_id = self._persist_to_storage(
                user_id=user_id,
                original_filename=original_filename,
                stored_filename=stored_filename,
                normalized_extension=normalized_extension,
                uploaded_file_key=uploaded_file_key,
                data_s3_key=data_s3_key,
                extract_media=extract_media,
                store_media=store_media,
            )
        except MongoStorageError as e:
            safe_cleanup()
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"MongoDB persistence failed: {str(e)}",
            ) from e

        now_iso = self._now_iso()
        self.logger.info(
            "Extraction from byte buffer completed",
            extra={"upload_id": upload_id, "content_id": content_id},
        )
        return FileUploadResponse(
            upload_id=upload_id,
            user_id=user_id,
            original_filename=original_filename,
            stored_filename=stored_filename,
            extension=normalized_extension,
            extract_media=extract_media,
            store_media=store_media,
            uploaded_file_s3_key=uploaded_file_key,
            content_versions=[{"content_id": content_id, "version": 0}],
            created_at=now_iso,
            updated_at=now_iso,
        )

    def execute_reprocess(
        self,
        *,
        upload_id: str,
        extract_media: bool = True,
        store_media: bool = True,
    ) -> tuple[str, int]:
        """Re-extract content from the stored original file.

        Downloads the source from S3, runs the extraction pipeline, stores a new
        extracted JSON under a versioned S3 key, and appends a new content version
        to the upload record.

        Returns (new_content_id, new_version).
        """
        self.logger.info(
            "Starting reprocess",
            extra={
                "upload_id": upload_id,
                "extract_media": extract_media,
                "store_media": store_media,
            },
        )
        upload = self.mongo_adapter.get_upload(upload_id)
        if not upload:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Upload not found.",
            )

        uploaded_file_key: str = upload.get("uploaded_file_s3_key", "")
        stored_filename: str = upload.get("stored_filename", "")
        normalized_extension: str = upload.get("extension", "")

        try:
            file_bytes = self.s3_adapter.download_bytes(uploaded_file_key)
        except S3UploadError as e:
            self.logger.exception(
                "Failed to download source file for reprocess")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to download source file from S3: {str(e)}",
            ) from e

        basename = Path(stored_filename).stem
        extracted_data = self._run_extraction(
            file_bytes, normalized_extension, basename, extract_media=extract_media
        )

        extraction_folder = basename
        uploaded_keys: list[str] = []

        rewritten_payload, media_uploaded_keys = self._prepare_payload(
            extracted_data=extracted_data,
            extraction_folder=extraction_folder,
            normalized_extension=normalized_extension,
            extract_media=extract_media,
            store_media=store_media,
            on_error=lambda: self._cleanup_keys(uploaded_keys),
        )
        uploaded_keys.extend(media_uploaded_keys)

        # Delegate version computation to MongoDB atomically (via $inc).
        try:
            existing_contents = self.mongo_adapter.get_contents_for_upload(
                upload_id)
        except MongoStorageError as e:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=str(e),
            ) from e
        next_version = max((c.get("version", 0)
                           for c in existing_contents), default=-1) + 1
        self.logger.info(
            "Calculated next content version for reprocess",
            extra={"upload_id": upload_id, "next_version": next_version},
        )

        json_key_name = f"{basename}.json"
        data_s3_key = self.s3_adapter.build_key(
            "extracted", basename, f"version-{next_version}", json_key_name
        )
        try:
            json_bytes = json.dumps(
                rewritten_payload, default=str).encode("utf-8")
            self.s3_adapter.upload_bytes(
                data=json_bytes, key=data_s3_key, content_type="application/json"
            )
            uploaded_keys.append(data_s3_key)
        except S3UploadError as e:
            self._cleanup_keys(uploaded_keys)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Extracted JSON upload to S3 failed: {str(e)}",
            ) from e

        try:
            content_id, version = self.mongo_adapter.add_new_content_version(
                upload_id=upload_id,
                data_s3_key=data_s3_key,
            )
        except MongoStorageError as e:
            self._cleanup_keys(uploaded_keys)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"MongoDB persistence failed: {str(e)}",
            ) from e

        self.logger.info(
            "Reprocess completed",
            extra={
                "upload_id": upload_id,
                "content_id": content_id,
                "version": version,
            },
        )
        return content_id, version

    def _prepare_payload(
        self,
        *,
        extracted_data: dict,
        extraction_folder: str,
        normalized_extension: str,
        extract_media: bool,
        store_media: bool,
        on_error: Callable[[], None],
    ) -> tuple[dict, list[str]]:
        """Handle media post-processing and return (rewritten_payload, media_uploaded_keys)."""
        if not extract_media:
            self.logger.debug("Media extraction disabled; payload unchanged")
            return extracted_data, []
        if not store_media:
            self.logger.debug("Keeping media inline as base64")
            return self._inline_media_as_base64(extracted_data), []
        if normalized_extension in {"md", "txt"}:
            self.logger.debug(
                "Skipping media upload for text-only format",
                extra={"extension": normalized_extension},
            )
            return extracted_data, []
        try:
            rewritten, keys = self._upload_media_and_rewrite(
                payload=extracted_data, extraction_folder=extraction_folder
            )
            self.logger.info(
                "Uploaded media and rewrote payload",
                extra={"media_key_count": len(keys)},
            )
            return rewritten, keys
        except S3UploadError as e:
            on_error()
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Media upload to S3 failed: {str(e)}",
            ) from e

    async def _validate_upload(
        self, file: UploadFile, user_id: str
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
                "user_id": user_id,
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

        _EXT_ALIASES = self._EXT_ALIASES  # pylint: disable=invalid-name
        normalized_extension = _EXT_ALIASES.get(extension, extension)
        self.logger.debug(
            "Upload validation successful",
            extra={
                "original_filename": original_filename,
                "normalized_extension": normalized_extension,
                "size_bytes": len(file_bytes),
            },
        )
        return original_filename, file_bytes, normalized_extension

    def _assert_extension_supported(self, normalized_extension: str) -> None:
        """Raise HTTP 501 if the file extension is unsupported."""
        if normalized_extension not in {"docx", "pdf", "md", "txt", "html", "pptx"}:
            self.logger.warning(
                "Unsupported extension requested",
                extra={"extension": normalized_extension},
            )
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
        basename: str,
        extract_media: bool,
    ) -> dict:
        """Dispatch to the appropriate extraction adapter and return the result."""
        self._assert_extension_supported(normalized_extension)
        self.logger.info(
            "Running extraction adapter",
            extra={
                "extension": normalized_extension,
                "extract_media": extract_media,
                "size_bytes": len(file_bytes),
            },
        )
        kwargs: dict = {
            "file_bytes": file_bytes,
            "output_basename": basename,
            "include_media": extract_media,
        }

        if normalized_extension in self._JSON_ONLY_EXT:
            adapters = {
                "html": self.html_json_adapter,
                "md": self.markdown_json_adapter,
                "txt": self.text_json_adapter,
            }
            data, _ = adapters[normalized_extension].run(**kwargs)
            self.logger.info(
                _EXTRACTION_COMPLETED_MSG,
                extra={"extension": normalized_extension},
            )
            return data

        if normalized_extension == "pptx":
            data, _ = self.ppt_json_adapter.run(**kwargs)
            self.logger.info(
                _EXTRACTION_COMPLETED_MSG,
                extra={"extension": normalized_extension},
            )
            return data

        if normalized_extension == "pdf":
            try:
                data, _ = self.pdf_json_adapter.run(**kwargs)
                self.logger.info(
                    _EXTRACTION_COMPLETED_MSG,
                    extra={"extension": normalized_extension},
                )
                return data
            except (ValueError, TypeError, OSError) as e:
                self.logger.exception("PDF extraction failed")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to extract PDF: {str(e)}",
                ) from e

        data, _ = self.docx_json_adapter.run(**kwargs)
        self.logger.info(
            _EXTRACTION_COMPLETED_MSG,
            extra={"extension": normalized_extension},
        )
        return data

    def _persist_to_storage(
        self,
        *,
        user_id: str,
        original_filename: str,
        stored_filename: str,
        normalized_extension: str,
        uploaded_file_key: str,
        data_s3_key: str,
        extract_media: bool,
        store_media: bool,
    ) -> tuple[str, str]:
        """Persist metadata in MongoDB; return (upload_id, content_id)."""
        self.logger.info(
            "Persisting extraction metadata in MongoDB",
            extra={
                "original_filename": original_filename,
                "extension": normalized_extension,
                "extract_media": extract_media,
                "store_media": store_media,
            },
        )
        return self.mongo_adapter.save_extraction_bundle(
            user_id=user_id,
            original_filename=original_filename,
            stored_filename=stored_filename,
            extension=normalized_extension,
            extract_media=extract_media,
            store_media=store_media,
            uploaded_file_s3_key=uploaded_file_key,
            data_s3_key=data_s3_key,
        )

    def _upload_extracted_payload_to_s3(
        self, *, stored_filename: str, rewritten_payload: dict
    ) -> str:
        """Upload extracted JSON payload to S3 and return its key."""
        json_bytes = json.dumps(rewritten_payload, default=str).encode("utf-8")
        basename = Path(stored_filename).stem
        data_s3_key = self.s3_adapter.build_key(
            "extracted", basename, f"{basename}.json")
        self.s3_adapter.upload_bytes(
            data=json_bytes,
            key=data_s3_key,
            content_type="application/json",
        )
        self.logger.info(
            "Extracted JSON uploaded to S3",
            extra={"data_s3_key": data_s3_key, "size_bytes": len(json_bytes)},
        )
        return data_s3_key

    def _upload_media_and_rewrite(
        self, payload: dict, extraction_folder: str
    ) -> tuple[dict, list[str]]:
        """Upload embedded media to S3 and rewrite keys in the payload."""
        self.logger.debug(
            "Rewriting payload media references to S3 keys",
            extra={"extraction_folder": extraction_folder},
        )
        uploader = _MediaUploader(
            s3_adapter=self.s3_adapter,
            base_dir=self.base_dir,
            extraction_folder=extraction_folder,
            logger=self.logger,
        )
        walked = uploader.walk(dict(payload))
        self.logger.debug(
            "Completed media rewrite",
            extra={"uploaded_media_count": len(uploader.uploaded_keys)},
        )
        return (walked if isinstance(walked, dict) else payload), uploader.uploaded_keys

    def _inline_media_as_base64(self, payload: dict) -> dict:
        """Ensure extracted media stays inline when media storage is disabled."""
        self.logger.debug("Inlining media as base64 in extraction payload")
        inliner = _MediaBase64Inliner(
            base_dir=self.base_dir,
            logger=self.logger,
        )
        walked = inliner.walk(dict(payload))
        return walked if isinstance(walked, dict) else payload

    @staticmethod
    def _now_iso() -> str:
        """Return the current UTC time as an ISO-8601 string."""
        return datetime.now(timezone.utc).isoformat()
