"""Plain text extraction pipeline for content-extractor service."""

import logging
import re
from typing import Any

BULLET_PATTERN = r"^\s*[-*+]\s+"
NUMBERED_PATTERN = r"^\s*\d+[.)]\s+"

logger = logging.getLogger(__name__)


class TextExtractionPipeline:
    """Extract plain text content to JSON format."""

    @staticmethod
    def _is_list_line(line: str) -> bool:
        """Return True when the line starts with a bullet or numbered marker."""
        return bool(re.match(BULLET_PATTERN, line)) or bool(
            re.match(NUMBERED_PATTERN, line)
        )

    @staticmethod
    def _parse_block(raw: str) -> tuple[str, bool, bool, str | None, str | None]:
        """Normalize a block and derive list metadata.

        Returns: (text, is_bullet, is_numbered, list_kind, numbering_format)
        """
        is_bullet = bool(re.match(BULLET_PATTERN, raw))
        is_numbered = bool(re.match(NUMBERED_PATTERN, raw))
        numbering_format: str | None = None
        list_kind: str | None = None

        if is_bullet:
            list_kind = "bullet"
            numbering_format = "bullet"
            raw = re.sub(BULLET_PATTERN, "", raw, count=1)
        elif is_numbered:
            list_kind = "numbered"
            marker = re.match(r"^\s*(\d+[.)])\s+", raw)
            numbering_format = marker.group(1) if marker else "1."
            raw = re.sub(NUMBERED_PATTERN, "", raw, count=1)

        return raw, is_bullet, is_numbered, list_kind, numbering_format

    @staticmethod
    def _build_paragraph(
        paragraph_index: int,
        raw: str,
        is_bullet: bool,
        is_numbered: bool,
        list_kind: str | None,
        numbering_format: str | None,
    ) -> dict[str, Any]:
        """Build a normalized paragraph object."""
        return {
            "index": paragraph_index,
            "text": raw,
            "style": None,
            "is_bullet": is_bullet,
            "is_numbered": is_numbered,
            "list_info": {
                "kind": list_kind,
                "numbering_format": numbering_format,
            }
            if (is_bullet or is_numbered)
            else None,
            "numbering_format": numbering_format,
            "alignment": None,
            "runs": [
                {
                    "index": 0,
                    "text": raw,
                    "bold": None,
                    "italic": None,
                    "underline": None,
                    "font_name": None,
                    "font_size_pt": None,
                    "color_rgb": None,
                    "highlight_color": None,
                    "hyperlink_url": None,
                    "embedded_media": [],
                }
            ],
            "source": {"format": "txt"},
        }

    def run(
        self,
        file_bytes: bytes,
        include_media: bool = True,
    ) -> dict[str, Any]:
        """Extract plain text and return JSON data."""
        logger.debug(
            "Starting TXT extraction pipeline",
            extra={"include_media": include_media,
                   "size_bytes": len(file_bytes)},
        )
        _ = include_media
        try:
            text = file_bytes.decode("utf-8-sig", errors="replace")
        except Exception as e:
            logger.exception("TXT decode failed")
            raise ValueError(f"Failed to decode text: {str(e)}") from e

        lines = text.splitlines()
        paragraphs: list[dict[str, Any]] = []
        document_order: list[dict[str, Any]] = []

        paragraph_index = 0
        current_block: list[str] = []

        def flush_block() -> None:
            nonlocal paragraph_index, current_block
            if not current_block:
                return

            raw = "\n".join(current_block).strip()
            current_block = []
            if not raw:
                return

            raw, is_bullet, is_numbered, list_kind, numbering_format = self._parse_block(
                raw)

            paragraph = self._build_paragraph(
                paragraph_index=paragraph_index,
                raw=raw,
                is_bullet=is_bullet,
                is_numbered=is_numbered,
                list_kind=list_kind,
                numbering_format=numbering_format,
            )
            paragraphs.append(paragraph)
            document_order.append(
                {"type": "paragraph", "index": paragraph_index})
            paragraph_index += 1

        for line in lines:
            stripped = line.strip()
            if not stripped:
                flush_block()
                continue

            is_list_line = self._is_list_line(line)

            if is_list_line:
                # Keep each list item as its own paragraph entry
                flush_block()
                current_block.append(line.rstrip())
                flush_block()
            else:
                current_block.append(line.rstrip())

        flush_block()

        result = {
            "metadata": {
                "source_type": "txt",
                "extraction_mode": "txt",
            },
            "document_order": document_order,
            "document_defaults": None,
            "styles": [],
            "numbering": [],
            "sections": [],
            "paragraphs": paragraphs,
            "tables": [],
            "media": [],
        }
        logger.debug(
            "Completed TXT extraction pipeline",
            extra={"paragraph_count": len(paragraphs)},
        )
        return result
