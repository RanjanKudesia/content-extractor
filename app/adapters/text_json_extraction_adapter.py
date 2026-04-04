import json
import re
from typing import Any


class TextJsonExtractionAdapter:
    def __init__(self) -> None:
        pass

    def run(self, file_bytes: bytes, output_basename: str) -> tuple[dict[str, Any], str]:
        text = file_bytes.decode("utf-8-sig", errors="replace")
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

            is_bullet = bool(re.match(r"^\s*[-*+]\s+", raw))
            is_numbered = bool(re.match(r"^\s*\d+[.)]\s+", raw))
            numbering_format = None

            if is_bullet:
                numbering_format = "bullet"
                raw = re.sub(r"^\s*[-*+]\s+", "", raw, count=1)
            elif is_numbered:
                marker = re.match(r"^\s*(\d+[.)])\s+", raw)
                numbering_format = marker.group(1) if marker else "1."
                raw = re.sub(r"^\s*\d+[.)]\s+", "", raw, count=1)

            paragraph = {
                "index": paragraph_index,
                "text": raw,
                "style": None,
                "is_bullet": is_bullet,
                "is_numbered": is_numbered,
                "list_info": {
                    "kind": "bullet" if is_bullet else ("numbered" if is_numbered else None),
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
            paragraphs.append(paragraph)
            document_order.append(
                {"type": "paragraph", "index": paragraph_index})
            paragraph_index += 1

        for line in lines:
            if line.strip():
                current_block.append(line.rstrip())
            else:
                flush_block()

        flush_block()

        extracted: dict[str, Any] = {
            "metadata": {
                "source_type": "txt",
                "extraction_mode": "txt",
            },
            "document_order": document_order,
            "styles": [],
            "numbering": [],
            "sections": [],
            "media": [],
            "paragraphs": paragraphs,
            "tables": [],
            "document_defaults": None,
        }

        return extracted, f"virtual://extracted/{output_basename}.txt.json"
