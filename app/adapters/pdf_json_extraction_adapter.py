import json
import re
from collections import Counter
from io import BytesIO
from statistics import median
from typing import Any

import pdfplumber


class PdfJsonExtractionAdapter:
    _TABLE_SETTINGS_CANDIDATES: tuple[dict[str, Any], ...] = (
        {
            "vertical_strategy": "lines_strict",
            "horizontal_strategy": "lines_strict",
            "intersection_tolerance": 5,
            "snap_tolerance": 3,
            "join_tolerance": 3,
            "text_x_tolerance": 2,
            "text_y_tolerance": 2,
        },
        {
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "intersection_tolerance": 5,
            "snap_tolerance": 3,
            "join_tolerance": 3,
            "text_x_tolerance": 2,
            "text_y_tolerance": 2,
        },
    )

    def __init__(self) -> None:
        pass

    def run(self, file_bytes: bytes, output_basename: str) -> tuple[dict[str, Any], str]:
        paragraphs: list[dict[str, Any]] = []
        tables: list[dict[str, Any]] = []
        media: list[dict[str, Any]] = []
        ordered_blocks: list[dict[str, Any]] = []

        paragraph_index = 0
        table_index = 0

        with pdfplumber.open(BytesIO(file_bytes), unicode_norm="NFKC") as pdf:
            metadata = dict(pdf.metadata or {})
            page_count = len(pdf.pages)

            for page_idx, page in enumerate(pdf.pages):
                # Remove duplicated glyph overlays before extracting words.
                deduped_page = page.dedupe_chars(
                    tolerance=1, extra_attrs=("fontname", "size")
                )

                page_tables, table_bboxes, page_table_blocks, table_index = self._extract_tables(
                    deduped_page,
                    page_number=page_idx + 1,
                    start_index=table_index,
                )
                tables.extend(page_tables)
                ordered_blocks.extend(page_table_blocks)

                page_paragraphs, page_paragraph_blocks, paragraph_index = self._extract_paragraphs(
                    deduped_page,
                    page_number=page_idx + 1,
                    table_bboxes=table_bboxes,
                    start_index=paragraph_index,
                )
                paragraphs.extend(page_paragraphs)
                ordered_blocks.extend(page_paragraph_blocks)

                for image_idx, image in enumerate(page.images or []):
                    width = image.get("width")
                    height = image.get("height")
                    media.append(
                        {
                            "relationship_id": f"pdf_page_{page_idx + 1}_img_{image_idx + 1}",
                            "content_type": None,
                            "file_name": None,
                            "local_file_path": None,
                            "local_url": None,
                            "width_emu": int(width * 12700) if isinstance(width, (int, float)) else None,
                            "height_emu": int(height * 12700) if isinstance(height, (int, float)) else None,
                            "alt_text": None,
                            "source": {
                                "page_number": page_idx + 1,
                                "x0": image.get("x0"),
                                "y0": image.get("y0"),
                                "x1": image.get("x1"),
                                "y1": image.get("y1"),
                                "width": image.get("width"),
                                "height": image.get("height"),
                            },
                        }
                    )

        ordered_blocks.sort(
            key=lambda item: (item.get("page_number", 0),
                              item.get("top", 0.0), item.get("index", 0))
        )
        document_order = [
            {"type": item["type"], "index": item["index"]}
            for item in ordered_blocks
        ]

        extracted: dict[str, Any] = {
            "metadata": {
                "source_type": "pdf",
                "extraction_mode": "pdf",
                "page_count": page_count,
                "pdf_metadata": metadata,
            },
            "document_order": document_order,
            "styles": [],
            "numbering": [],
            "sections": [],
            "media": media,
            "paragraphs": paragraphs,
            "tables": tables,
            "document_defaults": None,
        }

        return extracted, f"virtual://extracted/{output_basename}.pdf.json"

    def _extract_tables(
        self,
        page,
        page_number: int,
        start_index: int,
    ) -> tuple[list[dict[str, Any]], list[tuple[float, float, float, float]], list[dict[str, Any]], int]:
        seen_bboxes: set[tuple[int, int, int, int]] = set()
        table_objects: list[Any] = []

        for settings in self._TABLE_SETTINGS_CANDIDATES:
            try:
                found = page.find_tables(table_settings=settings) or []
            except (ValueError, TypeError, AttributeError, KeyError):
                continue
            for table_obj in found:
                bbox = getattr(table_obj, "bbox", None)
                if not bbox or len(bbox) != 4:
                    continue
                key = tuple(int(round(v * 10)) for v in bbox)
                if key in seen_bboxes:
                    continue
                seen_bboxes.add(key)
                table_objects.append(table_obj)

        tables: list[dict[str, Any]] = []
        table_bboxes: list[tuple[float, float, float, float]] = []
        ordered_blocks: list[dict[str, Any]] = []
        table_index = start_index

        for table_obj in table_objects:
            bbox = getattr(table_obj, "bbox", None)
            if not bbox or len(bbox) != 4:
                continue

            width = float(bbox[2]) - float(bbox[0])
            height = float(bbox[3]) - float(bbox[1])
            if width < 40 or height < 20:
                continue

            try:
                raw_table = table_obj.extract(x_tolerance=2, y_tolerance=2)
            except (ValueError, TypeError, AttributeError, KeyError):
                raw_table = table_obj.extract()

            cleaned_rows: list[list[str]] = []
            for row in raw_table or []:
                row_cells = [self._clean_text(cell) for cell in (row or [])]
                if any(cell for cell in row_cells):
                    cleaned_rows.append(row_cells)

            if not cleaned_rows:
                continue

            row_count = len(cleaned_rows)
            col_count = max((len(r) for r in cleaned_rows), default=0)
            non_empty_cells = sum(
                1 for row in cleaned_rows for cell in row if cell.strip()
            )

            # Filter common false positives where layout lines are mistaken for tables.
            if row_count < 2 or col_count < 2 or non_empty_cells < 4:
                continue
            if row_count <= 2 and col_count <= 2 and non_empty_cells <= 6:
                continue

            rows: list[dict[str, Any]] = []
            max_cols = 0
            for row_idx, row in enumerate(cleaned_rows):
                cells: list[dict[str, Any]] = []
                for col_idx, cell_text in enumerate(row):
                    cells.append(
                        {
                            "text": cell_text,
                            "paragraphs": [
                                {
                                    "index": 0,
                                    "text": cell_text,
                                    "style": None,
                                    "is_bullet": False,
                                    "is_numbered": False,
                                    "list_info": None,
                                    "numbering_format": None,
                                    "alignment": None,
                                    "runs": [
                                        {
                                            "index": 0,
                                            "text": cell_text,
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
                                }
                            ],
                            "tables": [],
                            "cell_index": col_idx,
                        }
                    )
                max_cols = max(max_cols, len(cells))
                rows.append({"row_index": row_idx, "cells": cells})

            tables.append(
                {
                    "index": table_index,
                    "row_count": len(rows),
                    "column_count": max_cols,
                    "style": None,
                    "rows": rows,
                    "source": {
                        "page_number": page_number,
                        "bbox": {
                            "x0": bbox[0],
                            "top": bbox[1],
                            "x1": bbox[2],
                            "bottom": bbox[3],
                        },
                    },
                }
            )
            table_bboxes.append((bbox[0], bbox[1], bbox[2], bbox[3]))
            ordered_blocks.append(
                {
                    "type": "table",
                    "index": table_index,
                    "page_number": page_number,
                    "top": float(bbox[1]),
                }
            )
            table_index += 1

        return tables, table_bboxes, ordered_blocks, table_index

    def _extract_paragraphs(
        self,
        page,
        page_number: int,
        table_bboxes: list[tuple[float, float, float, float]],
        start_index: int,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
        words = page.extract_words(
            x_tolerance=2,
            y_tolerance=2,
            use_text_flow=True,
            keep_blank_chars=False,
            extra_attrs=["fontname", "size"],
        ) or []

        filtered_words = [
            w for w in words
            if self._clean_text(w.get("text"))
            and not self._word_in_any_bbox(w, table_bboxes)
        ]

        if not filtered_words:
            return [], [], start_index

        lines = self._group_words_into_lines(filtered_words)
        if not lines:
            return [], [], start_index

        line_heights = [
            max(1.0, float(line["bottom"]) - float(line["top"]))
            for line in lines
        ]
        typical_line_height = median(line_heights) if line_heights else 10.0
        paragraph_gap_threshold = max(6.0, float(typical_line_height) * 1.6)

        grouped_paragraphs: list[list[dict[str, Any]]] = []
        current_group: list[dict[str, Any]] = []
        prev_bottom: float | None = None

        for line in lines:
            if prev_bottom is not None and (float(line["top"]) - prev_bottom) > paragraph_gap_threshold:
                if current_group:
                    grouped_paragraphs.append(current_group)
                current_group = [line]
            else:
                current_group.append(line)
            prev_bottom = float(line["bottom"])

        if current_group:
            grouped_paragraphs.append(current_group)

        paragraphs: list[dict[str, Any]] = []
        ordered_blocks: list[dict[str, Any]] = []
        paragraph_index = start_index

        for group in grouped_paragraphs:
            paragraph_lines = [line["text"] for line in group if line["text"]]
            paragraph_text = "\n".join(paragraph_lines).strip()
            if not paragraph_text:
                continue

            all_words = [word for line in group for word in line["words"]]
            dominant_font_name, dominant_font_size = self._dominant_font(
                all_words)
            is_bullet, is_numbered, cleaned_text, numbering_format = self._detect_list_type(
                paragraph_text
            )

            # Remove common page-number artifacts (e.g., isolated "2").
            if re.fullmatch(r"\d{1,3}", cleaned_text):
                continue
            if len(cleaned_text) <= 2 and cleaned_text.isalnum():
                continue

            top = min(float(line["top"]) for line in group)
            bottom = max(float(line["bottom"]) for line in group)
            x0 = min(float(line["x0"]) for line in group)
            x1 = max(float(line["x1"]) for line in group)

            paragraphs.append(
                {
                    "index": paragraph_index,
                    "text": cleaned_text,
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
                            "text": cleaned_text,
                            "bold": None,
                            "italic": None,
                            "underline": None,
                            "strike": False,
                            "double_strike": False,
                            "subscript": False,
                            "superscript": False,
                            "rtl": False,
                            "all_caps": False,
                            "small_caps": False,
                            "font_name": dominant_font_name,
                            "font_size_pt": dominant_font_size,
                            "color_rgb": None,
                            "highlight_color": None,
                            "raw_xml": None,
                            "hyperlink_url": None,
                            "embedded_media": [],
                        }
                    ],
                    "source": {
                        "page_number": page_number,
                        "bbox": {
                            "x0": x0,
                            "top": top,
                            "x1": x1,
                            "bottom": bottom,
                        },
                    },
                }
            )
            ordered_blocks.append(
                {
                    "type": "paragraph",
                    "index": paragraph_index,
                    "page_number": page_number,
                    "top": top,
                }
            )
            paragraph_index += 1

        return paragraphs, ordered_blocks, paragraph_index

    def _group_words_into_lines(self, words: list[dict[str, Any]]) -> list[dict[str, Any]]:
        sorted_words = sorted(words, key=lambda w: (
            float(w.get("top", 0.0)), float(w.get("x0", 0.0))))
        if not sorted_words:
            return []

        lines: list[list[dict[str, Any]]] = []
        current_line: list[dict[str, Any]] = []
        current_top: float | None = None
        y_tolerance = 2.5

        for word in sorted_words:
            word_top = float(word.get("top", 0.0))
            if current_top is None or abs(word_top - current_top) <= y_tolerance:
                current_line.append(word)
                if current_top is None:
                    current_top = word_top
                else:
                    current_top = (
                        current_top * (len(current_line) - 1) + word_top) / len(current_line)
            else:
                lines.append(current_line)
                current_line = [word]
                current_top = word_top

        if current_line:
            lines.append(current_line)

        line_items: list[dict[str, Any]] = []
        for line_words in lines:
            ordered_line_words = sorted(
                line_words, key=lambda w: float(w.get("x0", 0.0)))
            text = self._join_words_with_spacing(ordered_line_words)
            x0 = min(float(w.get("x0", 0.0)) for w in ordered_line_words)
            x1 = max(float(w.get("x1", 0.0)) for w in ordered_line_words)
            top = min(float(w.get("top", 0.0)) for w in ordered_line_words)
            bottom = max(float(w.get("bottom", 0.0))
                         for w in ordered_line_words)
            line_items.append(
                {
                    "text": text,
                    "x0": x0,
                    "x1": x1,
                    "top": top,
                    "bottom": bottom,
                    "words": ordered_line_words,
                }
            )

        return line_items

    def _join_words_with_spacing(self, words: list[dict[str, Any]]) -> str:
        parts: list[str] = []
        prev_x1: float | None = None
        for word in words:
            text = self._clean_text(word.get("text"))
            if not text:
                continue

            x0 = float(word.get("x0", 0.0))
            if prev_x1 is not None and (x0 - prev_x1) > 3.0:
                parts.append(" ")
            elif parts:
                parts.append(" ")

            parts.append(text)
            prev_x1 = float(word.get("x1", 0.0))

        return "".join(parts).strip()

    def _word_in_any_bbox(
        self,
        word: dict[str, Any],
        bboxes: list[tuple[float, float, float, float]],
    ) -> bool:
        cx = (float(word.get("x0", 0.0)) + float(word.get("x1", 0.0))) / 2.0
        cy = (float(word.get("top", 0.0)) +
              float(word.get("bottom", 0.0))) / 2.0
        for x0, top, x1, bottom in bboxes:
            if (x0 - 1.0) <= cx <= (x1 + 1.0) and (top - 1.0) <= cy <= (bottom + 1.0):
                return True
        return False

    def _dominant_font(self, words: list[dict[str, Any]]) -> tuple[str | None, float | None]:
        fonts = [str(w.get("fontname")) for w in words if w.get("fontname")]
        sizes = [float(w.get("size"))
                 for w in words if isinstance(w.get("size"), (int, float))]
        font = Counter(fonts).most_common(1)[0][0] if fonts else None
        size = round(median(sizes), 2) if sizes else None
        return font, size

    def _detect_list_type(self, text: str) -> tuple[bool, bool, str, str | None]:
        raw = text.lstrip()

        bullet_match = re.match(
            r"^([\u2022\u25CF\u25E6\u25AA\u25AB\-*])+\s+(.*)$", raw)
        if bullet_match:
            return True, False, bullet_match.group(2).strip(), "bullet"

        num_match = re.match(r"^(\(?\d+[\.)]|[A-Za-z][\.)])\s+(.*)$", raw)
        if num_match:
            marker = num_match.group(1)
            body = num_match.group(2).strip()
            return False, True, body, marker

        return False, False, text, None

    def _clean_text(self, value: Any) -> str:
        if value is None:
            return ""
        text = str(value)
        # Collapse repeated inner whitespace but preserve explicit newlines.
        parts = [" ".join(line.split()) for line in text.splitlines()]
        return "\n".join(part for part in parts if part).strip()
