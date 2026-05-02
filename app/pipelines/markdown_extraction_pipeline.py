"""Markdown extraction pipeline for content-extractor service."""

import re
from typing import Any


class MarkdownExtractionPipeline:
    """Extract Markdown content to JSON format."""

    def run(self, file_bytes: bytes, include_media: bool = True) -> dict[str, Any]:
        """Extract Markdown and return JSON data."""
        try:
            text = file_bytes.decode("utf-8-sig", errors="replace")
        except Exception as e:
            raise ValueError(f"Failed to decode Markdown: {str(e)}") from e

        lines = text.splitlines()
        paragraphs: list[dict[str, Any]] = []
        tables: list[dict[str, Any]] = []
        media: list[dict[str, Any]] = []
        document_order: list[dict[str, Any]] = []

        paragraph_index = 0
        table_index = 0
        line_index = 0

        while line_index < len(lines):
            line = lines[line_index]
            stripped = line.strip()

            if not stripped:
                line_index += 1
                continue

            if self._is_table_start(lines, line_index):
                table_lines, line_index = self._collect_table_lines(
                    lines, line_index)
                table_entry = self._build_table_entry(table_lines, table_index)
                if table_entry:
                    tables.append(table_entry)
                    document_order.append(
                        {"type": "table", "index": table_index})
                    table_index += 1
                continue

            # Detect fenced code blocks (``` or ~~~) and collect as a unit
            if re.match(r"^(`{3,}|~{3,})", stripped):
                block_lines, line_index = self._collect_code_fence_block(
                    lines, line_index)
            else:
                block_lines, line_index = self._collect_paragraph_block(
                    lines, line_index, line)

            paragraph = self._build_paragraph(block_lines, paragraph_index)
            if include_media:
                media.extend(self._extract_inline_media(
                    block_lines, paragraph_index))
            paragraphs.append(paragraph)
            document_order.append(
                {"type": "paragraph", "index": paragraph_index})
            paragraph_index += 1

        return {
            "metadata": {
                "source_type": "markdown",
                "extraction_mode": "markdown",
            },
            "document_order": document_order,
            "document_defaults": None,
            "styles": [],
            "numbering": [],
            "sections": [],
            "paragraphs": paragraphs,
            "tables": tables,
            "media": media,
        }

    # ── table helpers ─────────────────────────────────────────────────────────

    def _is_table_start(self, lines: list[str], index: int) -> bool:
        if index + 1 >= len(lines):
            return False
        return (
            self._looks_like_table_row(lines[index])
            and self._looks_like_table_separator(lines[index + 1])
        )

    def _looks_like_table_row(self, line: str) -> bool:
        stripped = line.strip()
        return "|" in stripped and len([p for p in stripped.split("|") if p.strip()]) >= 2

    def _looks_like_table_separator(self, line: str) -> bool:
        stripped = line.strip().strip("|")
        if not stripped:
            return False
        parts = [p.strip() for p in stripped.split("|")]
        return all(bool(re.fullmatch(r":?-{3,}:?", p or "")) for p in parts if p)

    def _collect_table_lines(
        self, lines: list[str], line_index: int
    ) -> tuple[list[str], int]:
        """Collect contiguous table-row lines starting at line_index."""
        table_lines: list[str] = []
        while line_index < len(lines) and self._looks_like_table_row(lines[line_index]):
            table_lines.append(lines[line_index])
            line_index += 1
        return table_lines, line_index

    def _parse_table_lines(self, lines: list[str]) -> list[list[str]]:
        if len(lines) < 2:
            return []
        rows: list[list[str]] = []
        for index, line in enumerate(lines):
            if index == 1 and self._looks_like_table_separator(line):
                continue
            stripped = line.strip().strip("|")
            rows.append([cell.strip() for cell in stripped.split("|")])
        return rows

    def _build_rows_payload(
        self, table_rows: list[list[str]]
    ) -> tuple[list[dict[str, Any]], int]:
        """Build row/cell payload from parsed table rows."""
        rows_payload: list[dict[str, Any]] = []
        max_cols = max((len(row) for row in table_rows), default=0)
        for row_idx, row in enumerate(table_rows):
            cells = []
            for col_idx, cell_text in enumerate(row):
                cells.append({
                    "text": cell_text,
                    "paragraphs": [{
                        "index": 0,
                        "text": cell_text,
                        "style": None,
                        "is_bullet": False,
                        "is_numbered": False,
                        "list_info": None,
                        "numbering_format": None,
                        "alignment": None,
                        "runs": [{
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
                        }],
                    }],
                    "tables": [],
                    "cell_index": col_idx,
                })
            rows_payload.append({"row_index": row_idx, "cells": cells})
        return rows_payload, max_cols

    def _build_table_entry(
        self, table_lines: list[str], table_index: int
    ) -> dict[str, Any] | None:
        """Build a table dict from collected lines, or return None if empty."""
        table_rows = self._parse_table_lines(table_lines)
        if not table_rows:
            return None
        rows_payload, max_cols = self._build_rows_payload(table_rows)
        return {
            "index": table_index,
            "row_count": len(rows_payload),
            "column_count": max_cols,
            "style": None,
            "rows": rows_payload,
            "source": {"format": "markdown"},
        }

    # ── block collection helpers ──────────────────────────────────────────────

    def _collect_code_fence_block(
        self, lines: list[str], line_index: int
    ) -> tuple[list[str], int]:
        """Collect a fenced code block from opening ``` / ~~~ to matching close."""
        opening = lines[line_index]
        fence_match = re.match(r"^(`{3,}|~{3,})", opening.strip())
        fence_char = fence_match.group(1) if fence_match else "```"
        block_lines = [opening]
        line_index += 1
        while line_index < len(lines):
            current = lines[line_index]
            block_lines.append(current)
            line_index += 1
            if re.match(r"^" + re.escape(fence_char) + r"\s*$", current.strip()):
                break
        return block_lines, line_index

    def _collect_paragraph_block(
        self, lines: list[str], line_index: int, first_line: str
    ) -> tuple[list[str], int]:
        """Collect contiguous non-structural lines into a paragraph block."""
        block_lines = [first_line]
        line_index += 1
        while line_index < len(lines):
            next_line = lines[line_index]
            next_stripped = next_line.strip()
            if not next_stripped:
                break
            if self._is_structural_line(next_line) or self._is_table_start(lines, line_index):
                break
            block_lines.append(next_line)
            line_index += 1
        return block_lines, line_index

    def _is_structural_line(self, line: str) -> bool:
        stripped = line.strip()
        if not stripped:
            return False
        return bool(
            re.match(r"^\s{0,3}(#{1,6})\s+", line)
            or re.match(r"^\s*[-*+]\s+", line)
            or re.match(r"^\s*\d+[.)]\s+", line)
        )

    # ── paragraph builder ─────────────────────────────────────────────────────

    def _build_paragraph(
        self, block_lines: list[str], paragraph_index: int
    ) -> dict[str, Any]:
        heading_level = None
        style = None
        is_bullet = False
        is_numbered = False
        numbering_format = None
        list_indent = 0
        code_fence_language: str | None = None

        first_stripped = block_lines[0].strip() if block_lines else ""
        fence_open_match = re.match(r"^(`{3,}|~{3,})(\S*)", first_stripped)
        if fence_open_match:
            fence_marker = fence_open_match.group(1)
            code_fence_language = fence_open_match.group(2) or ""
            style = "CodeBlock"
            body_lines: list[str] = []
            for bline in block_lines[1:]:
                if re.match(r"^" + re.escape(fence_marker) + r"\s*$", bline.strip()):
                    break
                body_lines.append(bline)
            raw = "\n".join(body_lines)
            runs = [{
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
                "code": True,
            }] if raw.strip() else []
        else:
            first_line_raw = block_lines[0] if block_lines else ""
            raw = "\n".join(block_lines).strip()

            heading_match = re.match(r"^\s{0,3}(#{1,6})\s+(.*)$", raw)
            if heading_match:
                heading_level = len(heading_match.group(1))
                raw = heading_match.group(2).strip()
                style = f"Heading {heading_level}"
            else:
                bullet_match = re.match(r"^(\s*)[-*+]\s+(.*)$", first_line_raw)
                number_match = re.match(
                    r"^(\s*)(\d+[.)])\s+(.*)$", first_line_raw)
                if bullet_match:
                    is_bullet = True
                    numbering_format = "bullet"
                    list_indent = len(bullet_match.group(1))
                    m = re.match(r"^\s*[-*+]\s+(.*)", raw, re.DOTALL)
                    raw = m.group(1).strip() if m else raw
                elif number_match:
                    is_numbered = True
                    numbering_format = number_match.group(2)
                    list_indent = len(number_match.group(1))
                    m = re.match(r"^\s*\d+[.)]\s+(.*)", raw, re.DOTALL)
                    raw = m.group(1).strip() if m else raw
            runs = self._parse_inline_runs(raw)

        list_kind: str | None = (
            "bullet" if is_bullet else ("numbered" if is_numbered else None)
        )

        # Compute indent level (2-space units) and ordered-list start number.
        indent_level = list_indent // 2 if list_indent else 0
        list_start: int | None = None
        if is_numbered and numbering_format:
            try:
                digits = re.match(r"^(\d+)", numbering_format)
                if digits:
                    list_start = int(digits.group(1))
            except (ValueError, TypeError):
                pass

        return {
            "index": paragraph_index,
            "text": raw,
            "style": style,
            "code_fence_language": code_fence_language,
            "is_bullet": is_bullet,
            "is_numbered": is_numbered,
            "list_info": {
                "kind": list_kind,
                "numbering_format": numbering_format,
                "level": indent_level,
                "indent_level": indent_level,
                "start": list_start,
            } if (is_bullet or is_numbered) else None,
            "numbering_format": numbering_format,
            "list_level": indent_level if (is_bullet or is_numbered) else None,
            "alignment": None,
            "runs": runs,
            "source": {"format": "markdown"},
        }

    # ── inline run parser ─────────────────────────────────────────────────────

    def _classify_match(
        self, m: re.Match
    ) -> tuple[str | None, bool, bool, bool, str | None]:
        """Return (run_text, bold, italic, is_code, url) for a regex match."""
        bold = italic = is_code = False
        url = None
        run_text = None
        if m.group("bi"):
            run_text, bold, italic = m.group("bi"), True, True
        elif m.group("b"):
            run_text, bold = m.group("b"), True
        elif m.group("b2"):
            run_text, bold = m.group("b2"), True
        elif m.group("esc"):
            run_text = "\\" + m.group("esc")
        elif m.group("i"):
            run_text, italic = m.group("i"), True
        elif m.group("code"):
            run_text, is_code = m.group("code"), True
        elif m.group("link_text"):
            run_text = m.group("link_text")
            url = m.group("link_url")
        elif m.group("plain"):
            run_text = m.group("plain")
        elif m.group("any"):
            run_text = m.group("any")
        return run_text, bold, italic, is_code, url

    def _parse_inline_runs(self, text: str) -> list[dict[str, Any]]:
        """Parse inline Markdown into styled runs (bold/italic/code/links)."""
        runs: list[dict[str, Any]] = []
        pattern = re.compile(
            r"(\*\*\*(?P<bi>[^*]+?)\*\*\*"
            r"|\*\*(?P<b>[^*]+?)\*\*"
            r"|__(?P<b2>[^_]+?)__"
            r"|\\(?P<esc>.)"
            r"|(\*|_)(?P<i>[^*_]+?)(\*|_)"
            r"|`(?P<code>[^`]+?)`"
            r"|\[(?P<link_text>[^\]]+)\]\((?P<link_url>[^)]+)\)"
            r"|(?P<plain>[^*_`\\]+)"
            r"|(?P<any>.)"
            r")"
        )
        for m in pattern.finditer(text):
            run_text, bold, italic, is_code, url = self._classify_match(m)
            if not run_text:
                continue
            run: dict[str, Any] = {
                "index": len(runs),
                "text": run_text,
                "bold": bold or None,
                "italic": italic or None,
                "underline": None,
                "font_name": None,
                "font_size_pt": None,
                "color_rgb": None,
                "highlight_color": None,
                "hyperlink_url": url,
                "embedded_media": [],
            }
            if is_code:
                run["code"] = True
            runs.append(run)

        if not runs:
            runs.append({
                "index": 0,
                "text": text,
                "bold": None,
                "italic": None,
                "underline": None,
                "font_name": None,
                "font_size_pt": None,
                "color_rgb": None,
                "highlight_color": None,
                "hyperlink_url": None,
                "embedded_media": [],
            })

        return runs

    # ── media extractor ───────────────────────────────────────────────────────

    def _extract_inline_media(
        self, block_lines: list[str], paragraph_index: int
    ) -> list[dict[str, Any]]:
        """Extract ![alt](src) image references from block lines."""
        media: list[dict[str, Any]] = []
        pattern = re.compile(r"!\[(?P<alt>[^\]]*)\]\((?P<target>[^)\s]+)\)")
        block_text = "\n".join(block_lines)
        for image_index, match in enumerate(pattern.finditer(block_text)):
            target = match.group("target")
            media.append({
                "relationship_id": f"md_p_{paragraph_index}_img_{image_index}",
                "content_type": None,
                "file_name": target.rsplit("/", 1)[-1],
                "local_file_path": target,
                "local_url": target,
                "width_emu": None,
                "height_emu": None,
                "alt_text": match.group("alt") or None,
            })
        return media
