import json
import re
from typing import Any

from bs4 import BeautifulSoup, NavigableString, Tag


class HtmlJsonExtractionAdapter:
    _BLOCK_TAGS = {
        "address", "article", "aside", "blockquote", "details", "dialog",
        "div", "dl", "fieldset", "figcaption", "figure", "footer", "form",
        "h1", "h2", "h3", "h4", "h5", "h6", "header", "hr", "li", "main",
        "nav", "ol", "p", "pre", "section", "table", "ul",
    }
    # CSS direction values considered right-to-left
    _RTL_VALUES = {"rtl"}

    def __init__(self) -> None:
        pass

    def run(self, file_bytes: bytes, output_basename: str) -> tuple[dict[str, Any], str]:
        html = file_bytes.decode("utf-8-sig", errors="replace")
        soup = BeautifulSoup(html, "lxml")
        root = soup.body or soup

        paragraphs: list[dict[str, Any]] = []
        tables: list[dict[str, Any]] = []
        media: list[dict[str, Any]] = []
        document_order: list[dict[str, Any]] = []
        seen_tables: set[int] = set()

        paragraph_index = 0
        table_index = 0
        media_index = 0

        # ── helpers ────────────────────────────────────────────────────────────

        def add_media(image_elem: Tag) -> None:
            nonlocal media_index
            src = (image_elem.get("src") or "").strip()
            if not src:
                return
            media.append({
                "relationship_id": f"html_img_{media_index}",
                "content_type": None,
                "file_name": src.split("/")[-1],
                "local_file_path": src,
                "local_url": src,
                "width_emu": int(image_elem.get("width")) if image_elem.get("width", "").isdigit() else None,
                "height_emu": int(image_elem.get("height")) if image_elem.get("height", "").isdigit() else None,
                "alt_text": (image_elem.get("alt") or "").strip() or None,
            })
            document_order.append({"type": "media", "index": media_index})
            media_index += 1

        def add_paragraph_from_element(
            elem: Tag,
            *,
            heading_level: int | None = None,
            is_bullet: bool = False,
            is_numbered: bool = False,
            numbering_format: str | None = None,
            list_level: int = 0,
            list_start: int | None = None,
            direction: str | None = None,
        ) -> None:
            nonlocal paragraph_index

            runs = self._extract_runs(elem)
            text = "".join((run.get("text") or "") for run in runs).strip()
            if not text:
                return

            style = f"Heading {heading_level}" if heading_level else None

            # inherit rtl from inline style / dir attribute / Arabic content
            elem_dir = (elem.get("dir") or "").lower()
            inline_style = elem.get("style") or ""
            m_dir = re.search(r"\bdirection\s*:\s*(\w+)", inline_style, re.I)
            if not direction:
                if elem_dir in self._RTL_VALUES:
                    direction = "rtl"
                elif m_dir and m_dir.group(1).lower() in self._RTL_VALUES:
                    direction = "rtl"
                elif re.search(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", text):
                    # text contains Arabic/RTL unicode — mark as rtl
                    direction = "rtl"

            paragraphs.append({
                "index": paragraph_index,
                "text": text,
                "style": style,
                "is_bullet": is_bullet,
                "is_numbered": is_numbered,
                "list_info": {
                    "kind": "bullet" if is_bullet else ("numbered" if is_numbered else None),
                    "numbering_format": numbering_format,
                    "level": list_level,
                    "start": list_start,
                } if (is_bullet or is_numbered) else None,
                "numbering_format": numbering_format,
                "list_level": list_level if (is_bullet or is_numbered) else None,
                "alignment": None,
                "direction": direction,
                "runs": runs if runs else [self._default_run(text)],
                "source": {"format": "html"},
            })
            document_order.append(
                {"type": "paragraph", "index": paragraph_index})
            paragraph_index += 1

        def add_table(table_elem: Tag) -> None:
            nonlocal table_index

            table_id = id(table_elem)
            if table_id in seen_tables:
                return
            seen_tables.add(table_id)

            table_rows: list[dict[str, Any]] = []
            max_cols = 0
            row_index = 0

            # only rows that are DIRECT children of this table (not of nested tables)
            direct_rows = [
                tr for tr in table_elem.find_all("tr")
                if tr.find_parent("table") is table_elem
            ]

            for tr in direct_rows:
                cell_tags = [
                    cell for cell in tr.find_all(["th", "td"])
                    if cell.find_parent("table") is table_elem
                ]
                if not cell_tags:
                    continue

                cells = []
                for col_index, cell in enumerate(cell_tags):
                    cell_text = self._normalize_text(
                        cell.get_text(" ", strip=True))
                    cell_runs = self._extract_runs(cell)
                    # capture colspan / rowspan
                    try:
                        cs = int(cell.get("colspan", 1) or 1)
                    except (ValueError, TypeError):
                        cs = 1
                    try:
                        rs = int(cell.get("rowspan", 1) or 1)
                    except (ValueError, TypeError):
                        rs = 1
                    is_header = cell.name == "th"

                    # Collect nested table elements; recurse AFTER outer table saved
                    nested_table_elems = [
                        nt for nt in cell.find_all("table")
                        if nt.find_parent("table") is table_elem
                    ]

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
                            "runs": cell_runs if cell_runs else [self._default_run(cell_text)],
                        }],
                        "tables": [],
                        "cell_index": col_index,
                        "is_header": is_header,
                        "colspan": cs,
                        "rowspan": rs,
                        # temp key; patched below after outer table is saved
                        "_nested_elems": nested_table_elems,
                        "nested_table_indices": [],
                    })

                max_cols = max(max_cols, len(cells))
                table_rows.append({"row_index": row_index, "cells": cells})
                row_index += 1

            if not table_rows:
                return

            # Save outer table FIRST so its index is stable before recursing
            current_index = table_index
            tables.append({
                "index": current_index,
                "row_count": len(table_rows),
                "column_count": max_cols,
                "style": None,
                "rows": table_rows,
                "source": {"format": "html"},
            })
            document_order.append({"type": "table", "index": current_index})
            table_index += 1

            # Recurse into nested tables and record their assigned indices
            for row in table_rows:
                for cell in row["cells"]:
                    nested_elems = cell.pop("_nested_elems", [])
                    for nt in nested_elems:
                        pre_idx = table_index
                        add_table(nt)
                        if table_index > pre_idx:
                            cell["nested_table_indices"].append(pre_idx)

        def walk_list(list_elem: Tag, *, list_level: int = 0) -> None:
            """Recursively process <ul>/<ol> preserving nesting depth."""
            is_bullet = list_elem.name.lower() == "ul"
            try:
                start = int(list_elem.get("start", 1)
                            or 1) if not is_bullet else 1
            except (ValueError, TypeError):
                start = 1
            current_number = start

            for li in list_elem.find_all("li", recursive=False):
                numbering_format = "bullet" if is_bullet else f"{current_number}."

                # Text of the li excluding any child ul/ol/table — we emit
                # those separately after the li item itself.
                add_paragraph_from_element(
                    li,
                    is_bullet=is_bullet,
                    is_numbered=not is_bullet,
                    numbering_format=numbering_format,
                    list_level=list_level,
                    list_start=start if not is_bullet else None,
                )

                # Recurse into any nested lists
                for child in li.children:
                    if isinstance(child, Tag):
                        cname = child.name.lower()
                        if cname in {"ul", "ol"}:
                            walk_list(child, list_level=list_level + 1)
                        elif cname == "table":
                            add_table(child)

                current_number += 1

        def walk(parent: Tag) -> None:
            for child in parent.children:
                if isinstance(child, NavigableString):
                    txt = self._normalize_text(str(child))
                    if txt:
                        pseudo = BeautifulSoup(
                            f"<p>{str(child)}</p>", "lxml").p
                        if pseudo is not None:
                            add_paragraph_from_element(pseudo)
                    continue

                if not isinstance(child, Tag):
                    continue

                name = child.name.lower()

                if name in {"script", "style", "noscript", "meta", "link", "br", "hr"}:
                    if name == "hr":
                        # emit a horizontal-rule paragraph as a divider
                        paragraphs.append({
                            "index": paragraph_index,
                            "text": "---",
                            "style": "HorizontalRule",
                            "is_bullet": False,
                            "is_numbered": False,
                            "list_info": None,
                            "numbering_format": None,
                            "list_level": None,
                            "alignment": "CENTER",
                            "direction": None,
                            "runs": [self._default_run("---")],
                            "source": {"format": "html"},
                        })
                        document_order.append(
                            {"type": "paragraph", "index": paragraph_index})
                        # paragraph_index incremented via outer scope — use
                        # a closure trick
                        _inc_para()
                    continue

                if name == "img":
                    add_media(child)
                    continue

                if name == "table":
                    add_table(child)
                    continue

                if name in {"ul", "ol"}:
                    walk_list(child, list_level=0)
                    continue

                if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
                    add_paragraph_from_element(
                        child, heading_level=int(name[1]))
                    continue

                if name == "p":
                    # A <p> might illegally contain block children (ul/ol/table).
                    # Hoist those out; emit the text portion as a paragraph.
                    block_children = [
                        c for c in child.children
                        if isinstance(c, Tag) and c.name.lower() in {"ul", "ol", "table", "img"}
                    ]
                    if block_children:
                        # emit text part first
                        text_only_runs = self._extract_runs_skip_block_children(
                            child)
                        text_only = "".join((r.get("text") or "")
                                            for r in text_only_runs).strip()
                        if text_only:
                            nonlocal_add_para_direct(child, text_only_runs)
                        for bc in block_children:
                            bcname = bc.name.lower()
                            if bcname in {"ul", "ol"}:
                                walk_list(bc, list_level=0)
                            elif bcname == "table":
                                add_table(bc)
                            elif bcname == "img":
                                add_media(bc)
                    else:
                        add_paragraph_from_element(child)
                    continue

                if name in {"blockquote", "pre"}:
                    add_paragraph_from_element(child)
                    continue

                if name in {"div", "section", "article", "main", "header", "footer", "aside", "nav", "figure", "figcaption"}:
                    # check for RTL via dir attr, inline style, or Arabic Unicode content
                    elem_dir = (child.get("dir") or "").lower()
                    inline_style = child.get("style") or ""
                    m_dir = re.search(
                        r"\bdirection\s*:\s*(\w+)", inline_style, re.I)
                    elem_text_rtl = child.get_text()
                    has_arabic = bool(re.search(
                        r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", elem_text_rtl))
                    is_rtl = (
                        elem_dir in self._RTL_VALUES
                        or (m_dir and m_dir.group(1).lower() in self._RTL_VALUES)
                        or has_arabic
                    )

                    has_block_child = any(
                        isinstance(grand, Tag) and grand.name and grand.name.lower(
                        ) in self._BLOCK_TAGS
                        for grand in child.children
                    )
                    if has_block_child:
                        walk(child)
                    else:
                        add_paragraph_from_element(
                            child, direction="rtl" if is_rtl else None)
                    continue

                # anything else with text
                txt = self._normalize_text(child.get_text(" ", strip=True))
                if txt:
                    add_paragraph_from_element(child)

        # ── paragraph_index closure helpers ────────────────────────────────────
        def _inc_para() -> None:
            nonlocal paragraph_index
            paragraph_index += 1

        def nonlocal_add_para_direct(elem: Tag, runs: list[dict]) -> None:
            nonlocal paragraph_index
            text = "".join((r.get("text") or "") for r in runs).strip()
            if not text:
                return
            paragraphs.append({
                "index": paragraph_index,
                "text": text,
                "style": None,
                "is_bullet": False,
                "is_numbered": False,
                "list_info": None,
                "numbering_format": None,
                "list_level": None,
                "alignment": None,
                "direction": None,
                "runs": runs if runs else [self._default_run(text)],
                "source": {"format": "html"},
            })
            document_order.append(
                {"type": "paragraph", "index": paragraph_index})
            paragraph_index += 1

        walk(root)

        extracted: dict[str, Any] = {
            "metadata": {
                "source_type": "html",
                "extraction_mode": "html",
                "title": (soup.title.string.strip() if soup.title and soup.title.string else None),
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

        return extracted, f"virtual://extracted/{output_basename}.html.json"

    # ── run extraction ────────────────────────────────────────────────────────

    def _extract_runs(self, element: Tag) -> list[dict[str, Any]]:
        return self._collect_runs(element, skip_block_children=False)

    def _extract_runs_skip_block_children(self, element: Tag) -> list[dict[str, Any]]:
        return self._collect_runs(element, skip_block_children=True)

    def _collect_runs(self, element: Tag, *, skip_block_children: bool) -> list[dict[str, Any]]:
        runs: list[dict[str, Any]] = []
        _BLOCK_SKIP = {"ul", "ol", "table", "img"}

        def push_text(text: str, style: dict[str, Any]) -> None:
            normalized = self._normalize_text(text)
            if not normalized:
                return
            run = {
                "index": len(runs),
                "text": normalized,
                "bold": style.get("bold"),
                "italic": style.get("italic"),
                "underline": style.get("underline"),
                "strikethrough": style.get("strikethrough"),
                "code": style.get("code"),
                "color_rgb": style.get("color_rgb"),
                "font_name": style.get("font_name"),
                "font_size_pt": None,
                "highlight_color": None,
                "hyperlink_url": style.get("hyperlink_url"),
                "embedded_media": [],
            }
            if runs and self._same_style(runs[-1], run):
                runs[-1]["text"] = f"{runs[-1]['text']} {run['text']}".strip()
            else:
                run["index"] = len(runs)
                runs.append(run)

        def rec(node: Any, style: dict[str, Any]) -> None:
            if isinstance(node, NavigableString):
                push_text(str(node), style)
                return
            if not isinstance(node, Tag):
                return

            name = (node.name or "").lower()

            if name in {"script", "style", "noscript"}:
                return
            if skip_block_children and name in _BLOCK_SKIP:
                return
            if name in {"table", "ul", "ol"}:
                return
            if name == "br":
                push_text("\n", style)
                return

            next_style = dict(style)

            # semantic formatting
            if name in {"b", "strong"}:
                next_style["bold"] = True
            if name in {"i", "em"}:
                next_style["italic"] = True
            if name in {"u", "ins"}:
                next_style["underline"] = True
            if name in {"s", "del", "strike"}:
                next_style["strikethrough"] = True
            if name in {"code", "kbd", "samp", "tt"}:
                next_style["code"] = True
            if name == "a":
                href = (node.get("href") or "").strip()
                if href:
                    next_style["hyperlink_url"] = href

            # inline style colour / font extraction
            inline = node.get("style") or ""
            if inline:
                m_color = re.search(r"\bcolor\s*:\s*([^;]+)", inline, re.I)
                if m_color:
                    raw = m_color.group(1).strip()
                    next_style["color_rgb"] = self._css_color_to_rgb(raw)
                m_font = re.search(
                    r"\bfont-family\s*:\s*([^;]+)", inline, re.I)
                if m_font:
                    font_raw = m_font.group(1).strip().strip(
                        "'\"").split(",")[0].strip().strip("'\"")
                    if font_raw:
                        next_style["font_name"] = font_raw

            for child in node.children:
                rec(child, next_style)

        rec(element, {
            "bold": None, "italic": None, "underline": None,
            "strikethrough": None, "code": None,
            "color_rgb": None, "font_name": None,
            "hyperlink_url": None,
        })
        for idx, run in enumerate(runs):
            run["index"] = idx
        return runs

    def _same_style(self, a: dict[str, Any], b: dict[str, Any]) -> bool:
        keys = ("bold", "italic", "underline", "strikethrough", "code",
                "color_rgb", "font_name", "hyperlink_url")
        return all(a.get(k) == b.get(k) for k in keys)

    @staticmethod
    def _css_color_to_rgb(raw: str) -> str | None:
        """Best-effort convert CSS color value to a display string."""
        raw = raw.strip()
        if raw.startswith("#"):
            return raw
        m_rgb = re.match(
            r"rgb\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)", raw, re.I)
        if m_rgb:
            r, g, b = int(m_rgb.group(1)), int(
                m_rgb.group(2)), int(m_rgb.group(3))
            return f"#{r:02x}{g:02x}{b:02x}"
        # named colors — keep as-is for display
        if re.match(r"^[a-zA-Z]+$", raw):
            return raw
        return None

    def _default_run(self, text: str) -> dict[str, Any]:
        return {
            "index": 0,
            "text": text,
            "bold": None,
            "italic": None,
            "underline": None,
            "strikethrough": None,
            "code": None,
            "font_name": None,
            "font_size_pt": None,
            "color_rgb": None,
            "highlight_color": None,
            "hyperlink_url": None,
            "embedded_media": [],
        }

    def _normalize_text(self, value: str) -> str:
        if not value:
            return ""
        value = value.replace("\r\n", "\n").replace("\r", "\n")
        lines = []
        for line in value.split("\n"):
            cleaned = re.sub(r"\s+", " ", line).strip()
            if cleaned:
                lines.append(cleaned)
        return "\n".join(lines)
