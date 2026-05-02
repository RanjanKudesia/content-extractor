"""HtmlExtractionPipeline — main extraction class."""

import logging
from typing import Any

from bs4 import BeautifulSoup, Doctype, NavigableString, Tag

from app.pipelines.html._rtl import _ARABIC_RE, _DIRECTION_RE, _is_element_rtl
from app.pipelines.html._run_collector import _extract_runs, _extract_runs_skip_blocks
from app.pipelines.html._state import _ExtractionState
from app.pipelines.html._tag_style import (
    _default_run,
    _normalize_text,
)
from app.pipelines.html._css_helpers import build_css_cascade


def _build_list_info(
    is_bullet: bool,
    is_numbered: bool,
    numbering_format: str | None,
    list_level: int,
    list_start: int | None,
) -> dict | None:
    """Build list_info dict for a paragraph, or None if not a list item."""
    if not (is_bullet or is_numbered):
        return None
    if is_bullet:
        kind = "bullet"
    elif is_numbered:
        kind = "numbered"
    else:
        kind = None
    return {
        "kind": kind,
        "numbering_format": numbering_format,
        "level": list_level,
        "start": list_start,
    }


def _get_cell_direction(cell: Tag) -> str | None:
    """Return 'rtl' if the table cell has RTL direction, else None."""
    if (cell.get("dir") or "").lower() == "rtl":
        return "rtl"
    style_val = cell.get("style") or ""
    m = _DIRECTION_RE.search(style_val)
    if m and m.group(1).lower() == "rtl":
        return "rtl"
    if _is_element_rtl(cell):
        return "rtl"
    return None


class HtmlExtractionPipeline:
    """Extract HTML content to JSON format."""

    _BLOCK_TAGS: frozenset[str] = frozenset({
        "address", "article", "aside", "blockquote", "details", "dialog",
        "div", "dl", "fieldset", "figcaption", "figure", "footer", "form",
        "h1", "h2", "h3", "h4", "h5", "h6", "header", "hr", "li", "main",
        "nav", "ol", "p", "pre", "section", "table", "ul",
    })

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, file_bytes: bytes, include_media: bool = True) -> dict[str, Any]:
        """Extract HTML and return JSON data."""
        html = file_bytes.decode("utf-8-sig", errors="replace")
        soup = BeautifulSoup(html, "lxml")
        root = soup.body or soup

        state = _ExtractionState(include_media=include_media)
        state.css_cascade = build_css_cascade(soup)

        title = (
            soup.title.string.strip()
            if soup.title and soup.title.string
            else None
        )

        self._walk(root, state)

        # Collect doctype string.
        doctype_str: str | None = None
        for item in soup.contents:
            if isinstance(item, Doctype):
                doctype_str = f"<!doctype {item}>"
                break

        # Collect HTML-level attributes.
        html_tag = soup.find("html")
        html_attributes: dict = dict(html_tag.attrs) if html_tag else {}

        body_tag = soup.body
        body_attributes: dict = dict(body_tag.attrs) if body_tag else {}

        # Serialise head/body as raw HTML strings.
        head_html: str | None = str(soup.head) if soup.head else None
        body_html: str | None = str(body_tag) if body_tag else None

        # Style blocks text content.
        style_blocks = [
            (s.string or "") for s in soup.find_all("style")
        ]

        # Meta, link, and script tags.
        meta_tags = [dict(m.attrs) for m in soup.find_all("meta")]
        link_tags = [dict(lk.attrs) for lk in soup.find_all("link")]
        script_blocks = [
            {"src": s.get("src"), "content": s.string}
            for s in soup.find_all("script")
        ]

        return {
            "metadata": {
                "source_type": "html",
                "extraction_mode": "html",
                "title": title,
                "doctype": doctype_str,
                "head_html": head_html,
                "body_html": body_html,
                "html_attributes": html_attributes,
                "body_attributes": body_attributes,
                "style_blocks": style_blocks,
                "meta_tags": meta_tags,
                "link_tags": link_tags,
                "script_blocks": script_blocks,
            },
            "document_order": state.document_order,
            "styles": [],
            "numbering": [],
            "sections": [],
            "media": state.media,
            "paragraphs": state.paragraphs,
            "tables": state.tables,
            "document_defaults": None,
        }

    # ── internal helpers ─────────────────────────────────────────────────────

    def _add_media(self, image_elem: Tag, state: _ExtractionState) -> None:
        src = (image_elem.get("src") or "").strip()
        if not src:
            return
        w = image_elem.get("width", "")
        h = image_elem.get("height", "")
        state.media.append({
            "relationship_id": f"html_img_{state.media_index}",
            "content_type": None,
            "file_name": src.split("/")[-1],
            "local_file_path": src,
            "local_url": src,
            "width_emu": int(w) if str(w).isdigit() else None,
            "height_emu": int(h) if str(h).isdigit() else None,
            "alt_text": (image_elem.get("alt") or "").strip() or None,
        })
        state.document_order.append(
            {"type": "media", "index": state.media_index})
        state.media_index += 1

    def _add_paragraph(  # NOSONAR
        self,
        elem: Tag,
        state: _ExtractionState,
        *,
        heading_level: int | None = None,
        is_bullet: bool = False,
        is_numbered: bool = False,
        numbering_format: str | None = None,
        list_level: int = 0,
        list_start: int | None = None,
        direction: str | None = None,
        runs: list[dict[str, Any]] | None = None,
    ) -> None:
        if runs is None:
            runs = _extract_runs(elem)
        text = "".join((r.get("text") or "") for r in runs).strip()
        if not text:
            return

        style = f"Heading {heading_level}" if heading_level else None

        elem_dir = (elem.get("dir") or "").lower()
        inline_style = elem.get("style") or ""
        m_dir = _DIRECTION_RE.search(inline_style)

        # Compute cascaded style (stylesheet rules + inline style).
        computed: dict[str, str] = {}
        if state.css_cascade is not None:
            try:
                computed = state.css_cascade.inline_style(elem)
            except (AttributeError, TypeError, ValueError, KeyError):
                computed = {}

        if not direction:
            cascade_dir = computed.get("direction", "").lower()
            is_rtl = (
                elem_dir == "rtl"
                or cascade_dir == "rtl"
                or (m_dir is not None and m_dir.group(1).lower() == "rtl")
                or bool(_ARABIC_RE.search(text))
            )
            if is_rtl:
                direction = "rtl"

        # Resolve alignment from cascade.
        alignment: str | None = None
        text_align = computed.get("text-align", "").lower()
        if text_align in ("left", "center", "right", "justify"):
            alignment = text_align.upper()

        state.paragraphs.append({
            "index": state.paragraph_index,
            "text": text,
            "style": style,
            "is_bullet": is_bullet,
            "is_numbered": is_numbered,
            "list_info": _build_list_info(
                is_bullet, is_numbered, numbering_format, list_level, list_start
            ),
            "numbering_format": numbering_format,
            "list_level": list_level if (is_bullet or is_numbered) else None,
            "alignment": alignment,
            "direction": direction,
            "runs": runs if runs else [_default_run(text)],
            "source": {"format": "html", "tag": elem.name, "raw_html": str(elem)},
        })
        state.document_order.append(
            {"type": "paragraph", "index": state.paragraph_index})
        state.paragraph_index += 1

    def _add_table(self, table_elem: Tag, state: _ExtractionState) -> None:  # NOSONAR
        table_id = id(table_elem)
        if table_id in state.seen_tables:
            return
        state.seen_tables.add(table_id)

        table_rows: list[dict[str, Any]] = []
        max_cols = 0
        row_index = 0

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
                cell_text = _normalize_text(cell.get_text(" ", strip=True))
                cell_runs = _extract_runs(cell)
                try:
                    cs = int(cell.get("colspan", 1) or 1)
                except (ValueError, TypeError):
                    cs = 1
                try:
                    rs = int(cell.get("rowspan", 1) or 1)
                except (ValueError, TypeError):
                    rs = 1
                is_header = cell.name == "th"

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
                        "direction": _get_cell_direction(cell),
                        "runs": cell_runs if cell_runs else [_default_run(cell_text)],
                    }],
                    "tables": [],
                    "cell_index": col_index,
                    "is_header": is_header,
                    "colspan": cs,
                    "rowspan": rs,
                    "_nested_elems": nested_table_elems,
                    "nested_table_indices": [],
                })

            max_cols = max(max_cols, len(cells))
            table_rows.append({"row_index": row_index, "cells": cells})
            row_index += 1

        if not table_rows:
            return

        current_index = state.table_index
        state.tables.append({
            "index": current_index,
            "row_count": len(table_rows),
            "column_count": max_cols,
            "style": None,
            "rows": table_rows,
            "source": {"format": "html", "tag": "table", "raw_html": str(table_elem)},
        })
        state.document_order.append({"type": "table", "index": current_index})
        state.table_index += 1

        for row in table_rows:
            for cell in row["cells"]:
                nested_elems = cell.pop("_nested_elems", [])
                for nt in nested_elems:
                    pre_idx = state.table_index
                    self._add_table(nt, state)
                    if state.table_index > pre_idx:
                        cell["nested_table_indices"].append(pre_idx)

    def _walk_list(  # NOSONAR
        self, list_elem: Tag, state: _ExtractionState, *, list_level: int = 0
    ) -> None:
        """Recursively process <ul>/<ol> preserving nesting depth."""
        is_bullet = list_elem.name.lower() == "ul"
        try:
            start = int(list_elem.get("start", 1) or 1) if not is_bullet else 1
        except (ValueError, TypeError):
            start = 1
        current_number = start

        for li in list_elem.find_all("li", recursive=False):
            numbering_format = "bullet" if is_bullet else f"{current_number}."
            self._add_paragraph(
                li,
                state,
                is_bullet=is_bullet,
                is_numbered=not is_bullet,
                numbering_format=numbering_format,
                list_level=list_level,
                list_start=start if not is_bullet else None,
            )
            for child in li.children:
                if isinstance(child, Tag):
                    cname = child.name.lower()
                    if cname in {"ul", "ol"}:
                        self._walk_list(
                            child, state, list_level=list_level + 1)
                    elif cname == "table":
                        self._add_table(child, state)
            current_number += 1

    def _walk(  # pylint: disable=too-many-branches,too-many-statements  # NOSONAR
        self, parent: Tag, state: _ExtractionState
    ) -> None:
        for child in parent.children:
            if isinstance(child, NavigableString):
                if isinstance(child, Doctype):
                    continue
                txt = _normalize_text(str(child))
                if txt:
                    pseudo = BeautifulSoup(f"<p>{str(child)}</p>", "lxml").p
                    if pseudo is not None:
                        self._add_paragraph(pseudo, state)
                continue

            if not isinstance(child, Tag):
                continue

            name = child.name.lower()

            if name in {"script", "style", "noscript", "meta", "link"}:
                continue

            if name == "br":
                continue

            if name == "hr":
                state.paragraphs.append({
                    "index": state.paragraph_index,
                    "text": "---",
                    "style": "HorizontalRule",
                    "is_bullet": False,
                    "is_numbered": False,
                    "list_info": None,
                    "numbering_format": None,
                    "list_level": None,
                    "alignment": "CENTER",
                    "direction": None,
                    "runs": [_default_run("---")],
                    "source": {"format": "html"},
                })
                state.document_order.append(
                    {"type": "paragraph", "index": state.paragraph_index}
                )
                state.paragraph_index += 1
                continue

            if name == "img":
                if state.include_media:
                    self._add_media(child, state)
                continue

            if name == "table":
                self._add_table(child, state)
                continue

            if name in {"ul", "ol"}:
                self._walk_list(child, state, list_level=0)
                continue

            if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
                self._add_paragraph(child, state, heading_level=int(name[1]))
                continue

            if name == "p":
                block_children = [
                    c for c in child.children
                    if isinstance(c, Tag) and c.name.lower() in {"ul", "ol", "table", "img"}
                ]
                if block_children:
                    text_only_runs = _extract_runs_skip_blocks(child)
                    text_only = "".join(
                        (r.get("text") or "") for r in text_only_runs
                    ).strip()
                    if text_only:
                        self._add_paragraph(child, state, runs=text_only_runs)
                    for bc in block_children:
                        bcname = bc.name.lower()
                        if bcname in {"ul", "ol"}:
                            self._walk_list(bc, state, list_level=0)
                        elif bcname == "table":
                            self._add_table(bc, state)
                        elif bcname == "img" and state.include_media:
                            self._add_media(bc, state)
                else:
                    self._add_paragraph(child, state)
                continue

            if name in {"blockquote", "pre"}:
                self._add_paragraph(child, state)
                continue

            if name in {
                "div", "section", "article", "main", "header",
                "footer", "aside", "nav", "figure", "figcaption",
            }:
                direction = "rtl" if _is_element_rtl(child) else None
                has_block_child = any(
                    isinstance(grand, Tag) and grand.name and
                    grand.name.lower() in self._BLOCK_TAGS
                    for grand in child.children
                )
                if has_block_child:
                    self._walk(child, state)
                else:
                    self._add_paragraph(child, state, direction=direction)
                continue

            txt = _normalize_text(child.get_text(" ", strip=True))
            if txt:
                self._add_paragraph(child, state)
