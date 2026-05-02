"""HTML tag-to-style mapping and inline CSS extraction helpers."""

import re
from typing import Any

from bs4 import Tag

from app.pipelines.html._css_helpers import _css_color_to_rgb, _css_size_to_pt

_BLOCK_SKIP_TAGS: frozenset[str] = frozenset({"ul", "ol", "table", "img"})


def _normalize_text(value: str) -> str:
    if not value:
        return ""
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for line in value.split("\n"):
        cleaned = re.sub(r"\s+", " ", line).strip()
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)


def _default_run(text: str) -> dict[str, Any]:
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


def _same_style(a: dict[str, Any], b: dict[str, Any]) -> bool:
    keys = (
        "bold", "italic", "underline", "strikethrough", "code",
        "color_rgb", "font_name", "font_size_pt", "hyperlink_url",
        "highlight_color", "semantic_insert", "semantic_delete", "vertical_align",
    )
    return all(a.get(k) == b.get(k) for k in keys)


def _apply_tag_style_flags(name: str, style: dict[str, Any]) -> None:
    """Update style dict in-place based on semantic HTML tag name."""
    if name in {"b", "strong"}:
        style["bold"] = True
    if name in {"i", "em", "cite", "dfn", "var"}:
        style["italic"] = True
    if name == "u":
        style["underline"] = True
    if name == "ins":
        style["underline"] = True
        style["semantic_insert"] = True
    if name in {"s", "strike"}:
        style["strikethrough"] = True
    if name == "del":
        style["strikethrough"] = True
        style["semantic_delete"] = True
    if name in {"code", "kbd", "samp", "tt"}:
        style["code"] = True
    if name == "mark":
        style["highlight_color"] = "yellow"
    if name in {"sub", "sup"}:
        style["vertical_align"] = name
    if name == "a":
        href = (style.get("_node_href") or "").strip()
        if href:
            style["hyperlink_url"] = href


def _apply_inline_css(node: Tag, style: dict[str, Any]) -> None:
    """Update style dict in-place from element's inline CSS."""
    inline = node.get("style") or ""
    if not inline:
        return
    m_color = re.search(r"\bcolor\s*:\s*([^;]+)", inline, re.I)
    if m_color:
        style["color_rgb"] = _css_color_to_rgb(m_color.group(1).strip())
    m_font = re.search(r"\bfont-family\s*:\s*([^;]+)", inline, re.I)
    if m_font:
        font_raw = (
            m_font.group(1).strip().strip("'\"").split(
                ",")[0].strip().strip("'\"")
        )
        if font_raw:
            style["font_name"] = font_raw
    m_size = re.search(r"\bfont-size\s*:\s*([^;]+)", inline, re.I)
    if m_size:
        size_pt = _css_size_to_pt(m_size.group(1).strip())
        if size_pt is not None:
            style["font_size_pt"] = size_pt
