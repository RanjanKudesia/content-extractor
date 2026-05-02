"""Inline run accumulator for HTML content extraction."""

from typing import Any

from bs4 import NavigableString, Tag

from app.pipelines.html._tag_style import (
    _BLOCK_SKIP_TAGS,
    _apply_inline_css,
    _apply_tag_style_flags,
    _default_run,
    _same_style,
)


class _RunCollector:
    """Accumulates inline runs from a parsed HTML element tree."""

    def __init__(self, *, skip_block_children: bool) -> None:
        self.runs: list[dict[str, Any]] = []
        self._skip_block = skip_block_children

    def push_text(self, text: str, style: dict[str, Any]) -> None:
        """Append or merge a text run."""
        from app.pipelines.html._tag_style import _normalize_text  # pylint: disable=import-outside-toplevel
        normalized = "\n" if text == "\n" else _normalize_text(text)
        if not normalized:
            return
        run: dict[str, Any] = {
            "index": len(self.runs),
            "text": normalized,
            "bold": style.get("bold"),
            "italic": style.get("italic"),
            "underline": style.get("underline"),
            "strikethrough": style.get("strikethrough"),
            "code": style.get("code"),
            "color_rgb": style.get("color_rgb"),
            "font_name": style.get("font_name"),
            "font_size_pt": style.get("font_size_pt"),
            "highlight_color": style.get("highlight_color"),
            "hyperlink_url": style.get("hyperlink_url"),
            "embedded_media": [],
            "semantic_insert": style.get("semantic_insert"),
            "semantic_delete": style.get("semantic_delete"),
            "vertical_align": style.get("vertical_align"),
        }
        if self.runs and _same_style(self.runs[-1], run):
            prev_text = str(self.runs[-1].get("text") or "")
            if prev_text.endswith("\n") or run["text"].startswith("\n"):
                self.runs[-1]["text"] = f"{prev_text}{run['text']}"
            else:
                self.runs[-1]["text"] = f"{prev_text} {run['text']}".strip()
        else:
            run["index"] = len(self.runs)
            self.runs.append(run)

    def collect(self, node: Any, style: dict[str, Any]) -> None:  # NOSONAR
        """Recursively walk a node tree and accumulate runs."""
        if isinstance(node, NavigableString):
            self.push_text(str(node), style)
            return
        if not isinstance(node, Tag):
            return
        name = (node.name or "").lower()
        if name in {"script", "style", "noscript"}:
            return
        if self._skip_block and name in _BLOCK_SKIP_TAGS:
            return
        if name in {"table", "ul", "ol"}:
            return
        if name == "br":
            self.push_text("\n", style)
            return
        if name == "img":
            src = (node.get("src") or "").strip()
            if src:
                w = node.get("width", "")
                h = node.get("height", "")
                media_item: dict[str, Any] = {
                    "relationship_id": None,
                    "content_type": None,
                    "file_name": src.split("/")[-1],
                    "local_file_path": src,
                    "local_url": src,
                    "width_emu": int(w) if str(w).isdigit() else None,
                    "height_emu": int(h) if str(h).isdigit() else None,
                    "alt_text": (node.get("alt") or "").strip() or None,
                }
                if self.runs:
                    self.runs[-1]["embedded_media"].append(media_item)
                else:
                    placeholder = _default_run("")
                    placeholder["embedded_media"] = [media_item]
                    self.runs.append(placeholder)
            return
        next_style = dict(style)
        if name == "a":
            next_style["_node_href"] = (node.get("href") or "").strip()
        _apply_tag_style_flags(name, next_style)
        next_style.pop("_node_href", None)
        _apply_inline_css(node, next_style)
        for sub in node.children:
            self.collect(sub, next_style)


def _collect_runs(element: Tag, *, skip_block_children: bool) -> list[dict[str, Any]]:
    collector = _RunCollector(skip_block_children=skip_block_children)
    collector.collect(element, {
        "bold": None, "italic": None, "underline": None,
        "strikethrough": None, "code": None,
        "color_rgb": None, "font_name": None,
        "font_size_pt": None, "hyperlink_url": None,
    })
    for idx, run in enumerate(collector.runs):
        run["index"] = idx
    return collector.runs


def _extract_runs(element: Tag) -> list[dict[str, Any]]:
    return _collect_runs(element, skip_block_children=False)


def _extract_runs_skip_blocks(element: Tag) -> list[dict[str, Any]]:
    return _collect_runs(element, skip_block_children=True)
