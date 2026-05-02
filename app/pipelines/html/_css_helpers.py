"""CSS value conversion helpers and style-block cascade applier."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

try:
    import tinycss2
except ImportError:  # pragma: no cover - optional dependency
    tinycss2 = None

try:
    import lxml.html as lhtml
except ImportError:  # pragma: no cover - optional dependency
    lhtml = None

try:
    from cssselect import GenericTranslator, SelectorError
except ImportError:  # pragma: no cover - optional dependency
    GenericTranslator = None
    SelectorError = None

if TYPE_CHECKING:
    from bs4 import BeautifulSoup, Tag


def _css_color_to_rgb(raw: str) -> str | None:
    raw = raw.strip()
    if raw.startswith("#"):
        return raw
    m_rgb = re.match(
        r"rgb\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)", raw, re.I)
    if m_rgb:
        r, g, b = int(m_rgb.group(1)), int(m_rgb.group(2)), int(m_rgb.group(3))
        return f"#{r:02x}{g:02x}{b:02x}"
    if re.match(r"^[a-zA-Z]+$", raw):
        return raw
    return None


def _css_size_to_pt(raw: str) -> float | None:
    """Convert a CSS font-size value to points (best-effort)."""
    raw = raw.strip()
    m = re.match(r"([\d.]+)\s*(pt|px|em|rem)?", raw, re.I)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    unit = (m.group(2) or "pt").lower()
    if unit == "pt":
        return val
    if unit == "px":
        return round(val * 0.75, 2)
    if unit in ("em", "rem"):
        return round(val * 12.0, 2)
    return None


# ---------------------------------------------------------------------------
# CSS cascade: parse <style> blocks and apply computed styles to elements.
# ---------------------------------------------------------------------------

def build_css_cascade(soup: "BeautifulSoup") -> "_CssCascade":
    """Parse all <style> blocks in the document and return a cascade object."""
    return _CssCascade(soup)


class _CssCascade:
    """Lightweight CSS cascade: collects rules from <style> blocks and lets
    callers query the computed inline properties for any element.

    Only handles simple property→value rules (no @media, no pseudo-classes).
    Specificity is tracked so higher-specificity rules win.
    """

    def __init__(self, soup: "BeautifulSoup") -> None:
        # rules: list of (specificity, selector, {prop: value})
        self._rules: list[tuple[int, str, dict[str, str]]] = []
        self._parse(soup)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def computed_style(self, element: "Tag") -> dict[str, str]:
        """Return the merged CSS properties that apply to *element*.

        Properties are resolved in ascending specificity order so that
        higher-specificity rules overwrite lower ones.  Inline styles
        (from the element's own @style attribute) are applied last.
        """
        if GenericTranslator is None or lhtml is None:
            return _parse_inline_style(element.get("style") or "")

        translator = GenericTranslator()

        merged: dict[str, str] = {}

        for _specificity, selector, props in sorted(self._rules, key=lambda r: r[0]):
            try:
                xpath = translator.css_to_xpath(selector)
            except SelectorError:
                continue
            try:
                # BeautifulSoup doesn't support XPath natively; use lxml.
                doc_html = lhtml.fromstring(str(element.parent or element))
                matches = doc_html.xpath(xpath)
                # Convert element to lxml to check match.
                el_xpath = translator.css_to_xpath(
                    _element_unique_selector(element))
                el_nodes = doc_html.xpath(el_xpath)
                if matches and el_nodes:
                    merged.update(props)
            except (TypeError, ValueError, AttributeError):
                continue

        # Inline style always wins.
        merged.update(_parse_inline_style(element.get("style") or ""))
        return merged

    def inline_style(self, element: "Tag") -> dict[str, str]:
        """Return only the element's inline @style properties."""
        return _parse_inline_style(element.get("style") or "")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _parse(self, soup: "BeautifulSoup") -> None:
        if tinycss2 is None:
            return

        for style_tag in soup.find_all("style"):
            css_text = style_tag.get_text() or ""
            for spec, selector, props in _parse_css_rules(css_text):
                self._rules.append((spec, selector, props))


def _parse_css_rules(css_text: str) -> list[tuple[int, str, dict[str, str]]]:
    """Parse CSS text into cascade-ready rules with precomputed specificity."""
    parsed_rules: list[tuple[int, str, dict[str, str]]] = []
    rules = tinycss2.parse_stylesheet(
        css_text,
        skip_comments=True,
        skip_whitespace=True,
    )
    for rule in rules:
        parsed_rules.extend(_parse_qualified_rule(rule))
    return parsed_rules


def _parse_qualified_rule(rule: object) -> list[tuple[int, str, dict[str, str]]]:
    """Convert a tinycss2 qualified-rule into one rule per selector."""
    if getattr(rule, "type", None) != "qualified-rule":
        return []

    selector_text = tinycss2.serialize(rule.prelude).strip()
    props = _parse_rule_declarations(rule.content)
    if not props:
        return []

    parsed: list[tuple[int, str, dict[str, str]]] = []
    for selector in selector_text.split(","):
        selector = selector.strip()
        if selector:
            parsed.append((_selector_specificity(selector), selector, props))
    return parsed


def _parse_rule_declarations(content: object) -> dict[str, str]:
    """Extract declaration name/value pairs from a tinycss2 rule content block."""
    declarations = tinycss2.parse_declaration_list(
        content,
        skip_comments=True,
        skip_whitespace=True,
    )
    props: dict[str, str] = {}
    for decl in declarations:
        if getattr(decl, "type", None) != "declaration":
            continue
        props[decl.name.lower()] = tinycss2.serialize(decl.value).strip()
    return props


def _selector_specificity(selector: str) -> int:
    """Compute an integer specificity score for a CSS selector.

    Uses the (a, b, c) model flattened to a single integer (a*10000 + b*100 + c).
    Only handles common simple selectors — good enough for document CSS.
    """
    selector = re.sub(r"::[^:\s,]+", "", selector)  # strip pseudo-elements
    selector = re.sub(r":[^:\s,]+", "", selector)   # strip pseudo-classes
    ids = len(re.findall(r"#[\w-]+", selector))
    classes = len(re.findall(r"\.[\w-]+", selector))
    attrs = len(re.findall(r"\[[\w^$*~|=-]+\]", selector))
    tags = len(re.findall(
        r"(?<![.#\[])(?:^|[ +>~])([a-zA-Z][\w-]*)", selector))
    return ids * 10000 + (classes + attrs) * 100 + tags


def _element_unique_selector(element: "Tag") -> str:
    """Build the simplest CSS selector that identifies this element."""
    el_id = element.get("id")
    if el_id:
        return f"#{el_id}"
    classes = element.get("class", [])
    tag = element.name or "div"
    if classes:
        return f"{tag}.{'.'.join(classes)}"
    return tag


def _parse_inline_style(style_str: str) -> dict[str, str]:
    """Parse a CSS inline style string into a property→value dict."""
    result: dict[str, str] = {}
    if not style_str:
        return result
    for declaration in style_str.split(";"):
        declaration = declaration.strip()
        if ":" not in declaration:
            continue
        prop, _, value = declaration.partition(":")
        prop = prop.strip().lower()
        value = value.strip()
        if prop and value:
            result[prop] = value
    return result
