"""RTL (right-to-left) text direction detection helpers."""

import re

from bs4 import Tag

_ARABIC_RE = re.compile(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]")
_DIRECTION_RE = re.compile(r"\bdirection\s*:\s*(\w+)", re.I)


def _is_element_rtl(elem: Tag) -> bool:
    """Return True if the element has or implies RTL text direction."""
    elem_dir = (elem.get("dir") or "").lower()
    m_dir = _DIRECTION_RE.search(elem.get("style") or "")
    has_arabic = bool(_ARABIC_RE.search(elem.get_text()))
    return (
        elem_dir == "rtl"
        or (m_dir is not None and m_dir.group(1).lower() == "rtl")
        or has_arabic
    )
