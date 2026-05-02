"""Mutable extraction state for a single HTML parse run."""

from typing import Any


class _ExtractionState:
    """Holds mutable state for a single HTML extraction run."""

    def __init__(self, include_media: bool = True) -> None:
        self.paragraphs: list[dict[str, Any]] = []
        self.tables: list[dict[str, Any]] = []
        self.media: list[dict[str, Any]] = []
        self.document_order: list[dict[str, Any]] = []
        self.seen_tables: set[int] = set()
        self.paragraph_index: int = 0
        self.table_index: int = 0
        self.media_index: int = 0
        self.include_media: bool = include_media
        # Set by pipeline after parsing style blocks.
        self.css_cascade: Any = None
