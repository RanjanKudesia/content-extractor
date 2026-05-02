"""PowerPoint extraction pipeline for content-extractor service."""

from typing import Any

from app.pipelines.ppt_xml_extraction_pipeline import PptXmlExtractionPipeline


class PptExtractionPipeline:
    """Extract PPTX to JSON while retaining package-level fidelity metadata."""

    def __init__(self) -> None:
        self.pipeline = PptXmlExtractionPipeline()

    @staticmethod
    def _strip_media_from_parsed_slides(
        parsed_slides: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Return parsed slides with picture/media shapes removed."""
        sanitized: list[dict[str, Any]] = []
        for slide in parsed_slides:
            shapes = slide.get("shapes", []) or []
            non_media_shapes = [
                shape for shape in shapes if shape.get("kind") != "picture"
            ]
            updated_slide = dict(slide)
            updated_slide["shapes"] = non_media_shapes
            updated_slide["shape_count"] = len(non_media_shapes)
            updated_slide["image_count"] = 0
            sanitized.append(updated_slide)
        return sanitized

    @staticmethod
    def _build_run_dict(run: dict[str, Any]) -> dict[str, Any]:
        """Build a run dictionary from raw run data."""
        return {
            "index": run.get("index"),
            "text": run.get("text") or "",
            "bold": run.get("bold"),
            "italic": run.get("italic"),
            "underline": True if run.get("underline") else None,
            "color_rgb": run.get("color_rgb"),
            "font_name": None,
            "font_size_pt": run.get("font_size_pt"),
            "highlight_color": None,
            "hyperlink_url": None,
            "embedded_media": [],
        }

    @staticmethod
    def _build_cell_run_dict(cell_text: str) -> dict[str, Any]:
        """Build a run dictionary for table cell text."""
        return {
            "index": 0,
            "text": cell_text or "",
            "bold": None,
            "italic": None,
            "underline": None,
            "color_rgb": None,
            "font_name": None,
            "font_size_pt": None,
            "highlight_color": None,
            "hyperlink_url": None,
            "embedded_media": [],
        }

    @staticmethod
    def _build_cell_paragraph_dict(cell_text: str) -> dict[str, Any]:
        """Build a paragraph dictionary for table cell."""
        return {
            "index": 0,
            "text": cell_text or "",
            "style": None,
            "is_bullet": False,
            "is_numbered": False,
            "list_info": None,
            "numbering_format": None,
            "alignment": None,
            "runs": [PptExtractionPipeline._build_cell_run_dict(cell_text)],
        }

    @staticmethod
    def _build_cell_dict(cell: dict[str, Any]) -> dict[str, Any]:
        """Build a cell dictionary from raw cell data."""
        cell_text = cell.get("text") or ""
        return {
            "text": cell_text,
            "paragraphs": [PptExtractionPipeline._build_cell_paragraph_dict(cell_text)],
            "tables": [],
            "cell_index": cell.get("cell_index"),
            "is_header": False,
            "colspan": cell.get("grid_span"),
            "rowspan": cell.get("row_span"),
            "nested_table_indices": [],
        }

    @staticmethod
    def _build_row_dict(row: dict[str, Any]) -> dict[str, Any]:
        """Build a row dictionary from raw row data."""
        cells_out = [
            PptExtractionPipeline._build_cell_dict(cell)
            for cell in (row.get("cells", []) or [])
        ]
        return {
            "row_index": row.get("row_index"),
            "cells": cells_out,
        }

    @staticmethod
    def _build_paragraph_dict(
        para: dict[str, Any],
        shape: dict[str, Any],
        slide_idx: int,
        para_idx: int,
    ) -> dict[str, Any]:
        """Build a paragraph dictionary from raw paragraph data."""
        runs = [
            PptExtractionPipeline._build_run_dict(run)
            for run in (para.get("runs", []) or [])
        ]
        p_text = para.get("text") or ""
        para_level = para.get("level")
        has_level = para_level is not None and (para_level or 0) > 0

        return {
            "index": para_idx,
            "text": p_text,
            "style": "Heading 1" if shape.get("is_title") else None,
            "is_bullet": has_level,
            "is_numbered": False,
            "list_info": {
                "kind": "bullet",
                "numbering_format": None,
                "level": para_level,
                "start": None,
            } if has_level else None,
            "numbering_format": None,
            "list_level": para_level,
            "alignment": para.get("alignment"),
            "runs": runs,
            "source": {
                "format": "pptx",
                "slide_index": slide_idx,
                "shape_id": shape.get("shape_id"),
                "shape_name": shape.get("name"),
            },
        }

    @staticmethod
    def _build_table_dict(
        table_payload: dict[str, Any],
        shape: dict[str, Any],
        slide_idx: int,
        table_idx: int,
    ) -> dict[str, Any]:
        """Build a table dictionary from raw table data."""
        rows_out = [
            PptExtractionPipeline._build_row_dict(row)
            for row in (table_payload.get("rows", []) or [])
        ]
        return {
            "index": table_idx,
            "rows": rows_out,
            "source": {
                "format": "pptx",
                "slide_index": slide_idx,
                "frame_id": shape.get("frame_id"),
                "name": shape.get("name"),
            },
        }

    @staticmethod
    def _build_media_dict(shape: dict[str, Any], slide_idx: int) -> dict[str, Any]:
        """Build a media dictionary from raw shape data."""
        target_path = shape.get("target_path") or shape.get("target") or ""
        file_name = target_path.split("/")[-1] or None

        return {
            "relationship_id": shape.get("relationship_id"),
            "content_type": shape.get("content_type"),
            "file_name": file_name,
            "local_file_path": shape.get("target_path"),
            "local_url": shape.get("target"),
            "width_emu": shape.get("width_emu"),
            "height_emu": shape.get("height_emu"),
            "alt_text": shape.get("description"),
            "base64": shape.get("base64"),
            "source": {
                "format": "pptx",
                "slide_index": slide_idx,
                "name": shape.get("name"),
            },
        }

    def _process_shape(
        self,
        shape: dict[str, Any],
        slide_idx: int,
        slides_data: list[list[list[int]]],
        tables: list[dict[str, Any]],
        media: list[dict[str, Any]],
        paragraphs: list[dict[str, Any]],
        document_order: list[dict[str, Any]],
        indices: dict[str, int],
        include_media: bool,
    ) -> None:
        """Process a single shape and append to appropriate collections."""
        kind = shape.get("kind")

        if kind == "shape":
            para_indices: list[int] = []
            for para in shape.get("paragraphs", []) or []:
                para_dict = self._build_paragraph_dict(
                    para, shape, slide_idx, indices["paragraph"]
                )
                paragraphs.append(para_dict)
                document_order.append(
                    {"type": "paragraph", "index": indices["paragraph"]}
                )
                para_indices.append(indices["paragraph"])
                indices["paragraph"] += 1
            slides_data[-1][0].extend(para_indices)

        elif kind == "graphic_frame" and shape.get("graphic_type") == "table":
            table_payload = shape.get("table") or {}
            table_dict = self._build_table_dict(
                table_payload, shape, slide_idx, indices["table"]
            )
            tables.append(table_dict)
            document_order.append({"type": "table", "index": indices["table"]})
            slides_data[-1][1].append(indices["table"])
            indices["table"] += 1

        elif kind == "picture" and include_media:
            media_dict = self._build_media_dict(shape, slide_idx)
            media.append(media_dict)
            document_order.append({"type": "media", "index": indices["media"]})
            slides_data[-1][2].append(indices["media"])
            indices["media"] += 1

    def run(
        self,
        file_bytes: bytes,
        include_media: bool = True,
        output_basename: str = "",
    ) -> dict[str, Any]:
        """Extract PowerPoint and return JSON data."""
        xml_payload, _ = self.pipeline.run(
            file_bytes=file_bytes,
            output_basename=output_basename,
        )

        slides_in = xml_payload.get("parsed_slides", []) or []
        if not include_media:
            slides_in = self._strip_media_from_parsed_slides(slides_in)

        slides: list[dict[str, Any]] = []
        paragraphs: list[dict[str, Any]] = []
        tables: list[dict[str, Any]] = []
        media: list[dict[str, Any]] = []
        document_order: list[dict[str, Any]] = []
        slides_data: list[list[list[int]]] = []

        indices: dict[str, int] = {"paragraph": 0, "table": 0, "media": 0}

        for slide in slides_in:
            slide_idx = slide.get("index")
            slide_title = slide.get("title")
            slide_text = slide.get("text") or ""

            slides_data.append([[], [], []])

            for shape in slide.get("shapes", []) or []:
                self._process_shape(
                    shape, slide_idx, slides_data, tables, media,
                    paragraphs, document_order, indices, include_media,
                )

            notes = slide.get("notes") or {}
            slide_para_indices, slide_table_indices, slide_media_indices = (
                slides_data[-1]
            )
            slides.append({
                "index": slide_idx,
                "slide_number": (
                    (slide_idx + 1) if isinstance(slide_idx, int) else None
                ),
                "slide_id": slide.get("slide_id"),
                "path": slide.get("path"),
                "title": slide_title,
                "text": slide_text,
                "notes_text": (
                    notes.get("text") if isinstance(notes, dict) else None
                ),
                "paragraph_indices": slide_para_indices,
                "table_indices": slide_table_indices,
                "media_indices": slide_media_indices,
                "shape_count": slide.get("shape_count"),
                "image_count": slide.get("image_count"),
                "table_count": slide.get("table_count"),
                "relationships": slide.get("relationships"),
            })

        return {
            "format": "json",
            "document_type": "pptx",
            "metadata": {
                "slide_count": len(slides),
                "paragraph_count": len(paragraphs),
                "table_count": len(tables),
                "media_count": len(media),
            },
            "document_order": document_order,
            "styles": [],
            "numbering": [],
            "sections": [],
            "slides": slides,
            "media": media,
            "paragraphs": paragraphs,
            "tables": tables,
            "parts": xml_payload.get("parts", []),
            "binary_parts": xml_payload.get("binary_parts", []),
            "presentation_relationships": (
                xml_payload.get("presentation_relationships")
            ),
            "source_xml_metadata": {
                "presentation": xml_payload.get("presentation"),
                "content_types": xml_payload.get("content_types"),
            },
        }
