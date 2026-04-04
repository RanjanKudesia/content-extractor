import json
from typing import Any

from app.pipelines.ppt_xml_extraction_pipeline import PptXmlExtractionPipeline


class PptJsonExtractionAdapter:
    def __init__(self, pipeline: PptXmlExtractionPipeline) -> None:
        self.pipeline = pipeline

    def run(self, file_bytes: bytes, output_basename: str) -> tuple[dict[str, Any], str]:
        xml_payload, _ = self.pipeline.run(
            file_bytes=file_bytes,
            output_basename=output_basename,
        )

        slides_in = xml_payload.get("parsed_slides", []) or []

        slides: list[dict[str, Any]] = []
        paragraphs: list[dict[str, Any]] = []
        tables: list[dict[str, Any]] = []
        media: list[dict[str, Any]] = []
        document_order: list[dict[str, Any]] = []

        paragraph_index = 0
        table_index = 0
        media_index = 0

        for slide in slides_in:
            slide_idx = slide.get("index")
            slide_title = slide.get("title")
            slide_text = slide.get("text") or ""

            slide_paragraph_indices: list[int] = []
            slide_table_indices: list[int] = []
            slide_media_indices: list[int] = []

            for shape in slide.get("shapes", []) or []:
                kind = shape.get("kind")

                if kind == "shape":
                    for para in shape.get("paragraphs", []) or []:
                        runs = []
                        for run in para.get("runs", []) or []:
                            runs.append(
                                {
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
                            )

                        p_text = para.get("text") or ""
                        paragraphs.append(
                            {
                                "index": paragraph_index,
                                "text": p_text,
                                "style": "Heading 1" if shape.get("is_title") else None,
                                "is_bullet": (para.get("level") is not None and (para.get("level") or 0) > 0),
                                "is_numbered": False,
                                "list_info": {
                                    "kind": "bullet",
                                    "numbering_format": None,
                                    "level": para.get("level"),
                                    "start": None,
                                } if (para.get("level") is not None and (para.get("level") or 0) > 0) else None,
                                "numbering_format": None,
                                "list_level": para.get("level"),
                                "alignment": para.get("alignment"),
                                "runs": runs,
                                "source": {
                                    "format": "pptx",
                                    "slide_index": slide_idx,
                                    "shape_id": shape.get("shape_id"),
                                    "shape_name": shape.get("name"),
                                },
                            }
                        )
                        document_order.append(
                            {"type": "paragraph", "index": paragraph_index})
                        slide_paragraph_indices.append(paragraph_index)
                        paragraph_index += 1

                elif kind == "graphic_frame" and shape.get("graphic_type") == "table":
                    table_payload = shape.get("table") or {}
                    rows_out: list[dict[str, Any]] = []
                    for row in table_payload.get("rows", []) or []:
                        cells_out: list[dict[str, Any]] = []
                        for cell in row.get("cells", []) or []:
                            cell_runs = [
                                {
                                    "index": 0,
                                    "text": cell.get("text") or "",
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
                            ]
                            cells_out.append(
                                {
                                    "text": cell.get("text") or "",
                                    "paragraphs": [
                                        {
                                            "index": 0,
                                            "text": cell.get("text") or "",
                                            "style": None,
                                            "is_bullet": False,
                                            "is_numbered": False,
                                            "list_info": None,
                                            "numbering_format": None,
                                            "alignment": None,
                                            "runs": cell_runs,
                                        }
                                    ],
                                    "tables": [],
                                    "cell_index": cell.get("cell_index"),
                                    "is_header": False,
                                    "colspan": cell.get("grid_span"),
                                    "rowspan": cell.get("row_span"),
                                    "nested_table_indices": [],
                                }
                            )
                        rows_out.append(
                            {
                                "row_index": row.get("row_index"),
                                "cells": cells_out,
                            }
                        )

                    tables.append(
                        {
                            "index": table_index,
                            "rows": rows_out,
                            "source": {
                                "format": "pptx",
                                "slide_index": slide_idx,
                                "frame_id": shape.get("frame_id"),
                                "name": shape.get("name"),
                            },
                        }
                    )
                    document_order.append(
                        {"type": "table", "index": table_index})
                    slide_table_indices.append(table_index)
                    table_index += 1

                elif kind == "picture":
                    media.append(
                        {
                            "relationship_id": shape.get("relationship_id"),
                            "content_type": shape.get("content_type"),
                            "file_name": (shape.get("target_path") or shape.get("target") or "").split("/")[-1] or None,
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
                    )
                    document_order.append(
                        {"type": "media", "index": media_index})
                    slide_media_indices.append(media_index)
                    media_index += 1

            notes = slide.get("notes") or {}
            slides.append(
                {
                    "index": slide_idx,
                    "slide_number": (slide_idx + 1) if isinstance(slide_idx, int) else None,
                    "slide_id": slide.get("slide_id"),
                    "path": slide.get("path"),
                    "title": slide_title,
                    "text": slide_text,
                    "notes_text": notes.get("text") if isinstance(notes, dict) else None,
                    "paragraph_indices": slide_paragraph_indices,
                    "table_indices": slide_table_indices,
                    "media_indices": slide_media_indices,
                    "shape_count": slide.get("shape_count"),
                    "image_count": slide.get("image_count"),
                    "table_count": slide.get("table_count"),
                    "relationships": slide.get("relationships"),
                }
            )

        extracted: dict[str, Any] = {
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
            # Carry package-level parts so generator can reconstruct full design/theme.
            "parts": xml_payload.get("parts", []),
            "binary_parts": xml_payload.get("binary_parts", []),
            "presentation_relationships": xml_payload.get("presentation_relationships"),
            "source_xml_metadata": {
                "presentation": xml_payload.get("presentation"),
                "content_types": xml_payload.get("content_types"),
            },
        }

        return extracted, f"virtual://extracted/{output_basename}.json"
