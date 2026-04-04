import base64
from io import BytesIO
from pathlib import Path
from typing import Any
from zipfile import ZipFile, is_zipfile

from lxml import etree

P_NS = "http://schemas.openxmlformats.org/presentationml/2006/main"
A_NS = "http://schemas.openxmlformats.org/drawingml/2006/main"
R_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PR_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

NS = {
    "p": P_NS,
    "a": A_NS,
    "r": R_NS,
}


class PptXmlExtractionPipeline:
    """Extract rich, XML-centric structured data from a PPTX archive.

    Output contains:
    - Package-wide XML/rels part dump
    - Presentation-level metadata and slide ordering
    - Detailed per-slide parsed content (text, runs, shapes, images, tables, notes)
    - Relationships graph per presentation and per slide
    - Embedded media (base64) for image references
    """

    def run(self, file_bytes: bytes, output_basename: str) -> tuple[dict[str, Any], str]:
        if not is_zipfile(BytesIO(file_bytes)):
            raise ValueError(
                "Invalid PPTX file: not a valid ZIP archive. File may be corrupted."
            )

        parts_by_path: dict[str, str] = {}
        xml_parts: list[dict[str, str]] = []
        media_bytes_by_path: dict[str, str] = {}
        binary_parts: list[dict[str, str]] = []

        with ZipFile(BytesIO(file_bytes), "r") as archive:
            for member in archive.namelist():
                lower = member.lower()
                if member.endswith("/"):
                    continue

                if lower.startswith("ppt/media/"):
                    media_bytes_by_path[member] = base64.b64encode(
                        archive.read(member)
                    ).decode("ascii")

                if not (lower.endswith(".xml") or lower.endswith(".rels")):
                    # Preserve non-XML package parts (fonts, embeddings, etc.)
                    # so downstream reconstruction can keep original design fidelity.
                    if not lower.startswith("ppt/media/"):
                        raw_bin = archive.read(member)
                        binary_parts.append(
                            {
                                "path": member,
                                "base64": base64.b64encode(raw_bin).decode("ascii"),
                            }
                        )
                    continue

                raw = archive.read(member)
                try:
                    xml_text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    xml_text = raw.decode("utf-8", errors="replace")

                parts_by_path[member] = xml_text
                xml_parts.append({"path": member, "xml": xml_text})

        content_types = self._extract_content_types(parts_by_path)
        presentation_rels = self._extract_relationships(
            parts_by_path.get("ppt/_rels/presentation.xml.rels")
        )
        presentation_meta = self._extract_presentation_meta(parts_by_path)
        slides = self._extract_slides(
            parts_by_path=parts_by_path,
            presentation_rels=presentation_rels,
            media_bytes_by_path=media_bytes_by_path,
        )

        extracted: dict[str, Any] = {
            "format": "xml",
            "document_type": "pptx",
            "metadata": {
                "xml_part_count": len(xml_parts),
                "media_count": len(media_bytes_by_path),
                "binary_part_count": len(binary_parts),
                "slide_count": len(slides),
            },
            "content_types": content_types,
            "presentation": presentation_meta,
            "presentation_relationships": presentation_rels,
            "parsed_slides": slides,
            "parts": xml_parts,
            "binary_parts": binary_parts,
        }

        return extracted, f"virtual://extracted/{output_basename}.ppt.xml.json"

    def _extract_content_types(self, parts_by_path: dict[str, str]) -> dict[str, Any]:
        xml = parts_by_path.get("[Content_Types].xml")
        if not xml:
            return {"defaults": [], "overrides": []}

        try:
            root = etree.fromstring(xml.encode("utf-8"))
        except (etree.XMLSyntaxError, ValueError, TypeError):
            return {"defaults": [], "overrides": []}

        defaults: list[dict[str, str]] = []
        overrides: list[dict[str, str]] = []

        for child in root:
            local = child.tag.rsplit(
                "}", 1)[-1] if "}" in child.tag else child.tag
            if local == "Default":
                defaults.append(
                    {
                        "extension": child.get("Extension") or "",
                        "content_type": child.get("ContentType") or "",
                    }
                )
            elif local == "Override":
                overrides.append(
                    {
                        "part_name": child.get("PartName") or "",
                        "content_type": child.get("ContentType") or "",
                    }
                )

        return {"defaults": defaults, "overrides": overrides}

    def _extract_relationships(self, rels_xml: str | None) -> dict[str, dict[str, str]]:
        if not rels_xml:
            return {}

        try:
            root = etree.fromstring(rels_xml.encode("utf-8"))
        except (etree.XMLSyntaxError, ValueError, TypeError):
            return {}

        relationships: dict[str, dict[str, str]] = {}
        for rel in root.findall(f"{{{PR_NS}}}Relationship"):
            rid = rel.get("Id")
            if not rid:
                continue
            relationships[rid] = {
                "target": rel.get("Target") or "",
                "type": rel.get("Type") or "",
                "target_mode": rel.get("TargetMode") or "",
            }
        return relationships

    def _extract_presentation_meta(self, parts_by_path: dict[str, str]) -> dict[str, Any]:
        xml = parts_by_path.get("ppt/presentation.xml")
        if not xml:
            return {"slide_size": None, "notes_size": None, "slide_id_list": []}

        try:
            root = etree.fromstring(xml.encode("utf-8"))
        except (etree.XMLSyntaxError, ValueError, TypeError):
            return {"slide_size": None, "notes_size": None, "slide_id_list": []}

        slide_size = None
        sld_sz = root.find("p:sldSz", NS)
        if sld_sz is not None:
            slide_size = {
                "cx": self._as_int(sld_sz.get("cx")),
                "cy": self._as_int(sld_sz.get("cy")),
                "type": sld_sz.get("type"),
            }

        notes_size = None
        notes_sz = root.find("p:notesSz", NS)
        if notes_sz is not None:
            notes_size = {
                "cx": self._as_int(notes_sz.get("cx")),
                "cy": self._as_int(notes_sz.get("cy")),
            }

        slide_ids: list[dict[str, Any]] = []
        for sld_id in root.findall("p:sldIdLst/p:sldId", NS):
            slide_ids.append(
                {
                    "id": self._as_int(sld_id.get("id")),
                    "rid": sld_id.get(f"{{{R_NS}}}id"),
                }
            )

        return {
            "slide_size": slide_size,
            "notes_size": notes_size,
            "slide_id_list": slide_ids,
        }

    def _extract_slides(
        self,
        parts_by_path: dict[str, str],
        presentation_rels: dict[str, dict[str, str]],
        media_bytes_by_path: dict[str, str],
    ) -> list[dict[str, Any]]:
        presentation_meta = self._extract_presentation_meta(parts_by_path)
        slide_id_list = presentation_meta.get("slide_id_list", [])

        slides: list[dict[str, Any]] = []
        for index, item in enumerate(slide_id_list):
            rid = item.get("rid")
            rel = presentation_rels.get(rid or "", {})
            target = rel.get("target")
            if not target:
                continue

            slide_path = self._normalize_relationship_target(
                base_part="ppt/presentation.xml",
                target=target,
            )
            slide_xml = parts_by_path.get(slide_path)
            if not slide_xml:
                continue

            slide_rels_path = self._rels_path_for_part(slide_path)
            slide_rels = self._extract_relationships(
                parts_by_path.get(slide_rels_path))

            try:
                root = etree.fromstring(slide_xml.encode("utf-8"))
            except (etree.XMLSyntaxError, ValueError, TypeError):
                slides.append(
                    {
                        "index": index,
                        "slide_id": item.get("id"),
                        "rid": rid,
                        "path": slide_path,
                        "relationships": slide_rels,
                        "parse_error": "Failed to parse slide XML",
                    }
                )
                continue

            parsed_slide = self._extract_slide_content(
                root=root,
                slide_path=slide_path,
                slide_rels=slide_rels,
                parts_by_path=parts_by_path,
                media_bytes_by_path=media_bytes_by_path,
            )

            slides.append(
                {
                    "index": index,
                    "slide_id": item.get("id"),
                    "rid": rid,
                    "path": slide_path,
                    "relationships_path": slide_rels_path,
                    "relationships": slide_rels,
                    **parsed_slide,
                }
            )

        return slides

    def _extract_slide_content(
        self,
        root,
        slide_path: str,
        slide_rels: dict[str, dict[str, str]],
        parts_by_path: dict[str, str],
        media_bytes_by_path: dict[str, str],
    ) -> dict[str, Any]:
        title = None
        notes = self._extract_slide_notes(
            slide_path=slide_path,
            slide_rels=slide_rels,
            parts_by_path=parts_by_path,
        )

        shape_tree = root.find("p:cSld/p:spTree", NS)
        shapes: list[dict[str, Any]] = []
        slide_text_chunks: list[str] = []
        image_count = 0
        table_count = 0

        if shape_tree is not None:
            for draw_index, child in enumerate(shape_tree):
                local = child.tag.rsplit(
                    "}", 1)[-1] if "}" in child.tag else child.tag

                if local == "sp":
                    parsed = self._extract_shape_text(child)
                    parsed["kind"] = "shape"
                    parsed["draw_order"] = draw_index
                    shapes.append(parsed)
                    text = (parsed.get("text") or "").strip()
                    if text:
                        slide_text_chunks.append(text)
                    if parsed.get("is_title") and text and title is None:
                        title = text

                elif local == "pic":
                    parsed_pic = self._extract_picture(
                        pic_el=child,
                        slide_path=slide_path,
                        slide_rels=slide_rels,
                        media_bytes_by_path=media_bytes_by_path,
                    )
                    parsed_pic["kind"] = "picture"
                    parsed_pic["draw_order"] = draw_index
                    shapes.append(parsed_pic)
                    image_count += 1

                elif local == "graphicFrame":
                    parsed_graphic = self._extract_graphic_frame(
                        frame_el=child,
                        slide_path=slide_path,
                        slide_rels=slide_rels,
                    )
                    parsed_graphic["kind"] = "graphic_frame"
                    parsed_graphic["draw_order"] = draw_index
                    shapes.append(parsed_graphic)

                    if parsed_graphic.get("graphic_type") == "table":
                        table_count += 1

                elif local == "grpSp":
                    parsed_group = self._extract_group_shape(child)
                    parsed_group["kind"] = "group_shape"
                    parsed_group["draw_order"] = draw_index
                    shapes.append(parsed_group)

                elif local in {"spTree", "nvGrpSpPr", "grpSpPr"}:
                    # container internals, skip explicit record
                    continue

                else:
                    shapes.append(
                        {
                            "kind": "unknown",
                            "draw_order": draw_index,
                            "xml_tag": local,
                        }
                    )

        return {
            "title": title,
            "text": "\n".join(slide_text_chunks),
            "shape_count": len(shapes),
            "image_count": image_count,
            "table_count": table_count,
            "shapes": shapes,
            "notes": notes,
        }

    def _extract_shape_text(self, sp_el) -> dict[str, Any]:
        name = sp_el.find("p:nvSpPr/p:cNvPr", NS)
        ph = sp_el.find("p:nvSpPr/p:nvPr/p:ph", NS)

        shape_name = name.get("name") if name is not None else None
        shape_id = self._as_int(name.get("id")) if name is not None else None

        paragraphs: list[dict[str, Any]] = []
        text_chunks: list[str] = []

        for p_el in sp_el.findall("p:txBody/a:p", NS):
            para = self._extract_text_paragraph(p_el)
            paragraphs.append(para)
            if para["text"]:
                text_chunks.append(para["text"])

        return {
            "shape_id": shape_id,
            "name": shape_name,
            "placeholder": {
                "type": ph.get("type") if ph is not None else None,
                "idx": self._as_int(ph.get("idx")) if ph is not None else None,
            },
            "is_title": bool(ph is not None and (ph.get("type") in {"title", "ctrTitle"})),
            "text": "\n".join(text_chunks),
            "paragraphs": paragraphs,
        }

    def _extract_text_paragraph(self, p_el) -> dict[str, Any]:
        runs: list[dict[str, Any]] = []
        text_chunks: list[str] = []

        ppr = p_el.find("a:pPr", NS)
        level = self._as_int(ppr.get("lvl")) if ppr is not None else None
        align = ppr.get("algn") if ppr is not None else None

        run_index = 0
        for child in p_el:
            local = child.tag.rsplit(
                "}", 1)[-1] if "}" in child.tag else child.tag
            if local == "r":
                t = child.find("a:t", NS)
                text = t.text if t is not None and t.text is not None else ""
                rpr = child.find("a:rPr", NS)
                run = {
                    "index": run_index,
                    "text": text,
                    "bold": self._bool_attr(rpr, "b"),
                    "italic": self._bool_attr(rpr, "i"),
                    "underline": rpr.get("u") if rpr is not None else None,
                    "font_size_pt": self._font_size_pt(rpr),
                    "color_rgb": self._run_color(rpr),
                    "language": rpr.get("lang") if rpr is not None else None,
                }
                runs.append(run)
                text_chunks.append(text)
                run_index += 1
            elif local == "br":
                runs.append(
                    {"index": run_index, "text": "\n", "line_break": True})
                text_chunks.append("\n")
                run_index += 1
            elif local == "fld":
                t = child.find("a:t", NS)
                text = t.text if t is not None and t.text is not None else ""
                runs.append(
                    {
                        "index": run_index,
                        "text": text,
                        "field_id": child.get("id"),
                        "field_type": child.get("type"),
                    }
                )
                text_chunks.append(text)
                run_index += 1

        return {
            "level": level,
            "alignment": align,
            "text": "".join(text_chunks),
            "runs": runs,
        }

    def _extract_picture(
        self,
        pic_el,
        slide_path: str,
        slide_rels: dict[str, dict[str, str]],
        media_bytes_by_path: dict[str, str],
    ) -> dict[str, Any]:
        name_el = pic_el.find("p:nvPicPr/p:cNvPr", NS)
        name = name_el.get("name") if name_el is not None else None
        descr = name_el.get("descr") if name_el is not None else None

        blip = pic_el.find("p:blipFill/a:blip", NS)
        rid = blip.get(f"{{{R_NS}}}embed") if blip is not None else None
        rel = slide_rels.get(rid or "", {})

        target = rel.get("target")
        media_path = (
            self._normalize_relationship_target(slide_path, target)
            if target
            else None
        )

        ext = pic_el.find("p:spPr/a:xfrm/a:ext", NS)
        cx = self._as_int(ext.get("cx")) if ext is not None else None
        cy = self._as_int(ext.get("cy")) if ext is not None else None

        return {
            "name": name,
            "description": descr,
            "relationship_id": rid,
            "target": target,
            "target_path": media_path,
            "content_type": self._guess_image_content_type(media_path),
            "width_emu": cx,
            "height_emu": cy,
            "base64": media_bytes_by_path.get(media_path or ""),
        }

    def _run_color(self, rpr) -> str | None:
        if rpr is None:
            return None
        solid = rpr.find("a:solidFill", NS)
        if solid is None:
            return None

        srgb = solid.find("a:srgbClr", NS)
        if srgb is not None:
            value = (srgb.get("val") or "").strip()
            if len(value) == 6:
                return value.upper()

        # Keep scheme colors visible for downstream handling if explicit RGB is absent.
        scheme = solid.find("a:schemeClr", NS)
        if scheme is not None:
            val = (scheme.get("val") or "").strip()
            if val:
                return f"scheme:{val}"
        return None

    def _extract_graphic_frame(
        self,
        frame_el,
        slide_path: str,
        slide_rels: dict[str, dict[str, str]],
    ) -> dict[str, Any]:
        cnv = frame_el.find("p:nvGraphicFramePr/p:cNvPr", NS)
        name = cnv.get("name") if cnv is not None else None
        frame_id = self._as_int(cnv.get("id")) if cnv is not None else None

        graphic_data = frame_el.find("a:graphic/a:graphicData", NS)
        uri = graphic_data.get("uri") if graphic_data is not None else None

        result: dict[str, Any] = {
            "frame_id": frame_id,
            "name": name,
            "graphic_uri": uri,
            "graphic_type": "unknown",
        }

        if graphic_data is None:
            return result

        if graphic_data.find("a:tbl", NS) is not None:
            result["graphic_type"] = "table"
            result["table"] = self._extract_table(
                graphic_data.find("a:tbl", NS))
            return result

        chart = graphic_data.find(".//*[@r:id]", NS)
        if chart is not None:
            rid = chart.get(f"{{{R_NS}}}id")
            rel = slide_rels.get(rid or "", {})
            target = rel.get("target")
            chart_path = (
                self._normalize_relationship_target(slide_path, target)
                if target
                else None
            )
            result["graphic_type"] = "linked_part"
            result["relationship_id"] = rid
            result["target"] = target
            result["target_path"] = chart_path
            result["relationship_type"] = rel.get("type")
            return result

        return result

    def _extract_group_shape(self, grp_el) -> dict[str, Any]:
        name_el = grp_el.find("p:nvGrpSpPr/p:cNvPr", NS)
        name = name_el.get("name") if name_el is not None else None
        grp_id = self._as_int(name_el.get(
            "id")) if name_el is not None else None

        child_count = 0
        text_count = 0
        for child in grp_el:
            local = child.tag.rsplit(
                "}", 1)[-1] if "}" in child.tag else child.tag
            if local in {"sp", "pic", "graphicFrame", "grpSp", "cxnSp"}:
                child_count += 1
                if local == "sp":
                    texts = child.findall("p:txBody/a:p", NS)
                    text_count += len(texts)

        return {
            "group_id": grp_id,
            "name": name,
            "child_drawable_count": child_count,
            "child_text_paragraph_count": text_count,
        }

    def _extract_table(self, tbl_el) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        for r_idx, tr in enumerate(tbl_el.findall("a:tr", NS)):
            cells: list[dict[str, Any]] = []
            for c_idx, tc in enumerate(tr.findall("a:tc", NS)):
                paragraphs = [
                    self._extract_text_paragraph(p_el)
                    for p_el in tc.findall("a:txBody/a:p", NS)
                ]
                text = "\n".join(
                    p.get("text") or "" for p in paragraphs if p.get("text"))
                cells.append(
                    {
                        "cell_index": c_idx,
                        "text": text,
                        "paragraphs": paragraphs,
                        "row_span": self._as_int(tc.get("rowSpan")),
                        "grid_span": self._as_int(tc.get("gridSpan")),
                    }
                )
            rows.append({"row_index": r_idx, "cells": cells})
        return {"rows": rows}

    def _extract_slide_notes(
        self,
        slide_path: str,
        slide_rels: dict[str, dict[str, str]],
        parts_by_path: dict[str, str],
    ) -> dict[str, Any] | None:
        notes_rel = None
        for _, rel in slide_rels.items():
            rel_type = rel.get("type") or ""
            if rel_type.endswith("/notesSlide"):
                notes_rel = rel
                break

        if not notes_rel:
            return None

        notes_target = notes_rel.get("target")
        if not notes_target:
            return None

        notes_path = self._normalize_relationship_target(
            slide_path, notes_target)
        notes_xml = parts_by_path.get(notes_path)
        if not notes_xml:
            return {
                "path": notes_path,
                "text": "",
                "paragraphs": [],
            }

        try:
            root = etree.fromstring(notes_xml.encode("utf-8"))
        except (etree.XMLSyntaxError, ValueError, TypeError):
            return {
                "path": notes_path,
                "text": "",
                "paragraphs": [],
                "parse_error": "Failed to parse notes XML",
            }

        paragraphs: list[dict[str, Any]] = []
        text_chunks: list[str] = []
        for p_el in root.findall(".//a:p", NS):
            para = self._extract_text_paragraph(p_el)
            if para.get("text"):
                paragraphs.append(para)
                text_chunks.append(para["text"])

        return {
            "path": notes_path,
            "text": "\n".join(text_chunks),
            "paragraphs": paragraphs,
        }

    def _normalize_relationship_target(self, base_part: str, target: str) -> str:
        if not target:
            return ""
        if target.startswith("/"):
            return target.lstrip("/")

        base = Path(base_part)
        parent = base.parent
        resolved = (parent / target).as_posix()

        # Normalize ../ segments
        chunks: list[str] = []
        for chunk in resolved.split("/"):
            if chunk in {"", "."}:
                continue
            if chunk == "..":
                if chunks:
                    chunks.pop()
                continue
            chunks.append(chunk)
        return "/".join(chunks)

    def _rels_path_for_part(self, part_path: str) -> str:
        part = Path(part_path)
        return f"{part.parent.as_posix()}/_rels/{part.name}.rels"

    def _guess_image_content_type(self, media_path: str | None) -> str | None:
        if not media_path:
            return None
        lower = media_path.lower()
        if lower.endswith(".png"):
            return "image/png"
        if lower.endswith(".jpg") or lower.endswith(".jpeg"):
            return "image/jpeg"
        if lower.endswith(".gif"):
            return "image/gif"
        if lower.endswith(".bmp"):
            return "image/bmp"
        if lower.endswith(".tif") or lower.endswith(".tiff"):
            return "image/tiff"
        if lower.endswith(".svg"):
            return "image/svg+xml"
        if lower.endswith(".webp"):
            return "image/webp"
        return None

    def _as_int(self, value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _bool_attr(self, el, attr: str) -> bool | None:
        if el is None:
            return None
        val = el.get(attr)
        if val is None:
            return None
        if val in {"1", "true", "on"}:
            return True
        if val in {"0", "false", "off"}:
            return False
        return None

    def _font_size_pt(self, rpr) -> float | None:
        if rpr is None:
            return None
        sz = rpr.get("sz")
        if not sz:
            return None
        size = self._as_int(sz)
        if size is None:
            return None
        # DrawingML run size is in 1/100 pt.
        return size / 100.0
