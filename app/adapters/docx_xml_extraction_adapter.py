import base64
from io import BytesIO
from pathlib import Path
from typing import Any
from zipfile import ZipFile, is_zipfile

from lxml import etree

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
R_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PR_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
A_NS = "http://schemas.openxmlformats.org/drawingml/2006/main"
WP_NS = "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
XML_NS = {"w": W_NS, "r": R_NS, "a": A_NS, "wp": WP_NS}


class DocxXmlExtractionAdapter:
    def run(self, file_bytes: bytes, output_basename: str) -> tuple[dict[str, Any], str]:
        if not is_zipfile(BytesIO(file_bytes)):
            raise ValueError(
                "Invalid DOCX file: not a valid ZIP archive. File may be corrupted."
            )

        xml_parts: list[dict[str, str]] = []
        parts_by_path: dict[str, str] = {}
        media_bytes_by_path: dict[str, str] = {}
        with ZipFile(BytesIO(file_bytes), "r") as archive:
            members = archive.namelist()
            for member in members:
                lower = member.lower()
                if lower.startswith("word/media/"):
                    media_bytes_by_path[member] = base64.b64encode(
                        archive.read(member)
                    ).decode("ascii")

                if not (lower.endswith(".xml") or lower.endswith(".rels")):
                    continue

                raw = archive.read(member)
                try:
                    xml_text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    xml_text = raw.decode("utf-8", errors="replace")

                xml_parts.append({"path": member, "xml": xml_text})
                parts_by_path[member] = xml_text

        relationships = self._extract_document_relationships(parts_by_path)
        numbering_map = self._extract_numbering_map(parts_by_path)
        parsed_body = self._extract_parsed_body(
            parts_by_path,
            relationships,
            media_bytes_by_path,
            numbering_map,
        )
        document_defaults = self._extract_document_defaults(parts_by_path)
        styles = self._extract_styles(parts_by_path)

        extracted: dict[str, Any] = {
            "format": "xml",
            "metadata": {
                "xml_part_count": len(xml_parts),
            },
            "document_defaults": document_defaults,
            "styles": styles,
            "relationships": relationships,
            "parsed_body": parsed_body,
            "parts": xml_parts,
        }

        return extracted, f"virtual://extracted/{output_basename}.xml.json"

    def _extract_document_relationships(self, parts_by_path: dict[str, str]) -> dict[str, str]:
        rels_xml = parts_by_path.get("word/_rels/document.xml.rels")
        if not rels_xml:
            return {}

        try:
            root = etree.fromstring(rels_xml.encode("utf-8"))
        except (etree.XMLSyntaxError, ValueError, TypeError):
            return {}

        relationships: dict[str, str] = {}
        for rel in root.findall(f"{{{PR_NS}}}Relationship"):
            rid = rel.get("Id")
            target = rel.get("Target")
            if rid and target:
                relationships[rid] = target
        return relationships

    def _extract_parsed_body(
        self,
        parts_by_path: dict[str, str],
        relationships: dict[str, str],
        media_bytes_by_path: dict[str, str],
        numbering_map: dict[tuple[int, int], str],
    ) -> list[dict[str, Any]]:
        document_xml = parts_by_path.get("word/document.xml")
        if not document_xml:
            return []

        try:
            root = etree.fromstring(document_xml.encode("utf-8"))
        except (etree.XMLSyntaxError, ValueError, TypeError):
            return []

        body = root.find("w:body", XML_NS)
        if body is None:
            return []

        blocks: list[dict[str, Any]] = []
        paragraph_index = 0
        table_index = 0

        for child in body:
            local = child.tag.rsplit(
                "}", 1)[-1] if "}" in child.tag else child.tag
            if local == "p":
                blocks.append(
                    {
                        "type": "paragraph",
                        "index": paragraph_index,
                        "paragraph": self._extract_paragraph_block(
                            child,
                            relationships,
                            media_bytes_by_path,
                            numbering_map,
                        ),
                    }
                )
                paragraph_index += 1
            elif local == "tbl":
                blocks.append(
                    {
                        "type": "table",
                        "index": table_index,
                        "table": self._extract_table_block(
                            child,
                            relationships,
                            media_bytes_by_path,
                            numbering_map,
                        ),
                    }
                )
                table_index += 1

        return blocks

    def _extract_paragraph_block(
        self,
        paragraph_el,
        relationships: dict[str, str],
        media_bytes_by_path: dict[str, str],
        numbering_map: dict[tuple[int, int], str],
    ) -> dict[str, Any]:
        pstyle = paragraph_el.find("w:pPr/w:pStyle", XML_NS)
        style_id = pstyle.get(f"{{{W_NS}}}val") if pstyle is not None else None

        jc = paragraph_el.find("w:pPr/w:jc", XML_NS)
        alignment = jc.get(f"{{{W_NS}}}val") if jc is not None else None

        num_id, ilvl, list_format = self._extract_paragraph_numbering(
            paragraph_el,
            numbering_map,
        )

        runs: list[dict[str, Any]] = []
        run_index = 0

        for child in paragraph_el:
            local = child.tag.rsplit(
                "}", 1)[-1] if "}" in child.tag else child.tag
            if local == "r":
                runs.append(
                    self._extract_run_block(
                        child,
                        run_index,
                        relationships,
                        media_bytes_by_path,
                    )
                )
                run_index += 1
            elif local == "hyperlink":
                rid = child.get(f"{{{R_NS}}}id")
                target = relationships.get(rid) if rid else None
                for hr in child.findall("w:r", XML_NS):
                    run_data = self._extract_run_block(
                        hr,
                        run_index,
                        relationships,
                        media_bytes_by_path,
                    )
                    run_data["hyperlink_rid"] = rid
                    run_data["hyperlink_target"] = target
                    run_data["hyperlink_anchor"] = child.get(
                        f"{{{W_NS}}}anchor")
                    runs.append(run_data)
                    run_index += 1

        text = "".join((run.get("text") or "") for run in runs)
        return {
            "text": text,
            "style_id": style_id,
            "alignment": alignment,
            "is_bullet": list_format == "bullet",
            "is_numbered": bool(list_format and list_format != "bullet"),
            "list_level": ilvl,
            "list_number_id": num_id,
            "list_format": list_format,
            "runs": runs,
        }

    def _extract_run_block(
        self,
        run_el,
        index: int,
        relationships: dict[str, str],
        media_bytes_by_path: dict[str, str],
    ) -> dict[str, Any]:
        text = self._extract_run_text(run_el)
        rpr = run_el.find("w:rPr", XML_NS)

        def has(tag: str) -> bool | None:
            if rpr is None:
                return None
            elem = rpr.find(f"w:{tag}", XML_NS)
            return True if elem is not None else None

        color = None
        font_name = None
        font_size_pt = None
        if rpr is not None:
            color_elem = rpr.find("w:color", XML_NS)
            if color_elem is not None:
                val = color_elem.get(f"{{{W_NS}}}val")
                if val and val.lower() != "auto":
                    color = val.upper()

            fonts_elem = rpr.find("w:rFonts", XML_NS)
            if fonts_elem is not None:
                font_name = (
                    fonts_elem.get(f"{{{W_NS}}}ascii")
                    or fonts_elem.get(f"{{{W_NS}}}hAnsi")
                    or fonts_elem.get(f"{{{W_NS}}}cs")
                )

            size_elem = rpr.find("w:sz", XML_NS)
            if size_elem is not None:
                val = size_elem.get(f"{{{W_NS}}}val")
                if val and val.isdigit():
                    font_size_pt = int(val) / 2.0

        embedded_media = self._extract_run_media(
            run_el,
            relationships,
            media_bytes_by_path,
        )

        return {
            "index": index,
            "text": text,
            "bold": has("b"),
            "italic": has("i"),
            "underline": has("u"),
            "color_rgb": color,
            "font_name": font_name,
            "font_size_pt": font_size_pt,
            "embedded_media": embedded_media,
        }

    def _extract_run_text(self, run_el) -> str:
        chunks: list[str] = []
        for child in run_el:
            local = child.tag.rsplit(
                "}", 1)[-1] if "}" in child.tag else child.tag
            if local == "t":
                chunks.append(child.text or "")
            elif local in {"br", "cr"}:
                chunks.append("\n")
            elif local == "tab":
                chunks.append("\t")
        return "".join(chunks)

    def _extract_run_media(
        self,
        run_el,
        relationships: dict[str, str],
        media_bytes_by_path: dict[str, str],
    ) -> list[dict[str, Any]]:
        media_items: list[dict[str, Any]] = []
        for drawing in run_el.findall("w:drawing", XML_NS):
            for blip in drawing.xpath(".//a:blip", namespaces=XML_NS):
                rid = blip.get(f"{{{R_NS}}}embed")
                if not rid:
                    continue

                target = relationships.get(rid)
                archive_media_path = self._normalize_relationship_target(
                    target)
                media_b64 = media_bytes_by_path.get(archive_media_path)

                width_emu = None
                height_emu = None
                extent = drawing.find(".//wp:extent", XML_NS)
                if extent is not None:
                    cx = extent.get("cx")
                    cy = extent.get("cy")
                    if cx and cx.isdigit():
                        width_emu = int(cx)
                    if cy and cy.isdigit():
                        height_emu = int(cy)

                media_items.append(
                    {
                        "relationship_id": rid,
                        "file_name": Path(archive_media_path).name if archive_media_path else None,
                        "local_file_path": archive_media_path,
                        "width_emu": width_emu,
                        "height_emu": height_emu,
                        "base64_data": media_b64,
                    }
                )

        return media_items

    def _normalize_relationship_target(self, target: str | None) -> str | None:
        if not target:
            return None

        cleaned = target.replace("\\", "/")
        if cleaned.startswith("/"):
            cleaned = cleaned[1:]

        if cleaned.startswith("word/"):
            return cleaned
        return f"word/{cleaned}"

    def _extract_table_block(
        self,
        table_el,
        relationships: dict[str, str],
        media_bytes_by_path: dict[str, str],
        numbering_map: dict[tuple[int, int], str],
    ) -> dict[str, Any]:
        rows_data: list[dict[str, Any]] = []
        for row_i, tr in enumerate(table_el.findall("w:tr", XML_NS)):
            cells_data: list[dict[str, Any]] = []
            for cell_i, tc in enumerate(tr.findall("w:tc", XML_NS)):
                paragraphs = [
                    self._extract_paragraph_block(
                        p,
                        relationships,
                        media_bytes_by_path,
                        numbering_map,
                    )
                    for p in tc.findall("w:p", XML_NS)
                ]
                cells_data.append(
                    {
                        "row_index": row_i,
                        "cell_index": cell_i,
                        "paragraphs": paragraphs,
                        "text": "\n".join((p.get("text") or "") for p in paragraphs),
                    }
                )
            rows_data.append({"row_index": row_i, "cells": cells_data})

        return {
            "rows": rows_data,
            "row_count": len(rows_data),
            "column_count": max((len(r["cells"]) for r in rows_data), default=0),
        }

    def _extract_numbering_map(self, parts_by_path: dict[str, str]) -> dict[tuple[int, int], str]:
        numbering_xml = parts_by_path.get("word/numbering.xml")
        if not numbering_xml:
            return {}

        try:
            root = etree.fromstring(numbering_xml.encode("utf-8"))
        except (etree.XMLSyntaxError, ValueError, TypeError):
            return {}

        abstract_map: dict[int, dict[int, str]] = {}
        for abstract in root.findall("w:abstractNum", XML_NS):
            abstract_id_raw = abstract.get(f"{{{W_NS}}}abstractNumId")
            if not abstract_id_raw or not abstract_id_raw.isdigit():
                continue

            abstract_id = int(abstract_id_raw)
            level_map: dict[int, str] = {}
            for lvl in abstract.findall("w:lvl", XML_NS):
                ilvl_raw = lvl.get(f"{{{W_NS}}}ilvl")
                num_fmt = lvl.find("w:numFmt", XML_NS)
                fmt = num_fmt.get(
                    f"{{{W_NS}}}val") if num_fmt is not None else None
                if ilvl_raw and ilvl_raw.isdigit() and fmt:
                    level_map[int(ilvl_raw)] = fmt

            abstract_map[abstract_id] = level_map

        result: dict[tuple[int, int], str] = {}
        for num in root.findall("w:num", XML_NS):
            num_id_raw = num.get(f"{{{W_NS}}}numId")
            abs_ref = num.find("w:abstractNumId", XML_NS)
            abs_id_raw = abs_ref.get(
                f"{{{W_NS}}}val") if abs_ref is not None else None
            if not num_id_raw or not abs_id_raw:
                continue
            if not num_id_raw.isdigit() or not abs_id_raw.isdigit():
                continue

            num_id = int(num_id_raw)
            abs_id = int(abs_id_raw)
            for ilvl, fmt in abstract_map.get(abs_id, {}).items():
                result[(num_id, ilvl)] = fmt

        return result

    def _extract_paragraph_numbering(
        self,
        paragraph_el,
        numbering_map: dict[tuple[int, int], str],
    ) -> tuple[int | None, int | None, str | None]:
        num_pr = paragraph_el.find("w:pPr/w:numPr", XML_NS)
        if num_pr is None:
            return None, None, None

        num_id = None
        ilvl = 0

        num_id_el = num_pr.find("w:numId", XML_NS)
        if num_id_el is not None:
            val = num_id_el.get(f"{{{W_NS}}}val")
            if val and val.isdigit():
                num_id = int(val)

        ilvl_el = num_pr.find("w:ilvl", XML_NS)
        if ilvl_el is not None:
            val = ilvl_el.get(f"{{{W_NS}}}val")
            if val and val.isdigit():
                ilvl = int(val)

        if num_id is None:
            return None, ilvl, None

        return num_id, ilvl, numbering_map.get((num_id, ilvl))

    def _extract_document_defaults(self, parts_by_path: dict[str, str]) -> dict[str, Any] | None:
        styles_xml = parts_by_path.get("word/styles.xml")
        if not styles_xml:
            return None

        try:
            root = etree.fromstring(styles_xml.encode("utf-8"))
        except (etree.XMLSyntaxError, ValueError, TypeError):
            return None

        rpr = root.find("w:docDefaults/w:rPrDefault/w:rPr", XML_NS)
        if rpr is None:
            return None

        font_name = None
        font_size_pt = None
        color_rgb = None

        fonts_elem = rpr.find("w:rFonts", XML_NS)
        if fonts_elem is not None:
            font_name = (
                fonts_elem.get(f"{{{W_NS}}}ascii")
                or fonts_elem.get(f"{{{W_NS}}}hAnsi")
                or fonts_elem.get(f"{{{W_NS}}}cs")
            )

        size_elem = rpr.find("w:sz", XML_NS)
        if size_elem is not None:
            val = size_elem.get(f"{{{W_NS}}}val")
            if val and val.isdigit():
                font_size_pt = int(val) / 2.0

        color_elem = rpr.find("w:color", XML_NS)
        if color_elem is not None:
            val = color_elem.get(f"{{{W_NS}}}val")
            if val and val.lower() != "auto":
                color_rgb = val.upper()

        if font_name is None and font_size_pt is None and color_rgb is None:
            return None

        return {
            "font_name": font_name,
            "font_size_pt": font_size_pt,
            "color_rgb": color_rgb,
        }

    def _extract_styles(self, parts_by_path: dict[str, str]) -> list[dict[str, Any]]:
        styles_xml = parts_by_path.get("word/styles.xml")
        if not styles_xml:
            return []

        try:
            root = etree.fromstring(styles_xml.encode("utf-8"))
        except (etree.XMLSyntaxError, ValueError, TypeError):
            return []

        styles: list[dict[str, Any]] = []
        for style in root.findall("w:style", XML_NS):
            style_id = style.get(f"{{{W_NS}}}styleId")
            style_type = style.get(f"{{{W_NS}}}type")
            name_el = style.find("w:name", XML_NS)
            name_val = (
                name_el.get(f"{{{W_NS}}}val")
                if name_el is not None
                else None
            )

            font = self._extract_style_font(style)
            styles.append(
                {
                    "style_id": style_id,
                    "name": name_val,
                    "type": style_type,
                    "font": font,
                }
            )

        return styles

    def _extract_style_font(self, style_el) -> dict[str, Any] | None:
        rpr = style_el.find("w:rPr", XML_NS)
        if rpr is None:
            return None

        def _has(tag: str) -> bool | None:
            elem = rpr.find(f"w:{tag}", XML_NS)
            return True if elem is not None else None

        name = None
        size_pt = None
        color_rgb = None
        highlight_color = None

        fonts_elem = rpr.find("w:rFonts", XML_NS)
        if fonts_elem is not None:
            name = (
                fonts_elem.get(f"{{{W_NS}}}ascii")
                or fonts_elem.get(f"{{{W_NS}}}hAnsi")
                or fonts_elem.get(f"{{{W_NS}}}cs")
            )

        size_elem = rpr.find("w:sz", XML_NS)
        if size_elem is not None:
            val = size_elem.get(f"{{{W_NS}}}val")
            if val and val.isdigit():
                size_pt = int(val) / 2.0

        color_elem = rpr.find("w:color", XML_NS)
        if color_elem is not None:
            val = color_elem.get(f"{{{W_NS}}}val")
            if val and val.lower() != "auto":
                color_rgb = val.upper()

        highlight_elem = rpr.find("w:highlight", XML_NS)
        if highlight_elem is not None:
            highlight_color = highlight_elem.get(f"{{{W_NS}}}val")

        if (
            name is None
            and size_pt is None
            and _has("b") is None
            and _has("i") is None
            and _has("u") is None
            and color_rgb is None
            and highlight_color is None
        ):
            return None

        return {
            "name": name,
            "size_pt": size_pt,
            "bold": _has("b"),
            "italic": _has("i"),
            "underline": _has("u"),
            "color_rgb": color_rgb,
            "highlight_color": highlight_color,
        }
