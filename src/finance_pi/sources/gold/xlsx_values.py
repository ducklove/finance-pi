"""Minimal read-only XLSX values reader for the two official tabular sources."""

import io
import posixpath
import xml.etree.ElementTree as ET
import zipfile

NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"


def rows(blob, sheet_name):
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        strings = []
        if "xl/sharedStrings.xml" in archive.namelist():
            strings = [
                "".join(node.itertext())
                for node in ET.fromstring(archive.read("xl/sharedStrings.xml"))
            ]
        sheets = ET.fromstring(archive.read("xl/workbook.xml"))
        sheet = next(
            s for s in sheets.findall("m:sheets/m:sheet", NS) if s.attrib["name"] == sheet_name
        )
        rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        target = next(r.attrib["Target"] for r in rels if r.attrib["Id"] == sheet.attrib[REL])
        path = target.lstrip("/") if target.startswith("/") else posixpath.normpath("xl/" + target)
        for row in ET.fromstring(archive.read(path)).findall("m:sheetData/m:row", NS):
            values = []
            for cell in row:
                column = "".join(c for c in cell.attrib["r"] if c.isalpha())
                index = 0
                for c in column:
                    index = index * 26 + ord(c) - 64
                values.extend([None] * (index - len(values)))
                value = cell.find("m:v", NS)
                if cell.attrib.get("t") == "inlineStr":
                    values[index - 1] = "".join(cell.find("m:is", NS).itertext())
                elif value is not None and cell.attrib.get("t") != "e":
                    text = value.text
                    values[index - 1] = (
                        strings[int(text)] if cell.attrib.get("t") == "s" else float(text)
                    )
            yield values
