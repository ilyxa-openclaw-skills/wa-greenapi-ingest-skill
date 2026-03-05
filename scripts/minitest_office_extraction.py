#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace


def _base_row(*, msg_id: str, file_name: str, mime_type: str) -> dict:
    return {
        "source_message_id": msg_id,
        "text": "<media:file>",
        "raw_obj": {"waArchiveIngestDiag": {}},
        "_payload": {
            "messageData": {
                "typeMessage": "fileMessage",
            }
        },
        "_media_meta": {
            "isMedia": True,
            "isAudio": False,
            "isImage": False,
            "isVoice": False,
            "messageType": "fileMessage",
            "mediaLabel": "document",
            "mimeType": mime_type,
            "fileName": file_name,
        },
    }


def _make_docx_with_table(path: Path) -> None:
    ns = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    doc = f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<w:document xmlns:w=\"{ns}\">
  <w:body>
    <w:p><w:r><w:t>Body line</w:t></w:r></w:p>
    <w:tbl>
      <w:tr>
        <w:tc><w:p><w:r><w:t>R1C1</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>R1C2</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>R2C1</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>R2C2</w:t></w:r></w:p></w:tc>
      </w:tr>
    </w:tbl>
  </w:body>
</w:document>
"""
    header_ok = f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<w:hdr xmlns:w=\"{ns}\"><w:p><w:r><w:t>Header text</w:t></w:r></w:p></w:hdr>
"""
    footer_ok = f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<w:ftr xmlns:w=\"{ns}\"><w:p><w:r><w:t>Footer text</w:t></w:r></w:p></w:ftr>
"""

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", "<?xml version='1.0' encoding='UTF-8'?><Types/>")
        zf.writestr("word/document.xml", doc)
        zf.writestr("word/header1.xml", header_ok)
        zf.writestr("word/footer1.xml", footer_ok)
        # Намеренно битый XML: нужен для проверки эвристики (есть ошибки, но текст извлечён).
        zf.writestr("word/header2.xml", "<w:hdr>")


def _make_xlsx_with_table(path: Path) -> None:
    workbook = """<?xml version='1.0' encoding='UTF-8'?>
<workbook xmlns='http://schemas.openxmlformats.org/spreadsheetml/2006/main'
 xmlns:r='http://schemas.openxmlformats.org/officeDocument/2006/relationships'>
  <sheets>
    <sheet name='TableSheet' sheetId='1' r:id='rId1'/>
  </sheets>
</workbook>
"""
    rels = """<?xml version='1.0' encoding='UTF-8'?>
<Relationships xmlns='http://schemas.openxmlformats.org/package/2006/relationships'>
  <Relationship Id='rId1' Type='http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet' Target='worksheets/sheet1.xml'/>
</Relationships>
"""
    shared = """<?xml version='1.0' encoding='UTF-8'?>
<sst xmlns='http://schemas.openxmlformats.org/spreadsheetml/2006/main' count='3' uniqueCount='3'>
  <si><t>Name</t></si>
  <si><t>Qty</t></si>
  <si><t>Widget</t></si>
</sst>
"""
    sheet = """<?xml version='1.0' encoding='UTF-8'?>
<worksheet xmlns='http://schemas.openxmlformats.org/spreadsheetml/2006/main'>
  <sheetData>
    <row r='1'>
      <c r='A1' t='s'><v>0</v></c>
      <c r='B1' t='s'><v>1</v></c>
    </row>
    <row r='2'>
      <c r='A2' t='s'><v>2</v></c>
      <c r='B2'><v>10</v></c>
      <c r='C2' t='inlineStr'><is><t>Inline</t></is></c>
    </row>
    <row r='3'>
      <c r='A3'><v>10</v></c>
      <c r='B3'><f>A3*2</f><v>20</v></c>
    </row>
  </sheetData>
</worksheet>
"""

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", "<?xml version='1.0' encoding='UTF-8'?><Types/>")
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", rels)
        zf.writestr("xl/sharedStrings.xml", shared)
        zf.writestr("xl/worksheets/sheet1.xml", sheet)


def _run_case(ingest, *, path: Path, row: dict) -> dict:
    orig_download = ingest.download_media_file

    def fake_download_media_file(**kwargs):
        return {
            "ok": True,
            "path": str(path),
            "size": int(path.stat().st_size),
            "sha256": "test",
            "mimeType": str(row["_media_meta"].get("mimeType") or ""),
            "sourceUrl": "mock://office",
            "attempts": 1,
            "sourceCandidates": ["mock://office"],
        }

    ingest.download_media_file = fake_download_media_file
    try:
        stats = ingest._enrich_media_and_transcript(
            client=SimpleNamespace(),
            row=row,
            media_dir=path.parent,
            transcribe_audio=False,
            transcribe_model="whisper-1",
            transcribe_language=None,
            describe_images=False,
            describe_model="gpt-4o-mini",
            keep_media_files=False,
        )
    finally:
        ingest.download_media_file = orig_download

    diag = row["raw_obj"]["waArchiveIngestDiag"].get("documentAnalysis", {})
    storage = row["raw_obj"]["waArchiveIngestDiag"].get("mediaStorage", {})

    return {
        "text": row.get("text"),
        "diag": diag,
        "stats": stats,
        "binary_deleted": bool(storage.get("binaryDeleted")),
        "path_exists": path.exists(),
    }


def main() -> int:
    this_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(this_dir))

    import greenapi_ingest as ingest  # noqa: WPS433

    with tempfile.TemporaryDirectory(prefix="greenapi-minitest-office-") as td:
        td_path = Path(td)

        docx_path = td_path / "sample-table.docx"
        _make_docx_with_table(docx_path)

        # Имитируем кейс, когда файл сохранён без расширения (частый источник false-fail ранее).
        xlsx_path = td_path / "sample-office.bin"
        _make_xlsx_with_table(xlsx_path)

        docx_row = _base_row(
            msg_id="minitest-docx-table",
            file_name="report.docx",
            mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        xlsx_row = _base_row(
            msg_id="minitest-xlsx-table",
            file_name="table.xlsx",
            mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        docx_case = _run_case(ingest, path=docx_path, row=docx_row)
        xlsx_case = _run_case(ingest, path=xlsx_path, row=xlsx_row)

    docx_text = str(docx_case.get("text") or "")
    xlsx_text = str(xlsx_case.get("text") or "")

    checks = {
        "docx_table_extracted": all(token in docx_text for token in ("Header text", "Body line", "R1C1", "R2C2", "Footer text")),
        "docx_not_failed_marker": docx_text != "[office extraction failed]",
        "docx_diag_has_required_fields": all(
            key in (docx_case["diag"] or {}) for key in ("engine", "bytes", "error", "extracted_chars")
        ),
        "docx_heuristic_kept_text_on_partial_errors": bool(docx_case["diag"].get("error")) and bool(docx_case["diag"].get("ok")),
        "xlsx_shared_strings_and_formula": all(
            token in xlsx_text for token in ("A1: Name", "B1: Qty", "A2: Widget", "B3: =A3*2 => 20")
        ),
        "xlsx_detected_from_zip_even_bin_ext": xlsx_case["diag"].get("format") == ".xlsx",
        "xlsx_not_failed_marker": xlsx_text != "[office extraction failed]",
        "cleanup_keep_0": (
            docx_case["binary_deleted"] and not docx_case["path_exists"] and xlsx_case["binary_deleted"] and not xlsx_case["path_exists"]
        ),
    }

    ok = all(checks.values())
    print(
        json.dumps(
            {
                "ok": ok,
                "checks": checks,
                "docx_case": docx_case,
                "xlsx_case": xlsx_case,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
