from io import BytesIO
from pathlib import Path

import pytest
from pypdf import PdfWriter
from pypdf.generic import DictionaryObject, NameObject, StreamObject

from research.business_profile_pdf_artifacts import (
    BUSINESS_PROFILE_PDF_ARTIFACT_SCHEMA_VERSION,
    BusinessProfilePdfArtifactExtractor,
    BusinessProfilePdfArtifactStore,
    build_not_disclosed_diagnostic,
    build_table_parse_failure_diagnostic,
    build_unsupported_template_diagnostic,
    ensure_archived_pdf_page_artifact,
)
from scripts.research_business_profile_pdf_artifact import build_pdf_artifact


def _pdf_bytes(page_texts, *, encrypted=False):
    writer = PdfWriter()
    for text in page_texts:
        page = writer.add_blank_page(width=612, height=792)
        font = DictionaryObject(
            {
                NameObject("/Type"): NameObject("/Font"),
                NameObject("/Subtype"): NameObject("/Type1"),
                NameObject("/BaseFont"): NameObject("/Helvetica"),
            }
        )
        page[NameObject("/Resources")] = DictionaryObject(
            {
                NameObject("/Font"): DictionaryObject(
                    {NameObject("/F1"): writer._add_object(font)}
                )
            }
        )
        escaped = (
            str(text).replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        )
        stream = StreamObject()
        stream.set_data(f"BT /F1 12 Tf 72 720 Td ({escaped}) Tj ET".encode("latin-1"))
        page[NameObject("/Contents")] = writer._add_object(stream)
    if encrypted:
        writer.encrypt("secret")
    output = BytesIO()
    writer.write(output)
    return output.getvalue()


def test_extracts_page_text_heading_density_and_hashes():
    content = _pdf_bytes(
        [
            "Principal Business and business model with enough native text for analysis",
            "Segment Information revenue and cost analysis with product details",
        ]
    )
    extractor = BusinessProfilePdfArtifactExtractor(low_text_character_threshold=10)

    artifact = extractor.extract_bytes(content, source_file_id="source-1")

    assert artifact.schema_version == BUSINESS_PROFILE_PDF_ARTIFACT_SCHEMA_VERSION
    assert artifact.status == "parsed"
    assert artifact.page_count == 2
    assert artifact.pages[0].native_text_status == "extracted"
    assert artifact.pages[0].text_density_per_square_inch > 0
    assert artifact.pages[0].text_hash
    assert artifact.pages[0].page_artifact_hash
    assert {item.heading_type for item in artifact.heading_index} >= {
        "principal_business",
        "business_model",
        "segment_information",
    }
    assert artifact.artifact_hash


def test_low_text_only_enters_ocr_queue_when_page_is_targeted():
    content = _pdf_bytes(["x", "y"])
    extractor = BusinessProfilePdfArtifactExtractor(low_text_character_threshold=10)

    artifact = extractor.extract_bytes(content, target_page_numbers=[2])

    assert artifact.low_text_pages == [1, 2]
    assert artifact.ocr_required_pages == [2]
    assert artifact.pages[0].field_relevant is False
    assert artifact.pages[0].ocr_required is False
    assert artifact.pages[1].field_relevant is True
    assert artifact.pages[1].ocr_required is True
    assert artifact.status == "ocr_required"


def test_failure_classes_cover_invalid_malformed_and_encrypted_pdf():
    extractor = BusinessProfilePdfArtifactExtractor()

    invalid = extractor.extract_bytes(b"not a pdf")
    malformed = extractor.extract_bytes(b"%PDF-1.7\nnot-valid")
    encrypted = extractor.extract_bytes(_pdf_bytes(["secret"], encrypted=True))

    assert invalid.status == "parse_failed"
    assert invalid.diagnostics["failure_class"] == "invalid_pdf_signature"
    assert invalid.parser_diagnostics[0].outcome == "unsupported"
    assert malformed.diagnostics["failure_class"] == "malformed_pdf"
    assert malformed.parser_diagnostics[0].outcome == "malformed"
    assert encrypted.encrypted is True
    assert encrypted.diagnostics["failure_class"] == "encrypted_password_required"
    assert encrypted.parser_diagnostics[0].outcome == "encrypted"


def test_glyph_decoding_failure_is_distinct_and_queues_target_page_for_ocr():
    content = _pdf_bytes(["Principal Business \x01\x02\x03 corrupted glyphs"])
    artifact = BusinessProfilePdfArtifactExtractor(
        low_text_character_threshold=1,
        glyph_decoding_ratio_threshold=0.01,
    ).extract_bytes(content, target_page_numbers=[1])

    assert artifact.pages[0].native_text_status == "glyph_decoding_error"
    assert artifact.pages[0].suspicious_glyph_count == 3
    assert artifact.diagnostics["glyph_decoding_pages"] == [1]
    assert artifact.ocr_required_pages == [1]
    assert [item.outcome for item in artifact.parser_diagnostics] == [
        "glyph_decoding",
        "ocr_required",
    ]


def test_downstream_diagnostics_keep_parse_failures_separate_from_not_disclosed():
    table_failure = build_table_parse_failure_diagnostic(
        page_numbers=[8, 7, 8],
        field_name="segment_revenue",
        detail="merged header spans pages",
    )
    unsupported = build_unsupported_template_diagnostic(
        page_numbers=[12],
        detail="issuer-specific reserve table",
    )
    not_disclosed = build_not_disclosed_diagnostic(
        field_name="hedge_quantity",
        section_parse_succeeded=True,
        page_numbers=[30],
    )

    assert table_failure.outcome == "table_parse_failure"
    assert table_failure.page_numbers == [7, 8]
    assert table_failure.blocks_numeric_candidates is True
    assert unsupported.outcome == "unsupported_template"
    assert not_disclosed.outcome == "not_disclosed"
    assert not_disclosed.blocks_numeric_candidates is True


def test_not_disclosed_requires_successful_section_parse_and_evidence():
    with pytest.raises(ValueError, match="successfully parsed"):
        build_not_disclosed_diagnostic(
            field_name="unit_cost",
            section_parse_succeeded=False,
            page_numbers=[20],
        )
    with pytest.raises(ValueError, match="evidence pages"):
        build_not_disclosed_diagnostic(
            field_name="unit_cost",
            section_parse_succeeded=True,
            page_numbers=[],
        )


def test_table_parse_failure_requires_evidence_pages():
    with pytest.raises(ValueError, match="requires evidence pages"):
        build_table_parse_failure_diagnostic(
            page_numbers=[],
            detail="merged header spans pages",
        )


def test_artifact_store_writes_gzip_beside_original_and_short_circuits(tmp_path):
    source_path = (
        tmp_path
        / "SSE"
        / "600309"
        / "2025-12-31"
        / "original"
        / "announcement_hash.pdf"
    )
    source_path.parent.mkdir(parents=True)
    source_path.write_bytes(_pdf_bytes(["Principal Business native text"]))
    artifact = BusinessProfilePdfArtifactExtractor(
        low_text_character_threshold=5
    ).extract_file(source_path, source_file_id="source-1")
    store = BusinessProfilePdfArtifactStore()

    first = store.write(artifact)
    second = store.write(artifact)
    stored = store.read(first.artifact_path)

    assert first.status == "written"
    assert second.status == "unchanged"
    assert Path(first.artifact_path).parent.parent.name == "derived"
    assert stored["artifact_hash"] == artifact.artifact_hash
    assert stored["source_file_id"] is None
    assert stored["source_pdf_path"] is None
    assert stored["pages"][0]["text"].startswith("Principal Business")


def test_artifact_identity_and_layout_are_stable(tmp_path):
    content = _pdf_bytes(["Principal Business native text"])
    extractor = BusinessProfilePdfArtifactExtractor(low_text_character_threshold=5)
    first = extractor.extract_bytes(content, source_pdf_path="/old/report.pdf")
    second = extractor.extract_bytes(
        content,
        source_file_id="another-source",
        source_pdf_path="/new/report.pdf",
    )
    targeted = extractor.extract_bytes(content, target_page_numbers=[1])
    source_path = tmp_path / "original" / "report.pdf"
    store = BusinessProfilePdfArtifactStore()

    assert first.artifact_hash == second.artifact_hash
    assert first.parameter_hash == second.parameter_hash
    assert targeted.parameter_hash != first.parameter_hash
    assert (
        store.artifact_path(
            source_path,
            source_content_hash=first.source_content_hash,
            extractor_version="table-parser.v1",
            parameter_hash=first.parameter_hash,
            artifact_kind="tables",
        ).parts[-3]
        == "tables"
    )
    assert (
        store.artifact_path(
            source_path,
            source_content_hash=first.source_content_hash,
            extractor_version="ocr-worker.v1",
            parameter_hash=first.parameter_hash,
            artifact_kind="ocr",
        ).parts[-3]
        == "ocr"
    )


def test_diagnostics_only_command_helper_does_not_write_artifact(tmp_path):
    source_path = tmp_path / "original" / "report.pdf"
    source_path.parent.mkdir(parents=True)
    source_path.write_bytes(_pdf_bytes(["Principal Business native text"]))

    payload = build_pdf_artifact(
        source_pdf=source_path,
        diagnostics_only=True,
        low_text_character_threshold=5,
    )

    assert payload["status"] == "parsed"
    assert payload["artifact_path"] is None
    assert payload["parameter_hash"]
    assert payload["parser_diagnostics"] == []
    assert not (tmp_path / "derived").exists()


def test_archived_manifest_page_artifact_is_hash_bound_and_reused(tmp_path):
    source_path = tmp_path / "original" / "report.pdf"
    source_path.parent.mkdir(parents=True)
    content = _pdf_bytes(["Principal Business native text"])
    source_path.write_bytes(content)
    manifest = {
        "source_file_id": "source-1",
        "archive_path": str(source_path),
        "content_hash": __import__("hashlib").sha256(content).hexdigest(),
    }

    first = ensure_archived_pdf_page_artifact(
        manifest,
        extractor=BusinessProfilePdfArtifactExtractor(low_text_character_threshold=5),
    )
    second = ensure_archived_pdf_page_artifact(
        manifest,
        extractor=BusinessProfilePdfArtifactExtractor(low_text_character_threshold=5),
    )

    assert first["status"] == "written"
    assert second["status"] == "unchanged"
    assert first["artifact_hash"] == second["artifact_hash"]

    source_path.write_bytes(content + b"changed")
    with pytest.raises(RuntimeError, match="hash mismatch"):
        ensure_archived_pdf_page_artifact(manifest)
