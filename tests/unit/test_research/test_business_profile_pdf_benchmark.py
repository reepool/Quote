import hashlib
import json
import time
from io import BytesIO

import pytest
from pypdf import PdfWriter
from pypdf.generic import DictionaryObject, NameObject, StreamObject

from research import business_profile_pdf_benchmark as benchmark


def _pdf_bytes(text):
    writer = PdfWriter()
    page = writer.add_blank_page(width=612, height=792)
    font = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
        }
    )
    page[NameObject("/Resources")] = DictionaryObject(
        {NameObject("/Font"): DictionaryObject({NameObject("/F1"): writer._add_object(font)})}
    )
    stream = StreamObject()
    stream.set_data(f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode("latin-1"))
    page[NameObject("/Contents")] = writer._add_object(stream)
    output = BytesIO()
    writer.write(output)
    return output.getvalue()


def _write_pdf(path, text):
    path.write_bytes(_pdf_bytes(text))
    return path


def test_same_corpus_trials_are_cache_isolated_and_fidelity_checked(tmp_path):
    first = _write_pdf(tmp_path / "first.pdf", "Principal Business alpha")
    second = _write_pdf(tmp_path / "second.pdf", "Segment Information beta")

    report = benchmark.run_pdf_parser_benchmark(
        pdf_paths=[first, second],
        concurrency_matrix=(1, 2),
        max_documents=2,
        max_elapsed_seconds=10,
    )

    assert report["schema_version"] == benchmark.PDF_BENCHMARK_SCHEMA_VERSION
    assert report["read_only"] is True
    assert report["production_state_writes"] == 0
    assert [row["concurrency"] for row in report["trials"]] == [1, 2]
    assert len({row["corpus_hash"] for row in report["trials"]}) == 1
    assert len({row["isolation_id"] for row in report["trials"]}) == 2
    for trial in report["trials"]:
        assert trial["cache_mode"] == "disabled_direct_byte_extraction"
        assert trial["isolation_root_removed"] is True
        assert trial["ordered_document_ids"] == [
            row["document_id"] for row in report["corpus"]
        ]
        assert trial["fidelity"]["passed"] is True
        assert trial["errors"] == []
        assert trial["resource_metrics"].keys() == {
            "rss_before_kib",
            "rss_after_kib",
            "process_peak_rss_kib",
        }
        assert all(row["page_hashes"] for row in trial["documents"])


def test_explicit_input_and_bounds_are_required(tmp_path):
    pdf = _write_pdf(tmp_path / "one.pdf", "Principal Business")
    duplicate_content = tmp_path / "duplicate.pdf"
    duplicate_content.write_bytes(pdf.read_bytes())

    with pytest.raises(ValueError, match="explicit local PDF"):
        benchmark.run_pdf_parser_benchmark(concurrency_matrix=(1,))
    with pytest.raises(ValueError, match="max_documents"):
        benchmark.load_explicit_pdf_corpus(pdf_paths=[pdf], max_documents=0)
    with pytest.raises(ValueError, match="concurrency"):
        benchmark.run_pdf_parser_benchmark(
            pdf_paths=[pdf], concurrency_matrix=(17,), max_documents=1
        )
    with pytest.raises(ValueError, match="duplicates"):
        benchmark.run_pdf_parser_benchmark(
            pdf_paths=[pdf], concurrency_matrix=(1, 1), max_documents=1
        )
    with pytest.raises(ValueError, match="duplicate PDF corpus content"):
        benchmark.load_explicit_pdf_corpus(
            pdf_paths=[pdf, duplicate_content], max_documents=2
        )
    with pytest.raises(ValueError, match="max_total_bytes"):
        benchmark.load_explicit_pdf_corpus(
            pdf_paths=[pdf], max_documents=1, max_total_bytes=1
        )


def test_verified_manifest_resolves_relative_path_and_rejects_hash_mismatch(tmp_path):
    pdf = _write_pdf(tmp_path / "one.pdf", "Principal Business")
    content_hash = hashlib.sha256(pdf.read_bytes()).hexdigest()
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps({"documents": [{"path": "one.pdf", "sha256": content_hash}]}),
        encoding="utf-8",
    )

    corpus = benchmark.load_explicit_pdf_corpus(
        manifest_paths=[manifest], max_documents=1
    )
    assert corpus[0].path == str(pdf.resolve())
    assert corpus[0].source_kind == "verified_manifest"

    manifest.write_text(
        json.dumps({"documents": [{"path": "one.pdf", "sha256": "0" * 64}]}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="hash mismatch"):
        benchmark.load_explicit_pdf_corpus(
            manifest_paths=[manifest], max_documents=1
        )


def test_partial_parser_failure_is_reported_without_aborting_corpus(tmp_path):
    valid = _write_pdf(tmp_path / "valid.pdf", "Principal Business")
    malformed = tmp_path / "malformed.pdf"
    malformed.write_bytes(b"%PDF-1.7\nnot-valid")

    report = benchmark.run_pdf_parser_benchmark(
        pdf_paths=[valid, malformed],
        concurrency_matrix=(2,),
        max_documents=2,
        max_elapsed_seconds=10,
    )
    trial = report["trials"][0]

    assert trial["successful_documents"] == 1
    assert trial["failed_documents"] == 1
    assert len(trial["errors"]) == 1
    assert trial["documents"][1]["status"] == "parse_failed"
    assert trial["documents"][1]["error"] == "malformed_pdf"
    assert trial["eligible_for_rollout"] is False


def test_elapsed_bound_marks_unfinished_documents_as_timed_out(tmp_path, monkeypatch):
    pdf = _write_pdf(tmp_path / "one.pdf", "Principal Business")
    original = benchmark._parse_document

    def slow_parse(*args, **kwargs):
        time.sleep(0.03)
        return original(*args, **kwargs)

    monkeypatch.setattr(benchmark, "_parse_document", slow_parse)
    report = benchmark.run_pdf_parser_benchmark(
        pdf_paths=[pdf],
        concurrency_matrix=(1,),
        max_documents=1,
        max_elapsed_seconds=0.001,
    )
    trial = report["trials"][0]

    assert trial["timed_out"] is True
    assert trial["documents"][0]["status"] == "timed_out"
    assert trial["errors"][0]["error"] == "benchmark_trial_timeout"


def test_fidelity_rejects_changed_page_output():
    baseline = {
        "corpus_hash": "same",
        "ordered_document_ids": ["pdf-1"],
        "documents": [
            {
                "document_id": "pdf-1",
                "page_count": 1,
                "normalized_text_hash": "text-a",
                "page_hashes": ["page-a"],
            }
        ],
    }
    candidate = json.loads(json.dumps(baseline))
    candidate["documents"][0]["normalized_text_hash"] = "text-b"

    fidelity = benchmark._evaluate_fidelity(baseline, candidate)

    assert fidelity["passed"] is False
    assert fidelity["mismatches"] == [
        {"document_id": "pdf-1", "reason": "normalized_text_hash_mismatch"}
    ]


def test_report_write_requires_explicit_existing_parent(tmp_path):
    report = {"schema_version": benchmark.PDF_BENCHMARK_SCHEMA_VERSION}
    output = benchmark.write_benchmark_report(report, tmp_path / "report.json")
    assert json.loads(output.read_text(encoding="utf-8")) == report

    with pytest.raises(ValueError, match="parent does not exist"):
        benchmark.write_benchmark_report(report, tmp_path / "missing" / "report.json")
