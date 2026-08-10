"""Read-only same-corpus benchmark for business-profile PDF page extraction."""

from __future__ import annotations

import hashlib
import json
import logging
import multiprocessing
import re
import tempfile
import threading
import time
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from research.business_profile_pdf_artifacts import (
    BusinessProfilePdfArtifactExtractor,
    _aggregate_pypdf_warnings,
)

PDF_BENCHMARK_SCHEMA_VERSION = "business_profile_pdf_benchmark.v1"
DEFAULT_CONCURRENCY_MATRIX = (4, 6, 8)
MAX_BENCHMARK_DOCUMENTS = 32
MAX_BENCHMARK_CONCURRENCY = 16
MAX_BENCHMARK_ELAPSED_SECONDS = 3600.0
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PdfCorpusDocument:
    document_id: str
    path: str
    content_hash: str
    size_bytes: int
    content: bytes
    source_kind: str

    def public_identity(self) -> dict[str, Any]:
        return {
            "document_id": self.document_id,
            "path": self.path,
            "content_hash": self.content_hash,
            "size_bytes": self.size_bytes,
            "source_kind": self.source_kind,
        }


def load_explicit_pdf_corpus(
    *,
    pdf_paths: Iterable[str | Path] = (),
    manifest_paths: Iterable[str | Path] = (),
    max_documents: int = 8,
    max_total_bytes: int = 512 * 1024 * 1024,
) -> tuple[PdfCorpusDocument, ...]:
    """Load an explicit local corpus and verify manifest hashes before parsing."""

    document_limit = _bounded_int(
        max_documents,
        name="max_documents",
        minimum=1,
        maximum=MAX_BENCHMARK_DOCUMENTS,
    )
    byte_limit = _bounded_int(
        max_total_bytes,
        name="max_total_bytes",
        minimum=1,
        maximum=4 * 1024 * 1024 * 1024,
    )
    candidates: list[tuple[Path, str | None, str]] = []
    for raw_path in pdf_paths:
        candidates.append((Path(raw_path).expanduser(), None, "explicit_path"))
    for raw_manifest_path in manifest_paths:
        manifest_path = Path(raw_manifest_path).expanduser().resolve()
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        rows = payload.get("documents") if isinstance(payload, Mapping) else payload
        if not isinstance(rows, list) or not rows:
            raise ValueError(f"PDF manifest has no documents: {manifest_path}")
        for row in rows:
            if not isinstance(row, Mapping):
                raise TypeError(f"PDF manifest row must be an object: {manifest_path}")
            raw_pdf_path = (
                row.get("path") or row.get("archive_path") or row.get("local_path")
            )
            expected_hash = row.get("sha256") or row.get("content_hash")
            if not raw_pdf_path or not re.fullmatch(
                r"[0-9a-fA-F]{64}", str(expected_hash or "")
            ):
                raise ValueError(
                    "PDF manifest rows require path and sha256/content_hash: "
                    f"{manifest_path}"
                )
            candidate_path = Path(str(raw_pdf_path)).expanduser()
            if not candidate_path.is_absolute():
                candidate_path = manifest_path.parent / candidate_path
            candidates.append(
                (candidate_path, str(expected_hash).lower(), "verified_manifest")
            )
    if not candidates:
        raise ValueError(
            "explicit local PDF paths or verified manifest paths are required"
        )
    if len(candidates) > document_limit:
        raise ValueError(
            f"PDF corpus exceeds max_documents: {len(candidates)}>{document_limit}"
        )

    documents: list[PdfCorpusDocument] = []
    seen_paths: set[str] = set()
    seen_hashes: set[str] = set()
    total_bytes = 0
    for source_path, expected_hash, source_kind in candidates:
        resolved = source_path.resolve(strict=True)
        if not resolved.is_file():
            raise ValueError(f"PDF corpus path is not a file: {resolved}")
        path_key = str(resolved)
        if path_key in seen_paths:
            raise ValueError(f"duplicate PDF corpus path: {resolved}")
        seen_paths.add(path_key)
        file_size = resolved.stat().st_size
        if total_bytes + file_size > byte_limit:
            raise ValueError(
                "PDF corpus exceeds max_total_bytes: "
                f"{total_bytes + file_size}>{byte_limit}"
            )
        content = resolved.read_bytes()
        content_hash = hashlib.sha256(content).hexdigest()
        if expected_hash is not None and content_hash != expected_hash:
            raise ValueError(f"PDF manifest hash mismatch: {resolved}")
        if content_hash in seen_hashes:
            raise ValueError(f"duplicate PDF corpus content: {resolved}")
        seen_hashes.add(content_hash)
        total_bytes += len(content)
        documents.append(
            PdfCorpusDocument(
                document_id="pdf-" + content_hash[:20],
                path=path_key,
                content_hash=content_hash,
                size_bytes=len(content),
                content=content,
                source_kind=source_kind,
            )
        )
    return tuple(documents)


def run_pdf_parser_benchmark(
    *,
    pdf_paths: Iterable[str | Path] = (),
    manifest_paths: Iterable[str | Path] = (),
    concurrency_matrix: Sequence[int] = DEFAULT_CONCURRENCY_MATRIX,
    max_documents: int = 8,
    max_total_bytes: int = 512 * 1024 * 1024,
    max_elapsed_seconds: float = 600.0,
) -> dict[str, Any]:
    """Benchmark pypdf extraction without discovery, downloads, or production writes."""

    matrix = _validate_concurrency_matrix(concurrency_matrix)
    elapsed_limit = _bounded_float(
        max_elapsed_seconds,
        name="max_elapsed_seconds",
        minimum=0.001,
        maximum=MAX_BENCHMARK_ELAPSED_SECONDS,
    )
    corpus = load_explicit_pdf_corpus(
        pdf_paths=pdf_paths,
        manifest_paths=manifest_paths,
        max_documents=max_documents,
        max_total_bytes=max_total_bytes,
    )
    ordered_identity = [
        (document.document_id, document.content_hash) for document in corpus
    ]
    corpus_hash = _stable_hash(ordered_identity)
    trials = []
    for concurrency in matrix:
        logger.info(
            "business-profile PDF benchmark trial started concurrency=%s "
            "documents=%s corpus_hash=%s elapsed_limit=%s",
            concurrency,
            len(corpus),
            corpus_hash,
            elapsed_limit,
        )
        trial = _run_trial(
            corpus,
            concurrency=concurrency,
            max_elapsed_seconds=elapsed_limit,
            corpus_hash=corpus_hash,
        )
        trials.append(trial)
        logger.info(
            "business-profile PDF benchmark trial completed concurrency=%s "
            "wall_seconds=%s successful=%s failed=%s timed_out=%s warnings=%s",
            concurrency,
            trial["wall_seconds"],
            trial["successful_documents"],
            trial["failed_documents"],
            trial["timed_out"],
            trial["warning_count"],
        )
    baseline = trials[0]
    for trial in trials:
        fidelity = _evaluate_fidelity(baseline, trial)
        trial["fidelity"] = fidelity
        trial["eligible_for_rollout"] = bool(
            not trial["timed_out"]
            and not trial["errors"]
            and fidelity["passed"]
        )
    eligible = [trial for trial in trials if trial["eligible_for_rollout"]]
    fastest = (
        max(eligible, key=lambda row: row["throughput_documents_per_second"])
        if eligible
        else baseline
    )
    baseline_throughput = float(baseline["throughput_documents_per_second"] or 0.0)
    improvement = (
        float(fastest["throughput_documents_per_second"]) / baseline_throughput - 1.0
        if baseline_throughput > 0
        else 0.0
    )
    change_supported = bool(
        eligible
        and fastest["concurrency"] != baseline["concurrency"]
        and improvement >= 0.10
    )
    return {
        "schema_version": PDF_BENCHMARK_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "pypdf",
        "read_only": True,
        "production_state_writes": 0,
        "corpus_hash": corpus_hash,
        "corpus": [document.public_identity() for document in corpus],
        "bounds": {
            "max_documents": max_documents,
            "max_total_bytes": max_total_bytes,
            "max_elapsed_seconds_per_trial": elapsed_limit,
            "max_concurrency": MAX_BENCHMARK_CONCURRENCY,
        },
        "trials": trials,
        "recommendation": {
            "baseline_concurrency": baseline["concurrency"],
            "eligible_trial_count": len(eligible),
            "fastest_eligible_concurrency": (
                fastest["concurrency"] if eligible else None
            ),
            "throughput_improvement_ratio": round(improvement, 6),
            "production_default_change_supported": change_supported,
            "reason": (
                "same-corpus fidelity passed and throughput improved by at least 10%"
                if change_supported
                else (
                    "no complete fidelity-passing trial; retain production default"
                    if not eligible
                    else "retain production default pending material same-corpus improvement"
                )
            ),
        },
    }


def write_benchmark_report(report: Mapping[str, Any], path: str | Path) -> Path:
    """Write only to an explicitly selected report path."""

    output_path = Path(path).expanduser().resolve()
    if not output_path.parent.is_dir():
        raise ValueError(f"benchmark report parent does not exist: {output_path.parent}")
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(dict(report), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output_path)
    return output_path


def _run_trial(
    corpus: Sequence[PdfCorpusDocument],
    *,
    concurrency: int,
    max_elapsed_seconds: float,
    corpus_hash: str,
) -> dict[str, Any]:
    started = time.monotonic()
    context_name = (
        "fork" if "fork" in multiprocessing.get_all_start_methods() else "spawn"
    )
    process_context = multiprocessing.get_context(context_name)
    trial: dict[str, Any]
    with tempfile.TemporaryDirectory(
        prefix=f"business-profile-pdf-benchmark-c{concurrency}-"
    ) as isolation_root:
        isolation_root_path = Path(isolation_root)
        isolation_id = hashlib.sha256(isolation_root.encode("utf-8")).hexdigest()[:16]
        result_path = isolation_root_path / "trial-result.json"
        process = process_context.Process(
            target=_trial_process_entry,
            args=(
                tuple(corpus),
                concurrency,
                corpus_hash,
                isolation_root_path,
                result_path,
            ),
            name=f"bp-pdf-benchmark-c{concurrency}",
            daemon=True,
        )
        process.start()
        process.join(max_elapsed_seconds)
        if process.is_alive():
            process.terminate()
            process.join(5)
            if process.is_alive():
                process.kill()
                process.join(5)
            trial = _failed_trial(
                corpus,
                concurrency=concurrency,
                corpus_hash=corpus_hash,
                wall_seconds=time.monotonic() - started,
                error="benchmark_trial_timeout",
                status="timed_out",
                timed_out=True,
            )
        elif process.exitcode != 0 or not result_path.is_file():
            trial = _failed_trial(
                corpus,
                concurrency=concurrency,
                corpus_hash=corpus_hash,
                wall_seconds=time.monotonic() - started,
                error=f"benchmark_trial_process_exit:{process.exitcode}",
                status="exception",
                timed_out=False,
            )
        else:
            trial = json.loads(result_path.read_text(encoding="utf-8"))
        trial["isolation_id"] = isolation_id
        trial["trial_process_start_method"] = context_name
    trial["isolation_root_removed"] = not isolation_root_path.exists()
    return trial


def _trial_process_entry(
    corpus: Sequence[PdfCorpusDocument],
    concurrency: int,
    corpus_hash: str,
    isolation_root: Path,
    result_path: Path,
) -> None:
    trial = _run_trial_worker(
        corpus,
        concurrency=concurrency,
        corpus_hash=corpus_hash,
        isolation_root=isolation_root,
    )
    temporary = result_path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(trial, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    temporary.replace(result_path)


def _run_trial_worker(
    corpus: Sequence[PdfCorpusDocument],
    *,
    concurrency: int,
    corpus_hash: str,
    isolation_root: Path,
) -> dict[str, Any]:
    started = time.monotonic()
    rss_before = _process_memory_kib()
    activity_lock = threading.Lock()
    activity = {"active": 0, "peak": 0}
    results: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, Any]] = []
    with ThreadPoolExecutor(
        max_workers=concurrency,
        thread_name_prefix=f"bp-pdf-c{concurrency}",
    ) as executor:
        future_documents: dict[Future[dict[str, Any]], PdfCorpusDocument] = {
            executor.submit(
                _parse_document,
                document,
                isolation_root,
                activity,
                activity_lock,
            ): document
            for document in corpus
        }
        for future, document in future_documents.items():
            try:
                result = future.result()
            except Exception as exc:  # noqa: BLE001 - isolate one corrupt document
                result = _exception_result(document, exc)
            results[document.document_id] = result
            if result.get("error"):
                errors.append(
                    {
                        "document_id": document.document_id,
                        "error": result["error"],
                    }
                )
    completed = time.monotonic()
    ordered_results = [results[document.document_id] for document in corpus]
    successful = sum(1 for row in ordered_results if not row.get("error"))
    wall_seconds = completed - started
    rss_after = _process_memory_kib()
    return {
        "engine": "pypdf",
        "concurrency": concurrency,
        "corpus_hash": corpus_hash,
        "ordered_document_ids": [document.document_id for document in corpus],
        "cache_mode": "disabled_direct_byte_extraction",
        "wall_seconds": round(wall_seconds, 6),
        "cpu_seconds": round(
            sum(float(row.get("cpu_seconds") or 0.0) for row in ordered_results),
            6,
        ),
        "throughput_documents_per_second": round(
            successful / wall_seconds if wall_seconds else 0.0,
            6,
        ),
        "successful_documents": successful,
        "failed_documents": len(ordered_results) - successful,
        "peak_concurrency": activity["peak"],
        "timed_out": False,
        "warning_count": sum(
            int(row.get("warning_count") or 0) for row in ordered_results
        ),
        "errors": errors,
        "resource_metrics": {
            "rss_before_kib": rss_before.get("rss_kib"),
            "rss_after_kib": rss_after.get("rss_kib"),
            "process_peak_rss_kib": rss_after.get("peak_rss_kib"),
        },
        "documents": ordered_results,
    }


def _failed_trial(
    corpus: Sequence[PdfCorpusDocument],
    *,
    concurrency: int,
    corpus_hash: str,
    wall_seconds: float,
    error: str,
    status: str,
    timed_out: bool,
) -> dict[str, Any]:
    documents = [
        {
            **document.public_identity(),
            "status": status,
            "error": error,
            "wall_seconds": None,
            "cpu_seconds": None,
            "warning_count": 0,
            "warning_samples": [],
            "page_count": None,
            "normalized_text_hash": None,
            "page_hashes": [],
            "heading_count": None,
        }
        for document in corpus
    ]
    return {
        "engine": "pypdf",
        "concurrency": concurrency,
        "corpus_hash": corpus_hash,
        "ordered_document_ids": [document.document_id for document in corpus],
        "cache_mode": "disabled_direct_byte_extraction",
        "wall_seconds": round(wall_seconds, 6),
        "cpu_seconds": 0.0,
        "throughput_documents_per_second": 0.0,
        "successful_documents": 0,
        "failed_documents": len(corpus),
        "peak_concurrency": 0,
        "timed_out": timed_out,
        "warning_count": 0,
        "errors": [
            {"document_id": document.document_id, "error": error}
            for document in corpus
        ],
        "resource_metrics": {
            "rss_before_kib": None,
            "rss_after_kib": None,
            "process_peak_rss_kib": None,
        },
        "documents": documents,
    }


def _parse_document(
    document: PdfCorpusDocument,
    _isolation_root: Path,
    activity: dict[str, int],
    activity_lock: threading.Lock,
) -> dict[str, Any]:
    with activity_lock:
        activity["active"] += 1
        activity["peak"] = max(activity["peak"], activity["active"])
    started = time.monotonic()
    thread_time = getattr(time, "thread_time", time.process_time)
    cpu_started = thread_time()
    try:
        with _aggregate_pypdf_warnings() as warning_summary:
            artifact = BusinessProfilePdfArtifactExtractor().extract_bytes(
                document.content,
                source_file_id=document.document_id,
                source_pdf_path=document.path,
            )
        normalized_pages = [_normalize_text(page.text) for page in artifact.pages]
        page_hashes = [
            hashlib.sha256(text.encode("utf-8")).hexdigest()
            for text in normalized_pages
        ]
        normalized_text_hash = hashlib.sha256(
            "\f".join(normalized_pages).encode("utf-8")
        ).hexdigest()
        failure_class = str((artifact.diagnostics or {}).get("failure_class") or "")
        error = (
            failure_class or "parse_failed"
            if artifact.status == "parse_failed"
            else None
        )
        return {
            **document.public_identity(),
            "status": artifact.status,
            "error": error,
            "wall_seconds": round(time.monotonic() - started, 6),
            "cpu_seconds": round(thread_time() - cpu_started, 6),
            "warning_count": int(warning_summary["count"]),
            "warning_samples": list(warning_summary["samples"]),
            "page_count": artifact.page_count,
            "normalized_text_hash": normalized_text_hash,
            "page_hashes": page_hashes,
            "heading_count": len(artifact.heading_index),
        }
    finally:
        with activity_lock:
            activity["active"] -= 1


def _exception_result(
    document: PdfCorpusDocument, exc: BaseException
) -> dict[str, Any]:
    return {
        **document.public_identity(),
        "status": "exception",
        "error": f"{type(exc).__name__}: {str(exc)[:500]}",
        "wall_seconds": None,
        "cpu_seconds": None,
        "warning_count": 0,
        "warning_samples": [],
        "page_count": None,
        "normalized_text_hash": None,
        "page_hashes": [],
        "heading_count": None,
    }


def _evaluate_fidelity(
    baseline: Mapping[str, Any], candidate: Mapping[str, Any]
) -> dict[str, Any]:
    baseline_rows = {
        str(row["document_id"]): row for row in baseline.get("documents", [])
    }
    candidate_rows = {
        str(row["document_id"]): row for row in candidate.get("documents", [])
    }
    mismatches: list[dict[str, Any]] = []
    if candidate.get("corpus_hash") != baseline.get("corpus_hash"):
        mismatches.append({"reason": "corpus_hash_mismatch"})
    if candidate.get("ordered_document_ids") != baseline.get("ordered_document_ids"):
        mismatches.append({"reason": "ordered_corpus_mismatch"})
    for document_id in baseline.get("ordered_document_ids", []):
        expected = baseline_rows.get(str(document_id))
        actual = candidate_rows.get(str(document_id))
        if expected is None or actual is None:
            mismatches.append(
                {"document_id": document_id, "reason": "missing_document_result"}
            )
            continue
        for field in ("page_count", "normalized_text_hash", "page_hashes"):
            if actual.get(field) != expected.get(field):
                mismatches.append(
                    {"document_id": document_id, "reason": f"{field}_mismatch"}
                )
    return {
        "policy": "strict_page_count_and_normalized_text_hashes",
        "passed": not mismatches,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:100],
    }


def _validate_concurrency_matrix(values: Sequence[int]) -> tuple[int, ...]:
    matrix = tuple(
        _bounded_int(
            value,
            name="concurrency",
            minimum=1,
            maximum=MAX_BENCHMARK_CONCURRENCY,
        )
        for value in values
    )
    if not matrix:
        raise ValueError("concurrency_matrix cannot be empty")
    if len(set(matrix)) != len(matrix):
        raise ValueError("concurrency_matrix cannot contain duplicates")
    return matrix


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value or "")).strip()


def _process_memory_kib() -> dict[str, int | None]:
    values: dict[str, int | None] = {"rss_kib": None, "peak_rss_kib": None}
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                values["rss_kib"] = int(line.split()[1])
            elif line.startswith("VmHWM:"):
                values["peak_rss_kib"] = int(line.split()[1])
    except (FileNotFoundError, OSError, ValueError, IndexError):
        pass
    return values


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _bounded_int(value: Any, *, name: str, minimum: int, maximum: int) -> int:
    parsed = int(value)
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return parsed


def _bounded_float(
    value: Any, *, name: str, minimum: float, maximum: float
) -> float:
    parsed = float(value)
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return parsed
