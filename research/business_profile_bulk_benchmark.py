"""Deterministic temporary-database benchmark for business-profile bulk writes."""

from __future__ import annotations

import tempfile
import time
from pathlib import Path
from typing import Any

from research.business_profile_governance import BusinessProfileRepository
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)


BUSINESS_PROFILE_BULK_BENCHMARK_SCHEMA_VERSION = (
    "business_profile_bulk_write_benchmark.v1"
)


def run_business_profile_bulk_write_benchmark(
    *,
    row_count: int = 1000,
    minimum_rows_per_second: float = 200.0,
    maximum_elapsed_seconds: float = 10.0,
) -> dict[str, Any]:
    """Write representative evidence rows to a temporary SQLite database."""

    normalized_count = int(row_count)
    if normalized_count < 1 or normalized_count > 5000:
        raise ValueError("row_count must be between 1 and 5000")
    if minimum_rows_per_second <= 0 or maximum_elapsed_seconds <= 0:
        raise ValueError("benchmark thresholds must be positive")
    with tempfile.TemporaryDirectory(prefix="business-profile-bulk-") as temp_dir:
        root = Path(temp_dir)
        storage = ResearchStorageManager(
            ResearchConfig(
                enabled=True,
                storage=ResearchStorageConfig(
                    db_path=str(root / "research.db"),
                    shadow_mode=True,
                    attach_quotes_db=False,
                    quotes_db_path=str(root / "quotes.db"),
                    quotes_db_alias="quotes",
                    financials_db_path=str(root / "financials.db"),
                    valuation_db_path=str(root / "valuation.db"),
                    interests_db_path=str(root / "interests.db"),
                ),
                budget=ResearchBudgetConfig(),
            )
        )
        storage.initialize()
        repository = BusinessProfileRepository(storage)
        records = [_benchmark_evidence(index) for index in range(normalized_count)]
        started = time.perf_counter()
        written = repository.upsert_many("evidence", records)
        elapsed = time.perf_counter() - started
        persisted = len(repository.list_records("evidence", limit=normalized_count))
    rows_per_second = written / max(elapsed, 1e-9)
    passed = (
        written == normalized_count
        and persisted == normalized_count
        and elapsed <= float(maximum_elapsed_seconds)
        and rows_per_second >= float(minimum_rows_per_second)
    )
    return {
        "schema_version": BUSINESS_PROFILE_BULK_BENCHMARK_SCHEMA_VERSION,
        "row_count": normalized_count,
        "written_count": written,
        "persisted_count": persisted,
        "elapsed_seconds": elapsed,
        "rows_per_second": rows_per_second,
        "minimum_rows_per_second": float(minimum_rows_per_second),
        "maximum_elapsed_seconds": float(maximum_elapsed_seconds),
        "passed": passed,
    }


def _benchmark_evidence(index: int) -> dict[str, Any]:
    identity = f"benchmark-{index:05d}"
    return {
        "evidence_id": identity,
        "instrument_id": f"{index % 999999:06d}.SZ",
        "source_document_id": f"document-{identity}",
        "source_tier": "official_filing",
        "document_hash": f"document-hash-{identity}",
        "report_period": "2025-12-31",
        "data_available_date": "2026-03-31",
        "availability_quality": "actual",
        "evidence_text_hash": f"text-hash-{identity}",
        "extraction_method": "benchmark_native_parser",
        "confidence": 1.0,
        "review_status": "candidate",
    }
