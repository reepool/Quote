"""
Minimal storage manager for research shadow data.

Phase 0 scope:
- Initialize research.db independently from quotes.db
- Create ingestion_runs and raw_payload_audit tables
- Provide simple write helpers for shadow ingestion
"""

from __future__ import annotations

import json
import os
import sqlite3
import hashlib
import threading
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any, Dict, Generator, List, Optional

from research.change_watermarks import append_change_record, ensure_change_log_schema
from research.announcements.models import (
    AnnouncementRecord,
    AnnouncementScanResult,
    ProviderCursor,
)
from research.financial_fact_aliases import (
    describe_core_financial_fact_alias,
    get_core_financial_fact_derivation_rules,
)
from research.financial_source_field_mapping import FinancialSourceFieldMapping
from research.official_shenwan_mapping import OfficialShenwanCodeMapping
from research.migrations.announcement_legacy_cleanup import (
    create_verified_backup_if_needed,
    migrate_reconcile_and_drop,
)
from utils import db_logger
from utils.config_manager import ResearchConfig, config_manager
from utils.date_utils import get_shanghai_time
from research.providers.base import (
    AnalystForecastSnapshot,
    CompanyProfileSnapshot,
    FinancialFactsSnapshot,
    FinancialIndicatorSnapshot,
    FinancialNumericFactSnapshot,
    FinancialSourceFileManifest,
    FinancialStatementBundle,
    FinancialStatementRawSnapshot,
    FinancialSummarySnapshot,
    IndustryClassificationHistorySnapshot,
    IndustryIndexAnalysisSnapshot,
    IndustrySnapshot,
    IndustrySourceFileSnapshot,
    IndustryTaxonomySnapshot,
    OfficialIndustryClassificationSnapshot,
    OfficialIndustryHistorySnapshot,
    ResearchReportSnapshot,
    RiskSnapshot,
    ShareholderSnapshot,
    SentimentEventSnapshot,
    TechnicalIndicatorLatestSnapshot,
    ValuationHistorySnapshot,
    ValuationInputSnapshot,
)

VALUATION_HISTORY_DETAILS_COMPACT_VERSION = 2

VALUATION_HISTORY_INPUT_DETAIL_KEY_MAP = {
    "source": "s",
    "source_mode": "sm",
    "input_kind": "k",
    "as_of_date": "d",
    "data_as_of": "da",
    "unit": "u",
    "market_cap": "mc",
    "shares_outstanding": "sh",
    "float_market_cap": "fmc",
    "float_shares": "fsh",
    "resolution": "res",
    "missing_reason": "mr",
}

VALUATION_HISTORY_METRIC_DETAIL_KEY_MAP = {
    "value": "v",
    "numerator": "n",
    "denominator": "d",
    "denominator_fact": "df",
    "report_periods": "rp",
    "availability_dates": "ad",
    "calc_method": "cm",
    "missing_reason": "mr",
    "status": "s",
}


@dataclass(frozen=True)
class IngestionRunRecord:
    """Simple value object for ingestion run status."""

    run_id: int
    status: str


class FinancialStatementStorageRepository:
    """Backend-portable facade for financial statement storage operations."""

    def __init__(self, storage: "ResearchStorageManager"):
        self._storage = storage

    def upsert_source_file_manifest(
        self,
        manifest: FinancialSourceFileManifest,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> str:
        with self._storage.financial_database_scope():
            return self._storage.upsert_financial_source_file_manifest(
                manifest,
                ingestion_run_id=ingestion_run_id,
            )

    def get_source_file_manifests(
        self,
        *,
        instrument_id: Optional[str] = None,
        exchange: Optional[str] = None,
        report_period: Optional[str] = None,
        source: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with self._storage.financial_database_scope():
            return self._storage.get_financial_source_file_manifests(
                instrument_id=instrument_id,
                exchange=exchange,
                report_period=report_period,
                source=source,
            )

    def upsert_numeric_facts(
        self,
        facts: List[FinancialNumericFactSnapshot],
        *,
        ingestion_run_id: Optional[int] = None,
        tier: str = "hot",
    ) -> int:
        with self._storage.financial_database_scope():
            return self._storage.upsert_financial_numeric_facts(
                facts,
                ingestion_run_id=ingestion_run_id,
                tier=tier,
            )

    def upsert_source_field_mappings(
        self,
        mappings: List[FinancialSourceFieldMapping],
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> int:
        with self._storage.financial_database_scope():
            return self._storage.upsert_financial_source_field_mappings(
                mappings,
                ingestion_run_id=ingestion_run_id,
            )

    def get_source_field_mappings(
        self,
        *,
        profile: Optional[str] = None,
        approved_for_core: Optional[bool] = None,
        mapping_version: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with self._storage.financial_database_scope():
            return self._storage.get_financial_source_field_mappings(
                profile=profile,
                approved_for_core=approved_for_core,
                mapping_version=mapping_version,
            )

    def upsert_mapping_audit_result(
        self,
        audit_result: Dict[str, Any],
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> str:
        with self._storage.financial_database_scope():
            return self._storage.upsert_financial_mapping_audit_result(
                audit_result,
                ingestion_run_id=ingestion_run_id,
            )

    def get_mapping_audit_results(
        self,
        *,
        profile: Optional[str] = None,
        mapping_version: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        with self._storage.financial_database_scope():
            return self._storage.get_financial_mapping_audit_results(
                profile=profile,
                mapping_version=mapping_version,
                limit=limit,
            )

    def get_local_core_facts(
        self,
        instrument_id: str,
        *,
        report_period: Optional[str] = None,
        requested_canonical_facts: Optional[List[str]] = None,
        profile: Optional[str] = None,
        mapping_version: Optional[str] = None,
        include_history: bool = False,
    ) -> Dict[str, Any]:
        with self._storage.financial_database_scope():
            return self._storage.get_financial_local_core_facts(
                instrument_id,
                report_period=report_period,
                requested_canonical_facts=requested_canonical_facts,
                profile=profile,
                mapping_version=mapping_version,
                include_history=include_history,
            )

    def get_core_facts(
        self,
        instrument_id: str,
        *,
        include_history: bool = False,
        report_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        with self._storage.financial_database_scope():
            return self._storage.get_financial_core_facts(
                instrument_id,
                include_history=include_history,
                report_period=report_period,
                limit=limit,
            )

    def get_numeric_facts(
        self,
        instrument_id: str,
        *,
        include_history: bool = False,
        report_period: Optional[str] = None,
        fact_name: Optional[str] = None,
        canonical_fact_name: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        with self._storage.financial_database_scope():
            return self._storage.get_financial_numeric_facts(
                instrument_id,
                include_history=include_history,
                report_period=report_period,
                fact_name=fact_name,
                canonical_fact_name=canonical_fact_name,
                limit=limit,
            )

    def detect_coverage_gaps(
        self,
        *,
        expected_periods: List[str],
        instrument_ids: List[str],
        required_core_facts: List[str],
        fallback_sources: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        with self._storage.financial_database_scope():
            return self._storage.detect_financial_coverage_gaps(
                expected_periods=expected_periods,
                instrument_ids=instrument_ids,
                required_core_facts=required_core_facts,
                fallback_sources=fallback_sources,
            )

    def validate_readiness(
        self,
        *,
        expected_periods: List[str],
        instrument_ids: List[str],
        required_core_facts: List[str],
        fallback_sources: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        with self._storage.financial_database_scope():
            return self._storage.validate_financial_statement_readiness(
                expected_periods=expected_periods,
                instrument_ids=instrument_ids,
                required_core_facts=required_core_facts,
                fallback_sources=fallback_sources,
            )

    def maintain_tiers(
        self,
        *,
        instrument_id: Optional[str] = None,
        hot_quarter_window: Optional[int] = None,
    ) -> Dict[str, Any]:
        with self._storage.financial_database_scope():
            return self._storage.maintain_financial_hot_cold_tiers(
                instrument_id=instrument_id,
                hot_quarter_window=hot_quarter_window,
            )


class ResearchStorageManager:
    """Manage the isolated research SQLite database."""

    def __init__(self, research_config: Optional[ResearchConfig] = None):
        self.research_config = research_config or config_manager.get_research_config()
        self.db_path = self.research_config.storage.db_path
        self.financials_db_path = self.research_config.storage.financials_db_path
        self.valuation_db_path = self.research_config.storage.valuation_db_path
        if (
            os.path.normpath(self.financials_db_path)
            == os.path.normpath("data/financials.db")
            and os.path.normpath(self.db_path) != os.path.normpath("data/research.db")
        ):
            self.financials_db_path = os.path.join(
                os.path.dirname(self.db_path),
                "financials.db",
            )
            self.research_config.storage.financials_db_path = self.financials_db_path
        if (
            os.path.normpath(self.valuation_db_path)
            == os.path.normpath("data/valuation.db")
            and os.path.normpath(self.db_path) != os.path.normpath("data/research.db")
        ):
            self.valuation_db_path = os.path.join(
                os.path.dirname(self.db_path),
                "valuation.db",
            )
            self.research_config.storage.valuation_db_path = self.valuation_db_path
        self.interests_db_path = getattr(
            self.research_config.storage, "interests_db_path", "data/interests.db"
        )
        if (
            os.path.normpath(self.interests_db_path)
            == os.path.normpath("data/interests.db")
            and os.path.normpath(self.db_path) != os.path.normpath("data/research.db")
        ):
            self.interests_db_path = os.path.join(
                os.path.dirname(self.db_path),
                "interests.db",
            )
            if hasattr(self.research_config.storage, "interests_db_path"):
                self.research_config.storage.interests_db_path = self.interests_db_path
        self.quotes_db_path = self.research_config.storage.quotes_db_path
        self.quotes_db_alias = self.research_config.storage.quotes_db_alias
        self._db_route_state = threading.local()
        self._active_db_path: Optional[str] = None
        self._financial_ingestion_run_ids: set[int] = set()
        self._valuation_ingestion_run_ids: set[int] = set()
        self.change_watermark_config = self.research_config.modules.get(
            "change_watermark", {}
        )
        self.financial_statements = FinancialStatementStorageRepository(self)

    @property
    def _active_db_path(self) -> Optional[str]:
        """Return the active database route for the current worker thread."""
        state = self.__dict__.get("_db_route_state")
        return getattr(state, "active_db_path", None) if state is not None else None

    @_active_db_path.setter
    def _active_db_path(self, value: Optional[str]) -> None:
        # API storage calls run through asyncio.to_thread, so route state must
        # not leak between concurrent workers sharing this manager instance.
        state = self.__dict__.get("_db_route_state")
        if state is None:
            state = threading.local()
            self.__dict__["_db_route_state"] = state
        state.active_db_path = value

    def initialize(self) -> None:
        """Ensure database file, pragmas, and base tables exist."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        announcement_backup = create_verified_backup_if_needed(self.db_path)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            self._create_tables(conn)
            self._migrate_tables(conn)
            cleanup_result = migrate_reconcile_and_drop(
                conn,
                backup=announcement_backup,
                now=get_shanghai_time().isoformat(),
            )
            ensure_change_log_schema(conn)
            if self._uses_separate_financial_database():
                self._drop_financial_tables_from_research_database(conn)
            if self._uses_separate_valuation_database():
                self._drop_valuation_tables_from_research_database(conn)
            if self._uses_separate_interests_database():
                self._drop_interests_tables_from_research_database(conn)

            if self.research_config.storage.attach_quotes_db:
                self._attach_quotes_db(conn)

            conn.commit()
        if cleanup_result is not None:
            db_logger.info(
                "[ResearchStorage] Legacy announcement cleanup completed: db=%s state_rows=%s audit_rows=%s backup=%s",
                self.db_path,
                cleanup_result["legacy_state_rows"],
                cleanup_result["legacy_audit_rows"],
                cleanup_result["backup"]["path"],
            )
        db_logger.info(f"[ResearchStorage] Initialized research database at {self.db_path}")
        if self.financials_db_path and self.financials_db_path != self.db_path:
            os.makedirs(os.path.dirname(self.financials_db_path), exist_ok=True)
            financial_announcement_backup = create_verified_backup_if_needed(
                self.financials_db_path
            )
            with self.financial_database_scope():
                with self.get_connection() as conn:
                    self._apply_pragmas(conn)
                    self._create_tables(conn)
                    self._migrate_tables(conn)
                    migrate_reconcile_and_drop(
                        conn,
                        backup=financial_announcement_backup,
                        now=get_shanghai_time().isoformat(),
                    )
                    ensure_change_log_schema(conn)
                    conn.commit()
            db_logger.info(
                "[ResearchStorage] Initialized financial database at %s",
                self.financials_db_path,
            )
        if self.valuation_db_path and self.valuation_db_path != self.db_path:
            os.makedirs(os.path.dirname(self.valuation_db_path), exist_ok=True)
            valuation_announcement_backup = create_verified_backup_if_needed(
                self.valuation_db_path
            )
            with self.valuation_database_scope():
                with self.get_connection() as conn:
                    self._apply_pragmas(conn)
                    self._create_tables(conn)
                    self._migrate_tables(conn)
                    migrate_reconcile_and_drop(
                        conn,
                        backup=valuation_announcement_backup,
                        now=get_shanghai_time().isoformat(),
                    )
                    ensure_change_log_schema(conn)
                    conn.commit()
            db_logger.info(
                "[ResearchStorage] Initialized valuation database at %s",
                self.valuation_db_path,
            )
        if self.interests_db_path and self.interests_db_path != self.db_path:
            os.makedirs(os.path.dirname(self.interests_db_path), exist_ok=True)
            interests_announcement_backup = create_verified_backup_if_needed(
                self.interests_db_path
            )
            with self.interests_database_scope():
                with self.get_connection() as conn:
                    self._apply_pragmas(conn)
                    self._create_tables(conn)
                    self._migrate_tables(conn)
                    migrate_reconcile_and_drop(
                        conn,
                        backup=interests_announcement_backup,
                        now=get_shanghai_time().isoformat(),
                    )
                    ensure_change_log_schema(conn)
                    conn.commit()
            db_logger.info(
                "[ResearchStorage] Initialized interests database at %s",
                self.interests_db_path,
            )

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Yield a sqlite3 connection for the research database."""
        conn = sqlite3.connect(self._active_db_path or self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def financial_database_scope(self) -> Generator[None, None, None]:
        """Route storage operations inside this block to the financial database."""
        previous_db_path = self._active_db_path
        self._active_db_path = self.financials_db_path or self.db_path
        try:
            yield
        finally:
            self._active_db_path = previous_db_path

    @contextmanager
    def valuation_database_scope(self) -> Generator[None, None, None]:
        """Route storage operations inside this block to the valuation database."""
        previous_db_path = self._active_db_path
        self._active_db_path = self.valuation_db_path or self.db_path
        try:
            yield
        finally:
            self._active_db_path = previous_db_path

    @contextmanager
    def interests_database_scope(self) -> Generator[None, None, None]:
        """Route storage operations inside this block to the interests (rates) database."""
        previous_db_path = self._active_db_path
        self._active_db_path = self.interests_db_path or self.db_path
        try:
            yield
        finally:
            self._active_db_path = previous_db_path

    def active_storage_db_path(self) -> str:
        """Return the SQLite path currently used by storage operations."""
        return self._active_db_path or self.db_path

    def _uses_separate_financial_database(self) -> bool:
        return bool(self.financials_db_path) and (
            os.path.abspath(self.financials_db_path) != os.path.abspath(self.db_path)
        )

    def _uses_separate_valuation_database(self) -> bool:
        return bool(self.valuation_db_path) and (
            os.path.abspath(self.valuation_db_path) != os.path.abspath(self.db_path)
        )

    def _uses_separate_interests_database(self) -> bool:
        return bool(self.interests_db_path) and (
            os.path.abspath(self.interests_db_path) != os.path.abspath(self.db_path)
        )

    @classmethod
    def _drop_financial_tables_from_research_database(
        cls,
        conn: sqlite3.Connection,
    ) -> None:
        """Remove financial-domain physical tables from the non-financial research DB."""
        for table_name in cls._financial_table_names():
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    @classmethod
    def _drop_valuation_tables_from_research_database(
        cls,
        conn: sqlite3.Connection,
    ) -> None:
        """Remove valuation-domain physical tables from the non-valuation research DB."""
        for table_name in cls._valuation_table_names():
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    @classmethod
    def _drop_interests_tables_from_research_database(
        cls,
        conn: sqlite3.Connection,
    ) -> None:
        """Remove interests(rates)-domain physical tables from the non-interests research DB."""
        for table_name in cls._interests_table_names():
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    @staticmethod
    def _interests_table_names() -> tuple[str, ...]:
        return (
            "risk_free_rate_series",
            "risk_free_rate_observations",
        )

    @staticmethod
    def _financial_table_names() -> tuple[str, ...]:
        return (
            "financial_core_facts_history",
            "financial_core_facts_hot",
            "financial_disclosure_event_state",
            "financial_facts",
            "financial_indicator_snapshots",
            "financial_indicator_snapshots_history",
            "financial_indicator_snapshots_hot",
            "financial_numeric_facts",
            "financial_numeric_facts_history",
            "financial_numeric_facts_hot",
            "financial_source_field_mappings",
            "financial_source_files",
            "financial_source_mapping_audits",
            "financial_statements_raw",
            "financial_summaries",
        )

    @staticmethod
    def _valuation_table_names() -> tuple[str, ...]:
        return (
            "valuation_history",
            "valuation_inputs",
        )

    def start_ingestion_run(
        self,
        *,
        domain: str,
        job_name: str,
        market: Optional[str] = None,
        source: Optional[str] = None,
        mode: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Create a new ingestion run row."""
        if self._is_financial_domain(domain) and self._active_db_path is None:
            with self.financial_database_scope():
                return self.start_ingestion_run(
                    domain=domain,
                    job_name=job_name,
                    market=market,
                    source=source,
                    mode=mode,
                    metadata=metadata,
                )
        if self._is_valuation_domain(domain) and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.start_ingestion_run(
                    domain=domain,
                    job_name=job_name,
                    market=market,
                    source=source,
                    mode=mode,
                    metadata=metadata,
                )
        now = get_shanghai_time().isoformat()
        metadata_json = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            cursor = conn.execute(
                """
                INSERT INTO ingestion_runs (
                    domain,
                    job_name,
                    market,
                    source,
                    mode,
                    status,
                    shadow_mode,
                    started_at,
                    metadata_json,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    domain,
                    job_name,
                    market,
                    source,
                    mode,
                    "running",
                    1 if self.research_config.storage.shadow_mode else 0,
                    now,
                    metadata_json,
                    now,
                    now,
                ),
            )
            conn.commit()
            run_id = int(cursor.lastrowid)
            if self._is_financial_domain(domain):
                self._financial_ingestion_run_ids.add(run_id)
            if self._is_valuation_domain(domain):
                self._valuation_ingestion_run_ids.add(run_id)
            return run_id

    def finish_ingestion_run(
        self,
        run_id: int,
        *,
        status: str,
        rows_written: int = 0,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> IngestionRunRecord:
        """Update an ingestion run on completion."""
        if run_id in self._financial_ingestion_run_ids and self._active_db_path is None:
            with self.financial_database_scope():
                return self.finish_ingestion_run(
                    run_id,
                    status=status,
                    rows_written=rows_written,
                    error_message=error_message,
                    metadata=metadata,
                )
        if run_id in self._valuation_ingestion_run_ids and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.finish_ingestion_run(
                    run_id,
                    status=status,
                    rows_written=rows_written,
                    error_message=error_message,
                    metadata=metadata,
                )
        now = get_shanghai_time().isoformat()
        metadata_json = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                UPDATE ingestion_runs
                SET status = ?,
                    rows_written = ?,
                    error_message = ?,
                    metadata_json = ?,
                    completed_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    rows_written,
                    error_message,
                    metadata_json,
                    now,
                    now,
                    run_id,
                ),
            )
            conn.commit()

        return IngestionRunRecord(run_id=run_id, status=status)

    def get_latest_successful_ingestion_run(
        self,
        *,
        domain: str,
        job_name: str,
        market: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return the latest successful ingestion run for checkpointing."""
        if self._is_financial_domain(domain) and self._active_db_path is None:
            with self.financial_database_scope():
                return self.get_latest_successful_ingestion_run(
                    domain=domain,
                    job_name=job_name,
                    market=market,
                )
        if self._is_valuation_domain(domain) and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.get_latest_successful_ingestion_run(
                    domain=domain,
                    job_name=job_name,
                    market=market,
                )
        filters = ["domain = ?", "job_name = ?", "status = ?"]
        params: List[Any] = [domain, job_name, "success"]
        if market is not None:
            filters.append("market = ?")
            params.append(market)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                f"""
                SELECT *
                FROM ingestion_runs
                WHERE {' AND '.join(filters)}
                ORDER BY completed_at DESC, id DESC
                LIMIT 1
                """,
                params,
            ).fetchone()

        if row is None:
            return None
        item = dict(row)
        item["metadata"] = self._deserialize_json(item.pop("metadata_json", None)) or {}
        return item

    def store_raw_payload(
        self,
        *,
        domain: str,
        instrument_id: Optional[str],
        source: str,
        source_mode: str,
        payload: Dict[str, Any],
        payload_hash: str,
        ingestion_run_id: Optional[int] = None,
    ) -> int:
        """Store or replace a raw payload snapshot."""
        if self._is_financial_domain(domain) and self._active_db_path is None:
            with self.financial_database_scope():
                return self.store_raw_payload(
                    domain=domain,
                    instrument_id=instrument_id,
                    source=source,
                    source_mode=source_mode,
                    payload=payload,
                    payload_hash=payload_hash,
                    ingestion_run_id=ingestion_run_id,
                )
        if self._is_valuation_domain(domain) and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.store_raw_payload(
                    domain=domain,
                    instrument_id=instrument_id,
                    source=source,
                    source_mode=source_mode,
                    payload=payload,
                    payload_hash=payload_hash,
                    ingestion_run_id=ingestion_run_id,
                )
        now = get_shanghai_time().isoformat()
        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            cursor = conn.execute(
                """
                INSERT INTO raw_payload_audit (
                    domain,
                    instrument_id,
                    source,
                    source_mode,
                    payload_hash,
                    payload_json,
                    fetched_at,
                    ingestion_run_id,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain, instrument_id, source, payload_hash)
                DO UPDATE SET
                    payload_json = excluded.payload_json,
                    fetched_at = excluded.fetched_at,
                    ingestion_run_id = excluded.ingestion_run_id
                """,
                (
                    domain,
                    instrument_id,
                    source,
                    source_mode,
                    payload_hash,
                    payload_json,
                    now,
                    ingestion_run_id,
                    now,
                ),
            )
            conn.commit()
            return int(cursor.lastrowid or 0)

    @staticmethod
    def _is_financial_domain(domain: Optional[str]) -> bool:
        return str(domain or "").startswith("financial")

    @staticmethod
    def _is_valuation_domain(domain: Optional[str]) -> bool:
        return str(domain or "").startswith("valuation")

    def upsert_company_profile(
        self,
        snapshot: CompanyProfileSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Upsert one normalized company profile snapshot."""
        now = get_shanghai_time().isoformat()
        profile_json = json.dumps(asdict(snapshot), ensure_ascii=False, sort_keys=True)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO company_profiles (
                    instrument_id,
                    symbol,
                    company_name,
                    short_name,
                    exchange,
                    market,
                    listed_date,
                    industry_raw,
                    sector_raw,
                    status,
                    source,
                    source_mode,
                    data_as_of,
                    profile_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    company_name = excluded.company_name,
                    short_name = excluded.short_name,
                    exchange = excluded.exchange,
                    market = excluded.market,
                    listed_date = excluded.listed_date,
                    industry_raw = excluded.industry_raw,
                    sector_raw = excluded.sector_raw,
                    status = excluded.status,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    data_as_of = excluded.data_as_of,
                    profile_json = excluded.profile_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.company_name,
                    snapshot.short_name,
                    snapshot.exchange,
                    snapshot.market,
                    snapshot.listed_date,
                    snapshot.industry_raw,
                    snapshot.sector_raw,
                    snapshot.status,
                    snapshot.source,
                    snapshot.source_mode,
                    now,
                    profile_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            conn.commit()

    def upsert_financial_summary(
        self,
        snapshot: FinancialSummarySnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Upsert one normalized financial summary snapshot."""
        now = get_shanghai_time().isoformat()
        summary_json = json.dumps(snapshot.summary_json, ensure_ascii=False, sort_keys=True)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO financial_summaries (
                    instrument_id,
                    symbol,
                    exchange,
                    report_date,
                    pub_date,
                    fiscal_year,
                    fiscal_quarter,
                    currency,
                    schema_version,
                    roe,
                    gross_margin,
                    net_margin,
                    current_ratio,
                    quick_ratio,
                    liability_to_asset,
                    yoy_asset,
                    yoy_equity,
                    yoy_net_profit,
                    cfo_to_revenue,
                    cfo_to_net_profit,
                    asset_turnover,
                    eps,
                    source,
                    source_mode,
                    data_as_of,
                    summary_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    report_date = excluded.report_date,
                    pub_date = excluded.pub_date,
                    fiscal_year = excluded.fiscal_year,
                    fiscal_quarter = excluded.fiscal_quarter,
                    currency = excluded.currency,
                    schema_version = excluded.schema_version,
                    roe = excluded.roe,
                    gross_margin = excluded.gross_margin,
                    net_margin = excluded.net_margin,
                    current_ratio = excluded.current_ratio,
                    quick_ratio = excluded.quick_ratio,
                    liability_to_asset = excluded.liability_to_asset,
                    yoy_asset = excluded.yoy_asset,
                    yoy_equity = excluded.yoy_equity,
                    yoy_net_profit = excluded.yoy_net_profit,
                    cfo_to_revenue = excluded.cfo_to_revenue,
                    cfo_to_net_profit = excluded.cfo_to_net_profit,
                    asset_turnover = excluded.asset_turnover,
                    eps = excluded.eps,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    data_as_of = excluded.data_as_of,
                    summary_json = excluded.summary_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.report_date,
                    snapshot.pub_date,
                    snapshot.fiscal_year,
                    snapshot.fiscal_quarter,
                    snapshot.currency,
                    snapshot.schema_version,
                    snapshot.roe,
                    snapshot.gross_margin,
                    snapshot.net_margin,
                    snapshot.current_ratio,
                    snapshot.quick_ratio,
                    snapshot.liability_to_asset,
                    snapshot.yoy_asset,
                    snapshot.yoy_equity,
                    snapshot.yoy_net_profit,
                    snapshot.cfo_to_revenue,
                    snapshot.cfo_to_net_profit,
                    snapshot.asset_turnover,
                    snapshot.eps,
                    snapshot.source,
                    snapshot.source_mode,
                    now,
                    summary_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            conn.commit()

    def upsert_shareholder_snapshot(
        self,
        snapshot: ShareholderSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
        return_stats: bool = False,
    ) -> Optional[Dict[str, int]]:
        """Upsert one normalized shareholder summary snapshot."""
        now = get_shanghai_time().isoformat()
        snapshot_json = json.dumps(
            snapshot.snapshot_json,
            ensure_ascii=False,
            sort_keys=True,
        )
        semantic_payload = {
            "instrument_id": snapshot.instrument_id,
            "symbol": snapshot.symbol,
            "exchange": snapshot.exchange,
            "coverage_status": snapshot.coverage_status,
            "holder_count": snapshot.holder_count,
            "holder_count_report_date": snapshot.holder_count_report_date,
            "top_holders_report_date": snapshot.top_holders_report_date,
            "top_holders_count": snapshot.top_holders_count,
            "top_holders_total_ratio": snapshot.top_holders_total_ratio,
            "control_owner_name": snapshot.control_owner_name,
            "control_owner_ratio": snapshot.control_owner_ratio,
            "schema_version": snapshot.schema_version,
            "source": snapshot.source,
            "source_mode": snapshot.source_mode,
            "snapshot": snapshot.snapshot_json,
        }
        row_hash = hashlib.sha256(
            json.dumps(
                semantic_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        ).hexdigest()

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            existing = conn.execute(
                """
                SELECT *
                FROM shareholder_snapshots
                WHERE instrument_id = ?
                """,
                (snapshot.instrument_id,),
            ).fetchone()
            old_hash = None
            if existing is not None:
                old_snapshot = self._deserialize_json(existing["snapshot_json"]) or {}
                old_payload = {
                    key: existing[key]
                    for key in (
                        "instrument_id",
                        "symbol",
                        "exchange",
                        "coverage_status",
                        "holder_count",
                        "holder_count_report_date",
                        "top_holders_report_date",
                        "top_holders_count",
                        "top_holders_total_ratio",
                        "control_owner_name",
                        "control_owner_ratio",
                        "schema_version",
                        "source",
                        "source_mode",
                    )
                }
                old_payload["snapshot"] = old_snapshot
                old_hash = existing["row_hash"] or hashlib.sha256(
                    json.dumps(
                        old_payload,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        default=str,
                    ).encode("utf-8")
                ).hexdigest()
                if old_hash == row_hash:
                    if not existing["row_hash"]:
                        conn.execute(
                            """
                            UPDATE shareholder_snapshots
                            SET row_hash = ?
                            WHERE instrument_id = ?
                            """,
                            (row_hash, snapshot.instrument_id),
                        )
                        conn.commit()
                    stats = {
                        "inserted": 0,
                        "changed": 0,
                        "unchanged": 1,
                        "changelog_written": 0,
                    }
                    return stats if return_stats else None
            row_version = int(existing["row_version"] or 1) + 1 if existing else 1
            conn.execute(
                """
                INSERT INTO shareholder_snapshots (
                    instrument_id,
                    symbol,
                    exchange,
                    coverage_status,
                    holder_count,
                    holder_count_report_date,
                    top_holders_report_date,
                    top_holders_count,
                    top_holders_total_ratio,
                    control_owner_name,
                    control_owner_ratio,
                    schema_version,
                    source,
                    source_mode,
                    data_as_of,
                    snapshot_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    coverage_status = excluded.coverage_status,
                    holder_count = excluded.holder_count,
                    holder_count_report_date = excluded.holder_count_report_date,
                    top_holders_report_date = excluded.top_holders_report_date,
                    top_holders_count = excluded.top_holders_count,
                    top_holders_total_ratio = excluded.top_holders_total_ratio,
                    control_owner_name = excluded.control_owner_name,
                    control_owner_ratio = excluded.control_owner_ratio,
                    schema_version = excluded.schema_version,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    data_as_of = excluded.data_as_of,
                    snapshot_json = excluded.snapshot_json,
                    row_hash = excluded.row_hash,
                    row_version = excluded.row_version,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.coverage_status,
                    snapshot.holder_count,
                    snapshot.holder_count_report_date,
                    snapshot.top_holders_report_date,
                    snapshot.top_holders_count,
                    snapshot.top_holders_total_ratio,
                    snapshot.control_owner_name,
                    snapshot.control_owner_ratio,
                    snapshot.schema_version,
                    snapshot.source,
                    snapshot.source_mode,
                    now,
                    snapshot_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            changelog_written = append_change_record(
                conn,
                config=self.change_watermark_config,
                domain="shareholder",
                dataset="shareholder_snapshots",
                change_type="update" if existing else "insert",
                business_key={
                    "instrument_id": snapshot.instrument_id,
                    "holder_count_report_date": snapshot.holder_count_report_date,
                    "top_holders_report_date": snapshot.top_holders_report_date,
                    "coverage_status": snapshot.coverage_status,
                },
                instrument_id=snapshot.instrument_id,
                period=snapshot.top_holders_report_date or snapshot.holder_count_report_date,
                old_hash=old_hash,
                new_hash=row_hash,
                row_version=row_version,
                source=snapshot.source,
                source_mode=snapshot.source_mode,
                source_profile=snapshot.schema_version,
                ingestion_run_id=ingestion_run_id,
                changed_at=now,
            )
            conn.commit()
        stats = {
            "inserted": int(existing is None),
            "changed": int(existing is not None),
            "unchanged": 0,
            "changelog_written": int(changelog_written),
        }
        return stats if return_stats else None

    def upsert_financial_statement_bundle(
        self,
        bundle: FinancialStatementBundle,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Upsert one latest financial statement bundle."""
        for raw_statement in bundle.raw_statements:
            self.upsert_financial_statement_raw(
                raw_statement,
                ingestion_run_id=ingestion_run_id,
            )

        if bundle.facts is not None:
            self.upsert_financial_facts(
                bundle.facts,
                ingestion_run_id=ingestion_run_id,
            )

        if bundle.indicators is not None:
            self.upsert_financial_indicator_snapshot(
                bundle.indicators,
                ingestion_run_id=ingestion_run_id,
            )

    def upsert_financial_statement_raw(
        self,
        snapshot: FinancialStatementRawSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Upsert one raw financial statement row."""
        now = get_shanghai_time().isoformat()
        statement_json = json.dumps(snapshot.statement_json, ensure_ascii=False, sort_keys=True)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO financial_statements_raw (
                    instrument_id,
                    symbol,
                    exchange,
                    statement_type,
                    report_period,
                    publish_date,
                    fiscal_year,
                    fiscal_quarter,
                    currency,
                    schema_version,
                    source,
                    source_mode,
                    data_as_of,
                    statement_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id, statement_type, report_period)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    publish_date = excluded.publish_date,
                    fiscal_year = excluded.fiscal_year,
                    fiscal_quarter = excluded.fiscal_quarter,
                    currency = excluded.currency,
                    schema_version = excluded.schema_version,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    data_as_of = excluded.data_as_of,
                    statement_json = excluded.statement_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.statement_type,
                    snapshot.report_period,
                    snapshot.publish_date,
                    snapshot.fiscal_year,
                    snapshot.fiscal_quarter,
                    snapshot.currency,
                    snapshot.schema_version,
                    snapshot.source,
                    snapshot.source_mode,
                    now,
                    statement_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            conn.commit()

    def upsert_financial_source_file_manifest(
        self,
        manifest: FinancialSourceFileManifest,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> str:
        """Upsert one financial source-file manifest and return its stable id."""
        now = get_shanghai_time().isoformat()
        source_file_id = manifest.source_file_id or self._build_financial_source_file_id(
            manifest
        )
        parser_diagnostics_json = json.dumps(
            manifest.parser_diagnostics,
            ensure_ascii=False,
            sort_keys=True,
        )
        metadata_json = json.dumps(
            manifest.metadata_json,
            ensure_ascii=False,
            sort_keys=True,
        )

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO financial_source_files (
                    source_file_id,
                    instrument_id,
                    symbol,
                    exchange,
                    report_period,
                    report_type,
                    filing_id,
                    source_url,
                    archive_path,
                    content_hash,
                    content_length,
                    published_at,
                    downloaded_at,
                    parser_version,
                    parser_diagnostics_json,
                    schema_version,
                    source,
                    source_mode,
                    source_tier,
                    status,
                    supersedes_source_file_id,
                    metadata_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_file_id)
                DO UPDATE SET
                    instrument_id = excluded.instrument_id,
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    report_period = excluded.report_period,
                    report_type = excluded.report_type,
                    filing_id = excluded.filing_id,
                    source_url = excluded.source_url,
                    archive_path = excluded.archive_path,
                    content_hash = excluded.content_hash,
                    content_length = excluded.content_length,
                    published_at = excluded.published_at,
                    downloaded_at = excluded.downloaded_at,
                    parser_version = excluded.parser_version,
                    parser_diagnostics_json = excluded.parser_diagnostics_json,
                    schema_version = excluded.schema_version,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    source_tier = excluded.source_tier,
                    status = excluded.status,
                    supersedes_source_file_id = excluded.supersedes_source_file_id,
                    metadata_json = excluded.metadata_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    source_file_id,
                    manifest.instrument_id,
                    manifest.symbol,
                    manifest.exchange,
                    manifest.report_period,
                    manifest.report_type,
                    manifest.filing_id,
                    manifest.source_url,
                    manifest.archive_path,
                    manifest.content_hash,
                    manifest.content_length,
                    manifest.published_at,
                    manifest.downloaded_at,
                    manifest.parser_version,
                    parser_diagnostics_json,
                    manifest.schema_version,
                    manifest.source,
                    manifest.source_mode,
                    manifest.source_tier,
                    manifest.status,
                    manifest.supersedes_source_file_id,
                    metadata_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            conn.commit()
        return source_file_id

    def upsert_financial_source_field_mappings(
        self,
        mappings: List[FinancialSourceFieldMapping],
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> int:
        """Persist versioned source-field mapping decisions as long-form rows."""
        if not mappings:
            return 0
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            for mapping in mappings:
                mapping_json = json.dumps(
                    mapping.to_dict(),
                    ensure_ascii=False,
                    sort_keys=True,
                )
                conn.execute(
                    """
                    INSERT INTO financial_source_field_mappings (
                        mapping_version,
                        profile,
                        canonical_fact,
                        statement_family,
                        sina_field,
                        ths_metric,
                        relationship,
                        source_unit,
                        canonical_unit,
                        unit_multiplier,
                        value_type,
                        approved_for_core,
                        semantic,
                        rejection_reason,
                        mapping_json,
                        ingestion_run_id,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(mapping_version, profile, canonical_fact, sina_field, ths_metric)
                    DO UPDATE SET
                        statement_family = excluded.statement_family,
                        relationship = excluded.relationship,
                        source_unit = excluded.source_unit,
                        canonical_unit = excluded.canonical_unit,
                        unit_multiplier = excluded.unit_multiplier,
                        value_type = excluded.value_type,
                        approved_for_core = excluded.approved_for_core,
                        semantic = excluded.semantic,
                        rejection_reason = excluded.rejection_reason,
                        mapping_json = excluded.mapping_json,
                        ingestion_run_id = excluded.ingestion_run_id,
                        updated_at = excluded.updated_at
                    """,
                    (
                        mapping.mapping_version,
                        mapping.profile,
                        mapping.canonical_fact,
                        mapping.statement_family,
                        mapping.sina_field,
                        mapping.ths_metric,
                        mapping.relationship,
                        mapping.source_unit,
                        mapping.canonical_unit,
                        mapping.unit_multiplier,
                        mapping.value_type,
                        1 if mapping.approved_for_core else 0,
                        mapping.semantic,
                        mapping.rejection_reason,
                        mapping_json,
                        ingestion_run_id,
                        now,
                        now,
                    ),
                )
            conn.commit()
        return len(mappings)

    def get_financial_source_field_mappings(
        self,
        *,
        profile: Optional[str] = None,
        approved_for_core: Optional[bool] = None,
        mapping_version: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Read persisted source-field mapping decisions."""
        clauses: List[str] = []
        params: List[Any] = []
        if profile is not None:
            clauses.append("profile = ?")
            params.append(profile)
        if approved_for_core is not None:
            clauses.append("approved_for_core = ?")
            params.append(1 if approved_for_core else 0)
        if mapping_version is not None:
            clauses.append("mapping_version = ?")
            params.append(mapping_version)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT *
                FROM financial_source_field_mappings
                {where_sql}
                ORDER BY mapping_version, profile, statement_family, canonical_fact, sina_field
                """,
                params,
            ).fetchall()
        return [self._row_to_financial_source_field_mapping(row) for row in rows]

    def upsert_financial_mapping_audit_result(
        self,
        audit_result: Dict[str, Any],
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> str:
        """Persist one bounded mapping audit result as JSON plus queryable summary."""
        now = get_shanghai_time().isoformat()
        mapping_version = str(audit_result.get("mapping_version") or "")
        profile = str(audit_result.get("profile") or "")
        instrument_id = audit_result.get("instrument_id")
        report_period = audit_result.get("report_period")
        status = str(audit_result.get("status") or "unknown")
        summary = audit_result.get("summary") or {}
        audit_json = json.dumps(audit_result, ensure_ascii=False, sort_keys=True)
        summary_json = json.dumps(summary, ensure_ascii=False, sort_keys=True)
        audit_hash = hashlib.sha256(audit_json.encode("utf-8")).hexdigest()
        audit_id = str(audit_result.get("audit_id") or f"{mapping_version}:{profile}:{audit_hash}")

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO financial_source_mapping_audits (
                    audit_id,
                    mapping_version,
                    profile,
                    instrument_id,
                    report_period,
                    status,
                    summary_json,
                    audit_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(audit_id)
                DO UPDATE SET
                    mapping_version = excluded.mapping_version,
                    profile = excluded.profile,
                    instrument_id = excluded.instrument_id,
                    report_period = excluded.report_period,
                    status = excluded.status,
                    summary_json = excluded.summary_json,
                    audit_json = excluded.audit_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    audit_id,
                    mapping_version,
                    profile,
                    instrument_id,
                    report_period,
                    status,
                    summary_json,
                    audit_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            conn.commit()
        return audit_id

    def get_financial_mapping_audit_results(
        self,
        *,
        profile: Optional[str] = None,
        mapping_version: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Read bounded source-field mapping audit results."""
        clauses: List[str] = []
        params: List[Any] = []
        if profile is not None:
            clauses.append("profile = ?")
            params.append(profile)
        if mapping_version is not None:
            clauses.append("mapping_version = ?")
            params.append(mapping_version)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(int(limit), 1))
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT *
                FROM financial_source_mapping_audits
                {where_sql}
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_financial_mapping_audit(row) for row in rows]

    def _row_to_financial_source_field_mapping(self, row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["approved_for_core"] = bool(item.get("approved_for_core"))
        item["mapping"] = self._deserialize_json(item.pop("mapping_json", None)) or {}
        return item

    def _row_to_financial_mapping_audit(self, row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["summary"] = self._deserialize_json(item.pop("summary_json", None)) or {}
        item["audit"] = self._deserialize_json(item.pop("audit_json", None)) or {}
        return item

    @staticmethod
    def _numeric_fact_has_local_core_lineage(
        row: Dict[str, Any],
        *,
        profile: Optional[str],
        mapping_version: Optional[str],
    ) -> bool:
        raw_fact = row.get("raw_fact") or {}
        lineage = raw_fact.get("local_core_mapping") or {}
        if not bool(lineage.get("approved_for_core")):
            return False
        if mapping_version is not None and lineage.get("mapping_version") != mapping_version:
            return False
        if profile is not None and profile not in set(lineage.get("profiles") or []):
            return False
        return True

    def get_financial_source_file_manifests(
        self,
        *,
        instrument_id: Optional[str] = None,
        exchange: Optional[str] = None,
        report_period: Optional[str] = None,
        source: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch financial source-file manifests with optional filters."""
        filters = []
        params: List[Any] = []
        if instrument_id:
            filters.append("instrument_id = ?")
            params.append(instrument_id)
        if exchange:
            filters.append("exchange = ?")
            params.append(exchange)
        if report_period:
            filters.append("report_period = ?")
            params.append(report_period)
        if source:
            filters.append("source = ?")
            params.append(source)
        where_clause = "" if not filters else "WHERE " + " AND ".join(filters)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT *
                FROM financial_source_files
                {where_clause}
                ORDER BY report_period DESC, updated_at DESC
                """,
                params,
            ).fetchall()

        manifests: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["parser_diagnostics"] = self._deserialize_json(
                item.pop("parser_diagnostics_json", None)
            ) or {}
            item["metadata"] = self._deserialize_json(
                item.pop("metadata_json", None)
            ) or {}
            manifests.append(item)
        return manifests

    def upsert_financial_numeric_facts(
        self,
        facts: List[FinancialNumericFactSnapshot],
        *,
        ingestion_run_id: Optional[int] = None,
        tier: str = "hot",
    ) -> int:
        """Upsert long-form numeric facts into the canonical and tier tables."""
        if not facts:
            return 0
        if tier not in {"hot", "history"}:
            raise ValueError("financial numeric fact tier must be 'hot' or 'history'")

        now = get_shanghai_time().isoformat()
        tier_table = (
            "financial_numeric_facts_hot"
            if tier == "hot"
            else "financial_numeric_facts_history"
        )
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            for fact in facts:
                self._upsert_financial_numeric_fact_row(
                    conn,
                    fact,
                    table_name=tier_table,
                    ingestion_run_id=ingestion_run_id,
                    change_watermark_config=self.change_watermark_config,
                    now=now,
                )
            conn.commit()
        return len(facts)

    def replace_financial_numeric_facts_for_source_file(
        self,
        source_file_id: str,
        facts: List[FinancialNumericFactSnapshot],
        *,
        ingestion_run_id: Optional[int] = None,
        tier: str = "hot",
        parser_version: Optional[str] = None,
        statement_family: Optional[str] = None,
    ) -> Dict[str, int]:
        """Replace parsed numeric facts for one source file in a physical tier table.

        This is intended for parser repair runs where newly rejected facts must
        remove stale rows that an earlier parser version wrote for the same
        source file.
        """
        if tier not in {"hot", "history"}:
            raise ValueError("financial numeric fact tier must be 'hot' or 'history'")
        table_name = (
            "financial_numeric_facts_hot"
            if tier == "hot"
            else "financial_numeric_facts_history"
        )
        now = get_shanghai_time().isoformat()
        filters = ["source_file_id = ?"]
        params: List[Any] = [source_file_id]
        if parser_version:
            filters.append("parser_version = ?")
            params.append(parser_version)
        if statement_family:
            filters.append("statement_family = ?")
            params.append(statement_family)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            existing_rows = conn.execute(
                f"SELECT * FROM {table_name} WHERE {' AND '.join(filters)}",
                params,
            ).fetchall()
            incoming_keys = {
                (
                    fact.source_file_id,
                    fact.fact_name,
                    fact.context_id or "",
                    fact.unit or "",
                    self._financial_dimensions_hash(fact.dimensions_json),
                )
                for fact in facts
            }
            for fact in facts:
                self._upsert_financial_numeric_fact_row(
                    conn,
                    fact,
                    table_name=table_name,
                    ingestion_run_id=ingestion_run_id,
                    change_watermark_config=self.change_watermark_config,
                    now=now,
                )
            deleted = 0
            for row in existing_rows:
                row_key = (
                    row["source_file_id"],
                    row["fact_name"],
                    row["context_id"],
                    row["unit"],
                    row["dimensions_hash"],
                )
                if row_key in incoming_keys:
                    continue
                cursor = conn.execute(
                    f"""
                    DELETE FROM {table_name}
                    WHERE source_file_id = ? AND fact_name = ?
                      AND context_id = ? AND unit = ? AND dimensions_hash = ?
                    """,
                    row_key,
                )
                deleted += int(cursor.rowcount or 0)
                old_hash = self._financial_numeric_fact_row_hash(row)
                append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="financial",
                    dataset="financial_numeric_facts",
                    change_type="delete_marker",
                    business_key={
                        "source_file_id": row["source_file_id"],
                        "instrument_id": row["instrument_id"],
                        "report_period": row["report_period"],
                        "fact_name": row["fact_name"],
                        "canonical_fact_name": row["canonical_fact_name"],
                        "context_id": row["context_id"],
                        "unit": row["unit"],
                        "dimensions_hash": row["dimensions_hash"],
                    },
                    instrument_id=row["instrument_id"],
                    period=row["report_period"],
                    old_hash=old_hash,
                    row_version=int(row["row_version"] or 1) + 1,
                    source=row["source"],
                    source_mode=row["source_mode"],
                    source_profile=row["parser_version"],
                    ingestion_run_id=ingestion_run_id,
                    changed_at=now,
                )
            conn.commit()
        return {"deleted": deleted, "inserted": len(facts)}

    def get_financial_numeric_facts(
        self,
        instrument_id: str,
        *,
        include_history: bool = False,
        report_period: Optional[str] = None,
        fact_name: Optional[str] = None,
        canonical_fact_name: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch long-form financial numeric facts through logical tier semantics."""
        tables = ["financial_numeric_facts_hot"]
        if include_history:
            tables.append("financial_numeric_facts_history")
        select_sql = " UNION ALL ".join(
            f"SELECT *, '{table_name}' AS physical_table FROM {table_name}"
            for table_name in tables
        )
        filters = ["instrument_id = ?"]
        params: List[Any] = [instrument_id]
        if report_period:
            filters.append("report_period = ?")
            params.append(report_period)
        if fact_name:
            filters.append("fact_name = ?")
            params.append(fact_name)
        if canonical_fact_name:
            filters.append("canonical_fact_name = ?")
            params.append(canonical_fact_name)
        limit_clause = "" if limit is None else "LIMIT ?"
        if limit is not None:
            params.append(int(limit))

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT *
                FROM ({select_sql})
                WHERE {' AND '.join(filters)}
                ORDER BY report_period DESC, fact_name ASC, updated_at DESC
                {limit_clause}
                """,
                params,
            ).fetchall()

        return [self._decode_financial_numeric_fact_row(row) for row in rows]

    def get_financial_local_core_facts(
        self,
        instrument_id: str,
        *,
        report_period: Optional[str] = None,
        requested_canonical_facts: Optional[List[str]] = None,
        profile: Optional[str] = None,
        mapping_version: Optional[str] = None,
        include_history: bool = False,
    ) -> Dict[str, Any]:
        """Read approved local-core facts with structured missing-field diagnostics."""
        approved_mappings = self.get_financial_source_field_mappings(
            profile=profile,
            approved_for_core=True,
            mapping_version=mapping_version,
        )
        approved_core_facts = sorted(
            {
                str(mapping.get("canonical_fact") or "")
                for mapping in approved_mappings
                if str(mapping.get("canonical_fact") or "")
            }
        )
        requested = (
            [str(item) for item in requested_canonical_facts if str(item)]
            if requested_canonical_facts is not None
            else list(approved_core_facts)
        )
        rows = self.get_financial_numeric_facts(
            instrument_id,
            include_history=include_history,
            report_period=report_period,
        )
        local_rows = [
            row
            for row in rows
            if self._numeric_fact_has_local_core_lineage(
                row,
                profile=profile,
                mapping_version=mapping_version,
            )
        ]
        if report_period is None and local_rows:
            selected_period = max(str(row["report_period"]) for row in local_rows)
            local_rows = [
                row for row in local_rows if str(row["report_period"]) == selected_period
            ]
        else:
            selected_period = report_period

        facts_by_canonical: Dict[str, Dict[str, Any]] = {}
        for row in local_rows:
            canonical_fact = str(row.get("canonical_fact_name") or "")
            if not canonical_fact or canonical_fact not in requested:
                continue
            facts_by_canonical.setdefault(canonical_fact, row)

        missing_fields = []
        if not approved_core_facts:
            missing_fields.append(
                {
                    "canonical_fact": None,
                    "reason": "mapping_catalog_empty",
                    "mapping_version": mapping_version,
                    "profile": profile,
                }
            )
        for canonical_fact in requested:
            if canonical_fact not in approved_core_facts:
                missing_fields.append(
                    {
                        "canonical_fact": canonical_fact,
                        "reason": "outside_approved_local_core",
                        "mapping_version": mapping_version,
                        "profile": profile,
                    }
                )
                continue
            if canonical_fact not in facts_by_canonical:
                missing_fields.append(
                    {
                        "canonical_fact": canonical_fact,
                        "reason": "missing_local_core_fact",
                        "mapping_version": mapping_version,
                        "profile": profile,
                        "report_period": selected_period,
                    }
                )

        return {
            "instrument_id": instrument_id,
            "report_period": selected_period,
            "profile": profile,
            "mapping_version": mapping_version,
            "requested_canonical_facts": requested,
            "approved_canonical_facts": approved_core_facts,
            "facts": facts_by_canonical,
            "missing_fields": missing_fields,
            "ready": not missing_fields,
        }

    def upsert_financial_facts(
        self,
        snapshot: FinancialFactsSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
        return_stats: bool = False,
    ) -> Optional[Dict[str, int]]:
        """Upsert one normalized financial facts row."""
        now = get_shanghai_time().isoformat()
        facts_json = json.dumps(snapshot.facts_json, ensure_ascii=False, sort_keys=True)
        semantic_payload = asdict(snapshot)
        semantic_payload["data_available_date"] = (
            snapshot.data_available_date or snapshot.publish_date
        )
        row_hash = self._semantic_hash(semantic_payload)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            existing = conn.execute(
                """
                SELECT * FROM financial_facts
                WHERE instrument_id = ? AND report_period = ?
                """,
                (snapshot.instrument_id, snapshot.report_period),
            ).fetchone()
            old_hash = None
            if existing is not None:
                old_payload = dict(existing)
                old_payload["facts_json"] = self._deserialize_json(
                    old_payload.get("facts_json")
                ) or {}
                old_payload["lineage_json"] = self._deserialize_json(
                    old_payload.get("lineage_json")
                ) or {}
                for key in (
                    "data_as_of",
                    "ingestion_run_id",
                    "created_at",
                    "updated_at",
                    "row_hash",
                    "row_version",
                ):
                    old_payload.pop(key, None)
                old_hash = existing["row_hash"] or self._semantic_hash(old_payload)
            row_version = int(existing["row_version"] or 1) + 1 if existing and old_hash != row_hash else 1
            if existing and old_hash == row_hash:
                row_version = int(existing["row_version"] or 1)
            self._upsert_financial_core_fact_row(
                conn,
                snapshot,
                table_name="financial_facts",
                facts_json=facts_json,
                row_hash=row_hash,
                row_version=row_version,
                ingestion_run_id=ingestion_run_id,
                now=now,
            )
            self._upsert_financial_core_fact_row(
                conn,
                snapshot,
                table_name="financial_core_facts_hot",
                facts_json=facts_json,
                row_hash=row_hash,
                row_version=row_version,
                ingestion_run_id=ingestion_run_id,
                now=now,
            )
            changelog_written = False
            if existing is None or old_hash != row_hash:
                lineage = snapshot.lineage_json or {}
                changelog_written = append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="financial",
                    dataset="financial_facts",
                    change_type="update" if existing else "insert",
                    business_key={
                        "instrument_id": snapshot.instrument_id,
                        "report_period": snapshot.report_period,
                        "statement_family": snapshot.statement_family,
                        "source_file_id": snapshot.source_file_id,
                    },
                    instrument_id=snapshot.instrument_id,
                    period=snapshot.report_period,
                    old_hash=old_hash,
                    new_hash=row_hash,
                    row_version=row_version,
                    source=snapshot.source,
                    source_mode=snapshot.source_mode,
                    source_profile=str(
                        lineage.get("mapping_version")
                        or lineage.get("parser_version")
                        or snapshot.schema_version
                    ),
                    ingestion_run_id=ingestion_run_id,
                    changed_at=now,
                )
            conn.commit()
        stats = {
            "inserted": int(existing is None),
            "changed": int(existing is not None and old_hash != row_hash),
            "unchanged": int(existing is not None and old_hash == row_hash),
            "changelog_written": int(changelog_written),
        }
        return stats if return_stats else None

    def upsert_financial_indicator_snapshot(
        self,
        snapshot: FinancialIndicatorSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Upsert one derived financial indicator row."""
        now = get_shanghai_time().isoformat()
        indicators_json = json.dumps(
            snapshot.indicators_json,
            ensure_ascii=False,
            sort_keys=True,
        )

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO financial_indicator_snapshots (
                    instrument_id,
                    symbol,
                    exchange,
                    report_period,
                    publish_date,
                    fiscal_year,
                    fiscal_quarter,
                    currency,
                    schema_version,
                    gross_margin,
                    operating_margin,
                    net_margin,
                    roe,
                    roa,
                    current_ratio,
                    quick_ratio,
                    asset_liability_ratio,
                    revenue_per_share,
                    operating_cf_to_revenue,
                    operating_cf_to_net_income,
                    book_value_per_share,
                    source,
                    source_mode,
                    data_as_of,
                    indicators_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id, report_period)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    publish_date = excluded.publish_date,
                    fiscal_year = excluded.fiscal_year,
                    fiscal_quarter = excluded.fiscal_quarter,
                    currency = excluded.currency,
                    schema_version = excluded.schema_version,
                    gross_margin = excluded.gross_margin,
                    operating_margin = excluded.operating_margin,
                    net_margin = excluded.net_margin,
                    roe = excluded.roe,
                    roa = excluded.roa,
                    current_ratio = excluded.current_ratio,
                    quick_ratio = excluded.quick_ratio,
                    asset_liability_ratio = excluded.asset_liability_ratio,
                    revenue_per_share = excluded.revenue_per_share,
                    operating_cf_to_revenue = excluded.operating_cf_to_revenue,
                    operating_cf_to_net_income = excluded.operating_cf_to_net_income,
                    book_value_per_share = excluded.book_value_per_share,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    data_as_of = excluded.data_as_of,
                    indicators_json = excluded.indicators_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.report_period,
                    snapshot.publish_date,
                    snapshot.fiscal_year,
                    snapshot.fiscal_quarter,
                    snapshot.currency,
                    snapshot.schema_version,
                    snapshot.gross_margin,
                    snapshot.operating_margin,
                    snapshot.net_margin,
                    snapshot.roe,
                    snapshot.roa,
                    snapshot.current_ratio,
                    snapshot.quick_ratio,
                    snapshot.asset_liability_ratio,
                    snapshot.revenue_per_share,
                    snapshot.operating_cf_to_revenue,
                    snapshot.operating_cf_to_net_income,
                    snapshot.book_value_per_share,
                    snapshot.source,
                    snapshot.source_mode,
                    now,
                    indicators_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            self._upsert_financial_indicator_row(
                conn,
                snapshot,
                table_name="financial_indicator_snapshots_hot",
                indicators_json=indicators_json,
                ingestion_run_id=ingestion_run_id,
                now=now,
            )
            conn.commit()

    def upsert_valuation_history(
        self,
        snapshot: ValuationHistorySnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
        return_stats: bool = False,
    ) -> Optional[Dict[str, int]]:
        """Upsert one derived valuation history row."""
        if self._uses_separate_valuation_database() and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.upsert_valuation_history(
                    snapshot,
                    ingestion_run_id=ingestion_run_id,
                    return_stats=return_stats,
                )
        now = get_shanghai_time().isoformat()
        details_json = self._valuation_history_details_json(snapshot.details_json)
        semantic_payload = asdict(snapshot)
        semantic_payload["details_json"] = self._deserialize_json(details_json) or {}
        row_hash = self._semantic_hash(semantic_payload)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            existing = conn.execute(
                """
                SELECT * FROM valuation_history
                WHERE instrument_id = ? AND as_of_date = ? AND calc_method = ?
                  AND calc_version = ? AND parameter_hash = ?
                """,
                (
                    snapshot.instrument_id,
                    snapshot.as_of_date,
                    snapshot.calc_method,
                    snapshot.calc_version,
                    snapshot.parameter_hash,
                ),
            ).fetchone()
            old_hash = None
            if existing is not None:
                old_payload = dict(existing)
                old_payload["details_json"] = self._deserialize_json(
                    old_payload.get("details_json")
                ) or {}
                for key in (
                    "data_as_of",
                    "row_hash",
                    "row_version",
                    "ingestion_run_id",
                    "created_at",
                    "updated_at",
                ):
                    old_payload.pop(key, None)
                old_hash = existing["row_hash"] or self._semantic_hash(old_payload)
            changed = existing is None or old_hash != row_hash
            row_version = (
                1
                if existing is None
                else int(existing["row_version"] or 1) + int(changed)
            )
            conn.execute(
                """
                INSERT INTO valuation_history (
                    instrument_id,
                    symbol,
                    exchange,
                    as_of_date,
                    currency,
                    close_price,
                    market_cap,
                    float_market_cap,
                    pe_ratio,
                    pb_ratio,
                    ps_ratio,
                    pe_static,
                    pe_ttm,
                    pe_forward,
                    pb_mrq,
                    ps_static,
                    ps_ttm,
                    ps_forward,
                    calc_method,
                    calc_version,
                    parameter_hash,
                    source,
                    source_mode,
                    data_as_of,
                    details_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id, as_of_date, calc_method, calc_version, parameter_hash)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    currency = excluded.currency,
                    close_price = excluded.close_price,
                    market_cap = excluded.market_cap,
                    float_market_cap = excluded.float_market_cap,
                    pe_ratio = excluded.pe_ratio,
                    pb_ratio = excluded.pb_ratio,
                    ps_ratio = excluded.ps_ratio,
                    pe_static = excluded.pe_static,
                    pe_ttm = excluded.pe_ttm,
                    pe_forward = excluded.pe_forward,
                    pb_mrq = excluded.pb_mrq,
                    ps_static = excluded.ps_static,
                    ps_ttm = excluded.ps_ttm,
                    ps_forward = excluded.ps_forward,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    data_as_of = excluded.data_as_of,
                    details_json = excluded.details_json,
                    row_hash = excluded.row_hash,
                    row_version = excluded.row_version,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.as_of_date,
                    snapshot.currency,
                    snapshot.close_price,
                    snapshot.market_cap,
                    snapshot.float_market_cap,
                    snapshot.pe_ratio,
                    snapshot.pb_ratio,
                    snapshot.ps_ratio,
                    snapshot.pe_static,
                    snapshot.pe_ttm,
                    snapshot.pe_forward,
                    snapshot.pb_mrq,
                    snapshot.ps_static,
                    snapshot.ps_ttm,
                    snapshot.ps_forward,
                    snapshot.calc_method,
                    snapshot.calc_version,
                    snapshot.parameter_hash,
                    snapshot.source,
                    snapshot.source_mode,
                    now,
                    details_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            changelog_written = False
            if changed:
                changelog_written = append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="valuation",
                    dataset="valuation_history",
                    change_type="update" if existing else "insert",
                    business_key={
                        "instrument_id": snapshot.instrument_id,
                        "as_of_date": snapshot.as_of_date,
                        "calc_method": snapshot.calc_method,
                        "calc_version": snapshot.calc_version,
                        "parameter_hash": snapshot.parameter_hash,
                    },
                    instrument_id=snapshot.instrument_id,
                    observation_date=snapshot.as_of_date,
                    old_hash=old_hash,
                    new_hash=row_hash,
                    row_version=row_version,
                    source=snapshot.source,
                    source_mode=snapshot.source_mode,
                    source_profile=snapshot.calc_version,
                    ingestion_run_id=ingestion_run_id,
                    changed_at=now,
                )
            conn.commit()
        stats = {
            "inserted": int(existing is None),
            "changed": int(existing is not None and changed),
            "unchanged": int(existing is not None and not changed),
            "changelog_written": int(changelog_written),
        }
        return stats if return_stats else None

    def upsert_valuation_history_many(
        self,
        snapshots: list[ValuationHistorySnapshot],
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Upsert derived valuation history rows in one transaction."""
        if not snapshots:
            return
        if self._uses_separate_valuation_database() and self._active_db_path is None:
            with self.valuation_database_scope():
                self.upsert_valuation_history_many(
                    snapshots,
                    ingestion_run_id=ingestion_run_id,
                )
            return

        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = []
            change_states = []
            for snapshot in snapshots:
                details_json = self._valuation_history_details_json(snapshot.details_json)
                semantic_payload = asdict(snapshot)
                semantic_payload["details_json"] = self._deserialize_json(
                    details_json
                ) or {}
                row_hash = self._semantic_hash(semantic_payload)
                existing = conn.execute(
                    """
                    SELECT * FROM valuation_history
                    WHERE instrument_id = ? AND as_of_date = ? AND calc_method = ?
                      AND calc_version = ? AND parameter_hash = ?
                    """,
                    (
                        snapshot.instrument_id,
                        snapshot.as_of_date,
                        snapshot.calc_method,
                        snapshot.calc_version,
                        snapshot.parameter_hash,
                    ),
                ).fetchone()
                old_hash = None
                if existing is not None:
                    old_payload = dict(existing)
                    old_payload["details_json"] = self._deserialize_json(
                        old_payload.get("details_json")
                    ) or {}
                    for key in (
                        "data_as_of",
                        "row_hash",
                        "row_version",
                        "ingestion_run_id",
                        "created_at",
                        "updated_at",
                    ):
                        old_payload.pop(key, None)
                    old_hash = existing["row_hash"] or self._semantic_hash(old_payload)
                changed = existing is None or old_hash != row_hash
                row_version = (
                    1
                    if existing is None
                    else int(existing["row_version"] or 1) + int(changed)
                )
                rows.append(
                    (
                        snapshot.instrument_id,
                        snapshot.symbol,
                        snapshot.exchange,
                        snapshot.as_of_date,
                        snapshot.currency,
                        snapshot.close_price,
                        snapshot.market_cap,
                        snapshot.float_market_cap,
                        snapshot.pe_ratio,
                        snapshot.pb_ratio,
                        snapshot.ps_ratio,
                        snapshot.pe_static,
                        snapshot.pe_ttm,
                        snapshot.pe_forward,
                        snapshot.pb_mrq,
                        snapshot.ps_static,
                        snapshot.ps_ttm,
                        snapshot.ps_forward,
                        snapshot.calc_method,
                        snapshot.calc_version,
                        snapshot.parameter_hash,
                        snapshot.source,
                        snapshot.source_mode,
                        now,
                        details_json,
                        row_hash,
                        row_version,
                        ingestion_run_id,
                        now,
                        now,
                    )
                )
                change_states.append(
                    (snapshot, existing, old_hash, row_hash, row_version, changed)
                )
            conn.executemany(
                """
                INSERT INTO valuation_history (
                    instrument_id,
                    symbol,
                    exchange,
                    as_of_date,
                    currency,
                    close_price,
                    market_cap,
                    float_market_cap,
                    pe_ratio,
                    pb_ratio,
                    ps_ratio,
                    pe_static,
                    pe_ttm,
                    pe_forward,
                    pb_mrq,
                    ps_static,
                    ps_ttm,
                    ps_forward,
                    calc_method,
                    calc_version,
                    parameter_hash,
                    source,
                    source_mode,
                    data_as_of,
                    details_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id, as_of_date, calc_method, calc_version, parameter_hash)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    currency = excluded.currency,
                    close_price = excluded.close_price,
                    market_cap = excluded.market_cap,
                    float_market_cap = excluded.float_market_cap,
                    pe_ratio = excluded.pe_ratio,
                    pb_ratio = excluded.pb_ratio,
                    ps_ratio = excluded.ps_ratio,
                    pe_static = excluded.pe_static,
                    pe_ttm = excluded.pe_ttm,
                    pe_forward = excluded.pe_forward,
                    pb_mrq = excluded.pb_mrq,
                    ps_static = excluded.ps_static,
                    ps_ttm = excluded.ps_ttm,
                    ps_forward = excluded.ps_forward,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    data_as_of = excluded.data_as_of,
                    details_json = excluded.details_json,
                    row_hash = excluded.row_hash,
                    row_version = excluded.row_version,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                rows,
            )
            for snapshot, existing, old_hash, row_hash, row_version, changed in change_states:
                if not changed:
                    continue
                append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="valuation",
                    dataset="valuation_history",
                    change_type="update" if existing else "insert",
                    business_key={
                        "instrument_id": snapshot.instrument_id,
                        "as_of_date": snapshot.as_of_date,
                        "calc_method": snapshot.calc_method,
                        "calc_version": snapshot.calc_version,
                        "parameter_hash": snapshot.parameter_hash,
                    },
                    instrument_id=snapshot.instrument_id,
                    observation_date=snapshot.as_of_date,
                    old_hash=old_hash,
                    new_hash=row_hash,
                    row_version=row_version,
                    source=snapshot.source,
                    source_mode=snapshot.source_mode,
                    source_profile=snapshot.calc_version,
                    ingestion_run_id=ingestion_run_id,
                    changed_at=now,
                )
            conn.commit()

    def get_existing_valuation_history_dates(
        self,
        instrument_id: str,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
        calc_method: str,
        calc_version: str,
        parameter_hash: str,
        parameter_hashes: Optional[List[str]] = None,
    ) -> set[str]:
        """Return dates already present for one valuation-history identity."""
        if self._uses_separate_valuation_database() and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.get_existing_valuation_history_dates(
                    instrument_id,
                    start_date=start_date,
                    end_date=end_date,
                    calc_method=calc_method,
                    calc_version=calc_version,
                    parameter_hash=parameter_hash,
                    parameter_hashes=parameter_hashes,
                )
        hash_values = [
            str(item)
            for item in (parameter_hashes or [parameter_hash])
            if str(item)
        ]
        if not hash_values:
            hash_values = [parameter_hash]
        hash_placeholders = ", ".join("?" for _ in hash_values)
        query = """
            SELECT as_of_date
            FROM valuation_history
            WHERE instrument_id = ?
              AND calc_method = ?
              AND calc_version = ?
              AND parameter_hash IN ({hash_placeholders})
        """.format(hash_placeholders=hash_placeholders)
        parameters: list[Any] = [
            instrument_id,
            calc_method,
            calc_version,
            *hash_values,
        ]
        if start_date:
            query += " AND as_of_date >= ?"
            parameters.append(start_date)
        if end_date:
            query += " AND as_of_date <= ?"
            parameters.append(end_date)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(query, tuple(parameters)).fetchall()
        return {str(row[0]) for row in rows}

    def upsert_valuation_input(
        self,
        snapshot: ValuationInputSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
        return_stats: bool = False,
    ) -> Optional[Dict[str, int]]:
        """Upsert one explicit valuation input row into valuation storage."""
        if self._uses_separate_valuation_database() and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.upsert_valuation_input(
                    snapshot,
                    ingestion_run_id=ingestion_run_id,
                    return_stats=return_stats,
                )
        if (
            snapshot.shares_outstanding is not None
            or snapshot.float_shares is not None
        ) and not self._is_share_unit(snapshot.unit):
            raise ValueError(
                "share-count valuation inputs require an explicit share unit"
            )
        now = get_shanghai_time().isoformat()
        diagnostics_json = json.dumps(
            snapshot.diagnostics_json,
            ensure_ascii=False,
            sort_keys=True,
        )
        data_as_of = snapshot.data_as_of or snapshot.as_of_date
        semantic_payload = asdict(snapshot)
        semantic_payload["data_as_of"] = data_as_of
        row_hash = self._semantic_hash(semantic_payload)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            existing = conn.execute(
                """
                SELECT * FROM valuation_inputs
                WHERE instrument_id = ? AND as_of_date = ? AND source = ?
                  AND source_mode = ? AND input_kind = ?
                """,
                (
                    snapshot.instrument_id,
                    snapshot.as_of_date,
                    snapshot.source,
                    snapshot.source_mode,
                    snapshot.input_kind,
                ),
            ).fetchone()
            old_hash = None
            if existing is not None:
                old_payload = dict(existing)
                old_payload["diagnostics_json"] = self._deserialize_json(
                    old_payload.get("diagnostics_json")
                ) or {}
                for key in (
                    "row_hash",
                    "row_version",
                    "ingestion_run_id",
                    "created_at",
                    "updated_at",
                ):
                    old_payload.pop(key, None)
                old_hash = existing["row_hash"] or self._semantic_hash(old_payload)
            changed = existing is None or old_hash != row_hash
            row_version = (
                1
                if existing is None
                else int(existing["row_version"] or 1) + int(changed)
            )
            conn.execute(
                """
                INSERT INTO valuation_inputs (
                    instrument_id,
                    symbol,
                    exchange,
                    as_of_date,
                    currency,
                    market_cap,
                    shares_outstanding,
                    float_market_cap,
                    float_shares,
                    source,
                    source_mode,
                    input_kind,
                    unit,
                    data_as_of,
                    diagnostics_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id, as_of_date, source, source_mode, input_kind)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    currency = excluded.currency,
                    market_cap = excluded.market_cap,
                    shares_outstanding = excluded.shares_outstanding,
                    float_market_cap = excluded.float_market_cap,
                    float_shares = excluded.float_shares,
                    unit = excluded.unit,
                    data_as_of = excluded.data_as_of,
                    diagnostics_json = excluded.diagnostics_json,
                    row_hash = excluded.row_hash,
                    row_version = excluded.row_version,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.as_of_date,
                    snapshot.currency,
                    snapshot.market_cap,
                    snapshot.shares_outstanding,
                    snapshot.float_market_cap,
                    snapshot.float_shares,
                    snapshot.source,
                    snapshot.source_mode,
                    snapshot.input_kind,
                    snapshot.unit,
                    data_as_of,
                    diagnostics_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            changelog_written = False
            if changed:
                changelog_written = append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="valuation",
                    dataset="valuation_inputs",
                    change_type="update" if existing else "insert",
                    business_key={
                        "instrument_id": snapshot.instrument_id,
                        "as_of_date": snapshot.as_of_date,
                        "source": snapshot.source,
                        "source_mode": snapshot.source_mode,
                        "input_kind": snapshot.input_kind,
                    },
                    instrument_id=snapshot.instrument_id,
                    observation_date=snapshot.as_of_date,
                    old_hash=old_hash,
                    new_hash=row_hash,
                    row_version=row_version,
                    source=snapshot.source,
                    source_mode=snapshot.source_mode,
                    source_profile=snapshot.input_kind,
                    ingestion_run_id=ingestion_run_id,
                    changed_at=now,
                )
            conn.commit()
        stats = {
            "inserted": int(existing is None),
            "changed": int(existing is not None and changed),
            "unchanged": int(existing is not None and not changed),
            "changelog_written": int(changelog_written),
        }
        return stats if return_stats else None

    def upsert_analyst_forecast(
        self,
        snapshot: AnalystForecastSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Upsert one analyst forecast snapshot."""
        now = get_shanghai_time().isoformat()
        forecast_json = json.dumps(
            snapshot.forecast_json,
            ensure_ascii=False,
            sort_keys=True,
        )

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO analyst_forecasts (
                    instrument_id,
                    symbol,
                    exchange,
                    as_of_date,
                    rating_summary,
                    report_count,
                    institution_count,
                    buy_count,
                    overweight_count,
                    neutral_count,
                    underperform_count,
                    sell_count,
                    eps_fy1,
                    eps_fy2,
                    net_profit_fy1,
                    net_profit_fy2,
                    pe_fy1,
                    pe_fy2,
                    source,
                    source_mode,
                    data_as_of,
                    forecast_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id, as_of_date, source, source_mode)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    rating_summary = excluded.rating_summary,
                    report_count = excluded.report_count,
                    institution_count = excluded.institution_count,
                    buy_count = excluded.buy_count,
                    overweight_count = excluded.overweight_count,
                    neutral_count = excluded.neutral_count,
                    underperform_count = excluded.underperform_count,
                    sell_count = excluded.sell_count,
                    eps_fy1 = excluded.eps_fy1,
                    eps_fy2 = excluded.eps_fy2,
                    net_profit_fy1 = excluded.net_profit_fy1,
                    net_profit_fy2 = excluded.net_profit_fy2,
                    pe_fy1 = excluded.pe_fy1,
                    pe_fy2 = excluded.pe_fy2,
                    data_as_of = excluded.data_as_of,
                    forecast_json = excluded.forecast_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.as_of_date,
                    snapshot.rating_summary,
                    snapshot.report_count,
                    snapshot.institution_count,
                    snapshot.buy_count,
                    snapshot.overweight_count,
                    snapshot.neutral_count,
                    snapshot.underperform_count,
                    snapshot.sell_count,
                    snapshot.eps_fy1,
                    snapshot.eps_fy2,
                    snapshot.net_profit_fy1,
                    snapshot.net_profit_fy2,
                    snapshot.pe_fy1,
                    snapshot.pe_fy2,
                    snapshot.source,
                    snapshot.source_mode,
                    now,
                    forecast_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            conn.commit()

    def upsert_research_report(
        self,
        snapshot: ResearchReportSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Upsert one research report metadata row."""
        now = get_shanghai_time().isoformat()
        report_json = json.dumps(snapshot.report_json, ensure_ascii=False, sort_keys=True)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO research_reports (
                    report_id,
                    instrument_id,
                    symbol,
                    exchange,
                    publish_date,
                    report_title,
                    institution_name,
                    analyst_name,
                    rating,
                    rating_change,
                    target_price,
                    report_url,
                    source,
                    source_mode,
                    data_as_of,
                    report_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(report_id)
                DO UPDATE SET
                    instrument_id = excluded.instrument_id,
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    publish_date = excluded.publish_date,
                    report_title = excluded.report_title,
                    institution_name = excluded.institution_name,
                    analyst_name = excluded.analyst_name,
                    rating = excluded.rating,
                    rating_change = excluded.rating_change,
                    target_price = excluded.target_price,
                    report_url = excluded.report_url,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    data_as_of = excluded.data_as_of,
                    report_json = excluded.report_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.report_id,
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.publish_date,
                    snapshot.report_title,
                    snapshot.institution_name,
                    snapshot.analyst_name,
                    snapshot.rating,
                    snapshot.rating_change,
                    snapshot.target_price,
                    snapshot.report_url,
                    snapshot.source,
                    snapshot.source_mode,
                    now,
                    report_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            conn.commit()

    def upsert_sentiment_event(
        self,
        snapshot: SentimentEventSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Upsert one sentiment event row."""
        now = get_shanghai_time().isoformat()
        details_json = json.dumps(snapshot.details_json, ensure_ascii=False, sort_keys=True)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO sentiment_events (
                    event_id,
                    instrument_id,
                    symbol,
                    exchange,
                    event_date,
                    event_type,
                    event_subtype,
                    title,
                    sentiment_score,
                    severity,
                    source,
                    source_mode,
                    data_as_of,
                    details_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id)
                DO UPDATE SET
                    instrument_id = excluded.instrument_id,
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    event_date = excluded.event_date,
                    event_type = excluded.event_type,
                    event_subtype = excluded.event_subtype,
                    title = excluded.title,
                    sentiment_score = excluded.sentiment_score,
                    severity = excluded.severity,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    data_as_of = excluded.data_as_of,
                    details_json = excluded.details_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.event_id,
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.event_date,
                    snapshot.event_type,
                    snapshot.event_subtype,
                    snapshot.title,
                    snapshot.sentiment_score,
                    snapshot.severity,
                    snapshot.source,
                    snapshot.source_mode,
                    now,
                    details_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            conn.commit()

    def upsert_risk_snapshot(
        self,
        snapshot: RiskSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
        return_stats: bool = False,
    ) -> Optional[Dict[str, int]]:
        """Upsert one derived risk snapshot row."""
        now = get_shanghai_time().isoformat()
        details_json = json.dumps(snapshot.details_json, ensure_ascii=False, sort_keys=True)
        row_hash = self._semantic_hash(asdict(snapshot))

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            existing = conn.execute(
                """
                SELECT * FROM risk_snapshots
                WHERE instrument_id = ? AND as_of_date = ?
                  AND calc_method = ? AND calc_version = ? AND parameter_hash = ?
                """,
                (
                    snapshot.instrument_id,
                    snapshot.as_of_date,
                    snapshot.calc_method,
                    snapshot.calc_version,
                    snapshot.parameter_hash,
                ),
            ).fetchone()
            old_hash = None
            if existing is not None:
                old_payload = dict(existing)
                old_payload["details_json"] = self._deserialize_json(
                    old_payload.get("details_json")
                ) or {}
                for key in (
                    "data_as_of",
                    "row_hash",
                    "row_version",
                    "ingestion_run_id",
                    "created_at",
                    "updated_at",
                ):
                    old_payload.pop(key, None)
                old_hash = existing["row_hash"] or self._semantic_hash(old_payload)
            changed = existing is None or old_hash != row_hash
            row_version = (
                1
                if existing is None
                else int(existing["row_version"] or 1) + int(changed)
            )
            conn.execute(
                """
                INSERT INTO risk_snapshots (
                    instrument_id,
                    symbol,
                    exchange,
                    as_of_date,
                    benchmark_instrument_id,
                    volatility_20d,
                    volatility_60d,
                    beta_60d,
                    max_drawdown_252d,
                    average_turnover_20d,
                    average_amount_20d,
                    liability_to_asset,
                    current_ratio,
                    operating_cf_to_net_income,
                    negative_event_count_30d,
                    risk_score,
                    risk_level,
                    calc_method,
                    calc_version,
                    parameter_hash,
                    source,
                    source_mode,
                    data_as_of,
                    details_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id, as_of_date, calc_method, calc_version, parameter_hash)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    benchmark_instrument_id = excluded.benchmark_instrument_id,
                    volatility_20d = excluded.volatility_20d,
                    volatility_60d = excluded.volatility_60d,
                    beta_60d = excluded.beta_60d,
                    max_drawdown_252d = excluded.max_drawdown_252d,
                    average_turnover_20d = excluded.average_turnover_20d,
                    average_amount_20d = excluded.average_amount_20d,
                    liability_to_asset = excluded.liability_to_asset,
                    current_ratio = excluded.current_ratio,
                    operating_cf_to_net_income = excluded.operating_cf_to_net_income,
                    negative_event_count_30d = excluded.negative_event_count_30d,
                    risk_score = excluded.risk_score,
                    risk_level = excluded.risk_level,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    data_as_of = excluded.data_as_of,
                    details_json = excluded.details_json,
                    row_hash = excluded.row_hash,
                    row_version = excluded.row_version,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.as_of_date,
                    snapshot.benchmark_instrument_id,
                    snapshot.volatility_20d,
                    snapshot.volatility_60d,
                    snapshot.beta_60d,
                    snapshot.max_drawdown_252d,
                    snapshot.average_turnover_20d,
                    snapshot.average_amount_20d,
                    snapshot.liability_to_asset,
                    snapshot.current_ratio,
                    snapshot.operating_cf_to_net_income,
                    snapshot.negative_event_count_30d,
                    snapshot.risk_score,
                    snapshot.risk_level,
                    snapshot.calc_method,
                    snapshot.calc_version,
                    snapshot.parameter_hash,
                    snapshot.source,
                    snapshot.source_mode,
                    now,
                    details_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            changelog_written = False
            if changed:
                changelog_written = append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="risk",
                    dataset="risk_snapshots",
                    change_type="update" if existing else "insert",
                    business_key={
                        "instrument_id": snapshot.instrument_id,
                        "as_of_date": snapshot.as_of_date,
                        "calc_method": snapshot.calc_method,
                        "calc_version": snapshot.calc_version,
                        "parameter_hash": snapshot.parameter_hash,
                    },
                    instrument_id=snapshot.instrument_id,
                    observation_date=snapshot.as_of_date,
                    old_hash=old_hash,
                    new_hash=row_hash,
                    row_version=row_version,
                    source=snapshot.source,
                    source_mode=snapshot.source_mode,
                    source_profile=snapshot.calc_version,
                    ingestion_run_id=ingestion_run_id,
                    changed_at=now,
                )
            conn.commit()
        stats = {
            "inserted": int(existing is None),
            "changed": int(existing is not None and changed),
            "unchanged": int(existing is not None and not changed),
            "changelog_written": int(changelog_written),
        }
        return stats if return_stats else None

    def upsert_technical_indicator_latest(
        self,
        snapshot: TechnicalIndicatorLatestSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
        return_stats: bool = False,
    ) -> Optional[Dict[str, int]]:
        """Upsert one latest technical indicator snapshot."""
        now = get_shanghai_time().isoformat()
        summary_json = json.dumps(
            snapshot.summary_json,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
        row_hash = self._semantic_hash(asdict(snapshot))

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            existing = conn.execute(
                """
                SELECT * FROM technical_indicator_latest
                WHERE instrument_id = ? AND period = ? AND adjustment = ?
                  AND calc_method = ? AND calc_version = ? AND parameter_hash = ?
                """,
                (
                    snapshot.instrument_id,
                    snapshot.period,
                    snapshot.adjustment,
                    snapshot.calc_method,
                    snapshot.calc_version,
                    snapshot.parameter_hash,
                ),
            ).fetchone()
            old_hash = None
            if existing is not None:
                old_payload = dict(existing)
                old_payload["summary_json"] = self._deserialize_json(
                    old_payload.get("summary_json")
                ) or {}
                for key in (
                    "data_as_of",
                    "row_hash",
                    "row_version",
                    "ingestion_run_id",
                    "created_at",
                    "updated_at",
                ):
                    old_payload.pop(key, None)
                old_hash = existing["row_hash"] or self._semantic_hash(old_payload)
            changed = existing is None or old_hash != row_hash
            row_version = (
                1
                if existing is None
                else int(existing["row_version"] or 1) + int(changed)
            )
            conn.execute(
                """
                INSERT INTO technical_indicator_latest (
                    instrument_id,
                    symbol,
                    exchange,
                    period,
                    as_of_date,
                    adjustment,
                    applied_adjustment,
                    calc_method,
                    calc_version,
                    parameter_hash,
                    status,
                    missing_reason,
                    signal,
                    trend_score,
                    close_price,
                    pct_change_1d,
                    pct_change_20d,
                    sma20,
                    sma60,
                    ema12,
                    ema26,
                    macd,
                    macd_signal,
                    macd_hist,
                    rsi14,
                    adx,
                    plus_di,
                    minus_di,
                    stoch_k,
                    stoch_d,
                    cci,
                    williams_r,
                    boll_upper,
                    boll_middle,
                    boll_lower,
                    atr14,
                    volume_ratio,
                    distance_to_sma20,
                    distance_to_sma60,
                    source,
                    source_mode,
                    data_as_of,
                    summary_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id, period, adjustment, calc_method, calc_version, parameter_hash)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    as_of_date = excluded.as_of_date,
                    applied_adjustment = excluded.applied_adjustment,
                    status = excluded.status,
                    missing_reason = excluded.missing_reason,
                    signal = excluded.signal,
                    trend_score = excluded.trend_score,
                    close_price = excluded.close_price,
                    pct_change_1d = excluded.pct_change_1d,
                    pct_change_20d = excluded.pct_change_20d,
                    sma20 = excluded.sma20,
                    sma60 = excluded.sma60,
                    ema12 = excluded.ema12,
                    ema26 = excluded.ema26,
                    macd = excluded.macd,
                    macd_signal = excluded.macd_signal,
                    macd_hist = excluded.macd_hist,
                    rsi14 = excluded.rsi14,
                    adx = excluded.adx,
                    plus_di = excluded.plus_di,
                    minus_di = excluded.minus_di,
                    stoch_k = excluded.stoch_k,
                    stoch_d = excluded.stoch_d,
                    cci = excluded.cci,
                    williams_r = excluded.williams_r,
                    boll_upper = excluded.boll_upper,
                    boll_middle = excluded.boll_middle,
                    boll_lower = excluded.boll_lower,
                    atr14 = excluded.atr14,
                    volume_ratio = excluded.volume_ratio,
                    distance_to_sma20 = excluded.distance_to_sma20,
                    distance_to_sma60 = excluded.distance_to_sma60,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    data_as_of = excluded.data_as_of,
                    summary_json = excluded.summary_json,
                    row_hash = excluded.row_hash,
                    row_version = excluded.row_version,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.period,
                    snapshot.as_of_date,
                    snapshot.adjustment,
                    snapshot.applied_adjustment,
                    snapshot.calc_method,
                    snapshot.calc_version,
                    snapshot.parameter_hash,
                    snapshot.status,
                    snapshot.missing_reason,
                    snapshot.signal,
                    snapshot.trend_score,
                    snapshot.close_price,
                    snapshot.pct_change_1d,
                    snapshot.pct_change_20d,
                    snapshot.sma20,
                    snapshot.sma60,
                    snapshot.ema12,
                    snapshot.ema26,
                    snapshot.macd,
                    snapshot.macd_signal,
                    snapshot.macd_hist,
                    snapshot.rsi14,
                    snapshot.adx,
                    snapshot.plus_di,
                    snapshot.minus_di,
                    snapshot.stoch_k,
                    snapshot.stoch_d,
                    snapshot.cci,
                    snapshot.williams_r,
                    snapshot.boll_upper,
                    snapshot.boll_middle,
                    snapshot.boll_lower,
                    snapshot.atr14,
                    snapshot.volume_ratio,
                    snapshot.distance_to_sma20,
                    snapshot.distance_to_sma60,
                    snapshot.source,
                    snapshot.source_mode,
                    now,
                    summary_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            changelog_written = False
            if changed:
                changelog_written = append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="technical",
                    dataset="technical_indicator_latest",
                    change_type="update" if existing else "insert",
                    business_key={
                        "instrument_id": snapshot.instrument_id,
                        "period": snapshot.period,
                        "adjustment": snapshot.adjustment,
                        "calc_method": snapshot.calc_method,
                        "calc_version": snapshot.calc_version,
                        "parameter_hash": snapshot.parameter_hash,
                    },
                    instrument_id=snapshot.instrument_id,
                    observation_date=snapshot.as_of_date,
                    old_hash=old_hash,
                    new_hash=row_hash,
                    row_version=row_version,
                    source=snapshot.source,
                    source_mode=snapshot.source_mode,
                    source_profile=snapshot.calc_version,
                    ingestion_run_id=ingestion_run_id,
                    changed_at=now,
                )
            conn.commit()
        stats = {
            "inserted": int(existing is None),
            "changed": int(existing is not None and changed),
            "unchanged": int(existing is not None and not changed),
            "changelog_written": int(changelog_written),
        }
        return stats if return_stats else None

    def upsert_industry_membership(
        self,
        snapshot: IndustrySnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
        return_stats: bool = False,
    ) -> Optional[Dict[str, int]]:
        """Upsert one normalized industry membership snapshot and taxonomy row."""
        now = get_shanghai_time().isoformat()
        membership_json = json.dumps(snapshot.membership_json, ensure_ascii=False, sort_keys=True)
        semantic_payload = self._industry_membership_semantic_payload(snapshot)
        row_hash = self._semantic_hash(semantic_payload)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            self._upsert_industry_taxonomy_row(
                conn,
                IndustryTaxonomySnapshot(
                    taxonomy_system=snapshot.taxonomy_system,
                    taxonomy_version=snapshot.taxonomy_version,
                    industry_code=snapshot.industry_code,
                    industry_name=snapshot.industry_name,
                    industry_level=snapshot.industry_level,
                    parent_code=snapshot.parent_code,
                    source_classification=snapshot.source_classification,
                    source=snapshot.source,
                    source_mode=snapshot.source_mode,
                ),
                ingestion_run_id=ingestion_run_id,
                now=now,
            )
            existing = conn.execute(
                """
                SELECT * FROM industry_memberships
                WHERE instrument_id = ?
                  AND taxonomy_system = ?
                  AND taxonomy_version = ?
                """,
                (
                    snapshot.instrument_id,
                    snapshot.taxonomy_system,
                    snapshot.taxonomy_version or "",
                ),
            ).fetchone()
            old_hash = None
            if existing is not None:
                old_hash = existing["row_hash"] or self._semantic_hash(
                    self._industry_membership_row_semantic_payload(existing)
                )
            changed = existing is None or old_hash != row_hash
            if existing is None:
                previous_version = self._latest_industry_membership_version(
                    conn,
                    instrument_id=snapshot.instrument_id,
                    taxonomy_system=snapshot.taxonomy_system,
                    taxonomy_version=snapshot.taxonomy_version or "",
                )
                row_version = previous_version + 1
            else:
                row_version = int(existing["row_version"] or 1) + int(changed)
            conn.execute(
                """
                INSERT INTO industry_memberships (
                    instrument_id,
                    symbol,
                    exchange,
                    taxonomy_system,
                    taxonomy_version,
                    industry_code,
                    industry_name,
                    industry_level,
                    parent_code,
                    mapping_status,
                    effective_date,
                    source_classification,
                    source_industry_name,
                    sw_l1_code,
                    sw_l1_name,
                    sw_l2_code,
                    sw_l2_name,
                    sw_l3_code,
                    sw_l3_name,
                    sw_l1_index_code,
                    sw_l2_index_code,
                    sw_l3_index_code,
                    source,
                    source_mode,
                    data_as_of,
                    membership_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id, taxonomy_system, taxonomy_version)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    industry_code = excluded.industry_code,
                    industry_name = excluded.industry_name,
                    industry_level = excluded.industry_level,
                    parent_code = excluded.parent_code,
                    mapping_status = excluded.mapping_status,
                    effective_date = excluded.effective_date,
                    source_classification = excluded.source_classification,
                    source_industry_name = excluded.source_industry_name,
                    sw_l1_code = excluded.sw_l1_code,
                    sw_l1_name = excluded.sw_l1_name,
                    sw_l2_code = excluded.sw_l2_code,
                    sw_l2_name = excluded.sw_l2_name,
                    sw_l3_code = excluded.sw_l3_code,
                    sw_l3_name = excluded.sw_l3_name,
                    sw_l1_index_code = excluded.sw_l1_index_code,
                    sw_l2_index_code = excluded.sw_l2_index_code,
                    sw_l3_index_code = excluded.sw_l3_index_code,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    data_as_of = excluded.data_as_of,
                    membership_json = excluded.membership_json,
                    row_hash = excluded.row_hash,
                    row_version = excluded.row_version,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.taxonomy_system,
                    snapshot.taxonomy_version or "",
                    snapshot.industry_code,
                    snapshot.industry_name,
                    snapshot.industry_level,
                    snapshot.parent_code,
                    snapshot.mapping_status,
                    snapshot.effective_date,
                    snapshot.source_classification,
                    snapshot.source_industry_name,
                    snapshot.sw_l1_code,
                    snapshot.sw_l1_name,
                    snapshot.sw_l2_code,
                    snapshot.sw_l2_name,
                    snapshot.sw_l3_code,
                    snapshot.sw_l3_name,
                    snapshot.sw_l1_index_code,
                    snapshot.sw_l2_index_code,
                    snapshot.sw_l3_index_code,
                    snapshot.source,
                    snapshot.source_mode,
                    now,
                    membership_json,
                    row_hash,
                    row_version,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            changelog_written = False
            if changed:
                changelog_written = append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="industry",
                    dataset="industry_memberships",
                    change_type="update" if existing else "insert",
                    business_key={
                        "instrument_id": snapshot.instrument_id,
                        "taxonomy_system": snapshot.taxonomy_system,
                        "taxonomy_version": snapshot.taxonomy_version or "",
                        "effective_date": snapshot.effective_date,
                    },
                    instrument_id=snapshot.instrument_id,
                    observation_date=snapshot.effective_date,
                    old_hash=old_hash,
                    new_hash=row_hash,
                    row_version=row_version,
                    source=snapshot.source,
                    source_mode=snapshot.source_mode,
                    source_profile=snapshot.source_classification,
                    ingestion_run_id=ingestion_run_id,
                    changed_at=now,
                )
            conn.commit()
        stats = {
            "inserted": int(existing is None),
            "changed": int(existing is not None and changed),
            "unchanged": int(existing is not None and not changed),
            "changelog_written": int(changelog_written),
        }
        return stats if return_stats else None

    def upsert_industry_index_analysis(
        self,
        snapshot: IndustryIndexAnalysisSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Upsert one official Shenwan industry index-analysis daily row."""
        now = get_shanghai_time().isoformat()
        raw_payload_json = json.dumps(
            snapshot.raw_payload,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO industry_index_analysis_daily (
                    taxonomy_system,
                    taxonomy_version,
                    sw_index_code,
                    trade_date,
                    sw_index_name,
                    index_type,
                    close_index,
                    bargain_volume,
                    markup,
                    turnover_rate,
                    pe,
                    pb,
                    mean_price,
                    bargain_sum_rate,
                    negotiable_share_sum,
                    average_negotiable_share_sum,
                    dividend_yield,
                    source,
                    source_mode,
                    raw_payload_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(taxonomy_system, taxonomy_version, sw_index_code, trade_date)
                DO UPDATE SET
                    sw_index_name = excluded.sw_index_name,
                    index_type = excluded.index_type,
                    close_index = excluded.close_index,
                    bargain_volume = excluded.bargain_volume,
                    markup = excluded.markup,
                    turnover_rate = excluded.turnover_rate,
                    pe = excluded.pe,
                    pb = excluded.pb,
                    mean_price = excluded.mean_price,
                    bargain_sum_rate = excluded.bargain_sum_rate,
                    negotiable_share_sum = excluded.negotiable_share_sum,
                    average_negotiable_share_sum = excluded.average_negotiable_share_sum,
                    dividend_yield = excluded.dividend_yield,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    raw_payload_json = excluded.raw_payload_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.taxonomy_system,
                    snapshot.taxonomy_version or "",
                    snapshot.sw_index_code,
                    snapshot.trade_date,
                    snapshot.sw_index_name,
                    snapshot.index_type,
                    snapshot.close_index,
                    snapshot.bargain_volume,
                    snapshot.markup,
                    snapshot.turnover_rate,
                    snapshot.pe,
                    snapshot.pb,
                    snapshot.mean_price,
                    snapshot.bargain_sum_rate,
                    snapshot.negotiable_share_sum,
                    snapshot.average_negotiable_share_sum,
                    snapshot.dividend_yield,
                    snapshot.source,
                    snapshot.source_mode,
                    raw_payload_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            conn.commit()

    def summarize_industry_index_analysis_daily(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
    ) -> Dict[str, Any]:
        """Return readiness summary for official index-analysis daily rows."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            summary_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(DISTINCT sw_index_code) AS distinct_index_codes,
                    MAX(trade_date) AS latest_trade_date,
                    MAX(updated_at) AS latest_updated_at
                FROM industry_index_analysis_daily
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                """,
                (taxonomy_system, taxonomy_version or ""),
            ).fetchone()
            type_rows = conn.execute(
                """
                SELECT index_type, COUNT(*) AS row_count, COUNT(DISTINCT sw_index_code) AS code_count
                FROM industry_index_analysis_daily
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                GROUP BY index_type
                ORDER BY index_type ASC
                """,
                (taxonomy_system, taxonomy_version or ""),
            ).fetchall()

        return {
            "total": int((summary_row["total"] if summary_row is not None else 0) or 0),
            "distinct_index_codes": int(
                (summary_row["distinct_index_codes"] if summary_row is not None else 0)
                or 0
            ),
            "latest_trade_date": None if summary_row is None else summary_row["latest_trade_date"],
            "latest_updated_at": None if summary_row is None else summary_row["latest_updated_at"],
            "index_type_counts": {
                str(row["index_type"]): {
                    "rows": int(row["row_count"] or 0),
                    "codes": int(row["code_count"] or 0),
                }
                for row in type_rows
                if row["index_type"] is not None
            },
        }

    def get_latest_industry_index_analysis(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        sw_index_code: str,
        include_payload: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Fetch latest official index-analysis row for one Shenwan index code."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT *
                FROM industry_index_analysis_daily
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                  AND sw_index_code = ?
                ORDER BY trade_date DESC, updated_at DESC
                LIMIT 1
                """,
                (taxonomy_system, taxonomy_version or "", sw_index_code),
            ).fetchone()

        if row is None:
            return None
        item = dict(row)
        payload_json = item.pop("raw_payload_json", None)
        if include_payload:
            item["raw_payload"] = self._deserialize_json(payload_json) or {}
        return item

    def list_industry_index_analysis_daily(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        sw_index_code: Optional[str] = None,
        index_type: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = 100,
        offset: int = 0,
        include_payload: bool = True,
    ) -> list[Dict[str, Any]]:
        """List official Shenwan industry index-analysis rows with filters."""
        where_sql, params = self._industry_index_analysis_filter_sql(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            sw_index_code=sw_index_code,
            index_type=index_type,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        sql = f"""
            SELECT *
            FROM industry_index_analysis_daily
            {where_sql}
            ORDER BY trade_date DESC, index_type ASC, sw_index_code ASC
        """
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params.extend((int(limit), max(int(offset), 0)))

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()

        records: list[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            payload_json = item.pop("raw_payload_json", None)
            if include_payload:
                item["raw_payload"] = self._deserialize_json(payload_json) or {}
            records.append(item)
        return records

    def count_industry_index_analysis_daily(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        sw_index_code: Optional[str] = None,
        index_type: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> int:
        """Count official Shenwan industry index-analysis rows with filters."""
        where_sql, params = self._industry_index_analysis_filter_sql(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            sw_index_code=sw_index_code,
            index_type=index_type,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        sql = f"""
            SELECT COUNT(*)
            FROM industry_index_analysis_daily
            {where_sql}
        """
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            count = conn.execute(sql, params).fetchone()[0]
        return int(count or 0)

    def _industry_index_analysis_filter_sql(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        sw_index_code: Optional[str] = None,
        index_type: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> tuple[str, list[Any]]:
        """Build a WHERE clause for official Shenwan index-analysis queries."""
        conditions = ["taxonomy_system = ?", "taxonomy_version = ?"]
        params: list[Any] = [taxonomy_system, taxonomy_version or ""]
        if sw_index_code:
            conditions.append("sw_index_code = ?")
            params.append(sw_index_code)
        if index_type:
            conditions.append("index_type = ?")
            params.append(index_type)
        if trade_date:
            conditions.append("trade_date = ?")
            params.append(trade_date)
        else:
            if start_date:
                conditions.append("trade_date >= ?")
                params.append(start_date)
            if end_date:
                conditions.append("trade_date <= ?")
                params.append(end_date)
        return "WHERE " + " AND ".join(conditions), params

    def delete_industry_memberships(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        instrument_ids: List[str],
        ingestion_run_id: Optional[int] = None,
    ) -> int:
        """Delete normalized industry memberships for one taxonomy and instrument set."""
        normalized_ids = [
            str(instrument_id).strip()
            for instrument_id in instrument_ids
            if str(instrument_id).strip()
        ]
        if not normalized_ids:
            return 0

        placeholders = ",".join("?" for _ in normalized_ids)
        sql = f"""
            DELETE FROM industry_memberships
            WHERE taxonomy_system = ?
              AND taxonomy_version = ?
              AND instrument_id IN ({placeholders})
        """
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT * FROM industry_memberships
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                  AND instrument_id IN ({placeholders})
                """,
                [taxonomy_system, taxonomy_version or "", *normalized_ids],
            ).fetchall()
            cursor = conn.execute(
                sql,
                [taxonomy_system, taxonomy_version or "", *normalized_ids],
            )
            now = get_shanghai_time().isoformat()
            for row in rows:
                old_hash = row["row_hash"] or self._semantic_hash(
                    self._industry_membership_row_semantic_payload(row)
                )
                append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="industry",
                    dataset="industry_memberships",
                    change_type="delete_marker",
                    business_key={
                        "instrument_id": row["instrument_id"],
                        "taxonomy_system": row["taxonomy_system"],
                        "taxonomy_version": row["taxonomy_version"],
                        "effective_date": row["effective_date"],
                    },
                    instrument_id=row["instrument_id"],
                    observation_date=row["effective_date"],
                    old_hash=old_hash,
                    row_version=int(row["row_version"] or 1) + 1,
                    source=row["source"],
                    source_mode=row["source_mode"],
                    source_profile=row["source_classification"],
                    ingestion_run_id=ingestion_run_id,
                    changed_at=now,
                )
            conn.commit()
            return int(cursor.rowcount or 0)

    def upsert_industry_source_file(
        self,
        snapshot: IndustrySourceFileSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> int:
        """Persist one industry source-file manifest row and return its id."""
        now = get_shanghai_time().isoformat()
        raw_headers_json = json.dumps(snapshot.raw_headers, ensure_ascii=False, sort_keys=True)
        metadata_json = json.dumps(snapshot.metadata_json, ensure_ascii=False, sort_keys=True)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            cursor = conn.execute(
                """
                INSERT INTO industry_source_files (
                    source,
                    source_mode,
                    artifact_kind,
                    url,
                    parser_version,
                    status,
                    etag,
                    last_modified,
                    content_length,
                    sha256,
                    row_count,
                    max_source_update_time,
                    raw_headers_json,
                    metadata_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, source_mode, artifact_kind, sha256)
                DO UPDATE SET
                    url = excluded.url,
                    parser_version = excluded.parser_version,
                    status = excluded.status,
                    etag = excluded.etag,
                    last_modified = excluded.last_modified,
                    content_length = excluded.content_length,
                    row_count = excluded.row_count,
                    max_source_update_time = excluded.max_source_update_time,
                    raw_headers_json = excluded.raw_headers_json,
                    metadata_json = excluded.metadata_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.source,
                    snapshot.source_mode,
                    snapshot.artifact_kind,
                    snapshot.url,
                    snapshot.parser_version,
                    snapshot.status,
                    snapshot.etag,
                    snapshot.last_modified,
                    snapshot.content_length,
                    snapshot.sha256 or "",
                    snapshot.row_count,
                    snapshot.max_source_update_time,
                    raw_headers_json,
                    metadata_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                """
                SELECT id
                FROM industry_source_files
                WHERE source = ?
                  AND source_mode = ?
                  AND artifact_kind = ?
                  AND sha256 = ?
                """,
                (
                    snapshot.source,
                    snapshot.source_mode,
                    snapshot.artifact_kind,
                    snapshot.sha256 or "",
                ),
            ).fetchone()
            source_file_id = 0 if row is None else int(row["id"])
            conn.commit()
        return source_file_id

    def get_latest_industry_source_file(
        self,
        *,
        source: str,
        source_mode: str,
        artifact_kind: str,
    ) -> Optional[Dict[str, Any]]:
        """Return latest source-file manifest metadata for one artifact."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT
                    id,
                    source,
                    source_mode,
                    artifact_kind,
                    url,
                    parser_version,
                    status,
                    etag,
                    last_modified,
                    content_length,
                    sha256,
                    row_count,
                    max_source_update_time,
                    raw_headers_json,
                    metadata_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                FROM industry_source_files
                WHERE source = ?
                  AND source_mode = ?
                  AND artifact_kind = ?
                  AND status IN ('downloaded', 'unchanged')
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (source, source_mode, artifact_kind),
            ).fetchone()

        if row is None:
            return None
        item = dict(row)
        item["raw_headers"] = self._deserialize_json(item.pop("raw_headers_json", None)) or {}
        item["metadata"] = self._deserialize_json(item.pop("metadata_json", None)) or {}
        return item

    def get_latest_industry_source_files(
        self,
        *,
        source: str,
        source_mode: str,
        artifact_kinds: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """Return latest source-file manifests keyed by artifact kind."""
        result: Dict[str, Dict[str, Any]] = {}
        for artifact_kind in artifact_kinds:
            item = self.get_latest_industry_source_file(
                source=source,
                source_mode=source_mode,
                artifact_kind=artifact_kind,
            )
            if item is not None:
                result[artifact_kind] = item
        return result

    def replace_industry_classification_history(
        self,
        rows: List[IndustryClassificationHistorySnapshot],
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Replace full official classification history rows for one taxonomy version."""
        now = get_shanghai_time().isoformat()

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                DELETE FROM industry_classification_history
                WHERE taxonomy_system = ? AND taxonomy_version = ?
                """,
                (taxonomy_system, taxonomy_version or ""),
            )
            for row in rows:
                classification_json = json.dumps(
                    row.classification_json,
                    ensure_ascii=False,
                    sort_keys=True,
                )
                conn.execute(
                    """
                    INSERT INTO industry_classification_history (
                        row_hash,
                        instrument_id,
                        symbol,
                        exchange,
                        taxonomy_system,
                        taxonomy_version,
                        official_industry_code,
                        official_start_date,
                        official_update_time,
                        source_file_id,
                        source,
                        source_mode,
                        classification_json,
                        ingestion_run_id,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row.row_hash,
                        row.instrument_id,
                        row.symbol,
                        row.exchange,
                        row.taxonomy_system,
                        row.taxonomy_version or "",
                        row.official_industry_code,
                        row.official_start_date,
                        row.official_update_time,
                        row.source_file_id,
                        row.source,
                        row.source_mode,
                        classification_json,
                        ingestion_run_id,
                        now,
                        now,
                    ),
                )
            conn.commit()

    def clear_industry_standard_slice(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        include_source_files: bool = False,
        preserve_change_tracked: bool = False,
    ) -> Dict[str, int]:
        """Delete rebuildable strict-industry rows for one taxonomy version."""
        tables = [
            "industry_official_classifications",
            "industry_classification_history",
            "industry_component_sets",
            "industry_official_code_mappings",
        ]
        if not preserve_change_tracked:
            tables = ["industry_memberships", *tables, "industry_taxonomy"]
        deleted: Dict[str, int] = {}
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            for table in tables:
                cursor = conn.execute(
                    f"""
                    DELETE FROM {table}
                    WHERE taxonomy_system = ? AND taxonomy_version = ?
                    """,
                    (taxonomy_system, taxonomy_version or ""),
                )
                deleted[table] = int(cursor.rowcount or 0)

            if include_source_files:
                cursor = conn.execute(
                    """
                    DELETE FROM industry_source_files
                    WHERE source IN ('swsresearch', 'akshare')
                      AND artifact_kind LIKE 'shenwan_%'
                    """
                )
                deleted["industry_source_files"] = int(cursor.rowcount or 0)
            conn.commit()
        return deleted

    def deactivate_missing_industry_taxonomy(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        active_industry_codes: List[str],
        ingestion_run_id: Optional[int] = None,
    ) -> int:
        """Mark taxonomy nodes absent from a complete refresh as inactive."""
        normalized_codes = sorted(
            {str(code).strip() for code in active_industry_codes if str(code).strip()}
        )
        params: List[Any] = [taxonomy_system, taxonomy_version or ""]
        exclusion = ""
        if normalized_codes:
            placeholders = ",".join("?" for _ in normalized_codes)
            exclusion = f"AND industry_code NOT IN ({placeholders})"
            params.extend(normalized_codes)
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT * FROM industry_taxonomy
                WHERE taxonomy_system = ? AND taxonomy_version = ?
                  AND is_active = 1
                  {exclusion}
                """,
                params,
            ).fetchall()
            for row in rows:
                old_hash = row["row_hash"] or self._semantic_hash(
                    self._industry_taxonomy_row_semantic_payload(row)
                )
                new_payload = self._industry_taxonomy_row_semantic_payload(row)
                new_payload["is_active"] = 0
                new_hash = self._semantic_hash(new_payload)
                row_version = int(row["row_version"] or 1) + 1
                conn.execute(
                    """
                    UPDATE industry_taxonomy
                    SET is_active = 0, row_hash = ?, row_version = ?, updated_at = ?
                    WHERE taxonomy_system = ? AND taxonomy_version = ?
                      AND industry_code = ?
                    """,
                    (
                        new_hash,
                        row_version,
                        now,
                        row["taxonomy_system"],
                        row["taxonomy_version"],
                        row["industry_code"],
                    ),
                )
                append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="industry",
                    dataset="industry_taxonomy",
                    change_type="update",
                    business_key={
                        "taxonomy_system": row["taxonomy_system"],
                        "taxonomy_version": row["taxonomy_version"],
                        "industry_code": row["industry_code"],
                    },
                    series_id=row["industry_code"],
                    old_hash=old_hash,
                    new_hash=new_hash,
                    row_version=row_version,
                    source=row["source"],
                    source_mode=row["source_mode"],
                    source_profile=row["source_classification"],
                    ingestion_run_id=ingestion_run_id,
                    changed_at=now,
                )
            conn.commit()
        return len(rows)

    def delete_industry_memberships_outside_universe(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        exchanges: List[str],
        active_instrument_ids: List[str],
        ingestion_run_id: Optional[int] = None,
    ) -> int:
        """Delete memberships no longer present in the refreshed target universe."""
        normalized_exchanges = sorted(
            {str(exchange).strip() for exchange in exchanges if str(exchange).strip()}
        )
        if not normalized_exchanges:
            return 0
        normalized_ids = sorted(
            {
                str(instrument_id).strip()
                for instrument_id in active_instrument_ids
                if str(instrument_id).strip()
            }
        )
        exchange_placeholders = ",".join("?" for _ in normalized_exchanges)
        params: List[Any] = [
            taxonomy_system,
            taxonomy_version or "",
            *normalized_exchanges,
        ]
        universe_clause = ""
        if normalized_ids:
            id_placeholders = ",".join("?" for _ in normalized_ids)
            universe_clause = f"AND instrument_id NOT IN ({id_placeholders})"
            params.extend(normalized_ids)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT instrument_id
                FROM industry_memberships
                WHERE taxonomy_system = ? AND taxonomy_version = ?
                  AND exchange IN ({exchange_placeholders})
                  {universe_clause}
                """,
                params,
            ).fetchall()
        return self.delete_industry_memberships(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            instrument_ids=[str(row["instrument_id"]) for row in rows],
            ingestion_run_id=ingestion_run_id,
        )

    def summarize_industry_classification_history(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
    ) -> Dict[str, Any]:
        """Return summary metadata for official classification history rows."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            summary_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(DISTINCT symbol) AS distinct_symbols,
                    MAX(official_update_time) AS latest_official_update_time,
                    MAX(updated_at) AS latest_updated_at
                FROM industry_classification_history
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                """,
                (taxonomy_system, taxonomy_version or ""),
            ).fetchone()
            exchange_rows = conn.execute(
                """
                SELECT exchange, COUNT(DISTINCT symbol) AS symbol_count
                FROM industry_classification_history
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                GROUP BY exchange
                """,
                (taxonomy_system, taxonomy_version or ""),
            ).fetchall()

        return {
            "total": int((summary_row["total"] if summary_row is not None else 0) or 0),
            "distinct_symbols": int(
                (summary_row["distinct_symbols"] if summary_row is not None else 0) or 0
            ),
            "latest_official_update_time": None
            if summary_row is None
            else summary_row["latest_official_update_time"],
            "latest_updated_at": None if summary_row is None else summary_row["latest_updated_at"],
            "symbols_by_exchange": {
                str(row["exchange"]): int(row["symbol_count"] or 0)
                for row in exchange_rows
                if row["exchange"] is not None
            },
        }

    def list_latest_industry_classification_history(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        exchange: Optional[str] = None,
        instrument_ids: Optional[List[str]] = None,
        symbols: Optional[List[str]] = None,
    ) -> List[OfficialIndustryHistorySnapshot]:
        """Return latest cached official classification rows for selected instruments."""
        conditions = [
            "taxonomy_system = ?",
            "taxonomy_version = ?",
        ]
        params: list[Any] = [taxonomy_system, taxonomy_version or ""]
        if exchange:
            conditions.append("exchange = ?")
            params.append(exchange)

        normalized_ids = [
            str(instrument_id).strip()
            for instrument_id in (instrument_ids or [])
            if str(instrument_id).strip()
        ]
        normalized_symbols = [
            str(symbol).strip()
            for symbol in (symbols or [])
            if str(symbol).strip()
        ]
        lookup_clauses = []
        if normalized_ids:
            lookup_clauses.append(
                "instrument_id IN (" + ",".join("?" for _ in normalized_ids) + ")"
            )
            params.extend(normalized_ids)
        if normalized_symbols:
            lookup_clauses.append(
                "symbol IN (" + ",".join("?" for _ in normalized_symbols) + ")"
            )
            params.extend(normalized_symbols)
        if lookup_clauses:
            conditions.append("(" + " OR ".join(lookup_clauses) + ")")

        where_sql = " AND ".join(conditions)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT
                    instrument_id,
                    symbol,
                    exchange,
                    official_industry_code,
                    official_start_date,
                    official_update_time,
                    source,
                    source_mode,
                    classification_json
                FROM industry_classification_history
                WHERE {where_sql}
                ORDER BY
                    symbol ASC,
                    official_start_date ASC,
                    official_update_time ASC,
                    updated_at ASC
                """,
                params,
            ).fetchall()

        latest_by_symbol: Dict[str, sqlite3.Row] = {}
        for row in rows:
            latest_by_symbol[str(row["symbol"]).strip()] = row

        return [
            OfficialIndustryHistorySnapshot(
                instrument_id=str(row["instrument_id"]),
                symbol=str(row["symbol"]),
                exchange=str(row["exchange"]),
                official_industry_code=str(row["official_industry_code"]),
                start_date=row["official_start_date"],
                update_time=row["official_update_time"],
                source=str(row["source"]),
                source_mode=str(row["source_mode"]),
                raw_payload=self._deserialize_json(row["classification_json"]) or {},
            )
            for row in latest_by_symbol.values()
        ]

    def upsert_official_industry_classification(
        self,
        snapshot: OfficialIndustryClassificationSnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Upsert one latest official industry classification snapshot."""
        now = get_shanghai_time().isoformat()
        classification_json = json.dumps(
            snapshot.classification_json,
            ensure_ascii=False,
            sort_keys=True,
        )

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO industry_official_classifications (
                    instrument_id,
                    symbol,
                    exchange,
                    taxonomy_system,
                    taxonomy_version,
                    official_industry_code,
                    official_start_date,
                    official_update_time,
                    mapped_industry_code,
                    mapped_industry_name,
                    mapped_industry_level,
                    mapped_parent_code,
                    mapping_status,
                    mapping_confidence,
                    source,
                    source_mode,
                    classification_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id, taxonomy_system, taxonomy_version)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    official_industry_code = excluded.official_industry_code,
                    official_start_date = excluded.official_start_date,
                    official_update_time = excluded.official_update_time,
                    mapped_industry_code = excluded.mapped_industry_code,
                    mapped_industry_name = excluded.mapped_industry_name,
                    mapped_industry_level = excluded.mapped_industry_level,
                    mapped_parent_code = excluded.mapped_parent_code,
                    mapping_status = excluded.mapping_status,
                    mapping_confidence = excluded.mapping_confidence,
                    source = excluded.source,
                    source_mode = excluded.source_mode,
                    classification_json = excluded.classification_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    snapshot.instrument_id,
                    snapshot.symbol,
                    snapshot.exchange,
                    snapshot.taxonomy_system,
                    snapshot.taxonomy_version or "",
                    snapshot.official_industry_code,
                    snapshot.official_start_date,
                    snapshot.official_update_time,
                    snapshot.mapped_industry_code,
                    snapshot.mapped_industry_name,
                    snapshot.mapped_industry_level,
                    snapshot.mapped_parent_code,
                    snapshot.mapping_status,
                    snapshot.mapping_confidence,
                    snapshot.source,
                    snapshot.source_mode,
                    classification_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            conn.commit()

    def replace_official_industry_code_mappings(
        self,
        mappings: list[OfficialShenwanCodeMapping],
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        source: str,
        source_mode: str,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Replace the persisted official-code mapping cache for one taxonomy version."""
        now = get_shanghai_time().isoformat()

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                DELETE FROM industry_official_code_mappings
                WHERE taxonomy_system = ? AND taxonomy_version = ?
                """,
                (taxonomy_system, taxonomy_version or ""),
            )
            for mapping in mappings:
                mapping_json = json.dumps(asdict(mapping), ensure_ascii=False, sort_keys=True)
                conn.execute(
                    """
                    INSERT INTO industry_official_code_mappings (
                        taxonomy_system,
                        taxonomy_version,
                        official_industry_code,
                        best_taxonomy_industry_code,
                        mapped_industry_code,
                        mapping_status,
                        mapping_confidence,
                        overlap_count,
                        official_symbol_count,
                        taxonomy_symbol_count,
                        precision,
                        recall,
                        source,
                        source_mode,
                        built_at,
                        mapping_json,
                        ingestion_run_id,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        taxonomy_system,
                        taxonomy_version or "",
                        mapping.official_industry_code,
                        mapping.best_taxonomy_industry_code,
                        mapping.taxonomy_industry_code,
                        "mapped" if mapping.taxonomy_industry_code is not None else "unmapped",
                        mapping.confidence,
                        mapping.overlap_count,
                        mapping.official_symbol_count,
                        mapping.taxonomy_symbol_count,
                        mapping.precision,
                        mapping.recall,
                        source,
                        source_mode,
                        now,
                        mapping_json,
                        ingestion_run_id,
                        now,
                        now,
                    ),
            )
            conn.commit()

    def replace_industry_component_sets(
        self,
        component_sets: Dict[str, set[str]],
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        source: str,
        source_mode: str,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Replace persisted Shenwan component-set cache for one taxonomy version."""
        now = get_shanghai_time().isoformat()

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                DELETE FROM industry_component_sets
                WHERE taxonomy_system = ? AND taxonomy_version = ?
                """,
                (taxonomy_system, taxonomy_version or ""),
            )
            for industry_code, symbols in component_sets.items():
                normalized_symbols = sorted(
                    {
                        str(symbol).strip()
                        for symbol in (symbols or set())
                        if str(symbol).strip()
                    }
                )
                conn.execute(
                    """
                    INSERT INTO industry_component_sets (
                        taxonomy_system,
                        taxonomy_version,
                        industry_code,
                        component_count,
                        source,
                        source_mode,
                        built_at,
                        symbols_json,
                        ingestion_run_id,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        taxonomy_system,
                        taxonomy_version or "",
                        industry_code,
                        len(normalized_symbols),
                        source,
                        source_mode,
                        now,
                        json.dumps(normalized_symbols, ensure_ascii=False, sort_keys=True),
                        ingestion_run_id,
                        now,
                        now,
                    ),
                )
            conn.commit()

    def get_industry_component_sets(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        max_age_days: Optional[int] = None,
    ) -> Dict[str, set[str]]:
        """Return cached Shenwan component sets for one taxonomy version."""
        params: list[Any] = [taxonomy_system, taxonomy_version or ""]
        age_filter = ""
        if max_age_days is not None:
            cutoff = get_shanghai_time() - timedelta(days=int(max_age_days))
            age_filter = " AND built_at >= ?"
            params.append(cutoff.isoformat())

        sql = f"""
            SELECT industry_code, symbols_json
            FROM industry_component_sets
            WHERE taxonomy_system = ?
              AND taxonomy_version = ?
              {age_filter}
            ORDER BY industry_code ASC
        """

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()

        component_sets: Dict[str, set[str]] = {}
        for row in rows:
            symbols = self._deserialize_json(row["symbols_json"]) or []
            component_sets[str(row["industry_code"])] = {
                str(symbol).strip() for symbol in symbols if str(symbol).strip()
            }
        return component_sets

    def list_industry_component_set_records(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        industry_code: Optional[str] = None,
        max_age_days: Optional[int] = None,
        limit: Optional[int] = 100,
        offset: int = 0,
        include_symbols: bool = True,
    ) -> list[Dict[str, Any]]:
        """List cached Shenwan component-set rows with metadata."""
        where_sql, params = self._industry_component_set_filter_sql(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            industry_code=industry_code,
            max_age_days=max_age_days,
        )
        sql = f"""
            SELECT
                taxonomy_system,
                taxonomy_version,
                industry_code,
                component_count,
                source,
                source_mode,
                built_at,
                symbols_json,
                ingestion_run_id,
                created_at,
                updated_at
            FROM industry_component_sets
            {where_sql}
            ORDER BY industry_code ASC
        """
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params.extend((int(limit), max(int(offset), 0)))

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()

        records: list[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            symbols_json = item.pop("symbols_json", None)
            if include_symbols:
                item["symbols"] = self._deserialize_json(symbols_json) or []
            records.append(item)
        return records

    def list_industry_component_set_records_from_memberships(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        industry_code: Optional[str] = None,
        max_age_days: Optional[int] = None,
        limit: Optional[int] = 100,
        offset: int = 0,
        include_symbols: bool = True,
    ) -> tuple[list[Dict[str, Any]], int]:
        """Derive Shenwan component sets from authoritative stock memberships."""
        taxonomy_version = taxonomy_version or ""
        conditions = [
            "m.taxonomy_system = ?",
            "m.taxonomy_version = ?",
            "m.mapping_status = 'authoritative'",
        ]
        params: list[Any] = [taxonomy_system, taxonomy_version]

        group_override: Optional[str] = None
        if industry_code:
            with self.get_connection() as conn:
                self._apply_pragmas(conn)
                node = conn.execute(
                    """
                    SELECT industry_level
                    FROM industry_taxonomy
                    WHERE taxonomy_system = ?
                      AND taxonomy_version = ?
                      AND industry_code = ?
                      AND is_active = 1
                    LIMIT 1
                    """,
                    (taxonomy_system, taxonomy_version, industry_code),
                ).fetchone()

            group_override = industry_code
            if node and int(node["industry_level"] or 0) == 1:
                conditions.append("m.sw_l1_code = ?")
                params.append(industry_code)
            elif node and int(node["industry_level"] or 0) == 2:
                conditions.append("m.sw_l2_code = ?")
                params.append(industry_code)
            elif node and int(node["industry_level"] or 0) == 3:
                conditions.append("(m.sw_l3_code = ? OR m.industry_code = ?)")
                params.extend((industry_code, industry_code))
            else:
                conditions.append(
                    """
                    (
                        m.industry_code = ?
                        OR m.sw_l1_code = ?
                        OR m.sw_l2_code = ?
                        OR m.sw_l3_code = ?
                    )
                    """
                )
                params.extend((industry_code, industry_code, industry_code, industry_code))

        if max_age_days is not None and max_age_days >= 0:
            cutoff = get_shanghai_time() - timedelta(days=int(max_age_days))
            conditions.append("m.updated_at >= ?")
            params.append(cutoff.isoformat())

        where_sql = " AND ".join(conditions)
        sql = f"""
            SELECT
                m.instrument_id,
                m.symbol,
                m.industry_code,
                m.source,
                m.source_mode,
                m.ingestion_run_id,
                m.created_at,
                m.updated_at
            FROM industry_memberships m
            WHERE {where_sql}
            ORDER BY m.industry_code ASC, m.symbol ASC, m.instrument_id ASC
        """

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()

        grouped: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            code = group_override or str(row["industry_code"])
            item = grouped.setdefault(
                code,
                {
                    "taxonomy_system": taxonomy_system,
                    "taxonomy_version": taxonomy_version,
                    "industry_code": code,
                    "symbols": set(),
                    "sources": set(),
                    "source_modes": set(),
                    "ingestion_run_ids": set(),
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                },
            )
            symbol = str(row["symbol"] or "").strip()
            if symbol:
                item["symbols"].add(symbol)
            if row["source"]:
                item["sources"].add(str(row["source"]))
            if row["source_mode"]:
                item["source_modes"].add(str(row["source_mode"]))
            if row["ingestion_run_id"] is not None:
                item["ingestion_run_ids"].add(int(row["ingestion_run_id"]))
            if row["created_at"] and (
                item["created_at"] is None or row["created_at"] < item["created_at"]
            ):
                item["created_at"] = row["created_at"]
            if row["updated_at"] and (
                item["updated_at"] is None or row["updated_at"] > item["updated_at"]
            ):
                item["updated_at"] = row["updated_at"]

        records: list[Dict[str, Any]] = []
        for code in sorted(grouped):
            item = grouped[code]
            symbols = sorted(item["symbols"])
            sources = sorted(item["sources"])
            source_modes = sorted(item["source_modes"])
            run_ids = sorted(item["ingestion_run_ids"])
            record = {
                "taxonomy_system": item["taxonomy_system"],
                "taxonomy_version": item["taxonomy_version"],
                "industry_code": code,
                "component_count": len(symbols),
                "source": sources[0] if len(sources) == 1 else "industry_memberships",
                "source_mode": source_modes[0] if len(source_modes) == 1 else "derived",
                "built_at": item["updated_at"],
                "ingestion_run_id": run_ids[-1] if len(run_ids) == 1 else None,
                "created_at": item["created_at"],
                "updated_at": item["updated_at"],
            }
            if include_symbols:
                record["symbols"] = symbols
            records.append(record)

        total = len(records)
        if limit is not None:
            start = max(int(offset), 0)
            end = start + int(limit)
            records = records[start:end]
        return records, total

    def count_industry_component_sets(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        industry_code: Optional[str] = None,
        max_age_days: Optional[int] = None,
    ) -> int:
        """Count cached Shenwan component-set rows for one taxonomy version."""
        where_sql, params = self._industry_component_set_filter_sql(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            industry_code=industry_code,
            max_age_days=max_age_days,
        )
        sql = f"""
            SELECT COUNT(*)
            FROM industry_component_sets
            {where_sql}
        """
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            count = conn.execute(sql, params).fetchone()[0]
        return int(count or 0)

    def _industry_component_set_filter_sql(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        industry_code: Optional[str] = None,
        max_age_days: Optional[int] = None,
    ) -> tuple[str, list[Any]]:
        """Build a WHERE clause for Shenwan component-set cache queries."""
        conditions = ["taxonomy_system = ?", "taxonomy_version = ?"]
        params: list[Any] = [taxonomy_system, taxonomy_version or ""]
        if industry_code:
            conditions.append("industry_code = ?")
            params.append(industry_code)
        if max_age_days is not None and max_age_days >= 0:
            cutoff = (get_shanghai_time() - timedelta(days=int(max_age_days))).isoformat()
            conditions.append("built_at >= ?")
            params.append(cutoff)
        return "WHERE " + " AND ".join(conditions), params

    def get_latest_industry_component_set_cache_info(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
    ) -> Optional[Dict[str, Any]]:
        """Return the latest persisted component-set cache build info."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT
                    source,
                    source_mode,
                    built_at,
                    updated_at
                FROM industry_component_sets
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                ORDER BY COALESCE(built_at, updated_at) DESC, updated_at DESC
                LIMIT 1
                """,
                (taxonomy_system, taxonomy_version or ""),
            ).fetchone()
        return None if row is None else dict(row)

    def get_official_industry_code_mappings(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        max_age_days: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        """Return persisted official-code mapping rows for one taxonomy version."""
        return self.list_official_industry_code_mappings(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            max_age_days=max_age_days,
            limit=None,
            offset=0,
            include_mapping=True,
        )

    def list_official_industry_code_mappings(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        mapping_status: Optional[str] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        max_age_days: Optional[int] = None,
        limit: Optional[int] = 100,
        offset: int = 0,
        include_mapping: bool = True,
    ) -> list[Dict[str, Any]]:
        """List persisted official-code mapping rows with optional filters."""
        where_sql, params = self._official_mapping_filter_sql(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            mapping_status=mapping_status,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
        )
        sql = f"""
            SELECT
                taxonomy_system,
                taxonomy_version,
                official_industry_code,
                best_taxonomy_industry_code,
                mapped_industry_code,
                mapping_status,
                mapping_confidence,
                overlap_count,
                official_symbol_count,
                taxonomy_symbol_count,
                precision,
                recall,
                source,
                source_mode,
                built_at,
                mapping_json,
                ingestion_run_id,
                created_at,
                updated_at
            FROM industry_official_code_mappings
            {where_sql}
            ORDER BY
                CASE
                    WHEN mapping_status = 'unmapped' THEN 0
                    ELSE 1
                END,
                official_industry_code ASC
        """
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params.extend((int(limit), max(int(offset), 0)))

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()

        records: list[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            mapping_json = item.pop("mapping_json", None)
            if include_mapping:
                item["mapping"] = self._deserialize_json(mapping_json)
            records.append(item)
        return records

    def count_official_industry_code_mappings(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        mapping_status: Optional[str] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        max_age_days: Optional[int] = None,
    ) -> int:
        """Count persisted official-code mapping rows with optional filters."""
        where_sql, params = self._official_mapping_filter_sql(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            mapping_status=mapping_status,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
        )
        sql = f"""
            SELECT COUNT(*)
            FROM industry_official_code_mappings
            {where_sql}
        """
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            count = conn.execute(sql, params).fetchone()[0]
        return int(count or 0)

    def summarize_official_industry_code_mappings(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        max_age_days: Optional[int] = None,
    ) -> Dict[str, int]:
        """Return mapping-status counts for one taxonomy version."""
        where_sql, params = self._official_mapping_filter_sql(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
        )
        sql = f"""
            SELECT mapping_status, COUNT(*) AS row_count
            FROM industry_official_code_mappings
            {where_sql}
            GROUP BY mapping_status
        """
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()
        summary = {"mapped": 0, "unmapped": 0}
        for row in rows:
            summary[str(row["mapping_status"])] = int(row["row_count"] or 0)
        return summary

    def get_latest_official_industry_code_mapping_cache_info(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
    ) -> Optional[Dict[str, Any]]:
        """Return the latest persisted mapping-cache build info."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT
                    source,
                    source_mode,
                    built_at,
                    updated_at
                FROM industry_official_code_mappings
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                ORDER BY COALESCE(built_at, updated_at) DESC, updated_at DESC
                LIMIT 1
                """,
                (taxonomy_system, taxonomy_version or ""),
            ).fetchone()
        return None if row is None else dict(row)

    def summarize_official_industry_classifications(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
    ) -> Dict[str, Any]:
        """Return mapping-status summary for latest official classifications."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            count_rows = conn.execute(
                """
                SELECT mapping_status, COUNT(*) AS row_count
                FROM industry_official_classifications
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                GROUP BY mapping_status
                """,
                (taxonomy_system, taxonomy_version or ""),
            ).fetchall()
            summary_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    MAX(updated_at) AS latest_updated_at,
                    MAX(official_update_time) AS latest_official_update_time
                FROM industry_official_classifications
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                """,
                (taxonomy_system, taxonomy_version or ""),
            ).fetchone()

        counts = {"mapped": 0, "unmapped": 0}
        for row in count_rows:
            counts[str(row["mapping_status"])] = int(row["row_count"] or 0)

        return {
            "total": int((summary_row["total"] if summary_row is not None else 0) or 0),
            "counts": counts,
            "latest_updated_at": None if summary_row is None else summary_row["latest_updated_at"],
            "latest_official_update_time": None
            if summary_row is None
            else summary_row["latest_official_update_time"],
        }

    def summarize_industry_memberships(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
    ) -> Dict[str, Any]:
        """Return mapping-status summary for normalized industry memberships."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            count_rows = conn.execute(
                """
                SELECT mapping_status, COUNT(*) AS row_count
                FROM industry_memberships
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                GROUP BY mapping_status
                """,
                (taxonomy_system, taxonomy_version or ""),
            ).fetchall()
            summary_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    MAX(updated_at) AS latest_updated_at,
                    MAX(data_as_of) AS latest_data_as_of
                FROM industry_memberships
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                """,
                (taxonomy_system, taxonomy_version or ""),
            ).fetchone()

        counts = {"authoritative": 0, "reference_only": 0}
        for row in count_rows:
            counts[str(row["mapping_status"])] = int(row["row_count"] or 0)

        return {
            "total": int((summary_row["total"] if summary_row is not None else 0) or 0),
            "counts": counts,
            "latest_updated_at": None if summary_row is None else summary_row["latest_updated_at"],
            "latest_data_as_of": None if summary_row is None else summary_row["latest_data_as_of"],
        }

    def count_industry_memberships_by_exchange(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        mapping_status: Optional[str] = None,
    ) -> Dict[str, int]:
        """Count industry memberships per exchange with optional status filter."""
        conditions = [
            "taxonomy_system = ?",
            "taxonomy_version = ?",
        ]
        params: list[Any] = [taxonomy_system, taxonomy_version or ""]
        if mapping_status:
            conditions.append("mapping_status = ?")
            params.append(mapping_status)

        where_sql = " AND ".join(conditions)
        sql = f"""
            SELECT exchange, COUNT(*) AS row_count
            FROM industry_memberships
            WHERE {where_sql}
            GROUP BY exchange
        """
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()

        return {
            str(row["exchange"]): int(row["row_count"] or 0)
            for row in rows
            if row["exchange"] is not None
        }

    def list_industry_membership_instrument_ids(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        mapping_status: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> List[str]:
        """List distinct instrument ids covered by industry memberships."""
        conditions = [
            "taxonomy_system = ?",
            "taxonomy_version = ?",
        ]
        params: list[Any] = [taxonomy_system, taxonomy_version or ""]
        if mapping_status:
            conditions.append("mapping_status = ?")
            params.append(mapping_status)
        if exchange:
            conditions.append("exchange = ?")
            params.append(exchange)

        where_sql = " AND ".join(conditions)
        sql = f"""
            SELECT DISTINCT instrument_id
            FROM industry_memberships
            WHERE {where_sql}
              AND instrument_id IS NOT NULL
              AND instrument_id != ''
            ORDER BY instrument_id ASC
        """
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()

        return [str(row["instrument_id"]) for row in rows if row["instrument_id"]]

    def get_official_industry_code_mapping(
        self,
        official_industry_code: str,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        include_mapping: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Fetch one persisted official-code mapping row."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT
                    taxonomy_system,
                    taxonomy_version,
                    official_industry_code,
                    best_taxonomy_industry_code,
                    mapped_industry_code,
                    mapping_status,
                    mapping_confidence,
                    overlap_count,
                    official_symbol_count,
                    taxonomy_symbol_count,
                    precision,
                    recall,
                    source,
                    source_mode,
                    built_at,
                    mapping_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                FROM industry_official_code_mappings
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                  AND official_industry_code = ?
                LIMIT 1
                """,
                (taxonomy_system, taxonomy_version or "", official_industry_code),
            ).fetchone()
        if row is None:
            return None
        item = dict(row)
        mapping_json = item.pop("mapping_json", None)
        if include_mapping:
            item["mapping"] = self._deserialize_json(mapping_json)
        return item

    def list_unmapped_official_industry_code_backlog(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        max_age_days: Optional[int] = None,
        limit: Optional[int] = 100,
        offset: int = 0,
        include_mapping: bool = True,
        sample_limit: int = 5,
    ) -> list[Dict[str, Any]]:
        """List prioritized unmapped official-code backlog rows."""
        where_sql, params = self._official_mapping_filter_sql(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            mapping_status="unmapped",
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
            table_alias="m",
        )
        sql = f"""
            WITH classification_counts AS (
                SELECT
                    official_industry_code,
                    COUNT(*) AS current_classification_count
                FROM industry_official_classifications
                WHERE taxonomy_system = ?
                  AND taxonomy_version = ?
                GROUP BY official_industry_code
            )
            SELECT
                m.taxonomy_system,
                m.taxonomy_version,
                m.official_industry_code,
                m.best_taxonomy_industry_code,
                m.mapped_industry_code,
                m.mapping_status,
                m.mapping_confidence,
                m.overlap_count,
                m.official_symbol_count,
                m.taxonomy_symbol_count,
                m.precision,
                m.recall,
                m.source,
                m.source_mode,
                m.built_at,
                m.mapping_json,
                m.ingestion_run_id,
                m.created_at,
                m.updated_at,
                COALESCE(cc.current_classification_count, 0) AS current_classification_count
            FROM industry_official_code_mappings AS m
            LEFT JOIN classification_counts AS cc
              ON cc.official_industry_code = m.official_industry_code
            {where_sql}
            ORDER BY
                current_classification_count DESC,
                m.official_symbol_count DESC,
                m.overlap_count DESC,
                m.official_industry_code ASC
        """
        query_params: list[Any] = [
            taxonomy_system,
            taxonomy_version or "",
            *params,
        ]
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            query_params.extend((int(limit), max(int(offset), 0)))

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, query_params).fetchall()

        records: list[Dict[str, Any]] = []
        official_codes = [str(row["official_industry_code"]) for row in rows]
        exchange_counts = self._get_official_classification_exchange_counts(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            official_codes=official_codes,
        )
        sample_instruments = self._get_official_classification_sample_instruments(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            official_codes=official_codes,
            sample_limit=sample_limit,
        )

        for row in rows:
            item = dict(row)
            mapping_json = item.pop("mapping_json", None)
            official_code = str(item["official_industry_code"])
            item["impacted_exchange_counts"] = exchange_counts.get(official_code, {})
            item["sample_instruments"] = sample_instruments.get(official_code, [])
            if include_mapping:
                item["mapping"] = self._deserialize_json(mapping_json)
            records.append(item)
        return records

    def summarize_unmapped_official_industry_code_backlog(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        max_age_days: Optional[int] = None,
    ) -> Dict[str, int]:
        """Summarize backlog size and current impact for unmapped official codes."""
        where_sql, params = self._official_mapping_filter_sql(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            mapping_status="unmapped",
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
            table_alias="m",
        )
        sql = f"""
            SELECT COALESCE(SUM(backlog.current_classification_count), 0) AS current_classification_total
            FROM (
                SELECT
                    m.official_industry_code,
                    COUNT(c.instrument_id) AS current_classification_count
                FROM industry_official_code_mappings AS m
                LEFT JOIN industry_official_classifications AS c
                  ON c.taxonomy_system = m.taxonomy_system
                 AND c.taxonomy_version = m.taxonomy_version
                 AND c.official_industry_code = m.official_industry_code
                {where_sql}
                GROUP BY m.official_industry_code
            ) AS backlog
        """
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            current_classification_total = conn.execute(sql, params).fetchone()[0]

        return {
            "official_code_total": self.count_official_industry_code_mappings(
                taxonomy_system=taxonomy_system,
                taxonomy_version=taxonomy_version,
                mapping_status="unmapped",
                source=source,
                source_mode=source_mode,
                max_age_days=max_age_days,
            ),
            "current_classification_total": int(current_classification_total or 0),
        }

    def _official_mapping_filter_sql(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        mapping_status: Optional[str] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        max_age_days: Optional[int] = None,
        table_alias: Optional[str] = None,
    ) -> tuple[str, list[Any]]:
        """Build a reusable WHERE clause for official mapping cache queries."""
        prefix = f"{table_alias}." if table_alias else ""
        conditions = [
            f"{prefix}taxonomy_system = ?",
            f"{prefix}taxonomy_version = ?",
        ]
        params: list[Any] = [taxonomy_system, taxonomy_version or ""]
        if mapping_status:
            conditions.append(f"{prefix}mapping_status = ?")
            params.append(mapping_status)
        if source:
            conditions.append(f"{prefix}source = ?")
            params.append(source)
        if source_mode:
            conditions.append(f"{prefix}source_mode = ?")
            params.append(source_mode)
        if max_age_days is not None and max_age_days >= 0:
            cutoff = (
                get_shanghai_time() - timedelta(days=int(max_age_days))
            ).isoformat()
            conditions.append(f"{prefix}built_at >= ?")
            params.append(cutoff)
        return "WHERE " + " AND ".join(conditions), params

    def _get_official_classification_exchange_counts(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        official_codes: list[str],
    ) -> Dict[str, Dict[str, int]]:
        """Return current impacted exchange counts for a set of official codes."""
        if not official_codes:
            return {}
        placeholders = ", ".join("?" for _ in official_codes)
        params: list[Any] = [taxonomy_system, taxonomy_version or "", *official_codes]
        sql = f"""
            SELECT official_industry_code, exchange, COUNT(*) AS row_count
            FROM industry_official_classifications
            WHERE taxonomy_system = ?
              AND taxonomy_version = ?
              AND official_industry_code IN ({placeholders})
            GROUP BY official_industry_code, exchange
            ORDER BY official_industry_code ASC, exchange ASC
        """
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()

        result: Dict[str, Dict[str, int]] = {}
        for row in rows:
            official_code = str(row["official_industry_code"])
            result.setdefault(official_code, {})[str(row["exchange"])] = int(
                row["row_count"] or 0
            )
        return result

    def _get_official_classification_sample_instruments(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: str,
        official_codes: list[str],
        sample_limit: int,
    ) -> Dict[str, list[str]]:
        """Return sample instruments for a set of official codes."""
        if not official_codes:
            return {}
        placeholders = ", ".join("?" for _ in official_codes)
        params: list[Any] = [taxonomy_system, taxonomy_version or "", *official_codes]
        sql = f"""
            SELECT official_industry_code, instrument_id
            FROM industry_official_classifications
            WHERE taxonomy_system = ?
              AND taxonomy_version = ?
              AND official_industry_code IN ({placeholders})
            ORDER BY official_industry_code ASC, exchange ASC, instrument_id ASC
        """
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()

        result: Dict[str, list[str]] = {}
        per_code_limit = max(1, int(sample_limit))
        for row in rows:
            official_code = str(row["official_industry_code"])
            bucket = result.setdefault(official_code, [])
            if len(bucket) >= per_code_limit:
                continue
            bucket.append(str(row["instrument_id"]))
        return result

    def upsert_industry_taxonomy(
        self,
        snapshot: IndustryTaxonomySnapshot,
        *,
        ingestion_run_id: Optional[int] = None,
        return_stats: bool = False,
    ) -> Optional[Dict[str, int]]:
        """Upsert one normalized industry taxonomy node."""
        now = get_shanghai_time().isoformat()

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            stats = self._upsert_industry_taxonomy_row(
                conn,
                snapshot,
                ingestion_run_id=ingestion_run_id,
                now=now,
            )
            conn.commit()
        return stats if return_stats else None

    def list_industry_taxonomy(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: Optional[str] = None,
        active_only: bool = True,
    ) -> List[IndustryTaxonomySnapshot]:
        """Return persisted taxonomy nodes for one taxonomy version."""
        where = "taxonomy_system = ? AND taxonomy_version = ?"
        params: list[Any] = [taxonomy_system, taxonomy_version or ""]
        if active_only:
            where += " AND is_active = 1"

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT
                    taxonomy_system,
                    taxonomy_version,
                    industry_code,
                    industry_name,
                    industry_level,
                    parent_code,
                    sw_index_code,
                    aliases_json,
                    source_classification,
                    source,
                    source_mode
                FROM industry_taxonomy
                WHERE {where}
                ORDER BY industry_level ASC, industry_code ASC
                """,
                params,
            ).fetchall()

        return [
            IndustryTaxonomySnapshot(
                taxonomy_system=str(row["taxonomy_system"]),
                taxonomy_version=str(row["taxonomy_version"] or "") or None,
                industry_code=str(row["industry_code"]),
                industry_name=str(row["industry_name"]),
                industry_level=int(row["industry_level"]),
                parent_code=row["parent_code"],
                sw_index_code=row["sw_index_code"],
                aliases_json=self._deserialize_json(row["aliases_json"]) or {},
                source_classification=row["source_classification"],
                source=str(row["source"]),
                source_mode=str(row["source_mode"]),
                raw_payload={"cache_source": "industry_taxonomy"},
            )
            for row in rows
        ]

    def list_industry_taxonomy_records(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: Optional[str] = None,
        industry_level: Optional[int] = None,
        parent_code: Optional[str] = None,
        industry_code: Optional[str] = None,
        sw_index_code: Optional[str] = None,
        active_only: bool = True,
        limit: Optional[int] = 100,
        offset: int = 0,
    ) -> list[Dict[str, Any]]:
        """List persisted taxonomy nodes as API-ready dictionaries."""
        where_sql, params = self._industry_taxonomy_filter_sql(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            industry_level=industry_level,
            parent_code=parent_code,
            industry_code=industry_code,
            sw_index_code=sw_index_code,
            active_only=active_only,
        )
        sql = f"""
            SELECT
                taxonomy_system,
                taxonomy_version,
                industry_code,
                industry_name,
                industry_level,
                parent_code,
                sw_index_code,
                aliases_json,
                source_classification,
                source,
                source_mode,
                is_active,
                created_at,
                updated_at
            FROM industry_taxonomy
            {where_sql}
            ORDER BY industry_level ASC, industry_code ASC
        """
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params.extend((int(limit), max(int(offset), 0)))

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()

        records: list[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["is_active"] = bool(item.get("is_active"))
            aliases_json = item.pop("aliases_json", None)
            item["aliases"] = self._deserialize_json(aliases_json) or {}
            records.append(item)
        return records

    def count_industry_taxonomy_records(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: Optional[str] = None,
        industry_level: Optional[int] = None,
        parent_code: Optional[str] = None,
        industry_code: Optional[str] = None,
        sw_index_code: Optional[str] = None,
        active_only: bool = True,
    ) -> int:
        """Count persisted taxonomy nodes with optional filters."""
        where_sql, params = self._industry_taxonomy_filter_sql(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            industry_level=industry_level,
            parent_code=parent_code,
            industry_code=industry_code,
            sw_index_code=sw_index_code,
            active_only=active_only,
        )
        sql = f"""
            SELECT COUNT(*)
            FROM industry_taxonomy
            {where_sql}
        """
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            count = conn.execute(sql, params).fetchone()[0]
        return int(count or 0)

    def _industry_taxonomy_filter_sql(
        self,
        *,
        taxonomy_system: str,
        taxonomy_version: Optional[str] = None,
        industry_level: Optional[int] = None,
        parent_code: Optional[str] = None,
        industry_code: Optional[str] = None,
        sw_index_code: Optional[str] = None,
        active_only: bool = True,
    ) -> tuple[str, list[Any]]:
        """Build a WHERE clause for industry taxonomy queries."""
        conditions = ["taxonomy_system = ?", "taxonomy_version = ?"]
        params: list[Any] = [taxonomy_system, taxonomy_version or ""]
        if industry_level is not None:
            conditions.append("industry_level = ?")
            params.append(int(industry_level))
        if parent_code is not None:
            conditions.append("parent_code = ?")
            params.append(parent_code)
        if industry_code:
            conditions.append("industry_code = ?")
            params.append(industry_code)
        if sw_index_code:
            conditions.append("sw_index_code = ?")
            params.append(sw_index_code)
        if active_only:
            conditions.append("is_active = 1")
        return "WHERE " + " AND ".join(conditions), params

    def get_company_profile(
        self,
        instrument_id: str,
        *,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Fetch one normalized company profile row."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT
                    instrument_id,
                    symbol,
                    company_name,
                    short_name,
                    exchange,
                    market,
                    listed_date,
                    industry_raw,
                    sector_raw,
                    status,
                    source,
                    source_mode,
                    data_as_of,
                    profile_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                FROM company_profiles
                WHERE instrument_id = ?
                """,
                (instrument_id,),
            ).fetchone()

        if row is None:
            return None

        data = dict(row)
        profile_json = data.pop("profile_json", None)
        if include_snapshot:
            data["profile"] = self._deserialize_json(profile_json)
        return data

    def get_financial_summary(
        self,
        instrument_id: str,
        *,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Fetch one normalized financial summary row."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT
                    instrument_id,
                    symbol,
                    exchange,
                    report_date,
                    pub_date,
                    fiscal_year,
                    fiscal_quarter,
                    currency,
                    schema_version,
                    roe,
                    gross_margin,
                    net_margin,
                    current_ratio,
                    quick_ratio,
                    liability_to_asset,
                    yoy_asset,
                    yoy_equity,
                    yoy_net_profit,
                    cfo_to_revenue,
                    cfo_to_net_profit,
                    asset_turnover,
                    eps,
                    source,
                    source_mode,
                    data_as_of,
                    summary_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                FROM financial_summaries
                WHERE instrument_id = ?
                """,
                (instrument_id,),
            ).fetchone()

        if row is None:
            return None

        data = dict(row)
        summary_json = data.pop("summary_json", None)
        if include_snapshot:
            data["summary"] = self._deserialize_json(summary_json)
        return data

    def get_financial_statement_bundle(
        self,
        instrument_id: str,
        *,
        include_statements: bool = True,
        report_period: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch one latest financial statement bundle for an instrument."""
        filters = ["instrument_id = ?"]
        params: List[Any] = [instrument_id]
        if report_period:
            filters.append("report_period = ?")
            params.append(report_period)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            facts_row = conn.execute(
                f"""
                SELECT
                    instrument_id,
                    symbol,
                    exchange,
                    report_period,
                    report_type,
                    statement_family,
                    data_available_date,
                    source_file_id,
                    filing_id,
                    publish_date,
                    fiscal_year,
                    fiscal_quarter,
                    currency,
                    schema_version,
                    revenue,
                    gross_profit,
                    operating_profit,
                    pre_tax_profit,
                    net_income,
                    operating_cf,
                    total_cf,
                    total_assets,
                    total_liabilities,
                    equity,
                    current_assets,
                    current_liabilities,
                    inventory,
                    receivables,
                    fixed_assets,
                    intangible_assets,
                    shares_outstanding,
                    source,
                    source_mode,
                    data_as_of,
                    facts_json,
                    lineage_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                FROM financial_facts
                WHERE {' AND '.join(filters)}
                ORDER BY report_period DESC, updated_at DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
            if facts_row is None:
                return None

            report_period = facts_row["report_period"]
            indicators_row = conn.execute(
                """
                SELECT
                    instrument_id,
                    report_period,
                    gross_margin,
                    operating_margin,
                    net_margin,
                    roe,
                    roa,
                    current_ratio,
                    quick_ratio,
                    asset_liability_ratio,
                    revenue_per_share,
                    operating_cf_to_revenue,
                    operating_cf_to_net_income,
                    book_value_per_share,
                    indicators_json,
                    source,
                    source_mode,
                    data_as_of,
                    ingestion_run_id,
                    created_at,
                    updated_at
                FROM financial_indicator_snapshots
                WHERE instrument_id = ? AND report_period = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (instrument_id, report_period),
            ).fetchone()
            statement_rows = conn.execute(
                """
                SELECT
                    instrument_id,
                    symbol,
                    exchange,
                    statement_type,
                    report_period,
                    publish_date,
                    fiscal_year,
                    fiscal_quarter,
                    currency,
                    schema_version,
                    source,
                    source_mode,
                    data_as_of,
                    statement_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                FROM financial_statements_raw
                WHERE instrument_id = ? AND report_period = ?
                ORDER BY statement_type ASC
                """,
                (instrument_id, report_period),
            ).fetchall()

        facts = dict(facts_row)
        facts_json = facts.pop("facts_json", None)
        lineage_json = facts.pop("lineage_json", None)
        indicators = dict(indicators_row) if indicators_row is not None else None
        if indicators is not None:
            indicators_json = indicators.pop("indicators_json", None)
            indicators["details"] = self._deserialize_json(indicators_json)

        statements = []
        for row in statement_rows:
            item = dict(row)
            statement_json = item.pop("statement_json", None)
            item["statement"] = self._deserialize_json(statement_json)
            statements.append(item)

        facts["facts"] = self._deserialize_json(facts_json)
        facts["lineage"] = self._deserialize_json(lineage_json) or {}
        facts["indicators"] = indicators
        if include_statements:
            facts["statements"] = statements
        return facts

    def get_financial_statement_bundles(
        self,
        instrument_id: str,
        *,
        include_statements: bool = True,
        report_periods: Optional[List[str]] = None,
        limit: int = 12,
    ) -> List[Dict[str, Any]]:
        """Fetch multiple financial statement bundles for one instrument."""
        normalized_limit = max(1, min(int(limit), 80))
        requested_periods = [
            str(item).strip()
            for item in (report_periods or [])
            if str(item).strip()
        ]
        params: List[Any] = [instrument_id]
        if requested_periods:
            placeholders = ",".join("?" for _ in requested_periods)
            where_period = f"AND report_period IN ({placeholders})"
            params.extend(requested_periods)
            limit_clause = ""
        else:
            where_period = ""
            limit_clause = "LIMIT ?"
            params.append(normalized_limit)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT DISTINCT report_period
                FROM financial_facts
                WHERE instrument_id = ?
                {where_period}
                ORDER BY report_period DESC
                {limit_clause}
                """,
                params,
            ).fetchall()

        periods = [str(row["report_period"]) for row in rows if row["report_period"]]
        if requested_periods:
            periods = periods[:normalized_limit]
        bundles: List[Dict[str, Any]] = []
        for period in periods:
            bundle = self.get_financial_statement_bundle(
                instrument_id,
                include_statements=include_statements,
                report_period=period,
            )
            if bundle is not None:
                bundles.append(bundle)
        return bundles

    def get_financial_core_facts(
        self,
        instrument_id: str,
        *,
        include_history: bool = False,
        report_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Read normalized financial facts through hot/history logical tiers."""
        tables = ["financial_core_facts_hot"]
        if include_history:
            tables.append("financial_core_facts_history")
        select_sql = " UNION ALL ".join(
            f"SELECT *, '{table_name}' AS physical_table FROM {table_name}"
            for table_name in tables
        )
        filters = ["instrument_id = ?"]
        params: List[Any] = [instrument_id]
        if report_period:
            filters.append("report_period = ?")
            params.append(report_period)
        limit_clause = "" if limit is None else "LIMIT ?"
        if limit is not None:
            params.append(int(limit))

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT *
                FROM ({select_sql})
                WHERE {' AND '.join(filters)}
                ORDER BY report_period DESC, updated_at DESC
                {limit_clause}
                """,
                params,
            ).fetchall()

        return [self._decode_financial_core_fact_row(row) for row in rows]

    def summarize_financial_period_coverage(
        self,
        *,
        expected_periods: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        include_history: bool = True,
    ) -> Dict[str, Any]:
        """Summarize core-fact period coverage for readiness and rollout checks."""
        tables = ["financial_core_facts_hot"]
        if include_history:
            tables.append("financial_core_facts_history")
        select_sql = " UNION ALL ".join(
            f"SELECT instrument_id, report_period FROM {table_name}"
            for table_name in tables
        )

        filters: List[str] = []
        params: List[Any] = []
        if instrument_ids:
            placeholders = ",".join("?" for _ in instrument_ids)
            filters.append(f"instrument_id IN ({placeholders})")
            params.extend(instrument_ids)
        where_clause = "" if not filters else "WHERE " + " AND ".join(filters)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT instrument_id, report_period
                FROM ({select_sql})
                {where_clause}
                GROUP BY instrument_id, report_period
                """,
                params,
            ).fetchall()

        periods_by_instrument: Dict[str, set[str]] = {}
        for row in rows:
            periods_by_instrument.setdefault(row["instrument_id"], set()).add(
                row["report_period"]
            )

        missing: Dict[str, List[str]] = {}
        expected = expected_periods or []
        if expected:
            target_instruments = instrument_ids or sorted(periods_by_instrument)
            for target in target_instruments:
                present = periods_by_instrument.get(target, set())
                missing_periods = [period for period in expected if period not in present]
                if missing_periods:
                    missing[target] = missing_periods

        covered_period_rows = sum(len(periods) for periods in periods_by_instrument.values())
        expected_rows = len(expected) * len(instrument_ids or periods_by_instrument)
        return {
            "instrument_count": len(periods_by_instrument),
            "covered_period_rows": covered_period_rows,
            "expected_period_rows": expected_rows,
            "coverage_ratio": None
            if expected_rows == 0
            else covered_period_rows / expected_rows,
            "missing_periods": missing,
        }

    def detect_financial_coverage_gaps(
        self,
        *,
        expected_periods: List[str],
        instrument_ids: List[str],
        required_core_facts: List[str],
        include_history: bool = True,
        fallback_sources: Optional[List[str]] = None,
        detail_limit: int = 50,
    ) -> Dict[str, Any]:
        """Detect financial statement coverage gaps through storage semantics."""
        expected = sorted(set(expected_periods or []))
        instruments = sorted(set(instrument_ids or []))
        required_fields = [
            field
            for field in required_core_facts
            if field in self._financial_core_fact_fields()
        ]
        fallback_source_set = set(fallback_sources or ["akshare"])
        core_rows = self._fetch_financial_core_rows_for_gap_detection(
            expected_periods=expected,
            instrument_ids=instruments,
            include_history=include_history,
        )
        manifest_rows = self._fetch_financial_manifest_rows_for_gap_detection(
            expected_periods=expected,
            instrument_ids=instruments,
        )

        core_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
        source_distribution: Dict[str, int] = {}
        source_mode_distribution: Dict[str, int] = {}
        fallback_core_rows = 0
        semantic_warning_count = 0
        semantic_warnings: List[Dict[str, Any]] = []
        for row in core_rows:
            item = dict(row)
            key = (item["instrument_id"], item["report_period"])
            core_by_key[key] = item
            source = str(item.get("source") or "unknown")
            mode = str(item.get("source_mode") or "unknown")
            source_distribution[source] = source_distribution.get(source, 0) + 1
            source_mode_distribution[mode] = source_mode_distribution.get(mode, 0) + 1
            if source in fallback_source_set:
                fallback_core_rows += 1
            lineage = self._deserialize_json(item.get("lineage_json")) or {}
            for warning in lineage.get("core_fact_warnings") or []:
                semantic_warning_count += 1
                if len(semantic_warnings) < detail_limit:
                    semantic_warnings.append(
                        {
                            "instrument_id": item.get("instrument_id"),
                            "report_period": item.get("report_period"),
                            "source": source,
                            "source_mode": mode,
                            **warning,
                        }
                    )

        source_file_keys: set[tuple[str, str]] = set()
        parser_version_distribution: Dict[str, int] = {}
        manifest_status_counts: Dict[str, int] = {}
        for row in manifest_rows:
            instrument_id = row["instrument_id"]
            report_period = row["report_period"]
            status = str(row["status"] or "unknown")
            if instrument_id and report_period and status == "parsed":
                source_file_keys.add((instrument_id, report_period))
            parser_version = str(row["parser_version"] or "unknown")
            parser_version_distribution[parser_version] = (
                parser_version_distribution.get(parser_version, 0) + 1
            )
            manifest_status_counts[status] = manifest_status_counts.get(status, 0) + 1

        missing_periods: Dict[str, List[str]] = {}
        missing_instruments: List[str] = []
        missing_source_files: List[Dict[str, str]] = []
        missing_core_facts: List[Dict[str, Any]] = []
        field_present_counts = {field: 0 for field in required_fields}
        present_core_cells = 0
        expected_keys = [(instrument_id, period) for instrument_id in instruments for period in expected]

        for instrument_id in instruments:
            has_any_core = any((instrument_id, period) in core_by_key for period in expected)
            if expected and not has_any_core:
                missing_instruments.append(instrument_id)
            for period in expected:
                key = (instrument_id, period)
                row = core_by_key.get(key)
                if row is None:
                    missing_periods.setdefault(instrument_id, []).append(period)
                    self._append_limited(
                        missing_core_facts,
                        {
                            "instrument_id": instrument_id,
                            "report_period": period,
                            "missing_fields": list(required_fields),
                        },
                        detail_limit,
                    )
                else:
                    missing_fields = []
                    for field in required_fields:
                        if row.get(field) is None:
                            missing_fields.append(field)
                        else:
                            field_present_counts[field] += 1
                            present_core_cells += 1
                    if missing_fields:
                        self._append_limited(
                            missing_core_facts,
                            {
                                "instrument_id": instrument_id,
                                "report_period": period,
                                "missing_fields": missing_fields,
                            },
                            detail_limit,
                        )
                if key not in source_file_keys:
                    self._append_limited(
                        missing_source_files,
                        {"instrument_id": instrument_id, "report_period": period},
                        detail_limit,
                    )

        covered_period_rows = sum(1 for key in expected_keys if key in core_by_key)
        expected_period_rows = len(expected_keys)
        expected_core_cells = expected_period_rows * len(required_fields)
        missing_period_row_count = expected_period_rows - covered_period_rows
        missing_source_file_count = sum(
            1 for key in expected_keys if key not in source_file_keys
        )
        missing_core_fact_count = expected_core_cells - present_core_cells
        parser_failure_count = sum(
            count
            for status, count in manifest_status_counts.items()
            if status in {"failed", "parse_failed"}
        )
        manifest_count = len(manifest_rows)
        core_row_count = len(core_rows)

        return {
            "target_instrument_count": len(instruments),
            "expected_periods": expected,
            "expected_period_rows": expected_period_rows,
            "required_core_facts": required_fields,
            "period_coverage": {
                "covered_period_rows": covered_period_rows,
                "missing_period_row_count": missing_period_row_count,
                "coverage_ratio": None
                if expected_period_rows == 0
                else covered_period_rows / expected_period_rows,
                "missing_periods": {
                    key: value[:detail_limit]
                    for key, value in list(missing_periods.items())[:detail_limit]
                },
            },
            "missing_instruments": missing_instruments[:detail_limit],
            "source_files": {
                "manifest_count": manifest_count,
                "missing_source_file_count": missing_source_file_count,
                "missing_source_files": missing_source_files,
                "status_counts": manifest_status_counts,
                "parser_version_distribution": parser_version_distribution,
                "parser_failure_count": parser_failure_count,
                "parser_failure_ratio": None
                if manifest_count == 0
                else parser_failure_count / manifest_count,
            },
            "core_facts": {
                "core_row_count": core_row_count,
                "expected_core_fact_cells": expected_core_cells,
                "present_core_fact_cells": present_core_cells,
                "missing_core_fact_count": missing_core_fact_count,
                "semantic_warning_count": semantic_warning_count,
                "semantic_warnings": semantic_warnings,
                "coverage_ratio": None
                if expected_core_cells == 0
                else present_core_cells / expected_core_cells,
                "missing_core_facts": missing_core_facts,
                "field_present_counts": field_present_counts,
            },
            "source_distribution": source_distribution,
            "source_mode_distribution": source_mode_distribution,
            "fallback_sources": sorted(fallback_source_set),
            "fallback_core_rows": fallback_core_rows,
            "fallback_share": None
            if core_row_count == 0
            else fallback_core_rows / core_row_count,
            "tier_coverage": self.summarize_financial_tier_coverage(
                instrument_ids=instruments or None,
            ),
        }

    def validate_financial_statement_readiness(
        self,
        *,
        expected_periods: List[str],
        instrument_ids: List[str],
        required_core_facts: List[str],
        include_history: bool = True,
        fallback_sources: Optional[List[str]] = None,
        readiness_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Validate financial statement rollout readiness at repository level."""
        readiness_cfg = readiness_config or self.research_config.modules.get(
            "financial_statements",
            {},
        ).get("readiness", {})
        gaps = self.detect_financial_coverage_gaps(
            expected_periods=expected_periods,
            instrument_ids=instrument_ids,
            required_core_facts=required_core_facts,
            include_history=include_history,
            fallback_sources=fallback_sources,
        )
        blockers: List[str] = []
        if not instrument_ids:
            blockers.append("no_target_instruments")
        if not expected_periods:
            blockers.append("no_expected_report_periods")
        if gaps["period_coverage"]["missing_period_row_count"]:
            blockers.append("missing_required_report_periods")
        if gaps["source_files"]["missing_source_file_count"]:
            blockers.append("missing_source_files")
        if gaps["core_facts"]["missing_core_fact_count"]:
            blockers.append("missing_core_facts")
        if (
            bool(readiness_cfg.get("block_on_core_fact_semantic_warnings", True))
            and gaps["core_facts"].get("semantic_warning_count")
        ):
            blockers.append("core_fact_semantic_warnings")

        min_period_coverage = float(
            readiness_cfg.get("min_period_coverage_ratio", 0.95)
        )
        period_coverage = gaps["period_coverage"]["coverage_ratio"]
        if period_coverage is not None and period_coverage < min_period_coverage:
            blockers.append("period_coverage_below_threshold")

        min_core_coverage = float(
            readiness_cfg.get("min_core_fact_coverage_ratio", 0.95)
        )
        core_coverage = gaps["core_facts"]["coverage_ratio"]
        if core_coverage is not None and core_coverage < min_core_coverage:
            blockers.append("core_fact_coverage_below_threshold")

        max_fallback_share = float(readiness_cfg.get("max_fallback_share", 0.5))
        fallback_share = gaps["fallback_share"]
        if fallback_share is not None and fallback_share > max_fallback_share:
            blockers.append("fallback_share_above_threshold")

        max_parser_failure_ratio = float(
            readiness_cfg.get("max_parser_failure_ratio", 0.05)
        )
        parser_failure_ratio = gaps["source_files"]["parser_failure_ratio"]
        if (
            parser_failure_ratio is not None
            and parser_failure_ratio > max_parser_failure_ratio
        ):
            blockers.append("parser_failure_ratio_above_threshold")

        tier_coverage = gaps["tier_coverage"]
        duplicate_conflicts = tier_coverage.get("duplicate_tier_conflicts", {})
        if any(int(value) > 0 for value in duplicate_conflicts.values()):
            blockers.append("duplicate_tier_conflicts")
        if int(tier_coverage.get("stale_hot_rows", 0)) > 0:
            blockers.append("stale_hot_rows")

        return {
            "status": "ready" if not blockers else "not_ready",
            "ready_for_rollout": not blockers,
            "blockers": blockers,
            "thresholds": {
                "min_period_coverage_ratio": min_period_coverage,
                "min_core_fact_coverage_ratio": min_core_coverage,
                "max_fallback_share": max_fallback_share,
                "max_parser_failure_ratio": max_parser_failure_ratio,
                "block_on_core_fact_semantic_warnings": bool(
                    readiness_cfg.get("block_on_core_fact_semantic_warnings", True)
                ),
            },
            "gaps": gaps,
        }

    def summarize_financial_tier_coverage(
        self,
        *,
        instrument_ids: Optional[List[str]] = None,
        hot_quarter_window: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Summarize hot/history tier coverage without moving data."""
        window = hot_quarter_window or self._financial_hot_quarter_window()
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            coverage = {
                table_name: self._count_financial_tier_table(
                    conn,
                    table_name,
                    instrument_ids=instrument_ids,
                )
                for table_name in (
                    "financial_core_facts_hot",
                    "financial_core_facts_history",
                    "financial_numeric_facts_hot",
                    "financial_numeric_facts_history",
                    "financial_indicator_snapshots_hot",
                    "financial_indicator_snapshots_history",
                )
            }
            duplicate_conflicts = self._count_duplicate_tier_conflicts(conn)
            instruments = self._financial_tier_instruments(conn, instrument_id=None)
            if instrument_ids:
                instruments = [
                    instrument_id
                    for instrument_id in instruments
                    if instrument_id in set(instrument_ids)
                ]
            stale_hot_rows = 0
            for instrument_id in instruments:
                periods = [
                    row["report_period"]
                    for row in conn.execute(
                        """
                        SELECT DISTINCT report_period
                        FROM financial_core_facts_hot
                        WHERE instrument_id = ?
                        ORDER BY report_period DESC
                        """,
                        (instrument_id,),
                    ).fetchall()
                ]
                stale_hot_rows += self._count_stale_hot_rows(
                    conn,
                    instrument_id,
                    set(periods[:window]),
                )

        return {
            "hot_quarter_window": window,
            "tables": coverage,
            "duplicate_tier_conflicts": duplicate_conflicts,
            "stale_hot_rows": stale_hot_rows,
        }

    def derive_financial_core_facts_from_numeric_facts(
        self,
        instrument_id: str,
        report_period: str,
        *,
        alias_mapping: Dict[str, List[str]],
        include_history: bool = True,
    ) -> Optional[FinancialFactsSnapshot]:
        """Build a normalized core-fact snapshot from long-form numeric facts."""
        numeric_facts = self.get_financial_numeric_facts(
            instrument_id,
            include_history=include_history,
            report_period=report_period,
        )
        if not numeric_facts:
            return None

        facts_by_name = {
            str(row.get("fact_name")): row
            for row in numeric_facts
            if row.get("fact_name")
        }
        core_values: Dict[str, Optional[float]] = {}
        supported_fields = {
            "revenue",
            "gross_profit",
            "operating_profit",
            "pre_tax_profit",
            "net_income",
            "operating_cf",
            "total_cf",
            "total_assets",
            "total_liabilities",
            "equity",
            "current_assets",
            "current_liabilities",
            "inventory",
            "receivables",
            "fixed_assets",
            "intangible_assets",
            "shares_outstanding",
        }
        alias_matches: Dict[str, Dict[str, Any]] = {}
        alias_warnings: List[Dict[str, Any]] = []
        derivation_rules = get_core_financial_fact_derivation_rules()
        for core_field, aliases in alias_mapping.items():
            if core_field not in supported_fields:
                continue
            skipped_candidates: List[Dict[str, Any]] = []
            for alias in aliases:
                row = facts_by_name.get(alias)
                if row is None:
                    continue
                alias_info = describe_core_financial_fact_alias(core_field, alias)
                match_info = {
                    **alias_info,
                    "fact_name": row.get("fact_name"),
                    "fact_value": row.get("fact_value"),
                    "statement_family": row.get("statement_family"),
                    "source_file_id": row.get("source_file_id"),
                    "parser_version": row.get("parser_version"),
                    "unit": row.get("unit"),
                    "currency": row.get("currency"),
                }
                if alias_info["canonical_compatible"]:
                    core_values[core_field] = row.get("fact_value")
                    alias_matches[core_field] = match_info
                    break
                skipped_candidates.append({**match_info, "skipped": True})
                for warning in alias_info.get("warnings", []):
                    alias_warnings.append(
                        {
                            "core_field": core_field,
                            "fact_name": row.get("fact_name"),
                            "fact_value": row.get("fact_value"),
                            "warning": warning,
                            "canonical_semantic": alias_info.get("canonical_semantic"),
                            "alias_semantic": alias_info.get("alias_semantic"),
                        }
                    )
            else:
                if skipped_candidates:
                    alias_matches[core_field] = {
                        "selected": None,
                        "skipped_candidates": skipped_candidates,
                    }
            if core_field not in core_values:
                derived_match = self._derive_core_fact_from_components(
                    core_field,
                    facts_by_name,
                    derivation_rules.get(core_field, []),
                )
                if derived_match is not None:
                    core_values[core_field] = derived_match["fact_value"]
                    alias_matches[core_field] = derived_match
                    alias_warnings = [
                        warning
                        for warning in alias_warnings
                        if warning.get("core_field") != core_field
                    ]

        first = numeric_facts[0]
        source_file_id = first.get("source_file_id")
        return FinancialFactsSnapshot(
            instrument_id=instrument_id,
            symbol=str(first.get("symbol") or ""),
            exchange=str(first.get("exchange") or ""),
            report_period=report_period,
            report_type=first.get("report_type"),
            statement_family="core",
            source_file_id=source_file_id,
            currency=str(first.get("currency") or "CNY"),
            schema_version="financial_facts.derived.v1",
            source=str(first.get("source") or ""),
            source_mode=str(first.get("source_mode") or "direct"),
            facts_json={
                "derived_from_numeric_facts": True,
                "alias_mapping": alias_mapping,
                "core_fact_alias_matches": alias_matches,
                "core_fact_warnings": alias_warnings,
                "source_file_id": source_file_id,
            },
            lineage_json={
                "source_file_id": source_file_id,
                "numeric_fact_count": len(numeric_facts),
                "core_fact_alias_matches": alias_matches,
                "core_fact_warnings": alias_warnings,
            },
            **core_values,
        )

    @staticmethod
    def _derive_core_fact_from_components(
        core_field: str,
        facts_by_name: Dict[str, Dict[str, Any]],
        rules: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        for rule in rules:
            components = rule.get("components") or {}
            total = ResearchStorageManager._first_numeric_component(
                facts_by_name,
                components.get("total") or [],
            )
            minority = ResearchStorageManager._first_numeric_component(
                facts_by_name,
                components.get("minority") or [],
            )
            if total is None or minority is None:
                continue
            value = total["fact_value"] - minority["fact_value"]
            return {
                "core_field": core_field,
                "fact_name": f"{total['fact_name']}-{minority['fact_name']}",
                "fact_value": value,
                "method": rule.get("method"),
                "canonical_compatible": True,
                "derived": True,
                "components": {
                    "total": total,
                    "minority": minority,
                },
            }
        return None

    @staticmethod
    def _first_numeric_component(
        facts_by_name: Dict[str, Dict[str, Any]],
        aliases: List[str],
    ) -> Optional[Dict[str, Any]]:
        for alias in aliases:
            row = facts_by_name.get(alias)
            if row is None or row.get("fact_value") is None:
                continue
            return {
                "fact_name": row.get("fact_name"),
                "fact_value": row.get("fact_value"),
                "statement_family": row.get("statement_family"),
                "source_file_id": row.get("source_file_id"),
                "parser_version": row.get("parser_version"),
                "unit": row.get("unit"),
                "currency": row.get("currency"),
            }
        return None

    def maintain_financial_hot_cold_tiers(
        self,
        *,
        instrument_id: Optional[str] = None,
        hot_quarter_window: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Move financial rows outside the configured hot window into history."""
        window = hot_quarter_window or self._financial_hot_quarter_window()
        if window < 1:
            raise ValueError("hot_quarter_window must be positive")

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            instruments = self._financial_tier_instruments(
                conn,
                instrument_id=instrument_id,
            )
            moved_core = 0
            moved_numeric = 0
            moved_indicators = 0
            stale_hot_rows = 0

            for target in instruments:
                periods = [
                    row["report_period"]
                    for row in conn.execute(
                        """
                        SELECT DISTINCT report_period
                        FROM financial_core_facts_hot
                        WHERE instrument_id = ?
                        ORDER BY report_period DESC
                        """,
                        (target,),
                    ).fetchall()
                ]
                keep_periods = set(periods[:window])
                stale_periods = [period for period in periods if period not in keep_periods]
                for period in stale_periods:
                    moved_core += self._move_tier_rows(
                        conn,
                        "financial_core_facts_hot",
                        "financial_core_facts_history",
                        "instrument_id = ? AND report_period = ?",
                        (target, period),
                    )
                    moved_numeric += self._move_tier_rows(
                        conn,
                        "financial_numeric_facts_hot",
                        "financial_numeric_facts_history",
                        "instrument_id = ? AND report_period = ?",
                        (target, period),
                    )
                    moved_indicators += self._move_tier_rows(
                        conn,
                        "financial_indicator_snapshots_hot",
                        "financial_indicator_snapshots_history",
                        "instrument_id = ? AND report_period = ?",
                        (target, period),
                    )

                stale_hot_rows += self._count_stale_hot_rows(
                    conn,
                    target,
                    keep_periods,
                )

            duplicate_conflicts = self._count_duplicate_tier_conflicts(conn)
            conn.commit()

        return {
            "hot_quarter_window": window,
            "instrument_count": len(instruments),
            "moved_rows": {
                "core_facts": moved_core,
                "numeric_facts": moved_numeric,
                "indicators": moved_indicators,
            },
            "stale_hot_rows": stale_hot_rows,
            "duplicate_tier_conflicts": duplicate_conflicts,
        }

    def get_shareholder_snapshot(
        self,
        instrument_id: str,
        *,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Fetch one latest shareholder summary snapshot."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT
                    instrument_id,
                    symbol,
                    exchange,
                    coverage_status,
                    holder_count,
                    holder_count_report_date,
                    top_holders_report_date,
                    top_holders_count,
                    top_holders_total_ratio,
                    control_owner_name,
                    control_owner_ratio,
                    schema_version,
                    source,
                    source_mode,
                    data_as_of,
                    snapshot_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                FROM shareholder_snapshots
                WHERE instrument_id = ?
                LIMIT 1
                """,
                (instrument_id,),
            ).fetchone()

        if row is None:
            return None

        data = dict(row)
        snapshot_json = data.pop("snapshot_json", None)
        if include_snapshot:
            data["snapshot"] = self._deserialize_json(snapshot_json)
        return data

    def summarize_shareholder_snapshots(self) -> Dict[str, Any]:
        """Return aggregate summary for latest shareholder snapshots."""
        return self._summarize_shareholder_snapshots()

    def summarize_shareholder_snapshots_by_exchanges(
        self,
        exchanges: List[str],
    ) -> Dict[str, Any]:
        """Return aggregate shareholder snapshot summary for selected exchanges."""
        normalized_exchanges = [
            str(exchange).strip()
            for exchange in exchanges
            if str(exchange).strip()
        ]
        if not normalized_exchanges:
            return {
                "total": 0,
                "coverage_status_counts": {},
                "source_counts": {},
                "source_mode_counts": {},
                "scope_counts": {},
                "latest_updated_at": None,
                "latest_data_as_of": None,
            }
        return self._summarize_shareholder_snapshots(
            exchanges=normalized_exchanges,
        )

    def _summarize_shareholder_snapshots(
        self,
        exchanges: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Return aggregate summary for latest shareholder snapshots."""
        where_clause = ""
        params: List[str] = []
        if exchanges:
            placeholders = ",".join("?" for _ in exchanges)
            where_clause = f"WHERE exchange IN ({placeholders})"
            params = list(exchanges)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            coverage_rows = conn.execute(
                f"""
                SELECT coverage_status, COUNT(*) AS row_count
                FROM shareholder_snapshots
                {where_clause}
                GROUP BY coverage_status
                """,
                params,
            ).fetchall()
            source_rows = conn.execute(
                f"""
                SELECT source, COUNT(*) AS row_count
                FROM shareholder_snapshots
                {where_clause}
                GROUP BY source
                """,
                params,
            ).fetchall()
            mode_rows = conn.execute(
                f"""
                SELECT source_mode, COUNT(*) AS row_count
                FROM shareholder_snapshots
                {where_clause}
                GROUP BY source_mode
                """,
                params,
            ).fetchall()
            snapshot_rows = conn.execute(
                f"""
                SELECT snapshot_json
                FROM shareholder_snapshots
                {where_clause}
                """,
                params,
            ).fetchall()
            summary_row = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS total,
                    MAX(updated_at) AS latest_updated_at,
                    MAX(data_as_of) AS latest_data_as_of
                FROM shareholder_snapshots
                {where_clause}
                """,
                params,
            ).fetchone()

        scope_counts: Dict[str, int] = {}
        for row in snapshot_rows:
            snapshot = self._deserialize_json(row["snapshot_json"])
            if not isinstance(snapshot, dict):
                continue
            for scope in snapshot.get("coverage_scope", []) or []:
                scope_text = str(scope).strip()
                if not scope_text:
                    continue
                scope_counts[scope_text] = scope_counts.get(scope_text, 0) + 1

        return {
            "total": int((summary_row["total"] if summary_row is not None else 0) or 0),
            "coverage_status_counts": {
                str(row["coverage_status"]): int(row["row_count"] or 0)
                for row in coverage_rows
                if row["coverage_status"] is not None
            },
            "source_counts": {
                str(row["source"]): int(row["row_count"] or 0)
                for row in source_rows
                if row["source"] is not None
            },
            "source_mode_counts": {
                str(row["source_mode"]): int(row["row_count"] or 0)
                for row in mode_rows
                if row["source_mode"] is not None
            },
            "scope_counts": scope_counts,
            "latest_updated_at": None if summary_row is None else summary_row["latest_updated_at"],
            "latest_data_as_of": None if summary_row is None else summary_row["latest_data_as_of"],
        }

    def count_shareholder_snapshots_by_exchange(self) -> Dict[str, int]:
        """Count shareholder snapshots per exchange."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                """
                SELECT exchange, COUNT(*) AS row_count
                FROM shareholder_snapshots
                GROUP BY exchange
                """
            ).fetchall()
        return {
            str(row["exchange"]): int(row["row_count"] or 0)
            for row in rows
            if row["exchange"] is not None
        }

    def get_shareholder_snapshots(
        self,
        instrument_ids: List[str],
        *,
        include_snapshot: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch latest shareholder snapshots for a bounded instrument list."""
        normalized_ids = [
            str(instrument_id).strip()
            for instrument_id in instrument_ids
            if str(instrument_id).strip()
        ]
        if not normalized_ids:
            return {}

        result: Dict[str, Dict[str, Any]] = {}
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            for start in range(0, len(normalized_ids), 500):
                batch = normalized_ids[start : start + 500]
                placeholders = ",".join("?" for _ in batch)
                rows = conn.execute(
                    f"""
                    SELECT
                        instrument_id,
                        symbol,
                        exchange,
                        coverage_status,
                        holder_count,
                        holder_count_report_date,
                        top_holders_report_date,
                        top_holders_count,
                        top_holders_total_ratio,
                        control_owner_name,
                        control_owner_ratio,
                        schema_version,
                        source,
                        source_mode,
                        data_as_of,
                        snapshot_json,
                        ingestion_run_id,
                        created_at,
                        updated_at
                    FROM shareholder_snapshots
                    WHERE instrument_id IN ({placeholders})
                    """,
                    batch,
                ).fetchall()
                for row in rows:
                    data = dict(row)
                    snapshot_json = data.pop("snapshot_json", None)
                    if include_snapshot:
                        data["snapshot"] = self._deserialize_json(snapshot_json)
                    result[str(data["instrument_id"])] = data
        return result

    def get_announcement_scan_state(
        self,
        *,
        purpose_key: str,
        source: str,
        scope_key: str,
    ) -> Optional[Dict[str, Any]]:
        """Return one source-neutral announcement checkpoint."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT *
                FROM announcement_scan_state
                WHERE purpose_key = ? AND source = ? AND scope_key = ?
                LIMIT 1
                """,
                (
                    str(purpose_key).strip(),
                    str(source).strip().lower(),
                    str(scope_key).strip(),
                ),
            ).fetchone()
        if row is None:
            return None
        item = dict(row)
        item["scope"] = self._deserialize_json(item.pop("scope_json", None)) or {}
        item["attempts"] = self._deserialize_json(item.pop("attempts_json", None)) or []
        item["metadata"] = self._deserialize_json(item.pop("metadata_json", None)) or {}
        item["is_complete"] = bool(item.get("is_complete"))
        if item.get("committed_cursor_kind") and item.get("committed_cursor_value"):
            item["committed_cursor"] = {
                "kind": item["committed_cursor_kind"],
                "value": item["committed_cursor_value"],
            }
        else:
            item["committed_cursor"] = None
        return item

    def upsert_announcement_scan_state(
        self,
        *,
        scan_result: AnnouncementScanResult,
        normalized_scope: Optional[Dict[str, Any]] = None,
        selected_announcements: Optional[int] = None,
        last_scan_started_at: Optional[str] = None,
        last_scan_completed_at: Optional[str] = None,
        attempts: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Atomically persist diagnostics and conservatively commit provider cursor."""
        query = scan_result.query
        scope_key = query.scope.scope_key
        scope = normalized_scope or query.scope.stable_scope_payload()
        source = scan_result.source
        purpose_key = query.purpose_key
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            prior = conn.execute(
                """
                SELECT committed_cursor_kind, committed_cursor_value, max_published_at,
                       created_at
                FROM announcement_scan_state
                WHERE purpose_key = ? AND source = ? AND scope_key = ?
                """,
                (purpose_key, source, scope_key),
            ).fetchone()
            prior_cursor = (
                ProviderCursor(
                    kind=str(prior["committed_cursor_kind"]),
                    value=str(prior["committed_cursor_value"]),
                )
                if prior is not None
                and prior["committed_cursor_kind"]
                and prior["committed_cursor_value"]
                else None
            )
            committed_cursor = (
                scan_result.provider_cursor
                if scan_result.cursor_commit_allowed
                and scan_result.provider_cursor is not None
                else prior_cursor
            )
            committed_max_published_at = (
                scan_result.max_published_at
                if scan_result.cursor_commit_allowed
                and scan_result.max_published_at is not None
                else (None if prior is None else prior["max_published_at"])
            )
            merged_metadata = {
                **(metadata or {}),
                "cursor_commit_allowed": scan_result.cursor_commit_allowed,
                "observed_max_published_at": scan_result.max_published_at,
            }
            conn.execute(
                """
                INSERT INTO announcement_scan_state (
                    purpose_key, source, scope_key, scope_json,
                    committed_cursor_kind, committed_cursor_value,
                    max_published_at, last_scan_started_at,
                    last_scan_completed_at, pages_scanned, requests_made,
                    announcements_seen, selected_announcements, status,
                    is_complete, stop_reason, attempts_json, metadata_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(purpose_key, source, scope_key) DO UPDATE SET
                    scope_json = excluded.scope_json,
                    committed_cursor_kind = excluded.committed_cursor_kind,
                    committed_cursor_value = excluded.committed_cursor_value,
                    max_published_at = excluded.max_published_at,
                    last_scan_started_at = excluded.last_scan_started_at,
                    last_scan_completed_at = excluded.last_scan_completed_at,
                    pages_scanned = excluded.pages_scanned,
                    requests_made = excluded.requests_made,
                    announcements_seen = excluded.announcements_seen,
                    selected_announcements = excluded.selected_announcements,
                    status = excluded.status,
                    is_complete = excluded.is_complete,
                    stop_reason = excluded.stop_reason,
                    attempts_json = excluded.attempts_json,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    purpose_key,
                    source,
                    scope_key,
                    json.dumps(scope, ensure_ascii=False, sort_keys=True, default=str),
                    None if committed_cursor is None else committed_cursor.kind,
                    None if committed_cursor is None else committed_cursor.value,
                    committed_max_published_at,
                    last_scan_started_at,
                    last_scan_completed_at or now,
                    scan_result.pages_scanned,
                    scan_result.requests_made,
                    scan_result.announcements_seen,
                    len(scan_result.selected_records)
                    if selected_announcements is None
                    else int(selected_announcements),
                    scan_result.status,
                    int(scan_result.is_complete),
                    scan_result.stop_reason,
                    json.dumps(attempts or [], ensure_ascii=False, sort_keys=True, default=str),
                    json.dumps(merged_metadata, ensure_ascii=False, sort_keys=True, default=str),
                    now if prior is None else prior["created_at"],
                    now,
                ),
            )
            conn.commit()
        db_logger.info(
            "[ResearchStorage] Announcement cursor decision: purpose=%s source=%s scope_key=%s status=%s complete=%s commit_allowed=%s committed_cursor=%s stop_reason=%s pages=%s records=%s selected=%s",
            purpose_key,
            source,
            scope_key,
            scan_result.status,
            scan_result.is_complete,
            scan_result.cursor_commit_allowed,
            None if committed_cursor is None else committed_cursor.value,
            scan_result.stop_reason,
            scan_result.pages_scanned,
            scan_result.announcements_seen,
            len(scan_result.selected_records)
            if selected_announcements is None
            else int(selected_announcements),
        )
        state = self.get_announcement_scan_state(
            purpose_key=purpose_key,
            source=source,
            scope_key=scope_key,
        )
        if state is None:
            raise RuntimeError("announcement scan state write did not persist")
        return state

    def store_announcement_audit(
        self,
        *,
        purpose_key: str,
        record: AnnouncementRecord,
        instrument_id: Optional[str] = None,
        symbol: Optional[str] = None,
        ingestion_run_id: Optional[int] = None,
        scope_key: Optional[str] = None,
        context_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Idempotently store one purpose-specific normalized announcement."""
        now = get_shanghai_time().isoformat()
        effective_symbol = symbol or (record.symbols[0] if record.symbols else None)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO announcement_audit (
                    purpose_key, source, announcement_key,
                    source_announcement_id, instrument_id, symbol,
                    exchange, market, published_at, published_at_raw,
                    title, attachments_json, selection_reasons_json,
                    diagnostics_json, raw_payload_json, ingestion_run_id,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(purpose_key, announcement_key, instrument_id)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    market = excluded.market,
                    published_at = excluded.published_at,
                    published_at_raw = excluded.published_at_raw,
                    title = excluded.title,
                    attachments_json = excluded.attachments_json,
                    selection_reasons_json = excluded.selection_reasons_json,
                    diagnostics_json = excluded.diagnostics_json,
                    raw_payload_json = excluded.raw_payload_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    str(purpose_key).strip(),
                    record.source,
                    record.announcement_key,
                    record.source_announcement_id,
                    instrument_id or "",
                    effective_symbol,
                    record.exchange,
                    record.market,
                    record.published_at,
                    None
                    if record.published_at_raw is None
                    else str(record.published_at_raw),
                    record.title,
                    json.dumps(
                        [asdict(item) for item in record.attachments],
                        ensure_ascii=False,
                        sort_keys=True,
                        default=str,
                    ),
                    json.dumps(record.selection_reasons, ensure_ascii=False, sort_keys=True),
                    json.dumps(record.diagnostics, ensure_ascii=False, sort_keys=True),
                    json.dumps(record.raw_payload, ensure_ascii=False, sort_keys=True, default=str),
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            normalized_scope_key = str(scope_key or "").strip()
            if normalized_scope_key:
                context = dict(context_metadata or {})
                conn.execute(
                    """
                    INSERT INTO announcement_audit_context (
                        purpose_key, source, announcement_key, instrument_id,
                        scope_key, source_event_key, window_index,
                        search_start_date, search_end_date, search_basis,
                        context_json, ingestion_run_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(
                        purpose_key, announcement_key, instrument_id, scope_key
                    ) DO UPDATE SET
                        source_event_key = excluded.source_event_key,
                        window_index = excluded.window_index,
                        search_start_date = excluded.search_start_date,
                        search_end_date = excluded.search_end_date,
                        search_basis = excluded.search_basis,
                        context_json = excluded.context_json,
                        ingestion_run_id = excluded.ingestion_run_id,
                        updated_at = excluded.updated_at
                    """,
                    (
                        str(purpose_key).strip(),
                        record.source,
                        record.announcement_key,
                        instrument_id or "",
                        normalized_scope_key,
                        str(context.get("source_event_key") or ""),
                        context.get("window_index"),
                        context.get("search_start_date"),
                        context.get("search_end_date"),
                        context.get("search_basis"),
                        json.dumps(
                            context,
                            ensure_ascii=False,
                            sort_keys=True,
                            default=str,
                        ),
                        ingestion_run_id,
                        now,
                        now,
                    ),
                )
            conn.commit()

    def list_announcement_audit(
        self,
        *,
        purpose_key: str,
        source: Optional[str] = None,
        instrument_ids: Optional[List[str]] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Return bounded source-neutral operational audit rows."""
        bounded_limit = min(1000, max(1, int(limit)))
        params: List[Any] = [str(purpose_key).strip()]
        where = ["purpose_key = ?"]
        if source:
            where.append("source = ?")
            params.append(str(source).strip().lower())
        ids = [str(item) for item in (instrument_ids or []) if str(item)]
        if ids:
            where.append(f"instrument_id IN ({','.join('?' for _ in ids)})")
            params.extend(ids)
        params.append(bounded_limit)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT *
                FROM announcement_audit
                WHERE {' AND '.join(where)}
                ORDER BY published_at DESC, updated_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        output: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["attachments"] = self._deserialize_json(
                item.pop("attachments_json", None)
            ) or []
            item["selection_reasons"] = self._deserialize_json(
                item.pop("selection_reasons_json", None)
            ) or []
            item["diagnostics"] = self._deserialize_json(
                item.pop("diagnostics_json", None)
            ) or []
            item["raw_payload"] = self._deserialize_json(
                item.pop("raw_payload_json", None)
            ) or {}
            output.append(item)
        return output

    def reset_corporate_action_announcement_governance(
        self,
        *,
        purpose_key: str,
        instrument_ids: List[str],
        source_event_keys: List[str],
        preserve_instrument_ids: Optional[List[str]] = None,
        preserve_announcement_ids: Optional[List[str]] = None,
        dry_run: bool = True,
    ) -> Dict[str, int]:
        """Remove selected corporate-action scan lineage for a clean rebuild."""
        normalized_instruments = {
            str(item).strip() for item in instrument_ids if str(item).strip()
        }
        normalized_events = {
            str(item).strip() for item in source_event_keys if str(item).strip()
        }
        protected_instruments = {
            str(item).strip() for item in (preserve_instrument_ids or [])
            if str(item).strip()
        }
        preserved = {
            str(item).strip() for item in (preserve_announcement_ids or [])
            if str(item).strip()
        }
        if not normalized_instruments and not normalized_events:
            return {
                "scan_states": 0,
                "announcement_audit_contexts": 0,
                "announcement_audits": 0,
            }

        def matches_scope(row: sqlite3.Row) -> bool:
            scope = self._deserialize_json(row["scope_json"]) or {}
            metadata = self._deserialize_json(row["metadata_json"]) or {}
            options = scope.get("source_options") or {}
            selection = metadata.get("selection_metadata") or {}
            event_key = str(
                options.get("source_event_key")
                or selection.get("source_event_key")
                or ""
            ).strip()
            instrument_id = str(scope.get("instrument_id") or "").strip()
            if event_key:
                return event_key in normalized_events
            return bool(
                instrument_id
                and instrument_id in normalized_instruments
                and instrument_id not in protected_instruments
            )

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            scan_rows = conn.execute(
                """
                SELECT purpose_key, source, scope_key, scope_json, metadata_json
                FROM announcement_scan_state
                WHERE purpose_key = ? AND source = 'cninfo'
                """,
                (str(purpose_key).strip(),),
            ).fetchall()
            scan_keys = [
                (row["purpose_key"], row["source"], row["scope_key"])
                for row in scan_rows if matches_scope(row)
            ]
            audit_rows = []
            context_rows = []
            if normalized_instruments:
                placeholders = ",".join("?" for _ in normalized_instruments)
                audit_rows = conn.execute(
                    f"""
                    SELECT purpose_key, announcement_key, instrument_id,
                           source_announcement_id
                    FROM announcement_audit
                    WHERE purpose_key = ? AND source = 'cninfo'
                      AND instrument_id IN ({placeholders})
                    """,
                    [str(purpose_key).strip(), *sorted(normalized_instruments)],
                ).fetchall()
                context_rows = conn.execute(
                    f"""
                    SELECT purpose_key, announcement_key, instrument_id,
                           scope_key, source_event_key
                    FROM announcement_audit_context
                    WHERE purpose_key = ? AND source = 'cninfo'
                      AND instrument_id IN ({placeholders})
                    """,
                    [str(purpose_key).strip(), *sorted(normalized_instruments)],
                ).fetchall()
            deleted_scope_keys = {item[2] for item in scan_keys}
            context_keys = [
                (
                    row["purpose_key"],
                    row["announcement_key"],
                    row["instrument_id"],
                    row["scope_key"],
                )
                for row in context_rows
                if (
                    str(row["source_event_key"] or "") in normalized_events
                    or str(row["scope_key"] or "") in deleted_scope_keys
                )
            ]
            deleted_context_keys = set(context_keys)
            announcements_with_remaining_context = {
                (
                    row["purpose_key"],
                    row["announcement_key"],
                    row["instrument_id"],
                )
                for row in context_rows
                if (
                    row["purpose_key"],
                    row["announcement_key"],
                    row["instrument_id"],
                    row["scope_key"],
                ) not in deleted_context_keys
            }
            audit_keys = [
                (row["purpose_key"], row["announcement_key"], row["instrument_id"])
                for row in audit_rows
                if str(row["source_announcement_id"] or "") not in preserved
                and (
                    row["purpose_key"],
                    row["announcement_key"],
                    row["instrument_id"],
                ) not in announcements_with_remaining_context
            ]
            if not dry_run:
                conn.executemany(
                    """
                    DELETE FROM announcement_scan_state
                    WHERE purpose_key = ? AND source = ? AND scope_key = ?
                    """,
                    scan_keys,
                )
                conn.executemany(
                    """
                    DELETE FROM announcement_audit_context
                    WHERE purpose_key = ? AND announcement_key = ?
                      AND instrument_id = ? AND scope_key = ?
                    """,
                    context_keys,
                )
                conn.executemany(
                    """
                    DELETE FROM announcement_audit
                    WHERE purpose_key = ? AND announcement_key = ?
                      AND instrument_id = ?
                    """,
                    audit_keys,
                )
                conn.commit()
        return {
            "scan_states": len(scan_keys),
            "announcement_audit_contexts": len(context_keys),
            "announcement_audits": len(audit_keys),
        }

    def upsert_financial_disclosure_event_state(
        self,
        *,
        instrument_id: str,
        report_period: str,
        announcement_id: str,
        symbol: Optional[str],
        exchange: Optional[str],
        status: str,
        classification: str,
        title: Optional[str],
        announcement_time: Optional[str],
        selection_reasons: Optional[List[str]] = None,
        missing_fields: Optional[List[Dict[str, Any]]] = None,
        first_pending_at: Optional[str] = None,
        pending_recheck_until: Optional[str] = None,
        processed_at: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Persist financial announcement maintenance state by instrument-period."""
        now = get_shanghai_time().isoformat()
        reasons_json = json.dumps(selection_reasons or [], ensure_ascii=False, sort_keys=True)
        missing_json = json.dumps(missing_fields or [], ensure_ascii=False, sort_keys=True)
        metadata_json = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True, default=str)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO financial_disclosure_event_state (
                    instrument_id,
                    report_period,
                    announcement_id,
                    symbol,
                    exchange,
                    status,
                    classification,
                    title,
                    announcement_time,
                    selection_reasons_json,
                    missing_fields_json,
                    first_pending_at,
                    pending_recheck_until,
                    processed_at,
                    metadata_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id, report_period, announcement_id)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    status = excluded.status,
                    classification = excluded.classification,
                    title = excluded.title,
                    announcement_time = excluded.announcement_time,
                    selection_reasons_json = excluded.selection_reasons_json,
                    missing_fields_json = excluded.missing_fields_json,
                    first_pending_at = COALESCE(
                        financial_disclosure_event_state.first_pending_at,
                        excluded.first_pending_at
                    ),
                    pending_recheck_until = CASE
                        WHEN excluded.status IN ('pending_recheck', 'pending_delisting_risk')
                        THEN COALESCE(
                            financial_disclosure_event_state.pending_recheck_until,
                            excluded.pending_recheck_until
                        )
                        ELSE excluded.pending_recheck_until
                    END,
                    processed_at = excluded.processed_at,
                    metadata_json = excluded.metadata_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    instrument_id,
                    report_period,
                    announcement_id,
                    symbol,
                    exchange,
                    status,
                    classification,
                    title,
                    announcement_time,
                    reasons_json,
                    missing_json,
                    first_pending_at,
                    pending_recheck_until,
                    processed_at,
                    metadata_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            conn.commit()

    def list_financial_disclosure_event_states(
        self,
        *,
        statuses: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return persisted financial announcement maintenance state."""
        params: List[Any] = []
        where = ""
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            where = f"WHERE status IN ({placeholders})"
            params.extend(statuses)
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ?"
            params.append(int(limit))
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT *
                FROM financial_disclosure_event_state
                {where}
                ORDER BY updated_at ASC
                {limit_clause}
                """,
                params,
            ).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["selection_reasons"] = self._deserialize_json(
                item.pop("selection_reasons_json", None)
            ) or []
            item["missing_fields"] = self._deserialize_json(
                item.pop("missing_fields_json", None)
            ) or []
            item["metadata"] = self._deserialize_json(item.pop("metadata_json", None)) or {}
            result.append(item)
        return result

    def delete_financial_disclosure_event_state(
        self,
        *,
        instrument_id: str,
        report_period: str,
        announcement_id: str,
        statuses: Optional[List[str]] = None,
    ) -> int:
        """Delete obsolete financial disclosure maintenance state rows."""
        params: List[Any] = [instrument_id, report_period, announcement_id]
        status_clause = ""
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            status_clause = f" AND status IN ({placeholders})"
            params.extend(statuses)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            cursor = conn.execute(
                f"""
                DELETE FROM financial_disclosure_event_state
                WHERE instrument_id = ?
                  AND report_period = ?
                  AND announcement_id = ?
                  {status_clause}
                """,
                params,
            )
            conn.commit()
            return int(cursor.rowcount or 0)

    def get_shareholder_change_manifest(
        self,
        instrument_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Return one shareholder incremental manifest row."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT *
                FROM shareholder_change_manifest
                WHERE instrument_id = ?
                LIMIT 1
                """,
                (instrument_id,),
            ).fetchone()
        if row is None:
            return None
        return self._decode_shareholder_manifest_row(row)

    def get_shareholder_change_manifests(
        self,
        instrument_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """Return shareholder incremental manifests for a bounded instrument list."""
        normalized_ids = [
            str(instrument_id).strip()
            for instrument_id in instrument_ids
            if str(instrument_id).strip()
        ]
        if not normalized_ids:
            return {}
        result: Dict[str, Dict[str, Any]] = {}
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            for start in range(0, len(normalized_ids), 500):
                batch = normalized_ids[start : start + 500]
                placeholders = ",".join("?" for _ in batch)
                rows = conn.execute(
                    f"""
                    SELECT *
                    FROM shareholder_change_manifest
                    WHERE instrument_id IN ({placeholders})
                    """,
                    batch,
                ).fetchall()
                for row in rows:
                    item = self._decode_shareholder_manifest_row(row)
                    result[str(item["instrument_id"])] = item
        return result

    def list_pending_shareholder_rechecks(
        self,
        *,
        as_of: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return shareholder manifests still inside pending recheck horizon."""
        as_of_value = as_of or get_shanghai_time().isoformat()
        sql = """
            SELECT *
            FROM shareholder_change_manifest
            WHERE status = 'pending_recheck'
              AND pending_recheck_until IS NOT NULL
              AND pending_recheck_until >= ?
            ORDER BY pending_recheck_until ASC
        """
        params: List[Any] = [as_of_value]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()
        return [self._decode_shareholder_manifest_row(row) for row in rows]

    def upsert_shareholder_change_manifest(
        self,
        *,
        instrument_id: str,
        symbol: str,
        exchange: str,
        content_hash: Optional[str],
        top_holders_hash: Optional[str],
        holder_count_hash: Optional[str],
        ownership_hash: Optional[str],
        latest_report_date: Optional[str],
        coverage_scope: List[str],
        status: str,
        last_checked_at: Optional[str] = None,
        last_changed_at: Optional[str] = None,
        pending_recheck_until: Optional[str] = None,
        last_announcement_time: Optional[str] = None,
        reasons: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        ingestion_run_id: Optional[int] = None,
    ) -> None:
        """Upsert one shareholder incremental change-check manifest."""
        now = get_shanghai_time().isoformat()
        checked_at = last_checked_at or now
        scope_json = json.dumps(coverage_scope or [], ensure_ascii=False, sort_keys=True)
        reasons_json = json.dumps(reasons or [], ensure_ascii=False, sort_keys=True)
        metadata_json = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True, default=str)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO shareholder_change_manifest (
                    instrument_id,
                    symbol,
                    exchange,
                    content_hash,
                    top_holders_hash,
                    holder_count_hash,
                    ownership_hash,
                    latest_report_date,
                    coverage_scope_json,
                    status,
                    last_checked_at,
                    last_changed_at,
                    pending_recheck_until,
                    last_announcement_time,
                    reasons_json,
                    metadata_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id)
                DO UPDATE SET
                    symbol = excluded.symbol,
                    exchange = excluded.exchange,
                    content_hash = excluded.content_hash,
                    top_holders_hash = excluded.top_holders_hash,
                    holder_count_hash = excluded.holder_count_hash,
                    ownership_hash = excluded.ownership_hash,
                    latest_report_date = excluded.latest_report_date,
                    coverage_scope_json = excluded.coverage_scope_json,
                    status = excluded.status,
                    last_checked_at = excluded.last_checked_at,
                    last_changed_at = COALESCE(excluded.last_changed_at, shareholder_change_manifest.last_changed_at),
                    pending_recheck_until = excluded.pending_recheck_until,
                    last_announcement_time = excluded.last_announcement_time,
                    reasons_json = excluded.reasons_json,
                    metadata_json = excluded.metadata_json,
                    ingestion_run_id = excluded.ingestion_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    instrument_id,
                    symbol,
                    exchange,
                    content_hash,
                    top_holders_hash,
                    holder_count_hash,
                    ownership_hash,
                    latest_report_date,
                    scope_json,
                    status,
                    checked_at,
                    last_changed_at,
                    pending_recheck_until,
                    last_announcement_time,
                    reasons_json,
                    metadata_json,
                    ingestion_run_id,
                    now,
                    now,
                ),
            )
            conn.commit()

    def _decode_shareholder_manifest_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["coverage_scope"] = (
            self._deserialize_json(item.pop("coverage_scope_json", None)) or []
        )
        item["reasons"] = self._deserialize_json(item.pop("reasons_json", None)) or []
        item["metadata"] = self._deserialize_json(item.pop("metadata_json", None)) or {}
        return item

    def get_industry_membership(
        self,
        instrument_id: str,
        *,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Fetch the preferred latest industry membership row for one instrument."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT
                    instrument_id,
                    symbol,
                    exchange,
                    taxonomy_system,
                    taxonomy_version,
                    industry_code,
                    industry_name,
                    industry_level,
                    parent_code,
                    mapping_status,
                    effective_date,
                    source_classification,
                    source_industry_name,
                    sw_l1_code,
                    sw_l1_name,
                    sw_l2_code,
                    sw_l2_name,
                    sw_l3_code,
                    sw_l3_name,
                    sw_l1_index_code,
                    sw_l2_index_code,
                    sw_l3_index_code,
                    source,
                    source_mode,
                    data_as_of,
                    membership_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                FROM industry_memberships
                WHERE instrument_id = ?
                ORDER BY
                    CASE
                        WHEN source = 'dev_validation'
                            OR taxonomy_version LIKE 'dryrun%' THEN 1
                        ELSE 0
                    END,
                    CASE
                        WHEN mapping_status = 'authoritative' THEN 0
                        ELSE 1
                    END,
                    CASE
                        WHEN taxonomy_system LIKE 'sw%' THEN 0
                        ELSE 1
                    END,
                    updated_at DESC
                LIMIT 1
                """,
                (instrument_id,),
            ).fetchone()

        if row is None:
            return None

        data = dict(row)
        membership_json = data.pop("membership_json", None)
        if include_snapshot:
            data["membership"] = self._deserialize_json(membership_json)
        return data

    def get_industry_membership_as_of(
        self,
        instrument_id: str,
        as_of_date: str,
        *,
        taxonomy_system: Optional[str] = None,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """时点化行业归属 (REQ-07.2)。

        基于 industry_classification_history 的官方分类变更留痕, 返回在 as_of_date
        当日生效的分类, 并推导 effective_date/expiry_date 区间。
        as_of_date 为 'YYYY-MM-DD' 字符串; official_start_date 亦为 ISO 文本, 可字典序比较。
        不命中(该日之前无已知分类)时返回 None。
        """
        as_of_str = str(as_of_date)[:10]
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                """
                SELECT
                    instrument_id,
                    symbol,
                    exchange,
                    taxonomy_system,
                    taxonomy_version,
                    official_industry_code,
                    official_start_date,
                    official_update_time,
                    classification_json,
                    source,
                    source_mode,
                    created_at,
                    updated_at
                FROM industry_classification_history
                WHERE instrument_id = ?
                  AND (? IS NULL OR taxonomy_system = ?)
                ORDER BY
                    COALESCE(official_start_date, official_update_time, created_at) ASC,
                    COALESCE(official_update_time, updated_at) ASC
                """,
                (instrument_id, taxonomy_system, taxonomy_system),
            ).fetchall()

        if not rows:
            return None

        def _start(row: sqlite3.Row) -> str:
            return (row["official_start_date"] or row["official_update_time"] or "")[:10]

        eligible = [row for row in rows if _start(row) and _start(row) <= as_of_str]
        if not eligible:
            return None
        chosen = eligible[-1]
        chosen_start = _start(chosen)
        later_starts = sorted(
            {_start(row) for row in rows if _start(row) and _start(row) > chosen_start}
        )
        expiry = later_starts[0] if later_starts else None

        data = dict(chosen)
        classification_json = data.pop("classification_json", None)
        if include_snapshot:
            data["classification"] = self._deserialize_json(classification_json)
        data["effective_date"] = chosen_start
        data["expiry_date"] = expiry
        data["as_of_date"] = as_of_str
        return data

    # ----- REQ-13: 无风险利率序列 -----

    def upsert_risk_free_rate_series(self, series: Dict[str, Any]) -> None:
        """写入/更新一条无风险利率序列定义。"""
        now = get_shanghai_time().isoformat()
        semantic_payload = {
            key: series.get(key)
            for key in (
                "series_id",
                "name",
                "rate_type",
                "tenor",
                "currency",
                "unit",
                "frequency",
                "timezone",
                "source_profile",
                "source",
                "source_mode",
                "data_as_of",
            )
        }
        semantic_payload.update(
            {
                "name": series.get("name", series["series_id"]),
                "rate_type": series.get("rate_type", "unknown"),
                "currency": series.get("currency", "CNY"),
                "unit": series.get("unit", "percent_annual"),
                "frequency": series.get("frequency", "daily"),
                "timezone": series.get("timezone", "Asia/Shanghai"),
            }
        )
        row_hash = ResearchStorageManager._semantic_hash(semantic_payload)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            existing = conn.execute(
                "SELECT * FROM risk_free_rate_series WHERE series_id = ?",
                (series["series_id"],),
            ).fetchone()
            old_hash = None
            if existing is not None:
                old_payload = dict(existing)
                for key in (
                    "row_hash",
                    "row_version",
                    "ingestion_run_id",
                    "created_at",
                    "updated_at",
                ):
                    old_payload.pop(key, None)
                old_hash = existing["row_hash"] or ResearchStorageManager._semantic_hash(
                    old_payload
                )
            changed = existing is None or old_hash != row_hash
            row_version = (
                1
                if existing is None
                else int(existing["row_version"] or 1) + int(changed)
            )
            conn.execute(
                """
                INSERT INTO risk_free_rate_series (
                    series_id, name, rate_type, tenor, currency, unit, frequency,
                    timezone, source_profile, source, source_mode, data_as_of,
                    row_hash, row_version, ingestion_run_id, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(series_id) DO UPDATE SET
                    name=excluded.name, rate_type=excluded.rate_type, tenor=excluded.tenor,
                    currency=excluded.currency, unit=excluded.unit, frequency=excluded.frequency,
                    timezone=excluded.timezone, source_profile=excluded.source_profile,
                    source=excluded.source, source_mode=excluded.source_mode,
                    data_as_of=excluded.data_as_of, ingestion_run_id=excluded.ingestion_run_id,
                    row_hash=excluded.row_hash, row_version=excluded.row_version,
                    updated_at=excluded.updated_at
                """,
                (
                    series["series_id"], series.get("name", series["series_id"]),
                    series.get("rate_type", "unknown"), series.get("tenor"),
                    series.get("currency", "CNY"), series.get("unit", "percent_annual"),
                    series.get("frequency", "daily"), series.get("timezone", "Asia/Shanghai"),
                    series.get("source_profile"), series.get("source"),
                    series.get("source_mode"), series.get("data_as_of"),
                    row_hash, row_version, series.get("ingestion_run_id"), now, now,
                ),
            )
            if changed:
                append_change_record(
                    conn,
                    config=getattr(self, "change_watermark_config", None),
                    domain="interest_rate",
                    dataset="risk_free_rate_series",
                    change_type="update" if existing else "insert",
                    business_key={"series_id": series["series_id"]},
                    series_id=series["series_id"],
                    old_hash=old_hash,
                    new_hash=row_hash,
                    row_version=row_version,
                    source=series.get("source"),
                    source_mode=series.get("source_mode"),
                    source_profile=series.get("source_profile"),
                    ingestion_run_id=series.get("ingestion_run_id"),
                    changed_at=now,
                )
            conn.commit()

    def upsert_risk_free_rate_observations(
        self, series_id: str, observations: List[Dict[str, Any]]
    ) -> int:
        """批量写入观测值 (revision_id 默认 'latest')。返回写入行数。

        调用方 (research/risk_free_rate_sync.py) 负责分块调用以获得写入进度可见性;
        本方法保持存储层职责单一, 不在此处做日志/进度上报。
        """
        if not observations:
            return 0
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            for obs in observations:
                observation_date = str(obs["observation_date"])[:10]
                source_profile = obs.get("source_profile", "default")
                revision_id = obs.get("revision_id", "latest")
                semantic_payload = {
                    "series_id": series_id,
                    "observation_date": observation_date,
                    "source_profile": source_profile,
                    "value": obs.get("value"),
                    "revision_id": revision_id,
                    "source": obs.get("source"),
                    "source_mode": obs.get("source_mode"),
                    "data_as_of": obs.get("data_as_of"),
                }
                row_hash = ResearchStorageManager._semantic_hash(semantic_payload)
                existing = conn.execute(
                    """
                    SELECT *
                    FROM risk_free_rate_observations
                    WHERE series_id = ? AND observation_date = ?
                      AND source_profile = ? AND revision_id = ?
                    """,
                    (series_id, observation_date, source_profile, revision_id),
                ).fetchone()
                old_hash = None
                if existing is not None:
                    old_payload = dict(existing)
                    for key in (
                        "row_hash",
                        "row_version",
                        "ingestion_run_id",
                        "created_at",
                        "updated_at",
                    ):
                        old_payload.pop(key, None)
                    old_hash = existing["row_hash"] or ResearchStorageManager._semantic_hash(
                        old_payload
                    )
                changed = existing is None or old_hash != row_hash
                row_version = (
                    1
                    if existing is None
                    else int(existing["row_version"] or 1) + int(changed)
                )
                conn.execute(
                    """
                    INSERT INTO risk_free_rate_observations (
                        series_id, observation_date, source_profile, value, revision_id,
                        source, source_mode, data_as_of, row_hash, row_version,
                        ingestion_run_id, created_at, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(series_id, observation_date, source_profile, revision_id)
                    DO UPDATE SET value=excluded.value, source=excluded.source,
                        source_mode=excluded.source_mode, data_as_of=excluded.data_as_of,
                        row_hash=excluded.row_hash, row_version=excluded.row_version,
                        ingestion_run_id=excluded.ingestion_run_id,
                        updated_at=excluded.updated_at
                    """,
                    (
                        series_id,
                        observation_date,
                        source_profile,
                        obs.get("value"),
                        revision_id,
                        obs.get("source"),
                        obs.get("source_mode"),
                        obs.get("data_as_of"),
                        row_hash,
                        row_version,
                        obs.get("ingestion_run_id"),
                        now,
                        now,
                    ),
                )
                if changed:
                    append_change_record(
                        conn,
                        config=getattr(self, "change_watermark_config", None),
                        domain="interest_rate",
                        dataset="risk_free_rate_observations",
                        change_type="update" if existing else "insert",
                        business_key={
                            "series_id": series_id,
                            "observation_date": observation_date,
                            "source_profile": source_profile,
                            "revision_id": revision_id,
                        },
                        series_id=series_id,
                        observation_date=observation_date,
                        old_hash=old_hash,
                        new_hash=row_hash,
                        row_version=row_version,
                        source=obs.get("source"),
                        source_mode=obs.get("source_mode"),
                        source_profile=source_profile,
                        ingestion_run_id=obs.get("ingestion_run_id"),
                        changed_at=now,
                    )
            conn.commit()
        return len(observations)

    def list_risk_free_rate_series(self) -> List[Dict[str, Any]]:
        """列出所有无风险利率序列定义。"""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT * FROM risk_free_rate_series ORDER BY series_id"
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item.pop("row_hash", None)
            item.pop("row_version", None)
            result.append(item)
        return result

    def get_risk_free_rate_observations(
        self,
        series_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        revision_id: str = "latest",
    ) -> List[Dict[str, Any]]:
        """按日期区间读取某序列的观测值 (无数据返回空列表)。"""
        conditions = ["series_id = ?", "revision_id = ?"]
        params: list[Any] = [series_id, revision_id]
        if start_date:
            conditions.append("observation_date >= ?")
            params.append(str(start_date)[:10])
        if end_date:
            conditions.append("observation_date <= ?")
            params.append(str(end_date)[:10])
        where_sql = " AND ".join(conditions)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT observation_date, value, source, source_mode, data_as_of, revision_id
                FROM risk_free_rate_observations
                WHERE {where_sql}
                ORDER BY observation_date ASC
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def get_official_industry_classification(
        self,
        instrument_id: str,
        *,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Fetch one latest official industry classification row."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT
                    instrument_id,
                    symbol,
                    exchange,
                    taxonomy_system,
                    taxonomy_version,
                    official_industry_code,
                    official_start_date,
                    official_update_time,
                    mapped_industry_code,
                    mapped_industry_name,
                    mapped_industry_level,
                    mapped_parent_code,
                    mapping_status,
                    mapping_confidence,
                    source,
                    source_mode,
                    classification_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                FROM industry_official_classifications
                WHERE instrument_id = ?
                """,
                (instrument_id,),
            ).fetchone()

        if row is None:
            return None

        data = dict(row)
        classification_json = data.pop("classification_json", None)
        if include_snapshot:
            data["classification"] = self._deserialize_json(classification_json)
        return data

    def get_latest_analyst_forecast(
        self,
        instrument_id: str,
        *,
        include_details: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Fetch the latest analyst forecast snapshot for one instrument."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT
                    instrument_id,
                    symbol,
                    exchange,
                    as_of_date,
                    rating_summary,
                    report_count,
                    institution_count,
                    buy_count,
                    overweight_count,
                    neutral_count,
                    underperform_count,
                    sell_count,
                    eps_fy1,
                    eps_fy2,
                    net_profit_fy1,
                    net_profit_fy2,
                    pe_fy1,
                    pe_fy2,
                    source,
                    source_mode,
                    data_as_of,
                    forecast_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                FROM analyst_forecasts
                WHERE instrument_id = ?
                ORDER BY as_of_date DESC, updated_at DESC
                LIMIT 1
                """,
                (instrument_id,),
            ).fetchone()

        if row is None:
            return None

        data = dict(row)
        forecast_json = data.pop("forecast_json", None)
        if include_details:
            data["forecast"] = self._deserialize_json(forecast_json)
        return data

    def summarize_analyst_forecasts(self) -> Dict[str, Any]:
        """Return aggregate summary for analyst forecast metadata coverage."""
        return self._summarize_metadata_table(
            table_name="analyst_forecasts",
            date_column="as_of_date",
            extra_group_columns=(),
        )

    def count_analyst_forecasts_by_exchange(self) -> Dict[str, int]:
        """Count instruments with analyst forecast metadata per exchange."""
        return self._count_metadata_instruments_by_exchange("analyst_forecasts")

    def list_research_reports(
        self,
        instrument_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 20,
        include_details: bool = True,
    ) -> list[Dict[str, Any]]:
        """List research report metadata rows for one instrument."""
        query = """
            SELECT
                report_id,
                instrument_id,
                symbol,
                exchange,
                publish_date,
                report_title,
                institution_name,
                analyst_name,
                rating,
                rating_change,
                target_price,
                report_url,
                source,
                source_mode,
                data_as_of,
                report_json,
                ingestion_run_id,
                created_at,
                updated_at
            FROM research_reports
            WHERE instrument_id = ?
        """
        parameters: list[Any] = [instrument_id]
        if start_date:
            query += " AND publish_date >= ?"
            parameters.append(start_date)
        if end_date:
            query += " AND publish_date <= ?"
            parameters.append(end_date)
        query += " ORDER BY publish_date DESC, updated_at DESC"
        if limit > 0:
            query += " LIMIT ?"
            parameters.append(limit)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(query, tuple(parameters)).fetchall()

        items = []
        for row in rows:
            item = dict(row)
            report_json = item.pop("report_json", None)
            if include_details:
                item["report"] = self._deserialize_json(report_json)
            items.append(item)
        return items

    def summarize_research_reports(self) -> Dict[str, Any]:
        """Return aggregate summary for research report metadata coverage."""
        return self._summarize_metadata_table(
            table_name="research_reports",
            date_column="publish_date",
            extra_group_columns=("institution_name", "rating"),
        )

    def count_research_reports_by_exchange(self) -> Dict[str, int]:
        """Count instruments with research report metadata per exchange."""
        return self._count_metadata_instruments_by_exchange("research_reports")

    def list_sentiment_events(
        self,
        instrument_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        event_types: Optional[list[str]] = None,
        limit: int = 50,
        include_details: bool = True,
    ) -> list[Dict[str, Any]]:
        """List sentiment events for one instrument."""
        query = """
            SELECT
                event_id,
                instrument_id,
                symbol,
                exchange,
                event_date,
                event_type,
                event_subtype,
                title,
                sentiment_score,
                severity,
                source,
                source_mode,
                data_as_of,
                details_json,
                ingestion_run_id,
                created_at,
                updated_at
            FROM sentiment_events
            WHERE instrument_id = ?
        """
        parameters: list[Any] = [instrument_id]
        if start_date:
            query += " AND event_date >= ?"
            parameters.append(start_date)
        if end_date:
            query += " AND event_date <= ?"
            parameters.append(end_date)
        if event_types:
            placeholders = ", ".join("?" for _ in event_types)
            query += f" AND event_type IN ({placeholders})"
            parameters.extend(event_types)
        query += " ORDER BY event_date DESC, updated_at DESC"
        if limit > 0:
            query += " LIMIT ?"
            parameters.append(limit)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(query, tuple(parameters)).fetchall()

        items = []
        for row in rows:
            item = dict(row)
            details_json = item.pop("details_json", None)
            if include_details:
                item["details"] = self._deserialize_json(details_json)
            items.append(item)
        return items

    def summarize_sentiment_events(self) -> Dict[str, Any]:
        """Return aggregate summary for sentiment event metadata coverage."""
        return self._summarize_metadata_table(
            table_name="sentiment_events",
            date_column="event_date",
            extra_group_columns=("event_type", "severity"),
        )

    def count_sentiment_events_by_exchange(self) -> Dict[str, int]:
        """Count instruments with sentiment event metadata per exchange."""
        return self._count_metadata_instruments_by_exchange("sentiment_events")

    def get_sentiment_event_count(
        self,
        instrument_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        negative_only: bool = False,
    ) -> int:
        """Count sentiment events for one instrument."""
        query = """
            SELECT COUNT(1)
            FROM sentiment_events
            WHERE instrument_id = ?
        """
        parameters: list[Any] = [instrument_id]
        if start_date:
            query += " AND event_date >= ?"
            parameters.append(start_date)
        if end_date:
            query += " AND event_date <= ?"
            parameters.append(end_date)
        if negative_only:
            query += " AND sentiment_score < 0"

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            count = conn.execute(query, tuple(parameters)).fetchone()[0]

        return int(count or 0)

    def _summarize_metadata_table(
        self,
        *,
        table_name: str,
        date_column: str,
        extra_group_columns: tuple[str, ...],
    ) -> Dict[str, Any]:
        allowed_tables = {
            "analyst_forecasts",
            "research_reports",
            "sentiment_events",
        }
        allowed_columns = {
            "as_of_date",
            "publish_date",
            "event_date",
            "institution_name",
            "rating",
            "event_type",
            "severity",
        }
        if table_name not in allowed_tables:
            raise ValueError(f"unsupported metadata table: {table_name}")
        if date_column not in allowed_columns:
            raise ValueError(f"unsupported metadata date column: {date_column}")
        for column in extra_group_columns:
            if column not in allowed_columns:
                raise ValueError(f"unsupported metadata group column: {column}")

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            summary_row = conn.execute(
                f"""
                SELECT
                    COUNT(1) AS row_total,
                    COUNT(DISTINCT instrument_id) AS instrument_total,
                    MAX({date_column}) AS latest_item_date,
                    MAX(updated_at) AS latest_updated_at,
                    MAX(data_as_of) AS latest_data_as_of
                FROM {table_name}
                """
            ).fetchone()
            source_rows = conn.execute(
                f"""
                SELECT source, COUNT(DISTINCT instrument_id) AS instrument_count
                FROM {table_name}
                GROUP BY source
                """
            ).fetchall()
            mode_rows = conn.execute(
                f"""
                SELECT source_mode, COUNT(DISTINCT instrument_id) AS instrument_count
                FROM {table_name}
                GROUP BY source_mode
                """
            ).fetchall()
            extra_counts: Dict[str, Dict[str, int]] = {}
            for column in extra_group_columns:
                rows = conn.execute(
                    f"""
                    SELECT {column} AS group_value, COUNT(1) AS row_count
                    FROM {table_name}
                    GROUP BY {column}
                    """
                ).fetchall()
                extra_counts[f"{column}_counts"] = {
                    str(row["group_value"]): int(row["row_count"] or 0)
                    for row in rows
                    if row["group_value"] is not None
                }

        return {
            "row_total": int(
                (summary_row["row_total"] if summary_row is not None else 0) or 0
            ),
            "instrument_total": int(
                (summary_row["instrument_total"] if summary_row is not None else 0) or 0
            ),
            "source_counts": {
                str(row["source"]): int(row["instrument_count"] or 0)
                for row in source_rows
                if row["source"] is not None
            },
            "source_mode_counts": {
                str(row["source_mode"]): int(row["instrument_count"] or 0)
                for row in mode_rows
                if row["source_mode"] is not None
            },
            "latest_item_date": (
                None if summary_row is None else summary_row["latest_item_date"]
            ),
            "latest_updated_at": (
                None if summary_row is None else summary_row["latest_updated_at"]
            ),
            "latest_data_as_of": (
                None if summary_row is None else summary_row["latest_data_as_of"]
            ),
            **extra_counts,
        }

    def _count_metadata_instruments_by_exchange(self, table_name: str) -> Dict[str, int]:
        allowed_tables = {
            "analyst_forecasts",
            "research_reports",
            "sentiment_events",
        }
        if table_name not in allowed_tables:
            raise ValueError(f"unsupported metadata table: {table_name}")
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT exchange, COUNT(DISTINCT instrument_id) AS instrument_count
                FROM {table_name}
                GROUP BY exchange
                """
            ).fetchall()
        return {
            str(row["exchange"]): int(row["instrument_count"] or 0)
            for row in rows
            if row["exchange"] is not None
        }

    def get_latest_technical_indicator_snapshot(
        self,
        instrument_id: str,
        *,
        period: str = "1d",
        adjustment: Optional[str] = None,
        include_summary: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Fetch the latest persisted technical indicator snapshot."""
        query = """
            SELECT
                instrument_id,
                symbol,
                exchange,
                period,
                as_of_date,
                adjustment,
                applied_adjustment,
                calc_method,
                calc_version,
                parameter_hash,
                status,
                missing_reason,
                signal,
                trend_score,
                close_price,
                pct_change_1d,
                pct_change_20d,
                sma20,
                sma60,
                ema12,
                ema26,
                macd,
                macd_signal,
                macd_hist,
                rsi14,
                adx,
                plus_di,
                minus_di,
                stoch_k,
                stoch_d,
                cci,
                williams_r,
                boll_upper,
                boll_middle,
                boll_lower,
                atr14,
                volume_ratio,
                distance_to_sma20,
                distance_to_sma60,
                source,
                source_mode,
                data_as_of,
                summary_json,
                ingestion_run_id,
                created_at,
                updated_at
            FROM technical_indicator_latest
            WHERE instrument_id = ? AND period = ?
        """
        parameters: list[Any] = [instrument_id, period]
        if adjustment:
            query += " AND adjustment = ?"
            parameters.append(adjustment)
        query += " ORDER BY as_of_date DESC, updated_at DESC LIMIT 1"

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(query, tuple(parameters)).fetchone()

        if row is None:
            return None

        data = dict(row)
        summary_json = data.pop("summary_json", None)
        if include_summary:
            data["summary"] = self._deserialize_json(summary_json) or {}
        return data

    def summarize_technical_indicator_latest(
        self,
        *,
        period: Optional[str] = None,
        adjustment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return aggregate summary for persisted latest technical snapshots."""
        filters = []
        parameters: list[Any] = []
        if period:
            filters.append("period = ?")
            parameters.append(period)
        if adjustment:
            filters.append("adjustment = ?")
            parameters.append(adjustment)
        where_clause = "WHERE " + " AND ".join(filters) if filters else ""

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            summary_row = conn.execute(
                f"""
                SELECT
                    COUNT(1) AS row_total,
                    COUNT(DISTINCT instrument_id) AS instrument_total,
                    MAX(as_of_date) AS latest_as_of_date,
                    MAX(updated_at) AS latest_updated_at,
                    MAX(data_as_of) AS latest_data_as_of
                FROM technical_indicator_latest
                {where_clause}
                """,
                tuple(parameters),
            ).fetchone()
            source_rows = conn.execute(
                f"""
                SELECT source, COUNT(DISTINCT instrument_id) AS instrument_count
                FROM technical_indicator_latest
                {where_clause}
                GROUP BY source
                """,
                tuple(parameters),
            ).fetchall()
            mode_rows = conn.execute(
                f"""
                SELECT source_mode, COUNT(DISTINCT instrument_id) AS instrument_count
                FROM technical_indicator_latest
                {where_clause}
                GROUP BY source_mode
                """,
                tuple(parameters),
            ).fetchall()
            method_rows = conn.execute(
                f"""
                SELECT calc_method, COUNT(DISTINCT instrument_id) AS instrument_count
                FROM technical_indicator_latest
                {where_clause}
                GROUP BY calc_method
                """,
                tuple(parameters),
            ).fetchall()
            version_rows = conn.execute(
                f"""
                SELECT calc_version, COUNT(DISTINCT instrument_id) AS instrument_count
                FROM technical_indicator_latest
                {where_clause}
                GROUP BY calc_version
                """,
                tuple(parameters),
            ).fetchall()
            status_rows = conn.execute(
                f"""
                SELECT status, COUNT(DISTINCT instrument_id) AS instrument_count
                FROM technical_indicator_latest
                {where_clause}
                GROUP BY status
                """,
                tuple(parameters),
            ).fetchall()
            signal_rows = conn.execute(
                f"""
                SELECT signal, COUNT(DISTINCT instrument_id) AS instrument_count
                FROM technical_indicator_latest
                {where_clause}
                GROUP BY signal
                """,
                tuple(parameters),
            ).fetchall()

        return {
            "row_total": int(
                (summary_row["row_total"] if summary_row is not None else 0) or 0
            ),
            "instrument_total": int(
                (summary_row["instrument_total"] if summary_row is not None else 0) or 0
            ),
            "source_counts": {
                str(row["source"]): int(row["instrument_count"] or 0)
                for row in source_rows
                if row["source"] is not None
            },
            "source_mode_counts": {
                str(row["source_mode"]): int(row["instrument_count"] or 0)
                for row in mode_rows
                if row["source_mode"] is not None
            },
            "calc_method_counts": {
                str(row["calc_method"]): int(row["instrument_count"] or 0)
                for row in method_rows
                if row["calc_method"] is not None
            },
            "calc_version_counts": {
                str(row["calc_version"]): int(row["instrument_count"] or 0)
                for row in version_rows
                if row["calc_version"] is not None
            },
            "status_counts": {
                str(row["status"]): int(row["instrument_count"] or 0)
                for row in status_rows
                if row["status"] is not None
            },
            "signal_counts": {
                str(row["signal"]): int(row["instrument_count"] or 0)
                for row in signal_rows
                if row["signal"] is not None
            },
            "latest_as_of_date": (
                None if summary_row is None else summary_row["latest_as_of_date"]
            ),
            "latest_updated_at": (
                None if summary_row is None else summary_row["latest_updated_at"]
            ),
            "latest_data_as_of": (
                None if summary_row is None else summary_row["latest_data_as_of"]
            ),
        }

    def count_technical_indicator_latest_by_exchange(
        self,
        *,
        period: Optional[str] = None,
        adjustment: Optional[str] = None,
    ) -> Dict[str, int]:
        """Count instruments with latest technical snapshots by exchange."""
        filters = []
        parameters: list[Any] = []
        if period:
            filters.append("period = ?")
            parameters.append(period)
        if adjustment:
            filters.append("adjustment = ?")
            parameters.append(adjustment)
        where_clause = "WHERE " + " AND ".join(filters) if filters else ""

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT exchange, COUNT(DISTINCT instrument_id) AS instrument_count
                FROM technical_indicator_latest
                {where_clause}
                GROUP BY exchange
                """,
                tuple(parameters),
            ).fetchall()
        return {
            str(row["exchange"]): int(row["instrument_count"] or 0)
            for row in rows
            if row["exchange"] is not None
        }

    def get_latest_risk_snapshot(
        self,
        instrument_id: str,
        *,
        include_details: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Fetch the latest risk snapshot for one instrument."""
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT
                    instrument_id,
                    symbol,
                    exchange,
                    as_of_date,
                    benchmark_instrument_id,
                    volatility_20d,
                    volatility_60d,
                    beta_60d,
                    max_drawdown_252d,
                    average_turnover_20d,
                    average_amount_20d,
                    liability_to_asset,
                    current_ratio,
                    operating_cf_to_net_income,
                    negative_event_count_30d,
                    risk_score,
                    risk_level,
                    calc_method,
                    calc_version,
                    parameter_hash,
                    source,
                    source_mode,
                    data_as_of,
                    details_json,
                    ingestion_run_id,
                    created_at,
                    updated_at
                FROM risk_snapshots
                WHERE instrument_id = ?
                ORDER BY as_of_date DESC, updated_at DESC
                LIMIT 1
                """,
                (instrument_id,),
            ).fetchone()

        if row is None:
            return None

        data = dict(row)
        details_json = data.pop("details_json", None)
        if include_details:
            data["details"] = self._deserialize_json(details_json)
        return data

    def get_valuation_history_rows(
        self,
        instrument_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 120,
        include_details: bool = True,
        calc_method: Optional[str] = None,
        calc_version: Optional[str] = None,
        parameter_hash: Optional[str] = None,
        parameter_hashes: Optional[List[str]] = None,
    ) -> list[Dict[str, Any]]:
        """Fetch valuation history rows for one instrument."""
        if self._uses_separate_valuation_database() and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.get_valuation_history_rows(
                    instrument_id,
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit,
                    include_details=include_details,
                    calc_method=calc_method,
                    calc_version=calc_version,
                    parameter_hash=parameter_hash,
                    parameter_hashes=parameter_hashes,
                )
        query = """
            SELECT
                instrument_id,
                symbol,
                exchange,
                as_of_date,
                currency,
                close_price,
                market_cap,
                float_market_cap,
                pe_ratio,
                pb_ratio,
                ps_ratio,
                pe_static,
                pe_ttm,
                pe_forward,
                pb_mrq,
                ps_static,
                ps_ttm,
                ps_forward,
                calc_method,
                calc_version,
                parameter_hash,
                source,
                source_mode,
                data_as_of,
                details_json,
                ingestion_run_id,
                created_at,
                updated_at
            FROM valuation_history
            WHERE instrument_id = ?
        """
        parameters: list[Any] = [instrument_id]
        if start_date:
            query += " AND as_of_date >= ?"
            parameters.append(start_date)
        if end_date:
            query += " AND as_of_date <= ?"
            parameters.append(end_date)
        if calc_method:
            query += " AND calc_method = ?"
            parameters.append(calc_method)
        if calc_version:
            query += " AND calc_version = ?"
            parameters.append(calc_version)
        hash_values = [
            str(item)
            for item in (parameter_hashes or ([parameter_hash] if parameter_hash else []))
            if str(item)
        ]
        if hash_values:
            placeholders = ", ".join("?" for _ in hash_values)
            query += f" AND parameter_hash IN ({placeholders})"
            parameters.extend(hash_values)
        query += " ORDER BY as_of_date DESC"
        if parameter_hash and hash_values:
            query += ", CASE WHEN parameter_hash = ? THEN 0 ELSE 1 END"
            parameters.append(parameter_hash)
        query_limit = limit
        if parameter_hash and len(hash_values) > 1 and limit > 0:
            query_limit = limit * len(hash_values)
        if query_limit > 0:
            query += " LIMIT ?"
            parameters.append(query_limit)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(query, tuple(parameters)).fetchall()

        items = []
        seen_dates: set[str] = set()
        for row in rows:
            item = dict(row)
            if parameter_hash and len(hash_values) > 1:
                as_of_date = str(item.get("as_of_date") or "")
                if as_of_date in seen_dates:
                    continue
                seen_dates.add(as_of_date)
            details_json = item.pop("details_json", None)
            if include_details:
                item["details"] = self._deserialize_json(details_json)
            items.append(item)
        if limit > 0:
            items = items[:limit]
        return items

    def get_valuation_inputs(
        self,
        instrument_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 0,
        include_diagnostics: bool = True,
    ) -> list[Dict[str, Any]]:
        """Fetch explicit valuation inputs for one instrument."""
        if self._uses_separate_valuation_database() and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.get_valuation_inputs(
                    instrument_id,
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit,
                    include_diagnostics=include_diagnostics,
                )
        query = """
            SELECT
                instrument_id,
                symbol,
                exchange,
                as_of_date,
                currency,
                market_cap,
                shares_outstanding,
                float_market_cap,
                float_shares,
                source,
                source_mode,
                input_kind,
                unit,
                data_as_of,
                diagnostics_json,
                ingestion_run_id,
                created_at,
                updated_at
            FROM valuation_inputs
            WHERE instrument_id = ?
        """
        parameters: list[Any] = [instrument_id]
        if start_date:
            query += " AND as_of_date >= ?"
            parameters.append(start_date)
        if end_date:
            query += " AND as_of_date <= ?"
            parameters.append(end_date)
        query += " ORDER BY as_of_date DESC, updated_at DESC"
        if limit > 0:
            query += " LIMIT ?"
            parameters.append(limit)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(query, tuple(parameters)).fetchall()

        items = []
        for row in rows:
            item = dict(row)
            diagnostics_json = item.pop("diagnostics_json", None)
            if include_diagnostics:
                item["diagnostics"] = self._deserialize_json(diagnostics_json) or {}
            items.append(item)
        return items

    def get_latest_valuation_input(
        self,
        instrument_id: str,
        *,
        as_of_date: str,
        include_diagnostics: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Fetch the latest valuation input available on or before as_of_date."""
        rows = self.get_valuation_inputs(
            instrument_id,
            end_date=as_of_date,
            limit=0,
            include_diagnostics=include_diagnostics,
        )
        available_rows = [
            row for row in rows
            if str(row.get("data_as_of") or row.get("as_of_date") or "") <= as_of_date
        ]
        return available_rows[0] if available_rows else None

    def get_latest_valuation_history_row(
        self,
        instrument_id: str,
        *,
        include_details: bool = True,
        calc_method: Optional[str] = None,
        calc_version: Optional[str] = None,
        parameter_hash: Optional[str] = None,
        parameter_hashes: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch the latest valuation history row for one instrument."""
        rows = self.get_valuation_history_rows(
            instrument_id,
            limit=1,
            include_details=include_details,
            calc_method=calc_method,
            calc_version=calc_version,
            parameter_hash=parameter_hash,
            parameter_hashes=parameter_hashes,
        )
        if not rows:
            return None
        return rows[0]

    def summarize_valuation_history(
        self,
        *,
        calc_method: Optional[str] = None,
        calc_version: Optional[str] = None,
        parameter_hash: Optional[str] = None,
        parameter_hashes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Return aggregate summary for instruments with valuation history."""
        return self.summarize_valuation_history_for_identity(
            calc_method=calc_method,
            calc_version=calc_version,
            parameter_hash=parameter_hash,
            parameter_hashes=parameter_hashes,
        )

    def summarize_valuation_history_for_identity(
        self,
        *,
        calc_method: Optional[str] = None,
        calc_version: Optional[str] = None,
        parameter_hash: Optional[str] = None,
        parameter_hashes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Return aggregate summary for instruments with valuation history."""
        if self._uses_separate_valuation_database() and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.summarize_valuation_history_for_identity(
                    calc_method=calc_method,
                    calc_version=calc_version,
                    parameter_hash=parameter_hash,
                    parameter_hashes=parameter_hashes,
                )
        filters: list[str] = []
        params: list[Any] = []
        if calc_method:
            filters.append("calc_method = ?")
            params.append(calc_method)
        if calc_version:
            filters.append("calc_version = ?")
            params.append(calc_version)
        hash_values = [
            str(item)
            for item in (parameter_hashes or ([parameter_hash] if parameter_hash else []))
            if str(item)
        ]
        if hash_values:
            placeholders = ", ".join("?" for _ in hash_values)
            filters.append(f"parameter_hash IN ({placeholders})")
            params.extend(hash_values)
        where_clause = "" if not filters else "WHERE " + " AND ".join(filters)
        latest_where = "" if not filters else "WHERE " + " AND ".join(filters)
        latest_rows_query = """
            WITH latest AS (
                SELECT instrument_id, MAX(as_of_date) AS latest_as_of_date
                FROM valuation_history
                {latest_where}
                GROUP BY instrument_id
            )
            SELECT vh.*
            FROM valuation_history vh
            JOIN latest
              ON latest.instrument_id = vh.instrument_id
             AND latest.latest_as_of_date = vh.as_of_date
            {where_clause}
        """.format(latest_where=latest_where, where_clause=where_clause.replace("calc_", "vh.calc_").replace("parameter_hash", "vh.parameter_hash"))
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            summary_row = conn.execute(
                f"""
                SELECT
                    COUNT(DISTINCT instrument_id) AS total,
                    MAX(as_of_date) AS latest_as_of_date,
                    MAX(updated_at) AS latest_updated_at,
                    MAX(data_as_of) AS latest_data_as_of
                FROM valuation_history
                {where_clause}
                """,
                tuple(params),
            ).fetchone()
            source_rows = conn.execute(
                f"""
                SELECT source, COUNT(DISTINCT instrument_id) AS row_count
                FROM ({latest_rows_query})
                GROUP BY source
                """,
                tuple(params + params),
            ).fetchall()
            mode_rows = conn.execute(
                f"""
                SELECT source_mode, COUNT(DISTINCT instrument_id) AS row_count
                FROM ({latest_rows_query})
                GROUP BY source_mode
                """,
                tuple(params + params),
            ).fetchall()
            method_rows = conn.execute(
                f"""
                SELECT calc_method, COUNT(DISTINCT instrument_id) AS row_count
                FROM ({latest_rows_query})
                GROUP BY calc_method
                """,
                tuple(params + params),
            ).fetchall()
            version_rows = conn.execute(
                f"""
                SELECT calc_version, COUNT(DISTINCT instrument_id) AS row_count
                FROM ({latest_rows_query})
                GROUP BY calc_version
                """,
                tuple(params + params),
            ).fetchall()

        return {
            "total": int((summary_row["total"] if summary_row is not None else 0) or 0),
            "source_counts": {
                str(row["source"]): int(row["row_count"] or 0)
                for row in source_rows
                if row["source"] is not None
            },
            "source_mode_counts": {
                str(row["source_mode"]): int(row["row_count"] or 0)
                for row in mode_rows
                if row["source_mode"] is not None
            },
            "calc_method_counts": {
                str(row["calc_method"]): int(row["row_count"] or 0)
                for row in method_rows
                if row["calc_method"] is not None
            },
            "calc_version_counts": {
                str(row["calc_version"]): int(row["row_count"] or 0)
                for row in version_rows
                if row["calc_version"] is not None
            },
            "latest_as_of_date": (
                None if summary_row is None else summary_row["latest_as_of_date"]
            ),
            "latest_updated_at": (
                None if summary_row is None else summary_row["latest_updated_at"]
            ),
            "latest_data_as_of": (
                None if summary_row is None else summary_row["latest_data_as_of"]
            ),
        }

    def count_valuation_history_by_exchange(
        self,
        *,
        calc_method: Optional[str] = None,
        calc_version: Optional[str] = None,
        parameter_hash: Optional[str] = None,
        parameter_hashes: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """Count instruments with valuation history per exchange."""
        return self.count_valuation_history_by_exchange_for_identity(
            calc_method=calc_method,
            calc_version=calc_version,
            parameter_hash=parameter_hash,
            parameter_hashes=parameter_hashes,
        )

    def count_valuation_history_by_exchange_for_identity(
        self,
        *,
        calc_method: Optional[str] = None,
        calc_version: Optional[str] = None,
        parameter_hash: Optional[str] = None,
        parameter_hashes: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """Count instruments with valuation history per exchange."""
        if self._uses_separate_valuation_database() and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.count_valuation_history_by_exchange_for_identity(
                    calc_method=calc_method,
                    calc_version=calc_version,
                    parameter_hash=parameter_hash,
                    parameter_hashes=parameter_hashes,
                )
        filters: list[str] = []
        params: list[Any] = []
        if calc_method:
            filters.append("calc_method = ?")
            params.append(calc_method)
        if calc_version:
            filters.append("calc_version = ?")
            params.append(calc_version)
        hash_values = [
            str(item)
            for item in (parameter_hashes or ([parameter_hash] if parameter_hash else []))
            if str(item)
        ]
        if hash_values:
            placeholders = ", ".join("?" for _ in hash_values)
            filters.append(f"parameter_hash IN ({placeholders})")
            params.extend(hash_values)
        where_clause = "" if not filters else "WHERE " + " AND ".join(filters)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT exchange, COUNT(DISTINCT instrument_id) AS row_count
                FROM valuation_history
                {where_clause}
                GROUP BY exchange
                """,
                tuple(params),
            ).fetchall()
        return {
            str(row["exchange"]): int(row["row_count"] or 0)
            for row in rows
            if row["exchange"] is not None
        }

    def summarize_valuation_input_coverage(
        self,
        *,
        instrument_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Return aggregate coverage for explicit market-cap/share-count inputs."""
        if self._uses_separate_valuation_database() and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.summarize_valuation_input_coverage(
                    instrument_ids=instrument_ids,
                )
        filters: List[str] = []
        params: List[Any] = []
        if instrument_ids is not None:
            normalized_ids = [
                str(instrument_id).strip()
                for instrument_id in instrument_ids
                if str(instrument_id).strip()
            ]
            if not normalized_ids:
                return {
                    "instrument_count": 0,
                    "market_cap_count": 0,
                    "shares_outstanding_count": 0,
                    "source_counts": {},
                    "source_mode_counts": {},
                    "latest_as_of_date": None,
                    "latest_updated_at": None,
                }
            placeholders = ", ".join("?" for _ in normalized_ids)
            filters.append(f"vi.instrument_id IN ({placeholders})")
            params.extend(normalized_ids)
        where_clause = "" if not filters else "WHERE " + " AND ".join(filters)
        latest_rows_query = f"""
            WITH latest AS (
                SELECT instrument_id, MAX(as_of_date) AS latest_as_of_date
                FROM valuation_inputs
                GROUP BY instrument_id
            )
            SELECT vi.*
            FROM valuation_inputs vi
            JOIN latest
              ON latest.instrument_id = vi.instrument_id
             AND latest.latest_as_of_date = vi.as_of_date
            {where_clause}
        """
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                f"""
                SELECT
                    COUNT(DISTINCT instrument_id) AS instrument_count,
                    COUNT(DISTINCT CASE WHEN market_cap IS NOT NULL AND market_cap > 0 THEN instrument_id END) AS market_cap_count,
                    COUNT(DISTINCT CASE WHEN shares_outstanding IS NOT NULL AND shares_outstanding > 0 THEN instrument_id END) AS shares_outstanding_count,
                    COUNT(DISTINCT CASE WHEN (market_cap IS NOT NULL AND market_cap > 0) OR (shares_outstanding IS NOT NULL AND shares_outstanding > 0) THEN instrument_id END) AS usable_input_count,
                    MAX(as_of_date) AS latest_as_of_date,
                    MAX(updated_at) AS latest_updated_at
                FROM ({latest_rows_query})
                """,
                tuple(params),
            ).fetchone()
            source_rows = conn.execute(
                f"""
                SELECT source, COUNT(DISTINCT instrument_id) AS row_count
                FROM ({latest_rows_query})
                GROUP BY source
                """,
                tuple(params),
            ).fetchall()
            mode_rows = conn.execute(
                f"""
                SELECT source_mode, COUNT(DISTINCT instrument_id) AS row_count
                FROM ({latest_rows_query})
                GROUP BY source_mode
                """,
                tuple(params),
            ).fetchall()

        instrument_count = int((row["instrument_count"] if row is not None else 0) or 0)
        market_cap_count = int((row["market_cap_count"] if row is not None else 0) or 0)
        shares_count = int(
            (row["shares_outstanding_count"] if row is not None else 0) or 0
        )
        usable_count = int((row["usable_input_count"] if row is not None else 0) or 0)
        return {
            "instrument_count": instrument_count,
            "market_cap_count": market_cap_count,
            "shares_outstanding_count": shares_count,
            "usable_input_count": usable_count,
            "source_counts": {
                str(item["source"]): int(item["row_count"] or 0)
                for item in source_rows
                if item["source"] is not None
            },
            "source_mode_counts": {
                str(item["source_mode"]): int(item["row_count"] or 0)
                for item in mode_rows
                if item["source_mode"] is not None
            },
            "latest_as_of_date": None if row is None else row["latest_as_of_date"],
            "latest_updated_at": None if row is None else row["latest_updated_at"],
        }

    def summarize_valuation_metric_coverage(
        self,
        *,
        metric_fields: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        calc_method: Optional[str] = None,
        calc_version: Optional[str] = None,
        parameter_hash: Optional[str] = None,
        parameter_hashes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Summarize latest valuation metric coverage by metric variant."""
        if self._uses_separate_valuation_database() and self._active_db_path is None:
            with self.valuation_database_scope():
                return self.summarize_valuation_metric_coverage(
                    metric_fields=metric_fields,
                    instrument_ids=instrument_ids,
                    calc_method=calc_method,
                    calc_version=calc_version,
                    parameter_hash=parameter_hash,
                    parameter_hashes=parameter_hashes,
                )
        allowed_fields = {
            "pe_ratio",
            "pb_ratio",
            "ps_ratio",
            "pe_static",
            "pe_ttm",
            "pe_forward",
            "pb_mrq",
            "ps_static",
            "ps_ttm",
            "ps_forward",
        }
        fields = [
            field
            for field in (metric_fields or sorted(allowed_fields))
            if field in allowed_fields
        ]
        if not fields:
            return {"metrics": {}, "instrument_count": 0}

        filters: List[str] = []
        params: List[Any] = []
        if instrument_ids is not None:
            normalized_ids = [
                str(instrument_id).strip()
                for instrument_id in instrument_ids
                if str(instrument_id).strip()
            ]
            if not normalized_ids:
                return {"metrics": {}, "instrument_count": 0}
            placeholders = ", ".join("?" for _ in normalized_ids)
            filters.append(f"vh.instrument_id IN ({placeholders})")
            params.extend(normalized_ids)
        latest_filters: List[str] = []
        latest_params: List[Any] = []
        if calc_method:
            filters.append("vh.calc_method = ?")
            params.append(calc_method)
            latest_filters.append("calc_method = ?")
            latest_params.append(calc_method)
        if calc_version:
            filters.append("vh.calc_version = ?")
            params.append(calc_version)
            latest_filters.append("calc_version = ?")
            latest_params.append(calc_version)
        hash_values = [
            str(item)
            for item in (parameter_hashes or ([parameter_hash] if parameter_hash else []))
            if str(item)
        ]
        if hash_values:
            placeholders = ", ".join("?" for _ in hash_values)
            filters.append(f"vh.parameter_hash IN ({placeholders})")
            params.extend(hash_values)
            latest_filters.append(f"parameter_hash IN ({placeholders})")
            latest_params.extend(hash_values)
        where_clause = "" if not filters else "WHERE " + " AND ".join(filters)
        latest_where_clause = (
            "" if not latest_filters else "WHERE " + " AND ".join(latest_filters)
        )

        metric_selects = ",\n                    ".join(
            "COUNT(DISTINCT CASE "
            f"WHEN vh.{field} IS NOT NULL AND vh.{field} > 0 "
            f"THEN vh.instrument_id END) AS {field}_count"
            for field in fields
        )
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            row = conn.execute(
                f"""
                WITH latest AS (
                    SELECT instrument_id, MAX(as_of_date) AS latest_as_of_date
                    FROM valuation_history
                    {latest_where_clause}
                    GROUP BY instrument_id
                )
                SELECT
                    COUNT(DISTINCT vh.instrument_id) AS instrument_count,
                    {metric_selects}
                FROM valuation_history vh
                JOIN latest
                  ON latest.instrument_id = vh.instrument_id
                 AND latest.latest_as_of_date = vh.as_of_date
                {where_clause}
                """,
                tuple(latest_params + params),
            ).fetchone()

        instrument_count = int((row["instrument_count"] if row is not None else 0) or 0)
        metrics = {}
        for field in fields:
            covered = int((row[f"{field}_count"] if row is not None else 0) or 0)
            metrics[field] = {
                "covered_instruments": covered,
                "coverage_ratio": (
                    covered / instrument_count if instrument_count > 0 else None
                ),
            }
        return {
            "instrument_count": instrument_count,
            "metrics": metrics,
        }

    def get_latest_peer_valuation_rows(
        self,
        benchmark_code: str,
        *,
        exclude_instrument_id: Optional[str] = None,
        taxonomy_system: str = "sw",
        taxonomy_version: Optional[str] = None,
        benchmark_field: str = "sw_l2_code",
        limit: Optional[int] = None,
        include_details: bool = False,
        calc_method: Optional[str] = None,
        calc_version: Optional[str] = None,
        parameter_hash: Optional[str] = None,
        parameter_hashes: Optional[List[str]] = None,
    ) -> list[Dict[str, Any]]:
        """Fetch latest valuation rows for peers in the configured Shenwan level."""
        allowed_benchmark_fields = {"sw_l1_code", "sw_l2_code", "sw_l3_code"}
        if benchmark_field not in allowed_benchmark_fields:
            return []

        membership_query = f"""
            SELECT instrument_id
            FROM industry_memberships
            WHERE taxonomy_system = ?
              AND mapping_status = 'authoritative'
              AND {benchmark_field} = ?
        """
        membership_parameters: list[Any] = [taxonomy_system, benchmark_code]
        if taxonomy_version is not None:
            membership_query += " AND taxonomy_version = ?"
            membership_parameters.append(taxonomy_version)
        if exclude_instrument_id is not None:
            membership_query += " AND instrument_id != ?"
            membership_parameters.append(exclude_instrument_id)

        previous_db_path = self._active_db_path
        if self._uses_separate_valuation_database() and previous_db_path is None:
            self._active_db_path = self.db_path
        try:
            with self.get_connection() as conn:
                self._apply_pragmas(conn)
                peer_ids = [
                    row["instrument_id"]
                    for row in conn.execute(
                        membership_query,
                        tuple(membership_parameters),
                    ).fetchall()
                ]
        finally:
            self._active_db_path = previous_db_path

        if not peer_ids:
            return []

        peer_placeholders = ", ".join("?" for _ in peer_ids)
        valuation_filters: list[str] = []
        latest_filters: list[str] = []
        valuation_params: list[Any] = []
        latest_params: list[Any] = []
        if calc_method:
            valuation_filters.append("vh.calc_method = ?")
            valuation_params.append(calc_method)
            latest_filters.append("vh2.calc_method = ?")
            latest_params.append(calc_method)
        if calc_version:
            valuation_filters.append("vh.calc_version = ?")
            valuation_params.append(calc_version)
            latest_filters.append("vh2.calc_version = ?")
            latest_params.append(calc_version)
        hash_values = [
            str(item)
            for item in (parameter_hashes or ([parameter_hash] if parameter_hash else []))
            if str(item)
        ]
        if hash_values:
            hash_placeholders = ", ".join("?" for _ in hash_values)
            valuation_filters.append(f"vh.parameter_hash IN ({hash_placeholders})")
            valuation_params.extend(hash_values)
            latest_filters.append(f"vh2.parameter_hash IN ({hash_placeholders})")
            latest_params.extend(hash_values)
        valuation_where = (
            "" if not valuation_filters else " AND " + " AND ".join(valuation_filters)
        )
        latest_where = (
            "" if not latest_filters else " AND " + " AND ".join(latest_filters)
        )
        latest_query = f"""
            SELECT
                vh.instrument_id,
                vh.symbol,
                vh.exchange,
                vh.as_of_date,
                vh.currency,
                vh.close_price,
                vh.market_cap,
                vh.pe_ratio,
                vh.pb_ratio,
                vh.ps_ratio,
                vh.pe_static,
                vh.pe_ttm,
                vh.pe_forward,
                vh.pb_mrq,
                vh.ps_static,
                vh.ps_ttm,
                vh.ps_forward,
                vh.calc_method,
                vh.calc_version,
                vh.parameter_hash,
                vh.source,
                vh.source_mode,
                vh.data_as_of,
                vh.details_json,
                vh.ingestion_run_id,
                vh.created_at,
                vh.updated_at
            FROM valuation_history vh
            WHERE vh.instrument_id IN ({peer_placeholders})
              {valuation_where}
              AND vh.as_of_date = (
                  SELECT MAX(vh2.as_of_date)
                  FROM valuation_history vh2
                  WHERE vh2.instrument_id = vh.instrument_id
                    {latest_where}
              )
            ORDER BY vh.pe_ratio ASC, vh.instrument_id ASC
        """
        query_params = tuple(peer_ids) + tuple(valuation_params) + tuple(latest_params)
        if self._uses_separate_valuation_database() and self._active_db_path is None:
            self._active_db_path = self.valuation_db_path
        try:
            with self.get_connection() as conn:
                self._apply_pragmas(conn)
                sql_limit = limit
                if parameter_hash and len(hash_values) > 1:
                    sql_limit = None
                if sql_limit is not None and sql_limit > 0:
                    latest_query += " LIMIT ?"
                    valuation_rows = conn.execute(
                        latest_query,
                        query_params + (sql_limit,),
                    ).fetchall()
                else:
                    valuation_rows = conn.execute(
                        latest_query,
                        query_params,
                    ).fetchall()
        finally:
            self._active_db_path = previous_db_path

        items = []
        by_instrument: dict[str, Dict[str, Any]] = {}
        for row in valuation_rows:
            item = dict(row)
            if parameter_hash and len(hash_values) > 1:
                instrument_id = str(item.get("instrument_id") or "")
                existing = by_instrument.get(instrument_id)
                if existing is None or (
                    existing.get("parameter_hash") != parameter_hash
                    and item.get("parameter_hash") == parameter_hash
                ):
                    by_instrument[instrument_id] = item
                continue
            details_json = item.pop("details_json", None)
            if include_details:
                item["details"] = self._deserialize_json(details_json)
            items.append(item)
            if limit is not None and limit > 0 and len(items) >= limit:
                break
        if parameter_hash and len(hash_values) > 1:
            raw_items = sorted(
                by_instrument.values(),
                key=lambda item: (
                    item.get("pe_ratio") is None,
                    item.get("pe_ratio") if item.get("pe_ratio") is not None else 0,
                    str(item.get("instrument_id") or ""),
                ),
            )
            for item in raw_items:
                details_json = item.pop("details_json", None)
                if include_details:
                    item["details"] = self._deserialize_json(details_json)
                items.append(item)
                if limit is not None and limit > 0 and len(items) >= limit:
                    break
        return items

    @staticmethod
    def _apply_pragmas(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")

    @staticmethod
    def _deserialize_json(value: Optional[str]) -> Optional[Dict[str, Any]]:
        if not value:
            return None
        data = json.loads(value)
        if isinstance(data, dict):
            return ResearchStorageManager.expand_valuation_history_details_payload(data)
        return data

    @staticmethod
    def _valuation_history_details_json(value: Dict[str, Any]) -> str:
        compacted = ResearchStorageManager.compact_valuation_history_details_payload(
            value
        )
        return json.dumps(
            compacted,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def compact_valuation_history_details_payload(
        value: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Return the compact on-disk valuation-history details representation."""
        if not isinstance(value, dict):
            return {}
        if value.get("v") == VALUATION_HISTORY_DETAILS_COMPACT_VERSION and "m" in value:
            return value

        compact: Dict[str, Any] = {"v": VALUATION_HISTORY_DETAILS_COMPACT_VERSION}
        top_level_map = {
            "valuation_scope": "vs",
            "missing_reason": "mr",
            "report_period": "rp",
            "latest_financial_report_period": "rp",
            "latest_financial_available_date": "lad",
            "shares_outstanding": "sh",
            "float_market_cap": "fmc",
            "revenue": "rev",
            "net_income": "ni",
            "equity": "eq",
        }
        for source_key, compact_key in top_level_map.items():
            if compact_key in compact:
                continue
            item = value.get(source_key)
            if ResearchStorageManager._keep_compact_json_value(item):
                compact[compact_key] = item

        valuation_input = value.get("valuation_input")
        if isinstance(valuation_input, dict):
            compact_input = ResearchStorageManager._map_compact_json_keys(
                valuation_input,
                VALUATION_HISTORY_INPUT_DETAIL_KEY_MAP,
            )
            if compact_input:
                compact["vi"] = compact_input

        metrics = value.get("metrics")
        if isinstance(metrics, dict):
            compact_metrics: Dict[str, Dict[str, Any]] = {}
            shared_numerator = ResearchStorageManager._first_metric_numerator(metrics)
            if ResearchStorageManager._keep_compact_json_value(shared_numerator):
                compact["mc"] = shared_numerator
            for metric_name, metric_detail in metrics.items():
                if not isinstance(metric_detail, dict):
                    continue
                compact_metric = ResearchStorageManager._compact_metric_detail(
                    metric_detail,
                    shared_numerator=shared_numerator,
                )
                if compact_metric:
                    compact_metrics[str(metric_name)] = compact_metric
            if compact_metrics:
                compact["m"] = compact_metrics

        if set(compact) == {"v"}:
            return value
        return compact

    @staticmethod
    def expand_valuation_history_details_payload(value: Dict[str, Any]) -> Dict[str, Any]:
        """Expand compact valuation-history details for existing read APIs."""
        if value.get("v") != VALUATION_HISTORY_DETAILS_COMPACT_VERSION:
            return value

        expanded: Dict[str, Any] = {}
        reverse_top_level_map = {
            "vs": "valuation_scope",
            "mr": "missing_reason",
            "rp": "report_period",
            "lad": "latest_financial_available_date",
            "sh": "shares_outstanding",
            "fmc": "float_market_cap",
            "rev": "revenue",
            "ni": "net_income",
            "eq": "equity",
        }
        for compact_key, source_key in reverse_top_level_map.items():
            item = value.get(compact_key)
            if ResearchStorageManager._keep_compact_json_value(item):
                expanded[source_key] = item
        if "rp" in value:
            expanded["latest_financial_report_period"] = value.get("rp")

        valuation_input = value.get("vi")
        if isinstance(valuation_input, dict):
            expanded["valuation_input"] = ResearchStorageManager._unmap_compact_json_keys(
                valuation_input,
                VALUATION_HISTORY_INPUT_DETAIL_KEY_MAP,
            )

        metrics = value.get("m")
        if isinstance(metrics, dict):
            expanded_metrics: Dict[str, Dict[str, Any]] = {}
            for metric_name, metric_detail in metrics.items():
                if not isinstance(metric_detail, dict):
                    continue
                expanded_metrics[str(metric_name)] = (
                    ResearchStorageManager._expand_metric_detail(
                        str(metric_name),
                        metric_detail,
                        shared_numerator=value.get("mc"),
                    )
                )
            expanded["metrics"] = expanded_metrics

        return expanded

    @staticmethod
    def _compact_metric_detail(
        metric_detail: Dict[str, Any],
        *,
        shared_numerator: Any,
    ) -> Dict[str, Any]:
        compact = ResearchStorageManager._map_compact_json_keys(
            metric_detail,
            VALUATION_HISTORY_METRIC_DETAIL_KEY_MAP,
        )
        compact.pop("s", None)
        if compact.get("n") == shared_numerator:
            compact.pop("n", None)
        return compact

    @staticmethod
    def _expand_metric_detail(
        metric_name: str,
        metric_detail: Dict[str, Any],
        *,
        shared_numerator: Any,
    ) -> Dict[str, Any]:
        expanded = ResearchStorageManager._unmap_compact_json_keys(
            metric_detail,
            VALUATION_HISTORY_METRIC_DETAIL_KEY_MAP,
        )
        expanded["metric"] = metric_name
        expanded.setdefault("value", None)
        if "numerator" not in expanded and ResearchStorageManager._keep_compact_json_value(
            shared_numerator
        ):
            expanded["numerator"] = shared_numerator
        expanded.setdefault("denominator", None)
        expanded.setdefault("denominator_fact", None)
        expanded.setdefault("report_periods", [])
        expanded.setdefault("availability_dates", [])
        expanded.setdefault("calc_method", None)
        if "missing_reason" not in expanded:
            expanded["missing_reason"] = None
        expanded["status"] = (
            "available"
            if expanded.get("value") is not None
            and expanded.get("missing_reason") is None
            else "unavailable"
        )
        return expanded

    @staticmethod
    def _map_compact_json_keys(
        value: Dict[str, Any],
        key_map: Dict[str, str],
    ) -> Dict[str, Any]:
        return {
            compact_key: item
            for source_key, compact_key in key_map.items()
            for item in [value.get(source_key)]
            if ResearchStorageManager._keep_compact_json_value(item)
        }

    @staticmethod
    def _unmap_compact_json_keys(
        value: Dict[str, Any],
        key_map: Dict[str, str],
    ) -> Dict[str, Any]:
        reverse_map = {compact_key: source_key for source_key, compact_key in key_map.items()}
        return {
            source_key: item
            for compact_key, source_key in reverse_map.items()
            for item in [value.get(compact_key)]
            if ResearchStorageManager._keep_compact_json_value(item)
        }

    @staticmethod
    def _first_metric_numerator(metrics: Dict[str, Any]) -> Optional[Any]:
        for metric_detail in metrics.values():
            if not isinstance(metric_detail, dict):
                continue
            numerator = metric_detail.get("numerator")
            if ResearchStorageManager._keep_compact_json_value(numerator):
                return numerator
        return None

    @staticmethod
    def _keep_compact_json_value(value: Any) -> bool:
        return value is not None and value != [] and value != {}

    @staticmethod
    def _json_text(value: Dict[str, Any]) -> str:
        return json.dumps(value or {}, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _semantic_hash(value: Dict[str, Any]) -> str:
        raw = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _industry_taxonomy_semantic_payload(
        snapshot: IndustryTaxonomySnapshot,
    ) -> Dict[str, Any]:
        return {
            "taxonomy_system": snapshot.taxonomy_system,
            "taxonomy_version": snapshot.taxonomy_version or "",
            "industry_code": snapshot.industry_code,
            "industry_name": snapshot.industry_name,
            "industry_level": snapshot.industry_level,
            "parent_code": snapshot.parent_code,
            "sw_index_code": snapshot.sw_index_code,
            "aliases_json": snapshot.aliases_json or {},
            "source_classification": snapshot.source_classification,
            "source": snapshot.source,
            "source_mode": snapshot.source_mode,
            "is_active": 1,
        }

    @classmethod
    def _industry_taxonomy_row_semantic_payload(
        cls,
        row: sqlite3.Row,
    ) -> Dict[str, Any]:
        item = dict(row)
        item["aliases_json"] = cls._deserialize_json(item.get("aliases_json")) or {}
        for key in ("created_at", "updated_at", "row_hash", "row_version"):
            item.pop(key, None)
        return item

    @staticmethod
    def _industry_membership_semantic_payload(
        snapshot: IndustrySnapshot,
    ) -> Dict[str, Any]:
        return {
            "instrument_id": snapshot.instrument_id,
            "symbol": snapshot.symbol,
            "exchange": snapshot.exchange,
            "taxonomy_system": snapshot.taxonomy_system,
            "taxonomy_version": snapshot.taxonomy_version or "",
            "industry_code": snapshot.industry_code,
            "industry_name": snapshot.industry_name,
            "industry_level": snapshot.industry_level,
            "parent_code": snapshot.parent_code,
            "mapping_status": snapshot.mapping_status,
            "effective_date": snapshot.effective_date,
            "source_classification": snapshot.source_classification,
            "source_industry_name": snapshot.source_industry_name,
            "sw_l1_code": snapshot.sw_l1_code,
            "sw_l1_name": snapshot.sw_l1_name,
            "sw_l2_code": snapshot.sw_l2_code,
            "sw_l2_name": snapshot.sw_l2_name,
            "sw_l3_code": snapshot.sw_l3_code,
            "sw_l3_name": snapshot.sw_l3_name,
            "sw_l1_index_code": snapshot.sw_l1_index_code,
            "sw_l2_index_code": snapshot.sw_l2_index_code,
            "sw_l3_index_code": snapshot.sw_l3_index_code,
            "source": snapshot.source,
            "source_mode": snapshot.source_mode,
            "membership_json": snapshot.membership_json or {},
        }

    @classmethod
    def _industry_membership_row_semantic_payload(
        cls,
        row: sqlite3.Row,
    ) -> Dict[str, Any]:
        item = dict(row)
        item["membership_json"] = cls._deserialize_json(
            item.get("membership_json")
        ) or {}
        for key in (
            "data_as_of",
            "ingestion_run_id",
            "created_at",
            "updated_at",
            "row_hash",
            "row_version",
        ):
            item.pop(key, None)
        return item

    @staticmethod
    def _latest_industry_membership_version(
        conn: sqlite3.Connection,
        *,
        instrument_id: str,
        taxonomy_system: str,
        taxonomy_version: str,
    ) -> int:
        rows = conn.execute(
            """
            SELECT business_key_json, row_version
            FROM data_change_log
            WHERE dataset = 'industry_memberships' AND instrument_id = ?
            ORDER BY sequence_id DESC
            LIMIT 50
            """,
            (instrument_id,),
        ).fetchall()
        for row in rows:
            try:
                business_key = json.loads(str(row["business_key_json"]))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if (
                str(business_key.get("taxonomy_system") or "") == taxonomy_system
                and str(business_key.get("taxonomy_version") or "")
                == taxonomy_version
            ):
                return int(row["row_version"] or 0)
        return 0

    @staticmethod
    def _is_share_unit(unit: Optional[str]) -> bool:
        normalized = str(unit or "").strip().lower()
        return normalized in {"share", "shares", "股"}

    @staticmethod
    def _build_financial_source_file_id(
        manifest: FinancialSourceFileManifest,
    ) -> str:
        identity = {
            "source": manifest.source,
            "source_mode": manifest.source_mode,
            "instrument_id": manifest.instrument_id,
            "exchange": manifest.exchange,
            "report_period": manifest.report_period,
            "report_type": manifest.report_type,
            "filing_id": manifest.filing_id,
            "source_url": manifest.source_url,
            "content_hash": manifest.content_hash,
        }
        raw = json.dumps(identity, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _financial_dimensions_hash(dimensions_json: Dict[str, Any]) -> str:
        if not dimensions_json:
            return ""
        raw = json.dumps(dimensions_json, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @classmethod
    def _financial_numeric_fact_row_hash(cls, row: sqlite3.Row) -> str:
        if row["row_hash"]:
            return str(row["row_hash"])
        old_payload = dict(row)
        old_payload["dimensions_json"] = cls._deserialize_json(
            old_payload.get("dimensions_json")
        ) or {}
        old_payload["raw_fact_json"] = cls._deserialize_json(
            old_payload.get("raw_fact_json")
        ) or {}
        for key in (
            "dimensions_hash",
            "ingestion_run_id",
            "created_at",
            "updated_at",
            "row_hash",
            "row_version",
        ):
            old_payload.pop(key, None)
        return cls._semantic_hash(old_payload)

    @classmethod
    def _upsert_financial_numeric_fact_row(
        cls,
        conn: sqlite3.Connection,
        fact: FinancialNumericFactSnapshot,
        *,
        table_name: str,
        ingestion_run_id: Optional[int],
        change_watermark_config: Optional[Dict[str, Any]],
        now: str,
    ) -> Dict[str, int]:
        cls._assert_financial_table_name(
            table_name,
            {
                "financial_numeric_facts",
                "financial_numeric_facts_hot",
                "financial_numeric_facts_history",
            },
        )
        dimensions_json = cls._json_text(fact.dimensions_json)
        raw_fact_json = cls._json_text(fact.raw_fact_json)
        dimensions_hash = cls._financial_dimensions_hash(fact.dimensions_json)
        value_text = fact.value_text
        if value_text is None and fact.fact_value is not None:
            value_text = str(fact.fact_value)
        semantic_payload = asdict(fact)
        semantic_payload["context_id"] = fact.context_id or ""
        semantic_payload["unit"] = fact.unit or ""
        semantic_payload["value_text"] = value_text
        row_hash = cls._semantic_hash(semantic_payload)
        existing = conn.execute(
            f"""
            SELECT * FROM {table_name}
            WHERE source_file_id = ? AND fact_name = ?
              AND context_id = ? AND unit = ? AND dimensions_hash = ?
            """,
            (
                fact.source_file_id,
                fact.fact_name,
                fact.context_id or "",
                fact.unit or "",
                dimensions_hash,
            ),
        ).fetchone()
        old_hash = None
        if existing is not None:
            old_hash = cls._financial_numeric_fact_row_hash(existing)
        row_version = int(existing["row_version"] or 1) + 1 if existing and old_hash != row_hash else 1
        if existing and old_hash == row_hash:
            row_version = int(existing["row_version"] or 1)

        conn.execute(
            f"""
            INSERT INTO {table_name} (
                source_file_id,
                instrument_id,
                symbol,
                exchange,
                report_period,
                report_type,
                statement_family,
                fact_name,
                canonical_fact_name,
                canonical_statement_family,
                canonical_semantic,
                canonical_unit,
                canonical_version,
                taxonomy_namespace,
                context_id,
                unit,
                decimals,
                precision,
                period_start,
                period_end,
                instant,
                currency,
                fact_value,
                value_text,
                dimensions_hash,
                dimensions_json,
                parser_version,
                source,
                source_mode,
                raw_fact_json,
                row_hash,
                row_version,
                ingestion_run_id,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_file_id, fact_name, context_id, unit, dimensions_hash)
            DO UPDATE SET
                instrument_id = excluded.instrument_id,
                symbol = excluded.symbol,
                exchange = excluded.exchange,
                report_period = excluded.report_period,
                report_type = excluded.report_type,
                statement_family = excluded.statement_family,
                canonical_fact_name = excluded.canonical_fact_name,
                canonical_statement_family = excluded.canonical_statement_family,
                canonical_semantic = excluded.canonical_semantic,
                canonical_unit = excluded.canonical_unit,
                canonical_version = excluded.canonical_version,
                taxonomy_namespace = excluded.taxonomy_namespace,
                decimals = excluded.decimals,
                precision = excluded.precision,
                period_start = excluded.period_start,
                period_end = excluded.period_end,
                instant = excluded.instant,
                currency = excluded.currency,
                fact_value = excluded.fact_value,
                value_text = excluded.value_text,
                dimensions_json = excluded.dimensions_json,
                parser_version = excluded.parser_version,
                source = excluded.source,
                source_mode = excluded.source_mode,
                raw_fact_json = excluded.raw_fact_json,
                row_hash = excluded.row_hash,
                row_version = excluded.row_version,
                ingestion_run_id = excluded.ingestion_run_id,
                updated_at = excluded.updated_at
            """,
            (
                fact.source_file_id,
                fact.instrument_id,
                fact.symbol,
                fact.exchange,
                fact.report_period,
                fact.report_type,
                fact.statement_family,
                fact.fact_name,
                fact.canonical_fact_name,
                fact.canonical_statement_family,
                fact.canonical_semantic,
                fact.canonical_unit,
                fact.canonical_version,
                fact.taxonomy_namespace,
                fact.context_id or "",
                fact.unit or "",
                fact.decimals,
                fact.precision,
                fact.period_start,
                fact.period_end,
                fact.instant,
                fact.currency,
                fact.fact_value,
                value_text,
                dimensions_hash,
                dimensions_json,
                fact.parser_version,
                fact.source,
                fact.source_mode,
                raw_fact_json,
                row_hash,
                row_version,
                ingestion_run_id,
                now,
                now,
            ),
        )
        changelog_written = False
        if existing is None or old_hash != row_hash:
            changelog_written = append_change_record(
                conn,
                config=change_watermark_config,
                domain="financial",
                dataset="financial_numeric_facts",
                change_type="update" if existing else "insert",
                business_key={
                    "source_file_id": fact.source_file_id,
                    "instrument_id": fact.instrument_id,
                    "report_period": fact.report_period,
                    "fact_name": fact.fact_name,
                    "canonical_fact_name": fact.canonical_fact_name,
                    "context_id": fact.context_id or "",
                    "unit": fact.unit or "",
                    "dimensions_hash": dimensions_hash,
                },
                instrument_id=fact.instrument_id,
                period=fact.report_period,
                old_hash=old_hash,
                new_hash=row_hash,
                row_version=row_version,
                source=fact.source,
                source_mode=fact.source_mode,
                source_profile=fact.parser_version,
                ingestion_run_id=ingestion_run_id,
                changed_at=now,
            )
        return {
            "inserted": int(existing is None),
            "changed": int(existing is not None and old_hash != row_hash),
            "unchanged": int(existing is not None and old_hash == row_hash),
            "changelog_written": int(changelog_written),
        }

    @classmethod
    def _decode_financial_numeric_fact_row(
        cls,
        row: sqlite3.Row,
    ) -> Dict[str, Any]:
        item = dict(row)
        item.pop("row_hash", None)
        item.pop("row_version", None)
        item["dimensions"] = cls._deserialize_json(item.pop("dimensions_json", None)) or {}
        item["raw_fact"] = cls._deserialize_json(item.pop("raw_fact_json", None)) or {}
        return item

    @classmethod
    def _upsert_financial_core_fact_row(
        cls,
        conn: sqlite3.Connection,
        snapshot: FinancialFactsSnapshot,
        *,
        table_name: str,
        facts_json: str,
        row_hash: str,
        row_version: int,
        ingestion_run_id: Optional[int],
        now: str,
    ) -> None:
        cls._assert_financial_table_name(
            table_name,
            {
                "financial_facts",
                "financial_core_facts_hot",
                "financial_core_facts_history",
            },
        )
        lineage_json = cls._json_text(snapshot.lineage_json)
        data_available_date = snapshot.data_available_date or snapshot.publish_date
        conn.execute(
            f"""
            INSERT INTO {table_name} (
                instrument_id,
                symbol,
                exchange,
                report_period,
                report_type,
                statement_family,
                data_available_date,
                source_file_id,
                filing_id,
                publish_date,
                fiscal_year,
                fiscal_quarter,
                currency,
                schema_version,
                revenue,
                gross_profit,
                operating_profit,
                pre_tax_profit,
                net_income,
                operating_cf,
                total_cf,
                total_assets,
                total_liabilities,
                equity,
                current_assets,
                current_liabilities,
                inventory,
                receivables,
                fixed_assets,
                intangible_assets,
                shares_outstanding,
                source,
                source_mode,
                data_as_of,
                facts_json,
                lineage_json,
                row_hash,
                row_version,
                ingestion_run_id,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_id, report_period)
            DO UPDATE SET
                symbol = excluded.symbol,
                exchange = excluded.exchange,
                report_type = excluded.report_type,
                statement_family = excluded.statement_family,
                data_available_date = excluded.data_available_date,
                source_file_id = excluded.source_file_id,
                filing_id = excluded.filing_id,
                publish_date = excluded.publish_date,
                fiscal_year = excluded.fiscal_year,
                fiscal_quarter = excluded.fiscal_quarter,
                currency = excluded.currency,
                schema_version = excluded.schema_version,
                revenue = excluded.revenue,
                gross_profit = excluded.gross_profit,
                operating_profit = excluded.operating_profit,
                pre_tax_profit = excluded.pre_tax_profit,
                net_income = excluded.net_income,
                operating_cf = excluded.operating_cf,
                total_cf = excluded.total_cf,
                total_assets = excluded.total_assets,
                total_liabilities = excluded.total_liabilities,
                equity = excluded.equity,
                current_assets = excluded.current_assets,
                current_liabilities = excluded.current_liabilities,
                inventory = excluded.inventory,
                receivables = excluded.receivables,
                fixed_assets = excluded.fixed_assets,
                intangible_assets = excluded.intangible_assets,
                shares_outstanding = excluded.shares_outstanding,
                source = excluded.source,
                source_mode = excluded.source_mode,
                data_as_of = excluded.data_as_of,
                facts_json = excluded.facts_json,
                lineage_json = excluded.lineage_json,
                row_hash = excluded.row_hash,
                row_version = excluded.row_version,
                ingestion_run_id = excluded.ingestion_run_id,
                updated_at = excluded.updated_at
            """,
            (
                snapshot.instrument_id,
                snapshot.symbol,
                snapshot.exchange,
                snapshot.report_period,
                snapshot.report_type,
                snapshot.statement_family,
                data_available_date,
                snapshot.source_file_id,
                snapshot.filing_id,
                snapshot.publish_date,
                snapshot.fiscal_year,
                snapshot.fiscal_quarter,
                snapshot.currency,
                snapshot.schema_version,
                snapshot.revenue,
                snapshot.gross_profit,
                snapshot.operating_profit,
                snapshot.pre_tax_profit,
                snapshot.net_income,
                snapshot.operating_cf,
                snapshot.total_cf,
                snapshot.total_assets,
                snapshot.total_liabilities,
                snapshot.equity,
                snapshot.current_assets,
                snapshot.current_liabilities,
                snapshot.inventory,
                snapshot.receivables,
                snapshot.fixed_assets,
                snapshot.intangible_assets,
                snapshot.shares_outstanding,
                snapshot.source,
                snapshot.source_mode,
                now,
                facts_json,
                lineage_json,
                row_hash,
                row_version,
                ingestion_run_id,
                now,
                now,
            ),
        )

    @classmethod
    def _decode_financial_core_fact_row(
        cls,
        row: sqlite3.Row,
    ) -> Dict[str, Any]:
        item = dict(row)
        item.pop("row_hash", None)
        item.pop("row_version", None)
        item["facts"] = cls._deserialize_json(item.pop("facts_json", None)) or {}
        item["lineage"] = cls._deserialize_json(item.pop("lineage_json", None)) or {}
        return item

    @classmethod
    def _upsert_financial_indicator_row(
        cls,
        conn: sqlite3.Connection,
        snapshot: FinancialIndicatorSnapshot,
        *,
        table_name: str,
        indicators_json: str,
        ingestion_run_id: Optional[int],
        now: str,
    ) -> None:
        cls._assert_financial_table_name(
            table_name,
            {
                "financial_indicator_snapshots",
                "financial_indicator_snapshots_hot",
                "financial_indicator_snapshots_history",
            },
        )
        conn.execute(
            f"""
            INSERT INTO {table_name} (
                instrument_id,
                symbol,
                exchange,
                report_period,
                publish_date,
                fiscal_year,
                fiscal_quarter,
                currency,
                schema_version,
                gross_margin,
                operating_margin,
                net_margin,
                roe,
                roa,
                current_ratio,
                quick_ratio,
                asset_liability_ratio,
                revenue_per_share,
                operating_cf_to_revenue,
                operating_cf_to_net_income,
                book_value_per_share,
                source,
                source_mode,
                data_as_of,
                indicators_json,
                ingestion_run_id,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_id, report_period)
            DO UPDATE SET
                symbol = excluded.symbol,
                exchange = excluded.exchange,
                publish_date = excluded.publish_date,
                fiscal_year = excluded.fiscal_year,
                fiscal_quarter = excluded.fiscal_quarter,
                currency = excluded.currency,
                schema_version = excluded.schema_version,
                gross_margin = excluded.gross_margin,
                operating_margin = excluded.operating_margin,
                net_margin = excluded.net_margin,
                roe = excluded.roe,
                roa = excluded.roa,
                current_ratio = excluded.current_ratio,
                quick_ratio = excluded.quick_ratio,
                asset_liability_ratio = excluded.asset_liability_ratio,
                revenue_per_share = excluded.revenue_per_share,
                operating_cf_to_revenue = excluded.operating_cf_to_revenue,
                operating_cf_to_net_income = excluded.operating_cf_to_net_income,
                book_value_per_share = excluded.book_value_per_share,
                source = excluded.source,
                source_mode = excluded.source_mode,
                data_as_of = excluded.data_as_of,
                indicators_json = excluded.indicators_json,
                ingestion_run_id = excluded.ingestion_run_id,
                updated_at = excluded.updated_at
            """,
            (
                snapshot.instrument_id,
                snapshot.symbol,
                snapshot.exchange,
                snapshot.report_period,
                snapshot.publish_date,
                snapshot.fiscal_year,
                snapshot.fiscal_quarter,
                snapshot.currency,
                snapshot.schema_version,
                snapshot.gross_margin,
                snapshot.operating_margin,
                snapshot.net_margin,
                snapshot.roe,
                snapshot.roa,
                snapshot.current_ratio,
                snapshot.quick_ratio,
                snapshot.asset_liability_ratio,
                snapshot.revenue_per_share,
                snapshot.operating_cf_to_revenue,
                snapshot.operating_cf_to_net_income,
                snapshot.book_value_per_share,
                snapshot.source,
                snapshot.source_mode,
                now,
                indicators_json,
                ingestion_run_id,
                now,
                now,
            ),
        )

    @staticmethod
    def _financial_core_fact_fields() -> set[str]:
        return {
            "revenue",
            "gross_profit",
            "operating_profit",
            "pre_tax_profit",
            "net_income",
            "operating_cf",
            "total_cf",
            "total_assets",
            "total_liabilities",
            "equity",
            "current_assets",
            "current_liabilities",
            "inventory",
            "receivables",
            "fixed_assets",
            "intangible_assets",
            "shares_outstanding",
        }

    @staticmethod
    def _append_limited(items: List[Any], item: Any, limit: int) -> None:
        if len(items) < limit:
            items.append(item)

    def _fetch_financial_core_rows_for_gap_detection(
        self,
        *,
        expected_periods: List[str],
        instrument_ids: List[str],
        include_history: bool,
    ) -> List[sqlite3.Row]:
        tables = ["financial_core_facts_hot"]
        if include_history:
            tables.append("financial_core_facts_history")
        select_sql = " UNION ALL ".join(f"SELECT * FROM {table}" for table in tables)
        filters: List[str] = []
        params: List[Any] = []
        if instrument_ids:
            placeholders = ",".join("?" for _ in instrument_ids)
            filters.append(f"instrument_id IN ({placeholders})")
            params.extend(instrument_ids)
        if expected_periods:
            placeholders = ",".join("?" for _ in expected_periods)
            filters.append(f"report_period IN ({placeholders})")
            params.extend(expected_periods)
        where_clause = "" if not filters else "WHERE " + " AND ".join(filters)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            return conn.execute(
                f"""
                SELECT *
                FROM ({select_sql})
                {where_clause}
                ORDER BY report_period DESC, updated_at DESC
                """,
                params,
            ).fetchall()

    def _fetch_financial_manifest_rows_for_gap_detection(
        self,
        *,
        expected_periods: List[str],
        instrument_ids: List[str],
    ) -> List[sqlite3.Row]:
        filters: List[str] = []
        params: List[Any] = []
        if instrument_ids:
            placeholders = ",".join("?" for _ in instrument_ids)
            filters.append(f"instrument_id IN ({placeholders})")
            params.extend(instrument_ids)
        if expected_periods:
            placeholders = ",".join("?" for _ in expected_periods)
            filters.append(f"report_period IN ({placeholders})")
            params.extend(expected_periods)
        where_clause = "" if not filters else "WHERE " + " AND ".join(filters)

        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            return conn.execute(
                f"""
                SELECT instrument_id, report_period, source, source_mode,
                       parser_version, status
                FROM financial_source_files
                {where_clause}
                ORDER BY report_period DESC, updated_at DESC
                """,
                params,
            ).fetchall()

    @classmethod
    def _count_financial_tier_table(
        cls,
        conn: sqlite3.Connection,
        table_name: str,
        *,
        instrument_ids: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        cls._assert_financial_table_name(
            table_name,
            {
                "financial_core_facts_hot",
                "financial_core_facts_history",
                "financial_numeric_facts_hot",
                "financial_numeric_facts_history",
                "financial_indicator_snapshots_hot",
                "financial_indicator_snapshots_history",
            },
        )
        filters: List[str] = []
        params: List[Any] = []
        if instrument_ids:
            placeholders = ",".join("?" for _ in instrument_ids)
            filters.append(f"instrument_id IN ({placeholders})")
            params.extend(instrument_ids)
        where_clause = "" if not filters else "WHERE " + " AND ".join(filters)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS row_count,
                   COUNT(DISTINCT instrument_id) AS instrument_count,
                   COUNT(DISTINCT report_period) AS report_period_count
            FROM {table_name}
            {where_clause}
            """,
            params,
        ).fetchone()
        return {
            "row_count": int(row["row_count"]),
            "instrument_count": int(row["instrument_count"]),
            "report_period_count": int(row["report_period_count"]),
        }

    @staticmethod
    def _assert_financial_table_name(table_name: str, allowed: set[str]) -> None:
        if table_name not in allowed:
            raise ValueError(f"unsupported financial storage table: {table_name}")

    def _financial_hot_quarter_window(self) -> int:
        module_cfg = self.research_config.modules.get("financial_statements", {})
        storage_cfg = module_cfg.get("storage", {}) if isinstance(module_cfg, dict) else {}
        return int(storage_cfg.get("hot_quarter_window", 12))

    @staticmethod
    def _financial_tier_instruments(
        conn: sqlite3.Connection,
        *,
        instrument_id: Optional[str],
    ) -> List[str]:
        if instrument_id:
            return [instrument_id]
        rows = conn.execute(
            """
            SELECT instrument_id FROM financial_core_facts_hot
            UNION
            SELECT instrument_id FROM financial_core_facts_history
            UNION
            SELECT instrument_id FROM financial_facts
            ORDER BY instrument_id
            """
        ).fetchall()
        return [row["instrument_id"] for row in rows]

    @classmethod
    def _move_tier_rows(
        cls,
        conn: sqlite3.Connection,
        source_table: str,
        target_table: str,
        where_clause: str,
        params: tuple[Any, ...],
    ) -> int:
        allowed = {
            "financial_core_facts_hot",
            "financial_core_facts_history",
            "financial_numeric_facts_hot",
            "financial_numeric_facts_history",
            "financial_indicator_snapshots_hot",
            "financial_indicator_snapshots_history",
        }
        cls._assert_financial_table_name(source_table, allowed)
        cls._assert_financial_table_name(target_table, allowed)
        rows_to_move = conn.execute(
            f"SELECT COUNT(*) AS count FROM {source_table} WHERE {where_clause}",
            params,
        ).fetchone()["count"]
        if rows_to_move == 0:
            return 0
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {target_table}
            SELECT *
            FROM {source_table}
            WHERE {where_clause}
            """,
            params,
        )
        conn.execute(f"DELETE FROM {source_table} WHERE {where_clause}", params)
        return int(rows_to_move)

    @staticmethod
    def _count_stale_hot_rows(
        conn: sqlite3.Connection,
        instrument_id: str,
        keep_periods: set[str],
    ) -> int:
        if not keep_periods:
            return int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM financial_core_facts_hot
                    WHERE instrument_id = ?
                    """,
                    (instrument_id,),
                ).fetchone()["count"]
            )
        placeholders = ",".join("?" for _ in keep_periods)
        params: List[Any] = [instrument_id]
        params.extend(sorted(keep_periods))
        return int(
            conn.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM financial_core_facts_hot
                WHERE instrument_id = ? AND report_period NOT IN ({placeholders})
                """,
                params,
            ).fetchone()["count"]
        )

    @staticmethod
    def _count_duplicate_tier_conflicts(conn: sqlite3.Connection) -> Dict[str, int]:
        core = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM financial_core_facts_hot h
            INNER JOIN financial_core_facts_history c
                ON h.instrument_id = c.instrument_id
               AND h.report_period = c.report_period
            """
        ).fetchone()["count"]
        numeric = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM financial_numeric_facts_hot h
            INNER JOIN financial_numeric_facts_history c
                ON h.source_file_id = c.source_file_id
               AND h.fact_name = c.fact_name
               AND h.context_id = c.context_id
               AND h.unit = c.unit
               AND h.dimensions_hash = c.dimensions_hash
            """
        ).fetchone()["count"]
        indicators = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM financial_indicator_snapshots_hot h
            INNER JOIN financial_indicator_snapshots_history c
                ON h.instrument_id = c.instrument_id
               AND h.report_period = c.report_period
            """
        ).fetchone()["count"]
        return {
            "core_facts": int(core),
            "numeric_facts": int(numeric),
            "indicators": int(indicators),
        }

    def _upsert_industry_taxonomy_row(
        self,
        conn: sqlite3.Connection,
        snapshot: IndustryTaxonomySnapshot,
        *,
        ingestion_run_id: Optional[int],
        now: str,
    ) -> Dict[str, int]:
        semantic_payload = self._industry_taxonomy_semantic_payload(snapshot)
        row_hash = self._semantic_hash(semantic_payload)
        existing = conn.execute(
            """
            SELECT * FROM industry_taxonomy
            WHERE taxonomy_system = ?
              AND taxonomy_version = ?
              AND industry_code = ?
            """,
            (
                snapshot.taxonomy_system,
                snapshot.taxonomy_version or "",
                snapshot.industry_code,
            ),
        ).fetchone()
        old_hash = None
        if existing is not None:
            old_hash = existing["row_hash"] or self._semantic_hash(
                self._industry_taxonomy_row_semantic_payload(existing)
            )
        changed = existing is None or old_hash != row_hash
        row_version = (
            1
            if existing is None
            else int(existing["row_version"] or 1) + int(changed)
        )
        conn.execute(
            """
            INSERT INTO industry_taxonomy (
                taxonomy_system,
                taxonomy_version,
                industry_code,
                industry_name,
                industry_level,
                parent_code,
                sw_index_code,
                aliases_json,
                source_classification,
                source,
                source_mode,
                is_active,
                row_hash,
                row_version,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(taxonomy_system, taxonomy_version, industry_code)
            DO UPDATE SET
                industry_name = excluded.industry_name,
                industry_level = excluded.industry_level,
                parent_code = excluded.parent_code,
                sw_index_code = excluded.sw_index_code,
                aliases_json = excluded.aliases_json,
                source_classification = excluded.source_classification,
                source = excluded.source,
                source_mode = excluded.source_mode,
                is_active = excluded.is_active,
                row_hash = excluded.row_hash,
                row_version = excluded.row_version,
                updated_at = excluded.updated_at
            """,
            (
                snapshot.taxonomy_system,
                snapshot.taxonomy_version or "",
                snapshot.industry_code,
                snapshot.industry_name,
                snapshot.industry_level,
                snapshot.parent_code,
                snapshot.sw_index_code,
                json.dumps(snapshot.aliases_json, ensure_ascii=False, sort_keys=True),
                snapshot.source_classification,
                snapshot.source,
                snapshot.source_mode,
                1,
                row_hash,
                row_version,
                now,
                now,
            ),
        )
        changelog_written = False
        if changed:
            changelog_written = append_change_record(
                conn,
                config=self.change_watermark_config,
                domain="industry",
                dataset="industry_taxonomy",
                change_type="update" if existing else "insert",
                business_key={
                    "taxonomy_system": snapshot.taxonomy_system,
                    "taxonomy_version": snapshot.taxonomy_version or "",
                    "industry_code": snapshot.industry_code,
                },
                series_id=snapshot.industry_code,
                old_hash=old_hash,
                new_hash=row_hash,
                row_version=row_version,
                source=snapshot.source,
                source_mode=snapshot.source_mode,
                source_profile=snapshot.source_classification,
                ingestion_run_id=ingestion_run_id,
                changed_at=now,
            )
        return {
            "inserted": int(existing is None),
            "changed": int(existing is not None and changed),
            "unchanged": int(existing is not None and not changed),
            "changelog_written": int(changelog_written),
        }

    def _attach_quotes_db(self, conn: sqlite3.Connection) -> None:
        if not os.path.exists(self.quotes_db_path):
            db_logger.warning(
                "[ResearchStorage] Quotes database not found for attach: "
                f"{self.quotes_db_path}"
            )
            return
        conn.execute(
            f"ATTACH DATABASE ? AS {self.quotes_db_alias}",
            (self.quotes_db_path,),
        )

    @staticmethod
    def _sqlite_object_type(conn: sqlite3.Connection, object_name: str) -> Optional[str]:
        row = conn.execute(
            """
            SELECT type
            FROM sqlite_master
            WHERE name = ?
            ORDER BY type
            LIMIT 1
            """,
            (object_name,),
        ).fetchone()
        return None if row is None else str(row["type"])

    @staticmethod
    def _ensure_column(
        conn: sqlite3.Connection,
        table_name: str,
        column_name: str,
        column_sql: str,
    ) -> None:
        columns = {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if (
            column_name not in columns
            and ResearchStorageManager._sqlite_object_type(conn, table_name) != "view"
        ):
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")

    @staticmethod
    def _rename_column_if_needed(
        conn: sqlite3.Connection,
        table_name: str,
        old_column_name: str,
        new_column_name: str,
    ) -> None:
        columns = {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if old_column_name in columns and new_column_name not in columns:
            conn.execute(
                f"ALTER TABLE {table_name} RENAME COLUMN "
                f"{old_column_name} TO {new_column_name}"
            )

    @classmethod
    def _migrate_tables(cls, conn: sqlite3.Connection) -> None:
        """Apply lightweight additive migrations for research shadow tables."""
        cls._ensure_column(
            conn,
            "shareholder_snapshots",
            "row_hash",
            "row_hash TEXT",
        )
        cls._ensure_column(
            conn,
            "shareholder_snapshots",
            "row_version",
            "row_version INTEGER NOT NULL DEFAULT 1",
        )
        for table_name in (
            "financial_facts",
            "financial_core_facts_hot",
            "financial_core_facts_history",
            "financial_numeric_facts",
            "financial_numeric_facts_hot",
            "financial_numeric_facts_history",
        ):
            cls._ensure_column(conn, table_name, "row_hash", "row_hash TEXT")
            cls._ensure_column(
                conn,
                table_name,
                "row_version",
                "row_version INTEGER NOT NULL DEFAULT 1",
            )
        cls._ensure_column(
            conn,
            "industry_taxonomy",
            "taxonomy_version",
            "taxonomy_version TEXT NOT NULL DEFAULT ''",
        )
        cls._ensure_column(conn, "industry_taxonomy", "row_hash", "row_hash TEXT")
        cls._ensure_column(
            conn,
            "industry_taxonomy",
            "row_version",
            "row_version INTEGER NOT NULL DEFAULT 1",
        )
        cls._ensure_column(
            conn,
            "industry_memberships",
            "taxonomy_version",
            "taxonomy_version TEXT NOT NULL DEFAULT ''",
        )
        cls._ensure_column(
            conn,
            "industry_memberships",
            "mapping_status",
            "mapping_status TEXT NOT NULL DEFAULT 'reference_only'",
        )
        cls._ensure_column(
            conn,
            "industry_memberships",
            "effective_date",
            "effective_date TEXT",
        )
        cls._ensure_column(
            conn,
            "industry_memberships",
            "sw_l1_code",
            "sw_l1_code TEXT",
        )
        cls._ensure_column(
            conn,
            "industry_memberships",
            "sw_l1_name",
            "sw_l1_name TEXT",
        )
        cls._ensure_column(
            conn,
            "industry_memberships",
            "sw_l2_code",
            "sw_l2_code TEXT",
        )
        cls._ensure_column(
            conn,
            "industry_memberships",
            "sw_l2_name",
            "sw_l2_name TEXT",
        )
        cls._ensure_column(
            conn,
            "industry_memberships",
            "sw_l3_code",
            "sw_l3_code TEXT",
        )
        cls._ensure_column(
            conn,
            "industry_memberships",
            "sw_l3_name",
            "sw_l3_name TEXT",
        )
        cls._ensure_column(
            conn,
            "industry_memberships",
            "sw_l1_index_code",
            "sw_l1_index_code TEXT",
        )
        cls._ensure_column(
            conn,
            "industry_memberships",
            "sw_l2_index_code",
            "sw_l2_index_code TEXT",
        )
        cls._ensure_column(
            conn,
            "industry_memberships",
            "sw_l3_index_code",
            "sw_l3_index_code TEXT",
        )
        cls._ensure_column(conn, "industry_memberships", "row_hash", "row_hash TEXT")
        cls._ensure_column(
            conn,
            "industry_memberships",
            "row_version",
            "row_version INTEGER NOT NULL DEFAULT 1",
        )
        for table_name in (
            "risk_free_rate_series",
            "risk_free_rate_observations",
            "risk_snapshots",
            "technical_indicator_latest",
            "valuation_inputs",
            "valuation_history",
        ):
            cls._ensure_column(conn, table_name, "row_hash", "row_hash TEXT")
            cls._ensure_column(
                conn,
                table_name,
                "row_version",
                "row_version INTEGER NOT NULL DEFAULT 1",
            )
        cls._ensure_column(
            conn,
            "industry_taxonomy",
            "sw_index_code",
            "sw_index_code TEXT",
        )
        cls._ensure_column(
            conn,
            "industry_taxonomy",
            "aliases_json",
            "aliases_json TEXT NOT NULL DEFAULT '{}'",
        )
        cls._rename_column_if_needed(
            conn,
            "industry_index_analysis_daily",
            "bargain_amount",
            "bargain_volume",
        )
        cls._ensure_column(conn, "financial_facts", "report_type", "report_type TEXT")
        cls._ensure_column(
            conn,
            "financial_source_files",
            "source_tier",
            "source_tier TEXT",
        )
        cls._ensure_column(
            conn,
            "financial_source_files",
            "supersedes_source_file_id",
            "supersedes_source_file_id TEXT",
        )
        cls._ensure_column(
            conn,
            "financial_facts",
            "statement_family",
            "statement_family TEXT",
        )
        cls._ensure_column(
            conn,
            "financial_facts",
            "data_available_date",
            "data_available_date TEXT",
        )
        cls._ensure_column(
            conn,
            "financial_facts",
            "source_file_id",
            "source_file_id TEXT",
        )
        cls._ensure_column(conn, "financial_facts", "filing_id", "filing_id TEXT")
        cls._ensure_column(
            conn,
            "financial_facts",
            "lineage_json",
            "lineage_json TEXT NOT NULL DEFAULT '{}'",
        )
        for table_name in (
            "financial_numeric_facts",
            "financial_numeric_facts_hot",
            "financial_numeric_facts_history",
        ):
            cls._ensure_column(
                conn,
                table_name,
                "canonical_fact_name",
                "canonical_fact_name TEXT",
            )
            cls._ensure_column(
                conn,
                table_name,
                "canonical_statement_family",
                "canonical_statement_family TEXT",
            )
            cls._ensure_column(
                conn,
                table_name,
                "canonical_semantic",
                "canonical_semantic TEXT",
            )
            cls._ensure_column(conn, table_name, "canonical_unit", "canonical_unit TEXT")
            cls._ensure_column(
                conn,
                table_name,
                "canonical_version",
                "canonical_version TEXT",
            )
        if cls._sqlite_object_type(conn, "financial_numeric_facts") == "table":
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_financial_numeric_facts_canonical
                ON financial_numeric_facts(instrument_id, report_period, canonical_fact_name)
                """
            )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_financial_numeric_facts_hot_canonical
            ON financial_numeric_facts_hot(instrument_id, report_period, canonical_fact_name)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_financial_numeric_facts_history_canonical
            ON financial_numeric_facts_history(instrument_id, report_period, canonical_fact_name)
            """
        )
        for column_name in (
            "pe_static",
            "pe_ttm",
            "pe_forward",
            "pb_mrq",
            "ps_static",
            "ps_ttm",
            "ps_forward",
            "float_market_cap",
        ):
            cls._ensure_column(conn, "valuation_history", column_name, f"{column_name} REAL")
        for table_name in (
            "company_business_segments",
            "company_operating_facts",
            "company_value_chain_roles",
            "company_commodity_exposures",
        ):
            cls._ensure_column(
                conn,
                table_name,
                "business_regime_id",
                "business_regime_id TEXT",
            )
            cls._ensure_column(
                conn,
                table_name,
                "knowledge_from",
                "knowledge_from TEXT",
            )
            cls._ensure_column(
                conn,
                table_name,
                "knowledge_to",
                "knowledge_to TEXT",
            )
        for column_name in (
            "segment_cost",
            "cost_share",
            "profit_share",
            "gross_margin",
        ):
            cls._ensure_column(
                conn,
                "company_business_segments",
                column_name,
                f"{column_name} REAL",
            )
        for table_name in (
            "company_business_segments",
            "company_operating_facts",
            "company_value_chain_roles",
        ):
            cls._ensure_column(
                conn,
                table_name,
                "supersedes_record_id",
                "supersedes_record_id TEXT",
            )
        cls._ensure_column(
            conn,
            "company_commodity_exposures",
            "supersedes_exposure_id",
            "supersedes_exposure_id TEXT",
        )
        exposure_columns = {
            str(row["name"]): row
            for row in conn.execute(
                "PRAGMA table_info(company_commodity_exposures)"
            ).fetchall()
        }
        lag_column = exposure_columns.get("lag_days")
        if lag_column is not None and int(lag_column["notnull"] or 0) == 1:
            conn.execute(
                "ALTER TABLE company_commodity_exposures "
                "RENAME COLUMN lag_days TO lag_days_legacy_not_null"
            )
            conn.execute(
                "ALTER TABLE company_commodity_exposures ADD COLUMN lag_days INTEGER"
            )
            conn.execute(
                "UPDATE company_commodity_exposures "
                "SET lag_days = lag_days_legacy_not_null"
            )
        for column_name, definition in (
            ("fact_ids_json", "fact_ids_json TEXT NOT NULL DEFAULT '[]'"),
            ("mapping_ids_json", "mapping_ids_json TEXT NOT NULL DEFAULT '[]'"),
            ("assumption_ids_json", "assumption_ids_json TEXT NOT NULL DEFAULT '[]'"),
            ("direction_rule_id", "direction_rule_id TEXT"),
            ("build_policy_version", "build_policy_version TEXT"),
            ("build_policy_hash", "build_policy_hash TEXT"),
            ("component_lineage_hash", "component_lineage_hash TEXT"),
            ("legacy_compatibility_status", "legacy_compatibility_status TEXT"),
        ):
            cls._ensure_column(
                conn,
                "company_commodity_exposures",
                column_name,
                definition,
            )
        conn.execute(
            """
            CREATE VIEW IF NOT EXISTS company_commodity_exposures_legacy_compatibility AS
            SELECT * FROM company_commodity_exposures
            WHERE component_lineage_hash IS NULL
               OR json_array_length(fact_ids_json) = 0
               OR json_array_length(mapping_ids_json) = 0
            """
        )
        for table_name in (
            "company_business_segments",
            "company_operating_facts",
            "company_value_chain_roles",
            "company_commodity_exposures",
        ):
            conn.execute(
                f"""
                UPDATE {table_name}
                SET knowledge_from = data_available_date
                WHERE knowledge_from IS NULL
                  AND data_available_date IS NOT NULL
                """
            )

    @staticmethod
    def _create_tables(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                job_name TEXT NOT NULL,
                market TEXT,
                source TEXT,
                mode TEXT,
                status TEXT NOT NULL,
                shadow_mode INTEGER NOT NULL DEFAULT 1,
                started_at TEXT,
                completed_at TEXT,
                rows_written INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                metadata_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_ingestion_runs_domain_status
            ON ingestion_runs(domain, status);

            CREATE INDEX IF NOT EXISTS idx_ingestion_runs_job_name
            ON ingestion_runs(job_name, started_at);

            CREATE TABLE IF NOT EXISTS business_profile_semantic_runs (
                run_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                source_document_id TEXT NOT NULL,
                field_family TEXT NOT NULL,
                status TEXT NOT NULL,
                bundle_hash TEXT NOT NULL,
                fact_catalog_version TEXT,
                product_catalog_version TEXT,
                evidence_count INTEGER NOT NULL DEFAULT 0,
                fact_count INTEGER NOT NULL DEFAULT 0,
                activity_count INTEGER NOT NULL DEFAULT 0,
                relationship_count INTEGER NOT NULL DEFAULT 0,
                error_code TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                started_at TEXT NOT NULL,
                completed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_business_profile_semantic_runs_scope
            ON business_profile_semantic_runs(
                instrument_id, source_document_id, field_family, status, updated_at
            );

            CREATE TABLE IF NOT EXISTS business_profile_exceptions (
                exception_id TEXT PRIMARY KEY,
                target_type TEXT NOT NULL,
                target_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                field_family TEXT NOT NULL,
                tier TEXT NOT NULL,
                reason_codes_json TEXT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                next_retry_at TEXT,
                gate_signature TEXT NOT NULL,
                gate_manifest_hash TEXT NOT NULL,
                evidence_references_json TEXT NOT NULL DEFAULT '[]',
                ranked_choices_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'open',
                resolved_at TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(target_type, target_id, gate_signature)
            );

            CREATE INDEX IF NOT EXISTS idx_business_profile_exceptions_queue
            ON business_profile_exceptions(
                status, tier, field_family, next_retry_at, updated_at
            );

            CREATE INDEX IF NOT EXISTS idx_business_profile_exceptions_target
            ON business_profile_exceptions(instrument_id, target_type, target_id);

            CREATE TABLE IF NOT EXISTS company_profiles (
                instrument_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                company_name TEXT NOT NULL,
                short_name TEXT NOT NULL,
                exchange TEXT NOT NULL,
                market TEXT,
                listed_date TEXT,
                industry_raw TEXT,
                sector_raw TEXT,
                status TEXT,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                profile_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_company_profiles_exchange_source
            ON company_profiles(exchange, source, updated_at);

            CREATE TABLE IF NOT EXISTS business_profile_evidence (
                evidence_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                source_document_id TEXT NOT NULL,
                source_institution TEXT,
                source_tier TEXT NOT NULL,
                document_type TEXT,
                title TEXT,
                source_url TEXT,
                document_hash TEXT NOT NULL,
                report_period TEXT,
                publish_date TEXT,
                data_available_date TEXT NOT NULL,
                availability_quality TEXT NOT NULL,
                page_number INTEGER,
                table_name TEXT,
                section_path TEXT,
                evidence_text_hash TEXT NOT NULL,
                extraction_method TEXT NOT NULL,
                parser_version TEXT,
                ocr_status TEXT,
                confidence REAL NOT NULL,
                review_status TEXT NOT NULL,
                reviewed_by TEXT,
                reviewed_at TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_business_profile_evidence_instrument_period
            ON business_profile_evidence(instrument_id, report_period, data_available_date);
            CREATE INDEX IF NOT EXISTS idx_business_profile_evidence_review
            ON business_profile_evidence(review_status, updated_at);
            CREATE INDEX IF NOT EXISTS idx_business_profile_evidence_asof_v2
            ON business_profile_evidence(
                instrument_id, review_status, data_available_date, evidence_id
            );

            CREATE TABLE IF NOT EXISTS company_business_profile_events (
                event_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_date TEXT,
                event_date_quality TEXT,
                prior_regime_id TEXT,
                resulting_regime_id TEXT,
                materiality TEXT,
                description TEXT,
                evidence_id TEXT NOT NULL,
                data_available_date TEXT NOT NULL,
                confidence REAL NOT NULL,
                review_status TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                lineage_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (evidence_id) REFERENCES business_profile_evidence(evidence_id)
            );

            CREATE INDEX IF NOT EXISTS idx_company_business_profile_events_asof
            ON company_business_profile_events(
                instrument_id, data_available_date, event_date, review_status
            );
            CREATE INDEX IF NOT EXISTS idx_company_business_profile_events_review
            ON company_business_profile_events(review_status, materiality, updated_at);
            CREATE INDEX IF NOT EXISTS idx_company_business_profile_events_identity_v2
            ON company_business_profile_events(
                instrument_id, review_status, data_available_date, event_type, event_date
            );

            CREATE TABLE IF NOT EXISTS company_business_profile_regimes (
                regime_id TEXT PRIMARY KEY,
                regime_key TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                regime_name TEXT NOT NULL,
                regime_type TEXT NOT NULL,
                valid_from TEXT,
                valid_to TEXT,
                knowledge_from TEXT NOT NULL,
                knowledge_to TEXT,
                trigger_event_id TEXT,
                evidence_id TEXT NOT NULL,
                data_available_date TEXT NOT NULL,
                confidence REAL NOT NULL,
                review_status TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                lineage_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (evidence_id) REFERENCES business_profile_evidence(evidence_id)
            );

            CREATE INDEX IF NOT EXISTS idx_company_business_profile_regimes_asof
            ON company_business_profile_regimes(
                instrument_id, knowledge_from, knowledge_to, valid_from, valid_to,
                review_status
            );
            CREATE INDEX IF NOT EXISTS idx_company_business_profile_regimes_key
            ON company_business_profile_regimes(instrument_id, regime_key, version);
            CREATE INDEX IF NOT EXISTS idx_company_business_profile_regimes_identity_v2
            ON company_business_profile_regimes(
                instrument_id, review_status, regime_key, knowledge_from, knowledge_to,
                valid_from, valid_to, version
            );

            CREATE TABLE IF NOT EXISTS company_business_segments (
                record_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                report_period TEXT NOT NULL,
                segment_id TEXT NOT NULL,
                segment_name_raw TEXT NOT NULL,
                segment_name_normalized TEXT,
                segment_type TEXT NOT NULL,
                revenue REAL,
                revenue_share REAL,
                segment_cost REAL,
                cost_share REAL,
                segment_profit REAL,
                profit_share REAL,
                gross_margin REAL,
                segment_assets REAL,
                currency TEXT,
                consolidation_scope TEXT,
                geography TEXT,
                source_document_id TEXT,
                evidence_id TEXT NOT NULL,
                data_available_date TEXT NOT NULL,
                extraction_method TEXT,
                confidence REAL NOT NULL,
                review_status TEXT NOT NULL,
                valid_from TEXT,
                valid_to TEXT,
                business_regime_id TEXT,
                knowledge_from TEXT,
                knowledge_to TEXT,
                supersedes_record_id TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                lineage_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (evidence_id) REFERENCES business_profile_evidence(evidence_id)
            );

            CREATE INDEX IF NOT EXISTS idx_company_business_segments_asof
            ON company_business_segments(instrument_id, data_available_date, review_status);
            CREATE INDEX IF NOT EXISTS idx_company_business_segments_period
            ON company_business_segments(instrument_id, report_period, segment_type);
            CREATE INDEX IF NOT EXISTS idx_company_business_segments_identity_v2
            ON company_business_segments(
                instrument_id, review_status, segment_id, segment_type,
                consolidation_scope, report_period, knowledge_from, knowledge_to, version
            );
            CREATE INDEX IF NOT EXISTS idx_company_business_segments_supersession_v2
            ON company_business_segments(supersedes_record_id, review_status);

            CREATE TABLE IF NOT EXISTS company_operating_facts (
                record_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                report_period TEXT NOT NULL,
                segment_id TEXT,
                project_id TEXT,
                fact_type TEXT NOT NULL,
                value_raw REAL,
                unit_raw TEXT,
                value_normalized REAL,
                unit_normalized TEXT,
                fact_scope TEXT,
                currency TEXT,
                equity_basis TEXT,
                evidence_id TEXT NOT NULL,
                data_available_date TEXT NOT NULL,
                confidence REAL NOT NULL,
                review_status TEXT NOT NULL,
                valid_from TEXT,
                valid_to TEXT,
                business_regime_id TEXT,
                knowledge_from TEXT,
                knowledge_to TEXT,
                supersedes_record_id TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                lineage_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (evidence_id) REFERENCES business_profile_evidence(evidence_id)
            );

            CREATE INDEX IF NOT EXISTS idx_company_operating_facts_asof
            ON company_operating_facts(instrument_id, data_available_date, review_status);
            CREATE INDEX IF NOT EXISTS idx_company_operating_facts_type
            ON company_operating_facts(instrument_id, report_period, fact_type);
            CREATE INDEX IF NOT EXISTS idx_company_operating_facts_identity_v2
            ON company_operating_facts(
                instrument_id, review_status, fact_type, segment_id, project_id,
                fact_scope, unit_normalized, report_period, knowledge_from,
                knowledge_to, version
            );
            CREATE INDEX IF NOT EXISTS idx_company_operating_facts_supersession_v2
            ON company_operating_facts(supersedes_record_id, review_status);

            CREATE TABLE IF NOT EXISTS company_value_chain_roles (
                record_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                report_period TEXT NOT NULL,
                segment_id TEXT,
                role TEXT NOT NULL,
                materiality TEXT,
                revenue_share REAL,
                mapping_basis TEXT NOT NULL,
                evidence_id TEXT NOT NULL,
                data_available_date TEXT NOT NULL,
                confidence REAL NOT NULL,
                review_status TEXT NOT NULL,
                valid_from TEXT,
                valid_to TEXT,
                business_regime_id TEXT,
                knowledge_from TEXT,
                knowledge_to TEXT,
                supersedes_record_id TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                lineage_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (evidence_id) REFERENCES business_profile_evidence(evidence_id)
            );

            CREATE INDEX IF NOT EXISTS idx_company_value_chain_roles_asof
            ON company_value_chain_roles(instrument_id, data_available_date, review_status);
            CREATE INDEX IF NOT EXISTS idx_company_value_chain_roles_identity_v2
            ON company_value_chain_roles(
                instrument_id, review_status, segment_id, role, knowledge_from,
                knowledge_to, valid_from, valid_to, version
            );
            CREATE INDEX IF NOT EXISTS idx_company_value_chain_roles_supersession_v2
            ON company_value_chain_roles(supersedes_record_id, review_status);

            CREATE TABLE IF NOT EXISTS company_business_activities (
                activity_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                report_period TEXT NOT NULL,
                subject_scope TEXT NOT NULL,
                action TEXT NOT NULL,
                object_type TEXT NOT NULL,
                object_raw TEXT NOT NULL,
                object_id TEXT,
                segment_id TEXT,
                geography TEXT,
                value REAL,
                unit TEXT,
                share REAL,
                evidence_id TEXT NOT NULL,
                run_id TEXT,
                data_available_date TEXT NOT NULL,
                extraction_method TEXT NOT NULL,
                confidence REAL NOT NULL,
                review_status TEXT NOT NULL,
                valid_from TEXT,
                valid_to TEXT,
                business_regime_id TEXT,
                knowledge_from TEXT NOT NULL,
                knowledge_to TEXT,
                supersedes_activity_id TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                lineage_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (evidence_id) REFERENCES business_profile_evidence(evidence_id)
            );

            CREATE INDEX IF NOT EXISTS idx_company_business_activities_asof
            ON company_business_activities(
                instrument_id, review_status, report_period, knowledge_from, knowledge_to
            );
            CREATE INDEX IF NOT EXISTS idx_company_business_activities_identity
            ON company_business_activities(
                instrument_id, action, object_type, object_id, segment_id, version
            );
            CREATE INDEX IF NOT EXISTS idx_company_business_activities_supersession
            ON company_business_activities(supersedes_activity_id, review_status);

            CREATE TABLE IF NOT EXISTS company_supply_chain_relationships (
                relationship_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                report_period TEXT NOT NULL,
                relationship_type TEXT NOT NULL,
                direction TEXT NOT NULL,
                counterparty_name_raw TEXT,
                counterparty_name_normalized TEXT,
                counterparty_entity_id TEXT,
                resolution_basis TEXT,
                anonymous INTEGER NOT NULL DEFAULT 0,
                scope_type TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                object_raw TEXT,
                object_id TEXT,
                disclosed_value REAL,
                disclosed_unit TEXT,
                disclosed_share REAL,
                evidence_id TEXT NOT NULL,
                run_id TEXT,
                data_available_date TEXT NOT NULL,
                confidence REAL NOT NULL,
                review_status TEXT NOT NULL,
                valid_from TEXT,
                valid_to TEXT,
                business_regime_id TEXT,
                knowledge_from TEXT NOT NULL,
                knowledge_to TEXT,
                supersedes_relationship_id TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                lineage_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (evidence_id) REFERENCES business_profile_evidence(evidence_id)
            );

            CREATE INDEX IF NOT EXISTS idx_company_supply_chain_relationships_asof
            ON company_supply_chain_relationships(
                instrument_id, review_status, valid_from, valid_to,
                knowledge_from, knowledge_to
            );
            CREATE INDEX IF NOT EXISTS idx_company_supply_chain_relationships_identity
            ON company_supply_chain_relationships(
                instrument_id, relationship_type, counterparty_name_normalized,
                scope_id, version
            );
            CREATE INDEX IF NOT EXISTS idx_company_supply_chain_relationships_entity
            ON company_supply_chain_relationships(counterparty_entity_id, review_status);
            CREATE INDEX IF NOT EXISTS idx_company_supply_chain_relationships_supersession
            ON company_supply_chain_relationships(
                supersedes_relationship_id, review_status
            );

            CREATE TABLE IF NOT EXISTS company_commodity_exposures (
                exposure_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                report_period TEXT NOT NULL,
                scope_type TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                commodity_id TEXT NOT NULL,
                exposure_role TEXT NOT NULL,
                direction TEXT NOT NULL,
                materiality TEXT,
                mapping_basis TEXT NOT NULL,
                price_series_id TEXT,
                spread_definition_id TEXT,
                lag_days INTEGER,
                pass_through_score REAL,
                hedge_adjustment REAL,
                evidence_id TEXT NOT NULL,
                data_available_date TEXT NOT NULL,
                confidence REAL NOT NULL,
                review_status TEXT NOT NULL,
                effective_from TEXT,
                effective_to TEXT,
                business_regime_id TEXT,
                knowledge_from TEXT,
                knowledge_to TEXT,
                fact_ids_json TEXT NOT NULL DEFAULT '[]',
                mapping_ids_json TEXT NOT NULL DEFAULT '[]',
                assumption_ids_json TEXT NOT NULL DEFAULT '[]',
                direction_rule_id TEXT,
                build_policy_version TEXT,
                build_policy_hash TEXT,
                component_lineage_hash TEXT,
                legacy_compatibility_status TEXT,
                supersedes_exposure_id TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                lineage_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (evidence_id) REFERENCES business_profile_evidence(evidence_id)
            );

            CREATE INDEX IF NOT EXISTS idx_company_commodity_exposures_asof
            ON company_commodity_exposures(instrument_id, data_available_date, review_status);
            CREATE INDEX IF NOT EXISTS idx_company_commodity_exposures_commodity
            ON company_commodity_exposures(commodity_id, exposure_role, review_status);
            CREATE INDEX IF NOT EXISTS idx_company_commodity_exposures_identity_v2
            ON company_commodity_exposures(
                instrument_id, review_status, scope_type, scope_id, commodity_id,
                exposure_role, knowledge_from, knowledge_to, effective_from,
                effective_to, version
            );
            CREATE INDEX IF NOT EXISTS idx_company_commodity_exposures_supersession_v2
            ON company_commodity_exposures(supersedes_exposure_id, review_status);

            CREATE TABLE IF NOT EXISTS company_commodity_exposure_facts (
                fact_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                report_period TEXT NOT NULL,
                activity_id TEXT,
                segment_id TEXT,
                exposure_fact_type TEXT NOT NULL,
                object_raw TEXT NOT NULL,
                product_id TEXT,
                value_raw REAL,
                unit_raw TEXT,
                value_normalized REAL,
                unit_normalized TEXT,
                share REAL,
                fact_scope TEXT NOT NULL,
                evidence_id TEXT NOT NULL,
                run_id TEXT,
                data_available_date TEXT NOT NULL,
                confidence REAL NOT NULL,
                review_status TEXT NOT NULL,
                valid_from TEXT,
                valid_to TEXT,
                business_regime_id TEXT,
                knowledge_from TEXT NOT NULL,
                knowledge_to TEXT,
                supersedes_fact_id TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                lineage_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (evidence_id) REFERENCES business_profile_evidence(evidence_id)
            );

            CREATE INDEX IF NOT EXISTS idx_company_commodity_exposure_facts_asof
            ON company_commodity_exposure_facts(
                instrument_id, review_status, report_period, knowledge_from, knowledge_to
            );
            CREATE INDEX IF NOT EXISTS idx_company_commodity_exposure_facts_identity
            ON company_commodity_exposure_facts(
                instrument_id, activity_id, exposure_fact_type, product_id, segment_id,
                fact_scope, version
            );
            CREATE INDEX IF NOT EXISTS idx_company_commodity_exposure_facts_supersession
            ON company_commodity_exposure_facts(supersedes_fact_id, review_status);

            CREATE TABLE IF NOT EXISTS company_commodity_exposure_assumptions (
                assumption_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                scope_type TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                assumption_type TEXT NOT NULL,
                assumption_value REAL NOT NULL,
                unit TEXT NOT NULL,
                method TEXT NOT NULL,
                sample_start TEXT,
                sample_end TEXT,
                evidence_id TEXT,
                run_id TEXT,
                data_available_date TEXT NOT NULL,
                confidence REAL NOT NULL,
                review_status TEXT NOT NULL,
                effective_from TEXT,
                effective_to TEXT,
                knowledge_from TEXT NOT NULL,
                knowledge_to TEXT,
                supersedes_assumption_id TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                lineage_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (evidence_id) REFERENCES business_profile_evidence(evidence_id)
            );

            CREATE INDEX IF NOT EXISTS idx_company_commodity_exposure_assumptions_asof
            ON company_commodity_exposure_assumptions(
                instrument_id, review_status, effective_from, effective_to,
                knowledge_from, knowledge_to
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_company_commodity_exposure_assumptions_identity
            ON company_commodity_exposure_assumptions(
                instrument_id, scope_type, scope_id, assumption_type, version
            );
            CREATE INDEX IF NOT EXISTS idx_company_commodity_exposure_assumptions_supersession
            ON company_commodity_exposure_assumptions(
                supersedes_assumption_id, review_status
            );

            CREATE TABLE IF NOT EXISTS business_profile_review_audit (
                audit_id TEXT PRIMARY KEY,
                operation_id TEXT NOT NULL,
                record_type TEXT NOT NULL,
                record_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                decision TEXT NOT NULL,
                prior_status TEXT NOT NULL,
                new_status TEXT NOT NULL,
                prior_version INTEGER,
                new_version INTEGER,
                prior_updated_at TEXT NOT NULL,
                new_updated_at TEXT NOT NULL,
                record_lineage_hash TEXT,
                reviewer TEXT NOT NULL,
                reason TEXT NOT NULL,
                evidence_references_json TEXT NOT NULL DEFAULT '[]',
                replacement_record_id TEXT,
                prior_audit_hash TEXT,
                audit_hash TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                reviewed_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_business_profile_review_audit_record
            ON business_profile_review_audit(record_type, record_id, reviewed_at);
            CREATE INDEX IF NOT EXISTS idx_business_profile_review_audit_instrument
            ON business_profile_review_audit(instrument_id, reviewed_at);

            CREATE TRIGGER IF NOT EXISTS business_profile_review_audit_no_update
            BEFORE UPDATE ON business_profile_review_audit
            BEGIN
                SELECT RAISE(ABORT, 'business_profile_review_audit is immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS business_profile_review_audit_no_delete
            BEFORE DELETE ON business_profile_review_audit
            BEGIN
                SELECT RAISE(ABORT, 'business_profile_review_audit is immutable');
            END;

            CREATE TABLE IF NOT EXISTS financial_summaries (
                instrument_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                report_date TEXT,
                pub_date TEXT,
                fiscal_year INTEGER,
                fiscal_quarter INTEGER,
                currency TEXT NOT NULL DEFAULT 'CNY',
                schema_version TEXT NOT NULL,
                roe REAL,
                gross_margin REAL,
                net_margin REAL,
                current_ratio REAL,
                quick_ratio REAL,
                liability_to_asset REAL,
                yoy_asset REAL,
                yoy_equity REAL,
                yoy_net_profit REAL,
                cfo_to_revenue REAL,
                cfo_to_net_profit REAL,
                asset_turnover REAL,
                eps REAL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_summaries_exchange_source
            ON financial_summaries(exchange, source, updated_at);

            CREATE TABLE IF NOT EXISTS shareholder_snapshots (
                instrument_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                coverage_status TEXT NOT NULL,
                holder_count INTEGER,
                holder_count_report_date TEXT,
                top_holders_report_date TEXT,
                top_holders_count INTEGER,
                top_holders_total_ratio REAL,
                control_owner_name TEXT,
                control_owner_ratio REAL,
                schema_version TEXT NOT NULL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                snapshot_json TEXT NOT NULL,
                row_hash TEXT,
                row_version INTEGER NOT NULL DEFAULT 1,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_shareholder_snapshots_exchange_source
            ON shareholder_snapshots(exchange, source, updated_at);

            CREATE TABLE IF NOT EXISTS announcement_scan_state (
                purpose_key TEXT NOT NULL,
                source TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                scope_json TEXT NOT NULL,
                committed_cursor_kind TEXT,
                committed_cursor_value TEXT,
                max_published_at TEXT,
                last_scan_started_at TEXT,
                last_scan_completed_at TEXT,
                pages_scanned INTEGER NOT NULL DEFAULT 0,
                requests_made INTEGER NOT NULL DEFAULT 0,
                announcements_seen INTEGER NOT NULL DEFAULT 0,
                selected_announcements INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                is_complete INTEGER NOT NULL DEFAULT 0,
                stop_reason TEXT,
                attempts_json TEXT NOT NULL DEFAULT '[]',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (purpose_key, source, scope_key)
            );

            CREATE INDEX IF NOT EXISTS idx_announcement_scan_state_updated
            ON announcement_scan_state(purpose_key, source, updated_at);

            CREATE TABLE IF NOT EXISTS announcement_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                purpose_key TEXT NOT NULL,
                source TEXT NOT NULL,
                announcement_key TEXT NOT NULL,
                source_announcement_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL DEFAULT '',
                symbol TEXT,
                exchange TEXT,
                market TEXT,
                published_at TEXT,
                published_at_raw TEXT,
                title TEXT NOT NULL,
                attachments_json TEXT NOT NULL DEFAULT '[]',
                selection_reasons_json TEXT NOT NULL DEFAULT '[]',
                diagnostics_json TEXT NOT NULL DEFAULT '[]',
                raw_payload_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id),
                UNIQUE(purpose_key, announcement_key, instrument_id)
            );

            CREATE INDEX IF NOT EXISTS idx_announcement_audit_lookup
            ON announcement_audit(
                purpose_key, source, published_at, instrument_id
            );

            CREATE TABLE IF NOT EXISTS announcement_audit_context (
                purpose_key TEXT NOT NULL,
                source TEXT NOT NULL,
                announcement_key TEXT NOT NULL,
                instrument_id TEXT NOT NULL DEFAULT '',
                scope_key TEXT NOT NULL,
                source_event_key TEXT NOT NULL DEFAULT '',
                window_index INTEGER,
                search_start_date TEXT,
                search_end_date TEXT,
                search_basis TEXT,
                context_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (
                    purpose_key, announcement_key, instrument_id, scope_key
                ),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_announcement_audit_context_event
            ON announcement_audit_context(
                purpose_key, source_event_key, instrument_id, scope_key
            );

            CREATE TABLE IF NOT EXISTS financial_disclosure_event_state (
                instrument_id TEXT NOT NULL,
                report_period TEXT NOT NULL,
                announcement_id TEXT NOT NULL,
                symbol TEXT,
                exchange TEXT,
                status TEXT NOT NULL DEFAULT 'pending_recheck',
                classification TEXT NOT NULL,
                title TEXT,
                announcement_time TEXT,
                selection_reasons_json TEXT NOT NULL DEFAULT '[]',
                missing_fields_json TEXT NOT NULL DEFAULT '[]',
                first_pending_at TEXT,
                pending_recheck_until TEXT,
                processed_at TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, report_period, announcement_id),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_disclosure_event_state_status
            ON financial_disclosure_event_state(status, pending_recheck_until, updated_at);

            CREATE TABLE IF NOT EXISTS shareholder_change_manifest (
                instrument_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                content_hash TEXT,
                top_holders_hash TEXT,
                holder_count_hash TEXT,
                ownership_hash TEXT,
                latest_report_date TEXT,
                coverage_scope_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'unknown',
                last_checked_at TEXT,
                last_changed_at TEXT,
                pending_recheck_until TEXT,
                last_announcement_time TEXT,
                reasons_json TEXT NOT NULL DEFAULT '[]',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_shareholder_change_manifest_status
            ON shareholder_change_manifest(status, pending_recheck_until, updated_at);

            CREATE INDEX IF NOT EXISTS idx_shareholder_change_manifest_exchange
            ON shareholder_change_manifest(exchange, last_checked_at);

            CREATE TABLE IF NOT EXISTS financial_statements_raw (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                statement_type TEXT NOT NULL,
                report_period TEXT NOT NULL,
                publish_date TEXT,
                fiscal_year INTEGER,
                fiscal_quarter INTEGER,
                currency TEXT NOT NULL DEFAULT 'CNY',
                schema_version TEXT NOT NULL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                statement_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, statement_type, report_period),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_statements_raw_instrument
            ON financial_statements_raw(instrument_id, report_period, updated_at);

            CREATE TABLE IF NOT EXISTS financial_source_files (
                source_file_id TEXT PRIMARY KEY,
                instrument_id TEXT,
                symbol TEXT,
                exchange TEXT NOT NULL,
                report_period TEXT NOT NULL,
                report_type TEXT,
                filing_id TEXT,
                source_url TEXT,
                archive_path TEXT,
                content_hash TEXT,
                content_length INTEGER,
                published_at TEXT,
                downloaded_at TEXT,
                parser_version TEXT NOT NULL,
                parser_diagnostics_json TEXT NOT NULL DEFAULT '{}',
                schema_version TEXT NOT NULL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                source_tier TEXT,
                status TEXT NOT NULL,
                supersedes_source_file_id TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_source_files_period
            ON financial_source_files(exchange, report_period, source, updated_at);

            CREATE TABLE IF NOT EXISTS financial_source_field_mappings (
                mapping_version TEXT NOT NULL,
                profile TEXT NOT NULL,
                canonical_fact TEXT NOT NULL,
                statement_family TEXT NOT NULL,
                sina_field TEXT NOT NULL,
                ths_metric TEXT NOT NULL,
                relationship TEXT NOT NULL,
                source_unit TEXT NOT NULL,
                canonical_unit TEXT NOT NULL,
                unit_multiplier REAL NOT NULL DEFAULT 1.0,
                value_type TEXT NOT NULL,
                approved_for_core INTEGER NOT NULL DEFAULT 0,
                semantic TEXT NOT NULL DEFAULT '',
                rejection_reason TEXT,
                mapping_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (mapping_version, profile, canonical_fact, sina_field, ths_metric),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_source_field_mappings_profile
            ON financial_source_field_mappings(mapping_version, profile, approved_for_core);

            CREATE TABLE IF NOT EXISTS financial_source_mapping_audits (
                audit_id TEXT PRIMARY KEY,
                mapping_version TEXT NOT NULL,
                profile TEXT NOT NULL,
                instrument_id TEXT,
                report_period TEXT,
                status TEXT NOT NULL,
                summary_json TEXT NOT NULL DEFAULT '{}',
                audit_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_source_mapping_audits_lookup
            ON financial_source_mapping_audits(mapping_version, profile, instrument_id, report_period, updated_at);

            CREATE TABLE IF NOT EXISTS financial_numeric_facts (
                source_file_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                report_period TEXT NOT NULL,
                report_type TEXT,
                statement_family TEXT,
                fact_name TEXT NOT NULL,
                canonical_fact_name TEXT,
                canonical_statement_family TEXT,
                canonical_semantic TEXT,
                canonical_unit TEXT,
                canonical_version TEXT,
                taxonomy_namespace TEXT,
                context_id TEXT NOT NULL DEFAULT '',
                unit TEXT NOT NULL DEFAULT '',
                decimals TEXT,
                precision TEXT,
                period_start TEXT,
                period_end TEXT,
                instant TEXT,
                currency TEXT NOT NULL DEFAULT 'CNY',
                fact_value REAL,
                value_text TEXT,
                dimensions_hash TEXT NOT NULL DEFAULT '',
                dimensions_json TEXT NOT NULL DEFAULT '{}',
                parser_version TEXT NOT NULL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                raw_fact_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (source_file_id, fact_name, context_id, unit, dimensions_hash),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE TABLE IF NOT EXISTS financial_numeric_facts_hot (
                source_file_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                report_period TEXT NOT NULL,
                report_type TEXT,
                statement_family TEXT,
                fact_name TEXT NOT NULL,
                canonical_fact_name TEXT,
                canonical_statement_family TEXT,
                canonical_semantic TEXT,
                canonical_unit TEXT,
                canonical_version TEXT,
                taxonomy_namespace TEXT,
                context_id TEXT NOT NULL DEFAULT '',
                unit TEXT NOT NULL DEFAULT '',
                decimals TEXT,
                precision TEXT,
                period_start TEXT,
                period_end TEXT,
                instant TEXT,
                currency TEXT NOT NULL DEFAULT 'CNY',
                fact_value REAL,
                value_text TEXT,
                dimensions_hash TEXT NOT NULL DEFAULT '',
                dimensions_json TEXT NOT NULL DEFAULT '{}',
                parser_version TEXT NOT NULL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                raw_fact_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (source_file_id, fact_name, context_id, unit, dimensions_hash),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_numeric_facts_hot_instrument
            ON financial_numeric_facts_hot(instrument_id, report_period, fact_name);

            CREATE TABLE IF NOT EXISTS financial_numeric_facts_history (
                source_file_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                report_period TEXT NOT NULL,
                report_type TEXT,
                statement_family TEXT,
                fact_name TEXT NOT NULL,
                canonical_fact_name TEXT,
                canonical_statement_family TEXT,
                canonical_semantic TEXT,
                canonical_unit TEXT,
                canonical_version TEXT,
                taxonomy_namespace TEXT,
                context_id TEXT NOT NULL DEFAULT '',
                unit TEXT NOT NULL DEFAULT '',
                decimals TEXT,
                precision TEXT,
                period_start TEXT,
                period_end TEXT,
                instant TEXT,
                currency TEXT NOT NULL DEFAULT 'CNY',
                fact_value REAL,
                value_text TEXT,
                dimensions_hash TEXT NOT NULL DEFAULT '',
                dimensions_json TEXT NOT NULL DEFAULT '{}',
                parser_version TEXT NOT NULL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                raw_fact_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (source_file_id, fact_name, context_id, unit, dimensions_hash),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_numeric_facts_history_instrument
            ON financial_numeric_facts_history(instrument_id, report_period, fact_name);

            CREATE TABLE IF NOT EXISTS financial_facts (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                report_period TEXT NOT NULL,
                report_type TEXT,
                statement_family TEXT,
                data_available_date TEXT,
                source_file_id TEXT,
                filing_id TEXT,
                publish_date TEXT,
                fiscal_year INTEGER,
                fiscal_quarter INTEGER,
                currency TEXT NOT NULL DEFAULT 'CNY',
                schema_version TEXT NOT NULL,
                revenue REAL,
                gross_profit REAL,
                operating_profit REAL,
                pre_tax_profit REAL,
                net_income REAL,
                operating_cf REAL,
                total_cf REAL,
                total_assets REAL,
                total_liabilities REAL,
                equity REAL,
                current_assets REAL,
                current_liabilities REAL,
                inventory REAL,
                receivables REAL,
                fixed_assets REAL,
                intangible_assets REAL,
                shares_outstanding REAL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                facts_json TEXT NOT NULL,
                lineage_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, report_period),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_facts_instrument
            ON financial_facts(instrument_id, report_period, updated_at);

            CREATE TABLE IF NOT EXISTS financial_core_facts_hot (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                report_period TEXT NOT NULL,
                report_type TEXT,
                statement_family TEXT,
                data_available_date TEXT,
                source_file_id TEXT,
                filing_id TEXT,
                publish_date TEXT,
                fiscal_year INTEGER,
                fiscal_quarter INTEGER,
                currency TEXT NOT NULL DEFAULT 'CNY',
                schema_version TEXT NOT NULL,
                revenue REAL,
                gross_profit REAL,
                operating_profit REAL,
                pre_tax_profit REAL,
                net_income REAL,
                operating_cf REAL,
                total_cf REAL,
                total_assets REAL,
                total_liabilities REAL,
                equity REAL,
                current_assets REAL,
                current_liabilities REAL,
                inventory REAL,
                receivables REAL,
                fixed_assets REAL,
                intangible_assets REAL,
                shares_outstanding REAL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                facts_json TEXT NOT NULL,
                lineage_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, report_period),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_core_facts_hot_instrument
            ON financial_core_facts_hot(instrument_id, report_period, updated_at);

            CREATE TABLE IF NOT EXISTS financial_core_facts_history (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                report_period TEXT NOT NULL,
                report_type TEXT,
                statement_family TEXT,
                data_available_date TEXT,
                source_file_id TEXT,
                filing_id TEXT,
                publish_date TEXT,
                fiscal_year INTEGER,
                fiscal_quarter INTEGER,
                currency TEXT NOT NULL DEFAULT 'CNY',
                schema_version TEXT NOT NULL,
                revenue REAL,
                gross_profit REAL,
                operating_profit REAL,
                pre_tax_profit REAL,
                net_income REAL,
                operating_cf REAL,
                total_cf REAL,
                total_assets REAL,
                total_liabilities REAL,
                equity REAL,
                current_assets REAL,
                current_liabilities REAL,
                inventory REAL,
                receivables REAL,
                fixed_assets REAL,
                intangible_assets REAL,
                shares_outstanding REAL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                facts_json TEXT NOT NULL,
                lineage_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, report_period),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_core_facts_history_instrument
            ON financial_core_facts_history(instrument_id, report_period, updated_at);

            CREATE TABLE IF NOT EXISTS financial_indicator_snapshots (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                report_period TEXT NOT NULL,
                publish_date TEXT,
                fiscal_year INTEGER,
                fiscal_quarter INTEGER,
                currency TEXT NOT NULL DEFAULT 'CNY',
                schema_version TEXT NOT NULL,
                gross_margin REAL,
                operating_margin REAL,
                net_margin REAL,
                roe REAL,
                roa REAL,
                current_ratio REAL,
                quick_ratio REAL,
                asset_liability_ratio REAL,
                revenue_per_share REAL,
                operating_cf_to_revenue REAL,
                operating_cf_to_net_income REAL,
                book_value_per_share REAL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                indicators_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, report_period),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_indicator_snapshots_instrument
            ON financial_indicator_snapshots(instrument_id, report_period, updated_at);

            CREATE TABLE IF NOT EXISTS financial_indicator_snapshots_hot (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                report_period TEXT NOT NULL,
                publish_date TEXT,
                fiscal_year INTEGER,
                fiscal_quarter INTEGER,
                currency TEXT NOT NULL DEFAULT 'CNY',
                schema_version TEXT NOT NULL,
                gross_margin REAL,
                operating_margin REAL,
                net_margin REAL,
                roe REAL,
                roa REAL,
                current_ratio REAL,
                quick_ratio REAL,
                asset_liability_ratio REAL,
                revenue_per_share REAL,
                operating_cf_to_revenue REAL,
                operating_cf_to_net_income REAL,
                book_value_per_share REAL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                indicators_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, report_period),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_indicator_snapshots_hot_instrument
            ON financial_indicator_snapshots_hot(instrument_id, report_period, updated_at);

            CREATE TABLE IF NOT EXISTS financial_indicator_snapshots_history (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                report_period TEXT NOT NULL,
                publish_date TEXT,
                fiscal_year INTEGER,
                fiscal_quarter INTEGER,
                currency TEXT NOT NULL DEFAULT 'CNY',
                schema_version TEXT NOT NULL,
                gross_margin REAL,
                operating_margin REAL,
                net_margin REAL,
                roe REAL,
                roa REAL,
                current_ratio REAL,
                quick_ratio REAL,
                asset_liability_ratio REAL,
                revenue_per_share REAL,
                operating_cf_to_revenue REAL,
                operating_cf_to_net_income REAL,
                book_value_per_share REAL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                indicators_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, report_period),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_financial_indicator_snapshots_history_instrument
            ON financial_indicator_snapshots_history(instrument_id, report_period, updated_at);

            CREATE TABLE IF NOT EXISTS valuation_history (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                currency TEXT NOT NULL DEFAULT 'CNY',
                close_price REAL,
                market_cap REAL,
                float_market_cap REAL,
                pe_ratio REAL,
                pb_ratio REAL,
                ps_ratio REAL,
                pe_static REAL,
                pe_ttm REAL,
                pe_forward REAL,
                pb_mrq REAL,
                ps_static REAL,
                ps_ttm REAL,
                ps_forward REAL,
                calc_method TEXT NOT NULL,
                calc_version TEXT NOT NULL,
                parameter_hash TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                details_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, as_of_date, calc_method, calc_version, parameter_hash),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_valuation_history_instrument
            ON valuation_history(instrument_id, as_of_date, updated_at);

            CREATE INDEX IF NOT EXISTS idx_valuation_history_exchange_date
            ON valuation_history(exchange, as_of_date, instrument_id);

            CREATE INDEX IF NOT EXISTS idx_valuation_history_ingestion_run
            ON valuation_history(ingestion_run_id);

            CREATE TABLE IF NOT EXISTS valuation_inputs (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                currency TEXT NOT NULL DEFAULT 'CNY',
                market_cap REAL,
                shares_outstanding REAL,
                float_market_cap REAL,
                float_shares REAL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                input_kind TEXT NOT NULL,
                unit TEXT,
                data_as_of TEXT NOT NULL,
                diagnostics_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, as_of_date, source, source_mode, input_kind),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_valuation_inputs_instrument
            ON valuation_inputs(instrument_id, as_of_date, updated_at);

            CREATE TABLE IF NOT EXISTS analyst_forecasts (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                rating_summary TEXT,
                report_count INTEGER,
                institution_count INTEGER,
                buy_count INTEGER,
                overweight_count INTEGER,
                neutral_count INTEGER,
                underperform_count INTEGER,
                sell_count INTEGER,
                eps_fy1 REAL,
                eps_fy2 REAL,
                net_profit_fy1 REAL,
                net_profit_fy2 REAL,
                pe_fy1 REAL,
                pe_fy2 REAL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                forecast_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, as_of_date, source, source_mode),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_analyst_forecasts_instrument
            ON analyst_forecasts(instrument_id, as_of_date, updated_at);

            CREATE TABLE IF NOT EXISTS research_reports (
                report_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                publish_date TEXT NOT NULL,
                report_title TEXT NOT NULL,
                institution_name TEXT,
                analyst_name TEXT,
                rating TEXT,
                rating_change TEXT,
                target_price REAL,
                report_url TEXT,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                report_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_research_reports_instrument
            ON research_reports(instrument_id, publish_date, updated_at);

            CREATE TABLE IF NOT EXISTS sentiment_events (
                event_id TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                event_date TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_subtype TEXT,
                title TEXT,
                sentiment_score REAL,
                severity TEXT,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                details_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_sentiment_events_instrument
            ON sentiment_events(instrument_id, event_date, updated_at);

            CREATE TABLE IF NOT EXISTS risk_snapshots (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                benchmark_instrument_id TEXT,
                volatility_20d REAL,
                volatility_60d REAL,
                beta_60d REAL,
                max_drawdown_252d REAL,
                average_turnover_20d REAL,
                average_amount_20d REAL,
                liability_to_asset REAL,
                current_ratio REAL,
                operating_cf_to_net_income REAL,
                negative_event_count_30d INTEGER,
                risk_score REAL,
                risk_level TEXT,
                calc_method TEXT NOT NULL,
                calc_version TEXT NOT NULL,
                parameter_hash TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                details_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, as_of_date, calc_method, calc_version, parameter_hash),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_risk_snapshots_instrument
            ON risk_snapshots(instrument_id, as_of_date, updated_at);

            CREATE TABLE IF NOT EXISTS technical_indicator_latest (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                period TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                adjustment TEXT NOT NULL,
                applied_adjustment TEXT NOT NULL,
                calc_method TEXT NOT NULL,
                calc_version TEXT NOT NULL,
                parameter_hash TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                missing_reason TEXT,
                signal TEXT NOT NULL,
                trend_score REAL,
                close_price REAL,
                pct_change_1d REAL,
                pct_change_20d REAL,
                sma20 REAL,
                sma60 REAL,
                ema12 REAL,
                ema26 REAL,
                macd REAL,
                macd_signal REAL,
                macd_hist REAL,
                rsi14 REAL,
                adx REAL,
                plus_di REAL,
                minus_di REAL,
                stoch_k REAL,
                stoch_d REAL,
                cci REAL,
                williams_r REAL,
                boll_upper REAL,
                boll_middle REAL,
                boll_lower REAL,
                atr14 REAL,
                volume_ratio REAL,
                distance_to_sma20 REAL,
                distance_to_sma60 REAL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, period, adjustment, calc_method, calc_version, parameter_hash),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_technical_indicator_latest_exchange
            ON technical_indicator_latest(exchange, period, adjustment, updated_at);

            CREATE INDEX IF NOT EXISTS idx_technical_indicator_latest_as_of
            ON technical_indicator_latest(as_of_date, updated_at);

            CREATE TABLE IF NOT EXISTS industry_taxonomy (
                taxonomy_system TEXT NOT NULL,
                taxonomy_version TEXT NOT NULL DEFAULT '',
                industry_code TEXT NOT NULL,
                industry_name TEXT NOT NULL,
                industry_level INTEGER NOT NULL DEFAULT 1,
                parent_code TEXT,
                sw_index_code TEXT,
                aliases_json TEXT NOT NULL DEFAULT '{}',
                source_classification TEXT,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                row_hash TEXT,
                row_version INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (taxonomy_system, taxonomy_version, industry_code)
            );

            CREATE INDEX IF NOT EXISTS idx_industry_taxonomy_source
            ON industry_taxonomy(source, updated_at);

            CREATE TABLE IF NOT EXISTS industry_source_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                artifact_kind TEXT NOT NULL,
                url TEXT NOT NULL,
                parser_version TEXT NOT NULL,
                status TEXT NOT NULL,
                etag TEXT,
                last_modified TEXT,
                content_length INTEGER,
                sha256 TEXT NOT NULL DEFAULT '',
                row_count INTEGER NOT NULL DEFAULT 0,
                max_source_update_time TEXT,
                raw_headers_json TEXT NOT NULL DEFAULT '{}',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(source, source_mode, artifact_kind, sha256),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_industry_source_files_latest
            ON industry_source_files(source, source_mode, artifact_kind, updated_at);

            CREATE TABLE IF NOT EXISTS industry_classification_history (
                row_hash TEXT NOT NULL PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                taxonomy_system TEXT NOT NULL,
                taxonomy_version TEXT NOT NULL DEFAULT '',
                official_industry_code TEXT NOT NULL,
                official_start_date TEXT,
                official_update_time TEXT,
                source_file_id INTEGER,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                classification_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (source_file_id) REFERENCES industry_source_files(id),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_industry_classification_history_symbol
            ON industry_classification_history(symbol, official_update_time);

            CREATE INDEX IF NOT EXISTS idx_industry_classification_history_exchange
            ON industry_classification_history(exchange, official_industry_code);

            CREATE TABLE IF NOT EXISTS industry_official_classifications (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                taxonomy_system TEXT NOT NULL,
                taxonomy_version TEXT NOT NULL DEFAULT '',
                official_industry_code TEXT NOT NULL,
                official_start_date TEXT,
                official_update_time TEXT,
                mapped_industry_code TEXT,
                mapped_industry_name TEXT,
                mapped_industry_level INTEGER,
                mapped_parent_code TEXT,
                mapping_status TEXT NOT NULL DEFAULT 'unmapped',
                mapping_confidence TEXT,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                classification_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, taxonomy_system, taxonomy_version),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_industry_official_code
            ON industry_official_classifications(official_industry_code, updated_at);

            CREATE INDEX IF NOT EXISTS idx_industry_official_mapping_status
            ON industry_official_classifications(mapping_status, updated_at);

            CREATE TABLE IF NOT EXISTS industry_official_code_mappings (
                taxonomy_system TEXT NOT NULL,
                taxonomy_version TEXT NOT NULL DEFAULT '',
                official_industry_code TEXT NOT NULL,
                best_taxonomy_industry_code TEXT,
                mapped_industry_code TEXT,
                mapping_status TEXT NOT NULL DEFAULT 'unmapped',
                mapping_confidence TEXT,
                overlap_count INTEGER NOT NULL DEFAULT 0,
                official_symbol_count INTEGER NOT NULL DEFAULT 0,
                taxonomy_symbol_count INTEGER NOT NULL DEFAULT 0,
                precision REAL NOT NULL DEFAULT 0,
                recall REAL NOT NULL DEFAULT 0,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                built_at TEXT NOT NULL,
                mapping_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (taxonomy_system, taxonomy_version, official_industry_code),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_industry_official_code_mappings_status
            ON industry_official_code_mappings(mapping_status, updated_at);

            CREATE INDEX IF NOT EXISTS idx_industry_official_code_mappings_built_at
            ON industry_official_code_mappings(built_at, updated_at);

            CREATE TABLE IF NOT EXISTS industry_component_sets (
                taxonomy_system TEXT NOT NULL,
                taxonomy_version TEXT NOT NULL DEFAULT '',
                industry_code TEXT NOT NULL,
                component_count INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                built_at TEXT NOT NULL,
                symbols_json TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (taxonomy_system, taxonomy_version, industry_code),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_industry_component_sets_built_at
            ON industry_component_sets(built_at, updated_at);

            CREATE TABLE IF NOT EXISTS industry_memberships (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                taxonomy_system TEXT NOT NULL,
                taxonomy_version TEXT NOT NULL DEFAULT '',
                industry_code TEXT NOT NULL,
                industry_name TEXT NOT NULL,
                industry_level INTEGER NOT NULL DEFAULT 1,
                parent_code TEXT,
                mapping_status TEXT NOT NULL DEFAULT 'reference_only',
                effective_date TEXT,
                source_classification TEXT,
                source_industry_name TEXT,
                sw_l1_code TEXT,
                sw_l1_name TEXT,
                sw_l2_code TEXT,
                sw_l2_name TEXT,
                sw_l3_code TEXT,
                sw_l3_name TEXT,
                sw_l1_index_code TEXT,
                sw_l2_index_code TEXT,
                sw_l3_index_code TEXT,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                data_as_of TEXT NOT NULL,
                membership_json TEXT NOT NULL,
                row_hash TEXT,
                row_version INTEGER NOT NULL DEFAULT 1,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (instrument_id, taxonomy_system, taxonomy_version),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id),
                FOREIGN KEY (taxonomy_system, taxonomy_version, industry_code)
                    REFERENCES industry_taxonomy(taxonomy_system, taxonomy_version, industry_code)
            );

            CREATE INDEX IF NOT EXISTS idx_industry_memberships_exchange_source
            ON industry_memberships(exchange, source, updated_at);

            CREATE INDEX IF NOT EXISTS idx_industry_memberships_instrument
            ON industry_memberships(instrument_id, updated_at);

            CREATE INDEX IF NOT EXISTS idx_industry_memberships_gap_lookup
            ON industry_memberships(
                taxonomy_system,
                taxonomy_version,
                mapping_status,
                exchange,
                instrument_id
            );

            CREATE TABLE IF NOT EXISTS industry_index_analysis_daily (
                taxonomy_system TEXT NOT NULL,
                taxonomy_version TEXT NOT NULL DEFAULT '',
                sw_index_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                sw_index_name TEXT NOT NULL,
                index_type TEXT,
                close_index REAL,
                bargain_volume REAL,
                markup REAL,
                turnover_rate REAL,
                pe REAL,
                pb REAL,
                mean_price REAL,
                bargain_sum_rate REAL,
                negotiable_share_sum REAL,
                average_negotiable_share_sum REAL,
                dividend_yield REAL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                raw_payload_json TEXT NOT NULL DEFAULT '{}',
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (taxonomy_system, taxonomy_version, sw_index_code, trade_date),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_industry_index_analysis_latest
            ON industry_index_analysis_daily(
                taxonomy_system,
                taxonomy_version,
                trade_date,
                index_type
            );

            CREATE TABLE IF NOT EXISTS raw_payload_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                instrument_id TEXT,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id),
                UNIQUE(domain, instrument_id, source, payload_hash)
            );

            CREATE INDEX IF NOT EXISTS idx_raw_payload_domain_instrument
            ON raw_payload_audit(domain, instrument_id, fetched_at);

            CREATE INDEX IF NOT EXISTS idx_raw_payload_run_id
            ON raw_payload_audit(ingestion_run_id);

            -- REQ-13: 无风险利率序列 (仿 fx_series / fx_observations 模式)
            CREATE TABLE IF NOT EXISTS risk_free_rate_series (
                series_id TEXT NOT NULL PRIMARY KEY,
                name TEXT NOT NULL,
                rate_type TEXT NOT NULL,          -- e.g. china_treasury_yield / shibor
                tenor TEXT,                        -- e.g. 10Y / 3M / ON
                currency TEXT NOT NULL DEFAULT 'CNY',
                unit TEXT NOT NULL DEFAULT 'percent_annual',
                frequency TEXT NOT NULL DEFAULT 'daily',
                timezone TEXT NOT NULL DEFAULT 'Asia/Shanghai',
                source_profile TEXT,
                source TEXT,
                source_mode TEXT,
                data_as_of TEXT,
                row_hash TEXT,
                row_version INTEGER NOT NULL DEFAULT 1,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE TABLE IF NOT EXISTS risk_free_rate_observations (
                series_id TEXT NOT NULL,
                observation_date TEXT NOT NULL,
                source_profile TEXT NOT NULL DEFAULT 'default',
                value REAL,
                revision_id TEXT NOT NULL DEFAULT 'latest',
                source TEXT,
                source_mode TEXT,
                data_as_of TEXT,
                row_hash TEXT,
                row_version INTEGER NOT NULL DEFAULT 1,
                ingestion_run_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (series_id, observation_date, source_profile, revision_id),
                FOREIGN KEY (series_id) REFERENCES risk_free_rate_series(series_id),
                FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_risk_free_obs_series_date
            ON risk_free_rate_observations(series_id, observation_date);
            """
        )


def _route_financial_method_to_financial_db(method_name: str) -> None:
    original = getattr(ResearchStorageManager, method_name)

    def wrapped(self: ResearchStorageManager, *args: Any, **kwargs: Any) -> Any:
        if self._active_db_path is not None:
            return original(self, *args, **kwargs)
        with self.financial_database_scope():
            return original(self, *args, **kwargs)

    wrapped.__name__ = original.__name__
    wrapped.__doc__ = original.__doc__
    setattr(ResearchStorageManager, method_name, wrapped)


def _route_interests_method_to_interests_db(method_name: str) -> None:
    original = getattr(ResearchStorageManager, method_name)

    def wrapped(self: ResearchStorageManager, *args: Any, **kwargs: Any) -> Any:
        if self._active_db_path is not None:
            return original(self, *args, **kwargs)
        with self.interests_database_scope():
            return original(self, *args, **kwargs)

    wrapped.__name__ = original.__name__
    wrapped.__doc__ = original.__doc__
    setattr(ResearchStorageManager, method_name, wrapped)


for _interests_method_name in (
    "upsert_risk_free_rate_series",
    "upsert_risk_free_rate_observations",
    "list_risk_free_rate_series",
    "get_risk_free_rate_observations",
):
    _route_interests_method_to_interests_db(_interests_method_name)


for _financial_method_name in (
    "upsert_financial_summary",
    "get_financial_summary",
    "upsert_financial_statement_bundle",
    "upsert_financial_statement_raw",
    "upsert_financial_source_file_manifest",
    "get_financial_source_file_manifests",
    "upsert_financial_source_field_mappings",
    "get_financial_source_field_mappings",
    "upsert_financial_mapping_audit_result",
    "get_financial_mapping_audit_results",
    "upsert_financial_numeric_facts",
    "replace_financial_numeric_facts_for_source_file",
    "get_financial_numeric_facts",
    "get_financial_local_core_facts",
    "upsert_financial_facts",
    "upsert_financial_indicator_snapshot",
    "get_financial_statement_bundle",
    "get_financial_core_facts",
    "summarize_financial_period_coverage",
    "detect_financial_coverage_gaps",
    "validate_financial_statement_readiness",
    "summarize_financial_tier_coverage",
    "derive_financial_core_facts_from_numeric_facts",
    "maintain_financial_hot_cold_tiers",
):
    _route_financial_method_to_financial_db(_financial_method_name)
