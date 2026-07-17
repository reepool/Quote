"""CNInfo discovery adapter for governed company business-profile documents."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from research.business_profile_documents import (
    BusinessProfileDocumentClassification,
    classify_business_profile_document,
)
from research.providers.cninfo_announcements import (
    CninfoAnnouncementRecord,
    CninfoAnnouncementScanConfig,
    CninfoAnnouncementScanner,
)
from utils.date_utils import get_shanghai_time


BUSINESS_PROFILE_DISCOVERY_PURPOSE = "business_profile_evidence"
CNINFO_MARKET_CONFIGS: Dict[str, Dict[str, str]] = {
    "SSE": {"market": "SSE", "column": "sse", "plate": "sh"},
    "SZSE": {"market": "SZSE", "column": "szse", "plate": "sz"},
    "BSE": {"market": "BSE", "column": "neeq", "plate": "bj"},
}


@dataclass(frozen=True)
class BusinessProfileDocumentCandidate:
    """One classified official announcement candidate."""

    announcement_id: str
    title: str
    announcement_time: Optional[str]
    symbols: List[str]
    adjunct_url: Optional[str]
    adjunct_type: Optional[str]
    classification: BusinessProfileDocumentClassification
    selection_reasons: List[str] = field(default_factory=list)
    source: str = "cninfo"
    source_tier: str = "official_primary"
    raw_payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["classification"] = asdict(self.classification)
        return payload


@dataclass(frozen=True)
class BusinessProfileDiscoveryResult:
    """Bounded discovery result with source-state diagnostics."""

    status: str
    purpose_key: str
    instrument_id: Optional[str]
    symbol: Optional[str]
    exchange: str
    pages_scanned: int
    announcements_seen: int
    candidates: List[BusinessProfileDocumentCandidate]
    max_announcement_time: Optional[str]
    stopped_at_watermark: bool
    errors: List[str] = field(default_factory=list)
    source: str = "cninfo"
    source_tier: str = "official_primary"

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["candidates"] = [item.to_dict() for item in self.candidates]
        return payload


class CninfoBusinessProfileDiscoveryAdapter:
    """Discover official documents without downloading or writing profile facts."""

    def __init__(
        self,
        *,
        storage: Any = None,
        scanner: Optional[CninfoAnnouncementScanner] = None,
        purpose_key: str = BUSINESS_PROFILE_DISCOVERY_PURPOSE,
        market_configs: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> None:
        self.storage = storage
        self.scanner = scanner or CninfoAnnouncementScanner()
        self.purpose_key = purpose_key
        self.market_configs = {
            **CNINFO_MARKET_CONFIGS,
            **(market_configs or {}),
        }

    def discover_instrument(
        self,
        instrument: Dict[str, Any],
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        search_key: Optional[str] = None,
        category: Optional[str] = None,
        page_size: int = 30,
        max_pages: int = 20,
        dry_run: bool = True,
        ingestion_run_id: Optional[int] = None,
    ) -> BusinessProfileDiscoveryResult:
        """Discover bounded candidate documents for one A-share instrument."""
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        symbol = str(instrument.get("symbol") or "").strip()
        exchange = str(instrument.get("exchange") or "").strip().upper()
        if not instrument_id or not symbol:
            raise ValueError("instrument_id and symbol are required")
        market_config = self.market_configs.get(exchange)
        if market_config is None:
            raise ValueError(f"unsupported A-share exchange: {exchange}")
        identity = self.scanner.resolve_stock_identity(symbol)
        if not identity:
            return BusinessProfileDiscoveryResult(
                status="not_found",
                purpose_key=self.purpose_key,
                instrument_id=instrument_id,
                symbol=symbol,
                exchange=exchange,
                pages_scanned=0,
                announcements_seen=0,
                candidates=[],
                max_announcement_time=None,
                stopped_at_watermark=False,
                errors=["cninfo_stock_identity_not_found"],
            )
        scoped_purpose = f"{self.purpose_key}:{instrument_id}"
        state = self._get_state(
            purpose_key=scoped_purpose,
            market=market_config["market"],
            column=market_config["column"],
        )
        result = self.scanner.scan(
            CninfoAnnouncementScanConfig(
                purpose_key=scoped_purpose,
                market=market_config["market"],
                column=market_config["column"],
                plate=market_config.get("plate"),
                category=category,
                search_key=search_key,
                stock=identity.get("stock"),
                org_id=identity.get("org_id"),
                start_date=start_date,
                end_date=end_date,
                page_size=max(1, page_size),
                max_pages=max(1, max_pages),
                stop_at_watermark=(
                    None if state is None else state.get("last_watermark")
                ),
            ),
            filters=[self._record_filter],
        )
        candidates = [self._candidate(record) for record in result.selected_records]
        status = "success" if not result.errors else "degraded"
        if not dry_run:
            self._persist_discovery(
                scoped_purpose=scoped_purpose,
                instrument_id=instrument_id,
                symbol=symbol,
                market_config=market_config,
                scan_result=result,
                candidates=candidates,
                status=status,
                ingestion_run_id=ingestion_run_id,
            )
        return BusinessProfileDiscoveryResult(
            status=status,
            purpose_key=scoped_purpose,
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            pages_scanned=result.pages_scanned,
            announcements_seen=result.announcements_seen,
            candidates=candidates,
            max_announcement_time=result.max_announcement_time,
            stopped_at_watermark=result.stopped_at_watermark,
            errors=list(result.errors),
        )

    @staticmethod
    def _record_filter(record: CninfoAnnouncementRecord) -> List[str]:
        classification = classify_business_profile_document(
            record.title,
            adjunct_type=record.adjunct_type,
        )
        if not classification.selected:
            return []
        reasons = [f"business_profile_document:{classification.document_type}"]
        if classification.is_correction:
            reasons.append("business_profile_document_correction")
        reasons.extend(
            f"profile_event_hint:{event_type}"
            for event_type in classification.profile_event_hints
        )
        return reasons

    @staticmethod
    def _candidate(
        record: CninfoAnnouncementRecord,
    ) -> BusinessProfileDocumentCandidate:
        classification = classify_business_profile_document(
            record.title,
            adjunct_type=record.adjunct_type,
        )
        return BusinessProfileDocumentCandidate(
            announcement_id=record.announcement_id,
            title=record.title,
            announcement_time=record.announcement_time,
            symbols=list(record.symbols),
            adjunct_url=record.adjunct_url,
            adjunct_type=record.adjunct_type,
            classification=classification,
            selection_reasons=list(record.selection_reasons),
            source="cninfo",
            source_tier="official_primary",
            raw_payload=dict(record.raw_payload),
        )

    def _get_state(self, **kwargs: Any) -> Optional[Dict[str, Any]]:
        if self.storage is None or not hasattr(
            self.storage, "get_cninfo_announcement_scan_state"
        ):
            return None
        return self.storage.get_cninfo_announcement_scan_state(**kwargs)

    def _persist_discovery(
        self,
        *,
        scoped_purpose: str,
        instrument_id: str,
        symbol: str,
        market_config: Dict[str, str],
        scan_result: Any,
        candidates: List[BusinessProfileDocumentCandidate],
        status: str,
        ingestion_run_id: Optional[int],
    ) -> None:
        if self.storage is None:
            raise RuntimeError("storage is required when dry_run is false")
        now = get_shanghai_time().isoformat()
        prior_watermark = scan_result.config.stop_at_watermark
        committed_watermark = (
            prior_watermark
            if scan_result.errors
            else scan_result.max_announcement_time or prior_watermark
        )
        self.storage.upsert_cninfo_announcement_scan_state(
            purpose_key=scoped_purpose,
            market=market_config["market"],
            column=market_config["column"],
            last_watermark=committed_watermark,
            last_scan_started_at=now,
            last_scan_completed_at=now,
            pages_scanned=scan_result.pages_scanned,
            announcements_seen=scan_result.announcements_seen,
            selected_announcements=len(candidates),
            status=status,
            metadata={
                "instrument_id": instrument_id,
                "document_types": sorted(
                    {item.classification.document_type for item in candidates}
                ),
                "stopped_at_watermark": scan_result.stopped_at_watermark,
                "errors": list(scan_result.errors)[:5],
                "watermark_advanced": committed_watermark != prior_watermark,
            },
        )
        record_by_id = {
            record.announcement_id: record for record in scan_result.selected_records
        }
        for candidate in candidates:
            record = record_by_id[candidate.announcement_id]
            self.storage.store_cninfo_announcement_audit(
                purpose_key=self.purpose_key,
                announcement_id=candidate.announcement_id,
                instrument_id=instrument_id,
                symbol=symbol,
                market=record.market,
                column=record.column,
                announcement_time=candidate.announcement_time,
                title=candidate.title,
                adjunct_url=candidate.adjunct_url,
                selection_reasons=candidate.selection_reasons,
                raw_payload={
                    **record.raw_payload,
                    "business_profile_classification": asdict(candidate.classification),
                },
                ingestion_run_id=ingestion_run_id,
            )
