"""CNInfo discovery adapter for governed company business-profile documents."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from typing import Any, Dict, List, Optional

from research.business_profile_documents import (
    BusinessProfileDocumentClassification,
    classify_business_profile_document,
)
from research.announcements import (
    AnnouncementAcquisitionService,
    AnnouncementQuery,
    AnnouncementScope,
    ProviderCursor,
    load_announcement_acquisition_config,
)
from research.providers.registry import OfficialAnnouncementProviderRegistry


BUSINESS_PROFILE_DISCOVERY_PURPOSE = "business_profile_evidence"
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


class BusinessProfileAnnouncementDiscoveryAdapter:
    """Discover official documents without downloading or writing profile facts."""

    def __init__(
        self,
        *,
        storage: Any = None,
        acquisition_service: Optional[AnnouncementAcquisitionService] = None,
        purpose_key: str = BUSINESS_PROFILE_DISCOVERY_PURPOSE,
    ) -> None:
        self.storage = storage
        self.acquisition_service = acquisition_service
        self.last_route_result: Any = None
        self.purpose_key = purpose_key

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
        if exchange not in {"SSE", "SZSE", "BSE"}:
            raise ValueError(f"unsupported A-share exchange: {exchange}")
        return self._discover_with_common_service(
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
            search_key=search_key,
            category=category,
            page_size=page_size,
            max_pages=max_pages,
            dry_run=dry_run,
            ingestion_run_id=ingestion_run_id,
        )

    def _common_service(self) -> AnnouncementAcquisitionService:
        if self.acquisition_service is None:
            from utils.config_manager import config_manager

            research_config = config_manager.get_research_config()
            self.acquisition_service = AnnouncementAcquisitionService(
                registry=OfficialAnnouncementProviderRegistry(
                    research_config=research_config
                ),
                config=load_announcement_acquisition_config(research_config),
            )
        return self.acquisition_service

    def _discover_with_common_service(
        self,
        *,
        instrument_id: str,
        symbol: str,
        exchange: str,
        start_date: Optional[str],
        end_date: Optional[str],
        search_key: Optional[str],
        category: Optional[str],
        page_size: int,
        max_pages: int,
        dry_run: bool,
        ingestion_run_id: Optional[int],
    ) -> BusinessProfileDiscoveryResult:
        """Discover through source-neutral acquisition and classify downstream."""
        scoped_purpose = f"{self.purpose_key}:{instrument_id}"
        scope = AnnouncementScope(
            exchange=exchange,
            market=exchange,
            instrument_id=instrument_id,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            keyword=search_key,
            category=category,
            page_size=page_size,
            max_pages=max_pages,
        )
        provider_cursors: Dict[str, ProviderCursor] = {}
        if self.storage is not None and hasattr(
            self.storage, "get_announcement_scan_state"
        ):
            route = self._common_service().config.route_for(
                scoped_purpose,
                exchange,
            )
            for source in route.sources:
                state = self.storage.get_announcement_scan_state(
                    purpose_key=scoped_purpose,
                    source=source,
                    scope_key=scope.scope_key,
                )
                if state and state.get("committed_cursor"):
                    cursor = state["committed_cursor"]
                    provider_cursors[source] = ProviderCursor(
                        kind=str(cursor["kind"]),
                        value=str(cursor["value"]),
                    )
        query = AnnouncementQuery(purpose_key=scoped_purpose, scope=scope)
        route_result = self._common_service().acquire(
            query,
            selectors=[self._common_record_filter],
            provider_cursors=provider_cursors,
        )
        self.last_route_result = route_result
        scan_result = route_result.scan_result
        if scan_result is None:
            return BusinessProfileDiscoveryResult(
                status="degraded",
                purpose_key=scoped_purpose,
                instrument_id=instrument_id,
                symbol=symbol,
                exchange=exchange,
                pages_scanned=0,
                announcements_seen=0,
                candidates=[],
                max_announcement_time=None,
                stopped_at_watermark=False,
                errors=["announcement_route_returned_no_result"],
                source="cninfo",
                source_tier="official_primary",
            )
        candidates = [
            self._common_candidate(record) for record in scan_result.selected_records
        ]
        source = scan_result.source
        status = self._common_status(scan_result.status)
        if not dry_run:
            self._persist_common_discovery(
                scoped_purpose=scoped_purpose,
                instrument_id=instrument_id,
                symbol=symbol,
                scan_result=scan_result,
                route_result=route_result,
                candidates=candidates,
                ingestion_run_id=ingestion_run_id,
            )
        return BusinessProfileDiscoveryResult(
            status=status,
            purpose_key=scoped_purpose,
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            pages_scanned=scan_result.pages_scanned,
            announcements_seen=scan_result.announcements_seen,
            candidates=candidates,
            max_announcement_time=scan_result.max_published_at,
            stopped_at_watermark=scan_result.reached_prior_cursor,
            errors=list(scan_result.errors),
            source=source,
            source_tier=(
                "official_primary"
                if source == "cninfo"
                else "official_backup"
            ),
        )

    @staticmethod
    def _common_status(status: str) -> str:
        if status == "identity_not_found":
            return "not_found"
        if status in {"failed", "degraded", "indeterminate"}:
            return "degraded"
        return "success"

    @staticmethod
    def _common_record_filter(record: Any) -> List[str]:
        attachment_type = (
            record.attachments[0].file_extension
            if record.attachments
            else None
        )
        classification = classify_business_profile_document(
            record.title,
            adjunct_type=attachment_type,
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
    def _common_candidate(record: Any) -> BusinessProfileDocumentCandidate:
        attachment = record.attachments[0] if record.attachments else None
        classification = classify_business_profile_document(
            record.title,
            adjunct_type=(
                attachment.file_extension if attachment else None
            ),
        )
        return BusinessProfileDocumentCandidate(
            announcement_id=(
                record.source_announcement_id
                if record.source == "cninfo"
                else record.announcement_key
            ),
            title=record.title,
            announcement_time=record.published_at,
            symbols=list(record.symbols),
            adjunct_url=(
                attachment.resolved_url or attachment.source_url
                if attachment
                else None
            ),
            adjunct_type=attachment.file_extension if attachment else None,
            classification=classification,
            selection_reasons=list(record.selection_reasons),
            source=record.source,
            source_tier=(
                "official_primary"
                if record.source == "cninfo"
                else "official_backup"
            ),
            raw_payload=dict(record.raw_payload),
        )

    def _persist_common_discovery(
        self,
        *,
        scoped_purpose: str,
        instrument_id: str,
        symbol: str,
        scan_result: Any,
        route_result: Any,
        candidates: List[BusinessProfileDocumentCandidate],
        ingestion_run_id: Optional[int],
    ) -> None:
        if self.storage is None:
            raise RuntimeError("storage is required when dry_run is false")
        self.storage.upsert_announcement_scan_state(
            scan_result=scan_result,
            selected_announcements=len(candidates),
            attempts=[asdict(item) for item in route_result.attempts],
            metadata={
                "instrument_id": instrument_id,
                "route_fallback_used": route_result.fallback_used,
                "route_fallback_reason": route_result.fallback_reason,
                "document_types": sorted(
                    {item.classification.document_type for item in candidates}
                ),
            },
        )
        candidate_ids = {item.announcement_id for item in candidates}
        for record in scan_result.selected_records:
            candidate_id = (
                record.source_announcement_id
                if record.source == "cninfo"
                else record.announcement_key
            )
            if candidate_id not in candidate_ids:
                continue
            attachment_type = (
                record.attachments[0].file_extension
                if record.attachments
                else None
            )
            classification = classify_business_profile_document(
                record.title,
                adjunct_type=attachment_type,
            )
            audited = replace(
                record,
                raw_payload={
                    **record.raw_payload,
                    "business_profile_classification": asdict(classification),
                },
            )
            self.storage.store_announcement_audit(
                purpose_key=self.purpose_key,
                record=audited,
                instrument_id=instrument_id,
                symbol=symbol,
                ingestion_run_id=ingestion_run_id,
            )
