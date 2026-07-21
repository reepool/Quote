"""Business-profile view over common official-announcement routing."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from research.business_profile_discovery import (
    BusinessProfileAnnouncementDiscoveryAdapter,
    BusinessProfileDiscoveryResult,
    BusinessProfileDocumentCandidate,
)


OFFICIAL_EXCHANGE_SOURCE_TIER = "official_backup"


@dataclass(frozen=True)
class BusinessProfileSourceAttempt:
    """Compact diagnostic for one configured source attempt."""

    source: str
    source_tier: str
    status: str
    candidate_count: int
    pages_scanned: int
    announcements_seen: int
    errors: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class BusinessProfileDiscoveryResolution:
    """Chosen source result plus all configured route diagnostics."""

    status: str
    selected_source: Optional[str]
    selected_source_tier: Optional[str]
    fallback_used: bool
    fallback_reason: Optional[str]
    candidates: List[BusinessProfileDocumentCandidate]
    attempts: List[BusinessProfileSourceAttempt]

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "candidates": [item.to_dict() for item in self.candidates],
            "attempts": [asdict(item) for item in self.attempts],
        }


class BusinessProfileDiscoveryCoordinator:
    """Expose common announcement routing through the business result schema."""

    def __init__(
        self,
        *,
        primary_adapter: Optional[BusinessProfileAnnouncementDiscoveryAdapter] = None,
    ) -> None:
        self.primary_adapter = (
            primary_adapter or BusinessProfileAnnouncementDiscoveryAdapter()
        )

    @classmethod
    def from_research_config(
        cls,
        research_config: Any,
        *,
        primary_adapter: Optional[BusinessProfileAnnouncementDiscoveryAdapter] = None,
    ) -> "BusinessProfileDiscoveryCoordinator":
        del research_config
        return cls(primary_adapter=primary_adapter)

    def discover_instrument(
        self,
        instrument: Dict[str, Any],
        **kwargs: Any,
    ) -> BusinessProfileDiscoveryResolution:
        """Discover through the configured common route exactly once."""
        try:
            result = self.primary_adapter.discover_instrument(instrument, **kwargs)
        except Exception as exc:
            return BusinessProfileDiscoveryResolution(
                status="failed",
                selected_source=None,
                selected_source_tier=None,
                fallback_used=False,
                fallback_reason="primary_failed",
                candidates=[],
                attempts=[
                    BusinessProfileSourceAttempt(
                        source="configured_route",
                        source_tier="official_primary",
                        status="failed",
                        candidate_count=0,
                        pages_scanned=0,
                        announcements_seen=0,
                        errors=[str(exc)],
                    )
                ],
            )

        route_result = self.primary_adapter.last_route_result
        if route_result is None:
            attempts = [self._source_attempt(result)]
            fallback_used = False
            fallback_reason = None
        else:
            attempts = [
                BusinessProfileSourceAttempt(
                    source=item.source,
                    source_tier=self._source_tier(item.source),
                    status=item.status,
                    candidate_count=item.selected_count,
                    pages_scanned=item.pages_scanned,
                    announcements_seen=item.record_count,
                    errors=list(item.errors),
                )
                for item in route_result.attempts
            ]
            fallback_used = route_result.fallback_used
            fallback_reason = route_result.fallback_reason
            if fallback_reason == "success_empty":
                fallback_reason = "primary_empty"
            elif fallback_reason:
                fallback_reason = f"primary_{fallback_reason}"
        return BusinessProfileDiscoveryResolution(
            status=self._resolution_status(result),
            selected_source=result.source,
            selected_source_tier=result.source_tier,
            fallback_used=fallback_used,
            fallback_reason=fallback_reason,
            candidates=list(result.candidates),
            attempts=attempts,
        )

    @staticmethod
    def _source_tier(source: str) -> str:
        return "official_primary" if source == "cninfo" else OFFICIAL_EXCHANGE_SOURCE_TIER

    @classmethod
    def _source_attempt(
        cls,
        result: BusinessProfileDiscoveryResult,
    ) -> BusinessProfileSourceAttempt:
        return BusinessProfileSourceAttempt(
            source=result.source,
            source_tier=result.source_tier,
            status=result.status,
            candidate_count=len(result.candidates),
            pages_scanned=result.pages_scanned,
            announcements_seen=result.announcements_seen,
            errors=list(result.errors),
        )

    @staticmethod
    def _resolution_status(result: BusinessProfileDiscoveryResult) -> str:
        if result.candidates:
            return "success"
        if result.status in {"failed", "degraded"}:
            return "degraded"
        return "not_found"
