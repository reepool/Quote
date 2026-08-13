"""Provider protocol, capabilities, and registry for announcements."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from .models import AnnouncementQuery, AnnouncementScanResult


class AnnouncementQueryNotSupported(ValueError):
    """Raised before network access when a provider cannot satisfy a query."""


@dataclass(frozen=True)
class AnnouncementProviderCapabilities:
    """Explicit provider eligibility boundary; it is not route priority."""

    exchanges: frozenset[str]
    supports_market_scope: bool = True
    supports_instrument_scope: bool = False
    supports_date_filter: bool = False
    supports_keyword_filter: bool = False
    supports_category_filter: bool = False
    cursor_kind: str | None = None
    max_page_size: int = 30
    supports_attachment_retrieval: bool = False
    requires_provider_identity: bool = False
    attachment_version_signal: str | None = None

    def __post_init__(self) -> None:
        exchanges = frozenset(str(item).strip().upper() for item in self.exchanges if str(item).strip())
        if not exchanges:
            raise ValueError("provider capability exchanges cannot be empty")
        object.__setattr__(self, "exchanges", exchanges)
        object.__setattr__(self, "max_page_size", max(1, int(self.max_page_size)))
        signal = (
            None
            if self.attachment_version_signal in (None, "")
            else str(self.attachment_version_signal).strip().lower()
        )
        if signal not in {None, "etag", "last_modified", "source_version_id"}:
            raise ValueError("unsupported attachment version signal")
        object.__setattr__(self, "attachment_version_signal", signal)

    def validate(self, query: AnnouncementQuery) -> None:
        scope = query.scope
        if scope.exchange not in self.exchanges:
            raise AnnouncementQueryNotSupported(
                f"provider does not support exchange {scope.exchange}"
            )
        if scope.is_instrument_scoped and not self.supports_instrument_scope:
            raise AnnouncementQueryNotSupported("provider does not support instrument scope")
        if not scope.is_instrument_scoped and not self.supports_market_scope:
            raise AnnouncementQueryNotSupported("provider does not support market scope")
        if scope.start_date and not self.supports_date_filter:
            raise AnnouncementQueryNotSupported("provider does not support date filters")
        if scope.keyword and not self.supports_keyword_filter:
            raise AnnouncementQueryNotSupported("provider does not support keyword filters")
        if scope.category and not self.supports_category_filter:
            raise AnnouncementQueryNotSupported("provider does not support category filters")
        if scope.cursor and self.cursor_kind and scope.cursor.kind != self.cursor_kind:
            raise AnnouncementQueryNotSupported(
                f"provider expects cursor kind {self.cursor_kind}"
            )


@runtime_checkable
class AnnouncementProvider(Protocol):
    """Synchronous provider contract used by rate-limited research workflows."""

    source_name: str
    capabilities: AnnouncementProviderCapabilities

    def discover(self, query: AnnouncementQuery) -> AnnouncementScanResult:
        """Discover one bounded query and return normalized records."""


@dataclass
class AnnouncementProviderRegistry:
    """Resolve provider implementations independently from route order."""

    providers: dict[str, AnnouncementProvider] = field(default_factory=dict)

    def __init__(self, providers: Iterable[AnnouncementProvider] | None = None) -> None:
        self.providers = {}
        for provider in providers or ():
            self.register(provider)

    def register(self, provider: AnnouncementProvider) -> None:
        source = str(provider.source_name or "").strip().lower()
        if not source:
            raise ValueError("announcement provider source_name is required")
        if source in self.providers:
            raise ValueError(f"announcement provider already registered: {source}")
        self.providers[source] = provider

    def get(self, source: str) -> AnnouncementProvider | None:
        return self.providers.get(str(source or "").strip().lower())

    def require(self, source: str) -> AnnouncementProvider:
        provider = self.get(source)
        if provider is None:
            raise ValueError(f"announcement provider is not registered: {source}")
        return provider

    def validate_query(self, source: str, query: AnnouncementQuery) -> None:
        self.require(source).capabilities.validate(query.for_source(source))
