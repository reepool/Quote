"""Configuration parsing and validation for announcement acquisition."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Mapping, Optional, Tuple

from utils.config_manager import ResearchConfig


ALLOWED_FALLBACK_STATUSES = frozenset(
    {"failed", "degraded", "identity_not_found", "success_empty", "indeterminate"}
)


@dataclass(frozen=True)
class AnnouncementRouteConfig:
    """Ordered sources and explicit statuses that permit fallback."""

    sources: Tuple[str, ...]
    fallback_on: FrozenSet[str] = frozenset(
        {"failed", "degraded", "identity_not_found", "indeterminate"}
    )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AnnouncementRouteConfig":
        sources = tuple(
            str(item).strip().lower()
            for item in value.get("sources", ())
            if str(item).strip()
        )
        if not sources:
            raise ValueError("announcement route sources cannot be empty")
        if len(sources) != len(set(sources)):
            raise ValueError("announcement route sources must be unique")
        fallback_on = frozenset(
            str(item).strip().lower()
            for item in value.get(
                "fallback_on",
                ("failed", "degraded", "identity_not_found", "indeterminate"),
            )
            if str(item).strip()
        )
        unknown = fallback_on - ALLOWED_FALLBACK_STATUSES
        if unknown:
            raise ValueError(
                f"unsupported announcement fallback statuses: {sorted(unknown)}"
            )
        return cls(sources=sources, fallback_on=fallback_on)


@dataclass(frozen=True)
class AnnouncementAcquisitionConfig:
    """Validated provider parameters and purpose/exchange routes."""

    provider_configs: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    default_route: Optional[AnnouncementRouteConfig] = None
    purpose_routes: Dict[str, Dict[str, AnnouncementRouteConfig]] = field(
        default_factory=dict
    )

    def route_for(self, purpose_key: str, exchange: str) -> AnnouncementRouteConfig:
        purpose = str(purpose_key or "").strip()
        market = str(exchange or "").strip().upper()
        base_purpose = purpose.split(":", 1)[0]
        route = self.purpose_routes.get(purpose, {}).get(market)
        if route is None and base_purpose != purpose:
            route = self.purpose_routes.get(base_purpose, {}).get(market)
        if route is None:
            route = self.default_route
        if route is None:
            raise ValueError(
                f"announcement route is not configured: purpose={purpose} exchange={market}"
            )
        return route


def load_announcement_acquisition_config(
    research_config: ResearchConfig,
) -> AnnouncementAcquisitionConfig:
    """Load the new common announcement configuration from research config."""
    provider_configs: Dict[str, Dict[str, Any]] = {}
    for source, raw_source in (research_config.sources or {}).items():
        if not isinstance(raw_source, Mapping):
            continue
        raw_announcements = raw_source.get("announcements")
        if isinstance(raw_announcements, Mapping):
            provider_configs[str(source).strip().lower()] = dict(raw_announcements)

    routing = (research_config.routing or {}).get("official_announcements", {})
    if not isinstance(routing, Mapping):
        raise ValueError("routing.official_announcements must be a mapping")
    default_raw = routing.get("default")
    default_route = (
        AnnouncementRouteConfig.from_mapping(default_raw)
        if isinstance(default_raw, Mapping)
        else None
    )
    purpose_routes: Dict[str, Dict[str, AnnouncementRouteConfig]] = {}
    raw_purposes = routing.get("purposes", {})
    if raw_purposes and not isinstance(raw_purposes, Mapping):
        raise ValueError("official announcement purpose routes must be a mapping")
    for purpose_key, raw_exchanges in (raw_purposes or {}).items():
        if not isinstance(raw_exchanges, Mapping):
            raise ValueError(f"announcement purpose route must be a mapping: {purpose_key}")
        purpose_routes[str(purpose_key).strip()] = {
            str(exchange).strip().upper(): AnnouncementRouteConfig.from_mapping(raw_route)
            for exchange, raw_route in raw_exchanges.items()
            if isinstance(raw_route, Mapping)
        }
    configured_sources = set(provider_configs)
    all_routes = [route for routes in purpose_routes.values() for route in routes.values()]
    if default_route is not None:
        all_routes.append(default_route)
    for route in all_routes:
        missing = set(route.sources) - configured_sources
        if missing:
            raise ValueError(
                f"announcement route references unconfigured sources: {sorted(missing)}"
            )
    return AnnouncementAcquisitionConfig(
        provider_configs=provider_configs,
        default_route=default_route,
        purpose_routes=purpose_routes,
    )
