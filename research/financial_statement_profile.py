"""Resolve financial-statement mapping profiles from company metadata."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Mapping, Optional

from research.financial_source_field_mapping import FINANCIAL_STATEMENT_PROFILES
from research.listed_broker_dealer_scope import resolve_listed_broker_dealer_scope


PROFILE_BANK = "bank"
PROFILE_SECURITIES = "securities"
PROFILE_INSURANCE = "insurance"
PROFILE_NONBANK = "nonbank"

KNOWN_FINANCIAL_STATEMENT_PROFILES = set(FINANCIAL_STATEMENT_PROFILES)


@dataclass(frozen=True)
class FinancialStatementProfileResolution:
    """Resolved financial statement profile plus audit-friendly evidence."""

    profile: str
    confidence: str
    source: str
    reason: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def resolve_financial_statement_profile(
    *,
    explicit_profile: Optional[str] = None,
    industry_membership: Optional[Mapping[str, Any]] = None,
    company_profile: Optional[Mapping[str, Any]] = None,
    instrument: Optional[Mapping[str, Any]] = None,
    default_profile: str = PROFILE_NONBANK,
) -> FinancialStatementProfileResolution:
    """Resolve the statement-profile used by source-field mappings.

    The profile is an accounting-format bucket, not a full industry
    classification. Strict Shenwan industry membership is preferred because it
    is already normalized and versioned in this project.
    """
    explicit = _normalize_profile(explicit_profile)
    if explicit:
        return FinancialStatementProfileResolution(
            profile=explicit,
            confidence="explicit",
            source="explicit_profile",
            reason="caller_provided_profile",
            evidence={"explicit_profile": explicit_profile},
        )

    industry_result = _resolve_from_industry_membership(industry_membership)
    if industry_result is not None:
        return industry_result

    company_result = _resolve_from_company_profile(company_profile)
    if company_result is not None:
        return company_result

    instrument_result = _resolve_from_instrument(instrument)
    if instrument_result is not None:
        return instrument_result

    default = _normalize_profile(default_profile) or PROFILE_NONBANK
    return FinancialStatementProfileResolution(
        profile=default,
        confidence="default",
        source="default",
        reason="no_profile_evidence_available",
        evidence={},
    )


def resolve_financial_statement_profiles_for_instruments(
    *,
    storage: Optional[Any],
    instrument_ids: list[str],
    exchange: str,
) -> list[dict[str, Any]]:
    """Resolve statement-profile evidence for a bounded instrument set."""
    resolutions: list[dict[str, Any]] = []
    for instrument_id in instrument_ids:
        industry_membership = _storage_lookup(
            storage,
            "get_industry_membership",
            instrument_id,
        )
        company_profile = _storage_lookup(
            storage,
            "get_company_profile",
            instrument_id,
        )
        resolution = resolve_financial_statement_profile(
            industry_membership=industry_membership,
            company_profile=company_profile,
            instrument={
                "instrument_id": instrument_id,
                "exchange": exchange,
            },
        ).to_dict()
        resolutions.append(
            {
                "instrument_id": instrument_id,
                **resolution,
            }
        )
    return resolutions


def summarize_financial_statement_profile_resolutions(
    resolutions: list[dict[str, Any]],
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "instrument_count": len(resolutions),
        "profile_counts": {},
        "confidence_counts": {},
        "source_counts": {},
    }
    for item in resolutions:
        for key, bucket in (
            ("profile", "profile_counts"),
            ("confidence", "confidence_counts"),
            ("source", "source_counts"),
        ):
            value = str(item.get(key) or "")
            if not value:
                continue
            summary[bucket][value] = summary[bucket].get(value, 0) + 1
    return summary


def _storage_lookup(
    storage: Optional[Any],
    method_name: str,
    instrument_id: str,
) -> Optional[dict[str, Any]]:
    if storage is None:
        return None
    method = getattr(storage, method_name, None)
    if method is None:
        return None
    try:
        return method(instrument_id, include_snapshot=False)
    except TypeError:
        return method(instrument_id)


def _resolve_from_industry_membership(
    industry_membership: Optional[Mapping[str, Any]],
) -> Optional[FinancialStatementProfileResolution]:
    if not industry_membership or not isinstance(industry_membership, Mapping):
        return None
    texts = _industry_texts(industry_membership)
    profile = _profile_from_texts(texts)
    if profile == PROFILE_SECURITIES:
        broker_scope = resolve_listed_broker_dealer_scope(industry_membership)
        if not broker_scope.eligible:
            return FinancialStatementProfileResolution(
                profile=PROFILE_NONBANK,
                confidence="high",
                source="industry_membership",
                reason="securities_candidate_without_confirmed_broker_scope",
                evidence={
                    **_compact_industry_evidence(industry_membership),
                    "listed_broker_dealer_scope": broker_scope.to_dict(),
                },
            )
    if profile is None:
        return FinancialStatementProfileResolution(
            profile=PROFILE_NONBANK,
            confidence="high",
            source="industry_membership",
            reason="strict_industry_not_financial_statement_special_case",
            evidence=_compact_industry_evidence(industry_membership),
        )
    return FinancialStatementProfileResolution(
        profile=profile,
        confidence="high",
        source="industry_membership",
        reason="strict_industry_matches_financial_statement_profile",
        evidence=_compact_industry_evidence(industry_membership),
    )


def _resolve_from_company_profile(
    company_profile: Optional[Mapping[str, Any]],
) -> Optional[FinancialStatementProfileResolution]:
    if not company_profile or not isinstance(company_profile, Mapping):
        return None
    texts = [
        company_profile.get("industry_raw"),
        company_profile.get("sector_raw"),
        company_profile.get("company_name"),
        company_profile.get("short_name"),
    ]
    profile = _profile_from_texts(texts)
    if profile is None:
        return None
    if profile == PROFILE_SECURITIES:
        broker_scope = resolve_listed_broker_dealer_scope(company_profile)
        if not broker_scope.eligible:
            return FinancialStatementProfileResolution(
                profile=PROFILE_NONBANK,
                confidence="medium",
                source="company_profile",
                reason="securities_candidate_without_confirmed_broker_scope",
                evidence={
                    "industry_raw": company_profile.get("industry_raw"),
                    "sector_raw": company_profile.get("sector_raw"),
                    "company_name": company_profile.get("company_name"),
                    "short_name": company_profile.get("short_name"),
                    "listed_broker_dealer_scope": broker_scope.to_dict(),
                },
            )
    return FinancialStatementProfileResolution(
        profile=profile,
        confidence="medium",
        source="company_profile",
        reason="raw_company_profile_matches_financial_statement_profile",
        evidence={
            "industry_raw": company_profile.get("industry_raw"),
            "sector_raw": company_profile.get("sector_raw"),
            "company_name": company_profile.get("company_name"),
            "short_name": company_profile.get("short_name"),
        },
    )


def _resolve_from_instrument(
    instrument: Optional[Mapping[str, Any]],
) -> Optional[FinancialStatementProfileResolution]:
    if not instrument or not isinstance(instrument, Mapping):
        return None
    texts = [
        instrument.get("industry"),
        instrument.get("sector"),
        instrument.get("name"),
        instrument.get("short_name"),
    ]
    profile = _profile_from_texts(texts)
    if profile is None:
        return None
    if profile == PROFILE_SECURITIES:
        broker_scope = resolve_listed_broker_dealer_scope(instrument)
        if not broker_scope.eligible:
            return FinancialStatementProfileResolution(
                profile=PROFILE_NONBANK,
                confidence="low",
                source="instrument_metadata",
                reason="securities_candidate_without_confirmed_broker_scope",
                evidence={
                    "industry": instrument.get("industry"),
                    "sector": instrument.get("sector"),
                    "name": instrument.get("name"),
                    "short_name": instrument.get("short_name"),
                    "listed_broker_dealer_scope": broker_scope.to_dict(),
                },
            )
    return FinancialStatementProfileResolution(
        profile=profile,
        confidence="low",
        source="instrument_metadata",
        reason="instrument_metadata_matches_financial_statement_profile",
        evidence={
            "industry": instrument.get("industry"),
            "sector": instrument.get("sector"),
            "name": instrument.get("name"),
            "short_name": instrument.get("short_name"),
        },
    )


def _normalize_profile(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    text = str(raw).strip().lower()
    aliases = {
        "other": PROFILE_NONBANK,
        "general": PROFILE_NONBANK,
        "ordinary": PROFILE_NONBANK,
        "普通非银": PROFILE_NONBANK,
        "其他": PROFILE_NONBANK,
        "银行": PROFILE_BANK,
        "证券": PROFILE_SECURITIES,
        "券商": PROFILE_SECURITIES,
        "保险": PROFILE_INSURANCE,
    }
    text = aliases.get(text, text)
    if text not in KNOWN_FINANCIAL_STATEMENT_PROFILES:
        raise ValueError(f"Unsupported financial statement profile: {raw}")
    return text


def _industry_texts(industry_membership: Mapping[str, Any]) -> list[Any]:
    return [
        industry_membership.get("sw_l1_name"),
        industry_membership.get("sw_l2_name"),
        industry_membership.get("sw_l3_name"),
        industry_membership.get("industry_name"),
        industry_membership.get("source_industry_name"),
        industry_membership.get("source_classification"),
        industry_membership.get("industry_code"),
        industry_membership.get("sw_l1_code"),
        industry_membership.get("sw_l2_code"),
        industry_membership.get("sw_l3_code"),
    ]


def _profile_from_texts(values: list[Any]) -> Optional[str]:
    text = " ".join(str(value) for value in values if value is not None)
    if not text.strip():
        return None
    if "银行" in text or "bank" in text.lower() or "480" in text:
        return PROFILE_BANK
    if "证券" in text or "券商" in text or "490101" in text:
        return PROFILE_SECURITIES
    if "保险" in text or "490201" in text:
        return PROFILE_INSURANCE
    return None


def _compact_industry_evidence(industry_membership: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "taxonomy_system",
        "taxonomy_version",
        "industry_code",
        "industry_name",
        "sw_l1_name",
        "sw_l2_name",
        "sw_l3_name",
        "mapping_status",
        "source",
        "source_mode",
    )
    return {key: industry_membership.get(key) for key in keys if key in industry_membership}
