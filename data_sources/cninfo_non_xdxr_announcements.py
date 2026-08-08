"""Frozen operator decisions for CNInfo announcements that are not XDXR."""

from __future__ import annotations

from hashlib import sha256
import json
from typing import Any, Dict, Mapping, Sequence
import unicodedata


POLICY_VERSION = "cninfo_non_xdxr_operator_decision_v1"
REVIEWER = "operator_cninfo_non_xdxr_20260808"
EXPECTED_DECISION_COUNT = 2
EXPECTED_MANIFEST_HASH = (
    "c07fe5b001dd5b14ecb0ed07f45554da"
    "8087048e9563971a38922a0e5fff1db6"
)

FROZEN_DECISIONS: tuple[Dict[str, str], ...] = (
    {
        "announcement_key": "cninfo:1225459113",
        "source_announcement_id": "1225459113",
        "instrument_id": "000652.SZ",
        "expected_title": (
            "关于控股子公司泰达环保实施市场化债转股的进展公告"
        ),
        "decision": "non_xdxr",
        "decision_basis": (
            "subsidiary_market_debt_to_equity_no_listed_share_capital_change"
        ),
        "reviewer": REVIEWER,
        "approved_at": "2026-08-08",
    },
    {
        "announcement_key": "cninfo:1225461628",
        "source_announcement_id": "1225461628",
        "instrument_id": "603169.SH",
        "expected_title": (
            "兰石重装关于收到业绩补偿款暨业绩承诺履行完毕的公告"
        ),
        "decision": "non_xdxr",
        "decision_basis": (
            "cash_performance_compensation_no_listed_share_capital_change"
        ),
        "reviewer": REVIEWER,
        "approved_at": "2026-08-08",
    },
)


def normalize_announcement_title(value: Any) -> str:
    """Normalize title identity without weakening its semantic content."""
    normalized = unicodedata.normalize("NFKC", str(value or "")).strip()
    return "".join(normalized.split())


def decision_manifest_hash(
    decisions: Sequence[Mapping[str, Any]] = FROZEN_DECISIONS,
) -> str:
    """Return a stable hash for the complete frozen decision manifest."""
    payload = json.dumps(
        [dict(item) for item in decisions],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def validate_decision_manifest(
    decisions: Sequence[Mapping[str, Any]] = FROZEN_DECISIONS,
) -> str:
    """Reject incomplete, duplicate, or modified operator decisions."""
    if len(decisions) != EXPECTED_DECISION_COUNT:
        raise RuntimeError("CNInfo non-XDXR decision count drifted")
    identities = {
        (
            str(item.get("announcement_key") or "").strip(),
            str(item.get("instrument_id") or "").strip(),
        )
        for item in decisions
    }
    if len(identities) != EXPECTED_DECISION_COUNT or any(
        not announcement_key or not instrument_id
        for announcement_key, instrument_id in identities
    ):
        raise RuntimeError("CNInfo non-XDXR decision identity drifted")
    manifest_hash = decision_manifest_hash(decisions)
    if manifest_hash != EXPECTED_MANIFEST_HASH:
        raise RuntimeError("CNInfo non-XDXR decision manifest drifted")
    return manifest_hash


def resolve_non_xdxr_announcement_decision(
    *,
    announcement_key: Any,
    instrument_id: Any,
    title: Any,
) -> Dict[str, Any]:
    """Resolve one exact decision while keeping identity drift conservative."""
    validate_decision_manifest()
    normalized_key = str(announcement_key or "").strip()
    normalized_instrument = str(instrument_id or "").strip()
    normalized_title = normalize_announcement_title(title)
    decision = next(
        (
            dict(item)
            for item in FROZEN_DECISIONS
            if str(item.get("announcement_key") or "").strip()
            == normalized_key
        ),
        None,
    )
    if decision is None:
        return {
            "matched": False,
            "decision_found": False,
            "reason": "no_operator_decision",
            "mismatches": [],
            "policy_version": POLICY_VERSION,
            "decision": None,
        }
    mismatches = []
    if str(decision["instrument_id"]) != normalized_instrument:
        mismatches.append("instrument_id")
    if normalize_announcement_title(
        decision["expected_title"]
    ) != normalized_title:
        mismatches.append("title")
    if mismatches:
        return {
            "matched": False,
            "decision_found": True,
            "reason": "operator_decision_identity_mismatch",
            "mismatches": mismatches,
            "policy_version": POLICY_VERSION,
            "decision": decision,
        }
    return {
        "matched": True,
        "decision_found": True,
        "reason": "operator_verified_non_xdxr",
        "mismatches": [],
        "policy_version": POLICY_VERSION,
        "decision": decision,
    }
