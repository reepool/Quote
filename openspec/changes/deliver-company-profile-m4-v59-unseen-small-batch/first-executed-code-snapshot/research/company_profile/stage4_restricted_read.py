"""Read-only projection of the frozen Stage 4 chapter bundles.

Query uses this module through CompanyProfileReadService. It does not write
runtime records, replay artifacts, or publication control.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from research.company_profile.models import PRODUCTION_AUTHORIZATION

SCALE_QUALITY_CLAIM_ALLOWED = False
REPORT_PERIOD = "2025-12-31"

FROZEN_REPORTS: dict[str, dict[str, str]] = {
    "300750.SZ": {
        "report_id": "asset_3b09f6c831975c7177b6bb3287cab781",
        "document_version": "ver_09c0e677ec8192dc4fc12cb620069f29",
        "content_hash": (
            "c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9"
        ),
    },
    "603659.SH": {
        "report_id": "asset_50c70429093f66b34fc57ad8f896fcee",
        "document_version": "ver_c867a6a692048e88fd9cb80473fbf908",
        "content_hash": (
            "4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6"
        ),
    },
    "920015.BJ": {
        "report_id": "asset_b87f1d1a48e662dae376c540cd021f69",
        "document_version": "ver_cfdbd2d058af825b1fc39f494d7a9bd3",
        "content_hash": (
            "4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a"
        ),
    },
    "302132.SZ": {
        "report_id": "asset_0a488da55636b09107be6d719c9ebf39",
        "document_version": "ver_2d20ba3aebc5fac6c562cd619695995a",
        "content_hash": (
            "605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020"
        ),
    },
}

_CORE_GAPS = (
    {
        "chapter_task": "extract_business_overview",
        "reason": "not_in_this_delivery",
    },
    {
        "chapter_task": "extract_business_regime",
        "reason": "not_in_this_delivery",
    },
)


@dataclass(frozen=True)
class FrozenChapterArtifacts:
    """One current chapter and the four artifact hashes that bind it."""

    chapter_task: str
    plan_version: str
    replay_dir: str
    result_name: str
    enqueue_sha256: str
    run_sha256: str
    result_sha256: str
    source_review_sha256: str


FROZEN_CHAPTERS: tuple[FrozenChapterArtifacts, ...] = (
    FrozenChapterArtifacts(
        chapter_task="extract_material_inputs",
        plan_version="manufacturing_materials_stage4_material_inputs.2026-09-30.3",
        replay_dir=(
            "openspec/changes/archive/"
            "2026-09-30-scope-manufacturing-materials-stage4-material-input-"
            "four-report-successor/replay/20260930"
        ),
        result_name="material-input-stage4-material-inputs-20260930/result.json",
        enqueue_sha256=(
            "09edffece890b126e33679a70b19800c8ad6b4fe094efe2aebf1793b168d15f8"
        ),
        run_sha256=("00aa0f4d5bacbf0ff914b9cc7fc3a9740e6b60fa08a5c7c693699f5b77d1714e"),
        result_sha256=(
            "2e4b795206b308caa5df6be9ff03c5dd8b8b864b734414faffe6e1df35ad0ed4"
        ),
        source_review_sha256=(
            "b1346b1547b91f34c48aab1d4060062904596f534786b276cef59686bb9c6585"
        ),
    ),
    FrozenChapterArtifacts(
        chapter_task="extract_operating_quantities",
        plan_version="manufacturing_materials_stage4_operating_quantities.2026-10-01.4",
        replay_dir=(
            "openspec/changes/archive/"
            "2026-10-01-scope-stage4-operating-quantity-successor-after-failed-"
            "observations/replay/20261001"
        ),
        result_name=(
            "operating-quantity-stage4-operating-quantities-20261001/result.json"
        ),
        enqueue_sha256=(
            "6f2363a463627a5e233a5f76e094577294b7276de90bd51db61598967e706e52"
        ),
        run_sha256=("977939ba5ceabe8da0238f594a284cdc6352edca5bafd9cfae8e4021d11a8788"),
        result_sha256=(
            "6ccd40a07803ac7411631729a1abac69debd3d10119f80260a5731e73f747add"
        ),
        source_review_sha256=(
            "1a5c4753d0a3b407c84a188d68b98258e764e9ef463293ed5bdac0d0fcff8cdf"
        ),
    ),
    FrozenChapterArtifacts(
        chapter_task="extract_segment_financials",
        plan_version="manufacturing_materials_stage4_segment_financials.2026-10-01.4",
        replay_dir=(
            "openspec/changes/archive/"
            "2026-10-01-scope-stage4-segment-financial-successor-after-failed-"
            "observations/replay/20261001"
        ),
        result_name="segment-financial-stage4-segment-financials-20261001/result.json",
        enqueue_sha256=(
            "8c2c40e06848563723e208ca4d60d0310804ff986fa7ae86b50aecc7200d3bfa"
        ),
        run_sha256=("8f007a464e89adfd9bfcf80d810c27d47ce321e885d207e5faedfe6ecf897b90"),
        result_sha256=(
            "6c5188ef7dc43c45bb69c23478d2dbc3bd19e9e657ca3afb674ad2494c9acc59"
        ),
        source_review_sha256=(
            "0a38ac4c78d65dacd756cf310ea1033e9cd1e40e793a6e0a5a5ac256030dcfbd"
        ),
    ),
    FrozenChapterArtifacts(
        chapter_task="extract_counterparties_and_concentration",
        plan_version="manufacturing_materials_stage4_counterparties.2026-09-29.1",
        replay_dir=(
            "openspec/changes/archive/"
            "2026-09-29-scope-manufacturing-materials-stage4-counterparties-and-"
            "concentration/replay/20260929"
        ),
        result_name="counterparty-stage4-counterparties-20260929/result.json",
        enqueue_sha256=(
            "7e9c1a72f11723f2d8508d751c27f8ea3f96cae048eb0ab6edc6224eb301bf7a"
        ),
        run_sha256=("8d3c7bd7a6c1f81a2e2b76064eb3d7fe9685d7570442a1e4d113b786a4cf0d7f"),
        result_sha256=(
            "b0344d3d577f423a1716b6dacc7c46b100ef3fe9ddddb5d37ef0160c2458a57a"
        ),
        source_review_sha256=(
            "b2c38f9fa1cf947ab69e542606332689ac25ea1e6df63facc50ac6a1fa56baf4"
        ),
    ),
)


def default_repo_root() -> Path:
    """Return the Quote repository root that holds the archived replays."""

    return Path(__file__).resolve().parents[2]


def resolved_replay_dirs(
    repo_root: Path,
    chapters: Sequence[FrozenChapterArtifacts] = FROZEN_CHAPTERS,
) -> tuple[Path, ...]:
    """Return each archive ``replay`` root, including later sibling directories."""

    resolved: list[Path] = []
    seen: set[Path] = set()
    for binding in chapters:
        replay = Path(binding.replay_dir)
        if not replay.is_absolute():
            replay = repo_root / replay
        root = _replay_root(replay)
        if root not in seen:
            seen.add(root)
            resolved.append(root)
    return tuple(resolved)


def _replay_root(path: Path) -> Path:
    resolved = path.resolve()
    if resolved.name == "replay":
        return resolved
    if resolved.parent.name == "replay":
        return resolved.parent
    for candidate in resolved.parents:
        if candidate.name == "replay":
            return candidate
    return resolved


def project_stage4_restricted_views(
    instrument_ids: Sequence[str] = (),
    *,
    repo_root: Path,
    chapters: Sequence[FrozenChapterArtifacts],
) -> list[dict[str, Any]]:
    """Project the chapter bundles the caller explicitly bound.

    An empty instrument list returns the four frozen companies. Companies
    outside that set are omitted. Artifact failures reject only that chapter.
    """

    selected = _selected_instruments(instrument_ids)
    if not selected or not chapters:
        return []
    loaded = [_load_chapter(repo_root, binding) for binding in chapters]
    return [_view_for(instrument_id, loaded) for instrument_id in selected]


def _selected_instruments(instrument_ids: Sequence[str]) -> list[str]:
    requested = [str(item).strip() for item in instrument_ids if str(item).strip()]
    if not requested:
        return list(FROZEN_REPORTS)
    return [item for item in requested if item in FROZEN_REPORTS]


def _load_chapter(
    root: Path,
    binding: FrozenChapterArtifacts,
) -> tuple[FrozenChapterArtifacts, dict[str, Any] | None, str | None]:
    paths = _artifact_paths(root, binding)
    for name, path in paths.items():
        try:
            if not path.is_file():
                return binding, None, f"missing_artifact:{name}"
            actual = _sha256(path)
        except OSError:
            return binding, None, f"unreadable_artifact:{name}"
        if actual != _expected_hash(binding, name):
            return binding, None, f"hash_mismatch:{name}"
    try:
        payload = json.loads(paths["result"].read_text(encoding="utf-8"))
    except OSError:
        return binding, None, "unreadable_artifact:result"
    except json.JSONDecodeError:
        return binding, None, "unreadable_result"
    if not isinstance(payload, dict):
        return binding, None, "unreadable_result"
    if payload.get("plan_version") != binding.plan_version:
        return binding, None, "plan_mismatch"
    if payload.get("chapter_task") != binding.chapter_task:
        return binding, None, "chapter_mismatch"
    return binding, payload, None


def _artifact_paths(
    root: Path,
    binding: FrozenChapterArtifacts,
) -> dict[str, Path]:
    replay = Path(binding.replay_dir)
    if not replay.is_absolute():
        replay = root / replay
    return {
        "enqueue": replay / "enqueue.json",
        "run": replay / "run.json",
        "result": replay / binding.result_name,
        "source_review": replay / "source_review.json",
    }


def _expected_hash(binding: FrozenChapterArtifacts, name: str) -> str:
    return {
        "enqueue": binding.enqueue_sha256,
        "run": binding.run_sha256,
        "result": binding.result_sha256,
        "source_review": binding.source_review_sha256,
    }[name]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _view_for(
    instrument_id: str,
    loaded: Sequence[tuple[FrozenChapterArtifacts, dict[str, Any] | None, str | None]],
) -> dict[str, Any]:
    identity = FROZEN_REPORTS[instrument_id]
    return {
        "instrument_id": instrument_id,
        "report": {
            "instrument_id": instrument_id,
            "report_id": identity["report_id"],
            "document_version": identity["document_version"],
            "content_hash": identity["content_hash"],
            "report_period": REPORT_PERIOD,
        },
        "core_profile_complete": False,
        "gaps": [dict(item) for item in _CORE_GAPS],
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "scale_quality_claim_allowed": SCALE_QUALITY_CLAIM_ALLOWED,
        "chapters": [
            _chapter_view(instrument_id, binding, payload, reason)
            for binding, payload, reason in loaded
        ],
    }


def _chapter_view(
    instrument_id: str,
    binding: FrozenChapterArtifacts,
    payload: Mapping[str, Any] | None,
    reason: str | None,
) -> dict[str, Any]:
    header = {
        "chapter_task": binding.chapter_task,
        "plan_version": binding.plan_version,
    }
    if reason is not None or payload is None:
        return {
            **header,
            "delivered": False,
            "reason": reason or "missing_artifact:result",
            "facts": [],
            "coverage": [],
        }
    report = _matching_report(payload, instrument_id)
    if report is None:
        return {
            **header,
            "delivered": False,
            "reason": "report_identity_mismatch",
            "facts": [],
            "coverage": [],
        }
    sample_id = str(report.get("sample_id") or "")
    return {
        **header,
        "delivered": True,
        "reason": None,
        "facts": _matching_rows(payload.get("facts") or (), sample_id, instrument_id),
        "coverage": _matching_rows(
            _coverage_rows(payload),
            sample_id,
            instrument_id,
        ),
    }


def _coverage_rows(payload: Mapping[str, Any]) -> list[Any]:
    if "coverage" in payload:
        return list(payload.get("coverage") or ())
    return list(payload.get("scope_outcomes") or ())


def _matching_report(
    payload: Mapping[str, Any],
    instrument_id: str,
) -> Mapping[str, Any] | None:
    identity = FROZEN_REPORTS[instrument_id]
    for report in payload.get("reports") or ():
        if not isinstance(report, Mapping):
            continue
        if report.get("instrument_id") != instrument_id:
            continue
        if report.get("report_id") != identity["report_id"]:
            continue
        if report.get("document_version") != identity["document_version"]:
            continue
        if report.get("content_hash") != identity["content_hash"]:
            continue
        return report
    return None


def _matching_rows(
    rows: Sequence[Any],
    sample_id: str,
    instrument_id: str,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("sample_id") or "") != sample_id:
            continue
        if not _row_matches_frozen_identity(row, instrument_id):
            continue
        selected.append(dict(row))
    return selected


def _row_matches_frozen_identity(row: Mapping[str, Any], instrument_id: str) -> bool:
    identity = FROZEN_REPORTS[instrument_id]
    nested = row.get("report")
    if isinstance(nested, Mapping):
        if not _field_matches(nested.get("instrument_id"), instrument_id):
            return False
        if not _field_matches(nested.get("report_id"), identity["report_id"]):
            return False
        if not _field_matches(
            nested.get("document_version"),
            identity["document_version"],
        ):
            return False
        if not _field_matches(nested.get("report_period"), REPORT_PERIOD):
            return False
    if not _field_matches(row.get("report_id"), identity["report_id"]):
        return False
    if not _field_matches(row.get("document_version"), identity["document_version"]):
        return False
    period = row.get("report_period") or row.get("reported_period")
    return _field_matches(period, REPORT_PERIOD)


def _field_matches(actual: Any, expected: str) -> bool:
    return actual in (None, "", expected)
