"""Business-profile projections over authoritative shared announcement assets."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from research.announcement_assets import EnsureRequest


BUSINESS_PROFILE_SOURCE_ASSET_SCHEMA_VERSION = "business_profile_source_asset.v1"
BUSINESS_PROFILE_USABLE_SOURCE_ASSET_STATUSES = frozenset({"verified"})


def project_business_profile_source_asset(
    access: Any,
    asset: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify one shared asset and project the fields used by profile parsing."""

    asset_id = str(asset.get("asset_id") or "").strip()
    if not asset_id:
        raise ValueError("business-profile shared asset id is required")
    content = access.content_handle(asset_id)
    handle = content.get("file_handle")
    try:
        return {
            "schema_version": BUSINESS_PROFILE_SOURCE_ASSET_SCHEMA_VERSION,
            "source_file_id": f"shared-asset:{asset_id}",
            "source_asset_id": asset_id,
            "instrument_id": str(asset.get("instrument_id") or ""),
            "report_period": str(asset.get("report_period") or ""),
            "report_type": (
                "annual_report_correction"
                if bool(asset.get("is_correction"))
                else "annual_report"
            ),
            "filing_id": str(asset.get("source_announcement_id") or ""),
            "source": str(asset.get("source") or "").lower(),
            "archive_path": str(content["path"]),
            "content_hash": str(asset.get("content_hash") or "").lower(),
            "content_length": int(content["content_length"]),
            "published_at": asset.get("published_at"),
            "status": "verified",
            "integrity_status": "valid",
            "metadata": {
                "shared_asset_id": asset_id,
                "shared_attachment_id": asset.get("attachment_id"),
                "shared_observation_version": asset.get("observation_version"),
                "selector_kind": "shared_effective_asset",
            },
        }
    finally:
        if handle is not None:
            handle.close()


def project_bound_business_profile_source_asset(
    access: Any,
    asset: Mapping[str, Any],
    *,
    knowledge_cutoff: str | None = None,
) -> dict[str, Any]:
    """Project the immutable observation frozen into a business-profile work item."""

    required = {
        key: str(asset.get(key) or "").strip()
        for key in (
            "asset_id",
            "instrument_id",
            "report_period",
            "source",
            "source_announcement_id",
            "attachment_id",
            "observation_version",
            "content_hash",
        )
    }
    missing = sorted(key for key, value in required.items() if not value)
    if missing:
        raise ValueError(
            "business-profile bound shared asset is incomplete: " + ",".join(missing)
        )
    content = access.exact_observation_handle(
        EnsureRequest(
            instrument_id=required["instrument_id"],
            source=required["source"],
            source_announcement_id=required["source_announcement_id"],
            attachment_id=required["attachment_id"],
            observation_version=required["observation_version"],
            expected_content_hash=required["content_hash"],
            knowledge_cutoff=knowledge_cutoff,
            allow_network=False,
            consumer="business_profile",
            principal="business-profile",
        ),
        authorized=True,
    )
    handle = content.get("file_handle")
    try:
        return {
            "schema_version": BUSINESS_PROFILE_SOURCE_ASSET_SCHEMA_VERSION,
            "source_file_id": f"shared-asset:{required['asset_id']}",
            "source_asset_id": required["asset_id"],
            "instrument_id": required["instrument_id"],
            "report_period": required["report_period"],
            "report_type": (
                "annual_report_correction"
                if bool(asset.get("is_correction"))
                else "annual_report"
            ),
            "filing_id": required["source_announcement_id"],
            "source": required["source"].lower(),
            "archive_path": str(content["path"]),
            "content_hash": required["content_hash"].lower(),
            "content_length": int(content["content_length"]),
            "published_at": asset.get("published_at"),
            "status": "verified",
            "integrity_status": "valid",
            "metadata": {
                "shared_asset_id": required["asset_id"],
                "shared_attachment_id": required["attachment_id"],
                "shared_observation_version": required["observation_version"],
                "selector_kind": "bound_exact_observation",
            },
        }
    finally:
        if handle is not None:
            handle.close()


def load_business_profile_source_assets(
    access: Any,
    instrument_id: str,
    *,
    knowledge_cutoff: str | None = None,
    source_file_ids: Sequence[str] = (),
    verify_content: bool = True,
) -> tuple[dict[str, Any], ...]:
    """Load verified effective annual reports for one profile consumer scope."""

    requested = {
        str(item).removeprefix("shared-asset:")
        for item in source_file_ids
        if str(item).strip()
    }
    if requested:
        assets = [access.get_asset(asset_id) for asset_id in sorted(requested)]
    else:
        assets = []
        offset = 0
        while True:
            page = access.list_effective_assets(
                instrument_id=instrument_id,
                document_family="annual_report",
                knowledge_cutoff=knowledge_cutoff,
                availability="local_valid",
                limit=1000,
                offset=offset,
            )
            items = list(page.get("items") or ())
            assets.extend(items)
            if len(items) < 1000:
                break
            offset += len(items)
    projected: list[dict[str, Any]] = []
    for asset in assets:
        if asset is None or str(asset.get("instrument_id") or "") != instrument_id:
            continue
        if str(asset.get("availability") or "") != "local_valid":
            continue
        if verify_content:
            projected.append(
                project_bound_business_profile_source_asset(
                    access,
                    asset,
                    knowledge_cutoff=knowledge_cutoff,
                )
            )
        else:
            projected.append(
                {
                    "schema_version": BUSINESS_PROFILE_SOURCE_ASSET_SCHEMA_VERSION,
                    "source_file_id": f"shared-asset:{asset['asset_id']}",
                    "source_asset_id": asset["asset_id"],
                    "instrument_id": asset["instrument_id"],
                    "report_period": asset["report_period"],
                    "report_type": (
                        "annual_report_correction"
                        if bool(asset.get("is_correction"))
                        else "annual_report"
                    ),
                    "filing_id": asset["source_announcement_id"],
                    "source": str(asset.get("source") or "").lower(),
                    "archive_path": None,
                    "content_hash": asset.get("content_hash"),
                    "content_length": asset.get("content_length"),
                    "published_at": asset.get("published_at"),
                    "status": "verified",
                    "integrity_status": asset.get("integrity"),
                    "metadata": {"shared_asset_id": asset["asset_id"]},
                }
            )
    return tuple(projected)
