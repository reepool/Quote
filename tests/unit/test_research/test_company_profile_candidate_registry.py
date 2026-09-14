from __future__ import annotations

from research.company_profile.candidate_registry import (
    CANDIDATE_REGISTRY_SCHEMA_VERSION,
    PRODUCTION_SCOPE_POLICY,
    build_a_share_candidate_registry,
    candidate_registry_schema_manifest,
    load_a_share_candidate_registry,
)


def test_registry_keeps_full_market_and_missing_status_in_denominator():
    registry = build_a_share_candidate_registry(
        as_of="2026-09-14",
        universe_snapshot_id="snap-full-a",
        universe_policy_version="a_share_active.v1",
        eligible_instruments=(
            {
                "instrument_id": "601398.SH",
                "exchange": "SSE",
                "name": "工商银行",
                "listed_date": "2006-10-27",
            },
            {
                "instrument_id": "000717.SZ",
                "exchange": "SZSE",
                "name": "中南钢铁",
                "listed_date": "1997-05-08",
            },
            {
                "instrument_id": "300750.SZ",
                "exchange": "SZSE",
                "name": "宁德时代",
            },
        ),
        indeterminate=(
            {
                "instrument_id": "900901.SH",
                "exchange": "SSE",
                "name": "B股样本",
                "reason": "b_share",
            },
        ),
        asset_coverage={
            "601398.SH": {
                "status": "available",
                "fiscal_year": 2025,
                "evidence": {},
            },
            "000717.SZ": {
                "status": "incomplete",
                "evidence": {"coverage_blocker": "download_pending"},
            },
        },
        industry_memberships={
            "601398.SH": {
                "sw_l1_name": "银行",
                "taxonomy_system": "sw",
                "mapping_status": "authoritative",
            },
            "000717.SZ": {
                "sw_l1_name": "钢铁",
                "taxonomy_system": "sw",
            },
        },
        effective_reports={
            "601398.SH": {
                "asset_id": "asset-icbc-2025",
                "fiscal_year": 2025,
                "report_period": "2025-12-31",
                "content_hash": "abc",
                "availability": "local_valid",
                "decision_state": "effective",
                "published_at": "2026-03-31T00:00:00+00:00",
            }
        },
        excluded_instrument_ids=frozenset({"300750.SZ", "000717.SZ"}),
        allowed_industry_groups=frozenset({"coal", "steel"}),
    )

    ids = [item.instrument_id for item in registry.candidates]
    assert ids == ["000717.SZ", "300750.SZ", "601398.SH", "900901.SH"]
    assert registry.schema_version == CANDIDATE_REGISTRY_SCHEMA_VERSION
    assert registry.policy_version == PRODUCTION_SCOPE_POLICY
    assert registry.production_authorization == "not_authorized"

    bank = registry.candidate("601398.SH")
    assert bank.universe_status == "eligible"
    assert bank.exclusion_reason is None
    assert bank.classification_status == "present"
    assert bank.classification is not None
    assert bank.classification.sw_l1_name == "银行"
    assert bank.asset_status == "available"
    assert bank.latest_effective_annual_report is not None
    assert bank.latest_effective_annual_report.fiscal_year == 2025

    steel = registry.candidate("000717.SZ")
    assert steel.universe_status == "eligible"
    assert steel.classification_status == "present"
    assert steel.asset_status == "incomplete"
    assert steel.asset_blocker == "download_pending"
    assert steel.latest_effective_annual_report is None

    catl = registry.candidate("300750.SZ")
    assert catl.universe_status == "eligible"
    assert catl.classification_status == "missing"
    assert catl.classification is None
    assert catl.asset_status == "not_covered"
    assert catl.exclusion_reason is None

    b_share = registry.candidate("900901.SH")
    assert b_share.universe_status == "indeterminate"
    assert b_share.exclusion_reason == "b_share"

    assert registry.counts["total"] == 4
    assert registry.counts["eligible"] == 3
    assert registry.counts["indeterminate"] == 1
    assert registry.counts["classification_missing"] == 2
    assert registry.counts["asset_available"] == 1
    assert registry.counts["asset_not_available"] == 3


def test_loader_uses_existing_snapshot_and_does_not_call_shadow_filters():
    class _Universe:
        def __init__(self) -> None:
            self.coverage_calls: list[str] = []

        def get_latest_full_market_universe_snapshot(self):
            return {
                "snapshot_id": "snap-db",
                "policy_version": "a_share_active.v1",
                "instrument_rows": {
                    "items": [
                        {
                            "instrument_id": "600000.SH",
                            "exchange": "SSE",
                            "name": "浦发银行",
                        }
                    ]
                },
                "indeterminate_rows": {"items": []},
            }

        def get_latest_complete_universe_snapshot(self):
            raise AssertionError("full-market snapshot already present")

        def list_asset_coverage(self, universe_snapshot_id: str):
            self.coverage_calls.append(universe_snapshot_id)
            return [{"instrument_id": "600000.SH", "status": "retryable"}]

    def industry_lookup(instrument_id: str, as_of: str):
        assert as_of == "2026-09-14"
        assert instrument_id == "600000.SH"
        return {"official_industry_code": "480000", "taxonomy_system": "sw"}

    def report_lookup(instrument_id: str):
        assert instrument_id == "600000.SH"

    def first_wave_forbidden(*_args, **_kwargs):
        raise AssertionError("must not reuse manufacturing first-wave universe")

    def shadow_exclusion_forbidden(*_args, **_kwargs):
        raise AssertionError("must not reuse OOS/shadow exclusion lists")

    universe = _Universe()
    registry = load_a_share_candidate_registry(
        universe_repository=universe,
        industry_lookup=industry_lookup,
        effective_report_lookup=report_lookup,
        as_of="2026-09-14",
        first_wave_universe=first_wave_forbidden,
        shadow_exclusions=shadow_exclusion_forbidden,
    )
    assert registry.universe_snapshot_id == "snap-db"
    assert universe.coverage_calls == ["snap-db"]
    assert [item.instrument_id for item in registry.candidates] == ["600000.SH"]
    assert registry.candidate("600000.SH").classification_status == "present"
    assert registry.candidate("600000.SH").asset_status == "retryable"


def test_loader_requires_a_universe_snapshot():
    class _Empty:
        def get_latest_full_market_universe_snapshot(self):
            return None

        def get_latest_complete_universe_snapshot(self):
            return None

    try:
        load_a_share_candidate_registry(
            universe_repository=_Empty(),
            industry_lookup=lambda *_args, **_kwargs: None,
            effective_report_lookup=lambda *_args, **_kwargs: None,
            as_of="2026-09-14",
        )
    except ValueError as exc:
        assert "universe snapshot" in str(exc)
    else:
        raise AssertionError("expected missing snapshot to fail closed")


def test_schema_manifest_is_registered_before_use():
    manifest = candidate_registry_schema_manifest()
    assert manifest["schema_version"] == CANDIDATE_REGISTRY_SCHEMA_VERSION
    assert "AShareCandidateRegistry" in manifest["title"]
    assert "candidates" in manifest["properties"]
