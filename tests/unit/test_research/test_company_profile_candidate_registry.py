from __future__ import annotations

from research.announcement_assets.models import (
    AnnualReportVariant,
    AssetAvailability,
    EffectiveAnnualReport,
    EffectiveDecisionState,
)
from research.announcement_assets.repository import _snapshot_at_not_after
from research.company_profile.candidate_registry import (
    CANDIDATE_REGISTRY_SCHEMA_VERSION,
    PRODUCTION_SCOPE_POLICY,
    announcement_access_report_lookup,
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
                "snapshot_at": "2026-09-14T00:00:00+00:00",
                "paired_census_snapshot_id": "census-db",
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
    assert registry.universe_coverage_guarantee == "full_market"
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


def _access_projection(*, published_at: str) -> dict[str, object]:
    return {
        "asset_id": "asset-icbc-2025",
        "instrument_id": "601398.SH",
        "fiscal_year": 2025,
        "report_period": "2025-12-31",
        "content_hash": "abc",
        "availability": "local_valid",
        "asset_availability": "local_valid",
        "effective_state": "current",
        "effective_decision_state": "current",
        "published_at": published_at,
        "version_available_at": published_at,
        "activated_at": published_at,
    }


def test_access_projection_shape_binds_effective_report():
    registry = build_a_share_candidate_registry(
        as_of="2026-09-14",
        universe_snapshot_id="snap-full-a",
        universe_policy_version="a_share_active.v1",
        eligible_instruments=(
            {
                "instrument_id": "601398.SH",
                "exchange": "SSE",
                "name": "工商银行",
            },
        ),
        asset_coverage={"601398.SH": {"status": "available"}},
        effective_reports={
            "601398.SH": _access_projection(published_at="2026-03-31T00:00:00+00:00")
        },
    )
    report = registry.candidate("601398.SH").latest_effective_annual_report
    assert report is not None
    assert report.decision_state == "current"
    assert report.availability == "local_valid"
    assert report.fiscal_year == 2025


def test_repository_date_only_as_of_uses_shanghai_end_of_day():
    assert _snapshot_at_not_after("2025-01-01T15:00:00Z", "2025-01-01") is True
    assert _snapshot_at_not_after("2025-01-01T18:00:00Z", "2025-01-01") is False
    try:
        _snapshot_at_not_after("not-a-timestamp", "2025-01-01")
    except ValueError:
        pass
    else:
        raise AssertionError("expected invalid snapshot_at to fail closed")


def test_date_only_as_of_uses_shanghai_end_of_day():
    after_shanghai_day = build_a_share_candidate_registry(
        as_of="2025-01-01",
        eligible_instruments=(
            {
                "instrument_id": "601398.SH",
                "exchange": "SSE",
                "name": "工商银行",
            },
        ),
        asset_coverage={"601398.SH": {"status": "available"}},
        effective_reports={
            "601398.SH": _access_projection(published_at="2025-01-01T18:00:00Z")
        },
    )
    assert after_shanghai_day.candidate("601398.SH").latest_effective_annual_report is None

    still_on_shanghai_day = build_a_share_candidate_registry(
        as_of="2025-01-01",
        eligible_instruments=(
            {
                "instrument_id": "601398.SH",
                "exchange": "SSE",
                "name": "工商银行",
            },
        ),
        asset_coverage={"601398.SH": {"status": "available"}},
        effective_reports={
            "601398.SH": _access_projection(published_at="2025-01-01T15:00:00Z")
        },
    )
    assert still_on_shanghai_day.candidate("601398.SH").latest_effective_annual_report is not None


def test_invalid_as_of_timestamp_fails_closed():
    try:
        build_a_share_candidate_registry(
            as_of="2025-01-01",
            eligible_instruments=(
                {
                    "instrument_id": "601398.SH",
                    "exchange": "SSE",
                    "name": "工商银行",
                },
            ),
            effective_reports={
                "601398.SH": _access_projection(published_at="not-a-timestamp")
            },
        )
    except ValueError as exc:
        assert "timestamp" in str(exc).lower() or "as_of" in str(exc).lower()
    else:
        raise AssertionError("expected invalid published_at to fail closed")


def test_loader_rejects_snapshot_after_shanghai_date_as_of():
    class _Universe:
        def get_latest_full_market_universe_snapshot(self):
            return {
                "snapshot_id": "snap-utc-evening",
                "policy_version": "a_share_active.v1",
                "snapshot_at": "2025-01-01T18:00:00Z",
                "paired_census_snapshot_id": "census-utc-evening",
                "instrument_rows": {
                    "items": [
                        {
                            "instrument_id": "601398.SH",
                            "exchange": "SSE",
                            "name": "工商银行",
                        }
                    ]
                },
            }

        def get_latest_complete_universe_snapshot(self):
            raise AssertionError("must not fall back after a post-cutoff snapshot")

        def list_asset_coverage(self, universe_snapshot_id: str):
            return []

    try:
        load_a_share_candidate_registry(
            universe_repository=_Universe(),
            industry_lookup=lambda *_args, **_kwargs: None,
            effective_report_lookup=lambda *_args, **_kwargs: None,
            as_of="2025-01-01",
        )
    except ValueError as exc:
        assert "as of 2025-01-01" in str(exc)
    else:
        raise AssertionError("expected 18:00Z snapshot to be after Shanghai 2025-01-01")


def test_future_published_report_is_rejected_at_historical_as_of():
    registry = build_a_share_candidate_registry(
        as_of="2025-01-01",
        universe_snapshot_id="snap-full-a",
        universe_policy_version="a_share_active.v1",
        eligible_instruments=(
            {
                "instrument_id": "601398.SH",
                "exchange": "SSE",
                "name": "工商银行",
            },
        ),
        asset_coverage={"601398.SH": {"status": "available"}},
        effective_reports={
            "601398.SH": _access_projection(published_at="2026-04-01T00:00:00+00:00")
        },
    )
    candidate = registry.candidate("601398.SH")
    assert candidate.asset_status == "available"
    assert candidate.latest_effective_annual_report is None


def test_loader_rejects_current_snapshot_labeled_as_historical_as_of():
    class _Universe:
        def get_latest_full_market_universe_snapshot(self):
            return {
                "snapshot_id": "snap-2026",
                "policy_version": "a_share_active.v1",
                "snapshot_at": "2026-04-01T00:00:00+00:00",
                "paired_census_snapshot_id": "census-2026",
                "instrument_rows": {
                    "items": [
                        {
                            "instrument_id": "601398.SH",
                            "exchange": "SSE",
                            "name": "工商银行",
                        }
                    ]
                },
            }

        def get_latest_complete_universe_snapshot(self):
            raise AssertionError("must not silently fall back after a future snapshot")

        def list_asset_coverage(self, universe_snapshot_id: str):
            return []

    try:
        load_a_share_candidate_registry(
            universe_repository=_Universe(),
            industry_lookup=lambda *_args, **_kwargs: None,
            effective_report_lookup=lambda *_args, **_kwargs: None,
            as_of="2025-01-01",
        )
    except ValueError as exc:
        assert "as of 2025-01-01" in str(exc)
    else:
        raise AssertionError("expected historical as_of without a dated snapshot to fail")


def test_loader_uses_as_of_snapshot_and_filters_later_reports():
    class _Universe:
        def get_latest_full_market_universe_snapshot(self):
            raise AssertionError("must not use the undated latest snapshot")

        def get_latest_complete_universe_snapshot(self):
            raise AssertionError("full-market as_of snapshot already present")

        def get_full_market_universe_snapshot_as_of(self, as_of: str):
            assert as_of == "2025-01-01"
            return {
                "snapshot_id": "snap-2024",
                "policy_version": "a_share_active.v1",
                "snapshot_at": "2024-12-31T00:00:00+00:00",
                "paired_census_snapshot_id": "census-2024",
                "instrument_rows": {
                    "items": [
                        {
                            "instrument_id": "601398.SH",
                            "exchange": "SSE",
                            "name": "工商银行",
                        }
                    ]
                },
            }

        def list_asset_coverage(self, universe_snapshot_id: str):
            assert universe_snapshot_id == "snap-2024"
            return [{"instrument_id": "601398.SH", "status": "available"}]

    lookup_calls: list[tuple[str, str]] = []

    def report_lookup(instrument_id: str, as_of: str):
        lookup_calls.append((instrument_id, as_of))
        return _access_projection(published_at="2026-04-01T00:00:00+00:00")

    registry = load_a_share_candidate_registry(
        universe_repository=_Universe(),
        industry_lookup=lambda *_args, **_kwargs: None,
        effective_report_lookup=report_lookup,
        as_of="2025-01-01",
    )
    assert registry.universe_snapshot_id == "snap-2024"
    assert registry.as_of == "2025-01-01"
    assert registry.universe_coverage_guarantee == "full_market"
    assert lookup_calls == [("601398.SH", "2025-01-01")]
    assert registry.candidate("601398.SH").latest_effective_annual_report is None


def test_complete_fallback_records_missing_full_market_guarantee():
    class _Universe:
        def get_latest_full_market_universe_snapshot(self):
            return None

        def get_latest_complete_universe_snapshot(self):
            return {
                "snapshot_id": "snap-complete",
                "policy_version": "a_share_active.v1",
                "snapshot_at": "2026-09-01T00:00:00+00:00",
                "instrument_rows": {
                    "items": [
                        {
                            "instrument_id": "600000.SH",
                            "exchange": "SSE",
                            "name": "浦发银行",
                        }
                    ]
                },
            }

        def list_asset_coverage(self, universe_snapshot_id: str):
            return []

    registry = load_a_share_candidate_registry(
        universe_repository=_Universe(),
        industry_lookup=lambda *_args, **_kwargs: None,
        effective_report_lookup=lambda *_args, **_kwargs: None,
        as_of="2026-09-14",
    )
    assert registry.universe_snapshot_id == "snap-complete"
    assert registry.universe_coverage_guarantee == "complete_unpaired"


def test_repository_effective_report_object_and_access_lookup_bind():
    report = EffectiveAnnualReport(
        asset_id="asset-icbc-2025",
        instrument_id="601398.SH",
        fiscal_year=2025,
        report_period="2025-12-31",
        announcement_id="ann-1",
        attachment_id="att-1",
        version_id="ver-1",
        content_hash="abc",
        source="cninfo",
        source_announcement_id="ann-1",
        published_at="2026-03-31T00:00:00+00:00",
        variant=AnnualReportVariant.ORIGINAL,
        classifier_version="v1",
        decision_state=EffectiveDecisionState.CURRENT,
        availability=AssetAvailability.LOCAL_VALID,
        predecessor_asset_id=None,
        pending_candidate_id=None,
        activated_at="2026-03-31T00:00:00+00:00",
        last_checked_at="2026-03-31T00:00:00+00:00",
    )

    class _Access:
        def get_effective_asset(self, instrument_id: str, **kwargs):
            assert instrument_id == "601398.SH"
            assert kwargs["knowledge_cutoff"].startswith("2026-09-14")
            return _access_projection(published_at="2026-03-31T00:00:00+00:00")

    built = build_a_share_candidate_registry(
        as_of="2026-09-14",
        eligible_instruments=(
            {
                "instrument_id": "601398.SH",
                "exchange": "SSE",
                "name": "工商银行",
            },
        ),
        effective_reports={"601398.SH": report},
    )
    assert built.candidate("601398.SH").latest_effective_annual_report is not None
    assert (
        built.candidate("601398.SH").latest_effective_annual_report.decision_state
        == "current"
    )

    class _Universe:
        def get_latest_full_market_universe_snapshot(self):
            return {
                "snapshot_id": "snap-db",
                "policy_version": "a_share_active.v1",
                "snapshot_at": "2026-09-14T00:00:00+00:00",
                "paired_census_snapshot_id": "census-db",
                "instrument_rows": {
                    "items": [
                        {
                            "instrument_id": "601398.SH",
                            "exchange": "SSE",
                            "name": "工商银行",
                        }
                    ]
                },
            }

        def get_latest_complete_universe_snapshot(self):
            raise AssertionError("full-market snapshot already present")

        def list_asset_coverage(self, universe_snapshot_id: str):
            return [{"instrument_id": "601398.SH", "status": "available"}]

    loaded = load_a_share_candidate_registry(
        universe_repository=_Universe(),
        industry_lookup=lambda *_args, **_kwargs: None,
        effective_report_lookup=announcement_access_report_lookup(_Access()),
        as_of="2026-09-14",
    )
    assert loaded.candidate("601398.SH").latest_effective_annual_report is not None
    assert (
        loaded.candidate("601398.SH").latest_effective_annual_report.decision_state
        == "current"
    )


def _official_one_name_registry(storage, instrument_id: str = "601888.SH"):
    from research.company_profile.candidate_registry import (
        load_official_task_candidate_registry,
    )

    class _Universe:
        def get_latest_full_market_universe_snapshot(self):
            return {
                "snapshot_id": "snap-l1",
                "policy_version": "a_share_active.v1",
                "snapshot_at": "2026-09-17T00:00:00+00:00",
                "paired_census_snapshot_id": "census-l1",
                "instrument_rows": {
                    "items": [
                        {
                            "instrument_id": instrument_id,
                            "exchange": "SSE",
                            "name": "样本",
                        }
                    ]
                },
            }

        def get_latest_complete_universe_snapshot(self):
            raise AssertionError("full-market snapshot already present")

        def list_asset_coverage(self, universe_snapshot_id: str):
            assert universe_snapshot_id == "snap-l1"
            return [{"instrument_id": instrument_id, "status": "available"}]

    class _Access:
        repository = _Universe()

        def get_effective_asset(self, instrument_id: str, **kwargs):
            return None

    return load_official_task_candidate_registry(
        as_of="2026-09-17",
        storage=storage,
        shared_asset_access=_Access(),
    )


def _nested_l1(name: str | None) -> dict[str, object]:
    return {
        "official_industry_code": "450000",
        "taxonomy_system": "sw",
        "classification": {
            "levels": {
                "sw_l1": {
                    "industry_code": "450000",
                    "industry_name": name,
                }
            }
        },
    }


def test_as_of_nested_l1_name_is_exposed_and_uses_existing_service_form():
    from research.company_profile.live_plan import assign_disclosure_form

    class _Storage:
        def get_industry_membership_as_of(self, instrument_id: str, as_of: str):
            assert instrument_id == "601888.SH"
            assert as_of == "2026-09-17"
            return _nested_l1("商贸零售")

        def get_industry_membership(self, instrument_id: str):
            raise AssertionError("current membership must not replace the as-of row")

    registry = _official_one_name_registry(_Storage())
    candidate = registry.candidate("601888.SH")
    assert candidate.classification is not None
    assert candidate.classification.sw_l1_name == "商贸零售"
    assert assign_disclosure_form(candidate) == "service"


def test_as_of_top_level_l1_name_wins_over_nested_name():
    from research.company_profile.live_plan import assign_disclosure_form

    class _Storage:
        def get_industry_membership_as_of(self, instrument_id: str, as_of: str):
            row = _nested_l1("社会服务")
            row["sw_l1_name"] = "银行"
            return row

    registry = _official_one_name_registry(_Storage(), instrument_id="600000.SH")
    candidate = registry.candidate("600000.SH")
    assert candidate.classification is not None
    assert candidate.classification.sw_l1_name == "银行"
    assert assign_disclosure_form(candidate) == "finance"


def test_as_of_without_stored_l1_name_stays_other():
    from research.company_profile.live_plan import assign_disclosure_form

    class _Storage:
        def get_industry_membership_as_of(self, instrument_id: str, as_of: str):
            return {
                "official_industry_code": "450000",
                "taxonomy_system": "sw",
                "classification": {"levels": {"sw_l1": {"industry_name": "  "}}},
            }

        def get_industry_membership(self, instrument_id: str):
            raise AssertionError("missing as-of L1 name must not use current membership")

    registry = _official_one_name_registry(_Storage())
    candidate = registry.candidate("601888.SH")
    assert candidate.classification_status == "present"
    assert candidate.classification is not None
    assert candidate.classification.sw_l1_name is None
    assert assign_disclosure_form(candidate) == "other"


def test_history_without_pre_cutoff_row_does_not_use_later_current_membership():
    from research.company_profile.live_plan import assign_disclosure_form

    class _Storage:
        def get_industry_membership_as_of(self, instrument_id: str, as_of: str):
            return None

        def industry_classification_history_blocks_current_membership(
            self, instrument_id: str, as_of: str
        ):
            assert instrument_id == "601888.SH"
            assert as_of == "2026-09-17"
            return True

        def get_industry_membership(self, instrument_id: str):
            raise AssertionError("later current membership must not fill a pre-cutoff gap")

    registry = _official_one_name_registry(_Storage())
    candidate = registry.candidate("601888.SH")
    assert candidate.classification_status == "missing"
    assert assign_disclosure_form(candidate) == "other"


def test_as_of_row_missing_l1_name_does_not_copy_current_service():
    from research.company_profile.live_plan import assign_disclosure_form

    class _Storage:
        def get_industry_membership_as_of(self, instrument_id: str, as_of: str):
            return {"official_industry_code": "450000", "taxonomy_system": "sw"}

        def get_industry_membership(self, instrument_id: str):
            return {"sw_l1_name": "商贸零售", "taxonomy_system": "sw"}

    registry = _official_one_name_registry(_Storage())
    candidate = registry.candidate("601888.SH")
    assert candidate.classification is not None
    assert candidate.classification.sw_l1_name is None
    assert assign_disclosure_form(candidate) == "other"


def test_closed_shenwan_table_and_two_company_budget_stay_unchanged():
    from research.company_profile.live_plan import (
        _FINANCE_L1,
        _MANUFACTURING_L1,
        _SERVICE_L1,
        DEFAULT_LIVE_MAX_COMPANIES,
        record_company_profile_live_plan,
    )
    from research.company_profile.models import PRODUCTION_AUTHORIZATION

    assert _SERVICE_L1 == frozenset(
        {
            "交通运输",
            "房地产",
            "商贸零售",
            "社会服务",
            "计算机",
            "传媒",
            "通信",
            "综合",
            "美容护理",
            "建筑装饰",
            "公用事业",
            "环保",
        }
    )
    assert _FINANCE_L1 == frozenset({"银行", "非银金融"})
    assert "有色金属" in _MANUFACTURING_L1
    assert "商贸零售" not in _MANUFACTURING_L1
    plan = record_company_profile_live_plan()
    assert DEFAULT_LIVE_MAX_COMPANIES == 2
    assert plan.budget.max_companies_this_round == 2
    assert plan.expansion_thresholds.scale_quality_claim_allowed is False
    assert PRODUCTION_AUTHORIZATION == "not_authorized"
