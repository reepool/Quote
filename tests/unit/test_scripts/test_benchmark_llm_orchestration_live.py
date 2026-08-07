import asyncio
from dataclasses import asdict
from types import SimpleNamespace

import pytest

from scripts.dev_validation import benchmark_llm_orchestration_live as benchmark
from scripts.dev_validation.benchmark_llm_orchestration_live import (
    acceptance_reasons,
    build_controlled_config,
)
from utils.config_manager import config_manager


def _accepted_result():
    return {
        "concurrency": 10,
        "successes": 10,
        "rate_limits": 0,
        "provider_5xx": 0,
        "timeouts": 0,
        "parse_failures": 0,
        "schema_failures": 0,
        "failover_exhausted": 0,
        "first_event_count": 10,
        "identity_ok": True,
        "registry_empty_after_shutdown": True,
        "transport_active_after_shutdown": 0,
        "transport_peak": 10,
        "confirmed_aggregate_provider_concurrency": 20,
        "provider_limits_ok": True,
        "fd_delta": 0,
        "dispatch_counts": {
            "pipio:grok-4.5": 8,
            "pipio:gpt-5.6-luna": 2,
        },
        "normal_dispatch_counts": {
            "pipio:grok-4.5": 8,
            "pipio:gpt-5.6-luna": 2,
        },
        "configured_weight_ratio": {
            "pipio:grok-4.5": 0.75,
            "pipio:gpt-5.6-luna": 0.25,
        },
    }


def test_live_acceptance_gate_requires_clean_complete_stage():
    assert acceptance_reasons(_accepted_result()) == []
    failed = {**_accepted_result(), "successes": 9, "provider_5xx": 1}
    assert acceptance_reasons(failed) == [
        "not_all_requests_succeeded",
        "nonzero_provider_5xx",
    ]


def test_live_acceptance_gate_rejects_missing_measurements_and_identity():
    failed = {
        **_accepted_result(),
        "first_event_count": 9,
        "identity_ok": False,
    }

    assert acceptance_reasons(failed) == [
        "missing_first_event_measurements",
        "request_identity_mismatch",
    ]


def test_live_acceptance_gate_rejects_provider_limit_mismatch_or_overrun():
    failed = {
        **_accepted_result(),
        "provider_limits_ok": False,
        "transport_peak": 21,
    }

    assert acceptance_reasons(failed) == [
        "provider_limits_mismatch",
        "provider_concurrency_exceeded",
    ]


def test_live_acceptance_gate_checks_normal_not_borrowed_dispatch_ratio():
    borrowed = {
        **_accepted_result(),
        "dispatch_counts": {
            "pipio:grok-4.5": 3,
            "pipio:gpt-5.6-luna": 7,
        },
        "normal_dispatch_counts": {
            "pipio:grok-4.5": 3,
            "pipio:gpt-5.6-luna": 1,
        },
        "borrowed_dispatch_counts": {
            "pipio:grok-4.5": 0,
            "pipio:gpt-5.6-luna": 6,
        },
        "borrowed_dispatches": 6,
    }

    assert acceptance_reasons(borrowed) == []


def test_live_acceptance_gate_rejects_biased_normal_dispatch_ratio():
    biased = {
        **_accepted_result(),
        "dispatch_counts": {
            "pipio:grok-4.5": 3,
            "pipio:gpt-5.6-luna": 7,
        },
        "normal_dispatch_counts": {
            "pipio:grok-4.5": 1,
            "pipio:gpt-5.6-luna": 3,
        },
        "borrowed_dispatch_counts": {
            "pipio:grok-4.5": 2,
            "pipio:gpt-5.6-luna": 4,
        },
        "borrowed_dispatches": 6,
    }

    assert acceptance_reasons(biased) == [
        "dispatch_ratio_out_of_tolerance:pipio:grok-4.5",
        "dispatch_ratio_out_of_tolerance:pipio:gpt-5.6-luna",
    ]


def test_controlled_live_config_is_in_memory_and_quota_bounded():
    base = config_manager.get_llm_config()
    original = asdict(base)
    controlled = build_controlled_config(
        base,
        logical_profile="semantic_extraction",
        concurrency=10,
        confirmed_quota_scope="independent",
        confirmed_per_source_concurrency=10,
        confirmed_provider_rpm=10,
        timeout_seconds=120,
    )

    assert controlled.enabled is True
    assert controlled.pools["shared_semantic"].enabled is True
    assert controlled.pools["shared_semantic"].total_concurrency == 10
    assert all(
        controlled.profiles[name].enabled
        for name in (
            "semantic_extraction__pipio_grok",
            "semantic_extraction__pipio_luna",
        )
    )
    for resource_name in ("pipio:grok", "pipio:luna"):
        resource = controlled.provider_resources[resource_name]
        assert resource.hard_max_concurrency == 10
        assert resource.requests_per_minute == 10
    for profile_name in (
        "semantic_extraction__pipio_grok",
        "semantic_extraction__pipio_luna",
    ):
        profile = controlled.profiles[profile_name]
        assert profile.max_concurrency == 10
        assert profile.requests_per_minute == 0
    assert asdict(base) == original


def test_pool_stage_can_exceed_provider_caps_without_overriding_them():
    controlled = build_controlled_config(
        config_manager.get_llm_config(),
        logical_profile="semantic_extraction",
        concurrency=50,
        confirmed_quota_scope="independent",
        confirmed_per_source_concurrency=10,
        confirmed_provider_rpm=10,
        timeout_seconds=620,
    )

    assert controlled.pools["shared_semantic"].total_concurrency == 50
    assert {
        resource.hard_max_concurrency
        for resource in controlled.provider_resources.values()
    } == {10}
    assert {
        resource.requests_per_minute
        for resource in controlled.provider_resources.values()
    } == {10}


def test_controlled_live_config_supports_lower_per_resource_caps():
    controlled = build_controlled_config(
        config_manager.get_llm_config(),
        logical_profile="semantic_extraction",
        concurrency=50,
        confirmed_quota_scope="independent",
        confirmed_per_source_concurrency=10,
        confirmed_provider_rpm=10,
        timeout_seconds=620,
        resource_concurrency_limits={
            "pipio:grok": 8,
            "pipio:luna": 1,
        },
    )

    assert controlled.provider_resources["pipio:grok"].hard_max_concurrency == 8
    assert controlled.provider_resources["pipio:luna"].hard_max_concurrency == 1
    assert controlled.provider_resources["pipio:luna"].default_bulk_concurrency == 1
    assert controlled.profiles[
        "semantic_extraction__pipio_grok"
    ].max_concurrency == 8
    assert controlled.profiles[
        "semantic_extraction__pipio_luna"
    ].max_concurrency == 1


@pytest.mark.parametrize(
    ("limits", "message"),
    [
        ({"unknown": 1}, "unknown routed provider resource"),
        ({"pipio:luna": 0}, "positive integer"),
        ({"pipio:luna": 11}, "cannot exceed confirmed"),
    ],
)
def test_controlled_live_config_rejects_invalid_resource_caps(limits, message):
    with pytest.raises(ValueError, match=message):
        build_controlled_config(
            config_manager.get_llm_config(),
            logical_profile="semantic_extraction",
            concurrency=10,
            confirmed_quota_scope="independent",
            confirmed_per_source_concurrency=10,
            confirmed_provider_rpm=10,
            timeout_seconds=120,
            resource_concurrency_limits=limits,
        )


@pytest.mark.parametrize(
    ("source_limit", "rpm", "message"),
    [
        (0, 10, "concurrency must be positive"),
        (10, 0, "RPM must be positive"),
    ],
)
def test_controlled_live_config_rejects_unconfirmed_capacity(
    source_limit, rpm, message
):
    with pytest.raises(ValueError, match=message):
        build_controlled_config(
            config_manager.get_llm_config(),
            logical_profile="semantic_extraction",
            concurrency=10,
            confirmed_quota_scope="independent",
            confirmed_per_source_concurrency=source_limit,
            confirmed_provider_rpm=rpm,
            timeout_seconds=120,
        )


@pytest.mark.parametrize(
    ("quota_scope", "message"),
    [
        ("shared", "one provider resource"),
        ("unknown", "independent or shared"),
    ],
)
def test_controlled_live_config_validates_confirmed_quota_scope(
    quota_scope, message
):
    with pytest.raises(ValueError, match=message):
        build_controlled_config(
            config_manager.get_llm_config(),
            logical_profile="semantic_extraction",
            concurrency=10,
            confirmed_quota_scope=quota_scope,
            confirmed_per_source_concurrency=10,
            confirmed_provider_rpm=10,
            timeout_seconds=120,
        )


def test_staged_live_gate_stops_after_first_failed_level(monkeypatch):
    calls = []

    async def fake_run_stage(base, **kwargs):
        concurrency = kwargs["concurrency"]
        calls.append(concurrency)
        return {"concurrency": concurrency, "accepted": concurrency < 25}

    monkeypatch.setattr(benchmark, "run_stage", fake_run_stage)
    monkeypatch.setattr(
        benchmark.config_manager,
        "get_llm_config",
        lambda: object(),
    )
    args = SimpleNamespace(
        confirm_live=True,
        concurrency=[10, 25, 50],
        profile="semantic_extraction",
        confirmed_quota_scope="independent",
        confirmed_per_source_concurrency=50,
        confirmed_provider_rpm=50,
        timeout_seconds=120,
    )

    result = asyncio.run(benchmark.run_stages(args))

    assert calls == [10, 25]
    assert result["executed_stages"] == [10, 25]
    assert result["all_executed_stages_accepted"] is False


@pytest.mark.parametrize(
    ("confirm_live", "levels", "message"),
    [
        (False, [10], "--confirm-live"),
        (True, [25, 10], "unique and increasing"),
        (True, [10, 10], "unique and increasing"),
    ],
)
def test_staged_live_gate_rejects_unsafe_invocation(
    confirm_live, levels, message
):
    args = SimpleNamespace(
        confirm_live=confirm_live,
        concurrency=levels,
        profile="semantic_extraction",
        confirmed_quota_scope="independent",
        confirmed_per_source_concurrency=50,
        confirmed_provider_rpm=50,
        timeout_seconds=120,
    )

    with pytest.raises(ValueError, match=message):
        asyncio.run(benchmark.run_stages(args))


def test_live_cli_accepts_explicit_result_path(tmp_path):
    output_path = tmp_path / "live-result.json"

    args = benchmark.parse_args([
        "--confirm-live",
        "--confirmed-quota-scope",
        "independent",
        "--concurrency",
        "10",
        "25",
        "50",
        "--output-json",
        str(output_path),
        "--resource-concurrency-limit",
        "pipio:grok=8",
        "--resource-concurrency-limit",
        "pipio:luna=1",
    ])

    assert args.concurrency == [10, 25, 50]
    assert args.output_json == output_path
    assert args.resource_concurrency_limits == {
        "pipio:grok": 8,
        "pipio:luna": 1,
    }


@pytest.mark.parametrize(
    "values",
    [
        ["invalid"],
        ["pipio:luna=zero"],
        ["pipio:luna=0"],
        ["pipio:luna=1", "pipio:luna=2"],
    ],
)
def test_live_cli_rejects_invalid_resource_limit(values):
    argv = [
        "--confirm-live",
        "--confirmed-quota-scope",
        "independent",
    ]
    for value in values:
        argv.extend(("--resource-concurrency-limit", value))

    with pytest.raises(SystemExit):
        benchmark.parse_args(argv)
