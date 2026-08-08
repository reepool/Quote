import copy
import json
from pathlib import Path

import pytest

from utils.config_manager import ConfigurationError, UnifiedConfigManager
from utils.llm import (
    LlmConfig,
    LlmFailureEnvelope,
    LlmResponse,
)


def _profile(source_label: str, key_env: str, model: str, resource: str):
    return {
        "enabled": True,
        "provider": "openai_compatible",
        "provider_resource": resource,
        "source_label": source_label,
        "base_url": "https://pipio.example/v1",
        "endpoint": "/v1/chat/completions",
        "api_key_env": key_env,
        "model": model,
        "structured_output_mode": "auto",
        "supported_structured_output_modes": ["json_object"],
        "allow_prompt_only": False,
        "timeout_seconds": 60,
        "queue_timeout_seconds": 30,
        "attempt_timeout_seconds": 20,
        "max_retries": 1,
        "max_schema_repair_attempts": 1,
        "max_concurrency": 4,
        "requests_per_minute": 0,
        "temperature": 0.0,
        "max_output_tokens_field": "max_completion_tokens",
        "stream": True,
        "stream_include_usage": True,
        "max_retry_after_seconds": 5,
        "retry_backoff_seconds": 0,
        "retry_jitter_ratio": 0,
        "idempotency_header": "Idempotency-Key",
    }


def _routed_config(*, shared_resource: bool = False):
    grok_resource = "pipio:shared" if shared_resource else "pipio:grok"
    luna_resource = "pipio:shared" if shared_resource else "pipio:luna"
    resources = {
        "pipio:shared": {
            "provider": "openai_compatible",
            "quota_bucket": "pipio:shared-account",
            "hard_max_concurrency": 8,
            "default_bulk_concurrency": 8,
            "reserved_concurrency": 0,
            "http_max_connections": 8,
            "http_max_keepalive_connections": 8,
            "requests_per_minute": 20,
        }
    } if shared_resource else {
        "pipio:grok": {
            "provider": "openai_compatible",
            "quota_bucket": "pipio:grok-key",
            "hard_max_concurrency": 4,
            "default_bulk_concurrency": 4,
            "reserved_concurrency": 0,
            "http_max_connections": 4,
            "http_max_keepalive_connections": 4,
            "requests_per_minute": 10,
        },
        "pipio:luna": {
            "provider": "openai_compatible",
            "quota_bucket": "pipio:luna-key",
            "hard_max_concurrency": 4,
            "default_bulk_concurrency": 4,
            "reserved_concurrency": 0,
            "http_max_connections": 4,
            "http_max_keepalive_connections": 4,
            "requests_per_minute": 10,
        },
    }
    return {
        "enabled": True,
        "provider_resources": resources,
        "profiles": {
            "semantic__grok": _profile(
                "pipio:grok-4.5",
                "QUOTE_LLM_PIPIO_GROK_API_KEY",
                "grok-4.5",
                grok_resource,
            ),
            "semantic__luna": _profile(
                "pipio:gpt-5.6-luna",
                "QUOTE_LLM_PIPIO_LUNA_API_KEY",
                "gpt-5.6-luna",
                luna_resource,
            ),
        },
        "pools": {
            "semantic_pool": {
                "enabled": True,
                "total_concurrency": 6,
                "queue_size": 20,
                "strategy": "weighted_fair",
                "borrow_idle_capacity": True,
                "members": [
                    {
                        "source_label": "pipio:grok-4.5",
                        "weight": 3,
                        "profiles": {"semantic": "semantic__grok"},
                    },
                    {
                        "source_label": "pipio:gpt-5.6-luna",
                        "weight": 1,
                        "profiles": {"semantic": "semantic__luna"},
                    },
                ],
                "failover": {
                    "max_hops": 1,
                    "failure_threshold": 2,
                    "open_seconds": 30,
                    "half_open_max_probes": 1,
                    "min_attempt_seconds": 1,
                },
            }
        },
        "routes": {
            "semantic": {
                "pool": "semantic_pool",
                "required_structured_output_modes": ["json_object"],
                "revision": "v1",
            }
        },
    }


def test_routed_profiles_support_two_keys_same_url_and_independent_resources():
    config = LlmConfig.from_mapping(_routed_config())
    profiles = config.concrete_profiles_for("semantic")
    assert [profile.api_key_env for profile in profiles] == [
        "QUOTE_LLM_PIPIO_GROK_API_KEY",
        "QUOTE_LLM_PIPIO_LUNA_API_KEY",
    ]
    assert {profile.base_url for profile in profiles} == {
        "https://pipio.example/v1"
    }
    assert {profile.model for profile in profiles} == {
        "grok-4.5",
        "gpt-5.6-luna",
    }
    assert len({profile.provider_resource for profile in profiles}) == 2


def test_shared_quota_uses_one_resource_and_facade_hides_concrete_profiles():
    config = LlmConfig.from_mapping(_routed_config(shared_resource=True))
    description = config.describe_logical_profile("semantic")
    assert description.enabled is True
    assert description.routed is True
    assert description.pool == "semantic_pool"
    assert description.effective_max_concurrency == 6
    assert description.effective_requests_per_minute == 20
    assert description.supported_structured_output_modes == ("json_object",)
    safe = description.safe_dict()
    assert "concrete_profiles" not in safe
    assert "api_key_env" not in json.dumps(safe)
    assert config.effective_route_limits("semantic") == {
        "max_concurrency": 6,
        "requests_per_minute": 20,
    }


def test_facade_exposes_independent_source_resources_and_weights():
    description = LlmConfig.from_mapping(
        _routed_config(shared_resource=False)
    ).describe_logical_profile("semantic")

    assert dict(description.source_resources) == {
        "pipio:grok-4.5": "pipio:grok",
        "pipio:gpt-5.6-luna": "pipio:luna",
    }
    assert dict(description.source_weights) == {
        "pipio:grok-4.5": 3,
        "pipio:gpt-5.6-luna": 1,
    }
    assert description.provider_resource_limits["pipio:grok"][
        "hard_max_concurrency"
    ] == 4


def test_controlled_source_config_keeps_concrete_selection_inside_facade():
    config = LlmConfig.from_mapping(_routed_config())
    controlled = config.controlled_source_config("semantic", "pipio:gpt-5.6-luna")
    assert controlled.enabled is True
    assert controlled.pools["semantic_pool"].total_concurrency == 1
    assert [item.source_label for item in controlled.pools["semantic_pool"].members] == [
        "pipio:gpt-5.6-luna"
    ]
    assert controlled.is_logical_profile_enabled("semantic") is True
    assert config.pools["semantic_pool"].total_concurrency == 6


def test_controlled_source_config_rejects_unrouted_or_unknown_source():
    config = LlmConfig.from_mapping(_routed_config())
    with pytest.raises(ValueError, match="unknown or duplicate"):
        config.controlled_source_config("semantic", "missing")
    direct = LlmConfig.from_mapping(
        {"enabled": True, "profiles": {"legacy": {"enabled": True}}}
    )
    with pytest.raises(ValueError, match="routed logical profile"):
        direct.controlled_source_config("legacy", "legacy")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("total_concurrency", True),
        ("total_concurrency", 0),
        ("total_concurrency", 1.5),
        ("queue_size", False),
        ("queue_size", -1),
        ("queue_size", 2.5),
    ],
)
def test_pool_integer_limits_reject_boolean_non_positive_and_fraction(field, value):
    raw = _routed_config()
    raw["pools"]["semantic_pool"][field] = value
    with pytest.raises(ValueError, match=field):
        LlmConfig.from_mapping(raw)


@pytest.mark.parametrize("value", [True, 0, -1, 1.5])
def test_member_weight_rejects_invalid_values(value):
    raw = _routed_config()
    raw["pools"]["semantic_pool"]["members"][0]["weight"] = value
    with pytest.raises(ValueError, match="weight"):
        LlmConfig.from_mapping(raw)


def test_pool_rejects_unknown_strategy_and_project_hard_limit():
    raw = _routed_config()
    raw["pools"]["semantic_pool"]["strategy"] = "random"
    with pytest.raises(ValueError, match="strategy"):
        LlmConfig.from_mapping(raw)
    raw = _routed_config()
    raw["pools"]["semantic_pool"]["total_concurrency"] = 61
    with pytest.raises(ValueError, match="total_concurrency"):
        LlmConfig.from_mapping(raw)


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda raw: raw["routes"]["semantic"].update(pool="missing"), "unknown pool"),
        (
            lambda raw: raw["pools"]["semantic_pool"].update(enabled=False),
            "disabled pool",
        ),
        (
            lambda raw: raw["pools"]["semantic_pool"]["members"][0]["profiles"].clear(),
            "profiles must not be empty",
        ),
        (
            lambda raw: raw["pools"]["semantic_pool"]["members"][0]["profiles"].update(
                semantic="missing"
            ),
            "unknown concrete profile",
        ),
        (
            lambda raw: raw["profiles"]["semantic__grok"].update(enabled=False),
            "disabled concrete profile",
        ),
        (
            lambda raw: raw["profiles"]["semantic__grok"].update(
                source_label="pipio:wrong"
            ),
            "source label mismatch",
        ),
        (
            lambda raw: raw["profiles"]["semantic__grok"].update(
                supported_structured_output_modes=["json_schema"]
            ),
            "structured output capability",
        ),
    ],
)
def test_route_reference_and_capability_validation(mutator, message):
    raw = _routed_config()
    mutator(raw)
    with pytest.raises(ValueError, match=message):
        LlmConfig.from_mapping(raw)


def test_route_profile_collision_duplicate_labels_and_sensitive_labels_fail():
    raw = _routed_config()
    raw["profiles"]["semantic"] = copy.deepcopy(raw["profiles"]["semantic__grok"])
    with pytest.raises(ValueError, match="conflicts"):
        LlmConfig.from_mapping(raw)

    raw = _routed_config()
    raw["pools"]["semantic_pool"]["members"][1]["source_label"] = (
        "pipio:grok-4.5"
    )
    with pytest.raises(ValueError, match="source labels must be unique"):
        LlmConfig.from_mapping(raw)

    raw = _routed_config()
    raw["pools"]["semantic_pool"]["members"][0]["source_label"] = (
        "secret-token-value"
    )
    with pytest.raises(ValueError, match="sensitive"):
        LlmConfig.from_mapping(raw)


def test_routed_profile_requires_explicit_contract_fields():
    raw = _routed_config()
    raw["profiles"]["semantic__grok"].pop("stream")
    with pytest.raises(ValueError, match="explicitly declare"):
        LlmConfig.from_mapping(raw)


def test_shared_quota_bucket_cannot_be_split_across_resources():
    raw = _routed_config()
    raw["provider_resources"]["pipio:luna"]["quota_bucket"] = "pipio:grok-key"
    with pytest.raises(ValueError, match="split shared quota bucket"):
        LlmConfig.from_mapping(raw)


def test_route_fingerprint_is_stable_secret_free_and_changes_with_revision():
    raw = _routed_config()
    first = LlmConfig.from_mapping(raw).route_fingerprint("semantic")
    reordered = copy.deepcopy(raw)
    reordered["profiles"] = dict(reversed(list(reordered["profiles"].items())))
    assert LlmConfig.from_mapping(reordered).route_fingerprint("semantic") == first

    changed_key_name = copy.deepcopy(raw)
    changed_key_name["profiles"]["semantic__grok"]["api_key_env"] = "ANOTHER_KEY"
    assert LlmConfig.from_mapping(changed_key_name).route_fingerprint("semantic") == first

    changed_revision = copy.deepcopy(raw)
    changed_revision["routes"]["semantic"]["revision"] = "v2"
    assert LlmConfig.from_mapping(changed_revision).route_fingerprint("semantic") != first
    assert "QUOTE_LLM" not in first


def test_unrouted_profile_facade_and_response_defaults_remain_compatible():
    direct = LlmConfig.from_mapping({
        "enabled": True,
        "profiles": {"legacy": {"enabled": True, "model": "legacy-model"}},
    })
    description = direct.describe_logical_profile("legacy")
    assert description.routed is False
    assert description.enabled is True
    assert direct.is_logical_profile_enabled("missing") is False
    assert len(description.route_fingerprint) == 64

    response = LlmResponse(
        status="success",
        data={},
        raw_content="{}",
        provider="openai_compatible",
        model="legacy-model",
        finish_reason="stop",
        usage=None,
        request_id="request-1",
        provider_request_id=None,
        request_hash="request-hash",
        response_hash="response-hash",
        schema_name=None,
        schema_version=None,
        structured_output_mode="json_object",
        latency_ms=1,
        attempt_count=1,
    )
    assert response.source_label is None
    assert response.failover_count == 0
    assert response.attempts == ()

    failure = LlmFailureEnvelope(
        status="failed",
        error={"code": "provider_error"},
        request_id="request-1",
        request_hash=None,
        attempt_count=1,
    )
    assert failure.lineage == {}
    assert failure.route_fingerprint is None


def test_config_manager_rejects_duplicate_top_level_llm_owners(tmp_path):
    (tmp_path / "01_first.json").write_text(
        json.dumps({"llm": {"enabled": False}}), encoding="utf-8"
    )
    (tmp_path / "02_second.json").write_text(
        json.dumps({"llm": {"enabled": True}}), encoding="utf-8"
    )
    with pytest.raises(ConfigurationError, match="multiple owners"):
        UnifiedConfigManager(str(tmp_path))


def test_repository_llm_config_is_enabled_non_secret_and_has_one_owner():
    manager = UnifiedConfigManager("config")
    config = manager.get_llm_config()
    assert config.enabled is True
    assert set(config.routes) == {
        "corporate_action_title_classification",
        "semantic_extraction",
    }
    assert config.pools["shared_semantic"].enabled is True
    assert config.is_logical_profile_enabled("semantic_extraction") is True
    members = config.pools["shared_semantic"].members
    assert [member.source_label for member in members] == [
        "scorpio:grok-4.5",
        "scorpio:gpt-5.6-luna",
    ]
    assert all(member.weight > 0 for member in members)
    profiles = config.profiles
    assert profiles["semantic_extraction__scorpio_grok"].api_key_env == (
        "QUOTE_LLM_SCORPIO_GROK_API_KEY"
    )
    assert profiles["semantic_extraction__scorpio_luna"].api_key_env == (
        "QUOTE_LLM_SCORPIO_LUNA_API_KEY"
    )
    serialized = json.dumps(
        {name: profile.safe_dict() for name, profile in profiles.items()}
    )
    assert "Bearer " not in serialized
    assert "unit-test-key" not in serialized


def test_repository_llm_config_supports_single_grok_member():
    raw = json.loads(Path("config/13_llm.json").read_text(encoding="utf-8"))["llm"]
    pool = raw["pools"]["shared_semantic"]
    pool["members"] = [
        member
        for member in pool["members"]
        if member["source_label"] == "scorpio:grok-4.5"
    ]
    pool["failover"]["enabled"] = False
    for name, profile in raw["profiles"].items():
        if name.endswith("__scorpio_luna"):
            profile["enabled"] = False

    config = LlmConfig.from_mapping(raw)

    assert config.is_logical_profile_enabled("semantic_extraction") is True
    assert [
        profile.name for profile in config.concrete_profiles_for("semantic_extraction")
    ] == ["semantic_extraction__scorpio_grok"]
