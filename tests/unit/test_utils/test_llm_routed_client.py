import asyncio
import json
import logging
import time
from dataclasses import replace

import pytest

from utils.llm import (
    LlmAuthenticationError,
    LlmClient,
    LlmConfig,
    LlmMessage,
    LlmPoolCoordinatorRegistry,
    LlmRequest,
    LlmResponseParseError,
    LlmTransientTransportError,
)
from utils.llm.orchestration import ProviderCoordinatorRegistry
from utils.llm.rate_limit import ProfileLimiterRegistry
from utils.llm.testing import ScriptedTransport
from utils.llm.transport import TransportResponse


SCHEMA = {
    "type": "object",
    "required": ["label"],
    "properties": {"label": {"type": "string"}},
    "additionalProperties": False,
}


def _profile(source_label, key_env, model, resource, **overrides):
    profile = {
        "enabled": True,
        "provider": "openai_compatible",
        "provider_resource": resource,
        "source_label": source_label,
        "base_url": f"https://{model}.example/v1",
        "endpoint": "/v1/chat/completions",
        "api_key_env": key_env,
        "model": model,
        "structured_output_mode": "json_object",
        "supported_structured_output_modes": ["json_object"],
        "timeout_seconds": 0.5,
        "queue_timeout_seconds": 0.5,
        "attempt_timeout_seconds": 0.5,
        "max_retries": 0,
        "max_schema_repair_attempts": 0,
        "max_concurrency": 2,
        "requests_per_minute": 0,
        "retry_backoff_seconds": 0,
        "retry_jitter_ratio": 0,
        "max_output_tokens_field": "max_tokens",
        "stream": False,
        "stream_include_usage": True,
    }
    profile.update(overrides)
    return profile


def _config(
    *,
    allow_auth_failover=False,
    failure_threshold=3,
    max_hops=1,
    **profile_overrides,
):
    return LlmConfig.from_mapping({
        "enabled": True,
        "provider_resources": {
            "grok-resource": {
                "hard_max_concurrency": 2,
                "default_bulk_concurrency": 2,
                "reserved_concurrency": 0,
                "http_max_connections": 2,
                "http_max_keepalive_connections": 2,
                "requests_per_minute": 0,
            },
            "luna-resource": {
                "hard_max_concurrency": 2,
                "default_bulk_concurrency": 2,
                "reserved_concurrency": 0,
                "http_max_connections": 2,
                "http_max_keepalive_connections": 2,
                "requests_per_minute": 0,
            },
        },
        "profiles": {
            "semantic__grok": _profile(
                "pipio:grok-4.5",
                "TEST_GROK_KEY",
                "grok-4.5",
                "grok-resource",
                **profile_overrides,
            ),
            "semantic__luna": _profile(
                "pipio:gpt-5.6-luna",
                "TEST_LUNA_KEY",
                "gpt-5.6-luna",
                "luna-resource",
                **profile_overrides,
            ),
        },
        "pools": {
            "semantic-pool": {
                "enabled": True,
                "total_concurrency": 2,
                "queue_size": 4,
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
                    "enabled": True,
                    "max_hops": max_hops,
                    "failure_threshold": failure_threshold,
                    "open_seconds": 1,
                    "half_open_max_probes": 1,
                    "min_attempt_seconds": 0.01,
                    "allow_auth_failover": allow_auth_failover,
                    "on": [
                        "rate_limit_error",
                        "transient_transport_error",
                        "provider_error",
                        "response_parse_error",
                        "schema_validation_error",
                    ],
                },
            }
        },
        "routes": {
            "semantic": {
                "pool": "semantic-pool",
                "revision": "v1",
                "required_structured_output_modes": ["json_object"],
            }
        },
    })


def _request(**overrides):
    values = {
        "profile": "semantic",
        "messages": [LlmMessage(role="user", content="synthetic announcement")],
        "response_schema": SCHEMA,
        "schema_name": "semantic",
        "schema_version": "v1",
        "idempotency_key": "business-idempotency-key",
        "metadata": {"workload": "semantic", "business_item_key": "item-1"},
    }
    values.update(overrides)
    return LlmRequest(**values)


def _success(label, model):
    return {
        "choices": [{
            "message": {"content": json.dumps({"label": label})},
            "finish_reason": "stop",
        }],
        "id": f"provider-{model}",
        "model": model,
    }


def _client(config, transport):
    return LlmClient(
        config,
        transport=transport,
        environment={
            "TEST_GROK_KEY": "grok-secret",
            "TEST_LUNA_KEY": "luna-secret",
            "TEST_THIRD_KEY": "third-secret",
        },
        limiter_registry=ProfileLimiterRegistry(),
        provider_coordinator_registry=ProviderCoordinatorRegistry(),
        pool_coordinator_registry=LlmPoolCoordinatorRegistry(),
    )


def _three_source_config():
    config = _config(max_hops=2)
    luna_resource = config.provider_resources["luna-resource"]
    third_resource = replace(luna_resource, name="third-resource")
    luna_profile = config.profiles["semantic__luna"]
    third_profile = replace(
        luna_profile,
        name="semantic__third",
        provider_resource="third-resource",
        source_label="pipio:third",
        api_key_env="TEST_THIRD_KEY",
        model="third-model",
    )
    pool = config.pools["semantic-pool"]
    third_member = replace(
        pool.members[1],
        source_label="pipio:third",
        profiles={"semantic": "semantic__third"},
    )
    return replace(
        config,
        provider_resources={
            **config.provider_resources,
            "third-resource": third_resource,
        },
        profiles={**config.profiles, "semantic__third": third_profile},
        pools={
            "semantic-pool": replace(
                pool,
                members=(*pool.members, third_member),
            )
        },
    )


@pytest.mark.asyncio
async def test_rate_limit_fails_over_from_grok_to_luna_with_safe_lineage():
    transport = ScriptedTransport([
        {"status_code": 429, "data": {"error": "do-not-log-provider-body"}},
        _success("ok", "gpt-5.6-luna"),
    ])
    config = _config()
    client = _client(config, transport)
    response = await client.complete(_request())

    assert response.source_label == "pipio:gpt-5.6-luna"
    assert response.logical_profile == "semantic"
    assert response.selected_profile == "semantic__luna"
    assert response.failover_count == 1
    assert [attempt.get("error_code", attempt.get("status")) for attempt in response.attempts] == [
        "rate_limit_error",
        "success",
    ]
    assert response.attempts[0]["source_label"] == "pipio:grok-4.5"
    assert response.attempts[0]["request_id"] != response.attempts[1]["request_id"]
    assert [item["attempt_sequence"] for item in response.attempts] == [1, 2]
    assert [item["model"] for item in response.attempts] == [
        "grok-4.5",
        "gpt-5.6-luna",
    ]
    assert all(
        call["headers"]["Idempotency-Key"] == "business-idempotency-key"
        for call in transport.calls
    )
    snapshot = client.pool_coordinator_registry.get(
        config, "semantic-pool"
    ).snapshot()
    assert set(snapshot.provider_snapshots) == {
        "grok-resource",
        "luna-resource",
    }
    assert snapshot.failover_succeeded == 1
    assert snapshot.latency_ms["failover"] >= 0


@pytest.mark.asyncio
async def test_same_source_retry_success_records_rate_limit_without_health_failure():
    transport = ScriptedTransport([
        {"status_code": 429, "data": {}},
        _success("ok", "grok-4.5"),
    ])
    config = _config(max_retries=1)
    client = _client(config, transport)

    response = await client.complete(_request())

    assert response.source_label == "pipio:grok-4.5"
    assert response.attempts[0]["attempt_failures"] == [{
        "attempt_sequence": 1,
        "error_code": "rate_limit_error",
        "status_code": 429,
    }]
    member = client.pool_coordinator_registry.get(
        config, "semantic-pool"
    ).snapshot().members[0]
    assert member.rate_limits == 1
    assert member.provider_5xx == 0
    assert member.timeouts == 0
    assert member.successes == 1
    assert member.failures == 0
    assert member.consecutive_failures == 0
    assert member.circuit_state == "closed"


@pytest.mark.asyncio
async def test_http_503_is_counted_only_as_provider_5xx():
    transport = ScriptedTransport([
        {"status_code": 503, "data": {}},
        _success("ok", "gpt-5.6-luna"),
    ])
    config = _config()
    client = _client(config, transport)

    await client.complete(_request())

    member = client.pool_coordinator_registry.get(
        config, "semantic-pool"
    ).snapshot().members[0]
    assert member.rate_limits == 0
    assert member.provider_5xx == 1
    assert member.timeouts == 0


@pytest.mark.asyncio
async def test_transport_failure_without_http_status_is_counted_only_as_timeout():
    transport = ScriptedTransport([
        LlmTransientTransportError("synthetic timeout"),
        _success("ok", "gpt-5.6-luna"),
    ])
    config = _config()
    client = _client(config, transport)

    await client.complete(_request())

    member = client.pool_coordinator_registry.get(
        config, "semantic-pool"
    ).snapshot().members[0]
    assert member.rate_limits == 0
    assert member.provider_5xx == 0
    assert member.timeouts == 1


@pytest.mark.asyncio
async def test_streaming_idempotency_header_is_stable_across_failover():
    transport = ScriptedTransport([
        {"status_code": 429, "data": {}},
        _success("ok", "gpt-5.6-luna"),
    ])
    response = await _client(_config(stream=True), transport).complete(_request())

    assert response.failover_count == 1
    keys = [call["headers"]["Idempotency-Key"] for call in transport.calls]
    assert len(set(keys)) == 1
    assert keys[0] != "business-idempotency-key"
    assert len(keys[0]) == 64


@pytest.mark.asyncio
async def test_failed_attempt_lineage_uses_request_model_override():
    transport = ScriptedTransport([
        {"status_code": 429, "data": {}},
        _success("ok", "served-model"),
    ])
    response = await _client(_config(), transport).complete(
        _request(model="request-model-override")
    )
    assert response.attempts[0]["model"] == "request-model-override"


@pytest.mark.asyncio
async def test_multihop_failover_metrics_keep_initial_trigger_category():
    config = _three_source_config()
    transport = ScriptedTransport([
        {"status_code": 429, "data": {}},
        {"status_code": 503, "data": {}},
        _success("ok", "third-model"),
    ])
    client = _client(config, transport)
    response = await client.complete(_request())

    assert response.source_label == "pipio:third"
    assert response.failover_count == 2
    assert [attempt.get("error_code") for attempt in response.attempts[:2]] == [
        "rate_limit_error",
        "transient_transport_error",
    ]
    snapshot = client.pool_coordinator_registry.get(
        config, "semantic-pool"
    ).snapshot()
    assert snapshot.failover_by_error == {"rate_limit_error": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "first_failure",
    [
        LlmTransientTransportError("synthetic timeout"),
        TransportResponse(
            status_code=200,
            data={"choices": [{"message": {"content": "not-json"}}]},
        ),
    ],
)
async def test_transport_or_parse_exhaustion_fails_over(first_failure):
    transport = ScriptedTransport([first_failure, _success("ok", "gpt-5.6-luna")])
    response = await _client(_config(), transport).complete(_request())
    assert response.source_label == "pipio:gpt-5.6-luna"
    assert len(response.attempts) == 2
    assert response.attempts[0]["source_label"] != response.attempts[1]["source_label"]


@pytest.mark.asyncio
async def test_authentication_fails_closed_unless_explicitly_enabled():
    first = ScriptedTransport([
        {"status_code": 401, "data": {"error": "invalid credential"}},
        _success("unused", "gpt-5.6-luna"),
    ])
    with pytest.raises(LlmAuthenticationError) as captured:
        await _client(_config(), first).complete(_request())
    assert len(first.calls) == 1
    assert captured.value.lineage["failover_count"] == 0

    second = ScriptedTransport([
        {"status_code": 401, "data": {"error": "invalid credential"}},
        _success("ok", "gpt-5.6-luna"),
    ])
    response = await _client(
        _config(allow_auth_failover=True), second
    ).complete(_request())
    assert response.source_label == "pipio:gpt-5.6-luna"


@pytest.mark.asyncio
async def test_all_sources_failure_is_classified_and_releases_pool_permit():
    config = _config()
    transport = ScriptedTransport([
        LlmTransientTransportError("first"),
        LlmTransientTransportError("second"),
    ])
    client = _client(config, transport)
    with pytest.raises(LlmTransientTransportError) as captured:
        await client.complete(_request())

    lineage = captured.value.lineage
    assert lineage["logical_profile"] == "semantic"
    assert lineage["failover_count"] == 1
    assert len(lineage["attempts"]) == 2
    assert "secret" not in json.dumps(lineage)
    snapshot = client.pool_coordinator_registry.get(config, "semantic-pool").snapshot()
    assert snapshot.active == 0
    assert all(member.active == 0 for member in snapshot.members)


@pytest.mark.asyncio
async def test_single_source_failure_keeps_original_error_and_zero_failover_count():
    config = _config()
    pool = config.pools["semantic-pool"]
    single_source_config = replace(
        config,
        pools={
            "semantic-pool": replace(pool, members=(pool.members[0],)),
        },
    )
    transport = ScriptedTransport([LlmTransientTransportError("single-source")])
    client = _client(single_source_config, transport)

    with pytest.raises(LlmTransientTransportError) as captured:
        await client.complete(_request())

    assert captured.value.code == "transient_transport_error"
    assert captured.value.lineage["failover_count"] == 0
    assert len(captured.value.lineage["attempts"]) == 1


class _DelayedFailureTransport(ScriptedTransport):
    async def send(self, url, headers, payload, timeout_seconds):
        if not self.calls:
            self.calls.append({
                "url": url,
                "headers": dict(headers),
                "payload": dict(payload),
                "timeout_seconds": timeout_seconds,
            })
            await asyncio.sleep(0.05)
            raise LlmTransientTransportError("synthetic delayed timeout")
        return await super().send(url, headers, payload, timeout_seconds)


@pytest.mark.asyncio
async def test_failover_reuses_one_absolute_execution_deadline():
    transport = _DelayedFailureTransport([_success("ok", "gpt-5.6-luna")])
    response = await _client(
        _config(timeout_seconds=0.3, attempt_timeout_seconds=1), transport
    ).complete(_request())
    assert response.source_label == "pipio:gpt-5.6-luna"
    assert len(transport.calls) == 2
    assert transport.calls[1]["timeout_seconds"] < transport.calls[0]["timeout_seconds"] - 0.02


@pytest.mark.asyncio
async def test_routed_client_preserves_client_timeout_diagnostics():
    class BlockingTransport(ScriptedTransport):
        async def send(self, url, headers, payload, timeout_seconds):
            self.calls.append({
                "url": url,
                "headers": dict(headers),
                "payload": dict(payload),
            })
            await asyncio.sleep(0.05)
            return _success("unexpected", "grok-4.5")

    config = _config(
        timeout_seconds=0.2,
        attempt_timeout_seconds=0.01,
        max_hops=1,
    )
    transport = BlockingTransport([])
    with pytest.raises(LlmTransientTransportError) as captured:
        await _client(config, transport).complete(_request())

    error = captured.value
    assert error.status_code is None
    assert error.transport_error_type == "client_attempt_timeout"
    assert error.transport_phase == "request"
    assert error.transport_exception_type == "TimeoutError"
    attempts = error.lineage["attempts"]
    assert len(attempts) == 2
    assert all(
        item["attempt_failures"][0]["transport_error_type"]
        == "client_attempt_timeout"
        for item in attempts
    )
    assert all(item["attempt_failures"][0]["status_code"] is None for item in attempts)


@pytest.mark.asyncio
async def test_configuration_and_exhausted_deadline_do_not_fail_over():
    invalid = ScriptedTransport([_success("unused", "grok-4.5")])
    with pytest.raises(Exception) as captured:
        await _client(_config(), invalid).complete(_request(messages=[]))
    assert getattr(captured.value, "code", None) == "configuration_error"
    assert invalid.calls == []

    delayed = _DelayedFailureTransport([_success("unused", "gpt-5.6-luna")])
    with pytest.raises(Exception) as deadline_error:
        await _client(
            _config(timeout_seconds=0.02, attempt_timeout_seconds=1), delayed
        ).complete(_request())
    error = deadline_error.value
    assert getattr(error, "code", None) == "deadline_exceeded"
    assert error.status_code is None
    assert error.transport_error_type == "client_execution_deadline"
    assert error.transport_phase == "request"
    assert error.transport_exception_type == "TimeoutError"
    assert len(delayed.calls) == 1
    attempt_failure = error.lineage["attempts"][0]["attempt_failures"][0]
    assert attempt_failure["status_code"] is None
    assert attempt_failure["transport_error_type"] == "client_execution_deadline"


@pytest.mark.asyncio
async def test_cancellation_stops_failover_and_releases_pool_permit():
    class BlockingTransport(ScriptedTransport):
        def __init__(self):
            super().__init__([])
            self.started = asyncio.Event()

        async def send(self, url, headers, payload, timeout_seconds):
            self.calls.append({"url": url})
            self.started.set()
            await asyncio.Event().wait()

    config = _config()
    transport = BlockingTransport()
    client = _client(config, transport)
    task = asyncio.create_task(client.complete(_request()))
    await transport.started.wait()
    task.cancel()
    with pytest.raises(Exception) as captured:
        await task
    assert getattr(captured.value, "code", None) == "cancelled"
    assert len(transport.calls) == 1
    snapshot = client.pool_coordinator_registry.get(
        config, "semantic-pool"
    ).snapshot()
    assert snapshot.active == 0
    assert all(member.active == 0 for member in snapshot.members)


@pytest.mark.asyncio
async def test_routing_logs_use_stable_levels_and_redact_content(caplog):
    document = "sensitive-complete-document"
    provider_body = "sensitive-provider-body"
    transport = ScriptedTransport([
        {"status_code": 429, "data": {"error": provider_body}},
        _success("ok", "gpt-5.6-luna"),
    ])
    logger = logging.getLogger("LLM")
    previous_level = logger.level
    logger.setLevel(logging.DEBUG)
    logger.addHandler(caplog.handler)
    try:
        await _client(_config(), transport).complete(
            _request(messages=[LlmMessage(role="user", content=document)])
        )
    finally:
        logger.removeHandler(caplog.handler)
        logger.setLevel(previous_level)

    by_event = {
        record.getMessage().split()[0]: record.levelno
        for record in caplog.records
        if record.getMessage().startswith("event=")
    }
    assert by_event["event=llm.request.prepared"] == logging.DEBUG
    assert by_event["event=llm.attempt.admission_wait"] == logging.DEBUG
    assert by_event["event=llm.attempt.leases_released"] == logging.DEBUG
    assert by_event["event=llm.route.admitted"] == logging.INFO
    assert by_event["event=llm.route.failover_selected"] == logging.INFO
    assert by_event["event=llm.route.source_failed"] == logging.WARNING
    assert by_event["event=llm.route.completed"] == logging.INFO
    rendered = "\n".join(record.getMessage() for record in caplog.records)
    for forbidden in (
        "grok-secret",
        "luna-secret",
        document,
        provider_body,
        "Authorization",
        "Bearer",
    ):
        assert forbidden not in rendered


@pytest.mark.asyncio
async def test_terminal_route_log_has_safe_exception_context(caplog):
    provider_detail = "raw-provider-diagnostic-must-not-appear"
    transport = ScriptedTransport([
        LlmTransientTransportError(provider_detail),
        LlmTransientTransportError(provider_detail),
    ])
    logger = logging.getLogger("LLM")
    logger.addHandler(caplog.handler)
    previous_level = logger.level
    logger.setLevel(logging.ERROR)
    try:
        with pytest.raises(LlmTransientTransportError):
            await _client(_config(), transport).complete(_request())
    finally:
        logger.removeHandler(caplog.handler)
        logger.setLevel(previous_level)

    record = next(
        item
        for item in caplog.records
        if item.getMessage().startswith("event=llm.route.all_sources_failed")
    )
    assert record.exc_info is not None
    assert provider_detail not in record.getMessage()
    assert provider_detail not in "".join(
        logging.Formatter().formatException(record.exc_info)
    )
