import asyncio
import json
import os

import pytest

from utils.llm import (
    LlmAuthenticationError,
    LlmCancelledError,
    LlmClient,
    LlmConfig,
    LlmConfigurationError,
    LlmDeadlineExceededError,
    LlmMessage,
    LlmRequest,
    LlmSchemaValidationError,
    LlmTransientTransportError,
    normalize_openai_url,
)
from utils.llm.errors import LlmProviderError
from utils.llm.rate_limit import ProfileLimiter
from utils.llm.transport import HttpxOpenAICompatibleTransport
from utils.llm import load_project_environment
from utils.llm.testing import ScriptedTransport


SCHEMA = {
    "type": "object",
    "required": ["label", "score"],
    "properties": {
        "label": {"type": "string"},
        "score": {"type": "number"},
    },
    "additionalProperties": False,
}


def _config(**profile_overrides):
    profile = {
        "enabled": True,
        "base_url": "https://provider.example/v1",
        "endpoint": "/v1/chat/completions",
        "api_key_env": "TEST_LLM_KEY",
        "model": "test-model",
        "structured_output_mode": "auto",
        "supported_structured_output_modes": ["json_schema", "json_object"],
        "timeout_seconds": 2,
        "max_retries": 1,
        "max_schema_repair_attempts": 1,
        "max_concurrency": 1,
        "requests_per_minute": 0,
        "retry_backoff_seconds": 0,
    }
    profile.update(profile_overrides)
    return LlmConfig.from_mapping({"enabled": True, "profiles": {"test": profile}})


def _request(**kwargs):
    values = {
        "profile": "test",
        "messages": [LlmMessage(role="user", content="Classify this text.")],
        "response_schema": SCHEMA,
        "schema_name": "classification",
        "schema_version": "v1",
        "metadata": {"instrument": "600000.SH"},
    }
    values.update(kwargs)
    return LlmRequest(**values)


def _unstructured_request(**kwargs):
    return _request(
        response_schema=None,
        schema_name=None,
        schema_version=None,
        **kwargs,
    )


def _response(data, **kwargs):
    value = {
        "choices": [
            {
                "message": {"content": json.dumps(data)},
                "finish_reason": "stop",
            }
        ],
        "id": "provider-1",
    }
    value.update(kwargs)
    return value


def test_project_dotenv_loader_is_explicit_and_does_not_override_environment(tmp_path, monkeypatch):
    dotenv = tmp_path / ".env"
    dotenv.write_text("TEST_LLM_KEY=from-file\n", encoding="utf-8")
    monkeypatch.setenv("TEST_LLM_KEY", "from-process")
    assert load_project_environment(tmp_path) is True
    assert os.environ["TEST_LLM_KEY"] == "from-process"


def test_profile_defaults_preserve_declared_values_and_explicit_zeroes():
    profile = LlmConfig.from_mapping(
        {"enabled": True, "profiles": {"test": {"enabled": True}}}
    ).profiles["test"]
    assert profile.api_key_env == "QUOTE_LLM_API_KEY"
    assert profile.max_output_tokens_field == "max_tokens"
    assert profile.attempt_timeout_seconds == profile.timeout_seconds
    assert profile.max_retries == 2
    assert profile.max_schema_repair_attempts == 1
    assert profile.requests_per_minute == 20
    assert profile.stream is False
    assert profile.stream_include_usage is True

    disabled_limits = LlmConfig.from_mapping(
        {
            "enabled": True,
            "profiles": {
                "test": {
                    "enabled": True,
                    "max_retries": 0,
                    "max_schema_repair_attempts": 0,
                    "requests_per_minute": 0,
                }
            },
        }
    ).profiles["test"]
    assert disabled_limits.max_retries == 0
    assert disabled_limits.max_schema_repair_attempts == 0
    assert disabled_limits.requests_per_minute == 0

    with pytest.raises(ValueError, match="unsupported max_output_tokens_field"):
        LlmConfig.from_mapping({
            "enabled": True,
            "profiles": {"test": {
                "enabled": True,
                "max_output_tokens_field": "provider_magic_tokens",
            }},
        })


@pytest.mark.asyncio
async def test_profile_selects_max_completion_tokens_and_warns_on_usage_overrun():
    transport = ScriptedTransport([_response(
        {"label": "ok", "score": 1},
        usage={
            "prompt_tokens": 100,
            "completion_tokens": 5000,
            "total_tokens": 5100,
        },
    )])
    client = LlmClient(
        _config(max_output_tokens_field="max_completion_tokens"),
        transport=transport,
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    response = await client.complete(_request(max_output_tokens=4096))
    payload = transport.calls[0]["payload"]
    assert payload["max_completion_tokens"] == 4096
    assert "max_tokens" not in payload
    assert "provider_output_budget_exceeded" in response.warnings


@pytest.mark.asyncio
async def test_attempt_timeout_retries_within_total_deadline(monkeypatch):
    calls = []
    info_messages = []
    monkeypatch.setattr(
        "utils.llm.client.llm_logger.info",
        lambda message, *args: info_messages.append(message),
    )

    async def slow_once(url, headers, payload, timeout):
        calls.append(timeout)
        if len(calls) == 1:
            await asyncio.sleep(0.03)
        return _response({"label": "ok", "score": 1})

    from utils.llm import CallableTransport

    client = LlmClient(
        _config(
            timeout_seconds=0.2,
            attempt_timeout_seconds=0.01,
            max_retries=1,
        ),
        transport=CallableTransport(slow_once),
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    response = await client.complete(_unstructured_request())
    assert response.attempt_count == 2
    assert len(calls) == 2
    assert calls[0] == pytest.approx(0.01)
    assert any("LLM attempt started" in message for message in info_messages)
    assert any("LLM retry pending" in message for message in info_messages)


@pytest.mark.asyncio
async def test_non_json_http_errors_keep_status_for_classification():
    class Response:
        status_code = 401
        headers = {"x-request-id": "provider-auth-1"}

        def json(self):
            raise ValueError("html body")

    class Client:
        calls = 0

        async def post(self, *args, **kwargs):
            self.calls += 1
            return Response()

        async def aclose(self):
            return None

    transport = HttpxOpenAICompatibleTransport(client=Client())
    response = await transport.send("https://provider.example/v1/chat/completions", {}, {}, 1)
    assert response.status_code == 401
    assert response.data is None
    assert response.provider_request_id == "provider-auth-1"

    client_impl = Client()
    client = LlmClient(
        _config(max_retries=3),
        transport=HttpxOpenAICompatibleTransport(client=client_impl),
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    with pytest.raises(LlmAuthenticationError):
        await client.complete(_unstructured_request())
    assert client_impl.calls == 1


@pytest.mark.asyncio
async def test_streaming_transport_reassembles_json_and_usage():
    chunks = [
        'data: {"id":"stream-1","model":"test-model","choices":[{"delta":{"reasoning_content":"thinking"},"finish_reason":null}]}',
        "",
        'data: {"id":"stream-1","model":"test-model","choices":[{"delta":{"content":"{\\\"label\\\":\\\"ok\\\","},"finish_reason":null}]}',
        "",
        'data: {"id":"stream-1","model":"test-model","choices":[{"delta":{"content":"\\\"score\\\":1}"},"finish_reason":"stop"}]}',
        "",
        'data: {"id":"stream-1","model":"test-model","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":4,"total_tokens":14}}',
        "",
        "data: [DONE]",
        "",
    ]

    class Response:
        status_code = 200
        headers = {"x-request-id": "provider-stream-1"}

        async def aiter_lines(self):
            for line in chunks:
                yield line

        async def aread(self):
            return b""

        def json(self):
            raise AssertionError("successful stream must not use response.json()")

    class StreamContext:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, exc_type, exc, traceback):
            return None

    class Client:
        def __init__(self):
            self.calls = []

        def stream(self, method, url, **kwargs):
            self.calls.append({"method": method, "url": url, **kwargs})
            return StreamContext()

        async def aclose(self):
            return None

    client_impl = Client()
    client = LlmClient(
        _config(stream=True, stream_include_usage=True),
        transport=HttpxOpenAICompatibleTransport(client=client_impl),
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    response = await client.complete(_request(idempotency_key="stable-stream-job"))

    assert response.data == {"label": "ok", "score": 1}
    assert response.provider_request_id == "provider-stream-1"
    assert response.usage is not None
    assert response.usage.total_tokens == 14
    assert client_impl.calls[0]["json"]["stream"] is True
    assert client_impl.calls[0]["json"]["stream_options"] == {"include_usage": True}
    stream_idempotency_key = client_impl.calls[0]["headers"]["Idempotency-Key"]
    assert stream_idempotency_key != "stable-stream-job"
    assert len(stream_idempotency_key) == 64


@pytest.mark.asyncio
async def test_url_normalization_and_disabled_fail_closed(monkeypatch):
    assert normalize_openai_url("https://pipio.io/v1", "/v1/chat/completions") == (
        "https://pipio.io/v1/chat/completions"
    )
    transport = ScriptedTransport([_response({"label": "x", "score": 1})])
    client = LlmClient(
        LlmConfig.from_mapping({"enabled": False, "profiles": {}}),
        transport=transport,
        environment={"TEST_LLM_KEY": "secret"},
    )
    with pytest.raises(LlmConfigurationError, match="disabled"):
        await client.complete(_request())
    assert transport.calls == []


@pytest.mark.asyncio
async def test_missing_key_fails_before_transport():
    transport = ScriptedTransport([_response({"label": "x", "score": 1})])
    client = LlmClient(_config(), transport=transport, environment={})
    with pytest.raises(LlmAuthenticationError):
        await client.complete(_request())
    assert transport.calls == []


@pytest.mark.asyncio
async def test_invalid_url_or_endpoint_fails_before_transport():
    for overrides in (
        {"base_url": "ftp://provider.example/v1"},
        {"endpoint": "v1/chat/completions"},
    ):
        transport = ScriptedTransport([])
        client = LlmClient(
            _config(**overrides),
            transport=transport,
            environment={"TEST_LLM_KEY": "unit-secret"},
        )
        with pytest.raises(LlmConfigurationError):
            await client.complete(_unstructured_request())
        assert transport.calls == []


@pytest.mark.asyncio
async def test_json_schema_success_metadata_redaction_and_hash_stability():
    transport = ScriptedTransport([_response({"label": "positive", "score": 0.8})] * 2)
    client = LlmClient(_config(), transport=transport, environment={"TEST_LLM_KEY": "unit-secret"})
    first = await client.complete(_request())
    second = await client.complete(_request(metadata={"trace": "different"}))
    assert first.data == {"label": "positive", "score": 0.8}
    assert first.request_hash == second.request_hash
    assert first.response_hash == second.response_hash
    assert transport.calls[0]["url"] == "https://provider.example/v1/chat/completions"
    assert transport.calls[0]["payload"]["response_format"]["type"] == "json_schema"
    assert transport.calls[0]["headers"]["Authorization"] == "Bearer unit-secret"
    assert "instrument" not in json.dumps(transport.calls[0]["payload"])
    assert first.usage is None
    assert "provider_usage_missing" in first.warnings


@pytest.mark.asyncio
async def test_json_object_schema_failure_repairs_once():
    transport = ScriptedTransport(
        [
            _response({"label": "bad", "score": "not-a-number"}),
            _response({"label": "fixed", "score": 0.5}),
        ]
    )
    client = LlmClient(
        _config(
            structured_output_mode="json_object", supported_structured_output_modes=["json_object"]
        ),
        transport=transport,
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    result = await client.complete(_request())
    assert result.data["label"] == "fixed"
    assert result.attempt_count == 2
    assert "prior response failed" in transport.calls[1]["payload"]["messages"][-1]["content"]


@pytest.mark.asyncio
async def test_prompt_only_allowed_still_runs_local_schema_validation():
    transport = ScriptedTransport([_response({"label": "ok", "score": 0.7})])
    client = LlmClient(
        _config(
            structured_output_mode="prompt_only",
            supported_structured_output_modes=["prompt_only"],
            allow_prompt_only=True,
        ),
        transport=transport,
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    response = await client.complete(_request())
    assert response.structured_output_mode == "prompt_only"
    assert "response_format" not in transport.calls[0]["payload"]
    assert "Return only valid JSON" in transport.calls[0]["payload"]["messages"][0]["content"]


@pytest.mark.asyncio
async def test_repair_failure_remains_fail_closed():
    invalid = _response({"label": "bad", "score": "wrong-type"})
    transport = ScriptedTransport([invalid, invalid])
    client = LlmClient(
        _config(
            structured_output_mode="json_object",
            supported_structured_output_modes=["json_object"],
        ),
        transport=transport,
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    with pytest.raises(LlmSchemaValidationError):
        await client.complete(_request())
    assert len(transport.calls) == 2


@pytest.mark.asyncio
async def test_array_and_nested_optional_schemas_do_not_leak_state():
    array_schema = {
        "type": "array",
        "items": {"type": "integer"},
    }
    nested_schema = {
        "type": "object",
        "required": ["entity"],
        "properties": {
            "entity": {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string"},
                    "description": {"type": ["string", "null"]},
                },
            }
        },
    }
    transport = ScriptedTransport(
        [
            _response([1, 2, 3]),
            _response({"entity": {"name": "example"}}),
        ]
    )
    client = LlmClient(
        _config(),
        transport=transport,
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    array_response, nested_response = await asyncio.gather(
        client.complete(
            _request(
                response_schema=array_schema,
                schema_name="integer_array",
                schema_version="array.v1",
            )
        ),
        client.complete(
            _request(
                response_schema=nested_schema,
                schema_name="nested_entity",
                schema_version="nested.v1",
            )
        ),
    )
    assert array_response.data == [1, 2, 3]
    assert array_response.schema_name == "integer_array"
    assert nested_response.data == {"entity": {"name": "example"}}
    assert nested_response.schema_name == "nested_entity"


@pytest.mark.asyncio
async def test_auth_error_is_not_retried():
    transport = ScriptedTransport([{"status_code": 401, "data": {"error": "hidden"}}])
    client = LlmClient(_config(), transport=transport, environment={"TEST_LLM_KEY": "unit-secret"})
    with pytest.raises(LlmAuthenticationError):
        await client.complete(_unstructured_request())
    assert len(transport.calls) == 1


@pytest.mark.asyncio
async def test_rate_limit_retries_with_bounded_attempts():
    transport = ScriptedTransport(
        [
            {"status_code": 429, "headers": {"retry-after": "0"}, "data": {}},
            _response({"label": "ok", "score": 1}),
        ]
    )
    client = LlmClient(_config(), transport=transport, environment={"TEST_LLM_KEY": "unit-secret"})
    result = await client.complete(_unstructured_request())
    assert result.attempt_count == 2


@pytest.mark.asyncio
async def test_prompt_only_requires_explicit_permission_and_untrusted_safety_instruction():
    client = LlmClient(
        _config(
            structured_output_mode="prompt_only", supported_structured_output_modes=["prompt_only"]
        ),
        transport=ScriptedTransport([]),
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    with pytest.raises(LlmConfigurationError, match="prompt_only"):
        client._select_output_mode(client.config.profiles["test"], {"type": "object"})

    client = LlmClient(
        _config(),
        transport=ScriptedTransport([_response({"label": "x", "score": 1})]),
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    with pytest.raises(LlmConfigurationError, match="safety instruction"):
        await client.complete(_unstructured_request(content_is_untrusted=True))


@pytest.mark.asyncio
async def test_shared_concurrency_limit_is_enforced():
    active = 0
    maximum = 0

    async def callback(url, headers, payload, timeout):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1
        return _response({"label": "x", "score": 1})

    from utils.llm.transport import CallableTransport

    client = LlmClient(
        _config(max_concurrency=1),
        transport=CallableTransport(callback),
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    await asyncio.gather(
        client.complete(_unstructured_request()),
        client.complete(_unstructured_request()),
    )
    assert maximum == 1


@pytest.mark.asyncio
async def test_error_logs_do_not_expose_api_key(caplog):
    transport = ScriptedTransport([{"status_code": 401, "data": {"error": "secret"}}])
    client = LlmClient(
        _config(),
        transport=transport,
        environment={"TEST_LLM_KEY": "do-not-log-this-key"},
    )
    with pytest.raises(LlmAuthenticationError):
        await client.complete(_unstructured_request())
    assert "do-not-log-this-key" not in caplog.text
    assert "Authorization" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_code", "error_type"),
    [
        (400, LlmProviderError),
        (401, LlmAuthenticationError),
        (403, LlmAuthenticationError),
        (404, LlmProviderError),
    ],
)
async def test_non_retryable_http_statuses_make_one_call(status_code, error_type):
    transport = ScriptedTransport([{"status_code": status_code, "data": {}}])
    client = LlmClient(
        _config(max_retries=3),
        transport=transport,
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    with pytest.raises(error_type):
        await client.complete(_unstructured_request())
    assert len(transport.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [500, 502, 503, 504, 520, 521, 522, 523, 524])
async def test_retryable_provider_status_then_success_and_idempotency_header(status_code):
    transport = ScriptedTransport(
        [
            {"status_code": status_code, "data": {}},
            _response({"label": "ok", "score": 1}),
        ]
    )
    client = LlmClient(
        _config(),
        transport=transport,
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    response = await client.complete(_unstructured_request(idempotency_key="stable-job-1"))
    assert response.attempt_count == 2
    assert all(call["headers"]["Idempotency-Key"] == "stable-job-1" for call in transport.calls)


@pytest.mark.asyncio
async def test_transport_error_retries_and_missing_optional_fields_warn():
    transport = ScriptedTransport(
        [
            LlmTransientTransportError("temporary connection failure"),
            {"choices": [{"message": {"content": "analysis"}}]},
        ]
    )
    client = LlmClient(
        _config(),
        transport=transport,
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    first = await client.complete(_unstructured_request(metadata={"trace": "one"}))
    assert first.attempt_count == 2
    assert first.usage is None
    assert set(first.warnings) == {
        "provider_usage_missing",
        "provider_request_id_missing",
        "finish_reason_missing",
    }

    transport = ScriptedTransport([{"choices": [{"message": {"content": "analysis"}}]}])
    client = LlmClient(
        _config(),
        transport=transport,
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    second = await client.complete(_unstructured_request(metadata={"trace": "two"}))
    assert first.request_hash == second.request_hash


@pytest.mark.asyncio
async def test_deadline_and_cancellation_are_classified():
    async def slow_transport(url, headers, payload, timeout):
        await asyncio.sleep(1)
        return _response({"label": "late", "score": 0})

    from utils.llm import CallableTransport

    client = LlmClient(
        _config(timeout_seconds=0.01, max_retries=0),
        transport=CallableTransport(slow_transport),
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    with pytest.raises(LlmDeadlineExceededError):
        await client.complete(_unstructured_request(timeout_seconds=0.01))

    client = LlmClient(
        _config(timeout_seconds=2, max_retries=0),
        transport=CallableTransport(slow_transport),
        environment={"TEST_LLM_KEY": "unit-secret"},
    )
    task = asyncio.create_task(client.complete(_unstructured_request()))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(LlmCancelledError):
        await task


@pytest.mark.asyncio
async def test_profile_rate_limiter_uses_injected_clock():
    now = [0.0]

    async def advance(seconds):
        now[0] += seconds

    limiter = ProfileLimiter(
        max_concurrency=1,
        requests_per_minute=1,
        clock=lambda: now[0],
        sleeper=advance,
        window_seconds=60,
    )
    await limiter.acquire(deadline=120)
    limiter.release()
    await limiter.acquire(deadline=120)
    limiter.release()
    assert now[0] == 60
