"""Synthetic streams. They do not replay a company reading or a live model."""

import asyncio

import pytest

from tests.unit.test_utils.test_llm_gateway import _config, _request, _response
from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
from utils.llm.errors import (
    LlmResponseParseError,
    LlmResponseTruncatedError,
    LlmSchemaValidationError,
)
from utils.llm.testing import ScriptedTransport
from utils.llm.transport import _collect_openai_stream


def _sse(*payloads: str) -> list[str]:
    lines = []
    for payload in payloads:
        lines.extend((f"data: {payload}", ""))
    return lines


async def _collect(lines: list[str]):
    async def iterator():
        for line in lines:
            yield line

    return await _collect_openai_stream(
        iterator(), provider_request_id="provider-test", started=0
    )


def _complete(data):
    budget = _budget()
    client = _client(
        ScriptedTransport([{"status_code": 200, "data": data, "id": "provider-test"}]),
        budget,
        _config(max_retries=0, max_schema_repair_attempts=0),
    )
    try:
        asyncio.run(client.complete(_request(max_output_tokens=1000)))
    except Exception as exc:
        return exc, budget.snapshot()
    return None, budget.snapshot()


def test_length_with_empty_content_keeps_finish_usage_and_stream_summary():
    data = asyncio.run(_collect(_sse(
        '{"id":"s1","model":"deepseek-flash","choices":[{"delta":{},"finish_reason":"length"}]}',
        '{"id":"s1","choices":[],"usage":{"prompt_tokens":1137,"completion_tokens":1000,"total_tokens":2137}}',
        "[DONE]",
    )))
    summary = data["stream_summary"]
    assert summary["finish_reason"] == "length"
    assert summary["content_chars"] == 0
    assert summary["done"] is True
    assert summary["usage_present"] is True
    error, ledger = _complete(data)
    assert isinstance(error, LlmResponseTruncatedError)
    assert error.finish_reason == "length"
    assert error.stream_summary["event_count"] == 2
    attempt = ledger["attempts"][0]
    assert attempt["charged_tokens"] == 2137
    assert attempt["finish_reason"] == "length"
    assert attempt["error_code"] == "response_truncated"
    assert attempt["usage"]["output_tokens"] == 1000


def test_stop_with_empty_content_stays_a_parse_failure():
    data = asyncio.run(_collect(_sse(
        '{"id":"s2","choices":[{"delta":{"content":""},"finish_reason":"stop"}]}',
        "[DONE]",
    )))
    error, ledger = _complete(data)
    assert isinstance(error, LlmResponseParseError)
    assert not isinstance(error, LlmResponseTruncatedError)
    assert error.finish_reason == "stop"
    assert "content is empty" in error.message
    assert ledger["attempts"][0]["charged_tokens"] != 0 or ledger["attempts"][0]["usage_status"] == "unknown_reserved"
    assert ledger["attempts"][0]["error_code"] == "response_parse_error"
    assert ledger["attempts"][0]["finish_reason"] == "stop"


def test_content_fragments_ignore_reasoning_and_keep_the_answer():
    data = asyncio.run(_collect(_sse(
        '{"id":"s3","choices":[{"delta":{"reasoning_content":"draft the answer"},"finish_reason":null}]}',
        '{"id":"s3","choices":[{"delta":{"content":"{\\"label\\":\\"ok\\","},"finish_reason":null}]}',
        '{"id":"s3","choices":[{"delta":{"content":"\\"score\\":1}"},"finish_reason":"stop"}]}',
        '{"id":"s3","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}',
        "[DONE]",
    )))
    assert data["choices"][0]["message"]["content"] == '{"label":"ok","score":1}'
    assert "draft" not in data["choices"][0]["message"]["content"]
    assert data["stream_summary"]["reasoning_present"] is True
    assert data["stream_summary"]["content_fragments"] == 2
    error, _ledger = _complete(data)
    assert error is None


def test_reasoning_only_is_not_accepted_as_the_answer():
    data = asyncio.run(_collect(_sse(
        '{"id":"s4","choices":[{"delta":{"reasoning_content":"only thinking"},"finish_reason":"stop"}]}',
        "[DONE]",
    )))
    assert data["choices"][0]["message"]["content"] == ""
    assert data["stream_summary"]["reasoning_present"] is True
    assert data["stream_summary"]["content_chars"] == 0
    error, ledger = _complete(data)
    assert isinstance(error, LlmResponseParseError)
    assert "reasoning is not the answer" in error.message
    assert ledger["attempts"][0]["stream_summary"]["reasoning_chars"] == len("only thinking")


def test_missing_finish_and_trailing_usage_stay_visible():
    data = asyncio.run(_collect(_sse(
        '{"id":"s5","choices":[{"delta":{"content":""}}]}',
        '{"id":"s5","choices":[],"usage":{"prompt_tokens":4,"completion_tokens":0,"total_tokens":4}}',
        "[DONE]",
    )))
    assert data["stream_summary"]["finish_reason"] is None
    assert data["stream_summary"]["usage_present"] is True
    error, ledger = _complete(data)
    assert isinstance(error, LlmResponseParseError)
    assert error.finish_reason is None
    assert "finish_reason is missing" in error.message
    assert ledger["attempts"][0]["usage"]["total_tokens"] == 4
    assert "finish_reason" not in ledger["attempts"][0]


def test_invalid_sse_json_keeps_the_events_already_seen():
    with pytest.raises(LlmResponseParseError, match="invalid streaming JSON") as caught:
        asyncio.run(_collect(_sse(
            '{"id":"s6","choices":[{"delta":{"content":"{"},"finish_reason":null}]}',
            "not-json",
        )))
    assert caught.value.stream_summary["event_count"] == 1
    assert caught.value.stream_summary["done"] is False


def test_bad_json_and_schema_failure_keep_finish_without_clearing_the_ledger():
    bad_json = _response("not-json", usage={"prompt_tokens":2, "completion_tokens":1, "total_tokens":3})
    bad_json["choices"][0]["message"]["content"] = "not-json"
    bad_json["choices"][0]["finish_reason"] = "stop"
    error, ledger = _complete(bad_json)
    assert isinstance(error, LlmResponseParseError)
    assert error.finish_reason == "stop"
    assert ledger["attempts"][0]["charged_tokens"] == 3
    assert ledger["attempts"][0]["error_code"] == "response_parse_error"

    schema_fail = _response({"label": "ok"}, usage={"prompt_tokens":2, "completion_tokens":1, "total_tokens":3})
    error, ledger = _complete(schema_fail)
    assert isinstance(error, LlmSchemaValidationError)
    assert error.finish_reason == "stop"
    assert ledger["attempts"][0]["charged_tokens"] == 3
    assert ledger["attempts"][0]["error_code"] == "schema_validation_error"
