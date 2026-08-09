import pytest

from data_sources.sina_factor_parser import (
    SinaFactorParseError,
    parse_sina_qfq_factor_response,
)


def _payload(data, *, total=None, quote='"'):
    rows = ",".join(
        f"[{quote}{date}{quote},{quote}{factor}{quote}]" for date, factor in data
    )
    total_field = f"{quote}total{quote}:{len(data) if total is None else total},"
    return f"var hkfactor={{ {total_field}{quote}data{quote}:[{rows}] }};"


def test_parser_accepts_json_and_single_quoted_assignment_payloads():
    rows = [("1900-01-01", "0.5"), ("2025-05-16", "1.0")]
    assert parse_sina_qfq_factor_response(
        _payload(rows), http_status=200
    ) == [
        {"date": "1900-01-01", "qfq_factor": "0.5"},
        {"date": "2025-05-16", "qfq_factor": "1.0"},
    ]
    assert len(
        parse_sina_qfq_factor_response(
            _payload(rows, quote="'"), http_status=200
        )
    ) == 2


def test_base_only_payload_is_a_valid_no_event_source_result():
    assert parse_sina_qfq_factor_response(
        _payload([("1900-01-01", "1")]), http_status=200
    ) == [{"date": "1900-01-01", "qfq_factor": "1"}]


@pytest.mark.parametrize(
    ("body", "status", "code"),
    [
        ("<html>rate limited</html>", 200, "html_response"),
        ("", 200, "empty_body"),
        ("not an assignment", 200, "assignment_shape_invalid"),
        ("var x={broken", 200, "payload_syntax_invalid"),
        (_payload([("2025-01-01", "1")]), 200, "base_row_missing"),
        (_payload([("1900-01-01", "0")]), 200, "row_factor_nonpositive"),
        (_payload([("1900-01-01", "1")], total=2), 200, "declared_total_mismatch"),
        (_payload([("1900-01-01", "1")]), 503, "http_status_invalid"),
    ],
)
def test_parser_classifies_malformed_upstream_responses(body, status, code):
    with pytest.raises(SinaFactorParseError) as caught:
        parse_sina_qfq_factor_response(body, http_status=status)
    assert caught.value.code == code
    assert caught.value.response_hash


def test_parser_rejects_oversized_and_code_like_payloads_without_execution():
    with pytest.raises(SinaFactorParseError, match="size bound"):
        parse_sina_qfq_factor_response(
            "var x=" + " " * 200,
            http_status=200,
            max_body_bytes=20,
        )
    with pytest.raises(SinaFactorParseError) as caught:
        parse_sina_qfq_factor_response(
            "var x=__import__('os').system('id');",
            http_status=200,
            require_base_date=False,
        )
    assert caught.value.code == "payload_syntax_invalid"
