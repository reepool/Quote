from __future__ import annotations

from scripts.compare_company_profile_stage5_models import (
    _coerce_extract_response,
    _jsonable,
    build_parser,
)


def test_comparison_jsonable_accepts_plain_mapping() -> None:
    assert _jsonable({"items": [{"ok": True}]}) == {"items": [{"ok": True}]}


def test_comparison_coerces_json_arrays_to_strict_tuple_contract() -> None:
    response = _coerce_extract_response(
        {
            "schema_version": "company_profile_extract_response.v1",
            "request_id": "request-1",
            "items": [],
        }
    )

    assert response.items == ()


def test_comparison_parser_can_limit_execution_to_one_model() -> None:
    args = build_parser().parse_args(
        [
            "--sample-manifest",
            "manifest.json",
            "--evidence-plan",
            "plan.json",
            "--output",
            "comparison.json",
            "--run-id",
            "comparison-run",
            "--model",
            "gemini-3.8-flash",
        ]
    )

    assert args.models == ["gemini-3.8-flash"]
