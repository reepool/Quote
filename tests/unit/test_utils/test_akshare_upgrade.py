from __future__ import annotations

import json
from pathlib import Path

import pytest

from utils.akshare_upgrade import (
    build_upgrade_report,
    extract_telegram_targets,
    parse_smoke_summary,
    summarize_smoke,
    update_requirements_pin,
)


def test_extract_telegram_targets_reads_singular_chat_id():
    token, chats = extract_telegram_targets(
        {"bot_token": "abc", "chat_id": ["123", "456"]}
    )
    assert token == "abc"
    assert chats == ["123", "456"]


def test_extract_telegram_targets_prefers_chat_ids_and_scalar():
    token, chats = extract_telegram_targets(
        {"bot_token": "abc", "chat_ids": ["1"], "chat_id": ["2"]}
    )
    assert chats == ["1"]
    token, chats = extract_telegram_targets({"bot_token": "abc", "chat_id": 471105519})
    assert chats == ["471105519"]


def test_extract_telegram_targets_empty_when_missing():
    assert extract_telegram_targets({"chat_id": ["1"]}) == (None, ["1"])
    assert extract_telegram_targets({"bot_token": "abc"}) == ("abc", [])
    assert extract_telegram_targets({}) == (None, [])


def test_update_requirements_pin_replaces_bare_name(tmp_path: Path):
    requirements = tmp_path / "requirements.txt"
    requirements.write_text(
        "yfinance==0.2.50\nakshare\naiohttp>=3.11.13\n", encoding="utf-8"
    )
    result = update_requirements_pin(requirements, "akshare", "1.21.0")
    lines = requirements.read_text(encoding="utf-8").splitlines()
    assert result["action"] == "replaced"
    assert lines == ["yfinance==0.2.50", "akshare==1.21.0", "aiohttp>=3.11.13"]


def test_update_requirements_pin_replaces_old_pin_and_keeps_comment(tmp_path: Path):
    requirements = tmp_path / "requirements.txt"
    requirements.write_text(
        "akshare==1.19.1  # pinned after file_fold incident\npandas\n",
        encoding="utf-8",
    )
    result = update_requirements_pin(requirements, "akshare", "1.20.2")
    lines = requirements.read_text(encoding="utf-8").splitlines()
    assert result["action"] == "replaced"
    assert lines[0] == "akshare==1.20.2  # pinned after file_fold incident"
    assert lines[1] == "pandas"


def test_update_requirements_pin_normalizes_distribution_name(tmp_path: Path):
    requirements = tmp_path / "requirements.txt"
    requirements.write_text(
        "akshare-proxy-patch==0.5.0\n", encoding="utf-8"
    )
    update_requirements_pin(requirements, "akshare-proxy-patch", "0.6.0")
    assert requirements.read_text(encoding="utf-8").splitlines() == [
        "akshare-proxy-patch==0.6.0"
    ]


def test_update_requirements_pin_appends_when_missing(tmp_path: Path):
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("pandas\n", encoding="utf-8")
    result = update_requirements_pin(requirements, "akshare", "1.21.0")
    assert result["action"] == "appended"
    assert requirements.read_text(encoding="utf-8").splitlines()[-1] == "akshare==1.21.0"


def test_parse_smoke_summary_takes_last_json_line():
    payload = "loading akshare...\nwarning noise\n" + json.dumps(
        {"status": "success", "passed": 6, "failed": 0, "cases": [{"name": "a", "ok": True}]}
    )
    summary = parse_smoke_summary(payload)
    assert summary["status"] == "success"
    assert summary["passed"] == 6


def test_parse_smoke_summary_tolerates_missing_output():
    assert parse_smoke_summary(None)["status"] == "missing_output"
    assert parse_smoke_summary("not json")["status"] == "invalid_output"


def test_summarize_smoke_lists_failed_cases():
    summary = {
        "cases": [
            {"name": "ok_case", "ok": True},
            {"name": "bad_case", "ok": False, "error": "AssertionError: missing columns"},
        ]
    }
    text = summarize_smoke(summary)
    assert "passed=1" in text
    assert "failed=1" in text
    assert "bad_case" in text


def test_build_upgrade_report_contains_key_facts():
    report = build_upgrade_report(
        outcome="success",
        previous_version="1.19.1",
        target_version="1.21.0",
        smoke_summary={"cases": [{"name": "lme_sina_cad", "ok": True}]},
        restart_status="issued",
        requirements_action="replaced",
    )
    assert "1.19.1" in report and "1.21.0" in report
    assert "issued" in report
    assert "replaced" in report
    assert "手动执行" not in report


def test_build_upgrade_report_manual_restart_hint():
    report = build_upgrade_report(
        outcome="success",
        previous_version="1.19.1",
        target_version="1.21.0",
        smoke_summary={"cases": []},
        restart_status="manual_required",
    )
    assert "systemctl restart quote-system.service" in report


def test_build_upgrade_report_failure_mentions_rollback():
    report = build_upgrade_report(
        outcome="failed",
        previous_version="1.19.1",
        target_version="1.21.0",
        smoke_summary={"cases": [{"name": "x", "ok": False, "error": "boom"}]},
        restart_status="issued",
        rollback_smoke_summary={"cases": []},
    )
    assert "回滚" in report


@pytest.mark.parametrize(
    "text,expected",
    [
        ("akshare\n", "akshare==1.0.0\n"),
        ("akshare==0.9\n", "akshare==1.0.0\n"),
        ("AKSHARE==0.9\n", "akshare==1.0.0\n"),
        ("akshare[axis]\n", "akshare==1.0.0\n"),
    ],
)
def test_update_requirements_pin_variants(tmp_path: Path, text, expected):
    requirements = tmp_path / "requirements.txt"
    requirements.write_text(text, encoding="utf-8")
    update_requirements_pin(requirements, "akshare", "1.0.0")
    assert requirements.read_text(encoding="utf-8") == expected
