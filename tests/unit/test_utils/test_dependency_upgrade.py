from utils.akshare_upgrade import build_upgrade_report
from utils.dependency_upgrade import (
    ordered_upgrade_names,
    parse_package_spec,
    upgrade_one_package,
)


def test_parse_package_spec_accepts_name_and_pin():
    assert parse_package_spec("pytdx") == ("pytdx", None)
    assert parse_package_spec("yfinance==1.8.0") == ("yfinance", "1.8.0")


def test_ordered_upgrade_names_puts_transport_before_sources():
    assert ordered_upgrade_names(
        ["yfinance", "akshare", "curl_cffi", "akshare", "pytdx"]
    ) == ["curl_cffi", "akshare", "pytdx", "yfinance"]


def test_upgrade_one_package_pins_only_after_smoke_passes():
    calls = []

    def install(version, reinstall):
        calls.append((version, reinstall))
        return 0

    result = upgrade_one_package(
        name="pytdx",
        distribution="pytdx",
        previous_version="1.72",
        target_version="1.73",
        install=install,
        run_smoke=lambda: {
            "status": "success",
            "failed": 0,
            "cases": [{"name": "daily_bars", "ok": True}],
        },
        pin=lambda version: {"action": "replaced", "pin_line": f"pytdx=={version}"},
    )

    assert result["outcome"] == "success"
    assert calls == [("1.73", False)]
    assert result["requirements"]["pin_line"] == "pytdx==1.73"


def test_upgrade_one_package_rolls_back_when_smoke_fails():
    calls = []
    pins = []

    def install(version, reinstall):
        calls.append((version, reinstall))
        return 0

    smokes = iter(
        [
            {"status": "failed", "failed": 1, "cases": [{"name": "history_k", "ok": False}]},
            {"status": "success", "failed": 0, "cases": [{"name": "history_k", "ok": True}]},
        ]
    )
    result = upgrade_one_package(
        name="baostock",
        distribution="baostock",
        previous_version="0.9.4",
        target_version="0.9.5",
        install=install,
        run_smoke=lambda: next(smokes),
        pin=lambda version: pins.append(version) or {"action": "replaced"},
    )

    assert result["outcome"] == "failed"
    assert result["stage"] == "smoke"
    assert calls == [("0.9.5", False), ("0.9.4", True)]
    assert pins == []
    assert result["rollback_smoke"]["status"] == "success"


def test_upgrade_one_package_rolls_back_when_install_fails():
    calls = []

    def install(version, reinstall):
        calls.append((version, reinstall))
        return 1 if version == "1.9.0" else 0

    result = upgrade_one_package(
        name="yfinance",
        distribution="yfinance",
        previous_version="1.8.0",
        target_version="1.9.0",
        install=install,
        run_smoke=lambda: {"status": "success", "failed": 0, "cases": []},
        pin=lambda version: {"action": "replaced"},
    )

    assert result["outcome"] == "failed"
    assert result["stage"] == "install"
    assert calls == [("1.9.0", False), ("1.8.0", True)]
    assert "requirements" not in result


def test_build_upgrade_report_names_the_package():
    report = build_upgrade_report(
        package="curl_cffi",
        outcome="failed",
        previous_version="0.17.0",
        target_version="0.18.0",
        smoke_summary={"cases": [{"name": "chrome_tls_session", "ok": False, "error": "TypeError"}]},
        restart_status="issued",
        rollback_smoke_summary={"cases": []},
    )
    assert report.startswith("curl_cffi 自动升级 失败")
    assert "chrome_tls_session" in report
