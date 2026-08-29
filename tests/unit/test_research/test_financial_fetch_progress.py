from research.financial_fetch_progress import (
    DEFAULT_FINANCIAL_FETCH_PROGRESS_INTERVAL,
    format_financial_fetch_progress,
    should_emit_financial_fetch_progress,
)
from research.providers import akshare_financial_statements as akshare_module
from research.providers.akshare_financial_statements import (
    AkshareFinancialStatementsProvider,
)
from research.providers import official_financial_filings as official_module
from research.providers.official_financial_filings import (
    ConfiguredOfficialFinancialFilingProvider,
)


class _ListLogger:
    def __init__(self):
        self.messages = []

    def info(self, message, *args):
        if args:
            self.messages.append(message % args)
        else:
            self.messages.append(str(message))


def test_progress_emits_first_interval_and_last():
    assert DEFAULT_FINANCIAL_FETCH_PROGRESS_INTERVAL == 200
    assert should_emit_financial_fetch_progress(1, 1206) is True
    assert should_emit_financial_fetch_progress(199, 1206) is False
    assert should_emit_financial_fetch_progress(200, 1206) is True
    assert should_emit_financial_fetch_progress(400, 1206) is True
    assert should_emit_financial_fetch_progress(1206, 1206) is True
    assert should_emit_financial_fetch_progress(0, 1206) is False
    assert should_emit_financial_fetch_progress(1, 0) is False


def test_progress_message_uses_n_of_m():
    message = format_financial_fetch_progress(
        channel="ths_sina",
        processed=200,
        total=1206,
        elapsed_seconds=81.2,
        exchange="SSE",
        report_period="2026-06-30",
    )

    assert message == (
        "[FinancialFetch] ths_sina progress 200/1206 "
        "exchange=SSE report_period=2026-06-30 elapsed=81.2s"
    )


def test_ths_sina_fetch_logs_first_interval_and_last(monkeypatch):
    provider = AkshareFinancialStatementsProvider({})
    instruments = [
        {"instrument_id": f"{index:06d}.SZ", "type": "stock", "exchange": "SZSE"}
        for index in range(201)
    ]
    logger = _ListLogger()
    monkeypatch.setattr(akshare_module, "dm_logger", logger)
    monkeypatch.setattr(provider, "_akshare", lambda mode: object())
    monkeypatch.setattr(
        provider,
        "_fetch_target_period_bundles_with_statement_fallback",
        lambda *args, **kwargs: [],
    )

    provider._fetch_financial_statement_bundles_sync(
        instruments,
        "direct",
        ["2026-06-30"],
    )

    assert any("ths_sina start instruments=201" in message for message in logger.messages)
    progress = [message for message in logger.messages if "ths_sina progress" in message]
    assert any("1/201" in message for message in progress)
    assert any("200/201" in message for message in progress)
    assert any("201/201" in message for message in progress)


def test_official_fetch_logs_first_interval_and_last(monkeypatch):
    class _EmptySession:
        def get(self, *args, **kwargs):
            raise AssertionError("official progress test should not perform HTTP")

    provider = ConfiguredOfficialFinancialFilingProvider(
        source_name="cninfo",
        source_config={"request_interval_seconds": 0},
        session=_EmptySession(),
    )
    instruments = [
        {
            "instrument_id": f"{index:06d}.SZ",
            "symbol": f"{index:06d}",
            "exchange": "SZSE",
        }
        for index in range(201)
    ]
    logger = _ListLogger()
    monkeypatch.setattr(official_module, "dm_logger", logger)

    provider._fetch_sync(instruments, "SZSE", ["2026-06-30"], "direct")

    assert any("cninfo start instruments=201" in message for message in logger.messages)
    progress = [message for message in logger.messages if "cninfo progress" in message]
    assert any("1/201" in message for message in progress)
    assert any("200/201" in message for message in progress)
    assert any("201/201" in message for message in progress)
