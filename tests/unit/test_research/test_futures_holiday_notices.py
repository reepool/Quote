from datetime import datetime
from zoneinfo import ZoneInfo

from research.futures_market_data import (
    FuturesOfficialCalendarBackfillService,
    FuturesStorageManager,
    FuturesTradingCalendarDay,
    FuturesTradingDayGovernanceService,
)
from research.providers.official_futures import OfficialFuturesDailyProbeResult
from research.providers.official_futures_calendar import (
    parse_holiday_notice_text,
    select_holiday_notice_target,
)
from utils.config_manager import ResearchConfig, ResearchStorageConfig


NOTICE_TEXT = """
上海期货交易所关于2026年休市安排的公告
六、中秋节：9月25日（星期五）至9月27日（星期日）休市，9月28日（星期一）起照常开市。9月24日（星期四）晚上不进行夜盘交易。
七、国庆节：10月1日（星期四）至10月7日（星期三）休市，10月8日（星期四）起照常开市。
2025年12月31日（星期三）晚上不进行夜盘交易。
2月8日（星期日）照常开市。
"""


def _config(tmp_path):
    return ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(db_path=str(tmp_path / "research.db")),
        modules={
            "commodity_market_data": {
                "enabled": True,
                "storage": {"database": str(tmp_path / "futures.db")},
                "trading_day_governance": {
                    "enabled_exchanges": ["SHFE"],
                    "publication_policy": {
                        "repair_lookback_days": 5,
                        "exchanges": {"SHFE": {"timezone": "Asia/Shanghai", "cutoff": "18:00"}},
                    },
                    "official_calendar_backfill": {"retry_unresolved_passes": 0},
                    "holiday_notices": {
                        "enabled": True,
                        "exchanges": {"SHFE": {"notice_url": "https://example.test/shfe-2026"}},
                    },
                },
                "sources": {"exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"]}},
            }
        },
        sources={},
    )


def test_holiday_notice_parser_reads_ranges_night_session_and_explicit_year():
    parsed = parse_holiday_notice_text(NOTICE_TEXT)

    assert "2026-09-25" in parsed["closed_dates"]
    assert "2026-09-26" in parsed["closed_dates"]
    assert "2026-09-27" in parsed["closed_dates"]
    assert "2026-10-01" in parsed["closed_dates"]
    assert "2026-10-07" in parsed["closed_dates"]
    assert "2026-10-08" not in parsed["closed_dates"]
    assert parsed["night_session_suspensions"] == ["2025-12-31", "2026-09-24"]
    assert parsed["open_dates"] == ["2026-02-08"]
    assert parsed["confident"] is True


def test_weekend_rule_keeps_notice_opened_weekend_when_notice_refresh_is_unavailable(
    monkeypatch,
    tmp_path,
):
    config = _config(tmp_path)
    config.modules["commodity_market_data"]["trading_day_governance"]["holiday_notices"] = {
        "enabled": True,
        "exchanges": {"SHFE": {}},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="SHFE",
            trade_date="2026-02-08",
            is_trading_day=True,
            source_profile="exchange_official_holiday_notice",
            quality_flag="backfilled_verified",
            metadata={"classification_rule": "official_holiday_notice", "notice_opened_weekend": True},
        )
    ])

    def _probe(self, exchange, trade_date):
        raise AssertionError(f"weekend should not be probed: {trade_date}")

    monkeypatch.setattr(
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.probe_exchange_trading_day",
        _probe,
    )
    FuturesOfficialCalendarBackfillService(
        storage,
        config,
        config.modules["commodity_market_data"],
        now_provider=lambda: datetime(2026, 2, 9, 21, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
    ).run(exchanges=["SHFE"], start_date="2026-02-08", end_date="2026-02-08")
    stored = storage.list_calendar_days(exchange="SHFE", start_date="2026-02-08", end_date="2026-02-08")[0]

    assert stored["is_trading_day"] is True
    assert stored["metadata"]["classification_rule"] == "official_holiday_notice"


def test_dce_adopts_parsed_shfe_holiday_notice_without_probing_the_closure(monkeypatch, tmp_path):
    config = _config(tmp_path)
    config.modules["commodity_market_data"]["trading_day_governance"]["enabled_exchanges"] = ["SHFE", "DCE"]
    config.modules["commodity_market_data"]["trading_day_governance"]["holiday_notices"] = {
        "enabled": True,
        "exchanges": {
            "SHFE": {"notice_url": "https://example.test/shfe-2026"},
            "DCE": {"adopt_notice_from": "SHFE"},
        },
    }
    config.modules["commodity_market_data"]["trading_day_governance"]["publication_policy"]["exchanges"]["DCE"] = {
        "timezone": "Asia/Shanghai",
        "cutoff": "18:00",
    }
    config.modules["commodity_market_data"]["sources"]["exchange_official"]["enabled_exchanges"] = ["SHFE", "DCE"]
    storage = FuturesStorageManager(config)
    storage.initialize()
    FuturesTradingDayGovernanceService(
        storage,
        config.modules["commodity_market_data"],
    ).apply_holiday_notice_text(
        exchange="SHFE",
        url="https://example.test/shfe-2026",
        text=NOTICE_TEXT,
    )
    probed = []

    def _fetch(self, exchange):
        raise AssertionError(f"{exchange} should adopt the stored SHFE notice")

    def _probe(self, exchange, trade_date):
        probed.append((exchange, trade_date))
        return OfficialFuturesDailyProbeResult(
            exchange=exchange,
            trade_date=trade_date,
            status="trading",
            is_trading_day=True,
            row_count=1,
            source_interface="fixture",
            evidence_url=f"https://official.example/{trade_date}",
            parser_version="fixture.v1",
            metadata={"classification_rule": "official_daily_rows"},
        )

    monkeypatch.setattr(
        "research.providers.official_futures_calendar.OfficialFuturesCalendarProvider.fetch_holiday_notice_text",
        _fetch,
    )
    monkeypatch.setattr(
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.probe_exchange_trading_day",
        _probe,
    )
    result = FuturesOfficialCalendarBackfillService(
        storage,
        config,
        config.modules["commodity_market_data"],
        now_provider=lambda: datetime(2026, 9, 25, 21, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
    ).run(exchanges=["DCE"], start_date="2026-09-25", end_date="2026-09-25")
    stored = storage.list_calendar_days(exchange="DCE", start_date="2026-09-25", end_date="2026-09-25")[0]

    assert result["status"] == "success"
    assert probed == []
    assert stored["is_trading_day"] is False
    assert stored["metadata"]["adopted_from_exchange"] == "SHFE"
    assert stored["evidence_url"] == "https://example.test/shfe-2026"
    parsed = parse_holiday_notice_text("关于调整保证金的通知，不涉及休市日期。", notice_year=2026)

    assert parsed["confident"] is False
    assert parsed["closed_dates"] == []


def test_apply_holiday_notice_keeps_verified_trading_day_and_writes_other_closures(tmp_path):
    config = _config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="SHFE",
            trade_date="2026-09-24",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
            metadata={"classification_rule": "official_daily_rows"},
        ),
        FuturesTradingCalendarDay(
            exchange="SHFE",
            trade_date="2026-09-25",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
            metadata={"classification_rule": "official_daily_rows"},
        ),
    ])
    service = FuturesTradingDayGovernanceService(storage, config.modules["commodity_market_data"])

    result = service.apply_holiday_notice_text(
        exchange="SHFE",
        url="https://example.test/shfe-2026",
        text=NOTICE_TEXT,
    )
    stored = {
        item["trade_date"]: item
        for item in storage.list_calendar_days(exchange="SHFE", start_date="2026-09-24", end_date="2026-10-08")
    }

    assert "2026-09-25" in result["review_dates"]
    assert stored["2026-09-24"]["is_trading_day"] is True
    assert stored["2026-09-24"]["metadata"]["night_session_suspended"] is True
    assert stored["2026-09-25"]["is_trading_day"] is True
    assert stored["2026-09-26"]["is_trading_day"] is False
    assert stored["2026-09-26"]["metadata"]["classification_rule"] == "official_holiday_notice"
    assert stored["2026-10-01"]["metadata"]["classification_rule"] == "official_holiday_notice"
    opened = storage.list_calendar_days(exchange="SHFE", start_date="2026-02-08", end_date="2026-02-08")
    assert opened[0]["is_trading_day"] is True


def test_unchanged_notice_hash_refreshes_closed_dates(tmp_path):
    config = _config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()
    service = FuturesTradingDayGovernanceService(storage, config.modules["commodity_market_data"])
    first = service.apply_holiday_notice_text(exchange="SHFE", url="https://example.test/shfe-2026", text=NOTICE_TEXT)
    second = service.apply_holiday_notice_text(exchange="SHFE", url="https://example.test/shfe-2026", text=NOTICE_TEXT)

    assert first["notice_id"] == second["notice_id"]
    assert storage.list_calendar_notices(exchange="SHFE")
    assert len(storage.list_calendar_notices(exchange="SHFE")) == 1


def test_backfill_skips_notice_holiday_and_keeps_uncovered_404_unresolved(monkeypatch, tmp_path):
    config = _config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()
    probed = []

    def _fetch(self, exchange):
        return {"exchange": exchange, "url": "https://example.test/shfe-2026", "text": NOTICE_TEXT}

    def _probe(self, exchange, trade_date):
        probed.append(trade_date)
        if trade_date == "2026-09-28":
            return OfficialFuturesDailyProbeResult(
                exchange=exchange,
                trade_date=trade_date,
                status="trading",
                is_trading_day=True,
                row_count=2,
                source_interface="fixture",
                evidence_url="https://official.example/2026-09-28",
                parser_version="fixture.v1",
                metadata={"classification_rule": "official_daily_rows"},
            )
        return OfficialFuturesDailyProbeResult(
            exchange=exchange,
            trade_date=trade_date,
            status="unresolved",
            is_trading_day=None,
            row_count=0,
            source_interface="fixture",
            evidence_url="https://official.example/missing",
            parser_version="fixture.v1",
            failure_reason=f"404 Client Error: Not Found for url: {trade_date}",
            metadata={"classification_rule": "official_no_report_unresolved"},
        )

    monkeypatch.setattr(
        "research.providers.official_futures_calendar.OfficialFuturesCalendarProvider.fetch_holiday_notice_text",
        _fetch,
    )
    monkeypatch.setattr(
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.probe_exchange_trading_day",
        _probe,
    )
    result = FuturesOfficialCalendarBackfillService(
        storage,
        config,
        config.modules["commodity_market_data"],
        now_provider=lambda: datetime(2026, 9, 29, 21, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
    ).run(exchanges=["SHFE"], start_date="2026-09-24", end_date="2026-09-29")

    assert "2026-09-25" not in probed
    assert "2026-09-28" in probed
    assert "2026-09-29" in probed
    assert result["status"] == "blocked"
    reasons = " ".join(
        sample["reason"] for sample in result["exchanges"][0]["failure_samples"]
    )
    assert "2026-09-25" not in reasons
    assert "2026-09-29" in reasons or "post_cutoff" in reasons


def test_backfill_holiday_does_not_block_later_trading_day(monkeypatch, tmp_path):
    config = _config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()
    probed = []

    def _fetch(self, exchange):
        return {"exchange": exchange, "url": "https://example.test/shfe-2026", "text": NOTICE_TEXT}

    def _probe(self, exchange, trade_date):
        probed.append(trade_date)
        return OfficialFuturesDailyProbeResult(
            exchange=exchange,
            trade_date=trade_date,
            status="trading",
            is_trading_day=True,
            row_count=2,
            source_interface="fixture",
            evidence_url=f"https://official.example/{trade_date}",
            parser_version="fixture.v1",
            metadata={"classification_rule": "official_daily_rows"},
        )

    monkeypatch.setattr(
        "research.providers.official_futures_calendar.OfficialFuturesCalendarProvider.fetch_holiday_notice_text",
        _fetch,
    )
    monkeypatch.setattr(
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.probe_exchange_trading_day",
        _probe,
    )
    result = FuturesOfficialCalendarBackfillService(
        storage,
        config,
        config.modules["commodity_market_data"],
        now_provider=lambda: datetime(2026, 9, 28, 21, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
    ).run(exchanges=["SHFE"], start_date="2026-09-25", end_date="2026-09-28")

    assert result["status"] == "success"
    assert "2026-09-25" not in probed
    assert probed == ["2026-09-28"]


def test_backfill_parse_failure_does_not_invent_closures(monkeypatch, tmp_path):
    config = _config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()

    def _fetch(self, exchange):
        return {"exchange": exchange, "url": "https://example.test/bad", "text": "无休市安排"}

    def _probe(self, exchange, trade_date):
        return OfficialFuturesDailyProbeResult(
            exchange=exchange,
            trade_date=trade_date,
            status="trading",
            is_trading_day=True,
            row_count=1,
            source_interface="fixture",
            evidence_url="https://official.example/day",
            parser_version="fixture.v1",
            metadata={"classification_rule": "official_daily_rows"},
        )

    monkeypatch.setattr(
        "research.providers.official_futures_calendar.OfficialFuturesCalendarProvider.fetch_holiday_notice_text",
        _fetch,
    )
    monkeypatch.setattr(
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.probe_exchange_trading_day",
        _probe,
    )
    result = FuturesOfficialCalendarBackfillService(
        storage,
        config,
        config.modules["commodity_market_data"],
        now_provider=lambda: datetime(2026, 9, 25, 21, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
    ).run(exchanges=["SHFE"], start_date="2026-09-25", end_date="2026-09-25")
    stored = storage.list_calendar_days(exchange="SHFE", start_date="2026-09-25", end_date="2026-09-25")

    assert result["exchanges"][0]["holiday_notice"]["status"] == "review_required"
    assert stored[0]["is_trading_day"] is True
    assert stored[0]["metadata"]["classification_rule"] == "official_daily_rows"


def test_gfex_adopts_newer_shfe_notice_and_warns(monkeypatch, tmp_path):
    config = _config(tmp_path)
    governance = config.modules["commodity_market_data"]["trading_day_governance"]
    governance["enabled_exchanges"] = ["SHFE", "GFEX"]
    governance["publication_policy"]["exchanges"]["GFEX"] = {"timezone": "Asia/Shanghai", "cutoff": "18:00"}
    governance["holiday_notices"] = {
        "enabled": True,
        "exchanges": {
            "SHFE": {"listing_url": "https://example.test/shfe"},
            "GFEX": {
                "listing_url": "https://example.test/gfex-list",
                "notice_url": "https://example.test/gfex-2026",
                "adopt_notice_from": "SHFE",
            },
        },
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    service = FuturesOfficialCalendarBackfillService(
        storage,
        config,
        config.modules["commodity_market_data"],
        now_provider=lambda: datetime(2027, 1, 4, 21, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    service._holiday_notice_cache = {
        "SHFE": {
            "status": "parsed",
            "source_text": "上海期货交易所关于2027年休市安排的公告\n元旦：1月1日（星期五）至1月3日（星期日）休市。",
            "source_url": "https://example.test/shfe-2027",
        }
    }

    def _fetch(self, exchange):
        assert exchange == "GFEX"
        return {
            "exchange": exchange,
            "url": "https://example.test/gfex-2026",
            "text": "广州期货交易所关于2026年休市安排的通知\n中秋节：9月25日（星期五）至9月27日（星期日）休市。",
        }

    monkeypatch.setattr(
        "research.providers.official_futures_calendar.OfficialFuturesCalendarProvider.fetch_holiday_notice_text",
        _fetch,
    )
    refreshed = service._refresh_holiday_notice("GFEX", dry_run=True)

    assert refreshed["status"] == "parsed"
    assert refreshed["adopted_from_exchange"] == "SHFE"
    assert "2027-01-01" in refreshed["closed_dates"]
    assert "未发现新年度休市安排" in refreshed["warning"]
    assert "请核对GFEX新通知地址" in refreshed["warning"]


def test_listing_page_selects_the_newest_holiday_notice():
    html = """
    <a href="/2026/holiday.html">关于2026年休市安排的通知</a>
    <a href="/2027/holiday.html">关于2027年休市安排的通知</a>
    """
    selected = select_holiday_notice_target(html, "http://www.gfex.com.cn/gfex/tzts/list_tzts.shtml")

    assert selected["url"] == "http://www.gfex.com.cn/2027/holiday.html"
    assert selected["notice_year"] == 2027


def test_holiday_page_body_is_used_without_following_links():
    selected = select_holiday_notice_target(
        NOTICE_TEXT,
        "https://www.shfe.com.cn/services/calenderandholidays/holiday/",
    )

    assert selected["url"] == "https://www.shfe.com.cn/services/calenderandholidays/holiday/"
    assert selected["notice_year"] == 2026
