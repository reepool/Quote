import json
from pathlib import Path

from research.announcements import AnnouncementQuery, AnnouncementScope


FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "hkex_instrument_master"


class _Response:
    def __init__(self, payload, *, status_code=200, url=""):
        self._payload = payload
        self.status_code = status_code
        self.url = url
        self.text = payload if isinstance(payload, str) else json.dumps(payload)
        self.content = self.text.encode("utf-8")

    def json(self):
        if isinstance(self._payload, str):
            return json.loads(self._payload)
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")


class _Session:
    def __init__(self, payloads=()):
        self.payloads = list(payloads)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "params": dict(params or {}),
                "headers": dict(headers or {}),
                "timeout": timeout,
            }
        )
        if not self.payloads:
            raise AssertionError(f"unexpected GET {url}")
        payload = self.payloads.pop(0)
        if isinstance(payload, BaseException):
            raise payload
        return _Response(payload, url=url)


def _provider(session):
    from research.providers.hkexnews_announcements import HkexnewsAnnouncementProvider

    return HkexnewsAnnouncementProvider(
        source_config={
            "enabled": True,
            "endpoint_url": "https://www1.hkexnews.hk/search/titleSearchServlet.do",
            "warmup_url": "https://www1.hkexnews.hk/search/titlesearch.xhtml",
            "referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=EN",
            "artifact_base_url": "https://www1.hkexnews.hk",
            "request_timeout_seconds": 5.0,
            "request_interval_seconds": 0.0,
            "retry_attempts": 0,
            "markets": ["SEHK"],
        },
        session=session,
    )


def test_hkexnews_category_codes_map_headline_taxonomy():
    from research.announcements.categories import hkexnews_category_options

    assert hkexnews_category_options("trading_halt") == {
        "t1code": "10000",
        "t2code": "17960",
        "t2Gcode": "7",
    }
    assert hkexnews_category_options("trading_suspension")["t2code"] == "17850"
    assert hkexnews_category_options("trading_resumption")["t2code"] == "17650"
    assert hkexnews_category_options("trading_arrangement")["t2code"] == "18540"
    assert hkexnews_category_options("capital_reorganisation")["t2code"] == "18120"
    assert hkexnews_category_options("listing_by_introduction")["t2code"] == "15300"
    assert hkexnews_category_options("withdrawal_of_listing")["t2code"] == "17600"
    assert hkexnews_category_options("cis_matters")["t2code"] == "19400"


def test_hkexnews_provider_parses_servlet_result_string_and_stamps_category():
    payload = json.loads(
        (FIXTURE_DIR / "hkexnews_title_search_halt.json").read_text(encoding="utf-8")
    )
    session = _Session(payloads=[{}, payload])
    provider = _provider(session)
    result = provider.discover(
        AnnouncementQuery(
            purpose_key="instrument_master_hkex_trading_status",
            scope=AnnouncementScope(
                exchange="HKEX",
                market="SEHK",
                category="trading_halt",
                start_date="2026-08-01",
                end_date="2026-08-26",
                page_size=100,
                max_pages=2,
            ),
        )
    )

    by_id = {record.source_announcement_id: record for record in result.records}
    halt = by_id["20260824122101831"]
    resume_like = by_id["20260512090001803"]

    assert result.source == "hkexnews"
    assert result.status == "success"
    assert halt.symbols == ("01831",)
    assert halt.raw_payload["headline_category"] == "trading_halt"
    assert halt.published_at == "2026-08-24T04:21:00+00:00"
    assert resume_like.raw_payload["headline_category"] == "trading_halt"
    assert any(
        call["url"].endswith("titlesearch.xhtml") for call in session.calls
    )
    search_calls = [
        call for call in session.calls if call["url"].endswith("titleSearchServlet.do")
    ]
    assert search_calls[0]["params"]["t2code"] == "17960"
    assert search_calls[0]["params"]["market"] == "SEHK"
    assert search_calls[0]["params"]["fromDate"] == "20260801"
    assert search_calls[0]["params"]["toDate"] == "20260826"


def test_hkexnews_provider_chunks_market_wide_search_by_month():
    empty = {"recordCnt": 0, "hasNextPage": False, "result": "[]"}
    session = _Session(payloads=[{}, empty, empty, empty])
    provider = _provider(session)
    provider.discover(
        AnnouncementQuery(
            purpose_key="instrument_master_hkex_trading_status",
            scope=AnnouncementScope(
                exchange="HKEX",
                market="SEHK",
                category="trading_halt",
                start_date="2026-06-15",
                end_date="2026-08-26",
                page_size=100,
                max_pages=5,
            ),
        )
    )

    search_calls = [
        call for call in session.calls if call["url"].endswith("titleSearchServlet.do")
    ]
    windows = [(call["params"]["fromDate"], call["params"]["toDate"]) for call in search_calls]
    assert windows == [
        ("20260615", "20260630"),
        ("20260701", "20260731"),
        ("20260801", "20260826"),
    ]
