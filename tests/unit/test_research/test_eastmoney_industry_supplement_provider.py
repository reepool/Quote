from research.providers.eastmoney_industry_supplement import (
    EastmoneyIndustryNameSupplementProvider,
)


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _FakeSession:
    calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "params": params,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return _FakeResponse(
            {
                "data": {
                    "f57": "600117",
                    "f58": "西宁特钢",
                    "f127": "特钢Ⅱ",
                }
            }
        )


def test_eastmoney_industry_name_supplement_parses_industry_hint(monkeypatch):
    _FakeSession.calls = []
    monkeypatch.setattr(
        "research.providers.eastmoney_industry_supplement.requests.Session",
        lambda: _FakeSession(),
    )

    provider = EastmoneyIndustryNameSupplementProvider(
        request_interval_seconds=0,
        retry_attempts=1,
    )
    result = provider._fetch_industry_name_hints_sync(
        [
            {
                "instrument_id": "600117.SH",
                "symbol": "600117",
                "exchange": "SSE",
                "type": "stock",
            }
        ],
        "SSE",
        "direct",
    )

    assert len(result) == 1
    assert result[0].instrument_id == "600117.SH"
    assert result[0].industry_name == "特钢Ⅱ"
    assert result[0].source == "eastmoney"
    assert _FakeSession.calls[0]["params"]["secid"] == "1.600117"
    assert _FakeSession.calls[0]["params"]["fields"] == "f57,f58,f127"
