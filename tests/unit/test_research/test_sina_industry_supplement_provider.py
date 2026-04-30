from research.providers.sina_industry_supplement import (
    SinaIndustryNameSupplementProvider,
)


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload
        self.encoding = "utf-8"
        self.apparent_encoding = "utf-8"
        self.content = payload.encode("utf-8")

    @property
    def text(self):
        return self.payload

    def raise_for_status(self):
        return None


class _FakeSession:
    calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return _FakeResponse(
            """
            <html><body>
              <table>
                <tr><th>指数名称</th><th>指数代码</th><th>进入日期</th><th>退出日期</th></tr>
                <tr><td>申万制造</td><td>801250</td><td>2026-04-01</td><td>&nbsp;</td></tr>
                <tr><td>面板</td><td>850831</td><td>2026-04-01</td><td>&nbsp;</td></tr>
                <tr><td>申万Ａ指</td><td>801003</td><td>2026-04-01</td><td>&nbsp;</td></tr>
                <tr><td>电子</td><td>801080</td><td>2026-04-01</td><td>&nbsp;</td></tr>
                <tr><td>光学光电子</td><td>801084</td><td>2026-04-01</td><td>&nbsp;</td></tr>
                <tr><td>AMAC电子</td><td>H30066</td><td>2026-04-09</td><td>&nbsp;</td></tr>
                <tr><td>旧行业</td><td>850999</td><td>2024-01-01</td><td>2025-01-01</td></tr>
              </table>
            </body></html>
            """
        )


def test_sina_industry_supplement_parses_current_shenwan_leaf_candidates():
    candidates = SinaIndustryNameSupplementProvider._parse_shenwan_candidates(
        _FakeSession().get("https://example.test").text
    )

    assert [item["industry_name"] for item in candidates] == [
        "面板",
        "电子",
        "光学光电子",
    ]
    assert candidates[0]["industry_code"] == "850831.SI"
    assert all("申万" not in item["industry_name"] for item in candidates)
    assert all(item["industry_name"] != "旧行业" for item in candidates)


def test_sina_industry_supplement_fetches_hints_with_sina_stockid(monkeypatch):
    _FakeSession.calls = []
    monkeypatch.setattr(
        "research.providers.sina_industry_supplement.requests.Session",
        lambda: _FakeSession(),
    )

    provider = SinaIndustryNameSupplementProvider(
        request_interval_seconds=0,
        retry_attempts=1,
    )
    result = provider._fetch_industry_name_hints_sync(
        [
            {
                "instrument_id": "688781.SH",
                "symbol": "688781",
                "exchange": "SSE",
                "type": "stock",
            }
        ],
        "SSE",
        "direct",
    )

    assert [item.industry_name for item in result] == [
        "面板",
        "电子",
        "光学光电子",
    ]
    assert result[0].source == "sina"
    assert result[0].source_classification == "新浪财经相关资料"
    assert result[0].raw_payload["selected_candidate"]["industry_code"] == "850831.SI"
    assert _FakeSession.calls[0]["url"].endswith("/stockid/sh688781.phtml")
