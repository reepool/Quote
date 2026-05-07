from research.providers.official_financial_filings import (
    ConfiguredOfficialFinancialFilingProvider,
)


class _FakeResponse:
    status_code = 200
    headers = {"Content-Type": "application/json"}
    url = "https://query.sse.com.cn/commonQuery.do"

    def __init__(self, text: str):
        self.text = text
        self.content = text.encode("utf-8")

    def raise_for_status(self):
        return None


class _QueuedSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append({"method": "GET", "url": url, **kwargs})
        return self.responses.pop(0)

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)


def test_sse_context_maps_normalized_quarter_end_dates_to_report_type_ids():
    context_for = ConfiguredOfficialFinancialFilingProvider._context
    instrument = {"instrument_id": "600000.SH", "symbol": "600000"}

    assert context_for(instrument, "SSE", "2024-03-31")["report_type_id"] == "4000"
    assert context_for(instrument, "SSE", "2024-06-30")["report_type_id"] == "1000"
    assert context_for(instrument, "SSE", "2024-09-30")["report_type_id"] == "4400"
    assert context_for(instrument, "SSE", "2024-12-31")["report_type_id"] == "5000"


def test_endpoint_candidates_stop_after_first_structured_payload_by_default():
    provider = ConfiguredOfficialFinancialFilingProvider(
        source_name="sse",
        source_config={
            "endpoint_candidates": [
                _endpoint_candidate("income", "COMMON_MAP_INCOMESTATEMENT_C"),
                _endpoint_candidate("balance", "COMMON_MAP_BALANCESHEET_C"),
            ]
        },
        session=_QueuedSession(
            [
                _structured_json_response("COMMON_MAP_INCOMESTATEMENT_C"),
                _structured_json_response("COMMON_MAP_BALANCESHEET_C"),
            ]
        ),
    )

    payloads = provider._fetch_sync(
        [_instrument()],
        "SSE",
        ["2023-12-31"],
        "direct",
    )

    assert len(payloads) == 1
    assert provider.session.calls[0]["params"]["sqlId"] == "COMMON_MAP_INCOMESTATEMENT_C"


def test_endpoint_candidates_can_collect_all_structured_payloads():
    provider = ConfiguredOfficialFinancialFilingProvider(
        source_name="sse",
        source_config={
            "fetch_all_endpoint_candidates": True,
            "endpoint_candidates": [
                _endpoint_candidate("income", "COMMON_MAP_INCOMESTATEMENT_C"),
                _endpoint_candidate("balance", "COMMON_MAP_BALANCESHEET_C"),
            ],
        },
        session=_QueuedSession(
            [
                _structured_json_response("COMMON_MAP_INCOMESTATEMENT_C"),
                _structured_json_response("COMMON_MAP_BALANCESHEET_C"),
            ]
        ),
    )

    payloads = provider._fetch_sync(
        [_instrument()],
        "SSE",
        ["2023-12-31"],
        "direct",
    )

    assert len(payloads) == 2
    assert [payload.manifest.metadata_json["endpoint_candidate_key"] for payload in payloads] == [
        "income",
        "balance",
    ]
    assert [call["params"]["sqlId"] for call in provider.session.calls] == [
        "COMMON_MAP_INCOMESTATEMENT_C",
        "COMMON_MAP_BALANCESHEET_C",
    ]


def _instrument():
    return {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
        "type": "stock",
        "is_active": True,
    }


def _endpoint_candidate(key: str, sql_id: str):
    return {
        "key": key,
        "enabled": True,
        "url": "https://query.sse.com.cn/commonQuery.do",
        "request": {
            "query_params": {
                "sqlId": sql_id,
                "STOCK_ID": "{symbol}",
                "REPORT_YEAR": "{report_year}",
                "REPORT_PERIOD_ID": "{report_type_id}",
            }
        },
    }


def _structured_json_response(sql_id: str):
    return _FakeResponse(
        '{"sqlId":"%s","result":[{"STOCK_ID":"600000","REPORT_YEAR":"2023","S2020_0010":"1"}]}'
        % sql_id
    )
