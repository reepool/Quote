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


def test_context_resolver_adds_cninfo_org_id_before_endpoint_request():
    provider = ConfiguredOfficialFinancialFilingProvider(
        source_name="cninfo",
        source_config={
            "context_resolvers": [
                {
                    "key": "cninfo_top_search_org_id",
                    "kind": "json_row_template",
                    "enabled": True,
                    "url": "https://www.cninfo.com.cn/new/information/topSearch/query",
                    "request": {
                        "method": "POST",
                        "body_params": {
                            "keyWord": "{symbol}",
                            "maxNum": "10",
                        },
                    },
                    "row_match": {"code": "{symbol}"},
                    "outputs": {
                        "cninfo_org_id": "{orgId}",
                        "cninfo_stock_param": "{code},{orgId}",
                    },
                }
            ],
            "endpoint_candidates": [
                {
                    "key": "cninfo_his_announcement_query",
                    "enabled": True,
                    "url": "https://www.cninfo.com.cn/new/hisAnnouncement/query",
                    "request": {
                        "method": "POST",
                        "body_params": {"stock": "{cninfo_stock_param}"},
                    },
                }
            ],
        },
        session=_QueuedSession(
            [
                _FakeResponse(
                    '[{"code":"000001","orgId":"gssz0000001","zwjc":"PAB"}]'
                ),
                _FakeResponse('{"announcements":[]}'),
            ]
        ),
    )

    payloads = provider._fetch_sync(
        [
            {
                "instrument_id": "000001.SZ",
                "symbol": "000001",
                "exchange": "SZSE",
                "type": "stock",
            }
        ],
        "SZSE",
        ["2025-12-31"],
        "direct",
    )

    assert payloads == []
    assert provider.session.calls[0]["data"]["keyWord"] == "000001"
    assert provider.session.calls[1]["data"]["stock"] == "000001,gssz0000001"


def test_cninfo_data20_context_resolver_fetches_all_statement_candidates():
    provider = ConfiguredOfficialFinancialFilingProvider(
        source_name="cninfo",
        source_config={
            "fetch_all_endpoint_candidates": True,
            "context_resolvers": [
                {
                    "key": "cninfo_data20_company_info_sign",
                    "kind": "json_row_template",
                    "enabled": True,
                    "url": "https://www.cninfo.com.cn/data20/companyOverview/getCompanyInfo",
                    "request": {
                        "method": "GET",
                        "query_params": {"scode": "{symbol}"},
                    },
                    "row_list_path": "data.records",
                    "row_match": {"SECCODE": "{symbol}"},
                    "outputs": {"cninfo_data20_sign": "{F002N}"},
                }
            ],
            "endpoint_candidates": [
                _cninfo_data20_candidate(
                    "income",
                    "https://www.cninfo.com.cn/data20/financialData/getIncomeStatement",
                ),
                _cninfo_data20_candidate(
                    "balance",
                    "https://www.cninfo.com.cn/data20/financialData/getBalanceSheets",
                ),
                _cninfo_data20_candidate(
                    "cash",
                    "https://www.cninfo.com.cn/data20/financialData/getCashFlowStatement",
                ),
            ],
        },
        session=_QueuedSession(
            [
                _FakeResponse(
                    '{"data":{"records":[{"SECCODE":"920833","F002N":1}]}}'
                ),
                _cninfo_data20_response("/financialData/getIncomeStatement", "营业总收入"),
                _cninfo_data20_response("/financialData/getBalanceSheets", "总资产"),
                _cninfo_data20_response(
                    "/financialData/getCashFlowStatement",
                    "经营活动产生的现金流量净额",
                ),
            ]
        ),
    )

    payloads = provider._fetch_sync(
        [
            {
                "instrument_id": "920833.BJ",
                "symbol": "920833",
                "exchange": "BSE",
                "type": "stock",
            }
        ],
        "BSE",
        ["2025-12-31"],
        "direct",
    )

    assert len(payloads) == 3
    assert provider.session.calls[0]["params"]["scode"] == "920833"
    assert [call["params"]["sign"] for call in provider.session.calls[1:]] == [
        "1",
        "1",
        "1",
    ]
    assert [payload.manifest.metadata_json["endpoint_candidate_key"] for payload in payloads] == [
        "income",
        "balance",
        "cash",
    ]
    assert all(
        payload.manifest.metadata_json["structured_payload"] for payload in payloads
    )


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


def _cninfo_data20_candidate(key: str, url: str):
    return {
        "key": key,
        "enabled": True,
        "url": url,
        "request": {
            "query_params": {
                "scode": "{symbol}",
                "sign": "{cninfo_data20_sign}",
            }
        },
    }


def _cninfo_data20_response(path: str, fact_name: str):
    return _FakeResponse(
        '{"path":"%s","code":200,"data":{"records":[{"year":[{"index":"%s","2025":1.0}]}]}}'
        % (path, fact_name)
    )
