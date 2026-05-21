from scripts.dev_validation.probe_sse_official_period_availability import (
    classify_period_availability,
    count_sse_rows,
    extract_max_report_year,
    sse_report_type_id,
    summarize_period_probe_results,
)


def test_count_sse_rows_counts_commonquery_numeric_fields():
    payload = {
        "result": [
            {"S2020_0010": "1,234.5", "S2020_0020": "-", "SEC_NAME": "sample"},
            {"S2020_0010": 9.0, "S2020_0030": None},
        ]
    }

    assert count_sse_rows(payload) == (2, 2, 3)


def test_classify_period_availability_distinguishes_empty_payload():
    assert (
        classify_period_availability(
            http_status=200,
            response_class="json_manifest",
            row_count=0,
            numeric_field_count=0,
        )
        == "empty_structured_payload"
    )
    assert (
        classify_period_availability(
            http_status=200,
            response_class="structured_payload",
            row_count=1,
            numeric_field_count=2,
        )
        == "structured_numeric_rows"
    )


def test_summarize_period_probe_results_classifies_period_gap():
    summary = summarize_period_probe_results(
        [
            {
                "report_period": "2024-12-31",
                "http_status": 200,
                "row_count": 1,
                "numeric_field_count": 3,
                "period_availability": "structured_numeric_rows",
            },
            {
                "report_period": "2025-12-31",
                "http_status": 200,
                "row_count": 0,
                "numeric_field_count": 0,
                "period_availability": "empty_structured_payload",
            },
        ],
        report_periods=["2024-12-31", "2025-12-31"],
    )

    assert summary["assessment"] == "period_unavailable_or_query_adapter_gap"
    assert summary["periods_with_numeric_rows"] == ["2024-12-31"]
    assert summary["periods_without_numeric_rows"] == ["2025-12-31"]


def test_summarize_period_probe_results_uses_max_year_diagnostics():
    summary = summarize_period_probe_results(
        [
            {
                "report_period": "2025-12-31",
                "http_status": 200,
                "row_count": 0,
                "numeric_field_count": 0,
                "period_availability": "empty_structured_payload",
            }
        ],
        report_periods=["2025-12-31"],
        max_year_results=[
            {"report_type_id": "5000", "max_report_year": "2023"},
        ],
    )

    assert summary["assessment"] == "period_beyond_sse_report_type_max_year"
    assert summary["max_year_by_report_type_id"] == {"5000": "2023"}
    assert summary["periods_beyond_report_type_max_year"] == ["2025-12-31"]


def test_sse_report_type_id_supports_normalized_dates():
    assert sse_report_type_id("2025-03-31") == "4000"
    assert sse_report_type_id("2025-06-30") == "1000"
    assert sse_report_type_id("2025-09-30") == "4400"
    assert sse_report_type_id("2025-12-31") == "5000"


def test_extract_max_report_year_reads_commonquery_result():
    assert extract_max_report_year({"result": [{"REPORT_YEAR": "2023"}]}) == "2023"
    assert extract_max_report_year({"result": []}) is None
