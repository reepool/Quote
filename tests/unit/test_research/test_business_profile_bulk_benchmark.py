from research.business_profile_bulk_benchmark import (
    BUSINESS_PROFILE_BULK_BENCHMARK_SCHEMA_VERSION,
    run_business_profile_bulk_write_benchmark,
)


def test_bulk_write_benchmark_meets_regression_threshold():
    result = run_business_profile_bulk_write_benchmark(
        row_count=500,
        minimum_rows_per_second=100.0,
        maximum_elapsed_seconds=10.0,
    )

    assert result["schema_version"] == BUSINESS_PROFILE_BULK_BENCHMARK_SCHEMA_VERSION
    assert result["written_count"] == 500
    assert result["persisted_count"] == 500
    assert result["passed"] is True
