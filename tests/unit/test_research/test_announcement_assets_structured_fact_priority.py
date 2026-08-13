from __future__ import annotations

from data_manager import DataManager
from research.broker_risk_control import (
    BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    BROKER_RISK_CONTROL_CANONICAL_FACTS,
)


class _TripwireBrokerStorage:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def get_financial_numeric_facts(self, *args, **kwargs):
        canonical_name = str(kwargs["canonical_fact_name"])
        self.queries.append(canonical_name)
        return [
            {
                "canonical_fact_name": canonical_name,
                "fact_value": 260.0,
                "report_period": "2025-12-31",
                "source": "cninfo",
                "source_mode": "direct",
                "source_file_id": "annual-report-pdf",
                "raw_fact": {
                    "source_profile": (
                        BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
                    ),
                    "annual_report_asset_id": "must-not-be-read",
                },
            }
        ]

    def get_financial_source_file_manifests(self, **kwargs):
        raise AssertionError("PDF-derived lineage must not be read")


def test_structured_fact_precedes_pdf_without_annual_report_activity():
    manager = object.__new__(DataManager)

    def _unexpected_asset_access(**kwargs):
        raise AssertionError("shared annual-report asset access must not occur")

    manager._get_announcement_asset_access = _unexpected_asset_access
    manager.ensure_shared_annual_report = lambda **kwargs: (_ for _ in ()).throw(
        AssertionError("annual-report ensure must not occur")
    )
    storage = _TripwireBrokerStorage()
    structured_facts = {
        canonical_name: float(index + 1) * 100.0
        for index, canonical_name in enumerate(BROKER_RISK_CONTROL_CANONICAL_FACTS)
    }
    structured_facts["net_capital"] = 900.0
    bundle = {
        "report_period": "2025-12-31",
        "latest_facts": structured_facts,
        "lineage": {
            "net_capital": {
                "source_profile": "official_xbrl_numeric_facts",
                "source_file_id": "sse-xbrl-2025",
            }
        },
    }

    enriched = manager._enrich_dcf_bundle_with_broker_risk_control_facts(
        storage,
        "600030.SH",
        bundle,
    )

    assert enriched["latest_facts"]["net_capital"] == 900.0
    assert enriched["lineage"]["net_capital"]["source_profile"] == (
        "official_xbrl_numeric_facts"
    )
    assert "net_capital" not in storage.queries
    assert "broker_risk_control" not in enriched["lineage"]
