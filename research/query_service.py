"""
Query helpers for research read APIs.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from .storage import ResearchStorageManager


class ResearchQueryService:
    """Build stable read models from normalized research storage."""

    def __init__(self, storage: ResearchStorageManager):
        self.storage = storage

    def get_company_profile(
        self,
        instrument_id: str,
        *,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        return self.storage.get_company_profile(
            instrument_id,
            include_snapshot=include_snapshot,
        )

    def get_financial_summary(
        self,
        instrument_id: str,
        *,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        return self.storage.get_financial_summary(
            instrument_id,
            include_snapshot=include_snapshot,
        )

    def get_financial_statement_bundle(
        self,
        instrument_id: str,
        *,
        include_statements: bool = True,
    ) -> Optional[Dict[str, Any]]:
        return self.storage.get_financial_statement_bundle(
            instrument_id,
            include_statements=include_statements,
        )

    def get_industry_membership(
        self,
        instrument_id: str,
        *,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        return self.storage.get_industry_membership(
            instrument_id,
            include_snapshot=include_snapshot,
        )

    def get_latest_analyst_forecast(
        self,
        instrument_id: str,
        *,
        include_details: bool = True,
    ) -> Optional[Dict[str, Any]]:
        return self.storage.get_latest_analyst_forecast(
            instrument_id,
            include_details=include_details,
        )

    def list_research_reports(
        self,
        instrument_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 20,
        include_details: bool = True,
    ) -> list[Dict[str, Any]]:
        return self.storage.list_research_reports(
            instrument_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            include_details=include_details,
        )

    def list_sentiment_events(
        self,
        instrument_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        event_types: Optional[list[str]] = None,
        limit: int = 50,
        include_details: bool = True,
    ) -> list[Dict[str, Any]]:
        return self.storage.list_sentiment_events(
            instrument_id,
            start_date=start_date,
            end_date=end_date,
            event_types=event_types,
            limit=limit,
            include_details=include_details,
        )

    def get_latest_risk_snapshot(
        self,
        instrument_id: str,
        *,
        include_details: bool = True,
    ) -> Optional[Dict[str, Any]]:
        return self.storage.get_latest_risk_snapshot(
            instrument_id,
            include_details=include_details,
        )

    def get_valuation_history_rows(
        self,
        instrument_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 120,
        include_details: bool = True,
        calc_method: Optional[str] = None,
        calc_version: Optional[str] = None,
        parameter_hash: Optional[str] = None,
    ) -> list[Dict[str, Any]]:
        return self.storage.get_valuation_history_rows(
            instrument_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            include_details=include_details,
            calc_method=calc_method,
            calc_version=calc_version,
            parameter_hash=parameter_hash,
        )

    def get_latest_valuation_history_row(
        self,
        instrument_id: str,
        *,
        include_details: bool = True,
        calc_method: Optional[str] = None,
        calc_version: Optional[str] = None,
        parameter_hash: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.storage.get_latest_valuation_history_row(
            instrument_id,
            include_details=include_details,
            calc_method=calc_method,
            calc_version=calc_version,
            parameter_hash=parameter_hash,
        )

    def get_company_overview(
        self,
        instrument_id: str,
        *,
        include_profile_snapshot: bool = False,
        include_industry_snapshot: bool = False,
        include_financial_snapshot: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Build a compact company overview from available research sections."""
        profile = self.get_company_profile(
            instrument_id,
            include_snapshot=include_profile_snapshot,
        )
        industry = self.get_industry_membership(
            instrument_id,
            include_snapshot=include_industry_snapshot,
        )
        financial_summary = self.get_financial_summary(
            instrument_id,
            include_snapshot=include_financial_snapshot,
        )

        if profile is None and industry is None and financial_summary is None:
            return None

        source_summary = {
            "company_profile": self._build_source_status(profile),
            "industry": self._build_source_status(industry),
            "financial_summary": self._build_source_status(financial_summary),
        }
        missing_sections = [
            section
            for section, status in source_summary.items()
            if not status["available"]
        ]

        overview = {
            "instrument_id": self._pick_first(profile, financial_summary, key="instrument_id"),
            "symbol": self._pick_first(profile, financial_summary, key="symbol"),
            "exchange": self._pick_first(profile, financial_summary, key="exchange"),
            "market": self._pick_first(profile, key="market"),
            "company_name": self._pick_first(profile, key="company_name"),
            "short_name": self._pick_first(profile, key="short_name"),
            "listed_date": self._pick_first(profile, key="listed_date"),
            "industry_raw": self._pick_first(profile, key="industry_raw"),
            "sector_raw": self._pick_first(profile, key="sector_raw"),
            "industry_system": self._pick_first(industry, key="taxonomy_system"),
            "industry_taxonomy_version": self._pick_first(industry, key="taxonomy_version"),
            "industry_code": self._pick_first(industry, key="industry_code"),
            "industry_name": self._pick_first(industry, key="industry_name"),
            "industry_level": self._pick_first(industry, key="industry_level"),
            "industry_mapping_status": self._pick_first(industry, key="mapping_status"),
            "sw_l1_code": self._pick_first(industry, key="sw_l1_code"),
            "sw_l1_name": self._pick_first(industry, key="sw_l1_name"),
            "sw_l2_code": self._pick_first(industry, key="sw_l2_code"),
            "sw_l2_name": self._pick_first(industry, key="sw_l2_name"),
            "sw_l3_code": self._pick_first(industry, key="sw_l3_code"),
            "sw_l3_name": self._pick_first(industry, key="sw_l3_name"),
            "status": self._pick_first(profile, key="status"),
            "report_date": self._pick_first(financial_summary, key="report_date"),
            "pub_date": self._pick_first(financial_summary, key="pub_date"),
            "fiscal_year": self._pick_first(financial_summary, key="fiscal_year"),
            "fiscal_quarter": self._pick_first(financial_summary, key="fiscal_quarter"),
            "currency": self._pick_first(financial_summary, key="currency"),
            "schema_version": self._pick_first(financial_summary, key="schema_version"),
            "roe": self._pick_first(financial_summary, key="roe"),
            "gross_margin": self._pick_first(financial_summary, key="gross_margin"),
            "net_margin": self._pick_first(financial_summary, key="net_margin"),
            "current_ratio": self._pick_first(financial_summary, key="current_ratio"),
            "quick_ratio": self._pick_first(financial_summary, key="quick_ratio"),
            "liability_to_asset": self._pick_first(
                financial_summary,
                key="liability_to_asset",
            ),
            "yoy_asset": self._pick_first(financial_summary, key="yoy_asset"),
            "yoy_equity": self._pick_first(financial_summary, key="yoy_equity"),
            "yoy_net_profit": self._pick_first(financial_summary, key="yoy_net_profit"),
            "cfo_to_revenue": self._pick_first(financial_summary, key="cfo_to_revenue"),
            "cfo_to_net_profit": self._pick_first(
                financial_summary,
                key="cfo_to_net_profit",
            ),
            "asset_turnover": self._pick_first(financial_summary, key="asset_turnover"),
            "eps": self._pick_first(financial_summary, key="eps"),
            "data_as_of": self._pick_latest_timestamp(profile, industry, financial_summary),
            "source_summary": source_summary,
            "missing_sections": missing_sections,
        }

        if include_profile_snapshot and profile is not None:
            overview["company_profile"] = profile

        if include_industry_snapshot and industry is not None:
            overview["industry"] = industry

        if include_financial_snapshot and financial_summary is not None:
            overview["financial_summary"] = financial_summary

        return overview

    @staticmethod
    def _pick_first(*records: Optional[Dict[str, Any]], key: str) -> Any:
        for record in records:
            if not record:
                continue
            value = record.get(key)
            if value is not None:
                return value
        return None

    def _build_source_status(
        self,
        record: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if record is None:
            return {
                "available": False,
                "source": None,
                "source_mode": None,
                "data_as_of": None,
                "missing_reason": "snapshot_not_available",
            }

        return {
            "available": True,
            "source": record.get("source"),
            "source_mode": record.get("source_mode"),
            "data_as_of": self._pick_latest_timestamp(record),
            "missing_reason": None,
        }

    @staticmethod
    def _pick_latest_timestamp(*records: Optional[Dict[str, Any]]) -> Optional[str]:
        timestamps = []
        for record in records:
            if not record:
                continue
            for key in ("data_as_of", "updated_at", "created_at"):
                raw_value = record.get(key)
                if not raw_value:
                    continue
                if isinstance(raw_value, datetime):
                    timestamps.append(raw_value)
                    break
                timestamps.append(datetime.fromisoformat(str(raw_value)))
                break

        if not timestamps:
            return None

        return max(timestamps).isoformat()
