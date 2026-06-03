"""HKEX instrument master source adapters and lifecycle policy.

The HKEX master policy keeps official HKEX/HKEXnews lifecycle evidence separate
from supplemental market-data sources. Supplemental rows may help discovery and
metadata fill, but they must not activate or delist instruments on their own.
"""

from __future__ import annotations

import hashlib
import json
import re
import urllib.request
from dataclasses import dataclass, field
from io import BytesIO, StringIO
from typing import Any, Dict, Iterable, List, Optional, Set

import pandas as pd


HKEX_MASTER_PARSER_VERSION = "hkex-instrument-master-v1"
OFFICIAL_SOURCES = {"hkex_securities_list", "hkexnews_active_list", "hkexnews_delisted_list"}


def normalize_hkex_code(value: Any) -> str:
    """Return a 5-digit HKEX stock code."""
    text = str(value or "").strip()
    if not text:
        return ""
    if "." in text:
        text = text.split(".", 1)[0]
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(5)


def hkex_instrument_id(code: Any) -> str:
    normalized = normalize_hkex_code(code)
    return f"{normalized}.HK" if normalized else ""


def _snapshot_hash(raw_text: str) -> str:
    return hashlib.sha256((raw_text or "").encode("utf-8")).hexdigest()


def _snapshot_hash_bytes(raw_bytes: bytes) -> str:
    return hashlib.sha256(raw_bytes or b"").hexdigest()


def _normalized_columns(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = {}
    for column in frame.columns:
        key = str(column).strip().lower().replace(" ", "_").replace("-", "_")
        renamed[column] = key
    return frame.rename(columns=renamed)


def classify_hkex_product(row: Dict[str, Any]) -> Dict[str, Any]:
    """Classify HKEX products into research scope and derivative/debt buckets."""
    instrument_id = str(row.get("instrument_id") or "").strip().upper()
    symbol = str(row.get("symbol") or row.get("code") or "").strip()
    category = str(row.get("category") or row.get("hkex_category") or "").strip().lower()
    sub_category = str(row.get("sub_category") or row.get("hkex_sub_category") or "").strip().lower()
    name = str(row.get("name") or row.get("stock_name") or "").strip().lower()
    combined = " ".join([category, sub_category, name])

    if "old code" in combined or " old" in combined or "-old" in combined or "(旧)" in combined:
        product_type = "old_code"
        research_scope = "exclude"
    elif instrument_id[0:3] == "029" or symbol[0:3] == "029":
        product_type = "temporary_counter"
        research_scope = "exclude"
    elif (
        "rights" in combined
        or " rts" in combined
        or combined.endswith("rts")
        or "warrants for share rights" in combined
    ):
        product_type = "subscription_right"
        research_scope = "exclude"
    elif "-2000" in name or "-10k" in name or "-500" in name:
        product_type = "temporary_counter"
        research_scope = "exclude"
    elif "callable bull/bear" in combined or "bull/bear" in combined or "cbbc" in combined:
        product_type = "cbbc"
        research_scope = "exclude"
    elif "inline warrant" in combined:
        product_type = "inline_warrant"
        research_scope = "exclude"
    elif "warrant" in combined:
        product_type = "warrant"
        research_scope = "exclude"
    elif "debt" in combined or "bond" in combined or "note" in combined:
        product_type = "debt"
        research_scope = "exclude"
    elif "exchange traded fund" in combined or " etf" in combined or "tracker fund" in combined:
        product_type = "etf"
        research_scope = "fund"
    elif "real estate investment trust" in combined or " reit" in combined or "产业信托" in name or "房产基金" in name:
        product_type = "reit"
        research_scope = "fund"
    elif "equity" in category or "ordinary" in combined or not category:
        product_type = "ordinary_equity"
        research_scope = "equity"
    else:
        product_type = "unknown"
        research_scope = "review"

    return {
        "product_type": product_type,
        "research_scope": research_scope,
        "is_research_equity": product_type == "ordinary_equity",
    }


@dataclass
class HKEXProviderSnapshot:
    source: str
    source_url: str
    parser_version: str
    raw_snapshot_hash: str
    rows: List[Dict[str, Any]]
    diagnostics: Dict[str, Any] = field(default_factory=dict)


class HKEXSecuritiesListProvider:
    """Parser for official HKEX securities list snapshots."""

    source = "hkex_securities_list"

    def __init__(self, source_url: str = ""):
        self.source_url = source_url

    def fetch_csv(self, *, timeout_sec: float = 20.0) -> HKEXProviderSnapshot:
        if not self.source_url:
            raise ValueError("source_url is required for HKEX securities-list fetch")
        request = urllib.request.Request(
            self.source_url,
            headers={"User-Agent": "Quote-HKEX-InstrumentMaster/1.0"},
        )
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw_bytes = response.read()
        if self.source_url.lower().endswith((".xlsx", ".xls")):
            return self.parse_excel(raw_bytes)
        raw = raw_bytes.decode("utf-8-sig", errors="replace")
        return self.parse_csv(raw)

    def parse_excel(self, raw_excel: bytes) -> HKEXProviderSnapshot:
        frame = pd.read_excel(BytesIO(raw_excel), dtype=str, header=None).fillna("")
        header_idx = None
        for idx, row in frame.iterrows():
            normalized_values = {str(value).strip().lower().replace(" ", "_") for value in row.tolist()}
            if "stock_code" in normalized_values and "name_of_securities" in normalized_values:
                header_idx = idx
                break
        if header_idx is None:
            raise ValueError("HKEX securities-list Excel header row not found")

        header = [str(value).strip() for value in frame.iloc[header_idx].tolist()]
        data = frame.iloc[header_idx + 1 :].copy()
        data.columns = header
        data = _normalized_columns(data)
        rows, skipped = self._rows_from_frame(data)
        return HKEXProviderSnapshot(
            source=self.source,
            source_url=self.source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash_bytes(raw_excel),
            rows=rows,
            diagnostics={"row_count": len(rows), "skipped_count": skipped, "format": "excel"},
        )

    def parse_csv(self, raw_csv: str) -> HKEXProviderSnapshot:
        frame = _normalized_columns(pd.read_csv(StringIO(raw_csv), dtype=str).fillna(""))
        rows, skipped = self._rows_from_frame(frame)
        return HKEXProviderSnapshot(
            source=self.source,
            source_url=self.source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_csv),
            rows=rows,
            diagnostics={"row_count": len(rows), "skipped_count": skipped, "format": "csv"},
        )

    def _rows_from_frame(self, frame: pd.DataFrame) -> tuple[List[Dict[str, Any]], int]:
        rows: List[Dict[str, Any]] = []
        skipped = 0
        for item in frame.to_dict(orient="records"):
            code = normalize_hkex_code(item.get("stock_code") or item.get("code"))
            if not code:
                skipped += 1
                continue
            name = str(
                item.get("name_of_securities")
                or item.get("stock_name")
                or item.get("name")
                or ""
            ).strip()
            record = {
                "instrument_id": hkex_instrument_id(code),
                "symbol": code,
                "name": name,
                "exchange": "HKEX",
                "type": "stock",
                "currency": str(item.get("trading_currency") or "HKD").strip() or "HKD",
                "status": "active",
                "is_active": True,
                "trading_status": 1,
                "source": self.source,
                "source_symbol": code,
                "hkex_category": str(item.get("category") or "").strip(),
                "hkex_sub_category": str(item.get("sub_category") or "").strip(),
                "board_lot": str(item.get("board_lot") or "").strip(),
                "isin": str(item.get("isin") or "").strip(),
                "rmb_counter": str(item.get("rmb_counter") or "").strip(),
                "official_lifecycle_source": self.source,
                "source_url": self.source_url,
            }
            record.update(classify_hkex_product(record))
            rows.append(record)
        return rows, skipped


class HKEXNewsStockListProvider:
    """Parser for HKEXnews active and delisted lifecycle list snapshots."""

    def __init__(self, source_url: str = ""):
        self.source_url = source_url

    def fetch_html(self, *, lifecycle_status: str, timeout_sec: float = 20.0) -> HKEXProviderSnapshot:
        if not self.source_url:
            raise ValueError("source_url is required for HKEXnews fetch")
        request = urllib.request.Request(
            self.source_url,
            headers={"User-Agent": "Quote-HKEX-InstrumentMaster/1.0"},
        )
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8-sig", errors="replace")
        stripped = raw.lstrip()
        if stripped.startswith("[") or stripped.startswith("{"):
            return self.parse_json(raw, lifecycle_status=lifecycle_status)
        return self.parse_html(raw, lifecycle_status=lifecycle_status)

    def parse_json(self, raw_json: str, *, lifecycle_status: str) -> HKEXProviderSnapshot:
        payload = json.loads(raw_json)
        if isinstance(payload, dict):
            records = payload.get("data") or payload.get("rows") or payload.get("result") or []
        else:
            records = payload
        rows: List[Dict[str, Any]] = []
        skipped = 0
        status = lifecycle_status.lower().strip()
        for item in records or []:
            code = normalize_hkex_code(item.get("c") or item.get("code") or item.get("stock_code"))
            if not code:
                skipped += 1
                continue
            name = str(item.get("n") or item.get("name") or item.get("stock_name") or "").strip()
            rows.append({
                "instrument_id": hkex_instrument_id(code),
                "symbol": code,
                "name": name,
                "exchange": "HKEX",
                "type": "stock",
                "status": "active" if status == "active" else "delisted",
                "is_active": status == "active",
                "trading_status": 1 if status == "active" else 0,
                "source": f"hkexnews_{status}_list",
                "source_symbol": code,
                "stock_id": item.get("i"),
                "security_id": item.get("s"),
                "delisted_date": str(item.get("delisting_date") or "").strip() or None,
                "lifecycle_evidence": {
                    "source": f"hkexnews_{status}_list",
                    "source_url": self.source_url,
                    "status": status,
                    "format": "json",
                },
            })

        source = f"hkexnews_{status}_list"
        return HKEXProviderSnapshot(
            source=source,
            source_url=self.source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_json),
            rows=rows,
            diagnostics={"row_count": len(rows), "skipped_count": skipped, "format": "json"},
        )

    def parse_html(self, raw_html: str, *, lifecycle_status: str) -> HKEXProviderSnapshot:
        stripped = raw_html.lstrip("\ufeff \n\r\t")
        if stripped.startswith("[") or stripped.startswith("{"):
            return self.parse_json(raw_html, lifecycle_status=lifecycle_status)
        tables = pd.read_html(StringIO(raw_html))
        rows: List[Dict[str, Any]] = []
        skipped = 0
        for table in tables:
            frame = _normalized_columns(table.fillna(""))
            for item in frame.to_dict(orient="records"):
                code = normalize_hkex_code(item.get("stock_code") or item.get("code"))
                if not code:
                    skipped += 1
                    continue
                name = str(item.get("stock_name") or item.get("name") or "").strip()
                status = lifecycle_status.lower().strip()
                record = {
                    "instrument_id": hkex_instrument_id(code),
                    "symbol": code,
                    "name": name,
                    "exchange": "HKEX",
                    "type": "stock",
                    "status": "active" if status == "active" else "delisted",
                    "is_active": status == "active",
                    "trading_status": 1 if status == "active" else 0,
                    "source": f"hkexnews_{status}_list",
                    "source_symbol": code,
                    "market": str(item.get("market") or "").strip(),
                    "delisted_date": str(item.get("delisting_date") or "").strip() or None,
                    "lifecycle_evidence": {
                        "source": f"hkexnews_{status}_list",
                        "source_url": self.source_url,
                        "status": status,
                    },
                }
                rows.append(record)

        source = f"hkexnews_{lifecycle_status.lower().strip()}_list"
        return HKEXProviderSnapshot(
            source=source,
            source_url=self.source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_html),
            rows=rows,
            diagnostics={"row_count": len(rows), "skipped_count": skipped, "table_count": len(tables)},
        )


class HKEXSupplementalAdapter:
    """Normalize non-authoritative HKEX supplemental rows."""

    @staticmethod
    def parse_akshare_spot_csv(raw_csv: str, *, source_url: str = "") -> HKEXProviderSnapshot:
        frame = pd.read_csv(StringIO(raw_csv), dtype=str).fillna("")
        rows: List[Dict[str, Any]] = []
        for item in frame.to_dict(orient="records"):
            code = normalize_hkex_code(item.get("代码") or item.get("code"))
            if not code:
                continue
            rows.append({
                "instrument_id": hkex_instrument_id(code),
                "symbol": code,
                "name": str(item.get("名称") or item.get("name") or "").strip(),
                "exchange": "HKEX",
                "type": "stock",
                "currency": "HKD",
                "source": "akshare_hk_spot_em",
                "source_symbol": code,
                "lifecycle_authoritative": False,
                "source_url": source_url,
            })
        return HKEXProviderSnapshot(
            source="akshare_hk_spot_em",
            source_url=source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_csv),
            rows=rows,
            diagnostics={"row_count": len(rows)},
        )

    @staticmethod
    def parse_eastmoney_profile_csv(raw_csv: str, *, source_url: str = "") -> HKEXProviderSnapshot:
        frame = _normalized_columns(pd.read_csv(StringIO(raw_csv), dtype=str).fillna(""))
        rows: List[Dict[str, Any]] = []
        for item in frame.to_dict(orient="records"):
            code = normalize_hkex_code(item.get("code"))
            if not code:
                continue
            rows.append({
                "instrument_id": hkex_instrument_id(code),
                "symbol": code,
                "name": str(item.get("name") or "").strip(),
                "exchange": "HKEX",
                "type": "stock",
                "industry": str(item.get("industry") or "").strip() or None,
                "sector": str(item.get("sector") or "").strip() or None,
                "listed_date": str(item.get("listing_date") or "").strip() or None,
                "source": "eastmoney_hk_profile",
                "source_symbol": code,
                "lifecycle_authoritative": False,
                "source_url": source_url,
            })
        return HKEXProviderSnapshot(
            source="eastmoney_hk_profile",
            source_url=source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_csv),
            rows=rows,
            diagnostics={"row_count": len(rows)},
        )


class HKEXSuspensionReportProvider:
    """Parser for official HKEX prolonged-suspension reports.

    The live source is PDF. Text extraction is delegated to pypdf when
    available; tests and operator fixtures can feed extracted text directly.
    """

    source = "hkexnews_suspension_report"

    def __init__(self, source_url: str = "", market: str = ""):
        self.source_url = source_url
        self.market = market

    def fetch_pdf(self, *, timeout_sec: float = 20.0) -> HKEXProviderSnapshot:
        if not self.source_url:
            raise ValueError("source_url is required for HKEX suspension-report fetch")
        request = urllib.request.Request(
            self.source_url,
            headers={"User-Agent": "Quote-HKEX-InstrumentMaster/1.0"},
        )
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw_pdf = response.read()
        return self.parse_pdf(raw_pdf)

    def parse_pdf(self, raw_pdf: bytes) -> HKEXProviderSnapshot:
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise RuntimeError("pypdf is required to parse HKEX suspension PDF reports") from exc

        reader = PdfReader(BytesIO(raw_pdf))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
        snapshot = self.parse_text(text)
        snapshot.raw_snapshot_hash = _snapshot_hash_bytes(raw_pdf)
        snapshot.diagnostics["format"] = "pdf"
        snapshot.diagnostics["page_count"] = len(reader.pages)
        return snapshot

    def parse_text(self, raw_text: str) -> HKEXProviderSnapshot:
        rows: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        skipped = 0
        for line in (raw_text or "").splitlines():
            match = re.search(r"\b(\d{1,5})\b", line)
            if not match:
                continue
            code = normalize_hkex_code(match.group(1))
            if not code or code in seen:
                skipped += 1
                continue
            seen.add(code)
            name_text = line[match.end():].strip(" -\t")
            rows.append({
                "instrument_id": hkex_instrument_id(code),
                "symbol": code,
                "name": name_text,
                "exchange": "HKEX",
                "type": "stock",
                "status": "suspended",
                "is_active": True,
                "trading_status": 0,
                "source": self.source,
                "source_symbol": code,
                "market": self.market,
                "official_lifecycle_source": self.source,
                "source_url": self.source_url,
                "lifecycle_evidence": {
                    "source": self.source,
                    "source_url": self.source_url,
                    "status": "suspended",
                    "market": self.market,
                },
            })
        return HKEXProviderSnapshot(
            source=self.source,
            source_url=self.source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_text),
            rows=rows,
            diagnostics={"row_count": len(rows), "skipped_count": skipped, "format": "text"},
        )


class HKEXManualReviewProvider:
    """Parse operator-reviewed HKEX lifecycle evidence from JSON or CSV."""

    source = "hkex_manual_review"

    def __init__(self, source_url: str = ""):
        self.source_url = source_url

    def parse(self, raw_text: str) -> HKEXProviderSnapshot:
        stripped = (raw_text or "").lstrip("\ufeff \n\r\t")
        if stripped.startswith("[") or stripped.startswith("{"):
            return self.parse_json(raw_text)
        return self.parse_csv(raw_text)

    def parse_json(self, raw_json: str) -> HKEXProviderSnapshot:
        payload = json.loads(raw_json)
        if isinstance(payload, dict):
            records = payload.get("reviews") or payload.get("rows") or payload.get("data") or []
        else:
            records = payload
        return self._parse_records(records, raw_json, source_format="json")

    def parse_csv(self, raw_csv: str) -> HKEXProviderSnapshot:
        frame = _normalized_columns(pd.read_csv(StringIO(raw_csv), dtype=str).fillna(""))
        return self._parse_records(frame.to_dict(orient="records"), raw_csv, source_format="csv")

    def _parse_records(
        self,
        records: Iterable[Dict[str, Any]],
        raw_text: str,
        *,
        source_format: str,
    ) -> HKEXProviderSnapshot:
        rows: List[Dict[str, Any]] = []
        skipped = 0
        for item in records or []:
            code = normalize_hkex_code(
                item.get("instrument_id")
                or item.get("stock_code")
                or item.get("code")
                or item.get("symbol")
            )
            if not code:
                skipped += 1
                continue
            action = str(item.get("action") or item.get("status") or "").strip().lower()
            if action in {"deactivate", "inactive", "delist", "delisted"}:
                status = "delisted"
            elif action in {"suspend", "suspended"}:
                status = "suspended"
            elif action in {"activate", "active", "reactivate"}:
                status = "active"
            else:
                skipped += 1
                continue

            effective_date = (
                str(item.get("effective_date") or item.get("delisted_date") or "").strip()
                or None
            )
            rows.append({
                "instrument_id": hkex_instrument_id(code),
                "symbol": code,
                "name": str(item.get("name") or item.get("stock_name") or "").strip(),
                "exchange": "HKEX",
                "type": "stock",
                "status": status,
                "is_active": status != "delisted",
                "trading_status": 0 if status in {"delisted", "suspended"} else 1,
                "source": self.source,
                "source_symbol": code,
                "delisted_date": effective_date if status == "delisted" else None,
                "review_reason": str(item.get("reason") or "").strip(),
                "reviewed_by": str(item.get("reviewed_by") or "").strip(),
                "evidence_url": str(item.get("evidence_url") or item.get("source_url") or "").strip(),
                "official_lifecycle_source": self.source,
                "source_url": self.source_url,
                "lifecycle_evidence": {
                    "source": self.source,
                    "source_url": self.source_url,
                    "status": status,
                    "evidence_url": str(item.get("evidence_url") or item.get("source_url") or "").strip(),
                    "format": source_format,
                },
            })
        return HKEXProviderSnapshot(
            source=self.source,
            source_url=self.source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_text),
            rows=rows,
            diagnostics={"row_count": len(rows), "skipped_count": skipped, "format": source_format},
        )


class HKEXSourceEvidencePolicy:
    """Summarize source quorum and write gates for HKEX sync modes."""

    @staticmethod
    def assess(
        *,
        snapshots: Iterable[HKEXProviderSnapshot],
        errors: Iterable[str],
        official_active_rows: Iterable[Dict[str, Any]],
        official_delisted_rows: Iterable[Dict[str, Any]],
    ) -> Dict[str, Any]:
        sources = {snapshot.source for snapshot in snapshots or []}
        error_list = list(errors or [])
        primary_active_available = "hkex_securities_list" in sources
        fallback_active_available = "hkexnews_active_list" in sources
        delisted_available = "hkexnews_delisted_list" in sources or "hkex_manual_review" in sources
        suspension_available = "hkexnews_suspension_report" in sources or "hkex_manual_review" in sources
        has_active_rows = any(row.get("instrument_id") for row in official_active_rows or [])
        has_delisted_rows = any(row.get("instrument_id") for row in official_delisted_rows or [])
        active_fallback_used = not primary_active_available and fallback_active_available and has_active_rows

        return {
            "sources": sorted(sources),
            "source_error_count": len(error_list),
            "primary_active_source_available": primary_active_available,
            "fallback_active_source_available": fallback_active_available,
            "active_fallback_used": active_fallback_used,
            "delisted_source_available": delisted_available,
            "suspension_source_available": suspension_available,
            "safe_write_allowed": primary_active_available and has_active_rows and not error_list,
            "reactivation_write_allowed": primary_active_available and has_active_rows and not error_list,
            "delisting_write_allowed": has_delisted_rows and delisted_available and not error_list,
            "suspension_write_allowed": suspension_available and not error_list,
        }


def build_dual_counter_map(records: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Group HKEX counters sharing the same ISIN and expose canonical HKD legs."""
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for record in records:
        isin = str(record.get("isin") or "").strip()
        if not isin:
            continue
        groups.setdefault(isin, []).append(record)

    mapping: Dict[str, Dict[str, Any]] = {}
    for isin, items in groups.items():
        if len(items) < 2:
            continue
        canonical = None
        for item in sorted(items, key=lambda row: str(row.get("symbol") or "")):
            if str(item.get("currency") or "").upper() == "HKD":
                canonical = item
                break
        canonical = canonical or sorted(items, key=lambda row: str(row.get("symbol") or ""))[0]
        canonical_id = canonical.get("instrument_id")
        for item in items:
            mapping[item.get("instrument_id")] = {
                "isin": isin,
                "canonical_instrument_id": canonical_id,
                "is_canonical": item.get("instrument_id") == canonical_id,
                "counter_currency": item.get("currency"),
                "dual_counter_ids": sorted(row.get("instrument_id") for row in items if row.get("instrument_id")),
            }
    return mapping


class HKEXLifecyclePolicy:
    """Apply HKEX source-authority rules without performing database writes."""

    @staticmethod
    def build_decisions(
        *,
        local_rows: Iterable[Dict[str, Any]],
        official_active_rows: Iterable[Dict[str, Any]],
        official_delisted_rows: Iterable[Dict[str, Any]],
        supplemental_rows: Iterable[Dict[str, Any]] = (),
    ) -> Dict[str, Any]:
        local_by_id = {
            row.get("instrument_id"): row
            for row in local_rows
            if row.get("instrument_id")
        }
        active_by_id = {
            row.get("instrument_id"): row
            for row in official_active_rows
            if row.get("instrument_id")
        }
        delisted_by_id = {
            row.get("instrument_id"): row
            for row in official_delisted_rows
            if row.get("instrument_id")
        }
        supplemental_ids: Set[str] = {
            row.get("instrument_id")
            for row in supplemental_rows
            if row.get("instrument_id")
        }

        inserts: List[Dict[str, Any]] = []
        metadata_updates: List[Dict[str, Any]] = []
        reactivations: List[Dict[str, Any]] = []
        suspensions: List[Dict[str, Any]] = []
        delistings: List[Dict[str, Any]] = []
        review_required: List[Dict[str, Any]] = []

        for instrument_id, active_row in active_by_id.items():
            local = local_by_id.get(instrument_id)
            official_status = str(active_row.get("status") or "active").lower()
            if local is None:
                inserts.append(active_row)
                continue
            if official_status == "suspended":
                suspensions.append({
                    "instrument_id": instrument_id,
                    "reason": "official_suspension_evidence",
                    "official": active_row,
                    "local": local,
                })
                continue
            if local.get("is_active") in (False, 0, "0") or str(local.get("status") or "") != "active":
                reactivations.append({
                    "instrument_id": instrument_id,
                    "reason": "official_active_evidence_overrides_local_inactive",
                    "official": active_row,
                    "local": local,
                })
            else:
                metadata_updates.append(active_row)

        for instrument_id, local in local_by_id.items():
            if instrument_id in delisted_by_id:
                delistings.append({
                    "instrument_id": instrument_id,
                    "reason": "official_delisted_evidence",
                    "official": delisted_by_id[instrument_id],
                    "local": local,
                })
                continue
            if (
                local.get("is_active") in (True, 1, "1")
                and instrument_id not in active_by_id
            ):
                review_required.append({
                    "instrument_id": instrument_id,
                    "reason": "local_active_missing_from_official_active_without_delisting_evidence",
                    "local": local,
                    "supplemental_seen": instrument_id in supplemental_ids,
                })

        supplemental_only = sorted(supplemental_ids - set(active_by_id) - set(local_by_id))
        for instrument_id in supplemental_only[:50]:
            review_required.append({
                "instrument_id": instrument_id,
                "reason": "supplemental_only_candidate_requires_official_confirmation",
                "supplemental_seen": True,
            })

        return {
            "insert_candidates": inserts,
            "metadata_update_candidates": metadata_updates,
            "reactivation_candidates": reactivations,
            "suspension_candidates": suspensions,
            "delisting_candidates": delistings,
            "review_required": review_required,
            "counts": {
                "official_active": len(active_by_id),
                "official_delisted": len(delisted_by_id),
                "local": len(local_by_id),
                "supplemental": len(supplemental_ids),
                "insert_candidates": len(inserts),
                "metadata_update_candidates": len(metadata_updates),
                "reactivation_candidates": len(reactivations),
                "suspension_candidates": len(suspensions),
                "delisting_candidates": len(delistings),
                "review_required": len(review_required),
            },
        }


def build_quote_availability_diagnostics(
    *,
    local_rows: Iterable[Dict[str, Any]],
    yfinance_rows: Iterable[Dict[str, Any]] = (),
) -> Dict[str, Any]:
    """Build quote-availability diagnostics without lifecycle mutation output."""
    local_by_id = {
        row.get("instrument_id"): row
        for row in local_rows
        if row.get("instrument_id")
    }
    yf_by_id = {
        row.get("instrument_id"): row
        for row in yfinance_rows
        if row.get("instrument_id")
    }

    no_local_quote = []
    stale_local_quote = []
    yfinance_only_quote = []
    local_only_quote = []

    for instrument_id, local in local_by_id.items():
        last_quote = local.get("last_quote")
        if not last_quote:
            no_local_quote.append(instrument_id)
        if local.get("quote_stale"):
            stale_local_quote.append(instrument_id)
        if instrument_id not in yf_by_id:
            local_only_quote.append(instrument_id)

    for instrument_id in yf_by_id:
        if instrument_id not in local_by_id:
            yfinance_only_quote.append(instrument_id)

    return {
        "source": "local_quote_yfinance_diagnostics",
        "lifecycle_authoritative": False,
        "local_count": len(local_by_id),
        "yfinance_count": len(yf_by_id),
        "no_local_quote_count": len(no_local_quote),
        "stale_local_quote_count": len(stale_local_quote),
        "yfinance_only_quote_count": len(yfinance_only_quote),
        "local_only_quote_count": len(local_only_quote),
        "no_local_quote_samples": sorted(no_local_quote)[:20],
        "stale_local_quote_samples": sorted(stale_local_quote)[:20],
        "yfinance_only_quote_samples": sorted(yfinance_only_quote)[:20],
        "local_only_quote_samples": sorted(local_only_quote)[:20],
        "mutation_candidates": [],
    }
