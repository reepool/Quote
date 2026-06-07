from dataclasses import dataclass, replace

import pytest

from research.industry_standard_sync import IndustryStandardSyncService
from research.official_shenwan_mapping import OfficialShenwanCodeMapping
from research.providers.base import (
    BaseOfficialIndustryHistoryProvider,
    BaseIndustryNameSupplementProvider,
    BaseIndustryStandardProvider,
    IndustryClassificationHistorySnapshot,
    IndustryNameHintSnapshot,
    IndustrySnapshot,
    IndustrySourceFileSnapshot,
    IndustryTaxonomySnapshot,
    OfficialIndustryHistorySnapshot,
)
from research.providers.swsresearch_shenwan_classification import (
    SWSResearchClassificationBundle,
)
from research.providers.registry import (
    IndustryNameSupplementProviderRegistry,
    IndustryStandardProviderRegistry,
    OfficialIndustryHistoryProviderRegistry,
)
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


@dataclass
class _MockDbOps:
    instruments: list[dict]

    async def get_instruments_by_exchange(self, exchange: str):
        return [item for item in self.instruments if item["exchange"] == exchange]


@dataclass
class _MockResearchTargetDbOps:
    instruments: list[dict]

    async def get_instruments_by_exchange(self, exchange: str):
        raise AssertionError("industry_standard_sync should use research target helper first")

    async def get_research_target_instruments_by_exchange(
        self,
        exchange: str,
        *,
        is_active: bool = True,
    ):
        return [
            item
            for item in self.instruments
            if item["exchange"] == exchange and (not is_active or item.get("is_active", True))
        ]


class _MockIndustryStandardProvider(BaseIndustryStandardProvider):
    source_name = "akshare"

    def __init__(self):
        self._last_fetch_metadata = {}
        self.legacy_fetch_calls = 0
        self.component_fetch_calls = 0

    async def fetch_taxonomy(self, *, mode="direct"):
        return [
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801120.SI",
                industry_name="食品饮料",
                industry_level=1,
                source_classification="申万一级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801124.SI",
                industry_name="饮料乳品",
                industry_level=2,
                parent_code="801120.SI",
                source_classification="申万二级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="850111.SI",
                industry_name="白酒",
                industry_level=3,
                parent_code="801124.SI",
                source_classification="申万三级",
                source="akshare",
                source_mode=mode,
            ),
        ]

    async def fetch_component_sets(self, *, taxonomy_nodes=None, mode="direct"):
        self.component_fetch_calls += 1
        return {
            "850111.SI": {"600519", "000568"},
        }

    async def fetch_industries(self, *, instruments, exchange, mode="direct", limit=None):
        self.legacy_fetch_calls += 1
        selected = instruments[:limit] if limit is not None else instruments
        self._last_fetch_metadata = {"attempted_third_codes": 1, "matched_instruments": 1}
        return [
            IndustrySnapshot(
                instrument_id=selected[0]["instrument_id"],
                symbol=selected[0]["symbol"],
                exchange=exchange,
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="850111.SI",
                industry_name="白酒",
                industry_level=3,
                parent_code="801124.SI",
                mapping_status="authoritative",
                effective_date="2024-01-02",
                source_classification="申万标准行业",
                source_industry_name="白酒",
                sw_l1_code="801120.SI",
                sw_l1_name="食品饮料",
                sw_l2_code="801124.SI",
                sw_l2_name="饮料乳品",
                sw_l3_code="850111.SI",
                sw_l3_name="白酒",
                source="akshare",
                source_mode=mode,
                membership_json={"levels": {"sw_l2": {"industry_code": "801124.SI"}}},
                raw_payload={"constituent": {"股票代码": selected[0]["symbol"]}},
            )
        ]

    def get_last_fetch_metadata(self):
        return dict(self._last_fetch_metadata)


class _MockIndustryStandardOverrideProvider(BaseIndustryStandardProvider):
    source_name = "akshare"

    def __init__(self):
        self._last_fetch_metadata = {}
        self.legacy_fetch_calls = 0
        self.component_fetch_calls = 0

    async def fetch_taxonomy(self, *, mode="direct"):
        return [
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801780.SI",
                industry_name="银行",
                industry_level=1,
                source_classification="申万一级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801783.SI",
                industry_name="股份制银行Ⅱ",
                industry_level=2,
                parent_code="801780.SI",
                source_classification="申万二级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="857831.SI",
                industry_name="股份制银行Ⅲ",
                industry_level=3,
                parent_code="801783.SI",
                source_classification="申万三级",
                source="akshare",
                source_mode=mode,
            ),
        ]

    async def fetch_component_sets(self, *, taxonomy_nodes=None, mode="direct"):
        self.component_fetch_calls += 1
        return {
            "857831.SI": {"000001", "002142", "600000", "601166"},
        }

    async def fetch_industries(self, *, instruments, exchange, mode="direct", limit=None):
        self.legacy_fetch_calls += 1
        selected = instruments[:limit] if limit is not None else instruments
        self._last_fetch_metadata = {
            "exchange": exchange,
            "attempted_third_codes": 1,
            "matched_instruments": 0,
            "missing_instruments": len(selected),
            "missing_instrument_ids": [item["instrument_id"] for item in selected],
        }
        return []

    def get_last_fetch_metadata(self):
        return dict(self._last_fetch_metadata)


class _ModeAwareIndustryStandardProvider(BaseIndustryStandardProvider):
    source_name = "akshare"
    supported_modes = {"direct", "proxy_patch"}

    def __init__(self, *, direct_has_membership: bool, proxy_has_membership: bool):
        self.direct_has_membership = direct_has_membership
        self.proxy_has_membership = proxy_has_membership
        self._last_fetch_metadata = {}
        self.legacy_fetch_calls = 0
        self.component_fetch_calls = 0

    async def fetch_taxonomy(self, *, mode="direct"):
        return [
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="850111.SI",
                industry_name="白酒",
                industry_level=3,
                source_classification="申万三级",
                source="akshare",
                source_mode=mode,
            ),
        ]

    async def fetch_component_sets(self, *, taxonomy_nodes=None, mode="direct"):
        self.component_fetch_calls += 1
        has_membership = (
            self.direct_has_membership if mode == "direct" else self.proxy_has_membership
        )
        if not has_membership:
            return {}
        return {
            "850111.SI": {"600519", "000568"},
        }

    async def fetch_industries(self, *, instruments, exchange, mode="direct", limit=None):
        self.legacy_fetch_calls += 1
        selected = instruments[:limit] if limit is not None else instruments
        has_membership = (
            self.direct_has_membership if mode == "direct" else self.proxy_has_membership
        )
        if not has_membership:
            self._last_fetch_metadata = {
                "exchange": exchange,
                "attempted_third_codes": 1,
                "failed_third_codes": 1,
                "matched_instruments": 0,
                "missing_instruments": len(selected),
                "missing_instrument_ids": [item["instrument_id"] for item in selected],
            }
            return []

        self._last_fetch_metadata = {
            "exchange": exchange,
            "attempted_third_codes": 1,
            "failed_third_codes": 0,
            "matched_instruments": 1,
            "missing_instruments": 0,
            "missing_instrument_ids": [],
        }
        return [
            IndustrySnapshot(
                instrument_id=selected[0]["instrument_id"],
                symbol=selected[0]["symbol"],
                exchange=exchange,
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="850111.SI",
                industry_name="白酒",
                industry_level=3,
                mapping_status="authoritative",
                sw_l3_code="850111.SI",
                sw_l3_name="白酒",
                source="akshare",
                source_mode=mode,
                membership_json={"levels": {"sw_l3": {"industry_code": "850111.SI"}}},
                raw_payload={"constituent": {"股票代码": selected[0]["symbol"]}},
            )
        ]

    def get_last_fetch_metadata(self):
        return dict(self._last_fetch_metadata)


class _MockIndustryNameSupplementProvider(BaseIndustryNameSupplementProvider):
    source_name = "eastmoney"

    def __init__(self, names_by_symbol):
        self.names_by_symbol = dict(names_by_symbol)
        self.fetch_calls = 0

    async def fetch_industry_name_hints(
        self,
        *,
        instruments,
        exchange,
        mode="direct",
        limit=None,
    ):
        self.fetch_calls += 1
        selected = instruments[:limit] if limit is not None else instruments
        snapshots = []
        for instrument in selected:
            symbol = instrument["symbol"]
            industry_name = self.names_by_symbol.get(symbol)
            if not industry_name:
                continue
            snapshots.append(
                IndustryNameHintSnapshot(
                    instrument_id=instrument["instrument_id"],
                    symbol=symbol,
                    exchange=exchange,
                    taxonomy_system="sw",
                    taxonomy_version="sw_2021",
                    industry_name=industry_name,
                    source_classification="东方财富个股行业",
                    source="eastmoney",
                    source_mode=mode,
                    raw_payload={"f57": symbol, "f127": industry_name},
                )
            )
        return snapshots


class _MockNamedIndustryNameSupplementProvider(BaseIndustryNameSupplementProvider):
    supported_modes = {"direct"}

    def __init__(self, source_name, names_by_symbol):
        self.source_name = source_name
        self.names_by_symbol = dict(names_by_symbol)
        self.fetch_calls = 0

    async def fetch_industry_name_hints(
        self,
        *,
        instruments,
        exchange,
        mode="direct",
        limit=None,
    ):
        self.fetch_calls += 1
        selected = instruments[:limit] if limit is not None else instruments
        snapshots = []
        for instrument in selected:
            symbol = instrument["symbol"]
            raw_names = self.names_by_symbol.get(symbol)
            if not raw_names:
                continue
            names = raw_names if isinstance(raw_names, list) else [raw_names]
            for industry_name in names:
                snapshots.append(
                    IndustryNameHintSnapshot(
                        instrument_id=instrument["instrument_id"],
                        symbol=symbol,
                        exchange=exchange,
                        taxonomy_system="sw",
                        taxonomy_version="sw_2021",
                        industry_name=industry_name,
                        source_classification="测试行业补源",
                        source=self.source_name,
                        source_mode=mode,
                        raw_payload={
                            "source": self.source_name,
                            "industry_name": industry_name,
                        },
                    )
                )
        return snapshots


class _MockSteelIndustryStandardProvider(BaseIndustryStandardProvider):
    source_name = "akshare"

    def __init__(self):
        self.legacy_fetch_calls = 0

    async def fetch_taxonomy(self, *, mode="direct"):
        return [
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801040.SI",
                industry_name="钢铁",
                industry_level=1,
                source_classification="申万一级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801045.SI",
                industry_name="特钢Ⅱ",
                industry_level=2,
                parent_code="801040.SI",
                source_classification="申万二级",
                source="akshare",
                source_mode=mode,
            ),
        ]

    async def fetch_component_sets(self, *, taxonomy_nodes=None, mode="direct"):
        return {}

    async def fetch_industries(self, *, instruments, exchange, mode="direct", limit=None):
        self.legacy_fetch_calls += 1
        return []

    def get_last_fetch_metadata(self):
        return {
            "exchange": "SSE",
            "attempted_third_codes": 0,
            "matched_instruments": 0,
            "missing_instruments": 0,
        }


class _MockLeafComponentIndustryStandardProvider(BaseIndustryStandardProvider):
    source_name = "akshare"

    def __init__(self):
        self.legacy_fetch_calls = 0
        self.component_fetch_calls = 0

    async def fetch_taxonomy(self, *, mode="direct"):
        return [
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801040.SI",
                industry_name="钢铁",
                industry_level=1,
                source_classification="申万一级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801045.SI",
                industry_name="特钢Ⅱ",
                industry_level=2,
                parent_code="801040.SI",
                source_classification="申万二级",
                source="akshare",
                source_mode=mode,
            ),
        ]

    async def fetch_component_sets(self, *, taxonomy_nodes=None, mode="direct"):
        self.component_fetch_calls += 1
        return {"801045.SI": {"600117"}}

    async def fetch_industries(self, *, instruments, exchange, mode="direct", limit=None):
        self.legacy_fetch_calls += 1
        return []

    def get_last_fetch_metadata(self):
        return {
            "exchange": "SSE",
            "attempted_third_codes": 0,
            "matched_instruments": 0,
            "missing_instruments": 0,
        }


class _MockMixedLeafComponentIndustryStandardProvider(BaseIndustryStandardProvider):
    source_name = "akshare"

    def __init__(self):
        self.legacy_fetch_calls = 0
        self.component_fetch_calls = 0

    async def fetch_taxonomy(self, *, mode="direct"):
        return [
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801040.SI",
                industry_name="钢铁",
                industry_level=1,
                source_classification="申万一级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801044.SI",
                industry_name="普钢",
                industry_level=2,
                parent_code="801040.SI",
                source_classification="申万二级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="850441.SI",
                industry_name="板材",
                industry_level=3,
                parent_code="801044.SI",
                source_classification="申万三级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801045.SI",
                industry_name="特钢Ⅱ",
                industry_level=2,
                parent_code="801040.SI",
                source_classification="申万二级",
                source="akshare",
                source_mode=mode,
            ),
        ]

    async def fetch_component_sets(self, *, taxonomy_nodes=None, mode="direct"):
        self.component_fetch_calls += 1
        requested_codes = {
            str(node.industry_code)
            for node in (taxonomy_nodes or [])
        }
        component_sets = {
            "850441.SI": {"600010"},
            "801045.SI": {"600117"},
        }
        if not requested_codes:
            return component_sets
        return {
            code: symbols
            for code, symbols in component_sets.items()
            if code in requested_codes
        }

    async def fetch_industries(self, *, instruments, exchange, mode="direct", limit=None):
        self.legacy_fetch_calls += 1
        return []

    def get_last_fetch_metadata(self):
        return {
            "exchange": "SSE",
            "attempted_third_codes": 0,
            "matched_instruments": 0,
            "missing_instruments": 0,
        }


class _MockNestedNoComponentIndustryStandardProvider(BaseIndustryStandardProvider):
    source_name = "akshare"

    async def fetch_taxonomy(self, *, mode="direct"):
        return [
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801120.SI",
                industry_name="食品饮料",
                industry_level=1,
                source_classification="申万一级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801124.SI",
                industry_name="饮料乳品",
                industry_level=2,
                parent_code="801120.SI",
                source_classification="申万二级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="850111.SI",
                industry_name="白酒",
                industry_level=3,
                parent_code="801124.SI",
                source_classification="申万三级",
                source="akshare",
                source_mode=mode,
            ),
        ]

    async def fetch_component_sets(self, *, taxonomy_nodes=None, mode="direct"):
        return {}

    async def fetch_industries(self, *, instruments, exchange, mode="direct", limit=None):
        return []

    def get_last_fetch_metadata(self):
        return {
            "attempted_third_codes": 0,
            "matched_instruments": 0,
            "missing_instruments": 0,
        }


class _MockPanelNoComponentIndustryStandardProvider(BaseIndustryStandardProvider):
    source_name = "akshare"

    async def fetch_taxonomy(self, *, mode="direct"):
        return [
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801080.SI",
                industry_name="电子",
                industry_level=1,
                source_classification="申万一级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="801084.SI",
                industry_name="光学光电子",
                industry_level=2,
                parent_code="801080.SI",
                source_classification="申万二级",
                source="akshare",
                source_mode=mode,
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="850831.SI",
                industry_name="面板",
                industry_level=3,
                parent_code="801084.SI",
                source_classification="申万三级",
                source="akshare",
                source_mode=mode,
            ),
        ]

    async def fetch_component_sets(self, *, taxonomy_nodes=None, mode="direct"):
        return {}

    async def fetch_industries(self, *, instruments, exchange, mode="direct", limit=None):
        return []

    def get_last_fetch_metadata(self):
        return {
            "attempted_third_codes": 0,
            "matched_instruments": 0,
            "missing_instruments": 0,
        }


class _MockCachedTaxonomyOnlyIndustryStandardProvider(
    _MockPanelNoComponentIndustryStandardProvider
):
    async def fetch_taxonomy(self, *, mode="direct"):
        raise AssertionError("targeted gap fill should reuse cached taxonomy")


class _MockOfficialIndustryProvider(BaseOfficialIndustryHistoryProvider):
    source_name = "akshare"
    supported_modes = {"direct", "proxy_patch"}

    def __init__(self, *, latest_code: str | None = "340501", all_codes=None):
        self.latest_code = latest_code
        self.fetch_all_calls = 0
        self.all_codes = {
            "600519": "340501",
            "000568": "340501",
            "000001": "480301",
        }
        if all_codes is not None:
            self.all_codes = dict(all_codes)

    async def fetch_latest_classifications(
        self,
        *,
        instruments,
        exchange,
        mode="direct",
        limit=None,
    ):
        selected = instruments[:limit] if limit is not None else instruments
        snapshots = []
        for instrument in selected:
            symbol = instrument["symbol"]
            code = self.all_codes.get(symbol)
            if symbol == "600519" and self.latest_code is not None:
                code = self.latest_code
            if code is None:
                continue
            snapshots.append(
                OfficialIndustryHistorySnapshot(
                    instrument_id=instrument["instrument_id"],
                    symbol=symbol,
                    exchange=exchange,
                    official_industry_code=code,
                    start_date="2024-01-02",
                    update_time="2024-01-03",
                    source="akshare",
                    source_mode=mode,
                    raw_payload={"symbol": symbol, "industry_code": code},
                )
            )
        return snapshots

    async def fetch_all_latest_classifications(self, *, mode="direct"):
        self.fetch_all_calls += 1
        snapshots = []
        for symbol, code in self.all_codes.items():
            exchange = "SSE" if symbol.startswith("6") else "SZSE"
            suffix = "SH" if exchange == "SSE" else "SZ"
            snapshots.append(
                OfficialIndustryHistorySnapshot(
                    instrument_id=f"{symbol}.{suffix}",
                    symbol=symbol,
                    exchange=exchange,
                    official_industry_code=code,
                    start_date="2024-01-02",
                    update_time="2024-01-03",
                    source="akshare",
                    source_mode=mode,
                    raw_payload={"symbol": symbol, "industry_code": code},
                )
            )
        return snapshots


class _MockSWSResearchOfficialBundleProvider(BaseIndustryStandardProvider):
    source_name = "swsresearch"
    supported_modes = {"direct"}
    STOCK_HISTORY_ARTIFACT = "shenwan_stock_classification_history"
    CODE_TABLE_ARTIFACT = "shenwan_classification_code_table"

    def __init__(self):
        self.bundle_fetch_calls = 0
        self.previous_source_files = None

    def _taxonomy_nodes(self):
        return [
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="340000",
                industry_name="食品饮料",
                industry_level=1,
                source_classification="申万官方行业分类代码表",
                source="swsresearch",
                source_mode="direct",
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="340500",
                industry_name="白酒Ⅱ",
                industry_level=2,
                parent_code="340000",
                source_classification="申万官方行业分类代码表",
                source="swsresearch",
                source_mode="direct",
            ),
            IndustryTaxonomySnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="340501",
                industry_name="白酒Ⅲ",
                industry_level=3,
                parent_code="340500",
                source_classification="申万官方行业分类代码表",
                source="swsresearch",
                source_mode="direct",
            ),
        ]

    async def fetch_official_classification_bundle(
        self,
        *,
        mode="direct",
        previous_source_files=None,
        force_refresh=False,
    ):
        self.bundle_fetch_calls += 1
        self.previous_source_files = previous_source_files or {}
        return SWSResearchClassificationBundle(
            taxonomy_nodes=self._taxonomy_nodes(),
            history_rows=[
                IndustryClassificationHistorySnapshot(
                    instrument_id="600519.SH",
                    symbol="600519",
                    exchange="SSE",
                    taxonomy_system="sw",
                    taxonomy_version="sw_2021",
                    official_industry_code="340501",
                    official_start_date="2024-01-02",
                    official_update_time="2024-01-03",
                    row_hash="600519-340501",
                    source="swsresearch",
                    source_mode=mode,
                    classification_json={"symbol": "600519", "industry_code": "340501"},
                )
            ],
            latest_classifications=[
                OfficialIndustryHistorySnapshot(
                    instrument_id="600519.SH",
                    symbol="600519",
                    exchange="SSE",
                    official_industry_code="340501",
                    start_date="2024-01-02",
                    update_time="2024-01-03",
                    source="swsresearch",
                    source_mode=mode,
                    raw_payload={"symbol": "600519", "industry_code": "340501"},
                )
            ],
            source_files=[
                IndustrySourceFileSnapshot(
                    source="swsresearch",
                    source_mode=mode,
                    artifact_kind=self.STOCK_HISTORY_ARTIFACT,
                    url="https://example.test/StockClassifyUse_stock.xls",
                    parser_version="swsresearch_shenwan_classification.v1",
                    sha256="stock-sha",
                    row_count=1,
                ),
                IndustrySourceFileSnapshot(
                    source="swsresearch",
                    source_mode=mode,
                    artifact_kind=self.CODE_TABLE_ARTIFACT,
                    url="https://example.test/SwClassCode_2021.xls",
                    parser_version="swsresearch_shenwan_classification.v1",
                    sha256="code-sha",
                    row_count=3,
                ),
            ],
            changed=True,
            diagnostics={"unit": True},
        )

    async def fetch_taxonomy(self, *, mode="direct"):
        return self._taxonomy_nodes()

    async def fetch_industries(self, *, instruments, exchange, mode="direct", limit=None):
        return []


class _FailingSWSResearchOfficialBundleProvider(_MockSWSResearchOfficialBundleProvider):
    async def fetch_official_classification_bundle(
        self,
        *,
        mode="direct",
        previous_source_files=None,
        force_refresh=False,
    ):
        raise RuntimeError("certificate verify failed")


class _UnchangedSWSResearchOfficialBundleProvider(_MockSWSResearchOfficialBundleProvider):
    async def fetch_official_classification_bundle(
        self,
        *,
        mode="direct",
        previous_source_files=None,
        force_refresh=False,
    ):
        bundle = await super().fetch_official_classification_bundle(
            mode=mode,
            previous_source_files=previous_source_files,
            force_refresh=force_refresh,
        )
        return replace(
            bundle,
            changed=False,
            source_files=[],
            diagnostics={"source_files_unchanged": True, "unit": True},
        )


class _FailingOfficialIndustryProvider(_MockOfficialIndustryProvider):
    async def fetch_all_latest_classifications(self, *, mode="direct"):
        raise RuntimeError(
            "HTTPSConnectionPool(host='www.swsresearch.com', port=443): "
            "Max retries exceeded with url: /swindex/pdf/SwClass2021/"
            "StockClassifyUse_stock.xls (Caused by SSLError("
            "SSLCertVerificationError(1, '[SSL: CERTIFICATE_VERIFY_FAILED] "
            "certificate verify failed: unable to get local issuer certificate')))"
        )


class _EmptyTaxonomyIndustryStandardProvider(BaseIndustryStandardProvider):
    source_name = "akshare"

    async def fetch_taxonomy(self, *, mode="direct"):
        return []

    async def fetch_industries(self, *, instruments, exchange, mode="direct", limit=None):
        return []


def _build_research_config(
    tmp_path,
    *,
    markets=None,
    industry_module=None,
) -> ResearchConfig:
    return ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            quotes_db_alias="quotes",
        ),
        budget=ResearchBudgetConfig(default_mode="balanced", allow_paid_proxy=False),
        markets=markets or ["SSE"],
        modules={
            "industry": industry_module
            or {
                "enabled": True,
                "standard": {
                    "enabled": True,
                    "taxonomy_system": "sw",
                    "taxonomy_version": "sw_2021",
                },
            }
        },
        routing={
            "industry_standard": {
                "free_chain": [{"source": "akshare", "mode": "direct"}],
                "fallback_chain": [],
                "paid_chain": [],
            }
        },
        sources={
            "akshare": {
                "enabled": True,
                "supports_proxy_patch": True,
                "cost_tier": "free",
            }
        },
    )


@pytest.mark.asyncio
async def test_industry_standard_sync_uses_swsresearch_official_bundle_primary(tmp_path):
    research_config = _build_research_config(
        tmp_path,
        industry_module={
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "classification_primary_enabled": True,
            },
        },
    )
    research_config.routing["industry_standard"]["free_chain"] = [
        {"source": "swsresearch", "mode": "direct"}
    ]
    research_config.sources["swsresearch"] = {
        "enabled": True,
        "supports_proxy_patch": False,
        "cost_tier": "free",
    }
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    provider = _MockSWSResearchOfficialBundleProvider()

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"swsresearch": provider}),
        official_registry=OfficialIndustryHistoryProviderRegistry({}),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["source"] == "swsresearch"
    assert result["official_classification_primary"] is True
    assert result["taxonomy_nodes_written"] == 3
    assert result["classification_history_rows_written"] == 1
    assert result["source_files_written"] == 2
    assert result["total_memberships_written"] == 1
    assert result["total_official_classifications_written"] == 1
    assert provider.bundle_fetch_calls == 1

    with storage.get_connection() as conn:
        membership_row = conn.execute(
            """
            SELECT industry_code, sw_l1_code, sw_l2_code, sw_l3_code, source
            FROM industry_memberships
            WHERE instrument_id = ?
            """,
            ("600519.SH",),
        ).fetchone()
        official_row = conn.execute(
            """
            SELECT official_industry_code, mapped_industry_code, mapping_status
            FROM industry_official_classifications
            WHERE instrument_id = ?
            """,
            ("600519.SH",),
        ).fetchone()
        history_count = conn.execute(
            "SELECT COUNT(*) FROM industry_classification_history"
        ).fetchone()[0]
        source_file_count = conn.execute(
            "SELECT COUNT(*) FROM industry_source_files"
        ).fetchone()[0]

    assert tuple(membership_row) == (
        "340501",
        "340000",
        "340500",
        "340501",
        "swsresearch",
    )
    assert tuple(official_row) == ("340501", "340501", "mapped")
    assert history_count == 1
    assert source_file_count == 2


@pytest.mark.asyncio
async def test_industry_standard_sync_reports_existing_coverage_when_official_source_fails(
    tmp_path,
):
    research_config = _build_research_config(
        tmp_path,
        industry_module={
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "classification_primary_enabled": True,
            },
        },
    )
    research_config.routing["industry_standard"]["free_chain"] = [
        {"source": "swsresearch", "mode": "direct"}
    ]
    research_config.sources["swsresearch"] = {
        "enabled": True,
        "supports_proxy_patch": False,
        "cost_tier": "free",
    }
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    instruments = [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "name": "贵州茅台",
            "exchange": "SSE",
            "type": "stock",
            "is_active": True,
        }
    ]
    db_ops = _MockDbOps(instruments=instruments)
    service = IndustryStandardSyncService(
        db_ops=db_ops,
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry(
            {"swsresearch": _MockSWSResearchOfficialBundleProvider()}
        ),
        official_registry=OfficialIndustryHistoryProviderRegistry({}),
    )
    await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    failing_service = IndustryStandardSyncService(
        db_ops=db_ops,
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry(
            {"swsresearch": _FailingSWSResearchOfficialBundleProvider()}
        ),
        official_registry=OfficialIndustryHistoryProviderRegistry({}),
    )

    result = await failing_service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "degraded"
    assert result["source_unavailable"] is True
    assert result["total_memberships_written"] == 0
    assert result["reason"].startswith("官方分类上游临时不可用")
    diagnostics = result["exchanges"][0]["diagnostics"]
    assert diagnostics["source_unavailable"] is True
    assert diagnostics["existing_authoritative_memberships"] == 1
    assert diagnostics["target_instruments"] == 1


@pytest.mark.asyncio
async def test_industry_standard_sync_keeps_sws_unchanged_result_when_fallback_fails(
    tmp_path,
):
    research_config = _build_research_config(
        tmp_path,
        industry_module={
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "classification_primary_enabled": True,
            },
        },
    )
    research_config.routing["industry_standard"]["free_chain"] = [
        {"source": "swsresearch", "mode": "direct"},
        {"source": "akshare", "mode": "direct"},
    ]
    research_config.sources.update(
        {
            "swsresearch": {
                "enabled": True,
                "supports_proxy_patch": False,
                "cost_tier": "free",
            },
            "akshare": {
                "enabled": True,
                "supports_proxy_patch": True,
                "cost_tier": "free",
            },
        }
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    sws_provider = _UnchangedSWSResearchOfficialBundleProvider()
    for node in sws_provider._taxonomy_nodes():
        storage.upsert_industry_taxonomy(node)

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry(
            {
                "swsresearch": sws_provider,
                "akshare": _MockIndustryStandardProvider(),
            }
        ),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _FailingOfficialIndustryProvider()}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "degraded"
    assert result["source"] == "swsresearch"
    assert result["source_files_unchanged"] is True
    assert "source_unavailable" not in result
    assert result["attempted_sources"] == ["swsresearch:direct", "akshare:direct"]
    diagnostics = result["exchanges"][0]["diagnostics"]
    assert diagnostics["source_files_unchanged"] is True
    assert diagnostics["existing_authoritative_memberships"] == 0
    assert result["exchanges"][0]["error_message"] == (
        "Source files unchanged but existing membership coverage is incomplete"
    )


@pytest.mark.asyncio
async def test_industry_standard_sync_uses_akshare_fallback_with_cached_official_taxonomy(tmp_path):
    research_config = _build_research_config(
        tmp_path,
        industry_module={
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "classification_primary_enabled": True,
            },
        },
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    for node in _MockSWSResearchOfficialBundleProvider()._taxonomy_nodes():
        storage.upsert_industry_taxonomy(node)

    standard_provider = _MockIndustryStandardProvider()
    official_provider = _MockOfficialIndustryProvider(
        latest_code="340501",
        all_codes={"600519": "340501"},
    )
    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        official_registry=OfficialIndustryHistoryProviderRegistry({"akshare": official_provider}),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["source"] == "akshare"
    assert result["official_classification_primary"] is True
    assert result["taxonomy_nodes_written"] == 0
    assert result["classification_history_rows_written"] == 0
    assert result["total_memberships_written"] == 1
    assert official_provider.fetch_all_calls == 1
    assert standard_provider.component_fetch_calls == 0
    assert standard_provider.legacy_fetch_calls == 0

    loaded = storage.get_industry_membership("600519.SH")
    assert loaded is not None
    assert loaded["industry_code"] == "340501"
    assert loaded["sw_l1_code"] == "340000"
    assert loaded["source"] == "akshare"


@pytest.mark.asyncio
async def test_industry_standard_sync_writes_taxonomy_and_authoritative_memberships(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    standard_provider = _MockIndustryStandardProvider()

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _MockOfficialIndustryProvider()}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["source"] == "akshare"
    assert result["taxonomy_nodes_written"] == 3
    assert result["total_memberships_written"] == 1
    assert result["total_official_classifications_written"] == 1

    with storage.get_connection() as conn:
        taxonomy_count = conn.execute("SELECT COUNT(*) FROM industry_taxonomy").fetchone()[0]
        membership_count = conn.execute("SELECT COUNT(*) FROM industry_memberships").fetchone()[0]
        official_count = conn.execute(
            "SELECT COUNT(*) FROM industry_official_classifications"
        ).fetchone()[0]
        run_row = conn.execute(
            "SELECT domain, status, rows_written FROM ingestion_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()

    assert taxonomy_count == 3
    assert membership_count == 1
    assert official_count == 1
    assert tuple(run_row) == ("industry_standard", "success", 5)
    assert standard_provider.legacy_fetch_calls == 0
    assert standard_provider.component_fetch_calls == 1

    loaded = storage.get_industry_membership("600519.SH")
    assert loaded is not None
    assert loaded["mapping_status"] == "authoritative"
    assert loaded["sw_l2_code"] == "801124.SI"
    assert loaded["industry_name"] == "白酒"

    official = storage.get_official_industry_classification("600519.SH")
    assert official is not None
    assert official["official_industry_code"] == "340501"
    assert official["mapped_industry_code"] is None
    assert official["mapping_status"] == "unmapped"


@pytest.mark.asyncio
async def test_industry_standard_sync_prefers_research_target_instrument_helper(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = IndustryStandardSyncService(
        db_ops=_MockResearchTargetDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": _MockIndustryStandardProvider()}),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _MockOfficialIndustryProvider()}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    loaded = storage.get_industry_membership("600519.SH")
    assert loaded is not None
    assert loaded["mapping_status"] == "authoritative"


@pytest.mark.asyncio
async def test_industry_standard_sync_reports_taxonomy_progress_when_memberships_fail(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry(
            {
                "akshare": _ModeAwareIndustryStandardProvider(
                    direct_has_membership=False,
                    proxy_has_membership=False,
                )
            }
        ),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _MockOfficialIndustryProvider(latest_code=None, all_codes={})}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "degraded"
    assert result["source"] == "akshare"
    assert result["mode"] == "direct"
    assert result["taxonomy_nodes_written"] == 1
    assert result["total_memberships_written"] == 0
    assert result["total_official_classifications_written"] == 0
    assert result["exchanges"][0]["diagnostics"]["fallback_diagnostics"]["failed_third_codes"] == 1
    assert "attempted_third_codes=1" in result["exchanges"][0]["error_message"]


@pytest.mark.asyncio
async def test_industry_standard_sync_can_target_missing_instrument_ids_by_exchange(tmp_path):
    research_config = _build_research_config(tmp_path, markets=["SZSE"])
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "exchange": "SZSE",
                    "type": "stock",
                    "is_active": True,
                },
                {
                    "instrument_id": "002142.SZ",
                    "symbol": "002142",
                    "name": "宁波银行",
                    "exchange": "SZSE",
                    "type": "stock",
                    "is_active": True,
                },
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry(
            {"akshare": _MockIndustryStandardOverrideProvider()}
        ),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _MockOfficialIndustryProvider(latest_code=None, all_codes={})}
        ),
    )

    result = await service.sync(
        exchanges=["SZSE"],
        instrument_ids_by_exchange={"SZSE": ["000001.SZ"]},
    )

    assert result["status"] == "success"
    assert result["total_memberships_written"] == 1

    membership_000001 = storage.get_industry_membership("000001.SZ")
    membership_002142 = storage.get_industry_membership("002142.SZ")

    assert membership_000001 is not None
    assert membership_000001["mapping_status"] == "authoritative"
    assert membership_002142 is None


@pytest.mark.asyncio
async def test_industry_standard_sync_uses_name_supplement_for_component_miss(tmp_path):
    research_config = _build_research_config(
        tmp_path,
        industry_module={
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "name_supplement": {
                    "enabled": True,
                    "candidates": [{"source": "eastmoney", "mode": "direct"}],
                    "allow_level2_leaf_matches": True,
                },
            },
        },
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    standard_provider = _MockSteelIndustryStandardProvider()
    supplement_provider = _MockIndustryNameSupplementProvider({"600117": "特钢Ⅱ"})

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600117.SH",
                    "symbol": "600117",
                    "name": "西宁特钢",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        supplement_registry=IndustryNameSupplementProviderRegistry(
            {"eastmoney": supplement_provider}
        ),
        official_registry=OfficialIndustryHistoryProviderRegistry({}),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_memberships_written"] == 1
    diagnostics = result["exchanges"][0]["diagnostics"]
    assert diagnostics["component_current_memberships_written"] == 0
    assert diagnostics["name_supplement_attempted"] is True
    assert diagnostics["name_supplement_memberships_written"] == 1
    assert diagnostics["name_supplement_matched_level_counts"] == {"level_2": 1}
    assert diagnostics["fallback_attempted"] is False
    assert supplement_provider.fetch_calls == 1
    assert standard_provider.legacy_fetch_calls == 0

    membership = storage.get_industry_membership("600117.SH")
    assert membership is not None
    assert membership["mapping_status"] == "authoritative"
    assert membership["industry_code"] == "801045.SI"
    assert membership["industry_level"] == 2
    assert membership["sw_l1_code"] == "801040.SI"
    assert membership["sw_l2_code"] == "801045.SI"
    assert membership["sw_l3_code"] is None
    assert membership["source"] == "eastmoney"
    assert membership["source_classification"] == "申万行业名称补源"


@pytest.mark.asyncio
async def test_industry_standard_sync_builds_authoritative_membership_from_leaf_component_set(
    tmp_path,
):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    standard_provider = _MockLeafComponentIndustryStandardProvider()

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600117.SH",
                    "symbol": "600117",
                    "name": "西宁特钢",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        supplement_registry=IndustryNameSupplementProviderRegistry({}),
        official_registry=OfficialIndustryHistoryProviderRegistry({}),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_memberships_written"] == 1
    diagnostics = result["exchanges"][0]["diagnostics"]
    assert diagnostics["component_current_memberships_written"] == 1
    assert diagnostics["fallback_attempted"] is False
    assert standard_provider.component_fetch_calls == 1
    assert standard_provider.legacy_fetch_calls == 0

    membership = storage.get_industry_membership("600117.SH")
    assert membership is not None
    assert membership["mapping_status"] == "authoritative"
    assert membership["industry_code"] == "801045.SI"
    assert membership["industry_level"] == 2
    assert membership["sw_l1_code"] == "801040.SI"
    assert membership["sw_l2_code"] == "801045.SI"
    assert membership["sw_l3_code"] is None
    assert membership["source"] == "akshare"
    assert membership["source_classification"] == "申万叶子行业成分股"


@pytest.mark.asyncio
async def test_industry_standard_sync_does_not_use_non_leaf_l2_name_supplement(tmp_path):
    research_config = _build_research_config(
        tmp_path,
        industry_module={
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "name_supplement": {
                    "enabled": True,
                    "candidates": [{"source": "eastmoney", "mode": "direct"}],
                    "allow_level2_leaf_matches": True,
                },
            },
        },
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    supplement_provider = _MockIndustryNameSupplementProvider({"600519": "饮料乳品"})

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry(
            {"akshare": _MockNestedNoComponentIndustryStandardProvider()}
        ),
        supplement_registry=IndustryNameSupplementProviderRegistry(
            {"eastmoney": supplement_provider}
        ),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _MockOfficialIndustryProvider(latest_code=None, all_codes={})}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "degraded"
    diagnostics = result["exchanges"][0]["diagnostics"]
    assert diagnostics["name_supplement_hints_returned"] == 1
    assert diagnostics["name_supplement_memberships_written"] == 0
    assert diagnostics["name_supplement_unmatched_name_samples"][0]["reason"] == (
        "matched_name_is_not_usable_level3_or_leaf_level2"
    )
    assert storage.get_industry_membership("600519.SH") is None


@pytest.mark.asyncio
async def test_industry_standard_sync_uses_manual_name_supplement_entry(tmp_path):
    research_config = _build_research_config(
        tmp_path,
        markets=["SSE"],
        industry_module={
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "name_supplement": {
                    "enabled": True,
                    "candidates": [{"source": "manual", "mode": "configured"}],
                    "allow_level2_leaf_matches": True,
                    "manual_entries": [
                        {
                            "instrument_id": "688781.SH",
                            "industry_name": "面板",
                            "industry_code": "850831.SI",
                            "reason": "Manual supplement for validated panel mapping",
                        }
                    ],
                },
            },
        },
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "688781.SH",
                    "symbol": "688781",
                    "name": "视涯科技",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry(
            {"akshare": _MockPanelNoComponentIndustryStandardProvider()}
        ),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _MockOfficialIndustryProvider(latest_code="270301")}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    diagnostics = result["exchanges"][0]["diagnostics"]
    assert diagnostics["name_supplement_attempted"] is True
    assert diagnostics["name_supplement_memberships_written"] == 1
    assert diagnostics["name_supplement_attempted_sources"] == ["manual:configured"]
    assert diagnostics["name_supplement_matched_level_counts"] == {"level_3": 1}

    membership = storage.get_industry_membership("688781.SH")
    assert membership is not None
    assert membership["industry_code"] == "850831.SI"
    assert membership["sw_l1_code"] == "801080.SI"
    assert membership["sw_l2_code"] == "801084.SI"
    assert membership["sw_l3_code"] == "850831.SI"
    assert membership["source"] == "manual"
    assert membership["source_mode"] == "configured"
    assert membership["source_classification"] == "申万行业名称补源"


@pytest.mark.asyncio
async def test_industry_standard_sync_uses_sina_structured_leaf_before_coarse_hint(
    tmp_path,
):
    research_config = _build_research_config(
        tmp_path,
        markets=["SSE"],
        industry_module={
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "name_supplement": {
                    "enabled": True,
                    "candidates": [
                        {"source": "sina", "mode": "direct"},
                        {"source": "eastmoney", "mode": "direct"},
                    ],
                    "allow_level2_leaf_matches": True,
                },
            },
        },
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    sina_provider = _MockNamedIndustryNameSupplementProvider(
        "sina",
        {"688781": ["面板", "光学光电子"]},
    )
    eastmoney_provider = _MockNamedIndustryNameSupplementProvider(
        "eastmoney",
        {"688781": "光学光电子"},
    )
    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "688781.SH",
                    "symbol": "688781",
                    "name": "视涯科技",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry(
            {"akshare": _MockPanelNoComponentIndustryStandardProvider()}
        ),
        supplement_registry=IndustryNameSupplementProviderRegistry(
            {
                "sina": sina_provider,
                "eastmoney": eastmoney_provider,
            }
        ),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _MockOfficialIndustryProvider(latest_code="270301")}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    diagnostics = result["exchanges"][0]["diagnostics"]
    assert diagnostics["name_supplement_attempted_sources"] == ["sina:direct"]
    assert diagnostics["name_supplement_memberships_written"] == 1
    assert diagnostics["name_supplement_matched_level_counts"] == {"level_3": 1}
    assert sina_provider.fetch_calls == 1
    assert eastmoney_provider.fetch_calls == 0

    membership = storage.get_industry_membership("688781.SH")
    assert membership is not None
    assert membership["industry_code"] == "850831.SI"
    assert membership["sw_l2_code"] == "801084.SI"
    assert membership["sw_l3_code"] == "850831.SI"
    assert membership["source"] == "sina"


@pytest.mark.asyncio
async def test_targeted_industry_standard_sync_reuses_cached_taxonomy_for_gap_fill(
    tmp_path,
):
    research_config = _build_research_config(
        tmp_path,
        markets=["SSE"],
        industry_module={
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "name_supplement": {
                    "enabled": True,
                    "candidates": [{"source": "sina", "mode": "direct"}],
                    "allow_level2_leaf_matches": True,
                },
            },
        },
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    taxonomy_provider = _MockPanelNoComponentIndustryStandardProvider()
    for node in await taxonomy_provider.fetch_taxonomy(mode="direct"):
        storage.upsert_industry_taxonomy(node)

    sina_provider = _MockNamedIndustryNameSupplementProvider(
        "sina",
        {"688781": "面板"},
    )
    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "688781.SH",
                    "symbol": "688781",
                    "name": "视涯科技",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry(
            {"akshare": _MockCachedTaxonomyOnlyIndustryStandardProvider()}
        ),
        supplement_registry=IndustryNameSupplementProviderRegistry(
            {"sina": sina_provider}
        ),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _MockOfficialIndustryProvider(latest_code="270301")}
        ),
    )

    result = await service.sync(
        exchanges=["SSE"],
        instrument_ids_by_exchange={"SSE": ["688781.SH"]},
    )

    assert result["status"] == "success"
    assert result["taxonomy_source"] == "cache"
    assert result["total_official_classifications_written"] == 0
    diagnostics = result["exchanges"][0]["diagnostics"]
    assert diagnostics["official_target_records"] == 0
    assert diagnostics["name_supplement_attempted_sources"] == ["sina:direct"]

    membership = storage.get_industry_membership("688781.SH")
    assert membership is not None
    assert membership["industry_code"] == "850831.SI"
    assert membership["source"] == "sina"


@pytest.mark.asyncio
async def test_industry_standard_sync_can_fallback_to_proxy_patch_candidate(tmp_path):
    research_config = _build_research_config(tmp_path)
    research_config.budget.allow_paid_proxy = True
    research_config.routing["industry_standard"]["paid_chain"] = [
        {"source": "akshare", "mode": "proxy_patch"}
    ]

    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry(
            {
                "akshare": _ModeAwareIndustryStandardProvider(
                    direct_has_membership=False,
                    proxy_has_membership=True,
                )
            }
        ),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _MockOfficialIndustryProvider(latest_code=None, all_codes={})}
        ),
    )

    result = await service.sync(
        exchanges=["SSE"],
        limit_per_exchange=1,
        allow_paid_proxy=True,
    )

    assert result["status"] == "success"
    assert result["source"] == "akshare"
    assert result["mode"] == "proxy_patch"
    assert result["attempted_sources"] == ["akshare:direct", "akshare:proxy_patch"]
    assert result["taxonomy_nodes_written"] == 1
    assert result["total_memberships_written"] == 1
    assert result["total_official_classifications_written"] == 0


@pytest.mark.asyncio
async def test_industry_standard_sync_uses_component_membership_when_official_record_unmapped(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    standard_provider = _MockIndustryStandardProvider()

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _MockOfficialIndustryProvider(latest_code="999999")}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_memberships_written"] == 1
    assert result["total_official_classifications_written"] == 1
    assert result["exchanges"][0]["diagnostics"]["official_unmapped_records"] == 1
    assert result["exchanges"][0]["diagnostics"]["component_current_memberships_written"] == 1
    assert result["exchanges"][0]["diagnostics"]["fallback_memberships_written"] == 0
    assert standard_provider.legacy_fetch_calls == 0

    loaded = storage.get_industry_membership("600519.SH")
    assert loaded is not None
    assert loaded["industry_code"] == "850111.SI"
    assert loaded["source_classification"] == "申万叶子行业成分股"

    official = storage.get_official_industry_classification("600519.SH")
    assert official is not None
    assert official["mapping_status"] == "unmapped"


@pytest.mark.asyncio
async def test_industry_standard_sync_does_not_use_cached_official_code_mappings(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    storage.replace_official_industry_code_mappings(
        [
            OfficialShenwanCodeMapping(
                official_industry_code="340501",
                best_taxonomy_industry_code="850111.SI",
                taxonomy_industry_code="850111.SI",
                overlap_count=2,
                official_symbol_count=2,
                taxonomy_symbol_count=2,
                precision=1.0,
                recall=1.0,
                confidence="high",
            )
        ],
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="direct",
    )

    standard_provider = _MockIndustryStandardProvider()
    official_provider = _MockOfficialIndustryProvider()
    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": official_provider}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert (
        result["exchanges"][0]["diagnostics"]["official_mapping_source"]
        == "not_applied_to_current_sync"
    )
    assert result["exchanges"][0]["diagnostics"]["component_current_cache_source"] == "live_fetch"
    assert standard_provider.component_fetch_calls == 1
    assert official_provider.fetch_all_calls == 0

    membership = storage.get_industry_membership("600519.SH")
    assert membership is not None
    assert membership["source_classification"] == "申万叶子行业成分股"


@pytest.mark.asyncio
async def test_industry_standard_sync_does_not_rebuild_mapping_cache_on_miss(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    standard_provider = _MockIndustryStandardProvider()
    official_provider = _MockOfficialIndustryProvider()
    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": official_provider}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert (
        result["exchanges"][0]["diagnostics"]["official_mapping_source"]
        == "not_applied_to_current_sync"
    )
    assert standard_provider.component_fetch_calls == 1
    assert official_provider.fetch_all_calls == 0

    cached_rows = storage.get_official_industry_code_mappings(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        max_age_days=7,
    )
    component_sets = storage.get_industry_component_sets(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        max_age_days=7,
    )
    assert cached_rows == []
    assert result["exchanges"][0]["diagnostics"]["component_current_cache_source"] == "live_fetch"
    assert component_sets["850111.SI"] == {"600519", "000568"}


@pytest.mark.asyncio
async def test_official_mapping_refresh_rebuilds_and_persists_cache(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    standard_provider = _MockIndustryStandardProvider()
    official_provider = _MockOfficialIndustryProvider()
    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(instruments=[]),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": official_provider}
        ),
    )

    result = await service.refresh_official_mapping_cache(exchanges=["SSE"])

    assert result["status"] == "success"
    assert result["source"] == "akshare"
    assert result["mode"] == "direct"
    assert result["taxonomy_nodes_written"] == 3
    assert result["mapping_cache_rows_written"] == 2
    assert result["mapped_code_count"] == 1
    assert result["unmapped_code_count"] == 1
    assert result["total_code_count"] == 2
    assert result["component_taxonomy_count"] == 1
    assert result["mapping_source"] == "live_rebuild"
    assert result["component_cache_source"] == "live_fetch"
    assert standard_provider.component_fetch_calls == 1
    assert official_provider.fetch_all_calls == 1

    with storage.get_connection() as conn:
        membership_count = conn.execute(
            "SELECT COUNT(*) FROM industry_memberships"
        ).fetchone()[0]
        official_count = conn.execute(
            "SELECT COUNT(*) FROM industry_official_classifications"
        ).fetchone()[0]
        run_row = conn.execute(
            "SELECT domain, status, rows_written FROM ingestion_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()

    assert membership_count == 0
    assert official_count == 0
    assert tuple(run_row) == ("industry_official_mapping_refresh", "success", 5)


@pytest.mark.asyncio
async def test_industry_standard_sync_reuses_cached_component_sets_on_mapping_cache_miss(
    tmp_path,
):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    storage.replace_industry_component_sets(
        {
            "850111.SI": {"600519", "000568"},
        },
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="direct",
    )

    standard_provider = _MockIndustryStandardProvider()
    official_provider = _MockOfficialIndustryProvider()
    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": official_provider}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert (
        result["exchanges"][0]["diagnostics"]["official_mapping_source"]
        == "not_applied_to_current_sync"
    )
    assert result["exchanges"][0]["diagnostics"]["component_current_cache_source"] == "cache"
    assert standard_provider.component_fetch_calls == 0
    assert official_provider.fetch_all_calls == 0


@pytest.mark.asyncio
async def test_industry_standard_sync_force_component_refresh_bypasses_cache(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    storage.replace_industry_component_sets(
        {
            "850111.SI": {"600519", "000568"},
        },
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="direct",
    )

    standard_provider = _MockIndustryStandardProvider()
    official_provider = _MockOfficialIndustryProvider()
    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": official_provider}
        ),
    )

    result = await service.sync(
        exchanges=["SSE"],
        limit_per_exchange=1,
        force_component_refresh=True,
    )

    assert result["status"] == "success"
    assert (
        result["exchanges"][0]["diagnostics"]["component_current_cache_source"]
        == "forced_live_fetch"
    )
    assert result["exchanges"][0]["diagnostics"]["component_current_force_refresh"] is True
    assert standard_provider.component_fetch_calls == 1


@pytest.mark.asyncio
async def test_industry_standard_sync_repairs_missing_leaf_component_cache(tmp_path):
    research_config = _build_research_config(
        tmp_path,
        industry_module={
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "component_cache": {
                    "cache_max_age_days": 7,
                    "minimum_component_sets": 1,
                },
            },
        },
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    storage.replace_industry_component_sets(
        {
            "850441.SI": {"600010"},
        },
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="direct",
    )

    standard_provider = _MockMixedLeafComponentIndustryStandardProvider()
    official_provider = _MockOfficialIndustryProvider(latest_code=None, all_codes={})
    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600117.SH",
                    "symbol": "600117",
                    "name": "西宁特钢",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": official_provider}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["exchanges"][0]["diagnostics"]["component_current_cache_source"] == "cache_gap_fill"
    assert standard_provider.component_fetch_calls == 1

    component_sets = storage.get_industry_component_sets(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        max_age_days=7,
    )
    assert component_sets["850441.SI"] == {"600010"}
    assert component_sets["801045.SI"] == {"600117"}

    loaded = storage.get_industry_membership("600117.SH")
    assert loaded is not None
    assert loaded["industry_code"] == "801045.SI"
    assert loaded["sw_l2_code"] == "801045.SI"
    assert loaded["sw_l3_code"] is None


@pytest.mark.asyncio
async def test_industry_standard_sync_removes_stale_membership_when_target_unresolved(
    tmp_path,
):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    storage.upsert_industry_membership(
        IndustrySnapshot(
            instrument_id="688781.SH",
            symbol="688781",
            exchange="SSE",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            industry_code="850831.SI",
            industry_name="面板",
            industry_level=3,
            parent_code="801084.SI",
            mapping_status="authoritative",
            source_classification="申万三级成分股",
            sw_l1_code="801080.SI",
            sw_l1_name="电子",
            sw_l2_code="801084.SI",
            sw_l2_name="光学光电子",
            sw_l3_code="850831.SI",
            sw_l3_name="面板",
            source="akshare",
            source_mode="proxy_patch",
            membership_json={"legacy": True},
            raw_payload={"symbol": "688781"},
        )
    )

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "688781.SH",
                    "symbol": "688781",
                    "name": "N视涯",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry(
            {"akshare": _MockNestedNoComponentIndustryStandardProvider()}
        ),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _MockOfficialIndustryProvider(latest_code="270301")}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "degraded"
    assert (
        result["exchanges"][0]["diagnostics"]["stale_current_memberships_removed"]
        == 1
    )
    assert result["exchanges"][0]["diagnostics"]["unresolved_target_instruments"] == 1
    assert result["exchanges"][0]["diagnostics"]["unresolved_instrument_ids"] == [
        "688781.SH"
    ]
    assert storage.get_industry_membership("688781.SH") is None


@pytest.mark.asyncio
async def test_industry_standard_sync_ignores_manual_override_for_current_membership(tmp_path):
    research_config = _build_research_config(tmp_path)
    research_config.modules["industry"]["standard"]["official_mapping"] = {
        "min_overlap_count": 2,
        "min_precision": 0.6,
        "min_recall": 0.6,
        "manual_overrides": {
            "480301": {
                "taxonomy_industry_code": "857831.SI",
                "confidence": "high",
                "reason": "Ping An Bank manual override",
            }
        },
    }
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    standard_provider = _MockIndustryStandardOverrideProvider()
    official_provider = _MockOfficialIndustryProvider(
        latest_code="480301",
        all_codes={
            "000001": "480301",
            "002142": "480301",
        },
    )
    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "exchange": "SZSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": official_provider}
        ),
    )

    result = await service.sync(exchanges=["SZSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_memberships_written"] == 1
    assert result["exchanges"][0]["diagnostics"]["official_unmapped_records"] == 1
    assert result["exchanges"][0]["diagnostics"]["fallback_attempted"] is False
    assert standard_provider.legacy_fetch_calls == 0
    assert official_provider.fetch_all_calls == 0

    membership = storage.get_industry_membership("000001.SZ")
    assert membership is not None
    assert membership["sw_l2_code"] == "801783.SI"
    assert membership["sw_l3_code"] == "857831.SI"
    assert membership["source_classification"] == "申万叶子行业成分股"

    official = storage.get_official_industry_classification("000001.SZ")
    assert official is not None
    assert official["mapping_status"] == "unmapped"
    assert official["mapped_industry_code"] is None
    assert official["classification"]["mapping"] is None


@pytest.mark.asyncio
async def test_industry_standard_sync_ignores_cached_manual_override_mapping(tmp_path):
    research_config = _build_research_config(tmp_path)
    research_config.modules["industry"]["standard"]["official_mapping"] = {
        "cache_max_age_days": 7,
        "minimum_mapping_rows": 1,
        "minimum_mapped_rows": 1,
        "allow_live_rebuild_on_cache_miss": True,
        "manual_overrides": {
            "480301": {
                "taxonomy_industry_code": "857831.SI",
                "confidence": "high",
                "reason": "Ping An Bank manual override",
            }
        },
    }
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    storage.replace_official_industry_code_mappings(
        [
            OfficialShenwanCodeMapping(
                official_industry_code="480301",
                best_taxonomy_industry_code="857831.SI",
                taxonomy_industry_code=None,
                overlap_count=2,
                official_symbol_count=2,
                taxonomy_symbol_count=4,
                precision=0.5,
                recall=1.0,
                confidence="unmapped",
            )
        ],
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="direct",
    )

    standard_provider = _MockIndustryStandardOverrideProvider()
    official_provider = _MockOfficialIndustryProvider(
        latest_code="480301",
        all_codes={
            "000001": "480301",
            "002142": "480301",
        },
    )
    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "exchange": "SZSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": standard_provider}),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": official_provider}
        ),
    )

    result = await service.sync(exchanges=["SZSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert (
        result["exchanges"][0]["diagnostics"]["official_mapping_source"]
        == "not_applied_to_current_sync"
    )
    assert result["exchanges"][0]["diagnostics"]["component_current_cache_source"] == "live_fetch"
    assert standard_provider.component_fetch_calls == 1
    assert official_provider.fetch_all_calls == 0
    assert standard_provider.legacy_fetch_calls == 0

    membership = storage.get_industry_membership("000001.SZ")
    assert membership is not None
    assert membership["industry_code"] == "857831.SI"
    assert membership["source_classification"] == "申万叶子行业成分股"

    official = storage.get_official_industry_classification("000001.SZ")
    assert official is not None
    assert official["classification"]["mapping"] is None


@pytest.mark.asyncio
async def test_industry_standard_sync_allows_optional_empty_bse_when_no_memberships(
    tmp_path,
):
    research_config = _build_research_config(
        tmp_path,
        markets=["BSE"],
        industry_module={
            "enabled": True,
            "optional_empty_exchanges": ["BSE"],
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
            },
        },
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "430001.BJ",
                    "symbol": "430001",
                    "name": "北交样本",
                    "exchange": "BSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": _MockIndustryStandardOverrideProvider()}),
        official_registry=OfficialIndustryHistoryProviderRegistry(
            {"akshare": _MockOfficialIndustryProvider(all_codes={})}
        ),
    )

    result = await service.sync(exchanges=["BSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_memberships_written"] == 0
    assert result["successful_exchanges"] == 1
    assert result["exchanges"][0]["status"] == "success"
    assert result["exchanges"][0]["diagnostics"]["optional_empty_exchange"] is True


@pytest.mark.asyncio
async def test_industry_standard_sync_allows_all_optional_empty_bse_without_taxonomy_candidate(
    tmp_path,
):
    research_config = _build_research_config(
        tmp_path,
        markets=["BSE"],
        industry_module={
            "enabled": True,
            "optional_empty_exchanges": ["BSE"],
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
            },
        },
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = IndustryStandardSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "430001.BJ",
                    "symbol": "430001",
                    "name": "北交样本",
                    "exchange": "BSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryStandardProviderRegistry({"akshare": _EmptyTaxonomyIndustryStandardProvider()}),
        official_registry=OfficialIndustryHistoryProviderRegistry({}),
    )

    result = await service.sync(exchanges=["BSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["source"] is None
    assert result["taxonomy_nodes_written"] == 0
    assert result["total_memberships_written"] == 0
    assert result["successful_exchanges"] == 1
    assert result["exchanges"][0]["status"] == "success"
