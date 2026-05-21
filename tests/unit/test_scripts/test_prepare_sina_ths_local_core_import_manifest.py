import pytest

from scripts.dev_validation.prepare_sina_ths_local_core_import_manifest import (
    build_batches,
    build_local_core_import_manifest,
    collect_target_instruments,
    manifest_console_summary,
    write_batch_target_files,
)


class _FakeDbOps:
    def __init__(self, instruments_by_exchange):
        self.instruments_by_exchange = instruments_by_exchange

    async def get_research_target_instruments_by_exchange(self, exchange):
        return list(self.instruments_by_exchange.get(exchange, []))


class _FakeStorage:
    def get_industry_membership(self, instrument_id, include_snapshot=False):
        memberships = {
            "000001.SZ": {
                "sw_l1_name": "银行",
                "sw_l2_name": "股份制银行Ⅱ",
                "sw_l3_name": "股份制银行Ⅲ",
                "industry_code": "490101",
            },
            "600030.SH": {
                "sw_l1_name": "非银金融",
                "sw_l2_name": "证券Ⅱ",
                "sw_l3_name": "证券Ⅲ",
                "industry_code": "510101",
            },
            "601318.SH": {
                "sw_l1_name": "非银金融",
                "sw_l2_name": "保险Ⅱ",
                "sw_l3_name": "保险Ⅲ",
                "industry_code": "510201",
            },
            "600519.SH": {
                "sw_l1_name": "食品饮料",
                "sw_l2_name": "白酒Ⅱ",
                "sw_l3_name": "白酒Ⅲ",
                "industry_code": "340601",
            },
        }
        return memberships.get(instrument_id)

    def get_company_profile(self, instrument_id, include_snapshot=False):
        return None


@pytest.mark.asyncio
async def test_collect_target_instruments_uses_research_target_filter():
    db_ops = _FakeDbOps(
        {
            "SSE": [
                {"instrument_id": "600519.SH", "exchange": "SSE", "type": "stock"},
                {
                    "instrument_id": "000001.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": False,
                },
            ]
        }
    )

    result = await collect_target_instruments(db_ops, exchanges=["SSE"])

    assert [item["instrument_id"] for item in result["SSE"]] == ["600519.SH"]


def test_build_local_core_import_manifest_profiles_and_mapping_readiness():
    instruments_by_exchange = {
        "SSE": [
            {"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
            {"instrument_id": "600030.SH", "symbol": "600030", "exchange": "SSE"},
            {"instrument_id": "601318.SH", "symbol": "601318", "exchange": "SSE"},
        ],
        "SZSE": [
            {"instrument_id": "000001.SZ", "symbol": "000001", "exchange": "SZSE"}
        ],
    }

    manifest = build_local_core_import_manifest(
        instruments_by_exchange=instruments_by_exchange,
        storage=_FakeStorage(),
        report_periods=["2024-12-31", "2024-09-30"],
        batch_size=2,
    )

    assert manifest["status"] == "ready"
    assert manifest["target_count"] == 4
    assert manifest["target_count_by_profile"] == {
        "bank": 1,
        "insurance": 1,
        "nonbank": 1,
        "securities": 1,
    }
    assert "600519.SH:SSE:nonbank" in manifest["target_lines"]
    assert "000001.SZ:SZSE:bank" in manifest["target_lines"]
    assert manifest["mapping_readiness_by_profile"]["bank"]["ready"] is True
    assert manifest["batch_count"] == 2


def test_build_local_core_import_manifest_flags_default_profile_for_review():
    manifest = build_local_core_import_manifest(
        instruments_by_exchange={
            "BSE": [
                {"instrument_id": "920833.BJ", "symbol": "920833", "exchange": "BSE"}
            ]
        },
        storage=_FakeStorage(),
        report_periods=["2024-12-31"],
    )

    assert manifest["status"] == "needs_review"
    assert manifest["profile_resolution_risks"]["default_profile_count"] == 1


def test_build_batches_summarizes_exchange_and_profile_counts():
    batches = build_batches(
        [
            {"target": "a:SSE:nonbank", "exchange": "SSE", "profile": "nonbank"},
            {"target": "b:SSE:bank", "exchange": "SSE", "profile": "bank"},
            {"target": "c:BSE:nonbank", "exchange": "BSE", "profile": "nonbank"},
        ],
        batch_size=2,
    )

    assert len(batches) == 2
    assert batches[0]["target_count_by_profile"] == {"bank": 1, "nonbank": 1}
    assert batches[1]["target_count_by_exchange"] == {"BSE": 1}


def test_manifest_console_summary_omits_full_target_payload(tmp_path):
    manifest = {
        "status": "ready",
        "mapping_version": "v",
        "target_count": 1,
        "targets": [{"instrument_id": "600519.SH"}],
        "target_lines": ["600519.SH:SSE:nonbank"],
    }

    summary = manifest_console_summary(
        manifest,
        output_path=tmp_path / "manifest.json",
        target_output_path=tmp_path / "targets.txt",
        batch_target_dir=tmp_path / "batches",
    )

    assert summary["status"] == "ready"
    assert summary["output_path"].endswith("manifest.json")
    assert summary["batch_target_dir"].endswith("batches")
    assert "targets" not in summary
    assert "target_lines" not in summary


def test_write_batch_target_files(tmp_path):
    files = write_batch_target_files(
        tmp_path,
        [
            {
                "batch_index": 1,
                "target_count": 2,
                "target_lines": ["a:SSE:nonbank", "b:SSE:bank"],
                "target_count_by_exchange": {"SSE": 2},
                "target_count_by_profile": {"bank": 1, "nonbank": 1},
            }
        ],
    )

    assert files[0]["path"].endswith("batch_0001_targets.txt")
    assert files[0]["target_count"] == 2
    assert (tmp_path / "batch_0001_targets.txt").read_text(encoding="utf-8") == (
        "a:SSE:nonbank\nb:SSE:bank\n"
    )
