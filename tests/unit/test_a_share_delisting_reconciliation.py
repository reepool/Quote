from datetime import date, datetime, timedelta
import json
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest
import pytest_asyncio
from openpyxl import Workbook
from sqlalchemy import select, text
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from data_sources.a_share_official_stock_master import AShareOfficialStockMasterSource
from database.instrument_lifecycle import (
    APPLIED, DATE_CORRECTED, FUTURE, METADATA_DDL, VERIFICATION,
)
from database.models import InstrumentDB
from database.operations import DatabaseOperations
from instrument_master_delisting import (
    ANNOUNCEMENT_PARSER_VERSION,
    AShareDelistingReconciler,
    reusable_terminal_verifications,
)
from research.financial_disclosure_incremental_sync import (
    FinancialDisclosureIncrementalSyncService,
)
from utils.date_utils import get_shanghai_time


FIXTURES = Path(__file__).parents[1] / "fixtures" / "a_share_official_master"
KONKA = dict(
    instrument_id="000016.SZ",
    symbol="000016",
    name="*ST康佳A",
    exchange="SZSE",
    type="stock",
    currency="CNY",
    listed_date="1992-03-27",
    status="active",
    is_active=True,
    trading_status=1,
    source="szse_official",
)


@pytest_asyncio.fixture
async def owner(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'quotes.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(InstrumentDB.__table__.create)
    operations = DatabaseOperations(auto_initialize=False)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    operations.get_async_session = sessions
    operations._test_engine = engine
    yield operations
    await engine.dispose()


async def local_record(owner):
    async with owner.get_async_session() as session:
        row = (
            await session.execute(
                select(InstrumentDB).where(
                    InstrumentDB.instrument_id == KONKA["instrument_id"]
                )
            )
        ).scalar_one()
        return {
            "status": row.status,
            "is_active": row.is_active,
            "trading_status": row.trading_status,
            "delisted_date": row.delisted_date,
        }


@pytest.mark.asyncio
async def test_real_xlsx_to_delisting_to_financial_scope(owner):
    assert await owner.save_instruments_batch([KONKA])
    source = AShareOfficialStockMasterSource(
        "exchange_official_a_stock",
        config={
            "szse": {"terminal_list_file": str(FIXTURES / "szse_terminated.xlsx")},
        },
    )
    snapshot = await source.get_delisting_snapshot("SZSE")
    assert snapshot["complete"], snapshot["errors"]
    assert "200016.SZ" not in {row["instrument_id"] for row in snapshot["rows"]}
    result = await AShareDelistingReconciler(owner, None).reconcile(
        "SZSE",
        {"rows_by_id": {KONKA["instrument_id"]: KONKA}},
        [],
        {"official_current_list_complete": True},
        snapshot=snapshot,
    )
    assert result["deactivated_count"] == 1
    row = await local_record(owner)
    assert row == {
        "status": "delisted",
        "is_active": False,
        "trading_status": 0,
        "delisted_date": datetime(2026, 10, 23),
    }
    financial = FinancialDisclosureIncrementalSyncService.__new__(
        FinancialDisclosureIncrementalSyncService
    )
    financial.db_ops = owner
    assert await financial._load_active_instruments(["SZSE"]) == []
    repeated = await AShareDelistingReconciler(owner, None).reconcile(
        "SZSE",
        {"rows_by_id": {KONKA["instrument_id"]: {**KONKA, **row}}},
        [],
        {"official_current_list_complete": True},
        snapshot=snapshot,
    )
    assert repeated["deactivated_count"] == 0
    assert (await local_record(owner))["delisted_date"] == row["delisted_date"]


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [16, "16", "000016"])
async def test_xlsx_code_normalization_uses_raw_cells(tmp_path, value):
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["证券代码", "证券简称", "上市日期", "终止上市日期"])
    sheet.append([value, "*ST康佳A", "1992-03-27", "2026-10-23"])
    path = tmp_path / "synthetic.xlsx"
    workbook.save(path)
    # Deliberately retain inferred 16 instead of pre-padding fixture rows.
    assert pd.read_excel(path).iloc[0, 0] == 16
    source = AShareOfficialStockMasterSource(
        "exchange_official_a_stock",
        config={
            "szse": {"terminal_list_file": str(path)},
        },
    )
    snapshot = await source.get_delisting_snapshot("SZSE")
    assert snapshot["rows"][0]["instrument_id"] == "000016.SZ"


@pytest.mark.asyncio
async def test_real_sse_terminal_dates_and_a_share_scope():
    source = AShareOfficialStockMasterSource(
        "exchange_official_a_stock",
        config={
            "sse": {"terminal_list_file": str(FIXTURES / "sse_terminated.json")},
        },
    )
    snapshot = await source.get_delisting_snapshot("SSE")
    assert snapshot["complete"], snapshot["errors"]
    row = next(
        item for item in snapshot["rows"] if item["instrument_id"] == "600001.SH"
    )
    assert row["delisted_date"] == "2009-12-29"
    rows = [item for item in snapshot["rows"] if item["instrument_id"] == "600680.SH"]
    assert len(rows) == 1
    assert rows[0]["listed_date"] == "1993-10-18"
    assert rows[0]["delisted_date"] == "2019-05-24"


@pytest.mark.asyncio
async def test_sse_terminal_share_type_is_not_inferred_from_company_code(tmp_path):
    raw = json.loads((FIXTURES / "sse_terminated.json").read_text())
    b_row = next(
        row for row in raw["result"]
        if row["COMPANY_CODE"] == "600680" and row["STOCK_TYPE"] == "2"
    )
    a_row = {**b_row, "STOCK_TYPE": "1", "A_STOCK_CODE": "600625"}
    bad_row = {**b_row, "STOCK_TYPE": ""}
    path = tmp_path / "share-classes.json"
    path.write_text(json.dumps({"result": [b_row, a_row, bad_row]}))
    source = AShareOfficialStockMasterSource(
        "exchange_official_a_stock", config={"sse": {"terminal_list_file": str(path)}},
    )
    snapshot = await source.get_delisting_snapshot("SSE")
    assert [row["instrument_id"] for row in snapshot["rows"]] == ["600625.SH"]
    assert not snapshot["complete"]
    assert snapshot["errors"] == ["invalid SSE terminal share class"]


def evidence(**overrides):
    return {
        "instrument_id": "000016.SZ",
        "symbol": "000016",
        "exchange": "SZSE",
        "type": "stock",
        "listed_date": "1992-03-27",
        "delisted_date": "2026-10-23",
        "completion_confirmed": True,
        "source": "szse_official_terminal_list",
        "source_url": "https://www.szse.cn/api/report/ShowReport",
        "raw_snapshot_hash": "fixture",
        "parser_version": "test.v1",
        "observed_at": get_shanghai_time().isoformat(),
        **overrides,
    }


async def reconcile(owner, rows, *, local=None, audit=False, complete=True, errors=()):
    return await AShareDelistingReconciler(owner, None).reconcile(
        "SZSE",
        {"rows_by_id": {"000016.SZ": local or KONKA}},
        [],
        {"official_current_list_complete": True},
        audit_only=audit,
        snapshot={"rows": rows, "complete": complete, "errors": list(errors)},
    )


@pytest.mark.asyncio
async def test_future_plan_survives_restart_and_closes_atomically(owner, monkeypatch):
    now = get_shanghai_time()
    future = now.date() + timedelta(days=1)
    monkeypatch.setattr("instrument_master_delisting.get_shanghai_time", lambda: now)
    monkeypatch.setattr("database.instrument_lifecycle.get_shanghai_time", lambda: now)
    assert await owner.save_instruments_batch([KONKA])
    row = evidence(delisted_date=future.isoformat())
    result = await reconcile(owner, [row])
    assert result["future_effective_count"] == 1
    assert (await local_record(owner))["is_active"]
    assert await reusable_terminal_verifications(owner, ["SZSE"], 48) is not None
    initial_plan = (await owner.load_a_share_delisting_state(["SZSE"]))["plans"][0]

    next_day = now + timedelta(days=1)
    monkeypatch.setattr(
        "instrument_master_delisting.get_shanghai_time", lambda: next_day
    )
    monkeypatch.setattr(
        "database.instrument_lifecycle.get_shanghai_time", lambda: next_day
    )
    assert await reusable_terminal_verifications(owner, ["SZSE"], 48) is None
    # Recreate the reconciler, fetch no new matching row, recover the persisted plan.
    applied = await reconcile(owner, [])
    assert applied["deactivated_count"] == 1
    assert not (await owner.load_a_share_delisting_state(["SZSE"]))["plans"]
    await owner.save_instrument_master_discrepancies(
        [{**initial_plan, "reason": FUTURE}],
        exchange="SZSE",
        run_id="duplicate-observation",
    )
    assert not (await owner.load_a_share_delisting_state(["SZSE"]))["plans"]
    assert (await local_record(owner))["delisted_date"].date() == future


@pytest.mark.asyncio
async def test_atomic_plan_failure_rolls_back_and_can_retry(owner):
    assert await owner.save_instruments_batch([KONKA])

    def fail_closure(connection, cursor, statement, parameters, context, executemany):
        if (
            "INSERT INTO instrument_master_discrepancies" in statement
            and APPLIED in str(parameters)
        ):
            raise RuntimeError("injected closure failure")

    event.listen(owner._test_engine.sync_engine, "before_cursor_execute", fail_closure)
    assert not await owner.mark_instrument_delisted(
        "000016.SZ",
        delisted_date="2026-10-23",
        source="szse_official_terminal_list",
        evidence=evidence(),
        plan_keys=["plan-1"],
    )
    assert (await local_record(owner))["is_active"]
    event.remove(owner._test_engine.sync_engine, "before_cursor_execute", fail_closure)
    assert await owner.mark_instrument_delisted(
        "000016.SZ",
        delisted_date="2026-10-23",
        source="szse_official_terminal_list",
        evidence=evidence(),
        plan_keys=["plan-1"],
    )
    assert not (await local_record(owner))["is_active"]


@pytest.mark.asyncio
async def test_future_writer_rejects_and_missing_date_preserves_known_date(owner):
    assert await owner.save_instruments_batch([KONKA])
    assert not await owner.mark_instrument_delisted(
        "000016.SZ",
        delisted_date=date.today() + timedelta(days=10),
        source="szse_official",
    )
    assert (await local_record(owner))["is_active"]
    assert await owner.mark_instrument_delisted("000016.SZ", delisted_date="2026-10-23")
    assert await owner.mark_instrument_delisted("000016.SZ", delisted_date=None)
    assert (await local_record(owner))["delisted_date"] == datetime(2026, 10, 23)
    assert not await owner.mark_instrument_delisted(
        "000016.SZ", delisted_date="2026-10-22"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["szse_official", "baostock", "akshare"])
async def test_unknown_date_terminal_and_metadata_cannot_be_revived(owner, source):
    assert await owner.save_instruments_batch([KONKA])
    proof = evidence(delisted_date=None)
    assert await owner.mark_instrument_delisted("000016.SZ", evidence=proof)
    assert await owner.save_instruments_batch([{**KONKA, "source": source}])
    assert (
        await owner.save_instrument_master_metadata_batch(
            [
                {
                    "instrument_id": "000016.SZ",
                    "exchange": "SZSE",
                    "metadata": {"ordinary": True},
                }
            ]
        )
        == 1
    )
    row = await local_record(owner)
    assert row == {
        "status": "delisted",
        "is_active": False,
        "trading_status": 0,
        "delisted_date": None,
    }
    async with owner.get_async_session() as session:
        stored = (
            await session.execute(
                text(
                    "SELECT metadata_json FROM instrument_master_metadata WHERE instrument_id='000016.SZ'"
                )
            )
        ).scalar_one()
    assert (
        __import__("json").loads(stored)["metadata"]["a_share_terminal_evidence"]
        == proof
    )


@pytest.mark.asyncio
async def test_fallback_cache_cannot_establish_delisting_or_invent_history(owner):
    assert await owner.save_instruments_batch([KONKA])
    fallback = {
        **KONKA,
        "source": "baostock",
        "status": "delisted",
        "is_active": False,
        "delisted_date": "2026-10-23",
    }
    assert await owner.save_instruments_batch([fallback])
    assert (await local_record(owner))["is_active"]
    assert not await owner.save_instruments_batch(
        [{**fallback, "instrument_id": "000099.SZ", "symbol": "000099"}]
    )
    assert len(await owner.get_instruments_by_exchange("SZSE", is_active=None)) == 1


@pytest.mark.asyncio
async def test_audit_records_never_form_a_production_baseline(owner):
    assert await owner.save_instruments_batch([KONKA])
    result = await reconcile(owner, [evidence()], audit=True)
    assert result["would_deactivate_count"] == 1
    assert result["deactivated_count"] == 0
    assert (await local_record(owner))["is_active"]
    state = await owner.load_a_share_delisting_state(["SZSE"])
    assert not state["has_baseline"] and not state["plans"]
    assert await reusable_terminal_verifications(owner, ["SZSE"], 48) is None


@pytest.mark.asyncio
async def test_bad_plan_json_and_missing_baseline_never_mean_fresh(owner):
    assert await reusable_terminal_verifications(owner, ["SZSE"], 48) is None
    await owner.save_instrument_master_discrepancies(
        [
            {
                "reason": FUTURE,
                "scope": "a_share_stock",
                "mode": "production",
                "instrument_id": "000016.SZ",
                "plan_key": "bad",
                "effective_date": "2026-10-23",
            }
        ],
        exchange="SZSE",
        run_id="bad-payload",
    )
    async with owner.get_async_session() as session:
        await session.execute(
            text("UPDATE instrument_master_discrepancies SET payload_json='{bad'")
        )
        await session.commit()
    with pytest.raises(ValueError):
        await owner.load_a_share_delisting_state(["SZSE"])
    assert await reusable_terminal_verifications(owner, ["SZSE"], 48) is None


@pytest.mark.asyncio
async def test_conflicting_dates_do_not_guess_and_partial_positive_evidence_is_usable(
    owner,
):
    assert await owner.save_instruments_batch([KONKA])
    result = await reconcile(owner, [evidence(), evidence(delisted_date="2026-10-22")])
    assert result["conflict_count"] == 1
    assert (await local_record(owner))["is_active"]
    result = await reconcile(
        owner, [evidence()], complete=False, errors=["page 2 missing"]
    )
    assert result["deactivated_count"] == 1
    assert result["status"] == "warning"


@pytest.mark.asyncio
async def test_reviewed_predecessor_terminal_does_not_deactivate_current_issuer(owner):
    row = {
        **KONKA,
        "instrument_id": "600018.SH",
        "symbol": "600018",
        "exchange": "SSE",
        "name": "上港集团",
        "listed_date": "2000-07-19",
    }
    assert await owner.save_instruments_batch([row])
    terminal = evidence(
        instrument_id="600018.SH",
        symbol="600018",
        exchange="SSE",
        listed_date="2000-07-19",
        delisted_date="2006-09-25",
    )
    result = await AShareDelistingReconciler(owner, None).reconcile(
        "SSE",
        {"rows_by_id": {"600018.SH": row}},
        [row],
        {"official_current_list_complete": True},
        snapshot={"rows": [terminal], "complete": True, "errors": []},
    )
    assert result["deactivated_count"] == 0
    assert len(await owner.get_instruments_by_exchange("SSE")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("audit", [False, True])
async def test_real_policy_and_factory_obey_audit_and_terminal_contract(owner, audit):
    from data_manager import DataManager
    from data_sources.source_factory import DataSourceFactory
    from instrument_master_governance import (
        AShareStockPolicy,
        MasterGovernanceRequirement,
    )

    assert await owner.save_instruments_batch([KONKA])
    official = AShareOfficialStockMasterSource(
        "exchange_official_a_stock",
        config={
            "szse": {"terminal_list_file": str(FIXTURES / "szse_terminated.xlsx")},
        },
    )
    official.current_list_coverage["SZSE"] = {"complete": True}
    official.get_instrument_list = AsyncMock(
        return_value=[
            {
                **KONKA,
                "instrument_id": f"000{i:03}.SZ",
                "symbol": f"000{i:03}",
                "name": f"正常{i}",
                "source_authority": "official",
            }
            for i in range(100, 160)
        ]
    )
    factory = DataSourceFactory(owner)
    factory.routing = {"instrument_list": {"a_stock": ["exchange_official"]}}
    factory.source_instances_by_region = {"a_stock": {"exchange_official": official}}
    manager = DataManager.__new__(DataManager)
    manager.db_ops, manager.source_factory = owner, factory
    manager._get_instrument_master_sync_config = lambda: {
        "pytdx_validation_enabled": False
    }
    manager._load_a_share_terminal_announcements = AsyncMock(
        return_value={"rows": [], "complete": True, "errors": []}
    )
    result = await AShareStockPolicy(manager, {}).execute(
        MasterGovernanceRequirement(
            scope="a_share_stock",
            exchanges=["SZSE"],
            instrument_types=["stock"],
            mode="audit_only" if audit else "force_refresh",
            include_pytdx_validation=False,
        )
    )
    assert result["status"] == "success", result
    assert (await local_record(owner))["is_active"] is audit
    rows = await owner.get_instruments_by_exchange("SZSE", is_active=None)
    assert len(rows) == (1 if audit else 61)
    terminal = result["exchanges"]["SZSE"]["delisting_reconciliation"]
    assert terminal["deactivated_count"] == (0 if audit else 1)
    assert terminal["would_deactivate_count"] == (1 if audit else 0)


@pytest.mark.asyncio
async def test_sse_terminal_pagination_is_not_truncated_at_500():
    import json

    source = AShareOfficialStockMasterSource("exchange_official_a_stock")
    first = [
        {
            "COMPANY_CODE": f"600{i:03}",
            "COMPANY_ABBR": "终止样本",
            "LIST_DATE": "20000101",
            "DELIST_DATE": "20200101",
        }
        for i in range(500)
    ]
    final = [
        {
            "COMPANY_CODE": "688001",
            "COMPANY_ABBR": "最后一页",
            "LIST_DATE": "20190101",
            "DELIST_DATE": "20260101",
        }
    ]
    source._http_get = AsyncMock(
        side_effect=[
            json.dumps({"result": first, "pageHelp": {"total": 501}}).encode(),
            json.dumps({"result": final, "pageHelp": {"total": 501}}).encode(),
        ]
    )
    result = await source.get_delisting_snapshot("SSE")
    assert result["complete"]
    assert len(result["rows"]) == 501
    assert result["rows"][-1]["instrument_id"] == "688001.SH"
    assert (
        source._http_get.await_args_list[-1].kwargs["params"]["pageHelp.pageNo"] == "2"
    )


@pytest.mark.asyncio
async def test_sse_missing_terminal_page_retains_positive_rows_and_reports_incomplete():
    import json

    source = AShareOfficialStockMasterSource("exchange_official_a_stock")
    source._http_get = AsyncMock(
        side_effect=[
            json.dumps(
                {
                    "result": [{"COMPANY_CODE": "600001", "DELIST_DATE": "20200101"}],
                    "pageHelp": {"total": 501},
                }
            ).encode(),
            None,
        ]
    )
    snapshot = await source.get_delisting_snapshot("SSE")
    assert not snapshot["complete"] and snapshot["errors"]
    assert snapshot["rows"][0]["instrument_id"] == "600001.SH"


@pytest.mark.asyncio
async def test_sse_missing_current_board_does_not_claim_full_coverage():
    source = AShareOfficialStockMasterSource("exchange_official_a_stock")
    source._load_sse_payload = AsyncMock(
        side_effect=[
            (b"code,name,listed_date\n600000,bank,2000-01-01\n", "main.csv"),
            (None, "star.csv"),
        ]
    )
    rows = await source.get_instrument_list("SSE", ["stock"])
    assert rows
    assert not source.current_list_coverage["SSE"]["complete"]


@pytest.mark.asyncio
async def test_current_financial_target_cannot_override_effective_delisting():
    financial = FinancialDisclosureIncrementalSyncService.__new__(
        FinancialDisclosureIncrementalSyncService
    )
    financial.db_ops = Mock()
    financial.db_ops.get_instruments_by_exchange = AsyncMock(
        return_value=[
            {**KONKA, "status": "delisted"},
            {
                **KONKA,
                "instrument_id": "000001.SZ",
                "symbol": "000001",
                "delisted_date": "2026-10-23",
            },
        ]
    )
    assert (
        await financial._load_active_instruments(["SZSE"], target_symbols=["000016"])
        == []
    )
    assert await financial._load_active_instruments(["SZSE"]) == []


@pytest.mark.asyncio
async def test_new_failed_verification_is_not_hidden_by_older_success(owner):
    now = get_shanghai_time()
    await owner.save_instrument_master_discrepancies(
        [
            {
                "reason": VERIFICATION,
                "scope": "a_share_stock",
                "mode": "production",
                "verified_at": (now - timedelta(hours=1)).isoformat(),
                "status": "success",
                "summary": {},
            },
            {
                "reason": VERIFICATION,
                "scope": "a_share_stock",
                "mode": "production",
                "verified_at": now.isoformat(),
                "status": "error",
                "summary": {},
            },
        ],
        exchange="SZSE",
        run_id="verification-failure",
    )
    assert await reusable_terminal_verifications(owner, ["SZSE"], 48) is None


@pytest.mark.asyncio
async def test_empty_current_target_skips_announcement_and_repair_providers(owner):
    from contextlib import nullcontext
    from utils.config_manager import config_manager

    assert await owner.save_instruments_batch([KONKA])
    assert await owner.mark_instrument_delisted("000016.SZ", delisted_date="2026-10-23")
    storage = Mock()
    storage.financial_database_scope.side_effect = nullcontext
    storage.list_announcement_audit.return_value = []
    storage.list_financial_disclosure_event_states.return_value = []
    from research.financial_statement_maintenance_repair import (
        FinancialMaintenanceRepairRouter,
    )

    router = FinancialMaintenanceRepairRouter(
        storage=storage,
        research_config=config_manager.get_research_config(),
    )
    router.repair_targets = AsyncMock(
        side_effect=AssertionError("must not repair an empty current scope")
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=owner,
        storage=storage,
        research_config=config_manager.get_research_config(),
        announcement_service=Mock(),
        repair_router=router,
    )
    service._scan_announcements = Mock(
        side_effect=AssertionError("must not scan an empty current scope")
    )
    result = await service.sync(
        exchanges=["SZSE"],
        target_instrument_ids=["000016.SZ"],
        report_periods=["2026-09-30"],
        dry_run=True,
    )
    assert result["candidate_count"] == 0
    service._scan_announcements.assert_not_called()
    router.repair_targets.assert_not_called()


@pytest.mark.parametrize(
    "title,body,expected",
    [
        (
            "关于股票终止上市暨摘牌的公告",
            "证券代码：000016、200016。深圳证券交易所同意公司股票主动终止上市。终止上市决定日期：2026年10月16日。摘牌日期：2026年10月23日。",
            "2026-10-23",
        ),
        ("关于股票终止上市暨摘牌的公告", "证券代码：000016。公司股票已摘牌。", None),
        (
            "关于股票终止上市决定的公告",
            "证券代码：000016。深圳证券交易所决定股票终止上市，股票将摘牌。",
            "unconfirmed",
        ),
        (
            "关于股票可能被终止上市的风险提示公告",
            "证券代码：000016。股票将被终止上市。",
            "unconfirmed",
        ),
        (
            "关于股票终止上市暨摘牌的公告",
            "证券代码：000017。深圳证券交易所决定股票终止上市，摘牌日期：2026年10月23日。",
            "unconfirmed",
        ),
        (
            "关于收到公司股票终止上市决定的公告",
            "证券代码：000016。公司股票于2026年10月16日收到深圳证券交易所《关于股票终止上市的决定》，具体摘牌安排将另行公告。",
            "unconfirmed",
        ),
        (
            "关于收到公司股票终止上市决定的公告",
            "证券代码：000016。公司股票于2026年10月16日收到深圳证券交易所《关于股票终止上市的决定》。股票将于2026年10月23日在深圳证券交易所终止上市。",
            "2026-10-23",
        ),
        (
            "关于公司B股股票终止上市暨摘牌的公告",
            "证券代码：000016、200016。深圳证券交易所决定终止公司B股股票上市。B股摘牌日期：2026年10月23日；公司A股继续正常上市。",
            "unconfirmed",
        ),
        (
            "关于股票终止上市暨摘牌的公告",
            "证券代码：000016、200016。深圳证券交易所决定终止公司B股股票上市。B股股票已摘牌。公司A股继续正常上市。",
            "unconfirmed",
        ),
        (
            "关于公司债券终止上市暨摘牌的公告",
            "证券代码：000016。深圳证券交易所决定终止公司债券上市。摘牌日期：2026年10月23日。",
            "unconfirmed",
        ),
        (
            "关于公司A股股票终止上市暨摘牌的公告",
            "证券代码：000016、200016。深圳证券交易所决定终止公司A股股票上市。A股摘牌日期：2026年10月23日。B股摘牌日期：2026年10月30日。",
            "2026-10-23",
        ),
        (
            "关于股票终止上市决定的公告",
            "证券代码：000016。深圳证券交易所决定终止公司股票上市。预计摘牌日期为2026年10月23日，实际摘牌日期将另行公告。",
            "unconfirmed",
        ),
        (
            "关于股票终止上市决定的公告",
            "证券代码：000016。深圳证券交易所决定终止公司股票上市。预计股票将于2026年10月23日摘牌。",
            "unconfirmed",
        ),
        (
            "关于股票终止上市决定的公告",
            "证券代码：000016。深圳证券交易所决定终止公司股票上市。暂定摘牌日期为2026年10月23日。",
            "unconfirmed",
        ),
        (
            "关于股票终止上市决定的公告",
            "证券代码：000016。深圳证券交易所决定终止公司股票上市。股票将于2026年10月23日摘牌，具体日期以交易所后续公告为准。",
            "unconfirmed",
        ),
        (
            "关于股票终止上市决定的公告",
            "证券代码：000016。深圳证券交易所决定终止公司股票上市。股票将于2026年10月23日摘牌。实际摘牌日期将另行公告。",
            "unconfirmed",
        ),
        (
            "关于股票终止上市暨摘牌的公告",
            "证券代码：000016。公司股票已摘牌。预计摘牌日期为2026年10月23日，实际摘牌日期尚未确认。",
            None,
        ),
        (
            "关于股票终止上市暨摘牌的公告",
            "证券代码：000016。公司股票已摘牌，实际摘牌日期尚未确认。",
            None,
        ),
        (
            "关于股票终止上市决定的公告",
            "证券代码：000016。深圳证券交易所决定终止公司股票上市。预计公司股票已摘牌。",
            "unconfirmed",
        ),
        (
            "关于股票终止上市暨摘牌的公告",
            "证券代码：000016。深圳证券交易所决定终止公司股票上市。预计摘牌日期为2026年10月23日。现确定实际摘牌日期为2026年10月24日。",
            "2026-10-24",
        ),
        (
            "关于股票终止上市暨摘牌的公告",
            "证券代码：000016。深圳证券交易所决定终止公司股票上市。股票将于2026年10月23日摘牌。预计该事项不会影响公司经营。",
            "2026-10-23",
        ),
        (
            "关于股票终止上市决定的公告",
            "证券代码：000016。深圳证券交易所决定终止公司股票上市。预估摘牌日期为2026年10月23日。",
            "unconfirmed",
        ),
        (
            "关于股票终止上市决定的公告",
            "证券代码：000016。深圳证券交易所决定终止公司股票上市。预估公司股票将于2026年10月23日摘牌。",
            "unconfirmed",
        ),
        (
            "关于股票终止上市决定的公告",
            "证券代码：000016。深圳证券交易所决定终止公司股票上市。摘牌日期为2026年10月23日前后。",
            "unconfirmed",
        ),
        (
            "关于股票终止上市暨摘牌的公告",
            "证券代码：000016。深圳证券交易所决定终止公司股票上市。本公司股票终止上市暨摘牌日期为2026年10月23日（星期五）。",
            "2026-10-23",
        ),
    ],
)
def test_announcement_requires_stock_identity_and_actual_completion(
    title, body, expected
):
    from research.announcements import AnnouncementRecord, build_announcement_key
    from instrument_master_delisting import announcement_terminal_evidence

    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="test-1",
        announcement_key=build_announcement_key("cninfo", "test-1"),
        title=title,
        published_at="2026-10-17T00:00:00+08:00",
        exchange="SZSE",
        symbols=("000016",),
    )
    proof = announcement_terminal_evidence(record, body, "000016.SZ")
    if expected == "unconfirmed":
        assert proof is None
    else:
        assert proof["delisted_date"] == expected
        assert proof["completion_confirmed"]
        assert proof["hash_kind"] == "canonical_announcement_record"


def test_bounded_bse_scan_reuses_existing_purpose_and_body_evidence():
    from types import SimpleNamespace
    from research.announcements import AnnouncementRecord, build_announcement_key
    from instrument_master_delisting import scan_terminal_announcements

    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="bse-final",
        announcement_key=build_announcement_key("cninfo", "bse-final"),
        title="关于股票终止上市暨摘牌的公告",
        published_at="2026-01-01T00:00:00+08:00",
        exchange="BSE",
        symbols=("920680",),
        raw_payload={
            "body_text": "证券代码：920680。北京证券交易所决定公司股票终止上市。摘牌日期：2026年1月5日。"
        },
    )
    service = Mock()
    service.config.provider_configs = {}
    service.acquire.return_value = SimpleNamespace(
        scan_result=SimpleNamespace(
            records=[record],
            status="success",
            is_complete=True,
            errors=(),
            stop_reason="completed",
        )
    )
    result = scan_terminal_announcements(service, "BSE", ["920680.BJ"])
    assert result["rows"][0]["delisted_date"] == "2026-01-05"
    assert len(result["rows"]) == 1
    assert service.acquire.call_count == 2
    query = service.acquire.call_args.args[0]
    assert query.purpose_key == "instrument_master_bse_delisting"
    assert query.scope.max_pages == 2


@pytest.mark.asyncio
async def test_disabled_bse_check_neither_scans_nor_applies_plans(owner):
    loader = AsyncMock(side_effect=AssertionError("disabled BSE check must not scan"))
    factory = Mock()
    factory.get_official_delisting_snapshot = AsyncMock(
        side_effect=AssertionError("must not fetch")
    )
    result = await AShareDelistingReconciler(owner, factory, loader).reconcile(
        "BSE",
        {"rows_by_id": {}},
        [],
        {},
        enabled=False,
    )
    assert result["reason"] == "delisting_check_disabled"
    assert result["status"] == "warning"
    loader.assert_not_awaited()
    factory.get_official_delisting_snapshot.assert_not_awaited()
    state = await owner.load_a_share_delisting_state(["BSE"])
    assert (
        state["verifications"]["BSE"]["summary"]["reason"] == "delisting_check_disabled"
    )


@pytest.mark.asyncio
async def test_announcement_failure_preserves_state_and_is_visible(owner):
    assert await owner.save_instruments_batch([KONKA])
    loader = AsyncMock(side_effect=RuntimeError("official announcement unavailable"))
    result = await AShareDelistingReconciler(owner, None, loader).reconcile(
        "SZSE",
        {"rows_by_id": {"000016.SZ": KONKA}},
        [],
        {"official_current_list_complete": True},
        snapshot={"rows": [], "complete": True, "errors": []},
    )
    assert result["status"] == "warning"
    assert result["source_error_count"] == 1 and result["unconfirmed_count"] == 1
    assert (await local_record(owner))["is_active"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refresh,audit",
    [
        ("matching", False),
        ("matching", True),
        ("missing", False),
        ("conflicting", False),
    ],
)
async def test_expired_plan_requires_reverification_and_closes_with_fresh_proof(
    owner,
    monkeypatch,
    refresh,
    audit,
):
    now = get_shanghai_time()
    assert await owner.save_instruments_batch([KONKA])
    original = evidence(delisted_date=(now.date() + timedelta(days=1)).isoformat())
    await reconcile(owner, [original])
    initial = (await owner.load_a_share_delisting_state(["SZSE"]))["plans"][0]
    later = now + timedelta(days=3)
    monkeypatch.setattr("instrument_master_delisting.get_shanghai_time", lambda: later)
    monkeypatch.setattr(
        "database.instrument_lifecycle.get_shanghai_time", lambda: later
    )
    fresh = {
        **original,
        "raw_snapshot_hash": "fresh-official-proof",
        "observed_at": later.isoformat(),
    }
    if refresh == "conflicting":
        fresh["delisted_date"] = (now.date() + timedelta(days=2)).isoformat()
    result = await reconcile(
        owner, [] if refresh == "missing" else [fresh], audit=audit
    )
    applied = refresh == "matching" and not audit
    assert result["deactivated_count"] == int(applied)
    assert (await local_record(owner))["is_active"] is not applied
    state = await owner.load_a_share_delisting_state(["SZSE"])
    assert len(state["plans"]) == int(not applied)
    if applied:
        assert result["status"] == "success"
        assert await reusable_terminal_verifications(owner, ["SZSE"], 48) is not None
        async with owner.get_async_session() as session:
            payload = (
                await session.execute(
                    text(
                        "SELECT payload_json FROM instrument_master_discrepancies WHERE reason=:reason"
                    ),
                    {"reason": APPLIED},
                )
            ).scalar_one()
        import json

        assert json.loads(payload)["plan_key"] == initial["plan_key"]
    elif audit:
        assert result["would_deactivate_count"] == 1
    else:
        assert result["status"] == "warning"
        assert result["conflict_count"] == int(refresh == "conflicting")


@pytest.mark.asyncio
@pytest.mark.parametrize("audit", [False, True])
async def test_stale_current_row_conflict_is_visible_and_survives_reuse(owner, audit):
    assert await owner.save_instruments_batch([KONKA])
    result = await AShareDelistingReconciler(owner, None).reconcile(
        "SZSE",
        {"rows_by_id": {"000016.SZ": KONKA}},
        [KONKA],
        {"official_current_list_complete": True},
        audit_only=audit,
        snapshot={"rows": [evidence()], "complete": True, "errors": []},
    )
    assert result["status"] == "warning" and result["conflict_count"] == 1
    assert result["deactivated_count"] == int(not audit)
    assert (await local_record(owner))["is_active"] is audit
    sample = result["conflict_samples"][0]
    assert sample["current_evidence"]["is_active"] is True
    assert sample["terminal_evidence"][0]["delisted_date"] == "2026-10-23"
    assert sample["mode"] == ("audit_only" if audit else "production")
    reused = await reusable_terminal_verifications(owner, ["SZSE"], 48)
    if audit:
        assert reused is None
    else:
        assert reused["SZSE"]["status"] == "warning"
        assert reused["SZSE"]["summary"]["conflict_samples"] == [sample]


@pytest.mark.asyncio
async def test_conflicts_preserve_all_evidence_beyond_the_report_sample_limit(owner):
    import json

    rows = {}
    observations = []
    for number in range(100, 125):
        symbol = f"000{number}"
        iid = f"{symbol}.SZ"
        rows[iid] = {**KONKA, "instrument_id": iid, "symbol": symbol}
        for effective, announcement in (
            ("2026-10-22", "left"),
            ("2026-10-23", "right"),
        ):
            observations.append(
                evidence(
                    instrument_id=iid,
                    symbol=symbol,
                    delisted_date=effective,
                    announcement_id=f"{iid}:{announcement}",
                    raw_snapshot_hash=f"hash:{iid}:{announcement}",
                )
            )
    assert await owner.save_instruments_batch(list(rows.values()))
    result = await AShareDelistingReconciler(owner, None).reconcile(
        "SZSE",
        {"rows_by_id": rows},
        [],
        {"official_current_list_complete": True},
        snapshot={"rows": observations, "complete": True, "errors": []},
    )
    assert result["conflict_count"] == 25
    assert len(result["conflict_samples"]) == 20
    async with owner.get_async_session() as session:
        payloads = (
            (
                await session.execute(
                    text(
                        "SELECT payload_json FROM instrument_master_discrepancies "
                        "WHERE reason='a_share_terminal_conflict'"
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(payloads) == 25
    for payload in payloads:
        conflict = json.loads(payload)
        proof = conflict["terminal_evidence"]
        assert {row["delisted_date"] for row in proof} == {"2026-10-22", "2026-10-23"}
        assert {row["raw_snapshot_hash"] for row in proof} == {
            f"hash:{conflict['instrument_id']}:left",
            f"hash:{conflict['instrument_id']}:right",
        }
        assert conflict["existing_lifecycle"]["is_active"] is True
    assert len(await owner.get_instruments_by_exchange("SZSE")) == 25


@pytest.mark.asyncio
async def test_conflict_persistence_failure_cannot_form_healthy_verification(
    owner, monkeypatch
):
    assert await owner.save_instruments_batch([KONKA])
    original = owner.save_instrument_master_discrepancies

    async def save(rows, **kwargs):
        if rows[0]["reason"] == "a_share_terminal_conflict":
            return 0
        return await original(rows, **kwargs)

    monkeypatch.setattr(owner, "save_instrument_master_discrepancies", save)
    result = await reconcile(owner, [evidence(), evidence(delisted_date="2026-10-22")])
    assert result["status"] == "error" and result["write_failed_count"] == 1
    assert await reusable_terminal_verifications(owner, ["SZSE"], 48) is None
    assert (await local_record(owner))["is_active"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old_parser", ["official_stock_terminal_body.v1", "official_stock_terminal_body.v2"]
)
async def test_old_announcement_parser_plan_requires_fresh_reverification(
    owner,
    monkeypatch,
    old_parser,
):
    now = get_shanghai_time()
    assert await owner.save_instruments_batch([KONKA])
    old = evidence(
        delisted_date=(now.date() + timedelta(days=1)).isoformat(),
        parser_version=old_parser,
        source="cninfo_szse_delisting",
        announcement_id="legacy-plan",
    )
    await reconcile(owner, [old])
    later = now + timedelta(days=1)
    monkeypatch.setattr("instrument_master_delisting.get_shanghai_time", lambda: later)
    monkeypatch.setattr(
        "database.instrument_lifecycle.get_shanghai_time", lambda: later
    )
    result = await reconcile(owner, [])
    assert result["status"] == "warning" and result["deactivated_count"] == 0
    assert (await local_record(owner))["is_active"]
    fresh = {
        **old,
        "parser_version": ANNOUNCEMENT_PARSER_VERSION,
        "observed_at": later.isoformat(),
    }
    result = await reconcile(owner, [fresh])
    assert result["deactivated_count"] == 1
    assert not (await owner.load_a_share_delisting_state(["SZSE"]))["plans"]


@pytest.mark.asyncio
@pytest.mark.parametrize("scan_status", ["partial", "failed", "exception"])
async def test_announcement_partial_failure_retains_positive_evidence_and_degradation(
    owner,
    scan_status,
):
    from types import SimpleNamespace
    from research.announcements import AnnouncementRecord, build_announcement_key
    from instrument_master_delisting import scan_terminal_announcements

    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="partial-final",
        announcement_key=build_announcement_key("cninfo", "partial-final"),
        published_at="2026-10-23T00:00:00+08:00",
        title="关于股票终止上市暨摘牌的公告",
        exchange="SZSE",
        symbols=("000016",),
        raw_payload={
            "body_text": "证券代码：000016。深圳证券交易所决定终止公司股票上市。摘牌日期：2026年10月23日。"
        },
    )
    service = Mock()
    service.config.provider_configs = {}
    successful = SimpleNamespace(
        scan_result=SimpleNamespace(
            records=[record],
            status="success",
            is_complete=True,
            errors=(),
            stop_reason="completed",
        )
    )
    unsuccessful = SimpleNamespace(
        scan_result=SimpleNamespace(
            records=[record] if scan_status == "partial" else [],
            status=scan_status,
            is_complete=False,
            errors=("page 2 timed out",),
            stop_reason="page_limit",
        )
    )
    service.acquire.side_effect = [
        successful,
        (
            RuntimeError("page 2 timed out")
            if scan_status == "exception"
            else unsuccessful
        ),
    ]
    snapshot = scan_terminal_announcements(service, "SZSE", ["000016.SZ"])
    assert not snapshot["complete"] and len(snapshot["rows"]) == 1
    assert any("timed out" in error for error in snapshot["errors"])
    assert await owner.save_instruments_batch([KONKA])
    result = await reconcile(
        owner, snapshot["rows"], complete=False, errors=snapshot["errors"]
    )
    assert result["status"] == "warning" and result["source_error_count"] > 0
    assert result["deactivated_count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("max_pages", [1, 3, 30])
async def test_bse_manager_honors_configured_scan_bound_and_can_reach_third_page(
    max_pages,
):
    from types import SimpleNamespace
    from data_manager import DataManager
    from research.announcements import AnnouncementRecord, build_announcement_key

    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="third-page-final",
        announcement_key=build_announcement_key("cninfo", "third-page-final"),
        published_at="2026-01-05T00:00:00+08:00",
        title="关于股票终止上市暨摘牌的公告",
        exchange="BSE",
        symbols=("920680",),
        raw_payload={
            "body_text": "证券代码：920680。北京证券交易所决定终止公司股票上市。摘牌日期：2026年1月5日。"
        },
    )
    service = Mock()
    service.config.provider_configs = {}

    def acquire(query):
        reached = query.scope.max_pages >= 3
        return SimpleNamespace(
            scan_result=SimpleNamespace(
                records=[record] if reached else [],
                status="success" if reached else "partial",
                is_complete=reached,
                errors=(),
                stop_reason="completed" if reached else "page_limit",
            )
        )

    service.acquire.side_effect = acquire
    manager = DataManager.__new__(DataManager)
    manager._get_instrument_master_sync_config = lambda: {
        "bse_delisting_scan_days": 90,
        "bse_delisting_scan_max_pages": max_pages,
    }
    manager._build_official_announcement_acquisition_service = lambda **kwargs: service
    snapshot = await manager._load_a_share_terminal_announcements("BSE", ["920680.BJ"])
    assert {
        call.args[0].scope.max_pages for call in service.acquire.call_args_list
    } == {max_pages}
    assert len(snapshot["rows"]) == int(max_pages >= 3)
    assert snapshot["complete"] is (max_pages >= 3)
    scope = service.acquire.call_args.args[0].scope
    assert (
        date.fromisoformat(scope.end_date) - date.fromisoformat(scope.start_date)
    ).days == 90


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange,symbol", [("SZSE", "000016"), ("BSE", "920680")])
@pytest.mark.parametrize("offset", [-1, 1])
async def test_estimated_date_never_writes_terminal_or_durable_plan(
    owner, exchange, symbol, offset
):
    from types import SimpleNamespace
    from research.announcements import AnnouncementRecord, build_announcement_key
    from instrument_master_delisting import scan_terminal_announcements

    iid = f"{symbol}.{'SZ' if exchange == 'SZSE' else 'BJ'}"
    local = {
        **KONKA,
        "instrument_id": iid,
        "symbol": symbol,
        "exchange": exchange,
        "source": f"{exchange.lower()}_official",
    }
    assert await owner.save_instruments_batch([local])
    estimated = get_shanghai_time().date() + timedelta(days=offset)
    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="synthetic-estimate",
        announcement_key=build_announcement_key("cninfo", "synthetic-estimate"),
        title="关于公司股票终止上市决定的公告",
        exchange=exchange,
        published_at=get_shanghai_time().isoformat(),
        symbols=(symbol,),
        raw_payload={
            "body_text": f"证券代码：{symbol}。证券交易所决定终止公司股票上市。预计摘牌日期为{estimated:%Y年%m月%d日}，实际摘牌日期将另行公告。"
        },
    )
    service = Mock()
    service.config.provider_configs = {}
    service.acquire.return_value = SimpleNamespace(
        scan_result=SimpleNamespace(
            records=[record],
            status="success",
            is_complete=True,
            errors=(),
            stop_reason="completed",
        )
    )
    snapshot = scan_terminal_announcements(service, exchange, [iid])
    loader = AsyncMock(return_value=snapshot)
    result = await AShareDelistingReconciler(owner, None, loader).reconcile(
        exchange,
        {"rows_by_id": {iid: local}},
        [],
        {"official_current_list_complete": True},
        snapshot={"rows": [], "complete": True, "errors": []},
    )
    assert result["deactivated_count"] == result["future_effective_count"] == 0
    assert result["unconfirmed_count"] == 1
    assert (await owner.get_instruments_by_exchange(exchange))[0]["status"] == "active"
    assert not (await owner.load_a_share_delisting_state([exchange]))["plans"]


@pytest.mark.asyncio
@pytest.mark.parametrize("has_terminal", [False, True])
async def test_partial_current_parse_never_creates_absence_but_accepts_positive_evidence(
    owner,
    tmp_path,
    has_terminal,
):
    from data_manager import DataManager
    from data_sources.source_factory import DataSourceFactory
    from instrument_master_governance import (
        AShareStockPolicy,
        MasterGovernanceRequirement,
    )

    assert await owner.save_instruments_batch([KONKA])
    path = tmp_path / "partial.csv"
    path.write_text(
        "证券代码,证券简称,上市日期\n"
        + "\n".join(
            [f"{number:06d},正常样本{number},1992-03-27" for number in range(100, 160)]
            + ["000016,,1992-03-27"]
        )
        + "\n"
    )
    source = AShareOfficialStockMasterSource(
        "exchange_official_a_stock",
        config={"szse": {"current_list_file": str(path)}},
    )
    source.get_delisting_snapshot = AsyncMock(
        return_value={
            "rows": [evidence()] if has_terminal else [],
            "complete": True,
            "errors": [],
        }
    )
    factory = DataSourceFactory(owner)
    factory.routing = {"instrument_list": {"a_stock": ["exchange_official"]}}
    factory.source_instances_by_region = {"a_stock": {"exchange_official": source}}
    manager = DataManager.__new__(DataManager)
    manager.db_ops, manager.source_factory = owner, factory
    manager._get_instrument_master_sync_config = lambda: {
        "pytdx_validation_enabled": False
    }
    manager._load_a_share_terminal_announcements = AsyncMock(
        side_effect=AssertionError(
            "partial parse cannot establish current-list absence"
        )
    )
    result = await AShareStockPolicy(manager, {}).execute(
        MasterGovernanceRequirement(
            scope="a_share_stock",
            exchanges=["SZSE"],
            instrument_types=["stock"],
            mode="force_refresh",
            include_pytdx_validation=False,
        )
    )
    manager._load_a_share_terminal_announcements.assert_not_awaited()
    terminal = result["exchanges"]["SZSE"]["delisting_reconciliation"]
    assert result["status"] == "warning"
    assert terminal["deactivated_count"] == int(has_terminal)
    assert terminal["unconfirmed_count"] == 0
    assert (await local_record(owner))["is_active"] is not has_terminal
    async with owner.get_async_session() as session:
        count = (
            await session.execute(
                text(
                    "SELECT count(*) FROM instrument_master_discrepancies "
                    "WHERE reason='official_current_list_missing_no_terminal_evidence'"
                )
            )
        ).scalar_one()
    assert count == 0


DATE_CORRECTION = {
    "expected_delisted_date": "2026-10-22",
    "expected_listed_date": "1992-03-27",
    "reason": "Reviewed official completed-termination list supersedes legacy fallback date",
    "reference": "bounded-terminal-date-test",
}


async def legacy_delisted(owner):
    assert await owner.save_instruments_batch([KONKA])
    # Reconstruct an old persisted row, bypassing today's fallback-ingress guard
    # only in this isolated test fixture.
    async with owner.get_async_session() as session:
        record = (await session.execute(select(InstrumentDB))).scalar_one()
        record.status, record.is_active, record.trading_status = "delisted", False, 0
        record.delisted_date = datetime(2026, 10, 22)
        record.source = "baostock"
        await session.commit()


async def correction_rows(owner):
    async with owner.get_async_session() as session:
        return (await session.execute(
            text("SELECT payload_json FROM instrument_master_discrepancies WHERE reason=:reason"),
            {"reason": DATE_CORRECTED},
        )).scalars().all()


@pytest.mark.asyncio
async def test_controlled_date_correction_is_atomic_idempotent_and_preserves_evidence(owner):
    await legacy_delisted(owner)
    async with owner.get_async_session() as session:
        await session.execute(text(METADATA_DDL))
        await session.execute(text(
            "INSERT INTO instrument_master_metadata "
            "(instrument_id,exchange,metadata_json,updated_at) VALUES "
            "(:iid,'SZSE',:payload,'2026-10-01')"
        ), {"iid": KONKA["instrument_id"], "payload": json.dumps({
            "metadata": {"unrelated": {"keep": True}}, "name": KONKA["name"],
        })})
        await session.commit()
    proof = evidence()
    assert not await owner.mark_instrument_delisted(
        KONKA["instrument_id"], delisted_date=proof["delisted_date"],
        source=proof["source"], evidence=proof,
    )
    for _ in range(2):
        assert await owner.mark_instrument_delisted(
            KONKA["instrument_id"], delisted_date=proof["delisted_date"],
            source=proof["source"], evidence=proof, date_correction=DATE_CORRECTION,
        )
    assert (await local_record(owner))["delisted_date"] == datetime(2026, 10, 23)
    rows = await correction_rows(owner)
    assert len(rows) == 1
    saved = json.loads(rows[0])
    assert saved["before"]["delisted_date"] == "2026-10-22"
    assert saved["before"]["source"] == "baostock"
    assert saved["after"]["delisted_date"] == "2026-10-23"
    assert saved["mode"] == "production"
    async with owner.get_async_session() as session:
        metadata = json.loads((await session.execute(text(
            "SELECT metadata_json FROM instrument_master_metadata WHERE instrument_id=:iid"
        ), {"iid": KONKA["instrument_id"]})).scalar_one())
        assert metadata["metadata"]["a_share_terminal_evidence"] == proof
        assert metadata["metadata"]["unrelated"] == {"keep": True}
        assert metadata["name"] == KONKA["name"]
        assert (await session.execute(select(InstrumentDB.listed_date))).scalar_one() == datetime(1992, 3, 27)
    assert (await owner.load_a_share_delisting_state(["SZSE"]))["has_baseline"] is False
    # Reusing the reference for a different request must fail.
    assert not await owner.mark_instrument_delisted(
        KONKA["instrument_id"], delisted_date=proof["delisted_date"],
        source=proof["source"], evidence=proof,
        date_correction={**DATE_CORRECTION, "expected_delisted_date": "2026-10-21"},
    )
    assert len(await correction_rows(owner)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", [
    "old_date", "listed_date", "missing_reason", "other_id", "other_exchange",
    "other_type", "other_source", "other_listing", "missing_hash", "future", "active",
])
async def test_controlled_date_correction_rejects_invalid_expectations_and_evidence(owner, problem):
    await legacy_delisted(owner)
    proof, request = evidence(), dict(DATE_CORRECTION)
    if problem == "old_date":
        request["expected_delisted_date"] = "2026-10-21"
    elif problem == "listed_date":
        request["expected_listed_date"] = "1994-01-01"
    elif problem == "missing_reason":
        request["reason"] = ""
    elif problem == "other_id":
        proof["instrument_id"] = "200016.SZ"
    elif problem == "other_exchange":
        proof["exchange"] = "SSE"
    elif problem == "other_type":
        proof["type"] = "bond"
    elif problem == "other_source":
        proof["source"] = "baostock"
    elif problem == "other_listing":
        proof["listed_date"] = "1994-01-01"
    elif problem == "missing_hash":
        proof["raw_snapshot_hash"] = ""
    elif problem == "future":
        proof["delisted_date"] = (get_shanghai_time().date() + timedelta(days=1)).isoformat()
    elif problem == "active":
        async with owner.get_async_session() as session:
            await session.execute(text("UPDATE instruments SET status='active',is_active=1,trading_status=1"))
            await session.commit()
    before = await local_record(owner)
    assert not await owner.mark_instrument_delisted(
        KONKA["instrument_id"], delisted_date=proof["delisted_date"],
        source=proof["source"], evidence=proof, date_correction=request,
    )
    assert await local_record(owner) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_table", ["instrument_master_metadata", "instrument_master_discrepancies"])
async def test_controlled_correction_rolls_back_if_evidence_or_audit_write_fails(owner, failing_table):
    await legacy_delisted(owner)
    proof = evidence()
    before = await local_record(owner)

    def fail_insert(conn, cursor, statement, parameters, context, executemany):
        if f"INSERT INTO {failing_table}" in statement:
            raise RuntimeError("injected correction persistence failure")

    event.listen(owner._test_engine.sync_engine, "before_cursor_execute", fail_insert)
    try:
        assert not await owner.mark_instrument_delisted(
            KONKA["instrument_id"], delisted_date=proof["delisted_date"],
            source=proof["source"], evidence=proof, date_correction=DATE_CORRECTION,
        )
    finally:
        event.remove(owner._test_engine.sync_engine, "before_cursor_execute", fail_insert)
    assert await local_record(owner) == before
    assert await correction_rows(owner) == []


@pytest.mark.asyncio
async def test_controlled_correction_cas_rejects_a_change_after_validation(owner, monkeypatch):
    import database.instrument_lifecycle as lifecycle

    await legacy_delisted(owner)
    original = lifecycle._prepare_date_correction

    async def change_after_validation(session, *args):
        payload = await original(session, *args)
        await session.execute(text("UPDATE instruments SET source='concurrent_refresh'"))
        return payload

    monkeypatch.setattr(lifecycle, "_prepare_date_correction", change_after_validation)
    proof = evidence()
    assert not await owner.mark_instrument_delisted(
        KONKA["instrument_id"], delisted_date=proof["delisted_date"],
        source=proof["source"], evidence=proof, date_correction=DATE_CORRECTION,
    )
    assert (await local_record(owner))["delisted_date"] == datetime(2026, 10, 22)
    assert await correction_rows(owner) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("iid,name,accepted,source_listed,terminal", [
    ("000003.SZ", "PT金田A", "1991-07-03", "1991-01-14", "2002-06-14"),
    ("000004.SZ", "国华退", "1991-01-14", "1990-12-01", "2026-07-14"),
])
async def test_human_listing_date_review_resolves_only_approved_source_discrepancy(
    owner, iid, name, accepted, source_listed, terminal,
):
    local = {**KONKA, "instrument_id": iid, "symbol": iid[:6], "name": name,
             "listed_date": accepted, "delisted_date": terminal,
             "status": "delisted", "is_active": False, "trading_status": 0}
    # Build a persisted legacy instrument, with both date values kept intact.
    async with owner.get_async_session() as session:
        session.add(InstrumentDB(**{
            **local, "listed_date": datetime.fromisoformat(accepted),
            "delisted_date": datetime.fromisoformat(terminal),
        }))
        await session.commit()
    source = AShareOfficialStockMasterSource(
        "exchange_official_a_stock",
        config={"szse": {"terminal_list_file": str(FIXTURES / "szse_terminated.xlsx")}},
    )
    snapshot = await source.get_delisting_snapshot("SZSE")
    proof = next(r for r in snapshot["rows"] if r["instrument_id"] == iid)
    assert proof["listed_date"] == source_listed

    async def audit():
        return await AShareDelistingReconciler(owner, None).reconcile(
            "SZSE", {"rows_by_id": {iid: local}}, [],
            {"official_current_list_complete": True}, audit_only=True,
            snapshot={"rows": [proof], "complete": True, "errors": []},
        )

    assert (await audit())["conflict_count"] == 1
    review = {
        "status": "approved", "instrument_id": iid, "exchange": "SZSE", "type": "stock",
        "listed_date": accepted, "source_listed_date": source_listed,
        "delisted_date": terminal, "source": proof["source"],
        "reviewed_by": "user", "reviewed_at": get_shanghai_time().isoformat(),
        "reference": "human-listing-review-test", "reason": "User confirmed local listing date",
    }
    assert await owner.save_instrument_master_metadata_batch([{
        "instrument_id": iid, "exchange": "SZSE", "product_type": "stock",
        "metadata": {"a_share_listing_date_review": review},
    }]) == 1
    result = await audit()
    assert result["status"] == "success" and result["conflict_count"] == 0
    assert result["confirmed_count"] == 1
    # Routine metadata refresh cannot erase the review; the next run reads it anew.
    assert await owner.save_instrument_master_metadata_batch([{
        "instrument_id": iid, "exchange": "SZSE", "product_type": "stock",
        "metadata": {"source_authority": "official"},
    }]) == 1
    assert (await owner.get_instrument_master_listing_date_reviews([iid]))[iid] == review
    assert (await audit())["status"] == "success"
    async with owner.get_async_session() as session:
        record = (await session.execute(select(InstrumentDB))).scalar_one()
        assert record.listed_date == datetime.fromisoformat(accepted)
        assert record.delisted_date == datetime.fromisoformat(terminal)
        assert not record.is_active and record.status == "delisted"
    assert (await owner.load_a_share_delisting_state(["SZSE"]))["has_baseline"] is False


def test_human_listing_date_review_does_not_cover_changed_dates_or_other_identity():
    from instrument_master_delisting import _identity_issue

    local = {**KONKA, "delisted_date": "2026-10-23"}
    proof = evidence(listed_date="1991-01-14")
    review = {
        "status": "approved", "instrument_id": KONKA["instrument_id"],
        "exchange": "SZSE", "type": "stock", "listed_date": KONKA["listed_date"],
        "source_listed_date": "1991-01-14", "delisted_date": "2026-10-23",
        "source": proof["source"], "reviewed_by": "user",
        "reviewed_at": get_shanghai_time().isoformat(),
        "reference": "human-listing-review-test", "reason": "User confirmation",
    }
    assert _identity_issue(local, proof, None, review) is None
    assert _identity_issue(local, {**proof, "listed_date": "1991-02-01"}, None, review) == "listing_interval_conflict"
    assert _identity_issue(local, {**proof, "delisted_date": "2026-10-22"}, None, review) == "listing_interval_conflict"
    assert _identity_issue({**local, "listed_date": "1992-04-01"}, proof, None, review) == "listing_interval_conflict"
    for change in [
        {"instrument_id": "200016.SZ"}, {"status": "pending"},
        {"reviewed_at": "bad-time"}, {"source": "different_source"},
    ]:
        assert _identity_issue(local, proof, None, {**review, **change}) == "listing_interval_conflict"
    assert _identity_issue(local, proof, None) == "listing_interval_conflict"
