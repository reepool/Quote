"""
pytdx 集成测试 (需要真实网络连接)

覆盖:
  1. TdxSource 初始化 + IP 探测
  2. get_daily_data (000001.SZ 平安银行)
  3. get_latest_daily_data
  4. get_instrument_list (SSE)
  5. get_xdxr_events
  6. get_adjustment_factors (自研因子)
  7. health_check
  8. 数据格式一致性验证 (vol 单位、字段完整性)
"""

import asyncio
import sys
import os
from datetime import datetime, date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_sources.tdx_source import TdxSource
from data_sources.base_source import RateLimitConfig


async def run_integration_tests():
    """集成测试入口"""

    print("=" * 60)
    print("🔌 pytdx 集成测试 (需要真实网络连接)")
    print("=" * 60)

    rlc = RateLimitConfig(
        max_requests_per_minute=6000,
        max_requests_per_hour=100000,
        max_requests_per_day=1000000,
        retry_times=2,
        retry_interval=0.5,
    )

    source = TdxSource(
        name="pytdx_test",
        rate_limit_config=rlc,
        pool_size=2,
        connection_timeout=10.0,
    )

    passed = 0
    failed = 0

    # -----------------------------------------------------------
    # T1: 初始化
    # -----------------------------------------------------------
    print("\n--- T1: 初始化 (IP 探测 + 连接池) ---")
    try:
        await source._initialize_impl()
        active = sum(1 for e in source.ip_manager.ranked_ips if e.status == "active")
        total = len(source.ip_manager.ranked_ips)
        best = source.ip_manager.ranked_ips[0] if total > 0 else None
        print(f"  ✅ 探测完成: {active}/{total} 可达")
        if best:
            print(f"     最快 IP: {best.ip}:{best.port} ({best.latency_ms:.1f}ms)")
        passed += 1
    except Exception as e:
        print(f"  ❌ 初始化失败: {e}")
        failed += 1
        print("\n⚠️ 初始化失败, 无法继续集成测试!")
        return

    # -----------------------------------------------------------
    # T2: health_check
    # -----------------------------------------------------------
    print("\n--- T2: 健康检查 ---")
    try:
        ok = await source.health_check()
        assert ok, "health_check 返回 False"
        print("  ✅ 健康检查通过")
        passed += 1
    except Exception as e:
        print(f"  ❌ 健康检查失败: {e}")
        failed += 1

    # -----------------------------------------------------------
    # T3: get_daily_data (000001.SZ 平安银行)
    # -----------------------------------------------------------
    print("\n--- T3: 获取日线数据 (000001.SZ) ---")
    try:
        start = datetime(2025, 1, 2)
        end = datetime(2025, 1, 10)
        data = await source.get_daily_data("000001.SZ", "000001", start, end)
        assert len(data) > 0, "无数据返回"

        # 验证字段完整性
        required_fields = [
            "time", "instrument_id", "open", "high", "low", "close",
            "volume", "amount", "pre_close", "change", "pct_change",
            "tradestatus", "factor", "source",
        ]
        sample = data[0]
        for field in required_fields:
            assert field in sample, f"缺少字段: {field}"

        # ★ 验证 vol 单位 (应为股, 不是手)
        # 平安银行日均成交量几千万股到几亿股, 不可能 < 10000
        assert sample["volume"] >= 10000, (
            f"volume={sample['volume']}，疑似未乘 100"
        )

        # 验证 source
        assert sample["source"] == "pytdx"

        print(f"  ✅ 获取 {len(data)} 条日线")
        print(f"     样本: {sample['time'].strftime('%Y-%m-%d')} "
              f"O={sample['open']} H={sample['high']} L={sample['low']} C={sample['close']} "
              f"V={sample['volume']:,}")
        passed += 1
    except Exception as e:
        print(f"  ❌ 日线数据获取失败: {e}")
        failed += 1

    # -----------------------------------------------------------
    # T4: get_latest_daily_data
    # -----------------------------------------------------------
    print("\n--- T4: 获取最新日线 (600000.SH 浦发银行) ---")
    try:
        latest = await source.get_latest_daily_data("600000.SH", "600000")
        assert latest, "无数据返回"
        assert "close" in latest
        assert latest["source"] == "pytdx"
        print(f"  ✅ 最新: {latest['time'].strftime('%Y-%m-%d')} C={latest['close']}")
        passed += 1
    except Exception as e:
        print(f"  ❌ 最新日线获取失败: {e}")
        failed += 1

    # -----------------------------------------------------------
    # T5: get_instrument_list (SSE)
    # -----------------------------------------------------------
    print("\n--- T5: 获取品种列表 (SSE) ---")
    try:
        instruments = await source.get_instrument_list("SSE")
        if len(instruments) > 100:
            sample = instruments[0]
            assert "instrument_id" in sample
            assert sample["exchange"] == "SSE"
            print(f"  ✅ SSE 品种数: {len(instruments)}")
            print(f"     样本: {sample['instrument_id']} {sample.get('name', '')}")
        else:
            # pytdx 品种列表非主路由 (由 baostock 负责), 部分服务器可能不支持
            print(f"  ⚠️ SSE 品种数偏少: {len(instruments)} (非致命, 品种列表由 baostock 主导)")
        passed += 1
    except Exception as e:
        print(f"  ⚠️ 品种列表获取异常: {e} (非致命)")
        passed += 1  # 作为辅助功能, 不计入失败

    # -----------------------------------------------------------
    # T6: get_xdxr_events (000001.SZ)
    # -----------------------------------------------------------
    print("\n--- T6: 获取 XDXR 除权除息事件 (000001.SZ) ---")
    try:
        events = await source.get_xdxr_events("000001.SZ")
        assert len(events) > 0, "无 XDXR 事件"

        sample = events[0]
        assert "date" in sample
        assert "fenhong" in sample
        assert "songzhuangu" in sample

        print(f"  ✅ XDXR 事件数: {len(events)}")
        print(f"     最早: {sample['date'].strftime('%Y-%m-%d')} "
              f"分红={sample['fenhong']} 送转={sample['songzhuangu']}")
        passed += 1
    except Exception as e:
        print(f"  ❌ XDXR 事件获取失败: {e}")
        failed += 1

    # -----------------------------------------------------------
    # T7: get_adjustment_factors (自研因子)
    # -----------------------------------------------------------
    print("\n--- T7: 自研因子计算 (000001.SZ, 全历史) ---")
    try:
        factors = await source.get_adjustment_factors(
            "000001.SZ", "000001",
            datetime(1990, 1, 1), datetime.now(),
        )
        if factors:
            print(f"  ✅ 计算出 {len(factors)} 条因子")
            for f in factors[:3]:
                print(f"     {f['ex_date'].strftime('%Y-%m-%d')} "
                      f"factor={f['factor']:.6f} "
                      f"cum={f['cumulative_factor']:.6f} "
                      f"分红={f['fenhong']} 送转={f['songzhuangu']}")
            if len(factors) > 3:
                print(f"     ... (共 {len(factors)} 条)")
            passed += 1
        else:
            print("  ⚠️ 无因子返回 (可能无除权事件或前收盘价缺失)")
            passed += 1  # 非致命
    except Exception as e:
        print(f"  ❌ 因子计算失败: {e}")
        failed += 1

    # -----------------------------------------------------------
    # T8: 北交所数据 (430047.BJ)
    # -----------------------------------------------------------
    print("\n--- T8: 北交所日线 (430047.BJ) ---")
    try:
        start = datetime(2025, 1, 2)
        end = datetime(2025, 1, 10)
        data = await source.get_daily_data("430047.BJ", "430047", start, end)
        if data:
            print(f"  ✅ BSE 获取 {len(data)} 条日线")
            print(f"     样本: {data[0]['time'].strftime('%Y-%m-%d')} C={data[0]['close']}")
        else:
            print("  ⚠️ BSE 无数据 (可能该品种已退市或日期范围无数据)")
        passed += 1
    except Exception as e:
        print(f"  ❌ BSE 数据获取失败: {e}")
        failed += 1

    # -----------------------------------------------------------
    # 清理
    # -----------------------------------------------------------
    await source.close()

    # -----------------------------------------------------------
    # 总结
    # -----------------------------------------------------------
    print("\n" + "=" * 60)
    total = passed + failed
    print(f"📊 集成测试结果: {passed}/{total} 通过, {failed} 失败")
    print("=" * 60)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(run_integration_tests())
