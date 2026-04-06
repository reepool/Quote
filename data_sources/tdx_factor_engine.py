"""
通达信 XDXR 自研复权因子引擎

根据 pytdx 的 get_xdxr_info 返回的原始分红/送股/配股信息,
计算单日除权因子和累积后复权因子.

计算公式:
  除权价 = (前收盘 - 每股分红 + 配股价 × 每股配股) / (1 + 每股送转 + 每股配股)
  单日因子 = 前收盘 / 除权价
  累积因子 = 所有单日因子的连乘积

注意:
  - pytdx XDXR 的 fenhong/songzhuangu/peigu 单位是「每 10 股」, 需 ÷ 10 转为「每股」
  - 计算依赖前收盘价, 需要日线数据配合
  - 结果仅用于审计/验证, 不写入生产因子表
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from pytdx.hq import TdxHq_API

tdx_factor_logger = logging.getLogger("tdx_factor_engine")

# K线类型
KLINE_TYPE_DAILY = 9


class TdxFactorEngine:
    """基于 XDXR 的复权因子计算引擎"""

    @staticmethod
    def calculate_day_factor(
        pre_close: float,
        fenhong: float,
        songzhuangu: float,
        peigu: float,
        peigujia: float,
    ) -> float:
        """计算单日除权因子

        Args:
            pre_close: 除权日的前收盘价
            fenhong: 每 10 股分红 (元)
            songzhuangu: 每 10 股送转股
            peigu: 每 10 股配股
            peigujia: 配股价 (元)

        Returns:
            单日除权因子 (> 1 表示除权, < 1 表示特殊情况如缩股)
        """
        if pre_close <= 0:
            return 1.0

        # 每 10 股 → 每股
        per_share_dividend = fenhong / 10.0
        per_share_bonus = songzhuangu / 10.0
        per_share_rights = peigu / 10.0

        # 除权价
        numerator = pre_close - per_share_dividend + peigujia * per_share_rights
        denominator = 1.0 + per_share_bonus + per_share_rights

        if denominator <= 0 or numerator <= 0:
            tdx_factor_logger.warning(
                f"异常因子参数: pre_close={pre_close}, "
                f"fenhong={fenhong}, songzhuangu={songzhuangu}, "
                f"peigu={peigu}, peigujia={peigujia}"
            )
            return 1.0

        ex_price = numerator / denominator

        # 单日因子 = 前收盘 / 除权价
        factor = pre_close / ex_price
        return round(factor, 6)

    def compute_factors(
        self,
        api: TdxHq_API,
        market: int,
        code: str,
        instrument_id: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """计算指定品种在日期范围内的复权因子

        Args:
            api: pytdx 连接实例
            market: 市场代码 (0=深圳, 1=上海, 2=北交所)
            code: 股票代码
            instrument_id: 品种 ID (如 '000001.SZ')
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            因子列表, 每条包含:
                instrument_id, ex_date, factor, cumulative_factor,
                pre_close, fenhong, songzhuangu, peigu, peigujia, source
        """
        # 1. 获取 XDXR 事件
        xdxr = api.get_xdxr_info(market, code)
        if not xdxr:
            return []

        # 筛选 category=1 (除权除息) 且在日期范围内的事件
        div_events: list[dict] = []
        for x in xdxr:
            if x.get("category") != 1:
                continue
            try:
                ex_date = datetime(
                    int(x.get("year", 0)),
                    int(x.get("month", 1)),
                    int(x.get("day", 1)),
                )
            except (ValueError, TypeError):
                continue

            if start_date.date() <= ex_date.date() <= end_date.date():
                div_events.append({
                    "ex_date": ex_date,
                    "fenhong": float(x.get("fenhong", 0) or 0),
                    "songzhuangu": float(x.get("songzhuangu", 0) or 0),
                    "peigu": float(x.get("peigu", 0) or 0),
                    "peigujia": float(x.get("peigujia", 0) or 0),
                    "suogu": float(x.get("suogu", 0) or 0),
                })

        if not div_events:
            return []

        # 按日期排序
        div_events.sort(key=lambda e: e["ex_date"])

        # 2. 获取日线数据 (用于查前收盘价)
        #    需要获取从最早事件日之前到最晚事件日的日线
        earliest_event = div_events[0]["ex_date"]
        pre_close_map = self._build_pre_close_map(api, market, code, earliest_event)

        # 3. 计算因子
        results: list[dict] = []
        cumulative_factor = 1.0

        for event in div_events:
            ex_date = event["ex_date"]
            fenhong = event["fenhong"]
            songzhuangu = event["songzhuangu"]
            peigu = event["peigu"]
            peigujia = event["peigujia"]

            # 查找除权日前一交易日的收盘价
            pre_close = pre_close_map.get(ex_date)
            if pre_close is None or pre_close <= 0:
                tdx_factor_logger.warning(
                    f"[TdxFactorEngine] {instrument_id} "
                    f"除权日 {ex_date.date()} 无前收盘价, 跳过"
                )
                continue

            # 计算单日因子
            day_factor = self.calculate_day_factor(
                pre_close, fenhong, songzhuangu, peigu, peigujia
            )

            # 累积因子
            cumulative_factor *= day_factor

            results.append({
                "instrument_id": instrument_id,
                "ex_date": ex_date,
                "factor": round(day_factor, 6),
                "cumulative_factor": round(cumulative_factor, 6),
                "pre_close": round(pre_close, 4),
                "fenhong": fenhong,
                "songzhuangu": songzhuangu,
                "peigu": peigu,
                "peigujia": peigujia,
                "source": "tdx_xdxr",
            })

        return results

    def compute_factors_batch(
        self,
        api: TdxHq_API,
        stocks: list[dict],
        start_date: datetime,
        end_date: datetime,
    ) -> dict[str, list[dict]]:
        """批量计算因子

        Args:
            api: pytdx 连接实例
            stocks: 品种列表, 每项含 instrument_id, market (可选)
            start_date, end_date: 日期范围

        Returns:
            {instrument_id: [factor_records]}
        """
        from .tdx_source import _parse_instrument_id

        results: dict[str, list[dict]] = {}
        for stock in stocks:
            instrument_id = stock.get("instrument_id", "")
            try:
                market, code = _parse_instrument_id(instrument_id)
            except ValueError:
                continue

            factors = self.compute_factors(
                api, market, code, instrument_id, start_date, end_date
            )
            if factors:
                results[instrument_id] = factors

        return results

    def _build_pre_close_map(
        self,
        api: TdxHq_API,
        market: int,
        code: str,
        earliest_event: datetime,
    ) -> dict[datetime, float]:
        """构建 {除权日: 前一交易日收盘价} 映射

        思路: 获取足够多的日线, 构建日期→收盘价映射,
        然后对每个除权日, 查找 < ex_date 的最后一个交易日收盘价.
        """
        # 获取全历史日线 (从最新往前取, 直到早于 earliest_event)
        all_bars: list[tuple[datetime, float]] = []  # (date, close)
        offset = 0
        batch_size = 800

        for _ in range(200):  # 安全阀
            bars = api.get_security_bars(
                KLINE_TYPE_DAILY, market, code, offset, batch_size
            )
            if not bars:
                break

            for bar in bars:
                dt_str = bar.get("datetime", "")
                if dt_str:
                    try:
                        dt = datetime.strptime(dt_str[:10], "%Y-%m-%d")
                        close = float(bar.get("close", 0))
                        all_bars.append((dt, close))
                    except (ValueError, TypeError):
                        pass

            offset += len(bars)

            # 检查是否已足够早
            if all_bars:
                earliest_bar = min(all_bars, key=lambda x: x[0])
                if earliest_bar[0].date() < earliest_event.date() - \
                        __import__("datetime").timedelta(days=30):
                    break

            if len(bars) < batch_size:
                break

        # 按日期排序
        all_bars.sort(key=lambda x: x[0])

        # 构建 {日期: 收盘价}
        date_close_map: dict[datetime, float] = {dt: close for dt, close in all_bars}
        sorted_dates = sorted(date_close_map.keys())

        # 构建 {除权日: 前一交易日收盘价}
        pre_close_map: dict[datetime, float] = {}

        for ex_date_candidate in date_close_map.keys():
            # 找 < ex_date_candidate 的最近交易日
            prev_dates = [d for d in sorted_dates if d.date() < ex_date_candidate.date()]
            if prev_dates:
                prev_date = prev_dates[-1]
                pre_close_map[ex_date_candidate] = date_close_map[prev_date]

        return pre_close_map
