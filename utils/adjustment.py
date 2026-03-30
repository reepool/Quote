"""
通用复权计算引擎

支持多市场（A股/港股/美股/期货）的动态复权计算：
- 前复权 (forward): 以最新日为基准, 历史价格向下调整
- 后复权 (backward): 以上市日为基准, 价格向上调整
- 期货连续合约调整 (预留)

核心公式:
  单日除权因子   F_day = P_prev / P_ex
  累积后复权因子 F_cum(t) = ∏ F_day(i)  (i: 上市日 → t)
  前复权价      = 原始价 × F_cum(t) / F_cum(latest)
  后复权价      = 原始价 × F_cum(t)
"""

import logging
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("quote_system.adjustment")

# ---------------------------------------------------------------------------
# 默认字段名
# ---------------------------------------------------------------------------
DEFAULT_PRICE_FIELDS: Tuple[str, ...] = ("open", "high", "low", "close")
DEFAULT_VOLUME_FIELDS: Tuple[str, ...] = ("volume",)
PRECISION: int = 6  # 中间计算保留精度


class AdjustmentEngine:
    """通用复权计算引擎

    设计要点:
    1. 纯静态方法, 无状态, 线程安全
    2. price_fields / volume_fields 参数化, 适配不同市场
    3. 期货用价差（Gap）而非比例（Factor）, 用独立方法
    """

    # ------------------------------------------------------------------
    # 公开 API
    # ------------------------------------------------------------------

    @staticmethod
    def forward_adjust(
        quotes: List[Dict],
        factors: List[Dict],
        price_fields: Tuple[str, ...] = DEFAULT_PRICE_FIELDS,
        volume_fields: Tuple[str, ...] = DEFAULT_VOLUME_FIELDS,
    ) -> List[Dict]:
        """前复权计算

        以查询范围内最新交易日的累积因子为基准(=1.0),
        历史价格按比例缩小, 使得价格序列连续.

        Args:
            quotes: 非复权原始行情列表, 必须包含 'time' 字段
            factors: 复权因子事件列表, 每项含 {ex_date, cumulative_factor}
            price_fields: 需要调整的价格字段
            volume_fields: 需要反向调整的成交量字段

        Returns:
            前复权后的行情列表(新列表, 不修改原数据)
        """
        if not quotes:
            return []

        # 构建逐日累积因子映射
        daily_cum = AdjustmentEngine._build_daily_cumulative_map(factors)

        # 获取最新日的累积因子作为基准
        latest_cum = AdjustmentEngine._get_latest_cum_factor(quotes, daily_cum)
        if latest_cum == 0:
            logger.warning("[AdjustmentEngine] 最新累积因子为0, 跳过前复权")
            return [q.copy() for q in quotes]

        result = []
        for q in quotes:
            adjusted = q.copy()
            q_date = AdjustmentEngine._extract_date(q["time"])
            cum_factor = AdjustmentEngine._lookup_cumulative_factor(q_date, daily_cum)

            # 前复权因子 = 累积因子 / 最新累积因子
            forward_factor = round(cum_factor / latest_cum, PRECISION)

            # 调整价格字段
            for field in price_fields:
                if field in adjusted and adjusted[field] is not None:
                    adjusted[field] = round(
                        float(adjusted[field]) * forward_factor, 4
                    )

            # 反向调整成交量 (价格缩小, 量放大)
            if forward_factor > 0:
                for field in volume_fields:
                    if field in adjusted and adjusted[field] is not None:
                        adjusted[field] = int(
                            float(adjusted[field]) / forward_factor
                        )

            # 填充 factor 和 adjustment_type 供 API 输出
            adjusted["factor"] = forward_factor
            adjusted["adjustment_type"] = "forward"

            result.append(adjusted)

        return result

    @staticmethod
    def backward_adjust(
        quotes: List[Dict],
        factors: List[Dict],
        price_fields: Tuple[str, ...] = DEFAULT_PRICE_FIELDS,
        volume_fields: Tuple[str, ...] = DEFAULT_VOLUME_FIELDS,
    ) -> List[Dict]:
        """后复权计算

        以上市首日为基准(累积因子=1.0),
        后续价格按除权事件累乘放大, 反映真实持股收益.

        Args:
            quotes: 非复权原始行情列表
            factors: 复权因子事件列表
            price_fields: 需要调整的价格字段
            volume_fields: 需要反向调整的成交量字段

        Returns:
            后复权后的行情列表
        """
        if not quotes:
            return []

        daily_cum = AdjustmentEngine._build_daily_cumulative_map(factors)

        result = []
        for q in quotes:
            adjusted = q.copy()
            q_date = AdjustmentEngine._extract_date(q["time"])
            cum_factor = AdjustmentEngine._lookup_cumulative_factor(q_date, daily_cum)

            # 调整价格字段
            for field in price_fields:
                if field in adjusted and adjusted[field] is not None:
                    adjusted[field] = round(
                        float(adjusted[field]) * cum_factor, 4
                    )

            # 反向调整成交量
            if cum_factor > 0:
                for field in volume_fields:
                    if field in adjusted and adjusted[field] is not None:
                        adjusted[field] = int(
                            float(adjusted[field]) / cum_factor
                        )

            adjusted["factor"] = round(cum_factor, PRECISION)
            adjusted["adjustment_type"] = "backward"

            result.append(adjusted)

        return result

    @staticmethod
    def no_adjust(quotes: List[Dict]) -> List[Dict]:
        """不复权 - 标记 adjustment_type 为 none

        Args:
            quotes: 非复权原始行情列表

        Returns:
            标记了 adjustment_type='none' 的行情列表
        """
        if not quotes:
            return []

        result = []
        for q in quotes:
            adjusted = q.copy()
            adjusted["factor"] = 1.0
            adjusted["adjustment_type"] = "none"
            result.append(adjusted)

        return result

    @staticmethod
    def apply_adjustment(
        quotes: List[Dict],
        factors: List[Dict],
        adjust_type: str = "qfq",
        price_fields: Tuple[str, ...] = DEFAULT_PRICE_FIELDS,
        volume_fields: Tuple[str, ...] = DEFAULT_VOLUME_FIELDS,
    ) -> List[Dict]:
        """统一入口: 根据 adjust_type 分发到具体的复权方法

        Args:
            quotes: 非复权原始行情
            factors: 复权因子
            adjust_type: 'qfq'(前复权) / 'hfq'(后复权) / 'none'(不复权)
            price_fields: 价格字段
            volume_fields: 成交量字段

        Returns:
            复权后的行情列表
        """
        adjust_type = (adjust_type or "qfq").lower().strip()

        if adjust_type in ("qfq", "forward"):
            return AdjustmentEngine.forward_adjust(
                quotes, factors, price_fields, volume_fields
            )
        elif adjust_type in ("hfq", "backward"):
            return AdjustmentEngine.backward_adjust(
                quotes, factors, price_fields, volume_fields
            )
        elif adjust_type in ("none", ""):
            return AdjustmentEngine.no_adjust(quotes)
        else:
            logger.warning(
                "[AdjustmentEngine] 未知复权类型 '%s', 默认使用前复权", adjust_type
            )
            return AdjustmentEngine.forward_adjust(
                quotes, factors, price_fields, volume_fields
            )

    @staticmethod
    def compute_cumulative_factors(
        factor_events: List[Dict],
    ) -> List[Dict]:
        """从单日除权因子事件计算累积复权因子

        输入: [{ex_date, factor}, ...]  (factor 为单日除权因子, >1 表示除权)
        输出: [{ex_date, factor, cumulative_factor}, ...]

        Args:
            factor_events: 单日除权因子列表, 按 ex_date 升序

        Returns:
            带有 cumulative_factor 的因子列表
        """
        if not factor_events:
            return []

        # 按日期升序排列
        sorted_events = sorted(
            factor_events,
            key=lambda x: AdjustmentEngine._extract_date(x["ex_date"]),
        )

        cumulative = 1.0
        result = []
        for event in sorted_events:
            day_factor = float(event.get("factor", 1.0))
            cumulative *= day_factor
            enriched = event.copy()
            enriched["cumulative_factor"] = round(cumulative, PRECISION)
            result.append(enriched)

        return result

    @staticmethod
    def futures_continuous_adjust(
        quotes: List[Dict],
        roll_gaps: List[Dict],
        method: str = "back-adjusted",
        price_fields: Tuple[str, ...] = DEFAULT_PRICE_FIELDS,
    ) -> List[Dict]:
        """期货主力合约连续调整（预留扩展）

        与股票复权不同, 期货使用价差（加减法）而非比例（乘除法）:
        - back-adjusted: 保持最新价格不变, 历史价格 += 累积Gap
        - ratio-adjusted: 类似股票, 使用比例因子

        Args:
            quotes: 原始行情
            roll_gaps: [{roll_date, gap}, ...] 换月日和价差
            method: 'back-adjusted' / 'ratio-adjusted'
            price_fields: 价格字段

        Returns:
            调整后的行情列表
        """
        if not quotes or not roll_gaps:
            return [q.copy() for q in quotes]

        # 按换月日降序排列, 从最新向历史回溯
        sorted_gaps = sorted(
            roll_gaps,
            key=lambda x: AdjustmentEngine._extract_date(x["roll_date"]),
            reverse=True,
        )

        if method == "back-adjusted":
            return AdjustmentEngine._back_adjusted_futures(
                quotes, sorted_gaps, price_fields
            )
        else:
            logger.warning(
                "[AdjustmentEngine] 期货调整方法 '%s' 暂不支持, 返回原始数据",
                method,
            )
            return [q.copy() for q in quotes]

    # ------------------------------------------------------------------
    # 内部辅助方法
    # ------------------------------------------------------------------

    @staticmethod
    def _build_daily_cumulative_map(
        factors: List[Dict],
    ) -> Dict[date, float]:
        """构建逐日累积因子映射

        factors 中每条记录代表一个除权事件, 含 ex_date 和 cumulative_factor.
        对于无事件的普通交易日, 累积因子等于前一个事件日的累积因子.

        返回 {date -> cumulative_factor} 的字典, 用于快速查找.
        对于无记录的日期, 调用方使用 .get(date, 1.0) 获取默认值 1.0.
        """
        if not factors:
            return {}

        # 按 ex_date 升序排列
        sorted_factors = sorted(
            factors,
            key=lambda x: AdjustmentEngine._extract_date(x["ex_date"]),
        )

        cum_map: Dict[date, float] = {}
        for f in sorted_factors:
            d = AdjustmentEngine._extract_date(f["ex_date"])
            cum_val = float(f.get("cumulative_factor", 1.0))
            cum_map[d] = cum_val

        return cum_map

    @staticmethod
    def _lookup_cumulative_factor(
        target_date: date, daily_cum: Dict[date, float]
    ) -> float:
        """查找指定日期的有效累积因子

        逻辑: 如果 target_date 本身是除权日, 直接返回其因子.
        否则, 返回 target_date 之前最近一个除权事件日的因子值.
        如果 target_date 早于所有除权事件, 返回 1.0 (未经历任何除权).
        """
        if not daily_cum:
            return 1.0

        # 精确命中
        if target_date in daily_cum:
            return daily_cum[target_date]

        # 找 <= target_date 的最近因子日期
        candidates = [d for d in daily_cum.keys() if d <= target_date]
        if candidates:
            return daily_cum[max(candidates)]

        # target_date 早于所有除权事件
        return 1.0

    @staticmethod
    def _get_latest_cum_factor(
        quotes: List[Dict], daily_cum: Dict[date, float]
    ) -> float:
        """获取查询范围内最新交易日的累积因子

        逻辑: 找到 quotes 中最大日期, 然后向前搜索 daily_cum 中
              最近的一个累积因子. 如果没有任何因子记录, 返回 1.0.
        """
        if not quotes:
            return 1.0

        # 找到行情数据中的最大日期
        max_date = max(
            AdjustmentEngine._extract_date(q["time"]) for q in quotes
        )

        if not daily_cum:
            return 1.0

        # 找 <= max_date 的最近因子日期
        max_factor_date = max(daily_cum.keys())

        if max_date >= max_factor_date:
            return daily_cum[max_factor_date]

        # max_date 早于所有因子日期 — 无除权事件影响
        candidates = [d for d in daily_cum.keys() if d <= max_date]
        if candidates:
            return daily_cum[max(candidates)]

        return 1.0

    @staticmethod
    def _extract_date(dt_value) -> date:
        """从各种日期类型中提取 date 对象"""
        if isinstance(dt_value, datetime):
            return dt_value.date()
        if isinstance(dt_value, date):
            return dt_value
        if isinstance(dt_value, str):
            # 支持 'YYYY-MM-DD' 和 'YYYY-MM-DDTHH:MM:SS' 格式
            return datetime.fromisoformat(dt_value.replace("Z", "")).date()
        raise TypeError(
            f"无法从 {type(dt_value).__name__} 类型提取 date: {dt_value}"
        )

    @staticmethod
    def _back_adjusted_futures(
        quotes: List[Dict],
        sorted_gaps: List[Dict],
        price_fields: Tuple[str, ...],
    ) -> List[Dict]:
        """期货后复权（价差调整法）

        从最新换月日开始向历史回溯:
        - 换月日之前的价格, 加上该次换月的 gap
        - 多次换月的 gap 累加
        """
        result = [q.copy() for q in quotes]

        for gap_info in sorted_gaps:
            roll_date = AdjustmentEngine._extract_date(gap_info["roll_date"])
            gap = float(gap_info.get("gap", 0.0))

            if gap == 0.0:
                continue

            for adjusted in result:
                q_date = AdjustmentEngine._extract_date(adjusted["time"])
                if q_date < roll_date:
                    for field in price_fields:
                        if field in adjusted and adjusted[field] is not None:
                            adjusted[field] = round(
                                float(adjusted[field]) + gap, 4
                            )

        return result


# ---------------------------------------------------------------------------
# 便捷函数 (模块级)
# ---------------------------------------------------------------------------

def apply_adjustment(
    quotes: List[Dict],
    factors: List[Dict],
    adjust_type: str = "qfq",
) -> List[Dict]:
    """模块级便捷函数, 调用 AdjustmentEngine.apply_adjustment"""
    return AdjustmentEngine.apply_adjustment(quotes, factors, adjust_type)


def adjust_quotes(
    quotes: List[Dict],
    factors: List[Dict],
    adjust_type: str = "qfq",
    time_field: str = "time",
    price_fields: Tuple[str, ...] = DEFAULT_PRICE_FIELDS,
    volume_fields: Tuple[str, ...] = DEFAULT_VOLUME_FIELDS,
) -> List[Dict]:
    """通用复权入口 — 支持日线/周线/月线及自定义字段

    设计为多频率数据的统一接口, 通过参数化 time_field 和 price_fields
    支持不同时间粒度（日线/周线/月线）及不同市场的价格字段命名约定。

    复权逻辑与 AdjustmentEngine 完全一致:
    - 以 time_field 字段提取记录日期, 匹配 factors 中的 ex_date
    - 日线以外的频率（周线、月线）按记录所在日期向前查找最近因子

    Args:
        quotes:       行情列表 (日线/周线/月线均可), 每条须含 time_field 字段
        factors:      复权因子事件列表, 来自 adjustment_factors 表
        adjust_type:  'qfq'(前复权) / 'hfq'(后复权) / 'none'(不复权)
        time_field:   时间字段名 (日线='time', 周线/月线视实现而定)
        price_fields: 需调整的价格字段名元组
        volume_fields:需反向调整的成交量字段名元组

    Returns:
        复权后的行情列表 (新列表, 不修改原数据)

    使用示例:
        # 日线复权 (标准)
        adjusted = adjust_quotes(daily_quotes, factors, adjust_type='qfq')

        # 周线复权 (预留接口, 时间字段名为 'week_start')
        adjusted = adjust_quotes(
            weekly_quotes, factors,
            adjust_type='qfq',
            time_field='week_start',   # 周线记录使用周起始日期
        )

        # 月线复权 (预留接口)
        adjusted = adjust_quotes(
            monthly_quotes, factors,
            adjust_type='qfq',
            time_field='month_start',  # 月线记录使用月起始日期
        )

        # 港股/美股 (自定义价格字段, 无成交量反向调整)
        adjusted = adjust_quotes(
            hk_quotes, factors,
            price_fields=('open', 'high', 'low', 'close', 'adj_close'),
            volume_fields=(),
        )
    """
    if not quotes:
        return []

    # 若使用非默认 time_field, 临时转换为引擎期望的 'time' 字段
    if time_field != "time":
        normalized = []
        for q in quotes:
            nq = q.copy()
            nq["time"] = nq.pop(time_field, None)
            normalized.append(nq)
        adjusted = AdjustmentEngine.apply_adjustment(
            normalized, factors, adjust_type, price_fields, volume_fields
        )
        # 还原 time_field 字段名
        result = []
        for adj_q, orig_q in zip(adjusted, quotes):
            rq = adj_q.copy()
            rq[time_field] = rq.pop("time", orig_q.get(time_field))
            result.append(rq)
        return result
    else:
        return AdjustmentEngine.apply_adjustment(
            quotes, factors, adjust_type, price_fields, volume_fields
        )

