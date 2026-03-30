"""
复权因子提供者协议 (AdjustmentFactorProvider)

所有数据源在实现 get_adjustment_factors 时须遵循本协议。
未来新增美股/港股/期货等数据源时, 只需实现此接口即可无缝接入复权计算框架。

支持的市场扩展点:
- A 股 (BaoStock / AkShare): 送转股、现金分红、配股
- 港股 / 美股 (yFinance): 基于 Adj Close 反推因子
- 期货 (预留): 换月价差调整, 使用 AdjustmentEngine.futures_continuous_adjust()

因子字段规范:
    必填字段:
        instrument_id     str       品种 ID (与 instruments 表主键一致)
        ex_date           datetime  除权除息日
        factor            float     单日除权因子 (> 1 表示除权)
        cumulative_factor float     累积后复权因子 (= ∏ 历次 factor)
        source            str       数据源名称 (如 'baostock', 'akshare', 'yfinance')
    可选字段:
        dividend          float     每股现金分红 (元)
        bonus_shares      float     每股送转股 (股)
        rights_shares     float     每股配股数 (股)
        rights_price      float     配股价格 (元)
        event_type        str       事件类型: 'dividend'|'split'|'rights'|'mixed'|'roll'

复权计算上游使用 cumulative_factor 字段:
    前复权价  = 原始价 × cumulative_factor / max(cumulative_factor)
    后复权价  = 原始价 × cumulative_factor
    期货连续  = 原始价 ± 累积价差 (另见 AdjustmentEngine.futures_continuous_adjust)
"""

from typing import Any, Dict, List, Protocol, runtime_checkable
from datetime import datetime


@runtime_checkable
class AdjustmentFactorProvider(Protocol):
    """复权因子提供者协议

    满足本协议的数据源可直接接入 DataSourceFactory.get_adjustment_factors()。
    不支持复权因子的数据源返回空列表 [] 即可 (如仅提供指数数据的源)。

    Implementation Example:
        class MyDataSource(BaseDataSource):
            async def get_adjustment_factors(
                self,
                instrument_id: str,
                symbol: str,
                start_date: datetime,
                end_date: datetime,
            ) -> List[Dict[str, Any]]:
                # 返回因子列表
                return [{
                    'instrument_id': instrument_id,
                    'ex_date': some_date,
                    'factor': 1.05,
                    'cumulative_factor': 1.25,
                    'source': 'my_source',
                }]
    """

    async def get_adjustment_factors(
        self,
        instrument_id: str,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
    ) -> List[Dict[str, Any]]:
        """获取指定品种在日期范围内的复权因子事件列表。

        Args:
            instrument_id: 品种 ID, 格式与 instruments 表主键一致 (如 '600519.SH')
            symbol:        交易代码 (如 '600519')
            start_date:    查询开始时间 (含)
            end_date:      查询结束时间 (含)

        Returns:
            复权因子事件列表, 按 ex_date 升序排列。
            - 仅返回发生除权除息事件的日期记录 (非每日一条)
            - 无除权记录时返回 []
        """
        ...


def is_factor_provider(source: object) -> bool:
    """检查数据源是否实现了 AdjustmentFactorProvider 协议。

    用于 DataSourceFactory 在路由因子请求时做能力探测。

    Args:
        source: 数据源实例

    Returns:
        True if the source implements AdjustmentFactorProvider
    """
    return isinstance(source, AdjustmentFactorProvider)
