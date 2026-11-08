# 覆盖索引运维 runbook：财务就绪查询加速（条件项）

> 对应 change：`accelerate-financial-readiness-queries`（tasks 3.2/3.3）
>
> 脚本：`scripts/financial_readiness_covering_index.py`

## 状态：条件项（暂不执行）

2026-11-08 EXPLAIN 实证：窄口径查询（canonical IN 下推后每候选仅 ~7 行）已被**既有** `idx_financial_numeric_facts_hot_canonical`（热表）与 `financial_numeric_facts_history_canonical`（历史表）支撑为 index SEARCH——无需新索引即可满足性能目标。本 runbook 仅在 **M4.1 实测仍不达标**时启用；`raw_fact_json` 必须回表，index-only 对本查询形态不可达。

## 目标索引（条件触发后才建）

```
idx_financial_numeric_facts_hot_readiness_covering
ON financial_numeric_facts_hot
(instrument_id, report_period, canonical_fact_name, updated_at, fact_value, unit)
```

历史表（financial_numeric_facts_history，全表约 1.1 万行）不建索引，窄方法直查后合并。

## 维护窗口核查清单（全部满足才可 --apply）

`05_scheduler.json` 中写 `financials.db` 的启用任务全集：

| 任务 | 窗口 | 上限 |
|---|---|---|
| `financial_disclosure_incremental_sync` | 每日 21:45 | 4h（最坏 01:45 结束） |
| `financial_disclosure_reconciliation_sync` | 周日 09:30 | 8h（最坏 17:30 结束） |
| `financial_l1_full_import` | manual_only | 需确认无人在跑 |
| `broker_risk_control_incremental_sync` | manual_only | 需确认无人在跑 |

建议窗口：**凌晨 ≥02:00 且非周日**（避开增量的最坏结束时间与周日对账全天）。2026-11-02 曾发生增量跑到 01:45 后的情况——00:30-02:00 不再视为安全。

核查命令：

```bash
curl -s http://127.0.0.1:8000/api/v1/system/worker-runs     # 无 financial 写任务在跑
tail -20 log/task.log                                        # 无增量/对账收尾迹象
python scripts/financial_readiness_covering_index.py check   # 索引状态+磁盘余量
```

## 操作序列

```bash
# 1. 预检（索引状态、磁盘 ≥3GB、窗口清单）
python scripts/financial_readiness_covering_index.py check

# 2. 建索引（长事务；WAL 下读者不受影响，写者阻塞至完成；
#    中断安全：SQLite 回滚未完成的 CREATE INDEX，重跑即可）
python scripts/financial_readiness_covering_index.py apply

# 3. 验证 index-only scan（不得出现 TABLE row lookup）
python scripts/financial_readiness_covering_index.py verify

# 4.（可选）回滚
python scripts/financial_readiness_covering_index.py rollback
```

## 时长与空间

- 行数：`financial_numeric_facts_hot` 约 2,970 万行；
- 空间：估 1~2GB（窄键），建前 `check` 确认磁盘余量 ≥3GB；
- 时长：估 20~60 分钟——**tasks 3.1 要求先在备份副本实测后把实测值写回本节**（尚未执行：当前生产无现成 77GB 副本）。

## 回滚

`rollback` 子命令 DROP INDEX，无数据影响；查询回退到现有 `idx_financial_numeric_facts_hot_instrument` + 回表。
