# 财务 equity 口径与资产负债恒等式：调研结论与需求分析

> 日期：2026-11-03
> 来源：三季报（2026-09-30）数据抽查。起因：`financial_facts` 全市场约 31% 的记录
> 出现 `|total_assets − total_liabilities − equity| > 2%` 的"资产负债不匹配"。
> 本文给出根因证据、消费方影响清单与修正需求，作为后续实施卡的需求基线。

## 1. 根因（已实锤）

**数据没有错；是宽表列语义不完整导致的误读。**

三层证据：

1. **映射目录口径完整**（`financial_source_field_mappings`，v5/v6 一致）：
   - `equity_parent` ← 归属于母公司所有者权益合计（parent_holder_equity_total）
   - `equity_total` ← 所有者权益合计（holder_equity_total）
   - `minority_equity` ← 少数股东权益
2. **数值层三层概念齐全**（`financial_numeric_facts_hot`）：Q3 2026 三列均有值。
3. **宽表 `financial_facts.equity` 灌的是归母口径**：
   - 全市场 5,571 家 Q3：`facts.equity ≡ numeric.equity_parent` 100% 一致；
   - 用 `equity_total` 验恒等式 `资产 = 负债 + 权益`：**5,571/5,571 = 100% 成立**；
   - 差异恰好等于 `minority_equity`（例：中国联通 600050 差异 2,126.5 亿 = 少数股东权益）。

口径声明其实已存在于代码：
`research/financial_fact_aliases.py:72` `_CANONICAL_SEMANTICS_V1["equity"]
= "parent_attributable_equity"`。缺的是：宽表无 `equity_total`/`minority_equity`
列、无恒等式校验、无面向使用方的文档声明。

该"不匹配"比例在 2025Q1–2026Q3 各报告期稳定于 30–31%，与三季报采集无关，
是既有 schema 缺陷。

## 2. 受影响面（消费方清单）

| 消费方 | 用法 | 归母是否正确 | 影响 |
| --- | --- | --- | --- |
| `research/financial_pit_panel.py:914` | `bps = equity / shares` | ✅ 正确（BPS 必须归母） | 无 |
| `research/professional_dcf.py`（bank_residual_income.v1 / broker_excess_capital.v1） | required_fields 含 `equity`（账面价值） | ✅ 正确 | 无 |
| `research/valuation_service.py:561` | 传递 `equity` 给估值因子 | ✅ 按归母消费 | 无 |
| `research/storage.py:10952` 别名 `equity↔eq` | 展示/查询别名 | ✅ | 无 |
| API `/research/company/{id}/financial-facts/as-of` | 按 fact_name 键查数值层 | 不受影响 | 无 |
| `financial_summaries.yoy_equity` | 由 facts.equity 计算 | ✅ 语义一致（归母同比） | 无 |
| 任何做 `资产=负债+权益` 校验或用全口径权益做杠杆/资产负债率的使用方 | ❌ 目前只能误用 equity | 错误 | **本需求要解决** |

结论：**不存在按全口径假设消费 `equity` 列的现有业务**。补列是纯增量，不破坏任何现行消费者。

## 3. 需求（R1–R5）

- **R1 补列**：`financial_facts`、`financial_core_facts_hot`、
  `financial_core_facts_history` 增加 `equity_total REAL`、`minority_equity REAL`。
  通过既有 `_migrate_tables`/`_ensure_column` 迁移（research/storage.py:12208），
  CREATE TABLE 语句同步更新。`financial_facts` 如有 history 镜像表一并处理。
- **R2 回填**：从 `financial_numeric_facts_hot`/`_history` 按
  `(instrument_id, report_period)` 回填全部历史报告期。Q3 已验证可行性 100%。
- **R3 写入路径**：构造宽表行的服务在写入时同步填充两新列（数值层已有值，
  不新增抓取）。行内自洽性：`equity_total ≈ equity + minority_equity`（容差）。
- **R4 恒等式校验**：入库校验 `total_assets ≈ total_liabilities + equity_total`
  （相对阈值建议 1%），超限记 warning 日志并计入 ingestion metadata；
  不阻断落库（金融类报表结构差异可解释的除外）。目的：让真正的解析错误
  能被发现，而不是靠人工抽查。
- **R5 口径文档**：在本文件与 AGENTS 相关口径说明中声明：
  - `equity`（及其别名 `eq`）＝ 归母权益（parent_attributable_equity），服务 BPS/剩余收益等；
  - 恒等式必须用 `equity_total`；
  - **不改名** `equity`（别名注册表、API 消费方、字典键的稳定性优先）。

### 非目标

- 不修改既有比率（`yoy_equity`、`liability_to_asset` 等）的分子口径；
- 不把 `equity` 改为全口径（破坏性变更，无必要）；
- 不建设通用数据质量平台（R4 只做本路径必要校验）。

## 4. 实施与验收

阶段（建议单卡完成，vertical slice）：

1. schema 迁移 + CREATE TABLE 更新 + 单测（迁移幂等、新列可写）；
2. 写入路径填充 + 单测（新列非空、自洽关系）；
3. 全量回填脚本 + 抽样对账（每期随机 N 家 vs 数值层）；
4. R4 校验 + 超限日志；对全历史跑一遍统计超限率（预期接近 0）；
5. 文档（本文件补验收记录）。

验收标准：

- Q3 2026：三张表 `equity_total` 覆盖率 ≥ 99.9%（与数值层一致）；
- 恒等式（1% 阈值）全市场成立率 ≥ 99%；
- 现有单测全部通过；`bps`、DCF 所需字段行为不变（回归对照）。

## 5. 风险与限制

- 回填期间与调度器并发写同一 DB：安排在非采集高峰执行，或分批小事务；
- `financial_facts` 若被外部脚本 `SELECT *` 消费，新增列为增量字段，兼容；
- 金融类公司报表（银行/保险）少数股东权益语义仍适用，恒等式同样成立（已验证）。

## 6. 验收记录（2026-11-03 实施）

同日按本需求完成 R1–R5 实施：

- **R1/R3 代码**：`research/financial_fact_aliases.py`（别名映射新增
  `equity_total`/`minority_equity`）、`research/providers/base.py`
  （快照字段）、`research/storage.py`（三表 CREATE TABLE、`_migrate_tables`
  增量迁移、INSERT/UPDATE SQL、派生 `supported_fields`、
  `_warn_balance_identity_mismatch` 恒等式告警）。
- **R2 回填**：`scripts/maintenance/backfill_equity_total_minority.py`
  一次性扫描数值层（hot+history 2,975 万行）建临时聚合表后回填，
  共更新 235,436 行：
  - `financial_facts`：66,134 行，`equity_total` 覆盖 65,862（99.6%）；
  - `financial_core_facts_hot`：66,124 行，覆盖 65,852（99.6%）；
  - `financial_core_facts_history`：10/10 全覆盖。
- **R4 校验**：`financial_facts` 恒等式（1% 阈值）通过率
  **99.96%**（65,818/65,845），原 31%"不匹配"问题消除。
- **R5 文档**：即本文件；口径声明见 §1、§2。
- **测试**：新增 `tests/unit/test_research/test_financial_equity_semantics.py`
  5 项全部通过（派生填充、落库持久化、恒等式告警触发/不触发、迁移幂等）；
  存量回归（test_storage 派生/upsert 相关 5 项、test_financial_xbrl_parser
  21 项、vintage 集成 3 项）全部通过。`test_audit_financial_numeric_fact_coverage`
  的 3 项失败经 HEAD 对照确认为既有问题，与本次变更无关。
- 生产 DB 已应用增量列（ALTER）并完成回填；调度器进程重启后新写入路径
  自动携带新列。
