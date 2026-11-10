# Design: 窄化对账候选构建路径

## 1. 问题分析（全部为生产实测，2026-11-08~10）

### 1.1 扫描段成本去向（py-spy 3 次采样 + 代码核对）

| 成本项 | 证据 | 占比估计 |
|---|---|---|
| **官方源合并的宽查询** | 采样 2/3 命中：`_official_cninfo_facts_by_canonical` 用宽方法 `get_numeric_facts`（无投影、无过滤）取回标的全部事实行（570+ × 35 列），Python 逐行做 `_is_cninfo_data20_numeric_fact` 筛选；**每个有缺失字段的候选都触发** | 主导（约 2/3 样本时间） |
| **profile 逐候选解析** | `_candidate_for_period` 每候选调 `_load_profile_industry_membership` + `_load_profile_company_profile`（各一次 DB 读）+ `resolve_financial_statement_profile`；同一标的 11 个报告期重复 11 次；既有批量设施 `resolve_financial_statement_profiles_for_instruments` 未被使用 | 次要 |
| local-core 窄查询 | 采样 1/3 命中，已在上一 change 窄化（~4ms/候选） | 已优化 |
| 审计加载 | 循环前按标的批量预载 ✓ | 已批量化 |

### 1.2 合并读取的筛选语义（必须逐字段保持）

`_is_cninfo_data20_numeric_fact` 将 `source`、`source_mode`、`parser_version` 与 `raw_fact` 解码值拼接成 haystack 后判断是否包含 `cninfo_data20`。SQL 下推等价条件：

```sql
WHERE source LIKE '%cninfo_data20%'
   OR source_mode LIKE '%cninfo_data20%'
   OR parser_version LIKE '%cninfo_data20%'
   OR raw_fact_json LIKE '%cninfo_data20%'
```

（`raw_fact_json` 为 `raw_fact` 的序列化原文，LIKE 命中等价于解码值命中。）M1 的等价性双跑必须覆盖此谓词边界（source 命中、仅 raw_fact 命中、均不命中的样本）。

## 2. 目标与非目标

**目标**

1. 官方源合并读取每候选取回行数从 570+ 降至个位数量级，列投影仅含消费字段（canonical_fact_name、fact_value、unit、report_period、fact_name、updated_at、source、source_mode、parser_version、raw_fact_json）；
2. 同一标的的 profile 解析在一次扫描中只执行一次（11 期共享）；
3. 扫描段 **≤60 分钟**（61,284 候选，即 ≤0.06s/候选）；周日全量总时长 **≤2.5 小时**；
4. 连续两个周日达标后 `max_runtime_seconds` 43200 → 18000 回调；
5. 等价性：冻结样本（含新增"仅官方源有数"形态）双跑 100% 一致。

**非目标**

- 不做按标的批量就绪（L5 维持条件项）；不处理 CNInfo 限流；不动写路径与判定语义；不拆 77GB 库。

## 3. 方案

| 杠杆 | 内容 | 决策 |
|---|---|---|
| N1 合并读取窄化 | 复用 `get_financial_core_fact_readiness_rows` 设施：扩展可选参数（列集、无 canonical 过滤 + source 谓词），或同级新增窄方法共享同一 SQL 构造器——**不出现第三套独立事实查询实现** | **做**（证据主导项） |
| N2 profile 按标的去重 | 扫描循环按 instrument 建字典：首个候选解析后复用；优先改造为调用既有 `resolve_financial_statement_profiles_for_instruments` 批量设施（消灭同功能双实现） | **做** |
| N3 按标的批量就绪 | 一次查询覆盖标的全部 11 期 | 条件项（N1+N2 达标即不做） |

## 4. 等价性策略

- 复用 `scripts/research_financial_readiness_perf.py` 的 equivalence 基架，样本集扩充：**仅官方源有数、本地无数**的标的（生产存在则纳入，否则 fixture 构造）+ 谓词边界样本（§1.2 三形态）；
- 双跑对比 `_merge_official_cninfo_readiness` 的输出（facts/missing_fields/ready 全字段）；
- 黄金基线 = 现网宽实现输出，存档入库。

## 5. 生产不变量

1. 就绪判定与官方源合并结果 100% 等价（双跑证明）；
2. 写路径、任务报告契约、检查点/心跳协议零变化；
3. 宽方法 `get_financial_numeric_facts` 保持原样（其他调用方零影响）；
4. profile 解析结果与逐候选拟合一致（同一解析函数，仅去重执行次数）。

## 6. 风险

| 风险 | 缓解 |
|---|---|
| LIKE 谓词语义与 haystack 不完全等价（转义/大小写） | M1 双跑覆盖三形态边界样本；不等价则退化为"SQL 预筛 + Python 终判"双层（仍消除 95%+ 行） |
| N2 批量设施与逐候选解析行为差异 | M1 先对同一标的集比对两种解析结果一致后才切换 |
| 扫描速率仍不达标（剩余成本在别处） | M1 调用分解工具先行（复用 --decompose），不达标启动 N3 |

## 7. 验证

1. M1 基线：分解扫描段每候选调用（宽合并次数/行数、profile 解析次数），黄金基线存档；
2. 等价性双跑（§4）0 不一致为合入门槛；
3. 副本/夜间窗口全量 dry-run：扫描段 ≤60 分钟（心跳 `scan_rate_per_sec` ≥17）；
4. 生产验收：连续两个周日自然调度总时长 ≤2.5h → 回调 18000s；
5. 全量既有单测 + 新增谓词/去重单测。
