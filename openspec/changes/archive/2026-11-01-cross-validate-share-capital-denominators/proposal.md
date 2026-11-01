## Why

验收 #12 双源对比发现 bps/ocfps 分母（股本）存在数据缺陷。全量 census 与
双锚复验（EPS 反推 + 市值隐含）后的**事实基线**（修正记录见
`add-financial-pit-panel-cross-section` tasks.md §11）：

1. `share_capital_amount` canonical 混有两种来源语义：THS `equity`
   （股本指标，67,037 行，99.35% 数值即真股数，~405 行源侧坏值）与
   `实收资本(或股本)`（**元金额**，面值=1 元时数值恰等于股数，面值≠1 的
   ~98 行不可直接当股数）；
2. 映射表现成的 `source_unit / canonical_unit / unit_multiplier` 三列从未
   被用于区分上述两种单位；
3. 当前面板的过渡机制（fact 与 valuation 偏差 ≥50% 才替换）只能拦灾难级行，
   无法覆盖亚灾难偏差，且替换是静默的——消费方无法知道分母被换过、
   也无法知道各来源是否一致。

用户方向确认：**任何数据源都带股本字段；各源对非 1 元面值的处理方式未知；
因此用多源交叉验证判定股本正确性，存在分歧时以备注标识，不静默修复。**

## What Changes

- 面板分母从"单一 fact + 灾难替换"改为**多锚交叉验证**（详见 design D0/D1）：
  1. `shares_outstanding` canonical（期末股数，L1 后新数据）；
  2. THS `share_capital_amount` fact（期末股数语义，时点语义经 D0-1 前置验证）；
  3. valuation 隐含股本（market_cap/close，2024-04-30 起，仅 D0-2 核实为
     总市值的来源可取值）；
  4. EPS 隐含股本（净利/披露 basic_eps，加权口径，库内披露自洽）；
  5. `实收资本(或股本)` fact（**元金额，仅作校验锚**：一致投同意票、分歧
     永不取值——隐式覆盖非 1 元面值，无需显式面值表；仅全库无股数锚时
     末位兜底并标注面值假设）。
- **共识即通过、分歧即备注（分层）**：以被采纳值为基准，其余**股数锚**
  在带宽内（期末锚 ±5%、EPS 加权锚 ±15%）且实收资本未触发升级 →
  consensus；股数锚任一出带（或实收资本在股数锚<2 时实质校验分歧/
  与出带锚同向）→ 仍取优先序值但输出 `share_validation=discrepancy` +
  `share_candidates`（各锚数值/口径/absent/unit_mismatch 原因），消费方可
  按标记过滤或排查。实收资本单位错位在股数锚齐且一致时仅记
  `unit_mismatch` 注记，不误标状态（面值≠1 公司保护）；
- EPS 锚双条件跳过（净利或 basic_eps 任一近零即失稳）；
- 映射层启用单位三列：THS equity 声明 `source_unit=shares`；实收资本声明
  `source_unit=CNY` 且不标 `approved_for_core`；`MAPPING_VERSION` v5→v6
  走既有 upsert + audit 管道；
- 全期间双锚扫描隔离源侧坏行（canonical 改 `share_capital_suspect`，
  row_version+1，change log 留痕，raw_fact_json 保留证据）；
- aliases 加载断言：股数类 canonical `unit=='shares'`、金额类 `=='CNY'`。

## Capabilities

### Modified Capabilities

- `financial-pit-panel`: 分母解析改为交叉验证 + 标注（替换过渡性
  灾难替换语义；`share_capital_amount` 语义注记与 `shares_outstanding`
  别名归属同步修正）。

### New Capabilities

- `share-capital-canonical-hygiene`: 股本 canonical 的单位声明、坏行隔离
  与映射审计防再发。

## Impact

- Affected code:
  - `research/financial_pit_panel.py`（分母瀑布 → 交叉验证 + 标注字段）
  - `research/financial_source_field_mapping.py`（单位三列 + v6）
  - `research/financial_fact_aliases.py`（"总股本"别名移至 shares_outstanding）
  - `research/storage.py`（隔离迁移的受控写入）
  - tests：交叉验证共识/分歧/近零净利/面值分歧/隔离重跑场景
- Affected storage:
  - `financial_numeric_facts_hot/history`：坏行 canonical 重指（~500 行
    量级，census 全量扫描定案）；不修改数值，raw 证据保留
  - `financial_pit_panel`：新增 `share_validation` / `share_basis` /
    `share_candidates_json` 列（重建时物化）
- Non-goals:
  - 不建显式面值参考表（交叉验证隐式覆盖）；
  - 不新增外部股本采集（东财 F10 总股本等列为可选增强，另行决策）；
  - 不改 AKShare 侧口径（quantwin 拼接口径卡另行推进）。
