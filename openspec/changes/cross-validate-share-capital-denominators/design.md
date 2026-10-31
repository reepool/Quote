## Context

前序 change `add-financial-pit-panel-cross-section` 交付的面板以
`share_capital_amount` fact 为 bps/ocfps 分母。验收 #12 预跑暴露缺陷，
三轮归因有一次反复（详见该 change tasks.md §11 修正记录）：THS `equity`
语义上是股本且 99.35% 正确，真实问题是**行级坏值（~405 行）+ canonical
未区分股数/元金额（实收资本，面值≠1 的 ~98 行）+ 过渡机制静默替换不可审计**。

用户决策方向：多源交叉验证判定正确性，分歧以备注标识，不静默修复。

## Goals / Non-Goals

**Goals:**

- 每个 (instrument, period) 的分母可审计：用了哪个值、各锚是否一致；
- 源侧坏行隔离且可追溯（canonical 层面，不改数值）；
- 非 1 元面值无需显式面值表（实收资本与其他锚 diverge 即被标记/降权）；
- 映射层从此用单位声明区分来源语义，审计管道有硬校验。

**Non-Goals:**

- 不追求与 AKShare bps 完全一致（其分子扣永续债等口径差归 quantwin 口径卡）；
- 不建面值参考表、不新增外部采集（可选增强另立卡）。

## Decisions

### D1 四锚交叉验证（替代灾难替换）

| 锚 | 口径 | 覆盖 | 特性 |
|---|---|---|---|
| THS share_capital_amount | 期末股数 | 全期 | 99.35% 正确；坏行靠本机制检出 |
| 实收资本(或股本) | 元金额 | 16,623 行 | 仅当与其他锚一致时可当股数（隐式面值校验） |
| valuation 隐含（mcap/close） | 期末股数 | 2024-04-30+ | 其自身质量由本机制一并评估（锚之一，非真值） |
| EPS 隐含（净利/披露eps） | 加权股数 | 披露行 | 库内自洽、无外部依赖；|净利|过小时跳过 |

**规则**：
- 组装可用锚集合（缺失锚记 absent，不参与）；
- 带宽：股数锚间 ±5%；EPS 加权锚对期末锚 ±15%（加权/期末自然漂移带，
  实测集群样本 0.98–1.02 支持）；
- 全部两两一致 → `consensus`，取优先序第一（THS > 实收资本 > valuation）；
- 任一分歧 → `discrepancy`，仍取优先序第一可用值，但
  `share_candidates` 记录各锚数值+口径+absent 原因；
- 仅一锚可用 → `single_source`（无验证，如实标注）；
- 零锚 → 分母 null（bps/ocfps null）。

### D2 备注（而非静默修复）是第一公民

面板行新增三列：`share_validation`（consensus/discrepancy/single_source/null）、
`share_basis`（最终采用锚名）、`share_candidates_json`（分歧明细）。API 原样
透出，coverage 增加 `share_validation` 分布。量化消费方（quantwin）可按
`discrepancy` 过滤或排查——这正是用户要求的"差异用备注标识"。

### D3 坏行隔离（数据层，一次迁移）

- 判定：交叉验证 `discrepancy` 且 THS 锚与其他 ≥2 锚背离 >50% → THS 行
  判坏；全期间扫描（含 2024 前，该期无 valuation 锚时用 EPS+实收资本双锚）；
- 处置：canonical → `share_capital_suspect`（隔离名，数值不动），
  row_version+1，data_change_log 留痕；迁移脚本前后行数对账 + 抽样复核；
- 迁移后 THS 锚装配自然跳过 suspect 行，交叉验证自动降级为剩余锚。

### D4 映射层单位机制与防再发

- `financial_source_field_mapping.py`：THS equity 行补 `source_unit=shares`；
  实收资产行 `source_unit=CNY`、`approved_for_core=false`；v5→v6；
- `financial_fact_aliases.py`："总股本"别名移入 `shares_outstanding`
  （unit=shares，canonical 已存在）；`share_capital_amount` 语义注记
  "THS 股数 / 实收资本元金额混合来源，须经交叉验证"；
- aliases 加载断言：股数类 unit 必须 `shares`，金额类必须 `CNY`
  ——新映射批准时违反即拒绝（轻量断言，不建平台）。

## Risks / Trade-offs

| 风险 | 处置 |
|---|---|
| EPS 锚在亏损/微利季度失真 | |净利| 低于阈值（如 <营收 0.5%）时跳过该锚 |
| valuation 锚质量未定案 | 作为锚参与而非真值；其分歧率本身是该序列的质量度量，产出后回填文档 |
| 隔离误伤（源对、两锚齐错） | >50% 背离 + 至少 2 锚 + 抽样人工复核；suspect 行可逆（canonical 改回即恢复） |
| 2024 前仅 2-3 锚 | 如实标 single_source/两锚共识，不冒充强验证 |
| bps 与 AK 残余差异 | 口径差（永续债等）归 quantwin 卡，本 change 不追 |

## 验收（摘要，细则见 tasks）

1. 双锚扫描复跑：suspect 行重扫归零（无新检出）；
2. 面板重建后 `share_validation` 分布合理（consensus 为主，discrepancy
   占比 <5% 预期，如实报告）；
3. #12 复跑：bps/ocfps 差异率较 v5 基线（10.07%/9.74%）下降或差异可逐股
   归因至口径（永续债清单核对）；
4. 单测覆盖：共识/分歧/单锚/零锚/近零净利跳过/实收资本面值分歧/suspect
   隔离后降级；
5. 回归：映射管道与同步链路测试全绿。
