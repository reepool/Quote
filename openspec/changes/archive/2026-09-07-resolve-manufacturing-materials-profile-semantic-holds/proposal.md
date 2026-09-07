## Why

阶段 5 已证明四份制造/材料年报能够通过公共 LLM 网关完成有界 extract/verify 并生成隔离 bundle，但权威运行 `run-stage5-final-four-luna-20260905-f` 仍因主体口径、合法空值、加工量 verify 和同一控制比较列等真实语义问题保持 `hold`。在进入旧语义 reset 之前，必须把这些 hold 转化为可执行、可复验的通用裁决，并让 Gold 与 19 条冻结负例直接核验真实 accepted 输出，而不是由调用方传入布尔结论。

## What Changes

- 冻结 run-f 为问题基线，建立逐报告、逐 scope 的语义裁决记录，禁止从 targeted 运行或 Gold 向权威结果补值。
- 收口四类业务语义：主体依据、宁德时代业务变化合法空、璞泰来 `processing_volume`、中航成飞同一控制比较列。
- 对现有 bounded workflow 做最小修正，使 source-native 复合标签不会被误判为字段错配，并使业务无重大变化能形成有证据的 `not_applicable` coverage。
- 增加固定制造/材料合同的真实 post-run benchmark：24 条 Gold 和 19 条冻结负例必须从已提交 bundle 的 records、dispositions、coverage 与 Evidence 自动评估；不得接受调用方宣称通过。
- 使用新 run ID 先重跑受影响 scope，再在针对性结果通过后执行新的四报告权威运行；保留合法的 `not_disclosed`、`not_applicable` 和有证据的 `unclear`，但任何冻结 blocker 继续使整体 `hold`。
- 生成研究员可逐项核验的中文裁决/画像报告；报告与切片状态按照后续批准的研究验收政策计算，不要求 Gold 字符串全等或真实年报触发全部负例。
- 保持 `production_authorization=not_authorized`，不恢复旧 backfill、scheduler、API、Telegram，不写旧 approved 表，不启动阶段 6 reset 或商品暴露发布。

## Capabilities

### New Capabilities

- `manufacturing-materials-profile-semantic-adjudication`: 定义四报告 hold 的裁决输入、决策记录、针对性重跑、真实 benchmark 和研究切片完成门。

### Modified Capabilities

- `company-profile-bounded-semantic-workflow`: 明确复合 source-native 标签、业务变化合法空和同一控制比较列的 verify 行为。
- `manufacturing-materials-profile-isolated-slice`: 要求 post-run benchmark 从真实已提交输出自动评估，且新的权威四报告运行不得混用历史或 targeted 结果。

## Impact

- 仅修改 `research/company_profile/` 的既有阶段 4/5 权威链、对应测试、阶段 5.5 审核文档和 OpenSpec artifacts。
- 继续使用 `CompanyProfileSemanticService.run_task`、现有公共 LLM gateway、现有 Evidence plan 和隔离 run-bundle store；不增加新的语义循环、解析平台、生产 writer 或数据库 schema。
- 权威基线仍为 `var/company_profile_stage5/20260905/run-stage5-final-four-luna-20260905-f`；任何新运行使用独立 run ID 和不可变目录。

## Archive Outcome (2026-09-07)

- 本 change 交付了四类语义裁决、真实 committed-output Benchmark 和不可拼接运行约束，但没有登记 `research_slice_pass` 或 `research_slice_usable`。
- 后续 `relax-company-profile-research-acceptance-policy` 已取代本 change 最初对 Gold、未触发负例和 `unclear` 主体的过严完成门；归档同步采用后续政策口径。
- 四报告剩余执行与语义阻塞继续由新的收口 change 处理；阶段 6 与生产链保持关闭，`production_authorization=not_authorized`。
