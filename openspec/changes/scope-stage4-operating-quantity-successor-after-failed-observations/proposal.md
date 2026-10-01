## Why

产销量要重新进入 aggregate 判断，必须另立一份新的独立观察。OQ-3 `.2026-09-28.3` 的局部 38/38 不能清除 OQ-1 的 critical numeric error 1，也不能回填 OQ-2。缺的是一份只覆盖 `extract_operating_quantities` 的 successor 范围：新计划、同一四报告身份，以及本 change 自己的隔离制品。1.1 通过前不启动 replay。

## What Changes

- 新建 successor 范围。1.1 通过前只写 proposal、design、spec、tasks。
- 只覆盖 `extract_operating_quantities`。计划固定为 `manufacturing_materials_stage4_operating_quantities.2026-10-01.4`。run id 固定为 `stage4-operating-quantities-20261001`。制品只写入本 change 的 `replay/20261001/`。
- 复用 `replay_operating_quantity_research` 的显式 `plan_version`。默认仍是 `.2026-09-28.3`。不新增抽取器，不改抽取规则。
- 四报告身份及 PDF 哈希与 OQ-1、OQ-2、OQ-3 相同；来源页范围继承 OQ-3。OQ-2 才加入璞泰来第 27 页，OQ-3 才加入宁德时代第 21–22 页。历史 enqueue 保持原样。后续最小实现只把 dossier 路径改到已核实的归档目录，不改 dossier 字节。
- 继承已验证边界：541、121、661 GWh 分别保留；加工量与销量分开；产能类别、项目阶段、比较符和库存脚注按原绑定保留；合法未披露和不适用不变成虚构数量。
- 将来的 source-review 从年报建立分母。通过要求 recall 与 accuracy 都是 100%，critical numeric errors 为 0，并核对这些边界。不得预填 38/38。
- OQ-1、OQ-2、OQ-3 和旧 ledger 保持原值。OQ-1 的 critical numeric error 1 不回写。即使 `.4` 以后局部通过，也不能单独恢复 Stage 4 aggregate。

## Capabilities

### New Capabilities

- `scope-stage4-operating-quantity-successor-after-failed-observations`: 定义产销量 successor 的新计划、四报告身份、隔离制品和业务通过条件。1.1 通过前不创建 replay。

### Modified Capabilities

- 无。不改已归档的恢复合同、ledger、OQ-1、OQ-2、OQ-3 或它们的 replay。

## Impact

- 1.1 通过前只增加本 change 的 OpenSpec 范围文档。
- 不改 Python、publication、closure、mode、identity、checkpoint。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。`stage4_aggregate_expansion_gates_met` 保持 false。
