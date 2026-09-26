## Context

阶段 3 研究合同已经规定制造/材料研究从至少三份正式年报、至少两家公司开始，并且至少两份报告必须挑战焦点样本。当前 common-core 只对固定的 `600004.SH` / `600006.SH` 做过 `owned_page_facts=v8` + `material_input_facts=v1` 局部验收。那次 9/9 不证明跨公司普适性，也不授权扩大或生产。

历史四报告、六章隔离切片只描述当时的 Stage 5 验证，不作为当前准入合同。阶段 3 的 regime 覆盖已经完成：`302132.SZ` 2025 是已核验的转型/重大重组样本，研究索引记为 covered，Benchmark 为 3 stable + 1 restructuring，阶段 3 状态为 approved。本 change 有意只使用 `300750.SZ`、`603659.SH`、`920015.BJ` 作为当前最小竖切样本。`302132.SZ` 保留为阶段 3 历史 regime 基线，不纳入本卡定义样本。本 change 不重新评估或实现 `extract_business_regime`，也不得把这三份样本的结果外推为 regime 能力已被重新验证。

## Goals / Non-Goals

**Goals:**

- 把跨样本最小竖切的范围写成可审核合同，并停在 1.1。
- 样本下限使用 `300750.SZ` 2025、`603659.SH` 2025、`920015.BJ` 2025。它们分别位于 SZSE、SSE、BSE，是本次最小竖切下限，不是阶段 3 的全部四份样本，也不是当前固定两家公司。
- 三份独立 dossier 已经写完。最小竖切选定为 `extract_material_inputs`。产销量、客户供应商和 `extract_business_regime` 保持关闭。
- 后续实现复用 Evidence、Stage 5 `extract` / `repair` / `verify` 和研究隔离 bundle，输出只到 `accepted_for_review`。

**Non-Goals:**

- 1.1 通过前不改 Python，不改当前 identity，不入队、不 replay、不写 checkpoint。
- 不改 publication、closure、completed mode，不启动下一轮扩大。
- 不启用完整六章行业包、DCF、交易、旧 writer 或 common-core production。
- 不为单家公司、单页或单个物料写硬编码规则。
- 不预设 recall、accuracy 或扩大门槛通过。
- 不把 `600004.SH` / `600006.SH` 再当成这张卡的定义样本。
- 不把 `302132.SZ` 纳入本卡定义样本，不选择 `extract_business_regime`，不声称重新验证 regime。

## Decisions

1. **三份报告是本次竖切下限，不是阶段 3 全部样本。**
   `300750.SZ`、`603659.SH`、`920015.BJ` 满足三份报告、至少两家公司、至少两份挑战报告和三个交易所。阶段 3 另有已核验的 `302132.SZ` regime 基线，本 change 不把它并入定义样本，也不另挑发行人。dossier 必须自行记录披露形态和业务模式；如果这三份报告的披露形态或业务模式仍然过窄，记覆盖缺口，不假装已经覆盖。

2. **dossier 先于字段义务和竖切选择。**
   每份报告独立记录业务概述来源、章节任务、候选字段、主体、期间、来源单位、物理页锚、Evidence、合法空值、extraction failure 和未决问题。只在一份报告里出现的字段保持 subtype-specific、conditional、optional 或 unresolved。`required` 只表示必须检查，不表示必然披露。合法未披露不是 extraction failure。

3. **最小竖切选定为 `extract_material_inputs`。**
   三份 dossier 都有公司自身的具名材料投入，而且形态不同，足够脱离 `600006.SH` 检验现有材料角色。`300750.SZ` 物理页 40 写明生产经营所需主要原材料为正极材料、负极材料、隔膜和电解液；同报告另有产品角色重叠和“直接材料”成本。`603659.SH` 物理页 33 按业务线写明焦类、初级石墨、沥青、隔膜基膜、陶瓷材料、氧化铝、氢氧化铝、钢材和机加工件；同报告另有直接材料成本和存货原材料金额。`920015.BJ` 物理页 26 写明丁酮、双氧水、液氨、一甲基三氯硅烷、乙烯基三氯硅烷、乙醛等主要原材料；同报告另有丁酮肟委外加工、原材料存货金额，以及没有采购数量。
   因此只打开 `extract_material_inputs`。`extract_business_regime`、产销量、客户供应商和其他章节保持关闭。选择来自 dossier，不靠行业常识补名称，也不外推为 regime 能力已被重新验证。历史六章包不因此重新启用。

4. **研究隔离，不进 common-core production。**
   实现阶段复用现有 Evidence 准备、`CompanyProfileSemanticService` 的 extract/repair/verify，以及研究隔离 bundle。候选只到 `accepted_for_review`。Gold 只用于事后核对，不回填运行时字段。当前 `owned_page_facts=v8` + `material_input_facts=v1` identity 保持不变。

5. **1.1 通过后才允许实现和 focused replay。**
   通过标准是样本多样性、单一竖切规则、字段边界、研究隔离和后续验证方式被独立 Review 接受。focused replay 只覆盖已选报告和已选章节，并单独重算指标。门槛结果由正式模型派生，不在本设计里预填。

## Risks / Trade-offs

- [三份报告的披露形态仍然相近] → dossier 记录缺口；不把单一形态提升为全行业 required。
- [材料句被销售、泛称成本或委外加工带偏] → 只有公司自身的具名投入句成立；销售证据单独不生成投入角色；直接材料成本和原材料存货金额拒绝；丁酮肟委外加工不记成材料投入。没有数量仍可交付。
- [把三份稳定样本说成重新验证了 regime] → `302132.SZ` 只保留为阶段 3 历史基线；本卡不重新评估 regime。
- [把局部 9/9 当成普适性] → 固定两家公司的结果只作历史观察，不进入本样本，也不外推。
- [研究输出漏进 common-core] → 隔离 bundle 与 `accepted_for_review` 是范围硬边界；publication 和 identity 不改。
- [用硬编码补某一家的物料或页码] → 规则必须来自跨样本 dossier，单页或单物料特例拒绝。

## Migration Plan

1. 1.1 范围审核和 2.1 三份 dossier 已完成。
2. 2.2 只记录 `extract_material_inputs` 的选择和边界，不实现代码。
3. 2.2 审核通过后才做 2.3 最小研究实现。
4. 实现审核通过后才做 focused replay，并单独重算指标。
5. 回滚本 change 只删除这份范围文档；不删除已有 common-core work。

## Open Questions

- 无。最小竖切已定为 `extract_material_inputs`。合法未披露、unclear 和 extraction failure 的边界见 spec，不在实现前另选章节。
