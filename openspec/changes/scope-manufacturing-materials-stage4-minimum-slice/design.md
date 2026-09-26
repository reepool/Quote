## Context

阶段 3 研究合同已经规定制造/材料研究从至少三份正式年报、至少两家公司开始，并且至少两份报告必须挑战焦点样本。当前 common-core 只对固定的 `600004.SH` / `600006.SH` 做过 `owned_page_facts=v8` + `material_input_facts=v1` 局部验收。那次 9/9 不证明跨公司普适性，也不授权扩大或生产。

历史四报告、六章隔离切片只描述当时的 Stage 5 验证，不作为当前准入合同。阶段 3 的 regime 覆盖已经完成：`302132.SZ` 2025 是已核验的转型/重大重组样本，研究索引记为 covered，Benchmark 为 3 stable + 1 restructuring，阶段 3 状态为 approved。本 change 有意只使用 `300750.SZ`、`603659.SH`、`920015.BJ` 作为当前最小竖切样本。`302132.SZ` 保留为阶段 3 历史 regime 基线，不纳入本卡定义样本。本 change 不重新评估或实现 `extract_business_regime`，也不得把这三份样本的结果外推为 regime 能力已被重新验证。

## Goals / Non-Goals

**Goals:**

- 把跨样本最小竖切的范围写成可审核合同，并停在 1.1。
- 样本下限使用 `300750.SZ` 2025、`603659.SH` 2025、`920015.BJ` 2025。它们分别位于 SZSE、SSE、BSE，是本次最小竖切下限，不是阶段 3 的全部四份样本，也不是当前固定两家公司。
- 先为每份报告写独立 dossier，再决定字段义务，最后只选一个已有章节任务。
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

3. **只选一个已有章节任务，且不选 regime。**
   dossier 完成后，从现有章节任务里选一个最小竖切，但不得选择 `extract_business_regime`。选择必须有至少两家不同公司的 dossier 支持，并且不能靠行业常识补名称。未入选章节保持关闭。三份样本上的结果不得外推为 regime 能力已被重新验证。历史六章包不因此重新启用。

4. **研究隔离，不进 common-core production。**
   实现阶段复用现有 Evidence 准备、`CompanyProfileSemanticService` 的 extract/repair/verify，以及研究隔离 bundle。候选只到 `accepted_for_review`。Gold 只用于事后核对，不回填运行时字段。当前 `owned_page_facts=v8` + `material_input_facts=v1` identity 保持不变。

5. **1.1 通过后才允许实现和 focused replay。**
   通过标准是样本多样性、单一竖切规则、字段边界、研究隔离和后续验证方式被独立 Review 接受。focused replay 只覆盖已选报告和已选章节，并单独重算指标。门槛结果由正式模型派生，不在本设计里预填。

## Risks / Trade-offs

- [三份报告的披露形态仍然相近] → dossier 记录缺口；不把单一形态提升为全行业 required。
- [过早选定竖切] → 1.1 只接受选择规则；具体章节任务在三份 dossier 完成后才命名，且不得是 `extract_business_regime`。
- [把三份稳定样本说成重新验证了 regime] → `302132.SZ` 只保留为阶段 3 历史基线；本卡不重新评估 regime。
- [把局部 9/9 当成普适性] → 固定两家公司的结果只作历史观察，不进入本样本，也不外推。
- [研究输出漏进 common-core] → 隔离 bundle 与 `accepted_for_review` 是范围硬边界；publication 和 identity 不改。
- [用硬编码补某一家的物料或页码] → 规则必须来自跨样本 dossier，单页或单物料特例拒绝。

## Migration Plan

1. 完成 proposal、design、spec、tasks，全部任务保持未勾选。
2. 停在 1.1 独立范围审核。
3. 审核通过后才写 dossier、命名一个竖切并实现。
4. 实现审核通过后才做 focused replay 和独立复核。
5. 回滚本 change 只删除这份范围文档；不删除已有 common-core work。

## Open Questions

- 无。具体竖切章节留到 dossier 之后，不在 1.1 之前指定。
