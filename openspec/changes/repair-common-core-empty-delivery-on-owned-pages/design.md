## Context

2026-09-17 有界 `company_profile_common_core` 对 302132.SZ、600000.SH 完成了首次真实 run。独立核原文后 `company_profile_source_review.v1` 记录：source recall 0/7，accuracy unassessed，`expansion_gates_met=false`。生产仍为 `not_authorized`。

现场两条抽取缺陷：

1. 中航成飞已定位「报告期内公司从事的主要业务」摘录（p11–12）和分行业收入表，scope 里已有官方原文，但 `provider=None` 时 workflow 写 `provider-unavailable`，`records=[]`。
2. 浦发银行官方有独立标题「3.6 公司主要业务情况」，以及公司简介同行字段 `经营范围 银行业务；证券投资基金托管；公募证券投资基金销售；经批准的其它业务。`。heading index 打到了 `主要业务`，但 `_owned_heading` 要求去前缀后整行等于标题或标题后仅有标点；「公司主要业务情况」对不上「主要业务」，「经营范围 银行业务；…」也不是独立标题，于是 `chapter_missing`。

现行重跑缺口：

- `default_processing_identity()` 仍是 `{"rules": "company_profile_common_core.v1"}`。同样本再 run 会 `reused=1` 并直接完成，provider/runtime 不再执行。
- 已落盘 scope 回执按 instrument / period / chapter + `source_digest` + mapping policy 复用，不含 processing identity。后继 work 若只换队列项，仍会吃到空的 `provider-unavailable` scope。
- `CompanyProfileReadService._record_sort_key` 在同一报告上用 `work_id` 字典序决胜。新 JSON 若 `work_id` 更小，query 仍会返回旧空画像。

现行 owner 已能选 Evidence。本 change 要补：摘录收成事实、银行标题与经营范围同行值、受控 successor replay、query 以当前 identity 为准。

## Goals / Non-Goals

**Goals:**

- 官方已定位、已可读、已写明字段的 overview/segment 摘录，不依赖 LLM 收成 accepted 事实。
- 银行/服务年报的独立标题「公司主要业务情况」能定位 overview；公司简介「经营范围」按字段标签 + 同行值拥有。
- 发布与空交付不同的 processing identity，对 302132.SZ 与 600000.SH 做受控 successor replay，验证旧空交付变为 query 可见的 accepted facts。
- query 对同一报告优先返回当前 identity 的 successor，不依赖 `work_id` 碰巧更大。
- 仍走 `operations.py` → runtime → `company_profile_research_writer.v1` / `company_profile_read_service.v1`。
- 受控重跑并核原文后，至少能评 source accuracy，不再因空交付把 recall 打成 0。

**Non-Goals:**

- 不启用生产、DCF、交易、旧 writer。
- 不启动 M4，不回填申万 L1，不把浦发从 `other` 改成 `finance`。
- 不新开 PDF/OCR/版面恢复，不另建采集循环或语义平台。
- 不把本次观察外推为规模质量。
- 不删业务库、不把手工删除旧 JSON 当作 replay。
- 不采用「原 work_id 原地覆盖」：那会破坏已锁定的 completed-work reuse 合同。

## Decisions

1. **确定性投影挂在已有 Evidence 选择之后，不另建 writer。**
   `select_core_evidence` 已经产出 owned span 和 PreparedEvidence。新增最小投影：从 excerpt 生成 `BusinessOverview` / `Activity` / `Segment` / `Measurement`，作为 `deterministic_candidates` 进入现有 semantic service。
   备选是把空交付标成 failed：与「合格事实逐条交付、缺失不封锁」冲突，且会掩盖已定位摘录。

2. **只投影摘录已写明的字段，不猜。**
   overview 用已有 `_PRINCIPAL_PATTERN` / `_PRODUCT_PATTERN` / 收入句式；segment 只收带标签和金额的分行业/分产品行。投影不出的字段才进 LLM；`provider=None` 时这些字段保持 uncovered，不得写成 provider-unavailable。

3. **标题分两类：独立章节标题，以及公司简介字段标签 + 同行值。**
   独立标题增加「公司主要业务情况」「公司金融业务」。去前缀后必须整行等于标题，或标题后仅有全角/半角标点。
   「经营范围」是公司简介字段标签，不是独立标题。官方浦发 p22 为同一行：`经营范围` + 空白 + 业务值。匹配必须允许标签后直接跟值，并拥有该行值作为 overview span。
   目录页、带「……」或末尾页码的目录行、以及正文中间偶然出现的「经营范围 / 主要业务」不得拥有。业务值不得为空或仅标点。

4. **修复走 successor，不原地替换。**
   发布新 identity，例如 `{"rules": "company_profile_common_core.v1", "owned_page_facts": "v1"}`，与空交付的 `{"rules": "company_profile_common_core.v1"}` 区分。
   已有 enqueue 在 identity 变更时 insert 新 work 并 identity-supersede 旧项。本 change 必须使用这条路径。
   空的 predecessor scope 回执不得被 successor 当完成结果复用。scope 复用必须同时匹配当前 processing identity；或对无 accepted records / `provider-unavailable` 的回执拒绝复用。
   旧空 JSON 可留在命名空间里作为 predecessor。query 必须在同一 `instrument_id` + 同一 `report_id` / `document_version` 上优先选当前 published identity 的记录。`work_id` 只允许在 identity 相同的记录之间做最后决胜。验收必须构造 predecessor `work_id` 字典序大于 successor 的夹具，证明 query 仍返回 successor 的 accepted facts。

5. **验收样本固定为本次观察的两家，不扩样本。**
   修复证明用 302132.SZ 与 600000.SH 的官方页文本或已落盘 page artifact，不联网、不调 LLM。受控 replay 必须看到 enqueue `inserted >= 1` 且对这两家不是 `reused` completed 空结果，然后 query 读到新事实。

## Risks / Trade-offs

- [过宽标题匹配误收目录或无关段] → 独立标题仍须是 title line；经营范围只认行首字段标签 + 非空同行值；保留 TOC 排除。
- [投影把合计行当成收入模型] → 继续排除合计/抵消/无标签总额。
- [空核心仍标 completed] → 本 change 不改任务完成语义；质量看 accepted facts、query 和核原文。
- [successor 仍复用空 scope] → scope 复用绑定 processing identity，或拒绝复用空回执。
- [只修两家形态] → 明确不覆盖申万分层；不够再另开 change。

## Migration Plan

1. 发布新 processing identity，不改 job id、不改生产开关、不删库。
2. 对 302132.SZ、600000.SH 走发布入口受控 enqueue + drain；旧空 work 被 identity-supersede，新 work 执行投影。
3. query 这两家必须返回 successor 的 accepted facts。
4. 独立核原文；4.1 数值门槛仍未全部通过则不得讨论 M4。
5. 回滚即停用新 identity 与投影/标题规则；旧空交付仍可读但不再是当前 identity 的权威结果。

## Open Questions

无。申万 `sw_l1_name` 为空导致银行进 `other` 已记录，不在本 change 关闭。
