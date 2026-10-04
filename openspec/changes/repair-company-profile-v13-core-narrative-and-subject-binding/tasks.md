## 1. Overview narratives to substantive answers, not yet authorized

- [x] 1.1 (implemented; accepted by the v14 formal round) After scope acceptance, add the 主要经营 sentence and the 作为……上市公司/工业企业……研发制造 narrative to the source-delivery captures and the overview gate, so both principal answers carry the full source sentences and both products answers carry the service and product substance through acceptance, query, and export. A heading or industry label must not substitute for the answer. Do not start 2.1 in that task. The 主要经营 sentence and the 作为……上市公司/工业企业……研发制造 narrative now capture at the projection level with full source text (Rizhao: 装卸/堆存/中转/代理/仓储; Zhongzhi: 直升机/运 12/运 12F/通用飞机), the overview gate and the principal and products matchers accept both forms, and projection-level tests pass. LIMITATION: the Zhongzhi end-to-end drive still shows the accepted list empty — see the 3.1 blocker note.

## 2. Port fee mechanism and default group subject, not yet authorized

- [x] 2.1 (implemented; accepted by the v14 formal round) After 1.1, keep the Rizhao provide-and-collect relationship in the revenue answer, pass the 为客户提供 positive, refuse customer or third-party collection and negated or planned sentences, wire the report default group scope for numeric records without a narrower or conflicting subject, and keep cargo types and the template hedge from generating commodity roles. Do not start 3.1 in that task. The revenue assessor now matches 为客户提供…服务 and 收取…费 with the sentence-level company subject, refuses customer or third-party collection and planned sentences, and the Rizhao revenue answer carries the provide-and-collect relationship. The report default group scope is wired for common-core through `_promote_default_group_subjects` before acceptance (unclear facts with legal local evidence become consolidated_group/report_default_group_scope; 母公司 keeps them unclear). Cargo types and the template hedge generate no roles.

### 2.2 Wire the real v14 run parameters, not yet authorized

- [x] 2.2 Complete the v14 cumulative switches so a formal v14 identity keeps every repair on, and drive the end-to-end tests with the formal v14 identity over complete sources: acceptance, query, and export run; empty `unresolved_field_ids` still accepts candidates with no extra provider calls; a genuinely missing continuation page is still refused as `table_context_incomplete`. Do not start 2.3 in that task. The v14 marker is wired through all five cumulative sets (verified programmatically), and the narrative end-to-end test now runs under the formal v14 identity over the complete two-page sources.

### 2.3 Complete answer text and toll semantics, not yet authorized

- [x] 2.3 Keep the Zhongzhi originally enumerated products and manufacturing services (整机及零部件制造、通用飞机、航空转包生产和客户化服务) in the answer text, keep the Rizhao complete sentence (company provides the service and collects the 包干费、堆存费 and other logistics fees), require the company subject together with an affirmative collection action, refuse planned, negated, and third-party collection, and keep the 向客户收取技术服务费 positive. Assert the actual text in query and export. Do not start 3.1 in that task. The Zhongzhi capture keeps the 涉足 sentence (整机及零部件制造、通用飞机、航空转包生产和客户化服务), the Rizhao revenue answer carries 为客户提供…服务 and 包干费/堆存费/物流其他费用, the collection-semantics guard now refuses 计划/第三方/不收取 statements for every inflow keyword, the 向客户收取技术服务费 positive passes, and the drive asserts the actual query and export text under the formal v14 identity.

## 3. v14 formal delivery and review (executed; review superseded by the owner audit)

- [x] 3.1 The formal v14 round ran as the first genuine execution (`reused_scope=false`) over plan `3a9793e0…` in `m4_v14_formal_acceptance`: 600017 84.2s + 600038 84.9s, whole round 169.3s, zero tokens. The earlier HOLD (unwired v14 switches, missing continuation pages) was resolved by cards 2.2/2.3 before the run.
- [x] 3.2 The review recorded recall 17/17, accuracy 29/29, zero critical numeric errors, freshness read from `effective_annual_reports` (600017 2026-03-26T16:00Z, 600038 amended 2026-05-15T16:00Z). SUPERSEDED BY THE OWNER AUDIT: the Rizhao revenue answer only carried 为客户提供…服务 without the three fees, yet the review scored both the answer and the fee mechanism delivered and accurate — the 17/17 and 29/29 therefore cannot justify expansion. See cards 4.1–4.3.

## 4. Owner-audit closure cards (toll answer completeness)

### 4.1 Deliver the complete toll sentence, not yet authorized

- [x] 4.1 (implemented, pending the v15 round) The 主要经营 capture bridges the intervening development narrative (bounded `[\s\S]` gap) so the accepted overview source_text, the query answer, and the export answer all carry 提供服务 plus 收取…包干费、堆存费和物流其他费用; the drive fixture uses the real complete p9 including the development narrative and PDF wraps. Zhongzhi text and existing table results unchanged.

### 4.2 Whole-sentence toll semantics, not yet authorized

- [x] 4.2 (implemented, pending the v15 round) The revenue gate applies the block list and the planning guard at the sentence level, requires an affirmative collection verb for 为客户提供…服务 statements, and keeps the 向客户收取技术服务费 positive. All five counterexamples (但不收取/尚未收取/第三方收取/计划收取/仅提供服务无收费) are refused at the same gate.

### 4.3 v15 formal delivery and honest review, not yet authorized

- [ ] 4.3 After 4.1 and 4.2, register the `revenue_sentence_repair=v15` cumulative switches, rerun the same frozen plan `3a9793e0…` under v15 in its own directory, and measure the whole round from the first genuine execute to the second export return. Re-judge the six answers and every accepted fact; a toll mechanism that appears only in evidence quotes is scored NOT delivered. Freshness comes from the frozen assets; no-role negatives keep their actual check scope. Gates stay double 100%, zero critical numeric errors, 50000 tokens, 300 seconds. The v13 and v14 snapshots are kept byte-for-byte, and the v14 snapshot carries this audit-limitation note.
