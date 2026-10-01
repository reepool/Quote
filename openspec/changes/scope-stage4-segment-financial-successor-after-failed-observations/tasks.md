## 1. Scope review gate

- [x] 1.1 Independent review accepts this successor scope: only `extract_segment_financials`; plan `manufacturing_materials_stage4_segment_financials.2026-10-01.4`; the same four frozen 2025 reports as SF-1, SF-2, and SF-3; isolation directory `replay/20261001/`; and a later source-review that must state SF-1 and SF-2 remain historical rows. The business pass inherits the verified column binding, ten repaired cells, page-139 sections, page-178 formal title, elimination rows, and legal-empty semantics. A local pass requires an independent four-report reread, 100% source recall, 100% source accuracy, critical numeric errors 0, and those boundaries. Artifact hashes alone are not that pass, and 132/132 or 144/144 are not prefilled. Inclusion at 4.1 requires both the business pass and the artifact binding; a failure keeps the failed observation. `replay_segment_financial_research` already accepts an explicit `plan_version`, and its default stays `.2026-09-29.3`. Do not check 2.1 or any later task in this review. Do not change Python, create an enqueue, replay, or source-review, or add a parser repair. Do not reuse `.2026-09-28.1`, `.2026-09-29.2`, or `.2026-09-29.3`. Do not backfill SF-1 or SF-2, and do not rewrite the SF-3 result onto those rows. Do not create an aggregate row, a cross-chapter score, or aggregate `expansion_gates_met=true`. Do not start an operating-quantity successor, restricted promotion, the six-chapter package, scale quality, or production. Independent review accepted these acceptance conditions. Do not start 2.1 in that review

## 2. Implementation, not yet authorized

- [x] 2.1 After 1.1, reuse `replay_segment_financial_research` with explicit plan `.2026-10-01.4` and add directed tests that the inherited boundaries still hold. Do not change the default plan `.2026-09-29.3`. Do not add a parser repair, enqueue, or replay in that task. Do not edit dossier bytes, PDF hashes, SF-1, SF-2, SF-3, or the ledger. The archived dossier paths are readable, the explicit `.4` call stays isolated, and the default remains `.3`. Do not start 3.1 in that task

## 3. Replay and source review, not yet authorized

- [ ] 3.1 After the implementation review, run the controlled four-report replay into `replay/20261001/` with run id `stage4-segment-financials-20261001`. Output `accepted_for_review` only. Do not put recall, accuracy, critical numeric errors, a gate, or a source-review in the bundle
- [ ] 3.2 After replay, independently source-review the four reports. A pass requires the reread, 100% source recall, 100% source accuracy, critical numeric errors 0, and the inherited evidence and coverage boundaries. Derive the denominator from the reread. Do not treat artifact hashes as that pass, and do not prefill 132/132 or 144/144. Bind the review only to this plan's artifact hashes. State that SF-1 and SF-2 remain historical rows and that SF-3 is not rewritten onto them. Do not edit the old ledger or old replays, and do not archive in that review

## 4. Inclusion judgment, not yet authorized

- [ ] 4.1 After 3.2, confirm `.2026-10-01.4` as the current segment-financial inclusion row only when the business pass and the artifact binding both hold. If either fails, keep the failed observation and do not grant inclusion. Stage 4 aggregate stays a separate judgment. Do not start an operating-quantity successor in that task

## 5. Archive, not yet authorized

- [ ] 5.1 After 4.1, archive this successor as its own plan, report set, and chapter. `stage4_aggregate_expansion_gates_met` stays false. Do not backfill SF-1 or SF-2, and do not create aggregate `expansion_gates_met=true`
