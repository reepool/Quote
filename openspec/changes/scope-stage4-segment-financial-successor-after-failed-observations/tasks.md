## 1. Scope review gate

- [ ] 1.1 Independent review accepts this successor scope: only `extract_segment_financials`; plan `manufacturing_materials_stage4_segment_financials.2026-10-01.4`; the same four frozen 2025 reports as SF-1, SF-2, and SF-3; isolation directory `replay/20261001/`; and a later source-review that must state SF-1 and SF-2 remain historical rows. Do not check 2.1 or any later task in this review. Do not change Python, create an enqueue, replay, or source-review, or prefill recall, accuracy, critical numeric errors, or artifact hashes. Do not reuse `.2026-09-28.1`, `.2026-09-29.2`, or `.2026-09-29.3`. Do not backfill SF-1 or SF-2, and do not rewrite the SF-3 132/132 and 144/144 result onto those rows. Do not create an aggregate row, a cross-chapter score, or aggregate `expansion_gates_met=true`. Do not start an operating-quantity successor, restricted promotion, the six-chapter package, scale quality, or production

## 2. Implementation, not yet authorized

- [ ] 2.1 After 1.1, implement the minimum `.2026-10-01.4` successor on the existing segment-financial replay path and add directed tests. Do not retarget the `.2026-09-29.3` default replay. Do not enqueue or replay in that task, and do not edit SF-1, SF-2, SF-3, or the ledger

## 3. Replay and source review, not yet authorized

- [ ] 3.1 After the implementation review, run the controlled four-report replay into `replay/20261001/` with run id `stage4-segment-financials-20261001`. Output `accepted_for_review` only. Do not put recall, accuracy, critical numeric errors, a gate, or a source-review in the bundle
- [ ] 3.2 After replay, independently source-review the four reports. Derive recall, accuracy, critical numeric errors, and the gate from the reread. Bind the review only to this plan's artifact hashes. State that SF-1 and SF-2 remain historical rows and that SF-3 is not rewritten onto them. Do not edit the old ledger or old replays, and do not archive in that review

## 4. Inclusion judgment, not yet authorized

- [ ] 4.1 After the source-review passes, separately judge whether `.2026-10-01.4` can be the current segment-financial inclusion row. A local pass MUST stay inside this plan, these four reports, and `extract_segment_financials`. Do not restore the Stage 4 aggregate, and do not start an operating-quantity successor in that task

## 5. Archive, not yet authorized

- [ ] 5.1 After 4.1, archive this successor as its own plan, report set, and chapter. `stage4_aggregate_expansion_gates_met` stays false. Do not backfill SF-1 or SF-2, and do not create aggregate `expansion_gates_met=true`
