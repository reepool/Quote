## 1. Scope review gate

- [ ] 1.1 Independent review accepts this successor scope: only `extract_operating_quantities`; plan `manufacturing_materials_stage4_operating_quantities.2026-10-01.4`; run id `stage4-operating-quantities-20261001`; isolation directory `replay/20261001/`; the same four frozen 2025 reports, PDF hashes, and source pages as OQ-1, OQ-2, and OQ-3; reuse of `replay_operating_quantity_research` with an explicit plan while the default stays `.2026-09-28.3`; a dossier path correction that preserves the archived dossier hashes; and inherited boundaries for 541 GWh, 121 GWh, 661 GWh, separate processing and sales volumes, capacity category, project stage, qualifiers, inventory footnotes, and legal non-disclosure. A later source-review must build its denominator from the reports and must not prefill 38/38. OQ-1 critical numeric errors 1 stays historical. Do not check 2.1 or any later task in this review. Do not change Python, create an enqueue, replay, or source-review, or add an extraction rule. Do not create an aggregate row, a cross-chapter score, or aggregate `expansion_gates_met=true`. Do not start restricted promotion, the six-chapter package, scale quality, or production

## 2. Implementation, not yet authorized

- [ ] 2.1 After 1.1, point the dossier bindings at the archived directory and reuse `replay_operating_quantity_research` with explicit plan `.2026-10-01.4`. Add directed tests for plan passing, four-report binding, dossier readability, and the inherited quantity boundaries. Do not change the default plan `.2026-09-28.3`, dossier bytes, PDF hashes, or extraction rules. Do not enqueue or replay in that task

## 3. Replay and source review, not yet authorized

- [ ] 3.1 After the implementation review, run the controlled four-report replay into `replay/20261001/` with run id `stage4-operating-quantities-20261001`. Output `accepted_for_review` only. Do not put recall, accuracy, critical numeric errors, a gate, or a source-review in the bundle
- [ ] 3.2 After replay, independently source-review the four reports. Build the denominator from the annual reports. A pass requires 100% source recall, 100% source accuracy, critical numeric errors 0, and the inherited boundaries. Do not prefill 38/38, and do not treat artifact hashes as that pass. Do not edit OQ-1, OQ-2, OQ-3, or the ledger

## 4. Inclusion judgment, not yet authorized

- [ ] 4.1 After 3.2, confirm `.2026-10-01.4` as the current operating-quantity inclusion row only when the business pass and the artifact binding both hold. If either fails, keep the failed observation and do not grant inclusion. Stage 4 aggregate stays a separate judgment

## 5. Archive, not yet authorized

- [ ] 5.1 After 4.1, archive this successor as its own plan, report set, and chapter. `stage4_aggregate_expansion_gates_met` stays false. Do not backfill OQ-1 or OQ-2, and do not create aggregate `expansion_gates_met=true`
