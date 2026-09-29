## 1. Scope review gate

- [x] 1.1 Independent review accepts this aggregate-gate contract as hold-only: four named chapters and plan versions, one four-report set covering SZSE, SSE, and BSE, per-plan source-review and artifact binding, retention of the historical failures and critical numeric errors, and no backfill or cross-chapter sum. Plan `manufacturing_materials_stage4_material_inputs.2026-09-26.2` has only three reports, so it cannot satisfy the four-report set that includes `302132.SZ`. That incompatibility makes the current aggregate impossible to pass. Do not add `302132.SZ` to the three-report result, and do not replace it with another chapter's four-report result. Do not check 2.1 or 2.2 in this review, and do not record the 2.2 hold yet. Do not change Python, enqueue, replay, archived artifacts, the Stage 4 ledger, publication, closure, mode, identity, or checkpoint. Do not prefill an aggregate gate or a cross-chapter metric. Do not start the six-chapter package, scale quality, production, or a restricted-promotion implementation

## 2. Freeze and judgment, not yet authorized

- [x] 2.1 After 1.1, record the frozen chapters, plan versions, four-report set, exchange coverage, and failure-retention rules from this contract. Copy bindings from the existing ledger. Do not recompute the nine historical observations or edit archived files. The copy is `contract-binding.md`. Material-input `.2` stays a three-report row. This task does not record the 2.2 hold. Independent review accepted the binding. Do not start 2.2 in that review
- [x] 2.2 After 2.1, apply this contract to that ledger. If any condition fails, record hold. Do not invent aggregate `expansion_gates_met=true` or a cross-chapter score, and do not authorize restricted-promotion design in that task. The application is in design.md under "Contract application": hold, because MI-2 is a three-report row and the aggregate set includes `302132.SZ`. `contract-binding.md` is unchanged. Independent review accepted this hold. Do not archive in that review

## 3. Archive, not yet authorized

- [ ] 3.1 After 2.2, keep the conclusion and archive this read-only contract review. Do not create a successor replay or a promotion implementation
