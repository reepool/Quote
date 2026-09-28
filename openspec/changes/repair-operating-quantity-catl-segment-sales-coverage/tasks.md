## 1. Scope review gate

- [ ] 1.1 Independent review accepts this repair contract: only `300750.SZ` 2025 pages 21 and 22, only the existing `extract_operating_quantities` chapter, no new report, no resample, and no regime review. Power-battery sales of 541 GWh and storage-battery sales of 121 GWh become separate sales-volume facts, each with its own page, object, bounded quote, evidence, report identity, and page text hash. The two values are not added into 662 and do not replace the operating-table sales of 661 GWh. The 36 facts and 13 coverage rows from `replay/20260928.2` stay, and that replay is not rewritten. The other three reports are regression samples only. Successor output stays `accepted_for_review` in a new isolation directory under plan `manufacturing_materials_stage4_operating_quantities.2026-09-28.3`. Do not check 1.1 until that review. Do not change Python, enqueue, replay, common-core, publication, closure, mode, or production authorization, and do not preset recall, accuracy, critical numeric errors, or the gate

## 2. Minimum repair, not yet authorized

- [ ] 2.1 After 1.1, add the smallest implementation and tests for the two segment sales facts. Keep the operating-table 661 GWh row, the other three reports, the previously accurate facts, and the 13 coverage statuses unchanged. Do not hard-code the instrument, page, or product name in runtime. Do not start the successor replay in this task

## 3. Successor replay and recount, not yet authorized

- [ ] 3.1 After the implementation review, replay only `extract_operating_quantities` into a new research isolation directory with plan `manufacturing_materials_stage4_operating_quantities.2026-09-28.3`. Do not write into `replay/20260928.2` or `replay/20260928`
- [ ] 3.2 Independently reread the successor output and recompute source recall, source accuracy, critical numeric errors, and `expansion_gates_met`. Do not presume they pass, and do not reuse 36/38, 36/36, 26/36, 26/27, 23/23, 19/23, or 9/9
- [ ] 3.3 Do not close out or archive this change, and do not start another expansion, a scale-quality claim, production, the six-chapter package, counterparties, regime, DCF, trading, or price sensitivity until the new source review passes
