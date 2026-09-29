## 1. Scope review gate

- [x] 1.1 Independent review accepts this repair contract: only `extract_segment_financials`, only the original four reports, the page-139 printed sections `地区分部` and `业务分部`, the page-178 printed title `报告分部的财务信息`, and successor plan `manufacturing_materials_stage4_segment_financials.2026-09-29.3`. Same amounts under different printed sections stay separate. `分部间抵销` keeps `consolidation_adjustment`. The page-25 column repair, the ten repaired cells, and the empty-margin, empty-elimination, and company-margin boundaries stay in place. Do not check 1.1 until that review. Do not change Python, enqueue, replay, identity, publication, closure, mode, checkpoint, `replay/20260929.2`, or `replay/20260928`, and do not preset recall, accuracy, critical numeric errors, or the gate

## 2. Minimum repair, not yet authorized

- [x] 2.1 After 1.1, bind each footnote table to its printed section title and reset the previous table's section state when the next printed title starts. Keep same-amount totals, ordinary rows, and elimination columns in their own sections. Do not hard-code a security code, page number, company name, or product name in the runtime extractor
- [x] 2.2 Add directed tests for the two page-139 sections, the page-178 printed title, the unmerged same-amount rows, and the unchanged column-role, ten-cell, elimination, dash, and company-margin boundaries. Do not enqueue or replay in this task

## 3. Successor replay and source review, not yet authorized

- [x] 3.1 After implementation review, run one controlled replay under plan `manufacturing_materials_stage4_segment_financials.2026-09-29.3` into a new isolation directory. Keep `accepted_for_review`, provider calls at the implementation contract, and `production_authorization=not_authorized`. Do not overwrite `replay/20260929.2` or `replay/20260928`
- [ ] 3.2 Independently reread the four reports and derive recall, accuracy, critical numeric errors, and the gate. Do not presume a true gate, and do not reuse 102/132, 114/142, 122/132, 129/133, 38/38, 36/38, 26/36, 23/23, 19/23, or 9/9
