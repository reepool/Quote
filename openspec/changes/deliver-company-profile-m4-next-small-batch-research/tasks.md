## 1. Scope review gate

- [ ] 1.1 Independent review accepts this next-batch scope: two official annual reports not yet delivered under `owned_page_facts=v8` and `material_input_facts=v1`, one service disclosure and one manufacturing disclosure; cutoff, report identity, document version, and the existing two-company 50000-token budget frozen before enqueue; reuse of `company_profile_common_core` for evidence selection, extraction, persistence, query, and export; service company first, then manufacturing; independent snapshots under `reports/m4_next_small_batch/<plan_id>/`; historical `600004.SH` and `600006.SH` observations kept, including v8 recall 8/9 and the later 9/9 successor; pass criteria fixed before the run and scores not prefilled. Do not implement in this review. Do not change Python, enqueue, or edit the completed mode, plan pointer, closure, or control plane.

## 2. Freeze the plan, not yet authorized

- [ ] 2.1 After 1.1, select the two reports by the frozen rule and write only this round's plan snapshot. Record the cutoff, both report references, and the budget. Refuse the plan when either disclosure form has no legal candidate. Do not enqueue or overwrite the historical first-expansion files.

## 3. Deliver the first disclosure form, not yet authorized

- [ ] 3.1 After 2.1, run the service company through the existing common-core path and then query and export that company. Keep a failure on that company. Do not start the manufacturing company in that task, and do not calculate a source-review score.

## 4. Deliver the second disclosure form, not yet authorized

- [ ] 4.1 After 3.1, run the manufacturing company through the same path. Reuse a completed scope instead of calling the provider again. A failure of either company must not erase the other. Query and export the delivered company.

## 5. Independent reading, not yet authorized

- [ ] 5.1 After both runs, reread the two frozen reports and record this round's source-review snapshot. Count recall, accuracy, and critical numeric errors from that reading. Keep a miss as a miss. Do not authorize production or a scale-quality claim, and do not archive in that task.
