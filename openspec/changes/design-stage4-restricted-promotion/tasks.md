## 1. Design review gate

- [x] 1.1 Independent review accepts this restricted-delivery design: research query and export of material inputs, operating quantities, segment financials, and counterparties for the four frozen 2025 reports; input limited to the archived aggregate plans and 16 artifact hashes; read-only projection through `CompanyProfileTaskService` and `CompanyProfileReadService`; page, period, unit, and chapter semantics preserved; `302132.SZ` material inputs shown as legal empty with zero facts; repeated access does not write a second copy or overwrite history; core profile stays incomplete and overview, regime, and the six-chapter package stay gaps; research consumers stop before publication. Do not implement in this review. Do not change Python, replay, edit the control plane, or grant production, scale quality, DCF, or trading. Independent review accepted this contract. Do not start 2.1 in that review.

## 2. Read accepted results, not yet authorized

- [ ] 2.1 After 1.1, project the four frozen result bundles into the existing query response. Verify the 16 hashes, keep chapter-native facts, evidence, and coverage, and show `302132.SZ` material-input legal empty without creating facts. Do not write a runtime record or overwrite a common-core profile.

## 3. Export the same view, not yet authorized

- [ ] 3.1 After 2.1, export those facts, evidence, and coverage through the existing export action into the caller directory only. Repeated query and export must not duplicate stored facts or change replay, checkpoint, publication-control, or historical observation bytes. Do not add a UI, a generalized read model, overview, regime, or a production publication switch in that task.
