## 1. Scope review gate

- [ ] 1.1 Independent review accepts this scope. The gap is chapter activation and Evidence routing, not a missing domain model. Do not change business code until 1.1 is checked

## 2. Implementation after review

- [ ] 2.1 Activate the existing `EXTRACT_MATERIAL_INPUTS` chapter only when the official text names a material as a company production or operating input, and route it through the existing Stage 5 provider, acceptance, and writer
- [ ] 2.2 Deliver the existing `material_input` Relationship and `CommodityExposure(role=raw_material_input)` with source name, subject, report identity, period, and Evidence, even when quantity and market series are absent
- [ ] 2.3 Publish successor identity `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1"}` without force-replaying or overwriting v1–v8 work or old source-review snapshots
- [ ] 2.4 Add direct counterexamples for an explicit named input, a report with no explicit commodity role, one existing non-Dongfeng manufacturing fixture, price-risk-only wording, generic cost or inventory amounts, a sold product, and energy consumption staying `energy_consumption`

## 3. Official successor replay, not yet authorized

- [ ] 3.1 After implementation review, replay only the already frozen official reports on the new identity
- [ ] 3.2 Independently recount recall, accuracy, critical numeric errors, and workload. Do not presume recall 9/9 or that expansion gates pass
- [ ] 3.3 Do not start another expansion, authorize scale quality or production, add a full manufacturing package, or enable volume, material-cost ratio, procurement, reserves, suppliers, or customers
