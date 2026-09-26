## 1. Scope review gate

- [x] 1.1 Independent review accepts this scope. The gap is chapter activation and Evidence routing, not a missing domain model. Do not change business code until 1.1 is checked

## 2. Implementation after review

- [x] 2.1 Activate the existing `EXTRACT_MATERIAL_INPUTS` chapter only when the official text names a material as a company production or operating input, and route it through the existing Stage 5 provider, acceptance, and writer
- [x] 2.2 Deliver the existing `material_input` Relationship and `CommodityExposure(role=raw_material_input)` with source name, subject, report identity, period, and Evidence, even when quantity and market series are absent
- [x] 2.3 Publish successor identity `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1"}` without force-replaying or overwriting v1–v8 work or old source-review snapshots
- [x] 2.4 Add direct counterexamples for an explicit named input, a report with no explicit commodity role, one existing non-Dongfeng manufacturing fixture, price-risk-only wording, generic cost or inventory amounts, sales evidence alone not creating an input role, independent sales and input evidence keeping both roles without netting, pending and ambiguous catalog mappings still delivering the role, and energy consumption staying `energy_consumption`
- [x] 2.4a Bind a named material to the company's own input in the same sentence. Reject third-party procurement subjects and a principal-material list that only has 制造 or 生产 in a later window. Keep the four-name vehicle sentence.

## 3. Official successor replay

- [x] 3.1 After implementation review, replay only the already frozen official reports on the new identity
- [x] 3.2 Independently recount recall, accuracy, critical numeric errors, and workload. Do not presume recall 9/9 or that expansion gates pass
- [x] 3.3 Do not start another expansion, authorize scale quality or production, add a full manufacturing package, or enable volume, material-cost ratio, procurement, reserves, suppliers, or customers. Field result: works `bp-work-946a0a5296c2144c1ebc161a` and `bp-work-7b81740827ce0ccca634e2e9` on `owned_page_facts=v8` plus `material_input_facts=v1`; source review `material_input_facts_v1_successor_source_review.20260926.json` SHA-256 `9f6fb61e4e7fdbee9e680d091c0bc4bc13ca89f319b92cdd0457fdc3e2cdfbec`; recall 9/9, accuracy 9/9, critical 0, elapsed 5.149175 seconds, `expansion_gates_met=true`. That gate covers only these two frozen companies and this identity. `scale_quality_claim_allowed=false` and `production_authorization=not_authorized` stay in force. The live-run file matches the v5–v8 bytes and does not by itself prove the successor identity.
