## 1. Scope review gate

- [x] 1.1 Independent review accepts this hold: the stage-4 index stays read-only, no aggregate `expansion_gates_met=true` is claimed, and the only next assessment is a dossier review of `extract_counterparties_and_concentration` on the approved report list. Do not check 1.1 until that review. Do not change Python, enqueue, replay, identity, publication, closure, mode, checkpoint, or any archived artifact. Do not start the six-chapter package, scale quality, production, or an issuer outside the approved list

## 2. Dossier assessment, not yet authorized

- [x] 2.1 After 1.1, read the approved reports only and decide whether `extract_counterparties_and_concentration` has at least three reports, SZSE, SSE, BSE, and at least two disclosure forms. If it does not, stop. Do not add an issuer, write Python, enqueue, or replay in this task. The 2026-09-29 reading is in design.md under "Dossier assessment": four approved 2025 reports, SZSE/SSE/BSE, and more than one disclosure form. No coverage gap, no added issuer, and no recall or gate. Independent review accepted this reading. Implementation stays a separate change and is not started here

## 3. Close-out

- [x] 3.1 Close this read-only hold. The dossier assessment of the four approved 2025 reports is complete. `extract_counterparties_and_concentration` was implemented, replayed, and archived in `openspec/changes/archive/2026-09-29-scope-manufacturing-materials-stage4-counterparties-and-concentration/`. Its local 45/45 does not create an aggregate `expansion_gates_met=true`. `scale_quality_claim_allowed` stays false and `production_authorization` stays `not_authorized`. Archive this change to `openspec/changes/archive/2026-09-29-hold-stage4-expansion-and-review-next-scope/`. Do not open a six-chapter package, scale quality, production, or a successor replay for an archived slice
