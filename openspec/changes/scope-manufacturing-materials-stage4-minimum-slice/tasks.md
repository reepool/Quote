## 1. Scope review gate

- [x] 1.1 Independent review accepts sample diversity, the one-slice rule, field boundaries, research isolation, and the later verification method. The three defining reports are the slice floor, not the whole stage-three sample. Verified `302132.SZ` stays the historical regime baseline and is outside this sample. This card does not select `extract_business_regime` or claim a new regime validation. Do not change Python, identity, publication, closure, mode, or checkpoints until 1.1 is checked

## 2. Implementation after review, not yet authorized

- [x] 2.1 Write an independent dossier for each approved report before choosing a slice. Dossiers: `dossiers/300750-sz-2025.md`, `dossiers/603659-sh-2025.md`, `dossiers/920015-bj-2025.md`. No common required fields and no chapter selection in those files
- [x] 2.2 Name exactly one existing chapter task that two company dossiers support, excluding `extract_business_regime`, and leave the other chapters inactive. Selected task: `extract_material_inputs`, supported by the 300750.SZ, 603659.SH, and 920015.BJ dossiers. Operating quantities, counterparties, regime, and the other chapters stay closed. No extractor implementation in this task
- [x] 2.3 Implement that slice only by reusing Evidence, Stage 5 extract/repair/verify, and the research isolation bundle, with output limited to `accepted_for_review`
- [x] 2.3a Keep each research fact reconstructable from its own report identity, period, subject, record id, and Evidence. Store a sales role as its own Activity with separate Evidence. Cite Evidence and a reason for legal_empty, unclear, and extraction_failure. Do not replay
- [x] 2.3b Persist named-input legal_empty, unclear, and preparation extraction_failure through the research bundle writer. A failed page stays extraction_failure and does not become a fact. Do not replay

## 3. Focused replay, not yet authorized

- [x] 3.1 After implementation review, replay only the approved reports for the selected slice. Freeze the PDF hash, report identity, dossier hash, and material-input evidence binding before the run. Keep the run in the research isolation directory at `accepted_for_review`. Do not prefill recall, accuracy, critical errors, or gates
- [x] 3.2 Independently recount recall, accuracy, critical numeric errors, and expansion gates. Do not presume they pass, and do not reuse the fixed two-company 9/9. Snapshot: `replay/20260926/source_review.json`. Recall 19/23, accuracy 19/19, critical numeric errors 0, expansion_gates_met false. Catalog mapping is not a pass
- [x] 3.2a Bind the source review to `run.json` sha256 `f80c96e5523704d05cc880c3a56cd4201549081f2fca9a10e8a0433629819852` and `material-input-stage4-material-inputs-20260926/result.json` sha256 `9a7444d285b8c8897f5206cce0abe00420edc12ba133ae29e3ccd795e82279b9`. Do not replay or change the 19/23 recount
- [ ] 3.3 Do not start another expansion, authorize scale quality or production, enable the six-chapter package, or open DCF, trading, or the legacy writer
