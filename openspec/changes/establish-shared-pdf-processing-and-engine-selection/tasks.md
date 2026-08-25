## 1. Baseline And Contracts

- [ ] 1.1 Freeze the production PDF consumer inventory and map each direct engine import/page-extraction call to its owning domain, current behavior, tests, and removal condition.
- [x] 1.2 Define the shared PDF request, page-result, document-result, diagnostic, provenance, and profile contracts with stable schema/parser version fields.
- [x] 1.3 Add contract tests for valid, malformed, encrypted, empty, partially extracted, and content-hash-mismatched PDFs without changing production consumers.
- [x] 1.4 Define the engine profile configuration schema, validation rules, rollout state, fallback profile, resource limits, model/runtime versions, and redacted reporting fields.
- [x] 1.5 Add the 600036.SH 2025 annual report fixture identity (announcement ID, content hash, page count when available) to the benchmark manifest contract and document the expected viewer-readable/native-mapping-corrupt classification.
- [x] 1.6 Define OCR performance budgets and profile fields for render resolution, queue wait, page/document limits, concurrency, batch size, warm-up, CPU/RSS/GPU, and deferred/budget-exceeded outcomes.

## 2. Shared PDF Module And Adapters

- [x] 2.1 Create the shared PDF module under `research/document_processing/pdf/` (or the approved equivalent) with adapter protocols, immutable results, hashing, and typed failure diagnostics.
- [x] 2.2 Implement the `pypdf` native adapter, preserving page order, text normalization policy, encryption/malformed diagnostics, and native extraction timing.
- [x] 2.3 Implement quality diagnostics for empty/low-text pages, extraction errors, suspicious glyph decoding, and explicit target-page routing.
- [x] 2.4 Implement quality diagnostics for valid-Unicode mojibake and broken `ToUnicode`/CMap mappings, including a distinct `native_text_mapping_error` outcome that does not depend on replacement characters or heading matches.
- [x] 2.5 Implement the bounded router that prefers usable native text, tries alternate-native recovery for mapping errors, and routes only affected/requested pages to OCR.
- [x] 2.6 Add the PaddleOCR PP-OCR/PP-Structure adapter with page-addressable rendering/inference, model/session reuse, confidence, coordinates/tables when available, timing, and typed warnings.
- [x] 2.7 Add the optional `pdf-inspector` native/detection adapter and capability probe without making it the production default.
- [x] 2.8 Add profile-driven adapter selection and fallback behavior, including missing-runtime/model failures and no silent empty-success outcomes.
- [x] 2.9 Add warm worker/session reuse, optional page batching, OCR queue backpressure, and per-page/per-document budget enforcement.
- [x] 2.10 Add unit tests for native-only, mixed-page selective OCR, ToUnicode mapping corruption, alternate-native recovery, OCR failure/low-confidence, profile switching, warm reuse, queue bounds, budget outcomes, and repeated hash-equivalent parsing.

## 3. Database-Referenced Engine Evaluation

- [x] 3.1 Define a read-only evaluation manifest format that binds announcement/asset identity, local PDF path, content hash, document class, and optional gold labels without copying unrestricted source text.
- [x] 3.2 Implement explicit-corpus loading and SHA-256/PDF-signature verification; fail before provider discovery, downloading, queue mutation, or production writes when inputs are implicit or invalid.
- [ ] 3.3 Build a stratified bounded corpus from database-referenced official announcement assets covering native, scanned, mixed, low-quality, glyph-encoding, viewer-readable mapping-corrupt, table, and representative Chinese announcement cases; include 600036.SH when its archived hash is available.
- [x] 3.4 Implement isolated per-profile trial execution over identical ordered bytes with bounded document/page/concurrency/elapsed/resource limits and cache-isolation diagnostics.
- [ ] 3.5 Implement the OCR component matrix for PP-OCR, PP-Structure, `pdf-inspector` OCR, and available lightweight baseline, using equivalent rendered pages and separate cold/warm/batch trials.
- [x] 3.6 Implement fidelity metrics for page count, page identity, normalized text/page hashes, headings, page references, extraction errors, and OCR provenance.
- [ ] 3.7 Implement labeled accuracy metrics for Chinese character text, numeric/date exact match, heading detection, table structure, OCR confidence coverage, and low-quality recall.
- [ ] 3.8 Implement performance/resource metrics for native baseline time, routing time, OCR queue wait, cold/warm/model-load time, OCR pages per second, end-to-end throughput, P50/P95 latency, batch/concurrency, CPU, RSS/GPU, warnings, budget/deferred counts, and typed failure rates.
- [x] 3.9 Implement versioned JSON/Markdown evaluation reports containing corpus/profile/config hashes, gate results, constrained Pareto recommendations, candidate eligibility, and bounded diagnostics only.
- [ ] 3.10 Add tests proving identical corpus enforcement, read-only behavior, cache isolation, bounds, partial failures, fidelity rejection, OCR bottleneck detection, component comparison, and report reproducibility.
- [ ] 3.11 Run the evaluation for `pypdf_paddleocr`, `pdf_inspector_paddleocr`, structure-page profiles, and available lightweight baseline profiles; retain the report as change evidence without changing production defaults.

## 4. Primary And Fallback Engine Decision

- [ ] 4.1 Define configurable acceptance gates for fidelity, Chinese OCR accuracy, numeric/date accuracy, table/heading evidence, P95 latency, throughput, resource limits, failure rate, and operational maintainability.
- [ ] 4.2 Define separate efficiency gates for OCR page throughput, queue wait, cold/warm tail latency, model-load amortization, and CPU/RSS/GPU budgets relative to the native baseline.
- [x] 4.3 Compare evaluation results against the gates and record a versioned primary/fallback/component-specialized recommendation or an explicit no-change decision.
- [x] 4.4 Add configuration-only activation and rollback for the selected profile; verify that consumers do not require code changes to switch profiles.
- [ ] 4.5 Run a bounded canary on representative official announcement assets and verify source hash, page evidence, OCR provenance, queue behavior, resource budgets, and downstream fail-closed behavior before broader activation.

## 5. Consumer Migration

- [x] 5.1 Migrate CNInfo company-action document preparation to shared page results while preserving page selection, `extraction_method`, `quality_status`, text hashes, OCR warnings, semantic verification, and manual-required gates.
- [x] 5.2 Migrate business-profile PDF artifacts to shared page results while preserving artifact hashes, heading index, low-text/OCR-required diagnostics, section selection, and promotion evidence gates.
- [x] 5.3 Add a business-profile regression proving the 600036.SH mapping-corrupt pages are not treated as absent disclosure and are recovered by alternate-native or governed OCR before promotion decisions.
- [x] 5.4 Migrate broker risk-control parsing while preserving fixed-order/table fallback behavior, source-file manifests, parser versions, and required-fact readiness semantics.
- [x] 5.5 Migrate official index lifecycle parsing, HKEX suspension report parsing, and announcement classification to shared page results while preserving their existing row/title/date semantics.
- [ ] 5.6 Add consumer-specific golden fixtures and regression tests for native, scanned, mixed, malformed, encrypted, low-quality, and OCR-failure documents in each migrated domain.
- [ ] 5.7 Add bounded integration tests proving migrated consumers preserve source identity, page references, business outputs, database/write owners, and scheduler/API/CLI contracts.
- [ ] 5.8 Keep the previous parser path as a named compatibility fallback until each consumer slice passes its evaluation and release gate.

## 6. Cleanup, Verification, And Handoff

- [x] 6.1 Remove direct PDF engine imports and instantiation from migrated production modules; leave only shared adapters and documented non-production tooling.
- [x] 6.2 Run a repository residue check and update the consumer inventory with zero unresolved production call sites or explicit removal conditions.
- [ ] 6.3 Run focused unit tests, consumer regression tests, benchmark/report validation, Python compilation, and OpenSpec strict validation.
- [x] 6.4 Update current architecture/configuration/runbook documentation with the shared PDF owner, profile selection, OCR runtime prerequisites, evaluation command, and rollback procedure.
- [ ] 6.5 Review the final diff against the task-start worktree baseline and confirm no pre-existing config, announcement-asset, business-profile, or documentation changes were overwritten.
