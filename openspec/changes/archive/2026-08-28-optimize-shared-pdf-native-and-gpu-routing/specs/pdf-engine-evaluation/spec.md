## MODIFIED Requirements

### Requirement: Evaluation corpus is explicit and hash verified
The PDF engine evaluation SHALL accept only explicit local PDF paths or verified manifests that identify database-referenced announcement assets, and SHALL verify PDF signatures and SHA-256 content hashes before processing. The native-promotion corpus SHALL extend, not reuse unchanged, the narrow page-recovery manifest and SHALL bind mapping-corrupt正文, normal, scanned, mixed-native-recoverable, numeric, and table/read-order gold.

#### Scenario: Verified announcement corpus is supplied
- **WHEN** an operator supplies a bounded manifest of archived announcement PDFs with expected hashes
- **THEN** the evaluator SHALL load only those files, reject missing/non-PDF/hash-mismatched entries, and record the ordered corpus identity
- **AND** it SHALL not perform discovery or download as part of evaluation

#### Scenario: Expanded 600036.SH mapping-corruption fixture is available
- **WHEN** the archived 600036.SH 2025 annual report with content hash `abe612a273468072b176dd51ea460c1e1596f8ca729cbc6db3fa28ba9a57ea79` is evaluated for native promotion
- **THEN** the evaluator SHALL include physical pages 19, 41, and 51-62 plus the verified TOC/diagnostic set rather than only page 2 and SHALL retain the link to announcement `ann_e9a7df3862148a4699fd3a36284fe1c7`
- **AND** gold SHALL use actual source forms such as cover text `招商銀行`, verified Chinese headings, key numbers, page continuity, and selected table/read-order evidence
- **AND** the report SHALL record whether each engine recovers the required Chinese/numeric evidence, routes each page correctly, and preserves page-level provenance

#### Scenario: Normal baseline has meaningful gold
- **WHEN** 000717.SZ is used as the normal native baseline
- **THEN** its manifest SHALL contain read-only verified Chinese heading, numeric, and reading-order gold rather than an empty gold object

#### Scenario: Mixed corpus includes negative OCR evidence
- **WHEN** 002376.SZ or another hash-bound mixed report is evaluated
- **THEN** the manifest SHALL include both a truly native-unusable page and a PDFium-native-usable page
- **AND** the native-usable page SHALL be marked as a negative OCR expectation

#### Scenario: No explicit corpus is supplied
- **WHEN** the evaluator is invoked without explicit paths or a verified manifest
- **THEN** it SHALL fail before provider, archive, database-write, or parser activity

### Requirement: Engine comparisons use identical source bytes and isolated trials
Every configured engine/profile comparison SHALL process the same ordered document, page, content-hash, and expected-script set in isolated temporary output roots. Native adapters SHALL receive identical page sets; GPU and CPU OCR trials SHALL receive identical PDFium-rendered inputs and renderer/DPI/configuration identity. Cache reuse SHALL not satisfy extraction work for a later candidate unless the trial explicitly measures cache behavior.

#### Scenario: Native engines are compared
- **WHEN** `pypdfium2` and `pypdf` are evaluated for primary/fallback selection
- **THEN** both trials SHALL receive byte-identical PDFs, identical physical pages, expected-script policy, and timeout limits
- **AND** each trial SHALL report parser version, text hashes, quality outcomes, page/open timing, and artifact root

#### Scenario: GPU and CPU workers are compared
- **WHEN** GPU-first and CPU-only OCR profiles are evaluated
- **THEN** both trials SHALL use Paddle/PaddleOCR 3.3.1/3.7.0, the same model/inference configuration, and byte-equivalent PDFium-rendered page inputs
- **AND** device/runtime differences SHALL remain explicit in reports and cache identities

#### Scenario: Candidate trial partially fails
- **WHEN** one document or page fails during a trial
- **THEN** the evaluator SHALL retain the failure diagnostic and continue only within configured bounds
- **AND** the report SHALL distinguish partial failure from successful complete processing

### Requirement: Evaluation reports accuracy and evidence fidelity
The evaluator SHALL report native and OCR accuracy using a labeled gold subset covering Chinese text, expected-script evidence, numeric/date fields, headings, tables, scanned pages, mixed pages, and representative announcement layouts; it SHALL also report page/hash/evidence fidelity against the configured baseline. Numeric/ASCII residue without required Chinese/script evidence SHALL not count as usable native text for a Chinese-expected case.

#### Scenario: Candidate produces a faster result with changed evidence
- **WHEN** a candidate improves throughput but changes governed page count, page identity, normalized text/hash, numeric/read order, or evidence availability beyond policy
- **THEN** the evaluator SHALL mark the candidate ineligible for production selection
- **AND** the report SHALL include the mismatch categories and affected documents/pages

#### Scenario: OCR candidate is evaluated
- **WHEN** a candidate processes scanned or mixed pages
- **THEN** the report SHALL include character error rate or equivalent text accuracy, numeric/date exact-match results, table/heading metrics where labeled, confidence coverage, and low-quality recall

#### Scenario: Native engines disagree on a visually readable page
- **WHEN** one native engine returns non-empty but semantically corrupted or numeric-only residual text and another native engine returns readable Chinese text
- **THEN** the report SHALL classify the first candidate as a native text-mapping/expected-script failure rather than a scanned-page or usable-native success
- **AND** it SHALL compare mapping-error detection, native fallback, OCR suppression, and false `usable_native`/`not_disclosed` outcomes

#### Scenario: Mixed classification conflicts with page evidence
- **WHEN** a document-level classifier returns `mixed` but PDFium passes all page-level gold for a target page
- **THEN** the report SHALL fail any profile that routes that page to OCR solely because of the document class

### Requirement: Primary and fallback engine selection requires evidence
The evaluator SHALL select or recommend primary and fallback profiles only when configured fidelity, accuracy, performance, OCR throughput, tail-latency, resource, maintenance, and fail-closed gates pass; evaluation alone SHALL NOT change production defaults. It SHALL continue to support component-specialized recommendations for plain text, table/layout, CPU-only, and GPU-enabled workloads when no single OCR component is best. Native promotion and GPU enablement SHALL use independent approvals so PDFium can be promoted without waiting for GPU readiness. An approval is bound to profile, corpus/pages, renderer, runtime/model, inference configuration, and gate version and SHALL NOT authorize a materially different profile.

#### Scenario: PDFium passes all native gates
- **WHEN** `pypdfium2` recovers the expanded 600036.SH正文 gold, preserves the verified 000717.SZ baseline and table/numeric reading order, avoids false native success on scanned pages, and suppresses OCR on native-usable mixed pages within latency limits
- **THEN** the evaluator SHALL record PDFium as eligible primary and `pypdf` as fallback
- **AND** an operator SHALL be able to promote the native profile without enabling GPU OCR

#### Scenario: Existing GPU approval targets the old profile
- **WHEN** the approval artifact names `pypdf_paddleocr_gpu_canary` and the narrow one-page corpus hash
- **THEN** it SHALL remain runtime evidence for Paddle/PaddleOCR 3.3.1/3.7.0 on GRID P4
- **AND** it SHALL NOT approve the replacement PDFium-first isolated-worker profile

#### Scenario: Expanded GPU canary passes
- **WHEN** the replacement GPU profile passes expanded fidelity, PDFium-render parity, selective-page, fallback, cache-separation, latency, resource, and no-full-document-OCR gates
- **THEN** the evaluator SHALL issue a new profile/hash/config-bound GPU approval and SHALL record GPU-first with isolated CPU fallback as the eligible recommendation

#### Scenario: Accuracy and efficiency trade off across OCR components
- **WHEN** one component has higher accuracy but another has materially better throughput or resource efficiency while both pass accuracy floors
- **THEN** the evaluator SHALL report a constrained Pareto recommendation by workload/profile instead of selecting solely by one aggregate score
- **AND** the recommendation SHALL include the fallback component and the conditions that trigger it

#### Scenario: No candidate passes
- **WHEN** all candidates fail one or more required gates
- **THEN** the evaluator SHALL leave the current production profile unchanged
- **AND** it SHALL report blocking gates and preserve the current engine as the fallback baseline

## ADDED Requirements

### Requirement: Native worker crash and parallelism evaluation

The evaluator MUST provide a read-only canary for the supervised native worker pool. It MUST compare configured multi-process widths `1`, `2`, `4`, `6`, `8`, and `10` when host capacity allows, while keeping corpus bytes, requested pages, parser versions, deadlines, and quality gates fixed. It MUST include the known 603268.SH and 002496.SZ crash reports, run at least 20 rounds for the crash-isolation gate, and record parent-service liveness, worker exit signal/status, restart count, throughput, P50/P95/tail latency, memory, queue wait, and completed-page preservation.

#### Scenario: Known concurrent crash is isolated

- **WHEN** the evaluator runs the known 603268.SH/002496.SZ combination at the configured concurrent width
- **THEN** a PDFium worker `SIGTRAP`/native crash MUST be reported as a typed diagnostic and the Quote parent process MUST remain alive
- **AND** the service restart count MUST not increase because of the canary

#### Scenario: Width is promoted only after safe parallel evidence

- **WHEN** one tested width has higher throughput but a parent exit, unexplained page loss, or untyped worker failure
- **THEN** that width MUST be ineligible for production selection
- **AND** the report MUST select the highest width that passes liveness, fidelity, bounded-resource, and diagnostic gates, or retain serial width when no parallel width passes

#### Scenario: Native worker and OCR worker boundaries remain distinct

- **WHEN** an OCR trial is evaluated after native PDFium rendering
- **THEN** the report MUST prove that PDFium extraction/rasterization occurred in the native worker and PaddleOCR inference occurred only in the isolated GPU/CPU worker
- **AND** a GPU/CPU OCR success MUST NOT be accepted if the Quote parent directly opened PDFium during the trial
