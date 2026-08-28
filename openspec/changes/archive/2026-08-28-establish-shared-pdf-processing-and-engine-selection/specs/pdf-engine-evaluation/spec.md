## ADDED Requirements

### Requirement: Evaluation corpus is explicit and hash verified
The PDF engine evaluation SHALL accept only explicit local PDF paths or verified manifests that identify database-referenced announcement assets, and SHALL verify PDF signatures and SHA-256 content hashes before processing. The corpus SHALL include a pathological class for viewer-readable PDFs with corrupt native text mappings.

#### Scenario: Verified announcement corpus is supplied
- **WHEN** an operator supplies a bounded manifest of archived announcement PDFs with expected hashes
- **THEN** the evaluator SHALL load only those files, reject missing/non-PDF/hash-mismatched entries, and record the ordered corpus identity
- **AND** it SHALL not perform discovery or download as part of evaluation

#### Scenario: 600036.SH mapping-corruption fixture is available
- **WHEN** the archived 600036.SH 2025 annual report with content hash `abe612a273468072b176dd51ea460c1e1596f8ca729cbc6db3fa28ba9a57ea79` is available
- **THEN** the evaluator SHALL include it as a named mapping-corruption fixture linked to announcement `ann_e9a7df3862148a4699fd3a36284fe1c7`
- **AND** the report SHALL record whether each engine recovers Chinese headings/text, routes affected pages, and preserves page-level provenance

#### Scenario: No explicit corpus is supplied
- **WHEN** the evaluator is invoked without explicit paths or a verified manifest
- **THEN** it SHALL fail before provider, archive, database-write, or parser activity

### Requirement: Engine comparisons use identical source bytes and isolated trials
Every configured engine/profile comparison SHALL process the same ordered document and content-hash set in isolated temporary output roots, with cache reuse unable to satisfy extraction work for a later candidate unless the trial explicitly measures that cache behavior.

#### Scenario: Native and OCR profiles are compared
- **WHEN** `pypdf_paddleocr` and `pdf_inspector_paddleocr` are evaluated against the same corpus
- **THEN** both trials SHALL receive byte-identical inputs and the same page/timeout/concurrency limits
- **AND** each trial SHALL report its own profile, parser versions, model versions, and artifact root

#### Scenario: Candidate trial partially fails
- **WHEN** one document or page fails during a trial
- **THEN** the evaluator SHALL retain the failure diagnostic and continue only within configured bounds
- **AND** the report SHALL distinguish partial failure from successful complete processing

### Requirement: OCR component comparisons expose the native-to-OCR bottleneck
The evaluator SHALL compare OCR components and profiles independently of the native parser, including at least PP-OCR text recognition, PP-Structure/PP-StructureV3 for structure pages, `pdf-inspector` OCR when available, and a lightweight Tesseract/OCRmyPDF baseline when available; it SHALL measure both isolated OCR cost and end-to-end document cost.

#### Scenario: OCR component matrix is executed
- **WHEN** the same OCR page subset is processed by two or more available components
- **THEN** each component SHALL receive equivalent rendered page inputs, language settings, page limits, and resource limits
- **AND** the report SHALL identify component, model/runtime version, warm/cold mode, batch size, and worker configuration

#### Scenario: Native and OCR costs are compared
- **WHEN** a document contains native pages and OCR-routed pages
- **THEN** the report SHALL separate native extraction time, routing time, OCR queue wait, model load/warm-up, OCR inference, and total document time
- **AND** it SHALL report the OCR time share and incremental cost per OCR page relative to the native-only baseline

### Requirement: Evaluation reports accuracy and evidence fidelity
The evaluator SHALL report native and OCR accuracy using a labeled gold subset covering Chinese text, numeric/date fields, headings, tables, scanned pages, mixed pages, and representative announcement layouts; it SHALL also report page/hash/evidence fidelity against the configured baseline.

#### Scenario: Candidate produces a faster result with changed evidence
- **WHEN** a candidate improves throughput but changes governed page count, page identity, normalized text/hash, or evidence availability beyond policy
- **THEN** the evaluator SHALL mark the candidate ineligible for production selection
- **AND** the report SHALL include the mismatch categories and affected documents/pages

#### Scenario: OCR candidate is evaluated
- **WHEN** a candidate processes scanned or mixed pages
- **THEN** the report SHALL include character error rate or equivalent text accuracy, numeric/date exact-match results, table/heading metrics where labeled, confidence coverage, and low-quality recall

#### Scenario: Native engines disagree on a visually readable page
- **WHEN** one native engine returns non-empty but semantically corrupted text and another engine or OCR returns readable Chinese text
- **THEN** the report SHALL classify the case as a native text-mapping failure rather than a scanned-page success
- **AND** it SHALL compare mapping-error detection, alternate-native recovery, OCR fallback, and false `usable_native`/`not_disclosed` outcomes

### Requirement: Evaluation reports performance, resource, and failure metrics
Each trial SHALL record cold-start and warm wall time, per-document/page timing, P50/P95 latency, OCR pages per second, end-to-end documents/pages per minute, concurrency, queue wait, batch size, CPU/RSS/GPU/model-load/warm-up metrics when available, warning counts, OCR invocation counts, budget/deferred counts, and typed failure rates.

#### Scenario: Trial completes within bounds
- **WHEN** an engine trial completes over the explicit corpus
- **THEN** the report SHALL contain comparable timing/resource metrics and the corpus/profile identity
- **AND** it SHALL be reproducible from the recorded configuration and hashes without reading production state

#### Scenario: Trial exceeds a bound
- **WHEN** a trial reaches its elapsed, page, concurrency, or resource limit
- **THEN** the report SHALL mark the trial bounded/failed with the limit and observed metrics
- **AND** it SHALL not be treated as a passing performance result

#### Scenario: OCR creates a throughput bottleneck
- **WHEN** OCR-routed pages cause queue wait, P95 latency, or resource use to exceed configured budgets relative to native baseline
- **THEN** the candidate SHALL fail the performance/resource gate for that profile even if its recognition accuracy passes
- **AND** the report SHALL identify whether selective routing, batching, worker reuse, a different component, or a lower OCR scope could remove the bottleneck

### Requirement: Primary and fallback engine selection requires evidence
The evaluator SHALL select or recommend primary and fallback profiles only when configured fidelity, accuracy, performance, OCR throughput, tail-latency, resource, maintenance, and fail-closed gates pass; evaluation alone SHALL NOT change production defaults. It SHALL support component-specialized recommendations for plain text, table/layout, CPU-only, and GPU-enabled profiles when no single OCR component is best for all workloads.

#### Scenario: Candidate passes all configured gates
- **WHEN** a candidate meets the required evidence gates on the frozen corpus
- **THEN** the evaluator SHALL record its eligibility, primary/fallback recommendation, gate results, and report version
- **AND** an operator SHALL be able to apply that recommendation through configuration without consumer code changes

#### Scenario: Accuracy and efficiency trade off across OCR components
- **WHEN** one component has higher accuracy but another has materially better throughput or resource efficiency while both pass accuracy floors
- **THEN** the evaluator SHALL report a constrained Pareto recommendation by workload/profile instead of selecting solely by one aggregate score
- **AND** the recommendation SHALL include the fallback component and the conditions that trigger it

#### Scenario: No candidate passes
- **WHEN** all candidates fail one or more required gates
- **THEN** the evaluator SHALL leave the current production profile unchanged
- **AND** it SHALL report blocking gates and preserve the current engine as the fallback baseline

### Requirement: Evaluation is read-only to production state
The evaluator SHALL not mutate production databases, announcement rows, shared asset manifests, source PDFs, queues, canonical facts, or scheduler configuration; it SHALL write only explicitly selected reports and temporary/benchmark artifacts.

#### Scenario: Evaluation runs against database-referenced assets
- **WHEN** the evaluator resolves an archived PDF from database metadata
- **THEN** it SHALL read the metadata and file bytes for verification only
- **AND** any generated report SHALL contain bounded metrics/hashes rather than unbounded copied document text
