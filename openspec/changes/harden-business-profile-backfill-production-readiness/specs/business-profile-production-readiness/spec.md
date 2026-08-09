## ADDED Requirements

### Requirement: Business-profile semantic output is Chinese and source-preserving
The business-profile semantic extractor SHALL require Simplified Chinese for human-readable conclusions, SHALL preserve source-native labels, proper nouns, acronyms, numeric values, and units without translation, and SHALL leave canonical field mapping to deterministic program code.

#### Scenario: Chinese annual-report row is extracted
- **WHEN** selected annual-report evidence contains a Chinese segment or product label
- **THEN** the LLM response retains that source-native label in the source-label field
- **AND** any semantic summary is written in Simplified Chinese
- **AND** the LLM does not translate the label into English

#### Scenario: Source contains an acronym or unit symbol
- **WHEN** evidence contains a proper noun, acronym, registered name, or unit symbol such as `LED`, `MOSFET`, or `kW`
- **THEN** the response preserves that source token without inventing a translation
- **AND** the language gate does not reject an otherwise Chinese response merely because the token contains Latin characters

#### Scenario: Model returns an English-only human-readable value
- **WHEN** a source-label or semantic-summary field is English-only despite Chinese source evidence
- **THEN** the system rejects that response for a language-contract violation
- **AND** it performs at most one bounded Chinese repair request before machine rework
- **AND** it does not create a routine human-review task

### Requirement: Source, semantic, and canonical fields remain distinct
The system SHALL store source-native labels and values separately from Chinese semantic conclusions and program-generated canonical names, units, and identifiers.

#### Scenario: Semantic summary paraphrases the source
- **WHEN** the LLM produces a supported Chinese paraphrase from one or more evidence spans
- **THEN** the system stores the paraphrase as semantic synthesis
- **AND** it retains the source-native label and exact evidence independently
- **AND** it does not require the paraphrase to be a literal source substring

#### Scenario: Canonical segment identity is generated
- **WHEN** a source-native segment label is admitted as a candidate
- **THEN** deterministic code resolves or creates its canonical name and stable identifier
- **AND** model translation or paraphrase is not used as the cross-period identity key

### Requirement: Units are normalized only by deterministic code
The system SHALL require the LLM to return the source numeric value and source unit without conversion and SHALL calculate canonical units and values through a versioned deterministic unit parser using exact decimal arithmetic.

#### Scenario: Chinese magnitude unit is resolved
- **WHEN** the source unit contains a governed magnitude such as `千只`, `万个`, `万台（套）`, `亿元`, or `亿千瓦时`
- **THEN** program code separates the magnitude multiplier from the base dimension
- **AND** it preserves the untouched source unit
- **AND** it records the parser rule, multiplier, canonical unit, and catalog version

#### Scenario: Industry count classifier is resolved
- **WHEN** a source unit uses a classifier such as `颗`, `只`, `瓶`, `腔`, `台`, or `套`
- **THEN** the program maps the unit to the governed count dimension
- **AND** it retains the original classifier in lineage

#### Scenario: Compound classifier shares one dimension
- **WHEN** a source unit such as `只/瓶` contains alternatives that all resolve to the count dimension
- **THEN** the program may normalize the value to the canonical count unit
- **AND** it preserves every source alternative in lineage

#### Scenario: Unit is unresolved or dimensionally ambiguous
- **WHEN** deterministic grammar and the current catalog cannot resolve a source unit to exactly one dimension and multiplier
- **THEN** the system persists the semantic artifact with `unit_resolution_pending`
- **AND** it blocks canonical publication without discarding source data
- **AND** it does not repeat the successful LLM request

#### Scenario: Catalog version resolves a pending unit
- **WHEN** a later unit-catalog version can resolve a pending source unit
- **THEN** the system replays the stored semantic artifact and performs conversion without an LLM call
- **AND** it records the new catalog and rule lineage

#### Scenario: Unknown unit is compositionally resolvable
- **WHEN** an unseen source-unit string can be decomposed entirely into governed prefixes, base units, classifiers, numerators, and denominators
- **THEN** deterministic code resolves it automatically without an LLM or manual review
- **AND** it records the complete compositional rule trace

#### Scenario: LLM proposes a rule for unknown tokens
- **WHEN** deterministic parsing leaves one or more unknown unit tokens and the optional unit-proposal profile is enabled
- **THEN** the LLM may return a bounded data-only candidate decomposition and declarative formula referencing only supplied governed primitives
- **AND** the proposal does not convert company values, execute code, edit a catalog, or approve itself

#### Scenario: Candidate formula is mechanically provable
- **WHEN** program code can independently recompute a proposed multiplier from existing governed prefixes, prove dimensional compatibility, reject cycles and prohibited transformations, and pass exact round-trip test vectors
- **THEN** it appends an `auto_approved` rule to the governed runtime overlay
- **AND** it creates a new catalog version and replays matching pending artifacts automatically
- **AND** normalization and publication paths may use the rule only after its deterministic proof and catalog-version transaction commits

#### Scenario: Candidate formula depends on model assertion
- **WHEN** a proposal introduces a new base dimension, contextual or non-linear conversion, implicit FX rate, unproved multiplier, or ambiguous semantic mapping
- **THEN** the system quarantines the proposal and keeps the original fact pending
- **AND** it does not auto-maintain the production conversion rules from that proposal
- **AND** model confidence, repeated model agreement, or successful extraction cannot promote or activate the rule

### Requirement: All business-profile calculations are program-owned
The production extraction LLM SHALL return source-reported numeric values and source units unchanged, and deterministic program code SHALL perform every conversion, percentage, ratio, total, difference, margin, concentration, ranking, materiality, confidence, and exposure calculation.

#### Scenario: Source reports a percentage
- **WHEN** annual-report evidence explicitly reports `18.41%`
- **THEN** the LLM returns source value `18.41` and source unit `%`
- **AND** program code converts it to the governed fraction and records conversion lineage

#### Scenario: Source provides inputs but not a derived result
- **WHEN** evidence reports revenue and cost but does not report gross margin
- **THEN** the LLM returns only the reported revenue/cost values and units
- **AND** program code may calculate a separately identified derived margin

#### Scenario: Business logic needs a score or aggregate
- **WHEN** downstream value-chain, concentration, materiality, confidence, ranking, or commodity-exposure logic needs arithmetic
- **THEN** deterministic versioned code calculates the result from governed inputs
- **AND** no LLM-provided calculated value is accepted as authoritative

### Requirement: Structured numeric identities are reconciled before persistence
The structured semantic runtime SHALL calculate applicable arithmetic identities after unit normalization and SHALL never mark numeric reconciliation successful without executing the corresponding check.

#### Scenario: Revenue, cost, and reported margin agree
- **WHEN** a row contains revenue, segment cost, and reported gross margin in compatible canonical units
- **THEN** the system calculates `(revenue - segment_cost) / revenue` using exact decimal values
- **AND** it accepts the row only when the result agrees within the precision-derived tolerance with a one-basis-point default floor and ten-basis-point hard ceiling unless a versioned industry rule applies
- **AND** it persists the calculation inputs, tolerance, result, and pass status

#### Scenario: Reported margin conflicts with revenue and cost
- **WHEN** the calculated margin differs from the reported margin beyond tolerance
- **THEN** the row and its bundle are classified `numeric_reconciliation_failed`
- **AND** the reported value is not silently overwritten by the calculated value
- **AND** the candidate is not publishable

#### Scenario: Margin is not disclosed
- **WHEN** revenue and cost are disclosed but gross margin is absent
- **THEN** the system may persist a separately identified derived margin with deterministic derivation lineage
- **AND** it does not claim that a source-reported margin was reconciled

#### Scenario: Identity is not applicable
- **WHEN** required values are missing, zero-revenue handling is undefined, or dimensions are incompatible
- **THEN** reconciliation status is explicitly `not_applicable` or failed as appropriate
- **AND** it is never defaulted to successful

### Requirement: Validated semantic responses are replayable conversion inputs
The system SHALL persist an immutable, bounded semantic artifact after response-schema and evidence-scope validation but before unit conversion, numeric reconciliation, or candidate persistence.

#### Scenario: Conversion fails after a successful LLM response
- **WHEN** a validated semantic response cannot be converted under the current unit or fact catalog
- **THEN** the response JSON, evidence references, input and response hashes, model lineage, schema identity, and usage are persisted as a conversion-pending artifact
- **AND** the business work item remains resumable

#### Scenario: Conversion retry begins
- **WHEN** a compatible conversion-pending artifact exists for the same immutable input and processing identity
- **THEN** the worker replays that artifact before considering a new LLM request
- **AND** token accounting records zero new model tokens for the replay

#### Scenario: Artifact scope no longer matches
- **WHEN** the document, report period, selected evidence, prompt/schema identity, or input hash differs
- **THEN** the old artifact is retained for audit but not replayed as current input

### Requirement: Invalid shadow candidates recover automatically
The system SHALL automatically identify and make non-publishable structured shadow candidates produced by invalid reconciliation or obsolete semantic/unit identities, while retaining immutable source and audit history.

#### Scenario: Existing inconsistent candidate is found
- **WHEN** an existing candidate fails deterministic numeric reconciliation under the corrected implementation
- **THEN** it is marked rejected or superseded through governed history
- **AND** the corresponding work is requeued at the earliest reusable stage
- **AND** no operator must edit the candidate manually

#### Scenario: Existing unit-blocked retry is found
- **WHEN** a retry exception contains a complete validated semantic result and matching evidence hashes
- **THEN** recovery creates or reuses a replayable semantic artifact
- **AND** it resumes conversion without redownloading the annual report or repeating successful parse work

#### Scenario: Approved history is encountered
- **WHEN** deterministic audit detects a conflict in a previously approved record
- **THEN** the system preserves that record and creates a production-readiness blocker with full lineage
- **AND** it does not silently mutate approved history

### Requirement: Structured semantic concurrency is configurable and adaptive
The backfill SHALL support a configured ceiling of ten concurrent structured semantic requests while keeping shared gateway admission, provider limits, token budgets, timeout handling, and adaptive congestion control authoritative.

#### Scenario: Shadow backfill uses the new default
- **WHEN** structured-shadow backfill starts without an explicit semantic concurrency override
- **THEN** it requests a maximum semantic concurrency of ten
- **AND** parse and semantic work run outside the SQLite write gate

#### Scenario: Gateway admits fewer requests
- **WHEN** provider or pool limits admit fewer requests than the stage requests
- **THEN** the stage obeys the gateway admission result without bypassing or creating a second provider client

#### Scenario: Provider congestion increases
- **WHEN** timeout, transient-error, or queue-wait thresholds exceed configured limits
- **THEN** adaptive control reduces effective semantic concurrency for the cooldown window
- **AND** progress metrics expose requested, admitted, in-flight, throttled, and failed counts

### Requirement: Storage initialization occurs once per backfill run
The business-profile service SHALL complete schema readiness before starting workers and SHALL NOT execute full research, financial, valuation, or interests database initialization for each work item or stage.

#### Scenario: Backfill workers start
- **WHEN** a business-profile backfill run is created
- **THEN** storage initialization and migration readiness complete once before work is claimed
- **AND** every stage receives an initialized storage dependency or cheap readiness token

#### Scenario: Multiple work items execute
- **WHEN** acquire, parse, semantic, and publish workers process multiple companies
- **THEN** no stage invocation repeats full schema initialization
- **AND** initialization count is reported in run metrics

### Requirement: The single writer uses bounded observable transactions
Business-profile database writes SHALL remain serialized but SHALL be limited to short queue or bundle transactions, SHALL query only referenced evidence during bundle validation, and SHALL yield configured database access to other applications.

#### Scenario: Semantic bundle is persisted
- **WHEN** a converted field-family bundle is ready
- **THEN** its candidate rows, referenced evidence, semantic run, and state transition are written in a bounded bulk transaction
- **AND** evidence validation does not scan the entire evidence table

#### Scenario: Network or model request is running
- **WHEN** a worker downloads, parses, or awaits an LLM response
- **THEN** it does not hold the SQLite writer gate

#### Scenario: Writer telemetry is reported
- **WHEN** a backfill batch ends
- **THEN** the report includes transaction count, p50/p95/maximum transaction duration, lock duty, cumulative wait, maximum pending writers, and configured inter-write yield
- **AND** configured writer-duty or long-transaction thresholds can mark the run degraded

### Requirement: Production readiness uses observed semantic quality
Structured promotion SHALL remain disabled until a bounded shadow run proves Chinese output compliance, deterministic unit resolution behavior, numeric reconciliation, replay behavior, and acceptable writer and gateway metrics.

#### Scenario: Readiness audit passes
- **WHEN** the bounded validation cohort has no unclassified unit loss, no unproved or quarantined unit rule used for normalization/publication, no unconditional reconciliation success, no inconsistent publishable row, no repeated successful LLM call for conversion retry, and no storage/concurrency threshold breach
- **THEN** the system may produce a promotion manifest for the structured phase

#### Scenario: Readiness audit fails
- **WHEN** any required quality or operational metric fails
- **THEN** the phase remains shadow-only
- **AND** machine-readable blockers identify the affected instruments, field families, units, rows, or runtime metric
