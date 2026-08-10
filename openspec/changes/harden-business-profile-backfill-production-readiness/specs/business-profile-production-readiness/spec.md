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

#### Scenario: Model includes a non-authoritative derived hint
- **WHEN** an otherwise schema-valid response includes a `model_derived_hints` object such as a suggested margin, ratio, or unit multiplier
- **THEN** the system accepts the valid source facts, semantic summary, and evidence references
- **AND** it stores the hint as non-authoritative diagnostic data
- **AND** it recomputes or ignores the hint without rejecting the whole response

#### Scenario: One field is invalid while other fields are valid
- **WHEN** one extracted fact has a missing source unit or invalid evidence reference but other facts pass validation
- **THEN** the system preserves the valid fields and marks only the invalid field for machine rework
- **AND** it retries the LLM only when the invalid field cannot be repaired locally

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
- **WHEN** a source unit uses a classifier such as `颗`, `粒`, `羽`, `只`, `瓶`, `盒`, `袋`, `板`, `腔`, `台`, or `套`
- **THEN** the program maps the unit to the governed count dimension
- **AND** it retains the original classifier in lineage

#### Scenario: Compound classifier shares one dimension
- **WHEN** a source unit such as `只/瓶` or `瓶/支/盒/袋/板` contains any bounded number of alternatives that all resolve to the count dimension
- **THEN** the program may normalize the value to the canonical count unit
- **AND** it preserves every source alternative in lineage

#### Scenario: Ampere-hour capacity is resolved without energy inference
- **WHEN** a source unit is `Ah`, `mAh`, `kAh`, `万Ah`, `安时`, or `安培小时`
- **THEN** deterministic code resolves it to the governed electric-charge dimension
- **AND** it applies only the explicit scale multiplier
- **AND** it does not convert the value to `Wh` or `kWh` without voltage and derivation lineage

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
- **THEN** the LLM may return a bounded data-only candidate decomposition and declarative formula referencing only supplied governed primitives, dimensions, and canonical units
- **AND** round-trip vectors contain numeric source and canonical values without unit text
- **AND** the proposal does not convert company values, execute code, edit a catalog, or approve itself

#### Scenario: Candidate formula is mechanically provable
- **WHEN** program code can independently recompute a proposed multiplier from existing governed prefixes, prove dimensional compatibility, reject cycles and prohibited transformations, and pass exact round-trip test vectors
- **THEN** it appends an `auto_approved` rule to the governed runtime overlay
- **AND** it creates a new catalog version and replays matching pending artifacts automatically
- **AND** normalization and publication paths may use the rule only after its deterministic proof and catalog-version transaction commits

#### Scenario: Governed linear alias is automatically maintained
- **WHEN** the LLM maps an unknown token to an existing governed dimension and canonical unit using a bounded linear formula
- **AND** program code verifies the target vocabulary, primitive dimensions, source magnitude, exact multiplier, and numeric round trip
- **THEN** the system persists the mapping as a reusable runtime unit rule rather than a one-fact answer
- **AND** it activates the proved rule, creates a catalog version, replays matching artifacts, and sends an informational Telegram notification without routine human approval

#### Scenario: Candidate formula depends on model assertion
- **WHEN** a proposal introduces a new base dimension, contextual or non-linear conversion, implicit FX rate, unproved multiplier, or ambiguous semantic mapping
- **THEN** the system quarantines the proposal and keeps the original fact pending
- **AND** it does not auto-maintain the production conversion rules from that proposal
- **AND** model confidence, repeated model agreement, or successful extraction cannot promote or activate the rule

### Requirement: Unknown-unit rules are persistent, observable, and correctable
Every unknown source-unit proposal SHALL be stored as an append-only governed rule record with lifecycle state, proof lineage, catalog version, affected-fact counts, and replay status; unknown units SHALL NOT be solved only in memory for one fact.

#### Scenario: Unknown unit is first observed
- **WHEN** a source unit cannot be resolved by the current deterministic catalog
- **THEN** the system stores the raw unit, bounded source context, proposal JSON, input hashes, and status `proposed`
- **AND** it emits at most one deduplicated Telegram notification for the rule and current impact window
- **AND** the semantic artifact remains replayable without another extraction LLM call

#### Scenario: Mechanically provable rule is promoted
- **WHEN** deterministic proof derives the dimension and multiplier from governed primitives and exact test vectors pass
- **THEN** the system appends an `auto_approved` rule and a new catalog version
- **AND** matching pending facts are replayed automatically
- **AND** the rule becomes usable only after the catalog transaction commits

#### Scenario: Uncertain linear rule enters shadow use
- **WHEN** a candidate is linear and bounded but cannot yet be derived solely from existing primitives
- **THEN** the system stores it as `shadow_active` and may use it for non-publishable shadow calculations
- **AND** it gathers independent model agreement, repeated source observations, and reconciliation outcomes
- **AND** it promotes the rule automatically once configured corroboration thresholds pass

#### Scenario: Rule is corrected after activation
- **WHEN** later evidence or deterministic checks show that an active rule is wrong
- **THEN** the system appends a superseding rule and new catalog version instead of mutating history
- **AND** it automatically replays all affected semantic artifacts and reports the affected-fact count
- **AND** Telegram receives a correction notification with the old and new rule identities

#### Scenario: Operator corrects a remaining wrong proposal
- **WHEN** automated retries still leave a wrong proposal and an operator supplies a governed dimension, its governed canonical unit, and an exact positive multiplier
- **THEN** the system rejects unknown dimensions, mismatched canonical units, non-positive values, and formula strings
- **AND** it appends an `auto_approved` replacement with explicit operator-correction lineage and marks the old rule `superseded`
- **AND** it replays affected semantic artifacts and sends informational Telegram notices without editing prior rows

#### Scenario: Rule is unsafe or ambiguous
- **WHEN** a proposal requires a new dimension, FX, contextual/non-linear conversion, or has contradictory evidence
- **THEN** the system stores it as `quarantined`, keeps affected canonical facts pending, and sends a deduplicated Telegram alert
- **AND** no routine manual review task is created unless the quarantine remains unresolved after automated retries

#### Scenario: Catalog release resolves a quarantined rule
- **WHEN** deterministic parsing under the current catalog can resolve a previously quarantined source unit
- **THEN** the system appends an auto-approved replacement rule and supersedes the quarantined proposal
- **AND** it automatically replays all observed semantic artifacts with zero extraction LLM calls
- **AND** the notification identifies the effective replacement and prior quarantine reason

#### Scenario: Unit-rule notification is actionable
- **WHEN** a unit rule is activated, quarantined, promoted, or superseded
- **THEN** Telegram states whether the rule is effective, its stable reason codes, affected instruments, multiplier, canonical unit, and rule identity
- **AND** repeated events for the same normalized unit, lifecycle state, and impact window are aggregated or deduplicated

#### Scenario: One unit has quarantine and replacement events
- **WHEN** an earlier quarantined proposal is superseded by an auto-approved rule
- **THEN** the delivered notification states the final current status as enabled
- **AND** it identifies both the effective replacement and the superseded rule
- **AND** historical quarantine wording cannot be mistaken for the current effective state

#### Scenario: Count classifiers observed in production are resolved
- **WHEN** the source unit is `万张`, `点` in an explicit item-count context, or same-scale alternatives such as `万粒/万瓶`
- **THEN** deterministic code resolves the count dimension and exact magnitude
- **AND** matching quarantined artifacts are replayable without another extraction LLM call

#### Scenario: Parenthesized alternatives cross dimensions
- **WHEN** a source unit such as `万台（万千瓦时）` combines count and energy dimensions
- **THEN** it remains non-publishable with a stable cross-dimension reason
- **AND** the system does not create a single unit rule or multiplier for the combined text

### Requirement: Authoritative business-profile calculations are program-owned
The production extraction LLM SHALL provide source-reported numeric values and source units as the only authoritative numeric inputs. It MAY provide qualitative semantic conclusions and non-authoritative derived hints; deterministic program code SHALL perform every authoritative conversion, percentage, ratio, total, difference, margin, concentration, ranking, materiality, confidence, and numeric exposure calculation.

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

#### Scenario: Commodity exposure requires semantic interpretation
- **WHEN** evidence describes a commodity, product, input, customer, or directional exposure without a directly reported numeric exposure value
- **THEN** the LLM identifies the semantic commodity and relationship as a qualitative assertion with evidence
- **AND** program code derives any numeric score, ratio, amount, ranking, or aggregate from governed source facts
- **AND** the semantic assertion is not rejected merely because no arithmetic result is reported in the source

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

#### Scenario: Optional model-derived fields accompany a valid artifact
- **WHEN** a validated response contains non-authoritative model-derived hints alongside valid source facts
- **THEN** the artifact stores both the source envelope and the hints with separate authority labels
- **AND** conversion or reconciliation retries do not call the extraction LLM again solely because a hint was ignored

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

#### Scenario: Explicit governed table receives an empty semantic response
- **WHEN** selected evidence contains an explicit governed table but structured semantic extraction returns zero rows
- **THEN** the queue automatically returns the item to selection once and expands the table context
- **AND** the next extraction is a bounded automatic retry
- **AND** only a second empty result is finalized as machine rework

#### Scenario: Approved history is encountered
- **WHEN** deterministic audit detects a conflict in a previously approved record
- **THEN** the system preserves that record and creates a production-readiness blocker with full lineage
- **AND** it does not silently mutate approved history

### Requirement: Structured semantic concurrency is configurable and adaptive
The backfill SHALL support a configured ceiling of twenty concurrent structured semantic requests while keeping shared gateway admission, provider limits, token budgets, timeout handling, and adaptive congestion control authoritative.

#### Scenario: Shadow backfill uses the new default
- **WHEN** structured-shadow backfill starts without an explicit semantic concurrency override
- **THEN** it requests a maximum semantic concurrency of twenty
- **AND** parse and semantic work run outside the SQLite write gate

#### Scenario: Gateway admits fewer requests
- **WHEN** provider or pool limits admit fewer requests than the stage requests
- **THEN** the stage obeys the gateway admission result without bypassing or creating a second provider client

#### Scenario: Provider congestion increases
- **WHEN** timeout, transient-error, or queue-wait thresholds exceed configured limits
- **THEN** adaptive control reduces effective semantic concurrency for the cooldown window
- **AND** progress metrics expose requested, admitted, in-flight, throttled, and failed counts

#### Scenario: Upstream produces semantic work incrementally
- **WHEN** parse produces fewer items than the semantic concurrency ceiling
- **THEN** semantic workers start those items immediately and refill each free slot as new work becomes claimable
- **AND** they do not wait for an entire claimed wave to finish before polling for replacement work
- **AND** queue underfill is reported separately from provider throttling

#### Scenario: Publish waits for semantic completion
- **WHEN** publish temporarily has no claimable work while semantic is still running
- **THEN** publish keeps polling without consuming its active-work budget
- **AND** it drains newly published semantic outputs before ending or reaching its own completed-work bound

### Requirement: Immutable PDF page artifacts are reused before extraction
The backfill SHALL resolve and validate an existing page-artifact identity before parsing an archived PDF and SHALL share one hydrated page artifact across field-family selection plans for the same document.

#### Scenario: Matching page artifact already exists
- **WHEN** content hash, extractor version, parameter hash, schema, and artifact hash all match
- **THEN** the system reads the immutable artifact without invoking PDF text extraction
- **AND** it records cache-read and validation timings

#### Scenario: Two field families use the same annual report
- **WHEN** structured segments and operating facts select evidence from the same source document in one work item
- **THEN** both plans reuse one hydrated page artifact and one report outline
- **AND** the PDF is not parsed twice

#### Scenario: Existing artifact is corrupt or incompatible
- **WHEN** stored schema, content identity, parameters, or artifact hash does not match
- **THEN** the system fails closed with an actionable artifact error
- **AND** it does not silently overwrite the immutable artifact

#### Scenario: PDF parser emits repetitive handled warnings
- **WHEN** pypdf repairs repeated malformed cross-reference entries under non-strict parsing
- **THEN** the system suppresses per-entry log flooding and emits one bounded per-document warning summary
- **AND** parse failures and affected file identity remain visible

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
- **WHEN** the bounded validation cohort has no unclassified unit loss, no unproved or quarantined unit rule used for canonical publication, no unconditional reconciliation success, no inconsistent publishable row, no repeated successful LLM call for conversion retry, and no storage/concurrency threshold breach
- **THEN** the system may produce a promotion manifest for the structured phase

#### Scenario: Readiness audit fails
- **WHEN** any required quality or operational metric fails
- **THEN** the phase remains shadow-only
- **AND** machine-readable blockers identify the affected instruments, field families, units, rows, or runtime metric
