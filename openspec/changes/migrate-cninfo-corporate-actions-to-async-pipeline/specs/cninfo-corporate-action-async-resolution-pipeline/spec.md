## ADDED Requirements

### Requirement: CNInfo company-action resolution SHALL use a staged asynchronous pipeline
The system SHALL process SSE and SZSE unresolved CNInfo company-action events through bounded stages for inventory, search-window construction, official announcement discovery, title classification, attachment retrieval, PDF/OCR parsing, semantic extraction, semantic verification, deterministic validation, persistence, and factor eligibility. Independent ready stages SHALL overlap without a global per-instrument or per-phase barrier. BSE MUST remain excluded from CNInfo company-action routing unless a separate supported-source decision is approved.

#### Scenario: Work continues while one model request is pending
- **WHEN** title classification for one instrument is waiting on the LLM and other instruments or downstream document work are ready
- **THEN** the pipeline continues eligible work within resource limits instead of blocking on that request

#### Scenario: BSE request is not silently mixed into CNInfo history
- **WHEN** a task includes BSE in its requested exchanges
- **THEN** the CNInfo company-action pipeline reports BSE as unsupported/skipped according to policy and does not supplement the independent CNInfo tables with TDX rows

### Requirement: Official announcement access SHALL use the unified announcement module
Announcement discovery and attachment retrieval SHALL call `research.announcements` with the company-action purpose, source-qualified identities, approved routing, audit, attachment trust, pacing, and failure semantics. The company-action pipeline MUST NOT construct CNInfo transport parameters, download URLs, cookies, or fallback routing directly.

#### Scenario: Selected announcement is retrieved through the common boundary
- **WHEN** title classification selects a CNInfo announcement for body analysis
- **THEN** its attachment is retrieved by the unified announcement retriever and the resulting source ID, final URL, content hash, media diagnostics, and audit lineage are retained

#### Scenario: Provider failure is not converted to empty evidence
- **WHEN** announcement discovery or retrieval fails or is indeterminate
- **THEN** the event is routed to a classified retry/problem outcome and is not treated as proof that no relevant announcement exists

### Requirement: Title relevance SHALL be classified semantically with complete bundle identity
For each bounded instrument/search-window bundle, the pipeline SHALL send all eligible announcement titles to the configured LLM title classifier after only deterministic identity, date-window, duplicate, attachment, and document-type filtering. The structured response MUST contain exactly one source-qualified decision for every supplied announcement ID, including relevance, likely announcement role, confidence, and reason. Keyword lists MUST NOT be the authoritative relevance classifier.

#### Scenario: Unfamiliar wording can still be selected
- **WHEN** a relevant implementation announcement uses wording absent from local keyword lists
- **THEN** it remains in the title bundle and can be selected by the semantic classifier

#### Scenario: Incomplete title response fails closed
- **WHEN** the LLM omits, duplicates, or invents an announcement ID in its bundle response
- **THEN** the bundle fails schema/business validation and no ambiguous title decision advances to document extraction

### Requirement: All company-action LLM stages SHALL share one provider budget
Title classification, extraction, schema repair, and semantic verification SHALL use the common provider/account coordinator and one aggregate bulk target of 50 calls, subject to the provider hard ceiling of 60 shared with other business workflows. No company-action stage or profile may create an independent 50-call pool outside that coordinator, and no LLM lease may be held during download, parsing, deterministic validation, or persistence.

#### Scenario: Title and body calls share capacity
- **WHEN** title, extraction, and verification requests are simultaneously ready
- **THEN** their combined active calls remain within the one configured company-action/provider budget and admission fairness allows each ready class to progress

#### Scenario: Other LLM business remains protected
- **WHEN** a company-action backfill is active and another workflow submits calls to the same provider account
- **THEN** the common coordinator enforces the account ceiling and configured fairness rather than allowing the backfill to consume all capacity indefinitely

### Requirement: Document preparation SHALL have independent bounded resources
Attachment downloads SHALL use a separately configured bounded concurrency and source pacing. PDF text extraction and OCR SHALL share a CPU parsing pool of no more than eight active workers. Inter-stage queues SHALL carry immutable artifact/page references rather than retaining unbounded PDF bytes. OCR SHALL run only when explicitly enabled and native text quality is insufficient under bounded document rules.

#### Scenario: More than eight PDFs are ready
- **WHEN** more than eight documents await parsing
- **THEN** at most eight are parsed concurrently and remaining artifacts wait in a bounded queue without consuming LLM leases

#### Scenario: Duplicate artifact is reused
- **WHEN** the same source-qualified announcement ID and content hash are selected by resumed or related event work
- **THEN** the immutable artifact and valid page extraction are reused rather than downloaded and parsed again

### Requirement: Semantic extraction SHALL remain schema-driven and evidence-bound
The business adapter SHALL ask the LLM to return the versioned CNInfo company-action schema containing event match/stage, typed date facts, economic terms/primitives, exact official quotes, announcement/page references, conflicts, and uncertainty. Program logic SHALL validate schema and evidence lineage but MUST NOT replace semantic interpretation with an expanding phrase-enumeration parser.

#### Scenario: Model identifies multiple roles for one date
- **WHEN** an official quote explicitly states that ex-date, listing date, and resumption date are the same day
- **THEN** the structured result may attach that date to every supported role using the same valid evidence reference

#### Scenario: Unsupported inference is not promoted
- **WHEN** the model proposes a date that is absent from the cited official text or derives a term without the required source primitives
- **THEN** deterministic validation rejects automatic promotion while retaining the analysis for audit

### Requirement: Out-of-order outcomes SHALL retain exact event and evidence identity
Every stage SHALL preserve `source_event_key`, instrument, source-qualified announcement ID, artifact hash, page/section hash, run ID, stage sequence, request ID, request/input hash, schema/prompt version, and attempt lineage. The writer MUST revalidate these identities and current supersession state before committing.

#### Scenario: Two instruments finish in reverse order
- **WHEN** the second instrument's extraction returns before the first instrument's title result
- **THEN** each outcome advances only its own event and no mutable loop state can attach it to the wrong instrument

#### Scenario: Artifact changes after an earlier analysis
- **WHEN** a source announcement is retrieved with a new content hash
- **THEN** an analysis based on the old artifact is not reused as current evidence and new document processing is scheduled

### Requirement: Deterministically validated candidates SHALL be auto-promotable
When `auto_promote_validated=true`, the system SHALL automatically create resolved evidence only for analyses that pass identity, exact-quote, date-role, event-stage, economic-unit/formula, conflict, document-quality, and source-lineage gates. Manual review SHALL be reserved for genuine ambiguity, conflicts, unsupported semantics, low-quality evidence, or configured audit sampling; LLM origin alone MUST NOT require review. Raw CNInfo observations MUST remain immutable.

#### Scenario: Strong official evidence auto-promotes
- **WHEN** extraction and required verification cite exact official text, all deterministic gates pass, and no conflict remains
- **THEN** the writer atomically records validated analysis and resolved evidence without mandatory human review

#### Scenario: Conflicting implementation dates require review
- **WHEN** two valid official announcements provide incompatible effective dates and supersession cannot be established deterministically
- **THEN** neither date is auto-promoted and the event enters a structured manual/conflict queue

#### Scenario: Derived validation diagnostics survive governed promotion
- **WHEN** a validated candidate contains an explicitly supported deterministic diagnostic field that is not part of the strict LLM response schema
- **THEN** governed review excludes only that diagnostic from strict schema input, reruns deterministic validation, retains the recomputed diagnostic in audit output, and permits promotion when all gates pass

#### Scenario: Unknown public analysis field fails closed
- **WHEN** resolution is requested for a stored candidate containing a non-internal public field that is neither part of its versioned LLM schema nor an explicitly supported deterministic diagnostic
- **THEN** governed review blocks resolved evidence instead of silently removing or promoting the unknown field

#### Scenario: Malformed analysis can still be quarantined
- **WHEN** an operator records a rejected, conflict, or manual-required disposition for an analysis that cannot satisfy the resolved-result schema
- **THEN** the review preserves the original analysis and records the negative disposition without creating resolved evidence or requiring the promotion schema to pass

### Requirement: SQLite persistence SHALL be serialized, atomic, and idempotent
All company-action pipeline mutations SHALL pass through a bounded writer queue with one SQLite consumer by default. Compatible writes MAY be batched, but each event's analysis, validation, audit, promotion/review outcome, and stage checkpoint SHALL obey defined atomic transaction boundaries. Workers MUST NOT keep a database transaction open while awaiting external or CPU work.

#### Scenario: Fifty results become ready together
- **WHEN** many LLM outcomes complete concurrently
- **THEN** they queue for bounded serial persistence without database-lock retry storms or consumption of LLM slots

#### Scenario: Commit fails before checkpoint
- **WHEN** an event write transaction rolls back
- **THEN** no terminal checkpoint is acknowledged and the event remains eligible for safe resume without a partial promoted fact

### Requirement: Resume and cache reuse SHALL be input-version aware
Resume SHALL reuse only committed outcomes whose event identity, source artifact/page hashes, prompt version, schema version, model policy, and normalized input hash remain compatible. Failed, incomplete, changed-input, or operator-forced items SHALL rerun. Queue position and response arrival order MUST NOT determine resume state. Dry-run MUST NOT create resolved evidence, approvals, factor rows, or committed business-stage checkpoints.

#### Scenario: Unchanged successful analysis resumes without another model call
- **WHEN** an event has a committed successful analysis with identical input and version lineage
- **THEN** resume reuses it and advances from the next required stage

#### Scenario: Prompt version changes
- **WHEN** the business prompt version changes for an otherwise unchanged event
- **THEN** the previous response remains auditable but is not reused as the current analysis

### Requirement: Residual outcomes SHALL be routed by remediable cause
The governance inventory SHALL classify non-promoted analyses using persisted verifier
status, document-context completeness, event stage, review reason codes, and deterministic
gate results. Provider failures SHALL route to failed-stage retry, incomplete document
context SHALL route to context repair, proposal-only evidence SHALL route to implementation
rediscovery, source-event mismatch SHALL route to conflict review, and complete
evidence-insufficient analyses SHALL route to human review. These non-terminal states
MUST remain factor-blocking and MUST NOT all collapse into a generic LLM retry queue.

#### Scenario: Proposal evidence does not loop through extraction
- **WHEN** the latest analysis only supports a proposed action and no implementation-grade
  evidence has been resolved
- **THEN** the event returns to implementation discovery even if proposal candidates are
  already stored

#### Scenario: Omitted implementation pages are repaired before rediscovery
- **WHEN** a proposal-stage analysis has an incomplete prompt context and its archived
  candidate pages include omitted or truncated sections
- **THEN** the event enters bounded document-context repair before returning to
  implementation discovery, while a completed repair that still lacks implementation
  evidence remains eligible for rediscovery

#### Scenario: Complete insufficient evidence becomes review work
- **WHEN** semantic verification completed but the official text does not bind a usable
  date or economic term
- **THEN** the event enters evidence-bound human review instead of repeatedly calling the
  same extraction pipeline

#### Scenario: Incomplete provider result stays retryable
- **WHEN** extraction or semantic verification ends with a retryable provider failure
- **THEN** the event remains in a failed-stage retry queue and is not represented as a
  genuine human evidence conflict

#### Scenario: Document repair changes the machine input
- **WHEN** an analysis is routed to document-context repair because prompt pages were
  omitted or truncated
- **THEN** the governance runner isolates that event, bypasses reuse of the incomplete
  analysis, and submits a bounded archive slice led by the previously omitted sections
  with distinct repair lineage and input identity

#### Scenario: Bounded document repair does not loop forever
- **WHEN** one repair pass still cannot cover the complete archived context
- **THEN** the event moves to evidence-bound review rather than repeatedly submitting the
  same or equivalent document-repair request

### Requirement: Quick review MAY acknowledge complete archived context explicitly
Automatic promotion SHALL continue to require complete original model context. A resolved
quick review MAY explicitly acknowledge context omitted only by prompt page limits after
the review path reloads all archived pages for every cited announcement, confirms no
candidate metadata was omitted, reruns strict schema and deterministic evidence gates, and
stores a deterministic artifact/page lineage and its hash in both the review audit and
review idempotency identity. Missing archives, unsupported public fields, failed evidence
gates, or deep-review classification MUST still block batch resolution.

#### Scenario: Prompt omitted redundant official pages
- **WHEN** a quick-review candidate passed all evidence gates but automatic promotion was
  blocked because the prompt omitted archived pages, and an authorized reviewer explicitly
  acknowledges the fully reloaded archive
- **THEN** governed review may resolve the event after all gates pass without altering the
  original analysis or weakening automatic promotion

#### Scenario: Context acknowledgement is not implicit
- **WHEN** the original context was incomplete and the review payload does not explicitly
  acknowledge the archived context
- **THEN** resolved review remains blocked by the context-complete gate

#### Scenario: Archived evidence changes after an earlier review request
- **WHEN** any reloaded artifact content hash, parser identity, page text hash, extraction
  method, or quality metadata changes
- **THEN** the archived-context hash and review key change, while the complete prior and
  current lineages remain independently auditable

### Requirement: Progress and final reports SHALL be aggregate and operationally useful
Long-running tasks SHALL log periodic aggregate counts, queue depths, active workers, LLM admission/execution latency, download/parse throughput, writer backlog, retries, failures, auto-promotions, and manual/problem outcomes. Telegram SHALL receive bounded summary messages; per-event details SHALL be queryable and only problem details requiring attention may be split into bounded follow-up messages.

#### Scenario: Large full-market task reports progress
- **WHEN** thousands of events are processed
- **THEN** Telegram receives summaries rather than one message per event while logs and queryable records retain enough detail to diagnose individual failures

#### Scenario: Final partial status is truthful
- **WHEN** some events remain failed, conflicted, evidence-unavailable, or manual-required
- **THEN** the task returns partial with counts and problem categories rather than success

### Requirement: Rollout SHALL prove safety before full concurrency
The migration SHALL retain a temporary serial rollback path and SHALL validate dry-run and live execution at concurrency 10, 25, and 50 before the 50-call target is used for full historical work. Validation MUST measure identity correctness, 429/5xx/timeout rates, latency, memory, file descriptors, parse concurrency, database locking, idempotent resume, and output equivalence with the existing evidence policy.

#### Scenario: Intermediate concurrency is unstable
- **WHEN** the 25-call validation exceeds accepted provider errors, resource bounds, or identity correctness gates
- **THEN** rollout stops at or below the last accepted level and full concurrency is not enabled
