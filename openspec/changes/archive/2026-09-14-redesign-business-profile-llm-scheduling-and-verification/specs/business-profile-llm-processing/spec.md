## ADDED Requirements

### Requirement: Report-level semantic extraction

The system MUST submit at most one joint semantic extraction request per report processing identity and configured field-family bundle, using bounded, chapter-aware Chinese context and a versioned JSON schema.

#### Scenario: One report is extracted

- **WHEN** a parsed annual report has an eligible field-family bundle
- **THEN** the durable extraction stage creates one gateway request and persists its raw response before downstream validation

#### Scenario: Extraction is retried

- **WHEN** the gateway returns a retryable transport, rate-limit, provider, or response-parse failure
- **THEN** the request is durably scheduled with bounded backoff and the report worker is released rather than waiting indefinitely

### Requirement: Single global LLM concurrency

The system MUST enforce one global in-flight request limit through the shared LLM gateway, and report workers MUST NOT create nested provider-request pools or per-report event loops.

#### Scenario: Multiple reports run together

- **WHEN** several reports submit extraction or verification work
- **THEN** the total provider requests in flight never exceed the configured global limit, regardless of report-worker count

#### Scenario: Gateway capacity is exhausted

- **WHEN** a request cannot be admitted within the short admission timeout
- **THEN** the work item becomes `retry_due` with a diagnostic reason and no long-lived running task remains

### Requirement: Deterministic validation of every record

The system MUST validate every extracted record programmatically for schema, issuer and report period, evidence membership, numeric and unit semantics, arithmetic reconciliation, duplicates, conflicts, and catalog/version validity before publication.

#### Scenario: Deterministic checks pass

- **WHEN** all required fields and evidence references are valid and no conflict is found
- **THEN** the record is classified `validated` without an additional LLM request

#### Scenario: Deterministic checks fail or are ambiguous

- **WHEN** a record has missing context, conflicting evidence, unresolved entities, unit problems, or extraction/check disagreement
- **THEN** the record is classified `ambiguous` with reason codes and is eligible for one report-level batch verification item

### Requirement: Batched ambiguity verification

The system MUST place ambiguous records for one report and field family into at most one bounded verification request, whose response contains a target id, supported/unsupported/unclear status, failed aspects, and a Chinese reason for each target.

#### Scenario: Ambiguous records are verified

- **WHEN** a report has one or more ambiguous records
- **THEN** the system sends one bounded batch request and persists each returned target decision independently

#### Scenario: No ambiguous records exist

- **WHEN** deterministic validation classifies every record as safe
- **THEN** the system sends no verification request

### Requirement: Program-owned publication decisions

The system MUST determine publication state from deterministic validation and batch verification outcomes; model confidence or probability MUST be retained only as diagnostic metadata and MUST NOT override a failed deterministic check.

#### Scenario: Supported ambiguous record

- **WHEN** batch verification supports an otherwise non-blocking ambiguous record and required deterministic checks pass
- **THEN** the program marks it `verified` and permits publication

#### Scenario: Unsupported or unclear record

- **WHEN** batch verification returns `unsupported` or `unclear`, or a blocking deterministic check remains
- **THEN** the program marks the record `held` or `rejected` with a machine-readable reason and does not publish it

### Requirement: Scoped recovery and retry

The system MUST scope exception backlog and readiness gates to the current instrument, report identity, field family, and processing identity, and MUST classify retryable failures separately from authentication and deterministic contract failures.

#### Scenario: One company has a blocking failure

- **WHEN** a report for one instrument enters a blocking exception state
- **THEN** unrelated instruments remain claimable and publishable

#### Scenario: Rate limit with failover

- **WHEN** a provider returns a rate limit or transient failure
- **THEN** the gateway honors `Retry-After` when present, applies bounded cooldown/backoff, may fail over to a healthy provider, and durably retries the work

### Requirement: Offline semantic evaluation

The system MUST maintain a versioned offline evaluation set containing representative annual-report section artifacts and expected semantic labels, and MUST run evaluation without downloading reports or calling live providers.

#### Scenario: Regression evaluation

- **WHEN** a prompt, schema, validator, or scheduler changes
- **THEN** tests run against the fixed local corpus and report semantic agreement, evidence support, publication classification, request count, and latency metrics

#### Scenario: Industry coverage

- **WHEN** the initial corpus is assembled
- **THEN** it includes representative manufacturing, energy, pharmaceutical, finance, consumer, mining, and diversified-group reports
