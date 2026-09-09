## ADDED Requirements

### Requirement: Refined Evidence is validated by one controlled full-cohort replay
The system MUST permit exactly one provider-bearing replay of the frozen twenty-report manufacturing/materials shadow cohort using the refined v2 Evidence plan, a new immutable batch identity, the same primary Gemini logical profile, and the same extract-output, verify-output, request-deadline, and provider-call budgets as the first batch. Before the first provider request, the replay MUST validate the manifest revision and hash, every report and PDF-artifact identity, the v2 plan and preparation-audit hashes, required-field ownership, supported field assignments, bounded table context, and 100% Evidence traceability. The system MUST reject a different cohort, historical plan, provider profile, budget, existing output identity, or incomplete preparation without consuming the authorized replay.

#### Scenario: Refined replay inputs match the frozen contract
- **WHEN** the archived manifest, v2 Evidence plan, provider-free preparation audit, model profile, budgets, and new batch identity all match the replay contract
- **THEN** the existing report-local semantic owner executes all twenty reports once and persists each report result separately
- **AND** no candidate is imported from the first batch or any targeted/model-comparison run

#### Scenario: A replay input or budget differs
- **WHEN** a manifest/PDF/plan/preparation hash, provider profile, output budget, deadline, call ceiling, or batch identity differs from the frozen replay contract
- **THEN** admission fails before any semantic provider request
- **AND** the invalid attempt does not authorize a replacement plan or relaxed parameter

#### Scenario: The authorized replay encounters execution failures
- **WHEN** one or more report scopes end with a typed provider, schema, preparation, deadline, or verification failure after provider execution has begun
- **THEN** the batch records the failures and continues under the existing report-isolation policy
- **AND** the run is closed as the authoritative replay without targeted reruns or cross-run splicing

### Requirement: Replay results are compared through immutable source-bound evidence
The system MUST produce an immutable before/after audit outside both batch directories that binds the first and refined batch manifests, report outputs, review packages, readiness audits, and Evidence plans by content hash. The audit MUST report absolute values and deltas for execution completion, report-status distribution, accepted facts, Evidence traceability, frozen reason codes, unresolved human-review median/p90, provider calls and failures, input/output tokens, latency, source-review completeness, sampled precision, critical semantic errors, and readiness decision. Every blocker, caveat, unresolved item, proposed adjudication, and stable six-chapter sample in the refined batch MUST remain available with original quote, physical page, Evidence identity, runtime target, disposition, usage restriction, and recommendation.

#### Scenario: A valid refined result is compared with v1
- **WHEN** both complete immutable batch inputs and their review/readiness artifacts pass hash validation
- **THEN** the comparison audit records the v1 value, refined value, and delta for every contracted metric and reason code
- **AND** it does not modify or reinterpret either batch

#### Scenario: A comparison input was modified or is incomplete
- **WHEN** a source manifest, report file, review package, readiness audit, or declared content hash cannot be validated
- **THEN** comparison fails without writing an audit artifact
- **AND** no summary-only or partially reconstructed result is accepted

### Requirement: Refined replay readiness remains empirical and research-only
The refined replay MUST retain the existing scale-readiness gates and MUST disclose `ready`, `hold`, or `failed` from the actual reviewed result. Completion of this validation MUST NOT depend on a favorable status, but a later restricted production-promotion design MUST NOT begin unless execution completion is at least 95%, at least 90% of reports are `usable` or `usable_with_caveats`, accepted-record Evidence traceability is 100%, source review is complete with sampled precision at least 99% and zero critical semantic errors, and unresolved human-review median/p90 are no greater than two/five. All replay and comparison outputs MUST retain `production_authorization=not_authorized` and MUST NOT write approved data or enable Stage 6 or production downstreams.

#### Scenario: Every empirical readiness gate passes
- **WHEN** the reviewed refined batch satisfies every existing scale-readiness threshold with no frozen critical semantic error
- **THEN** the audit may state that a separate restricted production-promotion design is empirically justified
- **AND** the current change still retains research-only data and `production_authorization=not_authorized`

#### Scenario: One or more readiness gates fail
- **WHEN** the refined batch is `hold` or `failed`, or any reviewed readiness threshold is unmet
- **THEN** the validation closes with the exact failed gates and source-bound findings
- **AND** it does not trigger a targeted rerun, semantic-rule relaxation, production write, or Stage 6 activation
