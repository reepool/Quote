## ADDED Requirements

### Requirement: Shareholder coverage is backed by actual local fields
The system MUST derive shareholder coverage from normalized snapshot content for each instrument and MUST NOT treat a claimed scope label or a top-holder list as proof of actual-control coverage.

#### Scenario: Top holders without controller fields
- **WHEN** a local snapshot contains top holders but no supported actual-controller or ownership-control field
- **THEN** the snapshot may satisfy the top-holder scope but does not satisfy `reference_only_ownership_clues`.

#### Scenario: Incomplete or mixed-period top holders
- **WHEN** a main-board top-holder list is incomplete, contains duplicate ranks or names, or mixes report periods
- **THEN** it does not satisfy the required top-ten scope and the inconsistency is reported.

### Requirement: Top shareholder is not inferred to be actual controller
The system MUST preserve a first-ranked holder only as a top-holder fact unless a separate supported local control record explicitly identifies the controller.

#### Scenario: Aggregated provider returns ten holders
- **WHEN** a fallback provider returns a top-ten list whose first row has the largest holding ratio but no controller field
- **THEN** the system stores the holder list and leaves controller name and ratio unset.

#### Scenario: Local controller history supports the controller
- **WHEN** an eligible local control-history record explicitly identifies a controller at the requested cutoff
- **THEN** a local profile projection may return that controller with its source and availability date independently of top-holder rank.

### Requirement: Shareholder scopes merge by period and authority
The shareholder sync owner MUST select holder count, top holders, and ownership clues independently using report period first, configured source authority for equal periods, and content completeness as the final tie-breaker.

#### Scenario: Newer cross-source holder count
- **WHEN** an incoming holder-count scope has a later valid report period than the stored scope
- **THEN** it replaces the stored scope even when the provider differs, and `scope_sources` records the selected provider and period.

#### Scenario: Official top holders arrive after aggregate top holders
- **WHEN** complete official and aggregate top-holder scopes describe the same period
- **THEN** the configured authoritative official scope is selected regardless of ingestion order.

#### Scenario: Older authoritative data arrives
- **WHEN** an authoritative source returns an older report period than the current internally consistent scope
- **THEN** the current newer scope remains selected and the older occurrence remains diagnostic input only.

### Requirement: Shareholder readiness is per-instrument complete
Readiness MUST count instruments for which every required field-backed scope is satisfied and MUST NOT infer readiness from independent aggregate counts that can refer to different instruments.

#### Scenario: Scope totals come from different instruments
- **WHEN** the number of holder-count snapshots and top-holder snapshots each equals the target count but some instruments lack one of the scopes
- **THEN** readiness remains incomplete and reports the actual missing instrument set or count.

#### Scenario: Degraded sync result
- **WHEN** a required exchange, instrument, or scope remains unresolved
- **THEN** scheduler and task reporting preserve `degraded` or failure semantics instead of presenting the run as an unqualified success.

### Requirement: Business-profile shareholder consumption is local-only
Any business-profile or company research projection that includes top holders, actual controller, or control method MUST read the existing local shareholder snapshot and control-history repositories at the requested knowledge cutoff and MUST NOT trigger CNInfo, another remote provider, or an LLM call.

#### Scenario: Local shareholder data exists
- **WHEN** a company-profile query requests shareholder information and eligible local records exist
- **THEN** the response is assembled from those local records with source/report/availability dates.

#### Scenario: Local shareholder data is missing
- **WHEN** the local repositories have no eligible shareholder record at the cutoff
- **THEN** the query returns an explicit local-data gap and does not perform implicit remote acquisition.

### Requirement: Shareholder identity and batch failures fail closed locally
The shareholder owner MUST reject ambiguous security aliases or response-symbol mismatches for the affected instrument and MUST isolate single-instrument provider failures from unrelated instruments in the batch.

#### Scenario: Ambiguous BSE alias
- **WHEN** more than one local instrument could match a generated `920` alias
- **THEN** no automatic association is written and the ambiguity is reported.

#### Scenario: Provider response identifies another security
- **WHEN** the returned security identity does not match the requested canonical instrument
- **THEN** the response is rejected for that instrument without terminating the remaining batch.

#### Scenario: Candidate limit is enabled
- **WHEN** incremental shareholder discovery exceeds a positive candidate limit
- **THEN** missing-required-scope candidates remain prioritized and time-based candidates are selected newest first.

### Requirement: Existing incorrect shareholder projections are repairable locally
The system MUST provide a dry-run-first, idempotent repair mode that recomputes coverage and selected scopes from local snapshots, raw payloads, and control history without remote acquisition.

#### Scenario: Inferred controller is unsupported
- **WHEN** an aggregate-source controller equals the first top holder and no eligible local control record supports that inference
- **THEN** audit reports it and apply mode clears the controller fields and recalculates coverage.

#### Scenario: Local evidence is insufficient
- **WHEN** a required scope cannot be reconstructed from local persisted data
- **THEN** apply mode marks the scope incomplete and reports owner-managed follow-up instead of inventing a value or calling a provider.
