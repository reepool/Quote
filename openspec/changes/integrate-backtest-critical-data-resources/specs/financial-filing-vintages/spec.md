## ADDED Requirements

### Requirement: Filing artifacts are immutable revision anchors
The system SHALL use retained financial source files as immutable filing-version identities and SHALL preserve publication, availability, revision, and supersession lineage.

#### Scenario: New filing is archived
- **WHEN** the existing financial disclosure workflow acquires a filing artifact
- **THEN** it SHALL retain stable filing identity, content hash, `published_at`, normalized `available_at`, source profile, attachment lineage, parser version, and correction/amendment classification when known
- **AND** parsed long-form facts SHALL reference that source-file version and an immutable parse revision

#### Scenario: Correction supersedes an earlier filing
- **WHEN** stable ids or explicit official evidence prove a supersession relationship
- **THEN** the system SHALL append a relationship decision identifying both source files, relation type, evidence, and decision `available_at`
- **AND** both versions and their facts SHALL remain queryable

#### Scenario: Supersession is uncertain
- **WHEN** two filings may be related but evidence does not prove their revision relationship
- **THEN** the system SHALL retain both and mark the relationship unresolved
- **AND** it SHALL NOT guess a supersession link

#### Scenario: Relationship decision changes
- **WHEN** later evidence corrects or withdraws an earlier supersession decision
- **THEN** the system SHALL append a new relationship decision that supersedes the prior decision
- **AND** it SHALL preserve the earlier decision and its knowledge interval

### Requirement: Financial period semantics are explicit and evidenced
Every point-in-time financial fact SHALL declare a period semantic of `instant`, `single_quarter`, `ytd`, `annual`, `derived_single_quarter`, or `unknown`, with its semantic basis and quality.

#### Scenario: Source duration context is available
- **WHEN** a filing fact supplies period start and period end
- **THEN** semantic classification SHALL use those source dates and reporting context
- **AND** a `quarterly` label or report period alone SHALL NOT prove single-quarter rather than YTD meaning

#### Scenario: Single-quarter value is derived
- **WHEN** a single-quarter fact is calculated from cumulative facts
- **THEN** it SHALL be marked `derived_single_quarter`, retain the identities of all inputs and derivation version, and use the latest input `available_at`

#### Scenario: Semantic evidence is insufficient
- **WHEN** the system cannot distinguish a duration fact's single-quarter, YTD, or annual meaning
- **THEN** it SHALL mark the fact `unknown`
- **AND** strict semantic filters SHALL exclude it

### Requirement: Filing parses and facts are append-only revisions
The system SHALL preserve each material parse of a filing as an immutable parse revision with its own availability and parser lineage.

#### Scenario: Same filing is reparsed by a newer parser
- **WHEN** a parser, mapping, or fact-catalog version produces materially different facts for an existing `source_file_id`
- **THEN** the system SHALL append a new `parse_revision_id` and versioned fact rows with parser, mapping, catalog, input-hash, and parsed `available_at` lineage
- **AND** it SHALL NOT overwrite the earlier parse revision

#### Scenario: Reparse is semantically unchanged
- **WHEN** a new parse run produces identical facts and lineage-relevant semantics
- **THEN** the run MAY be recorded as unchanged
- **AND** it SHALL NOT create a misleading fact revision or change watermark

### Requirement: Point-in-time financial reads select filing vintages by availability
The system SHALL resolve financial facts from immutable filing and parse revisions that were available by the caller's `known_at`, rather than from the latest compatibility projection.

#### Scenario: Correction was not yet available
- **WHEN** a caller's `known_at` precedes a correction filing's `available_at`
- **THEN** the resolver SHALL select the earlier qualifying filing version if it was available and otherwise eligible
- **AND** it SHALL NOT leak corrected values backward in time

#### Scenario: New parser revision was not yet available
- **WHEN** a caller's `known_at` precedes a later parse revision's parsed `available_at`
- **THEN** the resolver SHALL use the earlier eligible parse revision
- **AND** it SHALL NOT expose facts first produced by the later parser

#### Scenario: Filing availability is unknown
- **WHEN** a filing lacks a trustworthy publication or local availability timestamp
- **THEN** strict PIT mode SHALL omit or reject that filing with an explicit reason
- **AND** it SHALL NOT silently substitute the report period or statutory deadline

#### Scenario: Supersession is discovered later
- **WHEN** a supersession relationship becomes available after the caller's `known_at`
- **THEN** that later relationship SHALL NOT suppress a filing revision that was eligible at the cutoff

#### Scenario: Resolver evaluates supersession at the cutoff
- **WHEN** one or more filing relationship decisions exist
- **THEN** the resolver SHALL use only the latest applicable relationship decision whose decision `available_at <= known_at`
- **AND** it SHALL NOT use the mutable current compatibility link as historical evidence

#### Scenario: Eligible filings conflict without a resolved relationship
- **WHEN** multiple filing revisions available by `known_at` provide conflicting values for the same fact scope and their relationship is unresolved at that cutoff
- **THEN** strict PIT mode SHALL omit or reject the ambiguous fact with an explicit relationship blocker
- **AND** it SHALL NOT choose a value solely by ingestion, parsing, or publication order

#### Scenario: Caller explicitly permits an estimate
- **WHEN** an estimated-availability policy is configured and explicitly requested
- **THEN** the response SHALL label the estimate policy, basis, and reduced quality
- **AND** strict-ready status SHALL remain false unless the strict contract is independently satisfied

### Requirement: Existing financial maintenance owns filing vintages
Filing archive, revision linkage, versioned parsing, and latest-projection updates SHALL extend the existing financial disclosure incremental and reconciliation workflows.

#### Scenario: Incremental disclosure sync finds a filing
- **WHEN** `financial_disclosure_incremental_sync` processes a new or revised artifact
- **THEN** it SHALL archive the immutable filing, append the immutable parse/fact revision, and only then update `financial_facts` or other latest compatibility projections
- **AND** it SHALL reuse the existing announcement acquisition, provider profile, throttling, and report context

#### Scenario: Historical filing gap is reconciled
- **WHEN** reconciliation or an operator-approved backfill searches for missing revisions
- **THEN** it SHALL reuse existing source-neutral announcement and official filing routes with bounded scopes and checkpoints
- **AND** no separate full-market financial downloader SHALL be scheduled for the same source surface

### Requirement: Filing-vintage readiness exposes lineage gaps
The system SHALL report vintage readiness by market, report period, fact scope, and availability range.

#### Scenario: Required revisions or timestamps are missing
- **WHEN** retained filing versions lack publication times, artifacts, semantic classification, or resolved identities
- **THEN** readiness SHALL expose counts and representative blockers
- **AND** strict point-in-time coverage SHALL exclude the affected scope
