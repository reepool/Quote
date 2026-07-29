## ADDED Requirements

### Requirement: Terminal Non-Factor Events Are Excluded
The CNInfo factor path SHALL exclude events governed as `non_effective`,
`scope_mismatch`, or operator-confirmed `superseded` without deleting or
changing the underlying CNInfo source observation.

#### Scenario: Non-effective event lacks an ex-date
- **WHEN** a current CNInfo event has no effective date and its persisted resolution state is `non_effective`
- **THEN** factor derivation records an auditable no-factor exclusion and does not create a missing-date pending root

#### Scenario: Scope-mismatched event lacks an ex-date
- **WHEN** a current CNInfo event has no effective date and its persisted resolution state is `scope_mismatch`
- **THEN** factor derivation records an auditable no-factor exclusion and does not block later events for the instrument

#### Scenario: Operator confirms an observation was superseded
- **WHEN** a current CNInfo observation has no effective date and its persisted resolution state is `superseded`
- **THEN** factor derivation records an auditable no-factor exclusion and does not block the replacement event

### Requirement: Confirmed Pre-Listing Events Do Not Affect Listed Factors
The CNInfo factor path SHALL exclude an event when its selected source or
governed effective date is strictly earlier than the instrument listing date.

#### Scenario: Explicit ex-date is before listing
- **WHEN** a CNInfo event has an ex-date earlier than `listed_date`
- **THEN** the event is classified as `pre_listing_corporate_action`, quote evidence is not required, and no listed-market factor is emitted

#### Scenario: Announcement alone predates listing
- **WHEN** a CNInfo event has no source or governed effective date and only its announcement date predates listing
- **THEN** the system does not classify the event as pre-listing solely from the announcement date

#### Scenario: Operator confirms a frozen announcement-only event set
- **WHEN** an operator explicitly confirms a fixed source-event-key manifest as pre-listing and the review preserves the source-row hash
- **THEN** the system persists terminal state `pre_listing` without inventing an effective date and excludes those events from listed-market factor calculation

#### Scenario: Future announcement-only event is not in the fixed manifest
- **WHEN** a future archive-unavailable event only has a pre-listing announcement anchor and has no explicit operator terminal review
- **THEN** the event remains an archive gap and is not automatically classified as `pre_listing`

### Requirement: Explanatory Zero-Economic Records Do Not Block Factors
The CNInfo factor path SHALL allow an operator-confirmed explanatory record
with no positive cash, bonus, capitalization, or rights term to be terminally
classified as `non_effective`.

#### Scenario: Historical record only describes retained or old-shareholder profit
- **WHEN** the frozen source observation has no positive economic term and an operator confirms that it does not describe a current listed-market distribution
- **THEN** the source observation remains unchanged, the review is auditable, and no factor or missing-date blocker is emitted

### Requirement: Remaining Post-Listing Archive Gaps Are Exported
After fixed terminal decisions are applied, the system SHALL export every
remaining post-listing archive gap for manual review using existing local data.

#### Scenario: Ten unresolved post-listing events remain
- **WHEN** the confirmed pre-listing and zero-economic manifests are applied
- **THEN** the review workbook contains exactly the remaining ten full event keys, CNInfo facts, listing dates, nearby TDX evidence, failure reasons, and blank operator-decision fields
- **AND** no document download, OCR, or LLM invocation occurs

### Requirement: Archive Date Recovery Uses TDX Date Only
For an `official_archive_unavailable` CNInfo event, the system SHALL use a TDX
effective date only when one bounded same-instrument candidate uniquely matches
the CNInfo economic terms. Operational dates SHALL use role-specific
directional windows and take precedence over announcement date; announcement
date SHALL be used only as a short forward-looking fallback.

#### Scenario: Unique TDX economic match exists
- **WHEN** exactly one eligible TDX event matches the CNInfo cash, bonus, rights, and rights-price terms within approved source-precision tolerances
- **THEN** the TDX date and resolvable audit-row identity are recorded as date-only reference evidence and the CNInfo terms are used to calculate the CNInfo factor

#### Scenario: TDX values differ materially
- **WHEN** a nearby TDX event has materially different economic terms
- **THEN** the system does not use its date and does not copy its terms or factor into the CNInfo path

#### Scenario: Multiple TDX candidates match
- **WHEN** more than one eligible TDX event matches the CNInfo economics
- **THEN** the system reports an ambiguous historical gap and does not choose a date

#### Scenario: Announcement-only candidate is too distant
- **WHEN** the only CNInfo date anchor is an announcement and an otherwise matching TDX event occurs outside the short forward announcement window
- **THEN** the system keeps a historical gap instead of assigning the distant TDX date

#### Scenario: Rebuild slice starts after an archive event
- **WHEN** a requested rebuild date range starts after an archive-unavailable CNInfo event but its bounded TDX date candidate is earlier than that range
- **THEN** candidate loading still reads the bounded historical anchor window and produces the same date-match conclusion as a full-history rebuild

### Requirement: Historical Root Gaps Do Not Expand Into Event Pending Rows
An unresolved `official_archive_unavailable` event SHALL remain visible as one
historical root gap while later calculable events remain available with an
incomplete-path marker.

#### Scenario: Later events follow an unresolved archive gap
- **WHEN** an instrument has one unresolved archive gap and multiple later events with complete dates and quote evidence
- **THEN** the result contains one historical root gap, does not emit `prior_unlocated_event_pending` for every later event, and marks later events as having a prior historical gap

#### Scenario: Calculable event predates the historical gap anchor
- **WHEN** a calculable event's effective date is earlier than the unresolved gap's earliest operational anchor, or announcement anchor when operational anchors are absent
- **THEN** that earlier event remains valid and is not marked as having a prior historical gap

#### Scenario: Promotion evaluates historical gaps
- **WHEN** any historical root gap remains in the requested factor universe
- **THEN** completeness and promotion eligibility remain false even if ordinary pending-factor events are otherwise empty

### Requirement: True Calculation Failures Remain Fail-Closed
The system SHALL retain downstream blocking for ordinary unresolved event,
quote, economic-field, and factor-calculation failures not covered by an
explicit no-factor or historical-gap disposition.

#### Scenario: Ordinary event lacks quote evidence
- **WHEN** an in-scope post-listing event has no valid effective trade date and is not a governed historical gap
- **THEN** the event remains pending and later cumulative events remain blocked
