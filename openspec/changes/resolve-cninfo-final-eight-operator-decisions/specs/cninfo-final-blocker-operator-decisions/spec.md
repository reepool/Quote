## ADDED Requirements

### Requirement: Fixed final-blocker manifest
The system SHALL apply the final operator instruction only to the eight frozen
CNInfo source-event identities and MUST reject observation or decision payload
drift.

#### Scenario: Preview exact final decisions
- **WHEN** the local operator script previews the current database
- **THEN** it validates exactly eight event keys, their current CNInfo row
  hashes, dates, factor effects, and complete canonical payload hash without
  writing data

#### Scenario: Reject an unrelated event
- **WHEN** an event identity outside the frozen manifest is introduced
- **THEN** the script rejects the batch rather than treating the instruction as
  a category-wide approval

### Requirement: Audited operator-attestation evidence
The system SHALL allow an explicit operator attestation to resolve an unchanged
current CNInfo event when no usable persisted announcement or LLM analysis
exists, without fabricating either source.

#### Scenario: Approve a pre-listing no-effect distribution
- **WHEN** the operator supplies unchanged CNInfo terms, a structured
  pre-listing basis, an effective date, and `factor_effect=none`
- **THEN** the system writes a resolved review and operator-attestation date
  evidence with no LLM analysis and no resolved economic-term overlay

#### Scenario: Approve an externally funded compensation payment
- **WHEN** the operator identifies a payment funded outside the listed company
  and declares that it caused no market ex-right adjustment
- **THEN** the event remains recorded with its CNInfo cash amount and is
  explicitly excluded from factor aggregation

#### Scenario: Reject an attested economic change
- **WHEN** an analysis-free operator attestation changes a current CNInfo
  economic field
- **THEN** the system rejects the review without writing a bundle

#### Scenario: Preserve evidence provenance
- **WHEN** an operator-attested review is persisted without an announcement
- **THEN** its evidence source, key, basis, supporting facts, reviewer, and
  instruction identify operator attestation and no synthetic announcement or
  analysis identity is stored

### Requirement: Review-only no-effect policy loading
The factor path SHALL recognize the latest resolved operator-attested
`factor_effect=none` review even when no resolved-term overlay exists.

#### Scenario: Load no-effect metadata without terms
- **WHEN** a resolved operator-attested review has no LLM analysis and no
  active resolved-term row
- **THEN** the resolved policy loader returns `factor_effect=none`, no resolved
  economic fields, and no authoritative economic override

#### Scenario: Active terms remain authoritative
- **WHEN** an event has an active resolved-term overlay
- **THEN** that overlay takes precedence over any review-only fallback

### Requirement: Suspended restructuring resumption dates
The two frozen suspended restructuring events SHALL retain their unchanged
CNInfo capitalization terms and use `2013-02-08` as the effective trading
session with `factor_effect=normal`.

#### Scenario: Resolve both suspended events
- **WHEN** the operator batch applies `600817.SH` and `600556.SH`
- **THEN** the CNInfo factor path uses per-share capitalization values `0.25`
  and `0.30` respectively on `2013-02-08`

### Requirement: Official reference-price treatment for 002076
The `002076.SZ` event SHALL retain the CNInfo `10转4.6股` event description but
MUST derive its factor from the official adjusted opening reference price
instead of the ordinary capitalization formula.

#### Scenario: Apply the published opening reference
- **WHEN** announcement `1215397977` and effective date `2022-12-21` are
  selected with official prices `2.60` and `2.23`
- **THEN** the event factor is `2.60 / 2.23 = 1.165919282511` under
  `factor_effect=official_reference_price`

### Requirement: Local application and post-write audit
Applying the eight decisions MUST perform no network, download, OCR, title-LLM,
or semantic-LLM work and SHALL prove review-write source isolation.

#### Scenario: Apply and audit all decisions
- **WHEN** the fixed command runs with `--write`
- **THEN** eight latest resolved reviews exist, zero frozen events remain
  factor-blocking, raw CNInfo observations and TDX rows are unchanged, and
  production factor tables are unchanged by the review write

### Requirement: Suspended implementation-date factor fallback
The CNInfo factor path SHALL use the first valid traded session on or after the
implementation date as the effective factor date when an event is implemented
while the instrument has no valid traded quote.

#### Scenario: Long suspension resumes after implementation
- **WHEN** an implemented CNInfo event has no `tradestatus=1` quote on its
  implementation date, the first available quote is explicitly suspended, and
  the first later valid quote is within the requested rebuild interval
- **THEN** the implementation date remains `source_ex_date`, the first later
  valid quote date becomes `effective_date`, and the ordinary factor uses the
  last valid traded close before that effective date

#### Scenario: Missing quote history is not suspension evidence
- **WHEN** no suspended quote exists on or after the implementation date and
  the next valid quote is more than fourteen days later
- **THEN** the event remains pending rather than treating the quote-history gap
  as a long suspension

#### Scenario: Compound actions during one suspension
- **WHEN** multiple CNInfo actions with distinct implementation dates map to
  the same first resumed trading session
- **THEN** their same-date terms are aggregated, their distinct-date factors
  are compounded chronologically, and one combined factor is emitted on the
  resumed session

#### Scenario: Do not cross the rebuild end date
- **WHEN** no valid traded quote exists from the implementation date through
  the requested rebuild `end_date`
- **THEN** the event remains pending instead of using a later out-of-range
  quote

#### Scenario: Resolve the two 002076 suspended distributions
- **WHEN** the full `002076.SZ` CNInfo path is rebuilt
- **THEN** the `2014-06-13` `10派0.5元` event is effective on `2014-09-11`
  and the `2017-06-01` `10派0.3元、10转10` event is effective on
  `2017-10-12`, without downloading or re-analyzing announcements
