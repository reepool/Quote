## ADDED Requirements

### Requirement: Auditable manual asymmetric override
The system SHALL allow an operator to resolve a persisted CNInfo asymmetric
corporate action with explicit total-share-capital terms, beneficiary-only
terms, effective date, factor effect, and review notes without modifying the raw
observation.

#### Scenario: Correct total-share-capital capitalization
- **WHEN** an operator supplies a corrected capitalization value and an
  effective date for an existing CNInfo event
- **THEN** the active resolved-term overlay uses the corrected per-share value
  and the review lineage preserves the raw value and operator instruction

#### Scenario: Preserve beneficiary-only terms
- **WHEN** circulating shareholders receive a different amount from the
  total-share-capital equivalent
- **THEN** the factor fields store only the total-share-capital equivalent and
  the beneficiary-only amount is stored as descriptive review lineage

### Requirement: Review supersession
The system MUST preserve prior review rows and link a new manual asymmetric
override to the latest prior review for the same CNInfo event.

#### Scenario: Correct an earlier review
- **WHEN** an operator approves corrected values for an event with an existing
  review
- **THEN** a new review row is written with `supersedes_review_id` and one
  active resolved-term overlay points to the new review

#### Scenario: Approved event leaves the manual queue
- **WHEN** a manual asymmetric override is persisted as resolved
- **THEN** its governance state is refreshed to terminal and non-blocking even
  if older governed evidence retained a different date

### Requirement: Explicit factor effect
Every manual asymmetric override SHALL declare whether its total-share-capital
terms have a normal adjustment-factor effect or no adjustment-factor effect.

#### Scenario: Debt-settlement shares do not affect listed holders
- **WHEN** an implemented share issue is recorded but listed shareholders
  receive no shares and no ex-right adjustment occurs
- **THEN** the event remains resolved and queryable with `factor_effect=none`
  while its economic terms remain preserved

### Requirement: Local persisted-data operation
Applying an operator decision to an existing CNInfo event MUST NOT download
documents, run OCR, invoke title classification, or invoke semantic extraction.

#### Scenario: Apply four operator decisions
- **WHEN** the four approved event payloads are submitted
- **THEN** the system writes review bundles using existing CNInfo identities and
  reports no network or LLM work

### Requirement: Analysis-free unchanged CNInfo approval
The system SHALL allow an operator to resolve an asymmetric event without a
persisted LLM analysis when the supplied factor-relevant terms are unchanged
from the current CNInfo observation, persisted official announcement evidence
is selected, and `factor_effect=normal`.

#### Scenario: Approve unchanged CNInfo terms
- **WHEN** an operator supplies current CNInfo terms, an official effective
  date, beneficiary-only descriptive lineage, and no analysis exists
- **THEN** the system writes a resolved review and effective-date evidence,
  writes no resolved-term overlay, and continues deriving factors from the raw
  CNInfo observation

#### Scenario: Reject an analysis-free economic change
- **WHEN** an analysis-free operator payload changes a CNInfo economic term or
  declares `factor_effect=none`
- **THEN** the system rejects the payload rather than fabricating an analysis
  row or silently changing factor behavior

### Requirement: CNInfo factor-source isolation
Manual asymmetric approvals SHALL NOT copy TDX economic terms or TDX day
factors into the CNInfo factor path.

#### Scenario: Record a TDX disagreement
- **WHEN** a persisted TDX comparison uses a different beneficiary perspective
  from the CNInfo observation
- **THEN** the disagreement may remain in audit lineage while the approved
  CNInfo factor path continues to use CNInfo terms only

### Requirement: Operator-approved TDX trading-date alignment
The system SHALL allow an operator-approved asymmetric CNInfo event to use the
trading effective date from an exact persisted TDX XDXR row without adopting
the TDX economic terms or factor.

#### Scenario: Align a non-trading CNInfo payment date
- **WHEN** an exact TDX row belongs to the same instrument, its ex-date is an
  exchange trading session, and it is compatible with the CNInfo record,
  payment, or share-arrival timeline
- **THEN** the resolved CNInfo effective date uses the TDX ex-date while the
  CNInfo factor economics continue to use the unchanged CNInfo observation

#### Scenario: Preserve source-specific asymmetric values
- **WHEN** CNInfo reports total-share-capital economics and TDX reports
  circulating-shareholder economics for the same asymmetric event
- **THEN** both source records remain unchanged, no CNInfo resolved-term overlay
  is written, and review lineage states that TDX economics and factor were not
  used

#### Scenario: Reject an unreviewed nearby TDX row
- **WHEN** the TDX row ID, instrument, expected date, special-event category, or
  trading-session validation does not match the explicit operator decision
- **THEN** the system rejects the write without guessing another TDX row
