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
