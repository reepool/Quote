## ADDED Requirements

### Requirement: Deterministic non-implementation title prefilter
The CNInfo special-action discovery workflow SHALL deterministically exclude
clearly non-implementation announcement document types before title LLM
classification and document resolution.

#### Scenario: Exclude irrelevant document types
- **WHEN** a title is a legal opinion, voting result, periodic report, valuation
  report, reply, pledge, transfer-registration notice, or similar
  non-implementation document
- **THEN** it is omitted from title LLM input and downstream document resolution
  with a deterministic exclusion reason

#### Scenario: Keep implementation notice with broad role word
- **WHEN** a title contains `董事会` or `股东大会` and also explicitly states
  implementation or completion
- **THEN** the broad role word alone does not exclude the announcement

### Requirement: Preserve prefilter lineage
The workflow MUST preserve the original announcement scan result and persist
the deterministic filter decision as rejected candidate evidence.

#### Scenario: Inspect a filtered announcement
- **WHEN** an operator audits a discovery run
- **THEN** the announcement title, identifier, matched filter rule, and
  exclusion status remain queryable even though no LLM call was made for it

### Requirement: Observable prefilter savings
The discovery report SHALL expose prefiltered announcement counts and bounded
samples grouped by exclusion reason.

#### Scenario: Mixed search window
- **WHEN** one search window returns implementation notices and filtered
  periodic or legal documents
- **THEN** the report separates eligible title-classification input from
  prefiltered announcements
