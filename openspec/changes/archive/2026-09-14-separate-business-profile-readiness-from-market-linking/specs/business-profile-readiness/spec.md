## ADDED Requirements

### Requirement: Profile readiness is independent from market links
The business-profile read service MUST determine the top-level `status` and `readiness.status` from the availability of valid, approved, point-in-time company facts, and MUST NOT require a commodity exposure to have a market price-series or spread mapping.

#### Scenario: Approved facts without market links
- **WHEN** a company has at least one eligible approved business fact and has approved commodity exposures with no executable market mappings
- **THEN** the response MUST return `status=ready` and `readiness.status=ready`
- **AND** it MUST preserve the approved facts and exposures
- **AND** it MUST report the missing market links separately

#### Scenario: No eligible approved facts
- **WHEN** a company has no eligible approved company facts at the requested cutoff
- **THEN** the response MUST return `status=not_ready` and include a profile evidence gap

### Requirement: Market-link readiness is reported separately
The response MUST include `market_link_status` and a `readiness.market_link` object describing the relationship between approved commodity exposures and executable market mappings.

#### Scenario: All approved exposures directly linked
- **WHEN** every eligible approved commodity exposure has an active price series or spread definition and a valid direction
- **THEN** `market_link_status` MUST be `direct_linked`
- **AND** `readiness.market_link.approved_exposure_count` MUST equal `readiness.market_link.executable_mapping_count`

#### Scenario: Some exposures linked
- **WHEN** at least one but fewer than all eligible approved commodity exposures has an executable mapping
- **THEN** `market_link_status` MUST be `partial`
- **AND** the response MUST list unresolved exposure identifiers

#### Scenario: Exposures have no market links
- **WHEN** eligible approved commodity exposures exist but no executable mapping can be built
- **THEN** `market_link_status` MUST be `unlinked`
- **AND** the response MUST preserve the exposures and provide missing-link diagnostics

#### Scenario: No approved exposures
- **WHEN** no eligible approved commodity exposure exists
- **THEN** `market_link_status` MUST be `not_applicable`
- **AND** the response MUST return an empty executable mapping list

### Requirement: Market mapping does not imply product identity
The read service MUST NOT create or infer a direct market mapping solely from equal or similar product names, and MUST NOT treat a warehouse receipt or standard deliverable commodity as identical to a company's physical product without an existing explicit mapping record.

#### Scenario: Same-name product without an explicit series mapping
- **WHEN** a product name resembles a market instrument but no explicit active series or spread mapping exists
- **THEN** the product MUST remain an approved semantic exposure with `market_link_status` `unlinked` or `partial`
- **AND** it MUST NOT be counted as an executable mapping
