## ADDED Requirements

### Requirement: PIT Quality Filters Apply After Latest Visible Selection
The cross-section API SHALL accept optional available_at_quality whitelist, exclude_unverified and exclude_discrepancy. Quality values SHALL be validated against exact, exact_event, statutory_deadline and delayed_estimate. Exclusion flags SHALL default to false and SHALL refer respectively to attribution_quality=unverified and share_validation=discrepancy. Quality filtering SHALL follow strict latest-visible-row selection and precede paging, without changing available_at, effective_at, strict revision guard or universe semantics.

#### Scenario: The newest visible filing is unverified
- **WHEN** the newest visible row is unverified and an older visible row is verified
- **AND** exclude_unverified=true
- **THEN** the instrument SHALL be excluded rather than replaced with the older report
- **AND** no row unavailable at as_of SHALL be considered

#### Scenario: Availability and share-quality filters are combined
- **WHEN** available_at_quality=exact,exact_event and exclude_discrepancy=true are supplied
- **THEN** only selected rows satisfying both conditions SHALL remain before pagination
- **AND** an unknown quality enum SHALL produce structured 422 instead of an empty-success fallback

### Requirement: PIT Filter Statistics Describe The Whole Selected Cross-Section
Cross-section responses SHALL preserve existing page-level stats and SHALL add filter_stats with scope selected_cross_section_before_paging, before_count, after_count, excluded_count and before/after distributions for availability, attribution and share validation. Total SHALL be the filtered population count, and field projection SHALL NOT disable quality checks or statistics.

#### Scenario: Pages traverse a filtered cross-section
- **WHEN** the same as_of and quality filters are requested with different limits or offsets
- **THEN** filter_stats SHALL be identical across those pages, before_count minus after_count SHALL equal excluded_count, and total SHALL equal after_count
- **AND** existing stats SHALL continue to describe only the returned page

#### Scenario: No quality filters are requested
- **WHEN** the caller omits the new quality controls
- **THEN** before_count SHALL equal after_count with excluded_count zero
- **AND** existing selection and listed-only universe limitations SHALL remain intact

### Requirement: PIT Ratios Provide A Decimal Projection Without Changing Stored Percentages
PIT cross-section responses SHALL retain legacy percentage-valued indicators and SHALL expose decimal ratios for gross_margin, operating_margin, net_margin, roe, roa and asset_liability_ratio in a ratios object with machine-readable units. Multiple-valued and currency-per-share indicators SHALL retain their own units.

#### Scenario: A legacy percentage is normalized once
- **WHEN** a selected PIT row has roe=4.687220683893516
- **THEN** its projected ratios.roe SHALL equal 0.04687220683893516 within serialization precision
- **AND** legacy roe SHALL remain 4.687220683893516
- **AND** neither stored panel rows nor their availability metadata SHALL be rewritten
