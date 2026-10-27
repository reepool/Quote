## ADDED Requirements

### Requirement: Fallback Fills Only Missing Canonical Facts

When the official source pass leaves individual canonical facts missing or invalid for a target, the fallback collection SHALL write fallback values for exactly those fields, and SHALL NOT overwrite canonical facts already supplied by the official source.

#### Scenario: Official source supplies four of five core facts

- **WHEN** CNInfo data20 supplies `revenue`, `net_income_parent`, `total_assets`, and `total_liabilities` for an instrument-period
- **AND** `equity_parent` is missing
- **THEN** the fallback bundle SHALL fill only `equity_parent`
- **AND** the other four core fields SHALL keep the official values

#### Scenario: Official and fallback values disagree on a field the official source has

- **WHEN** a canonical fact has a valid official value
- **AND** the fallback bundle carries a different value for the same fact
- **THEN** the authoritative core row SHALL keep the official value
- **AND** the disagreement SHALL be handled by source-fact reconciliation reporting rather than by overwriting

#### Scenario: No usable official facts for a target

- **WHEN** the official source provides no usable facts for an instrument-period
- **THEN** fallback SHALL supply all required canonical facts for that target under the same field-level policy

#### Scenario: Field-level policy disabled

- **WHEN** `fallback_only_missing_or_ambiguous_facts` is `false`
- **THEN** the fallback write SHALL preserve the legacy whole-bundle behavior

### Requirement: Per-Field Source Provenance

Core financial fact rows SHALL record, per canonical field, the source that supplied the authoritative value.

#### Scenario: Mixed-provenance row is written

- **WHEN** a core row is composed with some fields from the official source and some from fallback
- **THEN** the row SHALL record per-field source and source file lineage for every supplied field
- **AND** the row-level write lineage SHALL still identify the writing bundle

#### Scenario: Historical row without per-field provenance

- **WHEN** a core row predates this capability and has no per-field source record
- **THEN** readers SHALL fall back to row-level source semantics and SHALL NOT treat the row as invalid

### Requirement: Fill Policy Comes From Configuration

The field-level fill behavior SHALL be driven by the declared `disclosure_incremental_sync` configuration flags and SHALL default to field-level fill as declared.

#### Scenario: Configuration requests field-level fill

- **WHEN** `fallback_only_missing_or_ambiguous_facts` is `true` (the declared default)
- **THEN** fallback collection SHALL apply the field-level fill policy
