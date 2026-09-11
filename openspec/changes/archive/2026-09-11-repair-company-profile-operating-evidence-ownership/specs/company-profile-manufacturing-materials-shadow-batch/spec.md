## ADDED Requirements

### Requirement: Operating fields are owned by direct field-specific Evidence

The shadow Evidence planner MUST derive operating-quantity field IDs from field-specific
source text on the scope's direct candidate pages. Adjacent context, broad section keys,
selector reasons, headings, or anchor terms MUST NOT add a numeric operating field.
Bounded contextual pages MUST remain available for interpretation without becoming field
owners solely because they share a scope.

#### Scenario: Adjacent context mentions another operating concept

- **WHEN** a direct page supports production and sales quantities while an adjacent
  contextual page contains a broad capacity or inventory mention
- **THEN** the scope includes only the fields supported by field-specific text on the
  direct page
- **AND** the adjacent page remains context-only for those fields.

#### Scenario: A direct page contains a genuine capacity table

- **WHEN** a direct candidate page contains field-specific current or under-construction
  capacity disclosure
- **THEN** the matching capacity field remains planned and explicitly owned
- **AND** the correction does not remove the positive disclosure.

### Requirement: Prepared shadow Evidence preserves field ownership

The shadow Evidence preparer MUST bind each direct operating Evidence item only to the
scope fields supported by that page. Context-only Evidence MUST retain `field_id=None`,
and all bindings MUST preserve the original Evidence ID, source text, report identity,
and physical page.

#### Scenario: One direct page supports several fields

- **WHEN** a direct operating page supports production volume, sales volume, and
  inventory volume
- **THEN** the prepared scope contains one preserved Evidence identity bound to each of
  those supported fields
- **AND** it does not create bindings for unsupported capacity or processing fields.

#### Scenario: A bounded context page has no field-specific support

- **WHEN** a page is included only as adjacent or continuation context and has no
  field-specific operating signal
- **THEN** its prepared Evidence remains unbound
- **AND** it cannot own a candidate or legal-empty result.

### Requirement: Stage 5 respects explicit Evidence bindings

The Stage 5 request builder MUST use explicit prepared Evidence bindings when at least
one binding is present, filter them to active scope fields, and preserve unbound Evidence
as context-only. It MUST NOT recreate a page-by-field cross product in such a scope. For
a legacy scope whose Evidence items are all unbound, it MUST retain the existing
page-by-field compatibility behavior.

#### Scenario: Explicit bindings are present

- **WHEN** a prepared scope contains field-bound direct Evidence and unbound context
  Evidence
- **THEN** the semantic request contains only the explicit active bindings plus the
  unbound context
- **AND** no unrelated field binding is synthesized.

#### Scenario: A legacy scope has no explicit bindings

- **WHEN** every Evidence item in an existing non-shadow Stage 5 scope has
  `field_id=None`
- **THEN** the request builder applies the historical field expansion
- **AND** existing Stage 5 callers and fixtures remain compatible.

### Requirement: Ownership correction is proven provider-free

The change MUST rebuild and compare the frozen twenty-report plan and prepared scopes
without constructing or calling an LLM provider. The audit MUST cover all seventeen
previously failed operating scopes, report removed unsupported fields/bindings, prove
retention of known positive capacity and quantity disclosures, and keep the authoritative
batch immutable with `production_authorization=not_authorized`.

#### Scenario: Frozen provider-free comparison completes

- **WHEN** the corrected planner and preparer run against the hash-bound September 11
  cohort inputs
- **THEN** the audit accounts for every affected operating scope and its before/after
  field ownership
- **AND** `provider_calls=0`, no historical bundle is modified, and no production path is
  opened.
