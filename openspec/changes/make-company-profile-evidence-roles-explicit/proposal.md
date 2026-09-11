## Why

The 20-report shadow batch is structurally traceable, but recurring semantic failures show that the model can treat contextual Evidence as if it owned a requested field. This is especially harmful for legal-empty coverage, business-regime no-change claims, segment rows, and material-input decisions. The request contract should make Evidence ownership explicit before another provider-bearing validation.

## What Changes

- Mark each Evidence catalog entry as `field_owner`, `context_only`, or `mixed` based on the prepared field bindings.
- Expose the exact owning field IDs separately from contextual Evidence in extract, repair, and verify payloads.
- Add bounded instructions that legal-empty coverage requires field-owning Evidence; context-only Evidence may provide context but cannot close coverage.
- Clarify that control-scope no-change does not establish broader business-regime no-change, and that cost-component rows are not business segments.
- Preserve the existing closed schemas, validator guards, Evidence hashes, subject rules, and fail-closed behavior.
- Add provider-free tests for catalog role serialization and the affected instruction contract.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-bounded-semantic-workflow`: Evidence request payloads and instructions distinguish field-owning Evidence from contextual Evidence and constrain legal-empty coverage decisions.
- `company-profile-manufacturing-materials-shadow-batch`: shadow preparation requests expose explicit Evidence roles without changing the frozen cohort, fields, or semantic acceptance rules.

## Impact

Affected code is limited to the isolated stage-five provider adapter and its focused tests. No public API, historical bundle, Gold fixture, production table, Stage 6 path, or model/provider route changes are included.
