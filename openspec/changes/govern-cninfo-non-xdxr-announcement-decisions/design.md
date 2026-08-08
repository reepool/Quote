## Context

The daily CNInfo announcement scan stores unmatched exceptional notices in `announcement_scan_state.metadata_json`. Carryover revalidation can deterministically exclude generic title patterns, but operator review is currently represented only for structured source events. These two approved notices have no source event key, so event-level resolution tables cannot represent the decision and the queue remains partial indefinitely.

The repository already uses frozen, hash-validated operator decision manifests and preview-first application scripts for production governance. The research announcement store is SQLite-backed and the current scan-state JSON can be updated without changing event or factor schemas.

## Goals / Non-Goals

**Goals:**

- Represent exact announcement-level non-XDXR decisions with instrument, announcement key, expected title, reviewer, basis, and approval date.
- Make current-scan and carryover filtering consume the same decision resolver.
- Apply the two approved decisions idempotently to the live research queue with drift checks and an immutable before/after summary.
- Preserve conservative deferral for every announcement not exactly covered by a valid decision.

**Non-Goals:**

- Do not create or modify corporate-action observations, resolved terms, or adjustment factors.
- Do not add broad keyword exclusions for debt-to-equity or compensation notices.
- Do not make unmatched announcements eligible for automatic LLM promotion in this change.
- Do not introduce a database migration or public API.

## Decisions

### Use a frozen announcement decision catalog

Store operator decisions in a dedicated Python module keyed by `(announcement_key, instrument_id)` and validate the expected normalized title before applying a decision. This matches existing fixed-decision operational scripts and keeps approvals code-reviewed and versioned. A new mutable database table was rejected because it would add migration and write-path complexity for two exact decisions without improving daily lookup behavior.

### Filter by exact identity before generic title classification

Both newly scanned and carried announcements consult the exact decision resolver before the generic exceptional-title classifier. A decision applies only when announcement key, instrument ID, and normalized title all match. Identity drift remains conservatively selected and is reported as a mismatch rather than silently excluded.

### Keep runtime cleanup preview-first and idempotent

Add a bounded script that defaults to read-only preview. `--apply` is restricted to the configured project `research.db`, verifies the frozen manifest and announcement audit rows, removes only the two matching pending announcement entries, and removes an instrument candidate only when no factor or semantic work remains. Repeated application produces no further queue mutation.

### Preserve an audit trail in scan metadata and announcement diagnostics

The apply script records decision keys and before/after queue counts in scan-state metadata and adds the exact operator disposition to the existing announcement audit diagnostics. It does not delete audit rows. The versioned catalog remains authoritative if runtime metadata is rebuilt.

## Risks / Trade-offs

- **A reviewed title is corrected upstream** -> Title mismatch disables the decision and retains the notice for review.
- **An instrument has other pending work** -> Candidate removal is conditional on no remaining special announcements, semantic event keys, or factor retry membership.
- **Hard-coded decisions do not scale indefinitely** -> This change intentionally covers fixed operator approvals; a database-backed review API can be proposed when announcement-level review volume justifies it.
- **Direct SQLite updates race with the scheduler** -> The apply script validates exact rows and uses one immediate transaction; it must be run outside the 03:30 daily task window.
