## 1. Reviewed Lineage Catalog

- [x] 1.1 Add the versioned `600018.SH` code-lineage catalog with issuer regimes, evidence, transition policy, and the three approved source decisions
- [x] 1.2 Implement strict catalog loading and validation for dates, regimes, continuity policies, and reviewed row values

## 2. Audit And Reconciliation

- [x] 2.1 Normalize pytdx and AkShare/Tencent daily rows into a common quote representation
- [x] 2.2 Implement leading-gap, source-date, OHLC-conflict, and transition-boundary diagnostics
- [x] 2.3 Implement fail-closed reviewed reconciliation that selects the approved `2001-08-16` and `2003-11-17` rows

## 3. Repair Workflow

- [x] 3.1 Add an allowlisted dry-run-first CLI that fetches sources through existing adapters and the AkShare proxy patch
- [x] 3.2 Build missing-only write plans and preserve every existing local quote row
- [x] 3.3 Persist successful repair evidence and non-continuous transition metadata through `instrument_master_metadata`
- [x] 3.4 Verify persisted coverage and make reruns idempotent

## 4. Tests And Production Validation

- [x] 4.1 Add unit tests for catalog rejection, leading-gap detection, reviewed source arbitration, unresolved-conflict failure, missing-only writes, metadata ordering, and transition reporting
- [x] 4.2 Run focused tests and OpenSpec strict validation
- [x] 4.3 Run a live dry-run for `600018.SH` and verify the proposed dates and reviewed values
- [x] 4.4 Apply the targeted repair and verify row counts, date coverage, source counts, duplicate absence, post-2006 row stability, and lineage metadata
- [x] 4.5 Run Codex review on the uncommitted task changes, reassess findings, and resolve confirmed defects
