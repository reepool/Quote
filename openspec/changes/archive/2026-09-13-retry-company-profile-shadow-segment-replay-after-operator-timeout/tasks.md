## 1. Freeze the retry contract

- [x] 1.1 Bind a new batch ID to the frozen cohort, v5 Evidence plan, prepared
  scopes, audits, Gemini profile, budgets, and the interrupted-attempt hash.
- [x] 1.2 Add an admission receipt proving the new output path is absent and the
  predecessor is excluded.

## 2. Implement bounded retry execution

- [x] 2.1 Add a retry mode to the existing shadow operator without a second
  semantic loop or changes to extraction/verifier semantics.
- [x] 2.2 Reject reused identities, partial-output inputs, changed frozen hashes,
  changed model/budgets, and any operator-wide cutoff shorter than the contract.
- [x] 2.3 Add provider-free tests for predecessor exclusion, identity drift,
  output reuse, mode isolation, and research-only boundaries.

## 3. Execute the retry once

- [x] 3.1 Run one read-only Scorpio/Gemini preflight through the authorized path.
- [x] 3.2 Run the new twenty-report cohort exactly once without an external hard
  cutoff, preserving every report-local success or typed failure.

## 4. Produce empirical artifacts

- [x] 4.1 Generate source-review outcomes for all six core chapters and record the
  interrupted predecessor as excluded execution failure.
- [x] 4.2 Generate segment-recurrence, readiness, and immutable baseline
  comparison audits from the new batch only.

## 5. Close and verify

- [x] 5.1 Record `ready`, `hold`, or `failed` from the existing gates and retain
  `production_authorization=not_authorized`.
- [x] 5.2 Run focused tests, Ruff, hash/artifact checks, `git diff --check`, and
  strict OpenSpec validation; commit and push only isolated change artifacts.
