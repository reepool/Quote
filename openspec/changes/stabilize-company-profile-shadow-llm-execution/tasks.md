## 1. Freeze the execution failures and partition contract

- [x] 1.1 Add a provider-free fixture/audit that binds the refined replay and asserts five valid over-budget responses (maximum 40,673), five provider failures, four terminal schema failures, and maximum latency 259,482 ms.
- [x] 1.2 Freeze the segment partition eligibility threshold and metric partitions against the real oversized and representative non-oversized prepared scopes without changing Evidence or the archived replay.

## 2. Correct common gateway repair and output diagnostics

- [x] 2.1 Change schema repair to append one provider-compatible user correction envelope while preserving the original structured-output payload, complete prior response, schema, deadline, and one-repair limit.
- [x] 2.2 Distinguish locally valid provider output-budget excess from token-limit truncation with explicit warnings/errors and safe request/attempt diagnostics.
- [x] 2.3 Add gateway tests for repair role order and payload preservation, valid over-budget JSON, missing usage, truncated JSON, repair success, and terminal repair/provider failure.

## 3. Add bounded segment partition execution

- [x] 3.1 Partition only eligible `extract_segment_financials` requests into stable disjoint metric subsets with identical report-local Evidence and derived request IDs.
- [x] 3.2 Merge compact partition rows and field-local coverage under the original request identity; fail closed on row, Evidence, subject, period, or duplicate-cell conflicts before full expansion.
- [x] 3.3 Count and trace every physical partition call while preserving the logical extract/repair/verify order and the existing provider-call ceiling.
- [x] 3.4 Add Stage 5 tests for eligible and ineligible scopes, complete field union, successful merge without duplicate Segment records, partition failure isolation, merge conflicts, trace lineage, and physical call budgeting.

## 4. Validate and close the stability gate

- [x] 4.1 Run the focused gateway, Stage 5 provider, workflow, and shadow tests plus Ruff/format and strict OpenSpec validation.
- [x] 4.2 Run one bounded Gemini `json_object` repair integration probe outside the twenty-report cohort; record safe hashes, latency, usage, warnings, and typed failure details in a research-only receipt.
- [x] 4.3 Document that this change authorizes only one later separate new-ID twenty-report replay and keeps `production_authorization=not_authorized`, Stage 6, approved writers, scheduler/backfill, commodity exposure, value chain, and DCF closed.
