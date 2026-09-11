# Invalid extract item isolation closure

## Outcome

The Stage 5 extract adapter now preserves independently valid siblings from a
schema-parseable response while excluding locally invalid candidate, coverage, segment
row, or compact measurement items. A successful extract trace carries bounded typed
`candidate_schema_invalid` rejected-item diagnostics; envelope and request-identity
failures remain call-level failures. Historical traces without the new field remain
valid.

No invalid fact is repaired, rewritten, rebound to different Evidence, or accepted. If
all results for a required field are rejected, the existing workflow leaves the field
`unclear` with `required_result_missing` and keeps the task incomplete.

## Provider-free evidence

The immutable audit classifies all 29 failed extract traces in the frozen external
twenty-report replay:

- 18 context-only legal-empty Evidence failures;
- 5 contradictory business-regime legal-empty failures;
- 3 control-only no-change failures;
- 2 BusinessOverview source-text/Evidence mismatches;
- 1 ambiguous counterparty disclosed-share direction.

Five equivalent mixed-response fixtures retain five valid siblings and exclude five
invalid items with `provider_calls=0`. The historical raw provider payloads were not
persisted, so the actual historical candidate salvage count remains unavailable and is
not reconstructed.

The frozen source hashes remain:

- manifest: `99d7121b37e9284ca28471cc256cca9cbf672fd8bb22fe2eedf4a187f6bc9ee0`;
- readiness audit: `872f69cc133351bd3d2f15b10e01dafc9901984ce4bbfe8e883b15c877441799`;
- review package: `a6a16b11fb72134162a020ab0346e777e375a3223a1eed643b0e68f97a9bde5f`.

## Verification

- Stage 5 focused suite: `193 passed, 6 warnings`;
- Ruff: passed;
- strict OpenSpec validation: passed;
- repository diff and frozen-hash checks: passed;
- LLM/provider calls: 0.

## Adaptive output-token recommendation (separate change)

Dynamic request-time token selection is reasonable, but it is not the fix for these 29
schema-invalid responses. The frozen replay has 318 traces; 289 successful traces persist
output usage: 169 extract and 120 verify. Extract usage has median 3,838, p90 12,635.2,
p95 16,284.2, and maximum 29,495 tokens. Verify usage has median 1,768.5, p90 5,952.4,
p95 9,866.1, and maximum 28,402 tokens. Five extract and one verify responses exceeded
the frozen 20,000/18,000 budgets while remaining schema-parseable.

A later small change should keep the current `20000/18000` base tier for ordinary scopes
and select a larger tier before the request only when deterministic scope features warrant
it. Candidate tiers for empirical calibration are `32000/24000` and `40000/30000`.
Selection should use Evidence characters/count, active field count, table rows or numeric
occurrences, expected candidate count, and partition count. The selected tier and input
features should be written to the existing trace. A failed request must not be retried with
a larger budget, and scopes above the highest validated tier should be partitioned by
physical anchor or row group rather than given an unbounded budget.

Thresholds must be calibrated from the frozen trace corpus in that separate change; they
must not be guessed into production. A dry-run classifier that records the would-be tier
without changing provider parameters is the safest first validation step.

## Boundary

The authoritative twenty-report replay remains unchanged. This change does not rerun an
LLM, change prompts/models/timeouts/retries/partitions/token budgets, or authorize Stage 6,
approved writes, scheduler/backfill, commodity exposure, value-chain publication, or DCF.
`production_authorization=not_authorized` remains in force.
