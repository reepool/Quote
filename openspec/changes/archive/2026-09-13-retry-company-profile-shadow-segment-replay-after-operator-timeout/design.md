## Context

The original post-repair replay admitted its frozen inputs and began semantic
execution, but an operator-level 420-second cutoff stopped the process after one
report. That partial output is evidence of an interrupted attempt, not a valid
cohort result. The retry must preserve it, use a distinct identity, and avoid
reintroducing the historical-planner mismatch already fixed in the operator.

## Goals / Non-Goals

**Goals:**

- Admit one new, hash-bound replay using the already frozen prepared scopes.
- Make process supervision long enough for the full cohort without imposing a
  shorter wall-clock cutoff than the per-request contract.
- Produce complete report-local outputs and all required audits, even when the
  result is `hold` or `failed`.

**Non-Goals:**

- No semantic, Evidence, Gold, model, token, request-timeout, or readiness-gate
  changes.
- No resume, append, merge, or repair of the interrupted batch.
- No production authorization or downstream publication.

## Decisions

1. **Use a new batch identity.** The interrupted directory remains immutable and
   is bound as a failed-attempt reference; the retry writes to a fresh directory.
   Reusing the identity would make partial files indistinguishable from a valid
   result.
2. **Load frozen prepared scopes.** The retry reuses the prepared scopes from the
   routing/continuation batch rather than running the current planner. This keeps
   the replay tied to the v5 contract and avoids planner drift.
3. **Remove only the operator-wide cutoff.** Per-request 300-second and physical
   provider-call budgets remain unchanged. The operator is supervised by the
   caller/session, with no `timeout 420s` wrapper.
4. **Review only the new batch.** Source review, recurrence, readiness, and
   comparison artifacts use the new batch and the prior complete baseline; the
   partial attempt is reported as an execution failure and never merged.

## Risks / Trade-offs

- [Long-running process] → Monitor the session and preserve typed report-local
  failures; do not kill it for convenience.
- [Provider or network failure] → Keep existing transport classification and
  report isolation; close as `failed` or `hold` without retries outside the
  existing contract.
- [Partial output from the first attempt] → Validate that the new output path is
  distinct and retain the old path only as an audit reference.
