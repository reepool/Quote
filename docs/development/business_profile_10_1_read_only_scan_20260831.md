# Business Profile 10.1 Read-Only Scan

- Scan time: `2026-08-31 00:45:40 +08:00`
- Database: `data/research.db`
- Scope: all local business-profile instruments (`5,568`)
- Mode: audit only (`apply=false`, `result_policy=reuse`)
- Network access: `false`
- LLM access: `false`
- Database writes: `false`

## Scan Results

| Check | Result |
| --- | ---: |
| Approved as-of/knowledge temporal anomalies | `0` across activities, operating facts, relationships, exposure facts, and exposures |
| Exact legacy `mw`/`mW` values | `0` |
| Stale reusable semantic runs | `0` |
| Pending semantic receipts | `0` |
| Retired semantic runs | `0` |
| Retired semantic receipts | `0` |
| Exposure publications missing `source_activity_action` | `2` |
| Missing-action exposures recoverable from one referenced fact | `2` |
| Missing-action exposures with incomplete lineage | `0` |
| Held records | `10`; latest hold owner is automation for all 10 |
| Owned checkpoint files | `23`; orphan files `0`; unsafe paths `0` |

The two legacy exposure publications are approved records for `601088.SH`. Both
reference exactly one existing exposure fact whose action is `sells`; they are
eligible for deterministic lineage recovery during the controlled repair/cleanup
step and were not modified by this scan.

## Execution State Requiring 10.2 Handling

The scan found two non-completed work items:

- `bp-work-886520f89da88c93f1fc0827` (`002496.SZ`): `terminal_failure` at
  `verify`, caused by `governed record is missing or ambiguous` for an activity.
  Its owned checkpoint is the one cleanup candidate reported by the audit.
- `bp-work-b4f919e92f34cbf584fff10f` (`300750.SZ`): `retry_due` at `verify`, with
  a past `next_attempt_at` and no active lease. The last error was a provider
  congestion/gateway failure, so it remains a replay/retry candidate rather than
  an orphan.

The semantic repair audit also reported pre-existing local findings outside the
10.1 deletion set: `10` shareholder scope/controller findings and `2` internal
inventory-derived storage-role findings (`300708.SZ`, `600036.SH`). They were
not changed by this read-only operation.

## Gate Decision

10.1 read-only scanning is complete. Do not start the broad 10.5 batch yet.
Proceed to 10.2 only after confirming the terminal-failure cleanup scope and the
retry policy for the `300750.SZ` item; preserve approved records, evidence, and
review audits.
