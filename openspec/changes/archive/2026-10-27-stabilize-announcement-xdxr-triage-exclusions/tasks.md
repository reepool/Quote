## 1. Implementation

- [x] 1.1 In `data_sources/cninfo_corporate_action_incremental.py` `_daily_title_exclusion_reason`, relax the `restricted_shares_listing_circulation` condition from the exact substring `限售股份` to `限售股` (covering all listing-circulation title variants); keep the genuine-distribution early return and the implementation-marker guards unchanged.
- [x] 1.2 In `data_sources/cninfo_announcement_xdxr_triage.py` `govern`, after the structured-availability check, mark a non-terminal case whose retained announcement list is empty as `deterministic_excluded` (terminal, mode-independent) and clear its stale `last_error`; keep the partial-exclusion refresh and the disabled/shadow fallback unchanged.
- [x] 1.3 In the classifier's response validation, accept an action-family divergence when `disposition == "non_xdxr"`: keep the case's family as the decision's `action_family`, preserve the LLM's own label under `llm_action_family`, and keep raising for every other disposition.

## 2. Tests

Extend the existing per-module test files (no new files; mocked classifier/loader per repo convention).

- [x] 2.1 In `tests/unit/test_cninfo_corporate_action_incremental.py`: title-rule tests for `公司股改限售股上市流通公告` (the incident title), `限售股上市流通公告`, `限售股份上市流通公告` (regression), a combined genuine-distribution title (stays selectable), and a guard-marker title (not excluded by this rule).
- [x] 2.2 In `tests/unit/test_cninfo_announcement_xdxr_triage.py`: governance tests — a fully-excluded case becomes `deterministic_excluded` and is not dispatched; a case with a structured-matched announcement stays `structured_event_available`; the existing partial-exclusion refresh behavior is unchanged.
- [x] 2.3 In `tests/unit/test_cninfo_announcement_xdxr_triage.py`: classifier validation tests — `non_xdxr` with divergent family is accepted with `llm_action_family` recorded and the case family normalized; `probable_xdxr` with divergent family still raises.

## 3. Verification

- [x] 3.1 Run the affected pytest subset (`test_cninfo_corporate_action_incremental.py`, `test_cninfo_announcement_xdxr_triage.py`, `test_cninfo_primary_daily_maintenance.py`) and record results.
- [x] 3.2 Live verification on the first scheduled run after deployment (scheduler daemon reload required): the 600572.SH case routes to `deterministic_excluded`, the report shows `未匹配 0` with no `待处理公告` line for it, and no repeated `action family mismatch` error in the logs; the job completes without the anomaly-governance backlog issue.
- [x] 3.3 Confirm on a subsequent run that the healed case is no longer dispatched to the LLM classifier (no new LLM call for `600572.SH`).
