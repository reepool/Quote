## Why

On 2026-10-27 the corporate-action daily job reported `部分完成` with a stuck
anomaly-governance backlog: `600572.SH:公司股改限售股上市流通公告` was
`未匹配 1 / 待决 1` and re-erroring every run with
`ValueError: announcement XDXR action family mismatch`. Two defects combine:

1. **Title-rule gap.** Lockup-share listing announcements are deterministically
   non-XDXR (`restricted_shares_listing_circulation` in
   `_daily_title_exclusion_reason`), but the rule requires the exact substring
   `限售股份`. The SSE variant `限售股上市流通` (no `份`) escapes the rule, gets
   selected as an exceptional announcement via the `股改` marker, and is pushed
   into LLM triage as a `share_reform` case.
2. **Validator/prompt contradiction.** The LLM triage system prompt explicitly
   instructs that "restricted-share listing alone are non-XDXR", so the model
   legitimately answers `non_xdxr` with an action family judged from the full
   text. The response validator requires the returned `action_family` to equal
   the marker-derived case family (`share_reform`) exactly and raises otherwise.
   The case can therefore never be classified: it is re-dispatched each run,
   receives the same cached decision, errors again, and stays `active_pending`
   until the announcement ages out of the rolling window.

Net effect: one wasted LLM call per night, a permanent `部分完成` judgment, and a
backlog item that resurfaces for every future lockup-listing title variant. No
wrong factor data is written (the announcement is ultimately non-XDXR either way).

## What Changes

- Extend the `restricted_shares_listing_circulation` title rule to match the
  substring `限售股` (covering `限售股份上市流通`, `限售股上市流通`, and
  `股改限售股上市流通` variants), keeping the existing genuine-distribution
  protection and implementation-marker guards unchanged.
- In the triage governance re-filter, mark a case `deterministic_excluded`
  (terminal) when every retained announcement is deterministically excluded,
  instead of leaving the emptied case `active_pending` forever. Structured-event
  availability keeps precedence over exclusion.
- Relax the classifier's action-family equality validation for `non_xdxr`
  verdicts only: accept the divergence, keep the case's stable action family,
  and record the LLM's own label as a diagnostic. Any other disposition with a
  divergent family still fails validation.
- The live stuck case (600572.SH) heals organically on the next run through the
  new terminal branch — no state surgery, no data backfill.

## Capabilities

### New Capabilities

- `a-share-announcement-xdxr-triage`: Deterministic exclusion of lockup-share
  listing announcements, terminal handling of fully-excluded announcement-only
  cases, and the action-family validation contract for announcement-only XDXR
  triage.

## Impact

- Affected code:
  - `data_sources/cninfo_corporate_action_incremental.py`
    (`_daily_title_exclusion_reason` lockup rule).
  - `data_sources/cninfo_announcement_xdxr_triage.py`
    (governance re-filter terminal branch; classifier family validation).
- Affected tests: `tests/unit/test_cninfo_corporate_action_incremental.py`,
  `tests/unit/test_cninfo_announcement_xdxr_triage.py`.
- No schema, storage, configuration, or report-format changes.
- Explicitly out of scope: LLM prompt or schema changes; likelihood/confidence
  routing thresholds; archiving historical terminal cases; BSE-specific paths;
  consuming delisting announcements into instrument master lifecycle.
