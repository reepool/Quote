## Why

The incremental CNInfo corporate-action task repeatedly exhausts its announcement
page bound, refreshes instruments for unrelated disclosures, evaluates same-day
events before quotes exist, and labels independent TDX or reconciliation findings
as CNInfo historical unreadiness. This makes a healthy daily refresh permanently
report `partial` and performs excessive source work even after the CNInfo historical
inventory has been resolved.

## What Changes

- Separate the announcement-discovery window from the completed-market-data cutoff.
- Cover the prior natural day through the current run time for 365-day schedules,
  or the prior completed trading day through the current run time for
  trading-day-only schedules, including weekends and long exchange holidays.
- Use the latest completed trading session with locally available quotes as the
  default event-factor cutoff, so a pre-market run does not require same-day prices.
- Filter clearly unrelated and non-corporate-action announcement titles before
  selecting structured CNInfo refresh candidates, while retaining bounded retry,
  recent-event, deferred, and rotating safety recovery paths.
- Establish and advance a complete announcement cursor without repeatedly rescanning
  the same bounded history; preserve genuinely deferred relevant candidates and
  never advance a temporal cursor beyond the current run timestamp.
- Retain the configured Tuesday-to-Saturday schedule's weekend disclosures with a
  two-calendar-day query overlap, and drain legacy deferred candidates once when
  adopting the title policy.
- Persist factor-rebuild candidates whose events are newer than the completed quote
  cutoff until quotes catch up and the affected path can be rebuilt, independently
  of whether the invocation performs a market announcement scan.
- Separate CNInfo path readiness, TDX reference-path diagnostics, and cross-source
  reconciliation in daily results and reports.
- Exclude BSE observations from the CNInfo factor path while continuing independent
  TDX refresh and audit coverage.

## Capabilities

### New Capabilities

- `a-share-corporate-action-daily-readiness`: Calendar-aware announcement coverage,
  completed-session factor cutoffs, relevant candidate selection, cursor progress,
  source-separated readiness, and BSE boundary behavior for daily maintenance.

### Modified Capabilities

None.

## Impact

- `data_manager.py`: daily window resolution, announcement candidate discovery,
  factor-rebuild targeting, and source-separated readiness summaries.
- `data_sources/cninfo_corporate_action_incremental.py`: deterministic daily title
  relevance classification and candidate diagnostics.
- `scheduler/tasks.py`: daily report fields and labels.
- `config/05_scheduler.json`: explicit daily announcement scheduling mode and
  bounded catch-up behavior.
- Focused unit tests for natural-day and trading-day windows, long-holiday coverage,
  weekend overlap, pre-market cutoffs, title filtering, cursor/backlog migration,
  deferred factor rebuilding, BSE isolation, and readiness reporting.
- No production-factor promotion, source-value overwrite, document download, OCR,
  or LLM analysis is introduced.
