## Context

TDX XDXR rows contain explicit cash, bonus, rights, ex-date, and derived-factor fields. Production factor providers contain sparse factor paths but often do not expose the underlying company-action semantics. CNInfo exposes official announcement metadata, while AkShare's `stock_fhps_em` wraps Eastmoney structured distribution records. These sources provide different evidence and must not be collapsed into a single pass/fail comparison.

## Goals / Non-Goals

**Goals:**
- Separate event-field evidence, official-announcement evidence, and cumulative-result evidence.
- Provide a manual, read-only full-market or targeted validation workflow.
- Prioritize cash-dividend correctness for downstream dividend-yield research.
- Normalize provider factor paths before comparing year-end and latest cumulative results.
- Produce bounded, actionable follow-up samples and explicit source provenance.

**Non-Goals:**
- Treating AkShare as an independent upstream source.
- Parsing every historical CNInfo PDF or automatically correcting TDX data.
- Replacing production adjustment factors or adding persistent audit tables.
- Running the workflow in daily production schedules.

## Decisions

1. Add a pure validation module for normalization, event matching, and cumulative path metrics. Data-source acquisition and database reads remain in `DataManager`.
2. Eastmoney implementation rows are normalized only when an ex-date exists and the plan status indicates implementation. Cash and total bonus ratios retain the source's per-10-share unit so they can be compared directly with TDX raw fields.
3. Rights-only TDX events are excluded from Eastmoney missing-event counts because `stock_fhps_em` does not provide a complete rights-issue contract.
4. CNInfo metadata establishes that an official implementation announcement exists for an instrument and time window. It does not validate cash or share amounts unless document parsing is added later.
5. Cumulative paths are rebuilt from comparable event-day factors with a unit baseline. Year-end and latest anchors reduce false conflicts caused only by provider ex-date shifts. Acceptance thresholds default to 0.1% acceptable and 0.5% warning.
6. The scheduler exposes a manual-only task. The task is read-only, supports bounded source scans, and reports partial when material event or cumulative conflicts remain.

## Risks / Trade-offs

- [Historical Eastmoney coverage is incomplete] -> Report fetched periods, failed periods, and source coverage instead of treating absent rows as definitive TDX errors.
- [CNInfo title search includes unrelated convertible-bond notices] -> Require implementation-distribution title signals and preserve title samples for review.
- [Small cumulative error hides offsetting event errors] -> Keep cumulative status separate from event-level status and never upgrade unmatched events solely because the final factor converges.
- [Full-history source scans are slow] -> Keep the job manual-only, log report-period progress, and allow targeted instrument/date runs.

## Migration Plan

Deploy the validation module, task, and configuration disabled from cron by `manual_only=true`. No data migration is required. Operators first run targeted recent-year validation, then expand the date range after reviewing coverage and runtime. Rollback removes the task surface without changing stored data.

## Open Questions

- Historical PDF field extraction remains a follow-up capability for resolving high-priority cash-dividend conflicts.
