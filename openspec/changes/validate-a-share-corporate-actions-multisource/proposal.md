## Why

The completed TDX XDXR backfill proves that the source can be downloaded and converted consistently, but it does not prove that every cash dividend, bonus issue, or ex-date is correct and complete. BaoStock factor changes also use different semantics, so factor mismatches cannot be treated as direct evidence of missing company actions.

## What Changes

- Add a read-only manual A-share corporate-action validation task.
- Validate TDX cash-dividend and bonus-share fields against Eastmoney structured distribution records exposed through AkShare, while identifying the upstream source explicitly.
- Scan CNInfo official implementation-announcement metadata as existence evidence without claiming that metadata alone validates amounts.
- Compare TDX-derived and reference-provider cumulative factor paths at year-end and latest anchors after normalizing both paths from event-day factors.
- Classify acceptable, warning, and conflict cumulative errors separately from event-level missing or field-conflict evidence.
- Produce bounded operator reports and follow-up samples without overwriting production factors or adding database tables.

## Capabilities

### New Capabilities
- `a-share-corporate-action-multisource-validation`: Layered event, official-announcement, and cumulative-factor validation for A-share corporate actions.

### Modified Capabilities
- `scheduler`: Expose the validation workflow as a manual-only data-management task with bounded reporting.

## Impact

- Affects corporate-action validation helpers, `DataManager`, scheduler task/configuration, focused tests, and XDXR documentation.
- Uses existing TDX audit rows, production adjustment-factor rows, AkShare Eastmoney distribution data, CNInfo announcement metadata, and the governed trading calendar.
- Does not mutate `adjustment_factors`, replace TDX rows, introduce migrations, or run automatically.
