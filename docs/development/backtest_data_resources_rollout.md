# Backtest-Critical Data Rollout

This change integrates point-in-time backtest data into existing maintenance
owners. It does not add a full-market umbrella cron and deployment does not
start data acquisition.

## Route Decisions

- Index composition extends `index_master_governance_sync`. Current-only
  adapter responses are not historical evidence. Historical scope remains
  unavailable until a bounded probe proves effective-date and weight history.
- Security state extends `a_share_stock_master_sync` and official announcement
  evidence. The instrument master supplies forward observations only.
- Daily price limits extend `daily_data_update` only when its quote rows contain
  source-reported upper and lower limits. The current local schema does not, so
  historical/source-reported coverage remains unavailable.
- Financial filing vintages extend existing official financial filing parsing.
  The same downloaded manifest and parser output are appended before latest
  compatibility projections; there is no second downloader.
- Canonical corporate actions project existing CNInfo, TDX, effective-date,
  review, factor-governance, and coverage evidence. Projection performs no
  network requests.
- Existing industry return and industry membership APIs remain the discovery
  routes. Membership history is effective-date-only, not strict knowledge-time.

## Operator Sequence

1. Run `scripts/dev_validation/probe_backtest_data_resources.py` with no more
   than 20 identifiers and 31 days. Probes are read-only.
2. Initialize the quote and financial databases. Initialization only creates
   additive tables and does not download data.
3. Review `config/backtest_data_rollout.json`. Every stage is disabled by
   default. Enable one stage at a time after its parent workflow is healthy.
4. Run the parent workflow with a bounded market or instrument scope. Check the
   `backtest_stages` report for inherited scope, provider usage, row counts,
   blockers, and database watermark.
5. Use dry-run mode for operator backfill. Unsupported historical scopes return
   `unavailable` and perform no provider request or write.
6. Compare capability/readiness output with external platform fixtures before
   enabling strict consumption.

## Rollback

Set the affected stage's `enabled` flag to `false`. Additive revision tables and
watermarks remain for audit and PIT reads; do not delete them. Existing quote,
instrument, financial, industry, and corporate-action APIs retain their prior
defaults.

## Unresolved Coverage

- Historical index composition and weights: current source route has not proved
  historical depth.
- Historical ST and limit facts: current master is forward-only and daily quote
  rows do not include complete source-reported limit fields.
- Historical filing vintages: only retained source artifacts can be versioned;
  missing archives cannot be reconstructed.
- Industry membership: effective-date history exists, but knowledge-time
  revisions are not retained.
