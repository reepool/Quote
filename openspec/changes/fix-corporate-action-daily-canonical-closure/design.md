## Context

The promoted canonical factor series is current through 2026-07-31, but the daily workflow could not merge a targeted candidate because `a_share_quote_baostock_sina:SSE` and `:SZSE` were absent from `operational_watermarks`. The factor rebuild itself reported zero CNInfo and TDX pending events. Treating predecessor state as an instrument-level factor defect then queued all 115 affected instruments for repeated rebuilds. Separately, broad title markers classified financing statements containing "补偿" and share-cancellation notices as unresolved XDXR announcements. The newly integrated BSE scan completed successfully with an empty weekend result and was not the cause of the partial status.

## Goals / Non-Goals

**Goals:**

- Produce durable per-exchange BaoStock/Sina quote-composite watermarks from successful normal update runs.
- Recover safely when compatible historical data already exists but the newly introduced watermarks do not.
- Keep predecessor deferral distinct from genuine factor-path retry state.
- Avoid semantic XDXR work for deterministic non-XDXR titles while retaining genuine distribution notices.
- Make every daily `partial` result explain its blocking stage and bounded affected samples.
- Keep BSE success-empty results non-blocking and source-isolated.

**Non-Goals:**

- Backfill full-history BSE official corporate actions.
- Change the three-source canonical selection policy or overwrite source tables.
- Automatically approve genuinely exceptional restructuring, compensation-distribution, or asymmetric events.
- Relax factor-path correctness gates such as pending CNInfo events, historical gaps, blocked source-selection segments, or failed writes.

## Decisions

1. **Write predecessor watermarks at the producer boundary.** The normal A-share quote/factor update will write `a_share_quote_baostock_sina:<exchange>` only after its exchange-specific quote and composite-factor work completes successfully. A single aggregate watermark is insufficient because one exchange can lag independently.

2. **Use a bounded compatibility recovery path for missing watermarks.** If a per-exchange watermark is missing, readiness may verify local quote coverage through the required session and the existing BaoStock/Sina composite factor path for the requested instruments. A successful verification will return a distinct `verified_existing_data` readiness state and can persist the recovered watermark. It will not accept a stale watermark or incomplete local evidence. This avoids permanently blocking data created before watermarks existed without silently disabling the predecessor gate.

3. **Separate workflow deferral from factor retry.** Missing/stale predecessor readiness will keep canonical merge `partial`, but instruments will enter `daily_factor_retry` only for actual factor derivation/write failures, blocked canonical selection segments, or a failed targeted merge. A global predecessor deferral is retried by the next daily workflow and is reported separately.

4. **Use deterministic exclusion before semantic classification.** Titles that explicitly state no financial assistance/compensation for a private placement, ordinary repurchase cancellation, restricted-share cancellation, registration-capital change, or post-distribution repurchase-price adjustment will be excluded unless they also contain an implementation-distribution title pattern. Genuine `权益分派实施公告` and `利润分配实施公告` remain eligible.

5. **Report machine-readable blocker diagnostics.** Canonical maintenance will expose `blocker_reason`, predecessor details, candidate gates, retry scope/count, and bounded unresolved announcement samples. The scheduler message will render these fields directly instead of requiring database inspection.

6. **Preserve BSE empty-window semantics.** A complete BSE scan with zero matching implementation notices remains `success`; only transport, normalization, document, parse, or persistence failures can make the BSE stage partial.

7. **Revalidate persisted semantic queues on policy upgrades.** Deferred special announcements are state, not immutable source evidence. Each run reclassifies their stored titles with the active policy before carrying them forward. Entries that are now deterministic non-XDXR are removed together with an unmatched-special candidate reason when no other deferred semantic evidence remains. Entries now recognized as ordinary structured distributions are rerouted to the normal refresh candidate path. Missing-title or otherwise unclassified records fail closed and remain queued.

## Risks / Trade-offs

- **[Compatibility verification is weaker than a producer-written watermark]** -> Require both local quote cutoff and composite path evidence, label the recovery mode, and persist a normal watermark so the exception is not repeated indefinitely.
- **[Title exclusions could hide a real mixed-purpose announcement]** -> Give explicit implementation-distribution patterns precedence over exclusions and add representative positive and negative tests.
- **[Existing 115 retry markers remain after code deployment]** -> The next successful maintenance run will replace the retry scope with genuine pending instruments; add a targeted regression test for stale queue closure.
- **[Weekend validation cannot prove a new trading-session update]** -> Unit/integration tests validate both success-empty weekends and trading-day predecessor advancement; the next real trading-day run remains the operational confirmation.

## Migration Plan

1. Deploy the code and restart the application.
2. Run the normal A-share quote/factor update or the daily corporate-action task. Missing predecessor watermarks may be recovered only when local evidence is complete.
3. Re-run `a_share_cninfo_corporate_action_daily_sync`; confirm canonical targeted merge succeeds or reports a specific real blocker.
4. Confirm the 115 stale `daily_factor_retry` rows are removed unless individual factor defects remain.
5. Rollback requires only reverting the code; no schema changes or source-data rewrites are introduced.

## Open Questions

None. The next live trading-day execution is operational validation, not a design dependency.
