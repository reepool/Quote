## Context

The financial disclosure incremental service combines newly scanned announcement events with persisted maintenance state before applying a bounded candidate limit. It currently loads `accepted_disclosure_gap` rows into the daily pool, reads all pending rows without checking `pending_recheck_until`, and relies on broad periodic-report keywords that classify performance forecast announcements as formal filings. The scheduler status is derived from write/blocking counts and scan errors, while source-routing errors are only rendered as report text.

The change must preserve existing fact schemas, source order (`CNInfo data20 -> THS/Sina`), lifecycle-gap auditability, and the separate weekly reconciliation responsibility. It must remain compatible with existing event-state rows created by older versions.

## Goals / Non-Goals

**Goals:**

- Make the daily pool contain only new announcement candidates and active pending rechecks.
- Expire pending rechecks deterministically without deleting their audit rows or resetting their original deadline.
- Filter performance-result announcements while allowing explicit delayed-disclosure or listing-risk signals through.
- Ensure official-source degradation is visible in the task status even when fallback writes succeed.
- Keep candidate-limit selection deterministic and testable, and retain operator diagnostics for skipped/expired records.

**Non-Goals:**

- Changing canonical financial fact mappings, source priority, report-period history, or database schemas for facts.
- Retrofitting historical accepted gaps through the daily task; reconciliation remains responsible for those rows.
- Making CNInfo mandatory when configured fallback sources produce usable facts.
- Enabling the disabled filing-vintage rollout stage.

## Decisions

1. **Separate daily eligibility from reconciliation eligibility.**
   The incremental candidate builder will load only `pending_recheck` and `pending_delisting_risk` states whose `pending_recheck_until` is at or after the current Shanghai time. `accepted_disclosure_gap` remains queryable and auditable but is excluded from the daily pool. Reconciliation continues to inspect accepted gaps through its report-period scan.

2. **Use a terminal expired-pending state.**
   A pending row past its deadline will be marked `pending_recheck_expired` with the original `first_pending_at` and `pending_recheck_until` retained. It will not be retried or receive a new deadline. A terminal state is preferred over silently deleting the row so operators can distinguish an exhausted retry from a missing audit record.

3. **Make noise filtering explicit and exception-safe.**
   Add performance forecast/result keywords to the non-primary title set. The existing exception remains: an announcement with an explicit delayed-disclosure or trading/listing-risk phrase can still produce a disclosure-risk event. Period inference remains title-based and is never guessed from an ambiguous year.

4. **Derive status from source routing as well as write outcomes.**
   Pass the repair summary into status derivation. Any official-source routing error, unresolved fallback target, or pending recheck after attempted repair makes the run `degraded`; successful fallback writes remain counted as changed/written. This avoids treating a complete CNInfo failure as an unqualified success while preserving availability.

5. **Keep bounded selection deterministic.**
   Retain balanced selection across exchange/profile/report period for eligible daily candidates, but report expired pending counts and candidate-source counts separately. Since accepted gaps are removed from the daily pool, the existing limit no longer competes with historical lifecycle rows.

## Risks / Trade-offs

- **[Risk]** Excluding accepted gaps from daily runs could delay discovery if a previously accepted disclosure later becomes available without a new announcement. **Mitigation:** weekly reconciliation continues to scan the configured period window and explicitly rechecks accepted gaps.
- **[Risk]** Marking source-routing errors as degraded may increase alert volume during a transient CNInfo outage even when fallback succeeds. **Mitigation:** reports retain fallback success counts and classify the run as available-but-degraded rather than failed.
- **[Risk]** Adding result-announcement keywords could filter an unusual formal filing title. **Mitigation:** delayed-disclosure and listing-risk signals bypass the noise filter; unit tests cover both ordinary and exception titles.
- **[Risk]** Existing rows may use statuses not known by the new loader. **Mitigation:** only add the terminal status and keep unknown rows untouched; storage queries remain status-scoped.
