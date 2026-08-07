## Context

The annual-report pipeline uses CNInfo's governed annual category, 30-row pages, a 240-page per-window bound, local full-report classification, and persisted date-window bisection. A dense multi-day window currently reads all 7,200 allowed rows before it is bisected. Child scans then restart at page one because only a fully completed publication watermark is committed. CNInfo responses expose `totalpages`, but the provider currently ignores it.

The discovery stream is newest-first. Page offsets are unsafe for a live window because new announcements can shift rows between pages; they are acceptable for a historical single publication date that has already ended, with frontier identity deduplication as an additional safeguard.

## Goals / Non-Goals

**Goals:**

- Detect an over-bound multi-day result after one CNInfo page and split the window before a 240-page scan.
- Preserve the first-page discoveries and existing fallback suppression for resumable page bounds.
- Continue an unsplittable historical single-day window in bounded page chunks.
- Persist and expose enough pagination state for unattended progress and diagnosis.
- Read legacy persisted window state without migration.

**Non-Goals:**

- Trusting page offsets for fresh/current-day windows.
- Removing local title classification or weakening completeness checks.
- Changing default page limits, provider routing, queue concurrency, PDF acquisition, or semantic processing.
- Adding a database table or external dependency.

## Decisions

### Treat provider totals as planning hints, not completeness evidence

The CNInfo transport records a validated positive `totalpages` value, with a `totalAnnouncement`-derived fallback. When explicitly requested by the business-profile planner, a first page whose total exceeds the configured page allowance returns an incomplete `estimated_pages_exceed_bound` result. The first-page records remain available for normal selector, frontier, and audit persistence.

Using a separate count endpoint was rejected because CNInfo already returns the total with page one. Treating totals as proof of completion was rejected because upstream counts can drift while a query is running.

### Keep preflight opt-in and source-neutral

The planner requests preflight and page continuation through transient `AnnouncementScope` pagination fields that are deliberately excluded from the stable stream identity. The CNInfo provider owns interpretation of provider totals. Other announcement workloads retain their current bounded scan behavior, and source-neutral callers do not handle raw CNInfo response fields.

Enabling early exit globally was rejected because other jobs may intentionally prefer partial records up to their page allowance.

### Split multi-day windows; checkpoint only closed single days

Multi-day resumable windows enable preflight and are bisected when the provider reports an over-bound estimate. An incomplete one-day backlog stores the provider's `next_page` and resumes from it only when the date is earlier than the run cutoff. Fresh windows and current-day windows always begin from page one and use committed publication watermarks when available.

Persisting page offsets for all windows was rejected because inserts at the front of a live result set could skip records. Increasing the 240-page limit was rejected because it increases request cost without addressing convergence.

### Preserve compatibility in state and telemetry

Window dictionaries gain optional `next_page`; legacy dictionaries remain valid. Provider pagination diagnostics are copied into per-exchange discovery results, and `estimated_pages_exceed_bound` joins the service's resumable page-bound reasons so partial CNInfo results are not discarded through fallback.

## Risks / Trade-offs

- [CNInfo omits or corrupts its total count] -> Ignore invalid totals and retain the existing bounded scan-and-split path.
- [A historical one-day index changes after a checkpoint] -> Restrict page continuation to dates before the cutoff and retain announcement-identity deduplication; fresh overlap scans continue to detect later corrections.
- [Preflight page one is read again in child windows] -> Accept one duplicated page per split because it is small, idempotent, and safer than transferring offsets across changed date predicates.
- [A single historical day exceeds multiple chunks] -> Persist successive `next_page` values until the provider-reported last page or a short page completes the window.
- [An alternate provider does not understand preflight options] -> Options remain inert outside CNInfo; resumable fallback suppression is based on the normalized stop reason.

## Migration Plan

1. Add CNInfo total-page and start/next-page diagnostics with focused provider tests.
2. Add the new resumable stop reason to acquisition routing.
3. Enable preflight and historical one-day continuation in business-profile discovery, preserving legacy window payloads.
4. Run focused provider, service, and production-operation tests plus strict OpenSpec validation.
5. Resume the existing backlog without modifying its database rows; new state is written naturally after the next cycle.

Rollback restores the prior provider/planner behavior. Optional `next_page` fields are ignored by older code, and all frontier/audit rows remain valid.

## Open Questions

None. The existing fresh overlap window remains the recovery mechanism for corrections published after a historical page checkpoint.
