# Design: stabilize-announcement-xdxr-triage-exclusions

## Context

Incident (2026-10-27 investigation):

1. `600572.SH` published `公司股改限售股上市流通公告` (SSE lockup-listing
   title variant without `份`). The deterministic non-XDXR rule
   `restricted_shares_listing_circulation`
   (`cninfo_corporate_action_incremental.py:_daily_title_exclusion_reason`)
   requires `限售股份`, so the title escaped exclusion; the `股改` marker made it
   an exceptional announcement and it became a `share_reform` case
   (`routing_status=active_pending`, persisted in `announcement_scan_state`
   metadata).
2. The LLM classifier follows its system prompt ("restricted-share listing alone
   are non-XDXR") and answers `non_xdxr` with its own action family; the response
   validator (`cninfo_announcement_xdxr_triage.py`, `classify`) requires
   `action_family` equality with the case and raises
   `ValueError: announcement XDXR action family mismatch`.
3. The errored case never sets `classified_evidence_hash`, so the dispatch filter
   re-queues it every run (`max_cases` newest-first); identical payload +
   idempotency key returns the same cached decision; the error repeats nightly.
4. Report consequence: `异常治理仍有待办（未匹配 1）` → job `部分完成`.

Relevant mechanics verified against code:

- `_daily_title_exclusion_reason` first returns `None` (no exclusion) for titles
  carrying genuine distribution implementation markers
  (`权益分派实施/利润分配实施/现金红利发放/分红派息实施`), and the lockup rule
  carries its own implementation guards
  (`转增股本实施/转增股份实施/股份到账/股权登记/除权/除息`). Combined
  announcements stay selectable.
- The triage governance re-filter (`govern`) drops deterministically-excluded
  announcements from unmatched lists and refreshes a case when **part** of its
  evidence is excluded; the **fully**-excluded case is currently left untouched
  and stays dispatchable.
- `_is_deterministically_excluded` returns `False` for announcements matched to
  structured events, so structured evidence is always retained.
- `apply_announcement_xdxr_decision` does not write `action_family` back to the
  case; the decision's family is validation-only, and case identity
  (`instrument_id` + `action_family`) stays case-side.
- Routing (`apply_announcement_xdxr_decision`) is driven by
  likelihood/confidence thresholds: `non_xdxr` with low likelihood lands in
  `inactive_watch` (非除权).

## Goals / Non-Goals

Goals:

- Make every lockup-listing title variant deterministically excluded at
  discovery, before any LLM call.
- Make fully-excluded announcement-only cases terminal so the backlog and the
  nightly LLM retry loop stop by themselves.
- Stop punishing legitimate `non_xdxr` verdicts that judge the economic action
  under a different family label than the title marker.

Non-goals:

- No LLM prompt/schema change (the prompt already states the right economics).
- No routing-threshold change (`low_likelihood/high_likelihood/confidence_floor`
  stay).
- No persistence surgery for the existing stuck case; the new terminal branch
  heals it on the next scheduled run.
- No change to structured corporate-action matching, factor writes, or the
  operator report format.

## Decisions

### D1: Rule relaxation via substring `限售股` (not an added variant list)

`限售股份` contains `限售股`, so matching `限售股` alone covers
`限售股份上市流通`, `限售股上市流通`, `股改限售股上市流通`, and
`部分限售股上市流通` without enumerating variants. Combined titles stay
selectable through the two existing protections (genuine-distribution
implementation markers return `None` before this rule; the lockup rule's own
implementation-marker guards). An enumerated variant list would be exactly the
kind of special-casing this project avoids.

### D2: Fully-excluded cases become terminal in the re-filter

In `govern`'s per-case re-filter, after the structured-availability check, an
active case whose retained announcement list is empty gets
`routing_status = "deterministic_excluded"` (terminal, mode-independent).
Structural precedence:

1. `case_keys & structured_announcement_keys` → `structured_event_available`
   (existing; retained is never empty when a structured match exists, because
   `_is_deterministically_excluded` exempts structured keys — the ordering is
   defensive only);
2. no retained announcements → `deterministic_excluded` (new);
3. disabled/shadow deterministic fallback (existing).

Terminal cases persist in scan-state metadata (existing design for terminal
routing statuses) but are no longer dispatched (`ACTIVE_ROUTING_STATUSES`
filter) and no longer surface as unmatched.

### D3: Family divergence accepted only for `non_xdxr`

In the classifier's response validation:

- `disposition == "non_xdxr"` and divergent family → accept the decision, keep
  the case's action family as the decision's `action_family` (normalization is
  cosmetic — the applied decision never persists family), and record the LLM's
  own label in `llm_action_family` for diagnostics.
- Any other disposition (`probable_xdxr`, `future_or_proposal`, `uncertain`)
  with a divergent family → keep raising. For those outcomes the family feeds
  real XDXR routing; a divergent label there means the model misread the case
  and deserves a loud failure, not a silent accept.

Trade-off accepted: an LLM could return `non_xdxr` with a contradictory high
likelihood; routing is likelihood-driven, so such a case lands in
`active_probable_xdxr`/`active_uncertain` rather than being silently buried.
That contradiction is an LLM quality signal, not a validator concern.

### D4: Organic healing for the live incident — two verified paths

The 600572.SH case persists in scan-state metadata with its announcement and a
`pending_special_announcements_by_instrument` entry. On the first run after
deployment (scheduler daemon reload required) both carryovers heal
independently:

1. **Carryover revalidation re-classifies persisted pending specials**
   (`data_manager.py` carryover loop calls `classify_daily_corporate_action_title`
   per announcement): the relaxed rule yields
   `deterministic_exclusion:restricted_shares_listing_circulation`, the entry is
   counted `excluded` and dropped from the deferred/unmatched lists.
2. **The triage re-filter terminal branch (D2) marks the existing case
   `deterministic_excluded`**, removing it from LLM dispatch. This branch is
   load-bearing: the triage result is `partial` whenever any dispatched case
   errors, so without the terminal branch the nightly mismatch error would keep
   the job at `部分完成` even after the carryover list cleared.

Both mechanisms work through existing code paths; no manual state edit, no
reprocessing of historical cases. The terminal branch also clears any stale
`last_error` on the case (the field is otherwise only cleared on a successful
decision application), so terminal state stays inspectable without leftover
error noise.

## Risks / Trade-offs

- Relaxing to `限售股` could over-exclude a hypothetical title like
  `限售股上市流通暨转增股本实施` — protected by the existing guard markers
  (`转增股本实施` etc.); genuinely combined distribution implementations are
  protected by the genuine-distribution early return. Residual risk is a title
  phrasing that mentions neither distribution implementation nor guard markers
  while still carrying an ex-rights action — considered negligible for the
  lockup-listing genre.
- Terminal `deterministic_excluded` cases accumulate in scan-state metadata.
  This follows the existing terminal-status design (`structured_event_available`
  cases persist the same way); bounded by case volume, no cleanup contract
  added here.
- No new counters, tables, or report segments: the observable is the
  disappearance of `未匹配/待决/待处理公告` for this class.

## Migration Plan

Single vertical slice: rule relaxation + terminal branch + validator relaxation
+ tests. Deployment is the standard scheduler-daemon reload; the live stuck case
self-heals on the first subsequent run (acceptance 4.2).

## Open Questions

None.
