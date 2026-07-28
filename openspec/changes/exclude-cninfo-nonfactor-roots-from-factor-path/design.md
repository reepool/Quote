## Context

The CNInfo primary factor rebuild currently loads current source observations,
resolved dates, and resolved economic terms, but it does not load the persisted
resolution-state disposition or instrument listing date into factor input rows.
`derive_cninfo_factor_path` therefore treats every missing date as a factor
error and intentionally blocks all later cumulative events for the instrument.

The current database has three distinct cases:

- Explicit `non_effective`, `scope_mismatch`, and operator-confirmed
  `superseded` terminal events that must not affect factors.
- Events with an explicit source date before listing, for which listed-market
  quote evidence cannot and should not exist.
- `official_archive_unavailable` events that may have real economics but lack a
  recoverable CNInfo implementation announcement.

TDX is an audit/reference source. It may supply an effective date when a unique
economic event matches, but its economic fields and factor must not replace the
CNInfo values.

## Goals / Non-Goals

**Goals:**

- Make terminal non-factor governance dispositions effective in factor
  derivation.
- Exclude confirmed pre-listing events before quote lookup.
- Resolve archive gaps from a unique, bounded, economically matching TDX date
  while retaining CNInfo terms.
- Collapse an unresolved historical root to one auditable gap and annotate the
  affected cumulative path instead of emitting one pending row per later event.
- Keep unresolved historical gaps visible in completeness and promotion gates.

**Non-Goals:**

- Copy TDX cash, bonus, rights, rights price, factor, or cumulative factor into
  the CNInfo path.
- Infer pre-listing implementation from announcement date alone.
- Promote or overwrite the production factor series in this change.
- Download documents, rerun OCR, or invoke LLM analysis.
- Treat every terminal governance state as economically ineffective.

## Decisions

1. Factor input assembly attaches `resolution_state` and `listed_date`.
   `non_effective`, `scope_mismatch`, and operator-confirmed `superseded` map
   to an explicit no-factor disposition. `official_archive_unavailable` maps
   to a historical-gap disposition unless date-only reference evidence
   resolves it.

2. Pre-listing exclusion uses the selected source/effective date and requires
   that date to be strictly earlier than `listed_date`. Announcement date alone
   is never sufficient. Excluded events retain their source observation and are
   reported with reason `pre_listing_corporate_action`.

3. Archive date recovery compares normalized CNInfo economic terms with TDX
   events for the same instrument inside role-specific bounded anchor windows.
   Operational anchors take precedence over announcement date; announcement
   date is a fallback limited to the following 120 days. Record date matches
   forward only, while pay and share-arrival dates allow a short lookback and a
   bounded forward window for suspended shares. Candidate loading uses these
   anchor windows independently of the requested rebuild slice so a narrow
   rebuild reaches the same historical date conclusion as a full rebuild. The
   matcher accepts only one economic match, respects TDX source precision
   tolerances, rejects pending TDX rows, and returns only the TDX event date and
   audit metadata.

4. An unresolved archive gap is returned separately from `pending`. Later
   derived events remain available for event-level auditing but carry
   `path_has_prior_historical_gap=true`. "Later" is determined from the
   earliest operational anchor, or announcement date only when no operational
   anchor exists. A gap with no usable anchor conservatively affects the entire
   instrument path. Instruments with such gaps remain excluded from
   completeness and promotion eligibility.

5. Existing true calculation failures retain the current fail-closed
   propagation behavior. The change only removes propagation caused by an
   explicitly governed non-factor event or separately reported archive gap.

## Risks / Trade-offs

- [A broad date window could find an unrelated TDX event] -> Use role-specific
  directional windows, prefer operational anchors over announcement date,
  require matching instrument and all normalized economic fields, accept only
  one candidate, and preserve candidate diagnostics when ambiguous.
- [A historical archive gap could be hidden] -> Keep it in a dedicated result
  collection, mark all later path rows, and fail the historical-gap quality
  gate.
- [Announcement dates could be mistaken for implementation dates] -> Never use
  announcement date for pre-listing exclusion; it is only a bounded search
  anchor for unique TDX matching.
- [Resolved operator evidence could be overridden] -> Apply governed/operator
  resolved dates before archive matching and never run archive matching when a
  resolved date already exists.

## Migration Plan

1. Deploy the read-path and derivation changes without schema migration.
2. Run focused unit tests and the full SSE/SZSE factor rebuild in dry-run mode.
3. Confirm pre-listing and terminal non-factor roots leave `pending`, archive
   gaps are bounded root records, and no production rows are written.
4. A later operator run may persist factor observations after quality results
   are reviewed.

Rollback is a normal code revert; source observations and existing factor tables
are not migrated by this change.

## Open Questions

None. Unmatched archive gaps remain explicitly incomplete rather than being
assigned a guessed date or factor.
