## Context

The existing manual asymmetric-review API writes an audited review, optional
resolved-term overlay, effective-date evidence, and refreshed governance state.
It permits an analysis-free passthrough only when current CNInfo terms are
unchanged, a persisted CNInfo announcement is selected, and
`factor_effect=normal`. This deliberately kept the final eight events blocked.

The operator has now supplied explicit decisions for each frozen event. Six
events have no usable implementation announcement or LLM analysis, so applying
their decisions through the existing contract would require either fabricated
source evidence or an unjustified LLM rerun. Neither is acceptable. Review
payload JSON and effective-date evidence already support additional audited
lineage without a schema migration.

## Goals / Non-Goals

**Goals:**

- Persist exactly eight explicit operator decisions without changing raw
  CNInfo observations.
- Represent operator attestation as its own evidence source rather than as a
  synthetic CNInfo announcement or LLM analysis.
- Make analysis-free `factor_effect=none` visible to the factor loader even
  though the resolved-term table requires a non-null LLM analysis identity.
- Validate the frozen manifest, unchanged CNInfo economics, event dates,
  official price references, and post-write governance state.
- Preserve idempotent reruns and prove review writes do not mutate source or
  factor tables.

**Non-Goals:**

- Do not automatically approve future events by category, title, or age.
- Do not loosen automatic promotion, discovery, LLM, or ordinary evidence
  gates.
- Do not copy TDX economics or factors into CNInfo.
- Do not add a database migration solely to attach no-effect metadata to an
  analysis-free event.

## Decisions

1. **Use a structured operator-attestation payload.** The manual review method
   accepts an optional object containing an explicit basis and supporting
   facts. An absent announcement is allowed only with this object,
   `approval_classification=approved_cninfo_operator`, unchanged current
   CNInfo terms, and no TDX date substitution. This is more auditable than
   overloading an announcement identifier.

2. **Keep official-reference-price reviews document-backed.** An operator
   attestation cannot replace the persisted announcement for
   `factor_effect=official_reference_price`. `002076.SZ` uses announcement
   `1215397977`, effective date `2022-12-21`, and official prices `2.60` and
   `2.23`.

3. **Expose analysis-free no-effect reviews through the resolved policy
   loader.** The resolved-term loader supplements active term overlays with
   the latest resolved operator-attested review whose factor effect is `none`.
   The fallback contains no resolved economic fields and therefore cannot
   change CNInfo values; it only declares the no-effect policy.

4. **Use one immutable fixed-list script.** The script freezes all eight event
   keys, expected current-observation hashes, dates, factor effects, optional
   analysis and announcement identities, and the complete canonical decision
   payload hash. Preview is the default; `--write` is restricted to the
   configured `data/quotes.db`.

5. **Validate factor semantics after governance resolution.** The two suspended
   events remain `normal` at `2013-02-08`; five events are excluded with
   `none`; `002076.SZ` uses `2.60 / 2.23 = 1.165919282511`. The exact
   unrounded theoretical price remains audit commentary and is not mixed with
   the exchange-published two-decimal opening reference price.

6. **Shift suspended implementation events to the first resumed session.**
   The announcement implementation date remains the source event date. When
   the first available quote on or after that date is explicitly suspended,
   factor evidence uses the first later `tradestatus=1` quote, up to the rebuild
   `end_date`. Without that suspension evidence, lookup retains the normal
   fourteen-day bound. The prior close for the ordinary event formula remains
   the last valid traded close before that resumed session. Multiple actions
   implemented during one suspension are compounded in source-date order and
   share the resumed effective date.

## Risks / Trade-offs

- **[Risk] Operator attestation becomes an automatic bypass.** → Require an
  explicit structured payload, operator classification, unchanged source
  terms, and a manual API call; the production script additionally freezes
  exact event and payload hashes.
- **[Risk] Review-only no-effect policy changes economics.** → Return no
  resolved fields from the loader fallback and keep authoritative economic
  override false.
- **[Risk] A stale prior review wins.** → Select the latest resolved review per
  event and preserve normal review supersession keys.
- **[Risk] Special-factor direction is inverted.** → Continue using the
  established project convention of pre-adjustment price divided by adjusted
  price and assert the exact expected factor.
- **[Risk] Partial batch writes leave mixed state.** → Keep decisions
  idempotent, report persisted and pending keys, and rerun the same frozen
  command to complete the audit.
- **[Risk] An unbounded resumption search writes a future factor outside the
  requested rebuild.** → Require an explicit maximum effective date from the
  rebuild and leave the event pending when no valid traded quote exists by
  that date.
- **[Risk] Missing quote history is mistaken for suspension.** → Extend beyond
  fourteen days only when the first available row is explicitly marked
  suspended.
- **[Risk] Multiple suspended-period actions are added as simultaneous
  terms.** → Aggregate only same-source-date terms and compound distinct event
  dates in chronological order before emitting one resumed-session factor.

## Migration Plan

1. Deploy the validation, policy-loader fallback, fixed script, and tests.
2. Run the script without `--write` and verify the exact eight-event manifest,
   immutable source snapshots, and expected factor-effect counts.
3. Run with `--write`, audit eight latest reviews, and verify zero remaining
   factor blockers.
4. Rebuild the CNInfo factor path and verify two normal events, five no-effect
   exclusions, and one official-reference-price event.
5. Rebuild `002076.SZ` with bounded resumption lookup and verify the
   `2014-06-13` and `2017-06-01` implementation events become effective on
   `2014-09-11` and `2017-10-12`.
6. Roll back decisions by superseding them through the same governed review
   API; raw observations and prior reviews remain available.

## Open Questions

None. The operator supplied all event-specific dates and factor treatments.
