## Context

There are 82 nonterminal CNInfo events whose persisted raw classification is
`重整转增`, `股改分红`, or `承诺补偿`. They have no standard CNInfo ex-date, but
80 have a record date and all 81 affected instruments have persisted TDX XDXR
history. A read-only strict comparison currently finds 14 unique economic
matches close to the CNInfo record or arrival date.

The existing CNInfo/TDX factor reconciler assumes standard source ex-dates and
factor paths, so it cannot safely approve this backlog directly. The existing
asymmetric passthrough requires official-document and LLM evidence. The new path
must instead use only the operator-authorized CNInfo-to-TDX event comparison.

## Goals / Non-Goals

**Goals:**

- Reconcile only unresolved persisted CNInfo special events against persisted
  raw TDX XDXR rows.
- Automatically resolve only unique matches with compatible dates and every
  supported economic field within the existing strict normalized tolerance.
- Store `approved_asymmetric`, the selected TDX row, date-distance evidence, and
  field differences in immutable review lineage.
- Return a complete reasoned report for every non-match.

**Non-Goals:**

- Do not treat TDX as the source of the CNInfo observation or rewrite CNInfo
  terms.
- Do not approve CNInfo-only, TDX-only, ambiguous, or economically conflicting
  events.
- Do not use announcement dates as implementation dates.
- Do not download, OCR, classify titles, or invoke semantic extraction.
- Do not infer `factor_effect=none` from a TDX absence.

## Decisions

### Keep the persisted review decision compatible

The database decision remains `resolved`; the review payload and resolved-term
evidence record `approval_classification=approved_asymmetric` and
`resolution_policy=cninfo_tdx_asymmetric_match_v1`. Adding a new database
decision would require changing shared validation and every resolved-term
consumer without improving the business meaning.

### Match normalized raw economics, not production factor rows

CNInfo per-share fields are compared with TDX per-ten-share fields after
normalization. Cash, combined bonus/capitalization shares, rights shares, and
rights price must all be compatible. The existing exact tolerance of
`0.0001` per share is reused, which absorbs float32 noise but not economic
differences. The match does not depend on production factor-source selection.

### Use corporate-action date roles conservatively

A TDX ex-date is compatible when it equals a persisted CNInfo ex-date,
share-arrival date, or pay date, or when it is the record date or one of the
next three trading sessions. Announcement date alone never establishes a
match. Trading-session distance uses the persisted exchange calendar.

### Require a unique best match

Zero matches, multiple equally valid matches, missing calendar evidence,
out-of-window dates, and field conflicts remain unresolved. The report exposes
TDX candidates and normalized differences so the operator can assess the
remainder without rerunning external acquisition.

### Use the TDX event date as the reviewed effective date

For a unique match, the TDX XDXR date becomes the reviewed effective date and
the date basis states that it is a TDX XDXR comparison date. Raw CNInfo dates
and terms remain unchanged and are included in review lineage.

## Risks / Trade-offs

- [TDX may copy the same asymmetric beneficiary ratio as CNInfo] -> This path
  approves the event record but labels it asymmetric; it does not claim the
  terms are an all-shareholder equivalent.
- [A nearby unrelated TDX event has identical economics] -> Require the same
  instrument, a narrow role-aware date window, all fields, and a unique match.
- [Trading-calendar data is incomplete] -> Block the event rather than falling
  back to calendar-day guesses.
- [A matched asymmetric event has no factor effect] -> This change does not
  infer no-effect status; previously explicit operator no-effect overrides
  remain authoritative.

## Migration Plan

1. Deploy the deterministic matcher and report in dry-run mode.
2. Verify the 82-event distribution and inspect all matched rows.
3. Run the write mode to persist only unique matches.
4. Refresh governance inventory and verify promoted events are terminal and
   nonblocking.
5. Roll back by superseding an incorrect review; raw CNInfo and TDX rows are
   never modified.

## Open Questions

None. Events outside the conservative match contract intentionally remain for a
later operator decision.
