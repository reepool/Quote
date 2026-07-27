## Context

CNInfo raw observations, announcement candidates, archived pages, LLM analyses,
reviews, and resolved-term overlays already exist. The ordinary review path
requires document-backed all-shareholder validation, while the existing
asymmetric passthrough copies the current CNInfo observation unchanged. Neither
path can express an operator correction to total-share-capital economics,
beneficiary-only economics, or a recorded event that has no adjustment-factor
effect.

The remaining backlog should be handled from persisted CNInfo data. Raw
observations and raw announcement scans are immutable, and this change must not
introduce another data source, redownload documents, or rerun the LLM for the
four explicit operator decisions.

## Goals / Non-Goals

**Goals:**

- Persist operator-approved asymmetric corrections with full supersession and
  source lineage.
- Keep factor-relevant total-share-capital terms distinct from terms applying
  only to circulating or otherwise eligible shareholders.
- Record an implemented corporate action even when it has no adjustment-factor
  effect.
- Reduce future discovery cost by excluding clearly irrelevant announcement
  document types before title LLM and document processing.

**Non-Goals:**

- Do not infer total-share-capital economics for every unresolved asymmetric
  event.
- Do not delete or rewrite raw CNInfo observations, announcements, pages, or
  analyses.
- Do not automatically approve ordinary dividends or ambiguous special events.
- Do not change production factor source selection.

## Decisions

1. **Add a separate manual asymmetric review method.** The method accepts only
   a current CNInfo source event, an existing analysis identity, explicit
   operator terms, an effective date, and an adjustment-factor effect. It writes
   through the existing atomic review bundle and supersedes the latest prior
   review. This is preferable to weakening the ordinary evidence gates.

2. **Store additional semantics in existing JSON lineage.** No schema migration
   is required. `review_payload_json` records the policy, original observation,
   total-share-capital terms, beneficiary-only terms, operator instruction, and
   factor effect. `evidence_json` on resolved terms repeats the factor effect so
   factor consumers can load it efficiently and audit APIs expose it.

3. **Use `factor_effect=normal` or `factor_effect=none`.** `normal` means the
   total-share-capital fields contribute to factor derivation. `none` means the
   corporate action remains resolved and queryable but all of its economic
   fields are excluded from factor aggregation. Zeroing the stored terms was
   rejected because it would erase the actual event.

4. **Apply reviewed fields as authoritative overlays.** A manual asymmetric
   correction can replace an existing non-null raw observation value. This
   differs intentionally from automatic repair overlays, which only fill
   missing or placeholder values.

5. **Use conservative title prefilter rules.** Strong non-implementation
   document types such as legal opinions, voting results, periodic reports,
  valuation reports, replies, pledges, and transfer-registration notices are
  excluded by deterministic title rules. Broad role words such as `董事会`,
  `监事会`, `独立董事`, `股东大会`, and `国资委批准` are excluded unless the title
  also contains a strong implementation marker.

6. **Preserve filter lineage.** Prefiltered announcements remain visible in
   discovery results and are persisted as rejected candidate evidence with a
   deterministic reason. They are omitted only from title LLM input and
   downstream document resolution.

7. **Do not bulk-approve the remaining backlog solely by category.** A special
   event marker establishes asymmetric classification, but automatic approval
   still requires an existing trustworthy total-share-capital term and factor
   effect. Items lacking either remain manual.

8. **Allow review-only CNInfo passthrough without an LLM analysis.** When an
   operator keeps the current CNInfo economic terms unchanged, selects
   persisted official announcement evidence, and declares
   `factor_effect=normal`, the system may persist a resolved review and date
   evidence without a resolved-term overlay. The factor path continues to read
   the raw CNInfo terms. A missing analysis MUST NOT be replaced with a
   synthetic LLM row. Term-changing overrides and `factor_effect=none` continue
   to require the existing resolved-term overlay path.

9. **Keep TDX outside the CNInfo factor path.** TDX comparisons may be retained
   as audit lineage, but neither TDX economic fields nor TDX day factors may
   become the authoritative terms of a CNInfo manual decision. Non-tradable-only
   share contraction is descriptive capital-structure lineage unless an
   official CNInfo price-adjustment term explicitly represents it.

10. **Allow exact operator-approved TDX date alignment.** For the explicitly
    reviewed 15-event asymmetric conflict list, an exact persisted TDX row may
    supply the CNInfo factor effective date after its exchange calendar confirms
    that the date is a trading session and the date is compatible with the
    CNInfo record, payment, or share-arrival timeline. CNInfo economic fields
    remain unchanged and no resolved-term overlay is written. TDX economic
    fields and factors are retained only as audit comparison lineage.

11. **Require exact event and TDX identities.** Economic disagreement remains a
    blocking condition for unattended automation. The operator path accepts
    only an explicit CNInfo source-event key and TDX row ID pair, validates the
    current instrument and expected TDX date, and records review supersession.

12. **Treat the 55-event workbook decision as an explicit fixed-list review.**
    The operator instruction does not create a category-wide auto-approval
    rule. The write script freezes each event key, analysis, official
    announcement, effective date, and factor effect. Events outside that list
    remain subject to the ordinary gates.

13. **Allow TDX date-only lineage on a manual CNInfo terms overlay.** When an
    explicitly selected TDX row supplies the date for a reviewed economic
    conflict, validate its instrument, expected date, and exchange session,
    then persist the TDX evidence identity separately from the selected CNInfo
    announcement. The resolved terms remain the operator-approved CNInfo terms;
    `tdx_economic_terms_used` and `tdx_factor_used` remain false.

14. **Represent an official special reference-price adjustment explicitly.**
    For the five reviewed restructuring events whose announcements publish both
    the pre-adjustment reference price and adjusted opening reference price,
    store those prices in review lineage and use their ratio as the event
    factor. Do not pass the asymmetric capitalization ratio through the normal
    all-shareholder formula.

## Risks / Trade-offs

- **[Risk] A broad title word hides an implementation notice.** → Strong
  implementation markers override the broad-role filters, and tests cover
  mixed titles.
- **[Risk] Manual terms are entered in the wrong unit.** → The API accepts only
  per-share normalized values and records the original operator instruction;
  tests use the four known decisions.
- **[Risk] `factor_effect=none` is ignored by an older consumer.** → Extend the
  resolved-term loader and the current CNInfo factor rebuild together, with
  regression coverage.
- **[Risk] Existing reviews are silently overwritten.** → New reviews include
  `supersedes_review_id`; raw observations and prior review rows remain intact.
- **[Risk] An analysis-free review accidentally changes economics.** → Permit
  this path only when every supplied total-share-capital term equals the current
  CNInfo observation and `factor_effect=normal`; otherwise reject it.
- **[Risk] An omitted overlay leaves stale prior terms active.** → The atomic
  review bundle deactivates an existing resolved-term overlay when the new
  approved state explicitly returns to unchanged raw CNInfo terms.
- **[Risk] A nearby but unrelated TDX row supplies the date.** → Require the
  exact reviewed TDX row ID, matching instrument, expected date, supported
  asymmetric category, compatible CNInfo timeline, and an exchange trading
  session before permitting the operator approval.
- **[Risk] TDX beneficiary values leak into CNInfo economics.** → Persist
  `tdx_date_used=true`, `tdx_economic_terms_used=false`, and
  `tdx_factor_used=false`; derive all factor terms from the raw CNInfo
  observation and write no economic overlay.
- **[Risk] A blanket instruction silently resolves unsupported rows.** →
  Freeze and validate the exact 55-event approved list, while retaining six
  no-candidate rows, one proposal-stage document row, and one announcement
  with conflicting record dates as blockers.
- **[Risk] A special restructuring adjustment uses the ordinary bonus
  formula.** → Require two positive official reference prices and derive the
  override factor deterministically as pre-adjustment price divided by adjusted
  reference price.

## Migration Plan

1. Deploy code and tests without a schema migration.
2. Persist the four explicit operator decisions from existing CNInfo data.
3. Query reviews and resolved terms to verify supersession, values, dates, and
   factor effects.
4. Run a targeted dry-run factor rebuild for the four instruments and verify
   `000035.SZ` contributes no event factor.
5. Enable the deterministic title prefilter for subsequent discovery runs.
6. Apply and audit the `000623.SZ` CNInfo-only review without downloading or LLM
   work, then verify that raw CNInfo, TDX audit, and production factor rows are
   unchanged by the review write.
7. Preview and apply the exact 15-event operator list, verify all selected TDX
   dates are exchange sessions, and verify CNInfo/TDX economic tables and
   production factors remain unchanged.
8. Roll back by disabling the new review endpoint/prefilter and superseding any
   manual review with another governed review; raw data remains unchanged.
9. Preview and apply the fixed 55-event workbook decision, then verify eight
   excluded blockers remain and no raw CNInfo, TDX audit, or production-factor
   row changed.

## Open Questions

- The unresolved backlog can later support a separate batch policy for events
  whose total-share-capital economics and factor effect are already explicit.
  This change intentionally does not guess those values.
