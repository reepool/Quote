## Context

The September 11, 2026 Gemini shadow batch completed all twenty reports, but 15 segment scopes produced no accepted rows after partition reconciliation. Eleven reported only the conflicting field name (`reported_period`), because historical traces did not persist the two raw values; three explicitly failed because the model returned the ambiguous prefix `报告分部` instead of the complete table heading. Existing tests already establish that half-year periods, instant dates, duplicate cells, and genuine conflicts must remain distinct.

## Goals / Non-Goals

**Goals:**

- Make partition identity reconciliation tolerant of a small, explicit set of equivalent annual duration labels.
- Make complete report-segment headings available as schema choices when present in controlled Evidence.
- Preserve source values, source-native qualifiers, subject restrictions, Evidence ownership, and all substantive conflict guards.
- Prove the behavior without modifying the immutable batch or invoking an LLM.

**Non-Goals:**

- No fuzzy period matching, arbitrary substring acceptance, or generic heading alias table.
- No change to subject promotion, legal-empty Evidence ownership, cost-component rejection, coverage conflict handling, Gold expectations, budgets, or readiness thresholds.
- No new replay or production authorization.

## Decisions

1. **Closed annual normalization only in segment partition reconciliation.** For `period_type=duration`, only the prepared report date, `YYYY年`, and `YYYY年度` forms matching the prepared report year normalize to the canonical year. Narrower ranges, instant dates, event/expected periods, other strings, and non-partition extraction paths remain unchanged. This keeps source-native text in Evidence untouched and avoids expanding the change into a global time model.
2. **Enumerate the exact table-owning heading.** When Evidence contains `报告分部的财务信息`, add that exact line to the segment dimension enum. Do not accept `报告分部` as a generic prefix, do not admit the adjacent `报告分部的确定依据与会计政策` narrative heading, and do not map an ambiguous response after the fact.
3. **Offline proof before network use.** Unit fixtures exercise the normalizer and schema options; an audit compares the immutable batch hashes and reports only theoretically repairable failure classes. A future provider replay, if approved separately, uses a new identity.
4. **Preserve bounded conflict values in diagnostics.** Partition reconciliation errors include the conflicting metadata values for the closed identity fields. Historical responses were not persisted in full, so the next empirical run must reveal whether an unrecognized source-native annual label remains instead of collapsing all failures into a field-only message.

## Risks / Trade-offs

- [Risk] A report may use another annual label not in the closed set → retain the existing blocking review item rather than guess.
- [Risk] Multiple report-segment headings may coexist → expose only the table-owning financial-information heading; ambiguous, narrative-only, or invalid output still fails.
- [Risk] Offline evaluation could overstate runtime benefit → label it provider-free and do not change the historical result or readiness status.
