## 1. DCF Input Governance Helpers

- [x] 1.1 Add pure helpers for conservative A-share financial availability dates and point-in-time financial-bundle selection.
- [x] 1.2 Add annual and TTM capital-expenditure derivation from `cash_flow_sheet.pay_fixed_assets_etc_cash` with structured lineage and incomplete-bridge diagnostics.

## 2. Local Domain Assembly

- [x] 2.1 Enrich DCF instruments and input identity with current or historical authoritative Shenwan level-1/2/3 membership.
- [x] 2.2 Enrich the selected DCF financial bundle with point-in-time local shares, capex, cash, interest-bearing debt, and lease liabilities before model execution.
- [x] 2.3 Resolve the China 10-year government-bond yield from `interests.db` on or before the valuation date and preserve configured fallback behavior.
- [x] 2.4 Update the professional DCF engine to preserve local risk-free-rate and Beta quality metadata, reject non-positive/low-quality observed Beta from the discount rate, and treat conservative A-share availability estimates as warnings instead of blockers.

## 3. Documentation And Feasibility

- [x] 3.1 Historical documentation delivered; company-profile policy subsequently superseded. Its obsolete delta is removed from this change and must not be reapplied. Current authority: deliver-a-share-core-profiles-and-commodity-exposure (see SUPERSEDED-PROFILE-SCOPE.md).
- [x] 3.2 Update the professional DCF requirements current-state section with the corrected interest-rate baseline and this change's input-governance behavior.

## 4. Verification

- [x] 4.1 Add unit tests for filing-date estimation, historical bundle selection, TTM capex, cash/debt semantics, Shenwan model selection, point-in-time shares, and local risk-free-rate lineage.
- [x] 4.2 Run targeted DCF, storage, OpenSpec strict validation, syntax, and diff-quality checks.
