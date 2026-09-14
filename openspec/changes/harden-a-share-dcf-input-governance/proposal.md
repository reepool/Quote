## Why

The A-share professional DCF path has authoritative Shenwan memberships, share-count history, cash-flow detail, and a maintained China 10-year government-bond yield available locally, but it does not assemble those datasets into the DCF input bundle. As a result, model selection relies on coarse name/industry keywords and production calculations fail closed or fall back to manual assumptions even when usable local evidence exists.

## What Changes

- Enrich A-share DCF instruments with the authoritative current or point-in-time Shenwan level-1/2/3 membership before model selection, while preserving raw quote-master industry fields as reference-only data.
- Resolve share count from local valuation inputs on or before the requested valuation date and record source, as-of date, and lineage in the DCF bundle.
- Derive capital expenditure from the canonical cash-flow statement field for purchases of fixed, intangible, and other long-term assets, using a TTM bridge for cumulative interim statements when the required prior-period rows are available.
- Replace the blanket missing-availability-date production blocker for A-share periodic financial statements with an explicit conservative estimated availability date based on statutory latest filing deadlines. Actual publication dates remain preferred; estimates remain auditable and produce a warning.
- Resolve the RMB risk-free-rate assumption from `interests.db` using the latest China 10-year government-bond yield available on or before the valuation date, converting `percent_annual` to a decimal rate and falling back to configured assumptions only when local data is unavailable.
- Preserve point-in-time constraints for all enriched inputs and prevent future-dated shares, rates, classifications, and financial facts from entering historical DCF runs.
- The former company-profile governance scope is superseded by the all-A-share master requirements and deliver-a-share-core-profiles-and-commodity-exposure. Its obsolete delta has been removed and MUST NOT be synchronized during DCF archival.
- Limit this change to A shares. Hong Kong financial and industry DCF readiness remains deferred.

## Capabilities

### New Capabilities

None. The former company-business-profile-governance capability is retired; this change no longer defines company-profile or commodity-exposure policy.

### Modified Capabilities

- `professional-dcf-engine`: Use authoritative point-in-time Shenwan classification and local point-in-time financial, share-count, capex, and RMB risk-free-rate inputs, with conservative estimated filing availability when actual dates are absent.

## Impact

- Affected runtime code: `data_manager.py`, `research/professional_dcf.py`, and existing research storage read methods.
- Affected local databases: read-only consumption of `research.db`, `financials.db`, `valuation.db`, and `interests.db`; no schema migration or production data rewrite is required.
- Affected documentation: professional DCF current-state notes. Company-profile requirements are owned exclusively by the current all-A-share product contract; see SUPERSEDED-PROFILE-SCOPE.md.
- Affected tests: DCF input assembly, industry model selection, availability fallback, TTM capex derivation, point-in-time share count, and local risk-free-rate lineage.
