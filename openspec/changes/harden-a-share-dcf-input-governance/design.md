## Context

The DCF service currently loads the latest financial bundle and a quote-master instrument, then invokes `ProfessionalDcfEngine`. Authoritative Shenwan memberships live in `research.db`, shares in `valuation.db`, detailed cash-flow facts in `financials.db`, and the China 10-year government-bond yield in `interests.db`. Each dataset already has a local read path, but DCF assembly does not join them. The implementation must remain local-first, preserve point-in-time behavior, avoid database rewrites, and target A shares only.

## Goals / Non-Goals

**Goals:**

- Make authoritative Shenwan level-1/2/3 membership the primary A-share DCF industry identity.
- Assemble point-in-time share count and China 10-year government-bond yield from existing local stores.
- Derive annual or TTM capex from existing cash-flow details without treating cumulative interim cash flow as a full-year amount.
- Permit production DCF when an actual filing date is absent by using a conservative, explicit estimated availability date.
- Preserve DCF input assembly only; company-profile product policy has moved to the current all-A-share master contract.

**Non-Goals:**

- Backfilling or rewriting existing financial, valuation, industry, or interest-rate tables.
- Implementing Hong Kong financial DCF readiness.
- Building the complete company business-profile parser, commodity mapping population, or spread engine in this change.
- Replacing configured USD/HKD risk-free-rate fallbacks.

## Decisions

### Enrich at the DataManager boundary

`DataManager.get_research_dcf_valuation` will assemble local domain inputs before calling the pure valuation service. This keeps database routing out of formula code and lets the DCF engine continue to operate on an explicit input bundle. The enriched membership is copied into the instrument and financial bundle so model selection, input hashes, cache identity, diagnostics, and lineage see the same evidence.

Alternative considered: query research storage from `ProfessionalDcfEngine`. Rejected because formula code must not own storage or provider access.

### Use point-in-time Shenwan membership

For an explicit historical valuation date, use `get_industry_membership_as_of`. For a current valuation, use the preferred authoritative membership. Raw quote-master industry remains unchanged and is retained as reference evidence. If authoritative membership is unavailable, the existing fallback selector remains available but emits an industry-governance warning.

### Estimate missing filing availability conservatively

Actual `data_available_date` or `publish_date` remains authoritative. For SSE, SZSE, and BSE periodic reports with both fields absent, estimate availability at the statutory latest filing deadline:

- annual report: April 30 of the following year;
- first-quarter report: April 30 of the report year;
- semiannual report: August 31 of the report year;
- third-quarter report: October 31 of the report year.

The estimate is not presented as an actual publication date. It is stored in the runtime bundle with `estimated_statutory_deadline` source, an estimated quality flag, and a warning. This is deliberately later than most actual filings, trading some timeliness for protection against look-ahead bias.

Alternative considered: report period plus one day or a fixed 30-day lag. Rejected because that can expose statements before publication and creates avoidable backtest leakage.

### Select the financial bundle as of the valuation date

Load a bounded recent history and choose the latest bundle whose actual or estimated availability date is on or before the requested valuation date. This prevents a historical request from failing merely because the database also contains a newer report, and prevents future reports from entering the calculation.

### Derive capex with statement-period semantics

Use `cash_flow_sheet.pay_fixed_assets_etc_cash` as the capex cash outflow proxy. Annual reports use the absolute annual value. Interim cumulative values use `current cumulative + prior annual - prior-year same-period cumulative` to produce TTM capex. If bridge rows are missing, production remains blocked rather than silently annualizing a quarter. The bundle records the formula, source periods, field name, and source quality.

### Resolve shares and RMB risk-free rate on or before valuation date

Use `get_latest_valuation_input` for share count and `get_risk_free_rate_observations(..., end_date=valuation_date)` for `china_treasury_10y`. Rate values stored as `percent_annual` are divided by 100 before entering DCF. Both inputs record observation dates and local database lineage. Configured values remain explicit fallback only.

### Derive cash and interest-bearing debt from balance-sheet components

Use `total_cash` or `cash` for cash and equivalents. Build interest-bearing debt from available short-term loans, short-term bonds payable, current maturities of non-current debt, long-term loans, and bonds payable; keep lease debt separate. The source field named `total_debt` in the current detailed financial payload represents total liabilities for the relevant provider and must not enter DCF as interest-bearing debt. If debt components are absent, report the gap instead of falling back to total liabilities.

### Reject unusable observed Beta from CAPM

Preserve the on-demand Beta value, benchmark, quality flag, interpretation flags, R-squared, and p-value. A non-positive Beta or an observation explicitly rated low/unavailable is unsuitable as a production DCF discount-rate input and falls back to the configured default Beta with a warning. The raw observation remains in lineage; it is not overwritten or presented as the Beta used in WACC.

### Retired company-profile policy scope

The earlier company-profile policy is superseded and its delta removed. Industry membership can remain an input to DCF model selection under the DCF contract, but cannot by itself create company-specific commodity exposure. New profile semantics, fact-level acceptance, report_default_group_scope and commodity projections follow deliver-a-share-core-profiles-and-commodity-exposure. This DCF change does not authorize a new confidence-scoring platform, manufacturing-only profile admission or legacy Activity numeric compatibility.

## Risks / Trade-offs

- [Estimated dates delay many statements until the filing deadline] -> Preserve the conservative default and allow future source backfills to replace estimates with actual dates.
- [TTM capex proxy includes intangible and other long-term asset purchases] -> Name the source field and proxy explicitly; do not label it pure maintenance capex.
- [Current Shenwan membership may differ from historical membership] -> Use the historical membership API whenever an explicit historical valuation date is supplied.
- [Valuation share rows can contain future effective capital changes] -> Require both `as_of_date` and `data_as_of` not to exceed the valuation date through the existing latest-input read method.
- [AkShare interest-rate source is not an official direct adapter] -> Preserve its source profile and quality flag; configured fallback remains visible, and a future official Ministry of Finance/ChinaBond source can replace it without formula changes.
- [A historical DCF plan could reintroduce retired profile rules] -> Remove its obsolete profile delta; keep current profile acceptance and DCF consumption authorization separate.

## Migration Plan

1. Deploy read-only DCF assembly changes; no schema or data migration is required.
2. Run unit tests for pure date/capex helpers and DataManager local-domain assembly.
3. Compare representative bank, broker, insurer, utility, real-estate, coal, metal, steel, chemical, and oil-company model selection before and after enrichment.
4. Retain configured assumptions and keyword selection as visible fallbacks for unmapped instruments.
5. Roll back by reverting the assembly changes; underlying databases remain untouched.

## Open Questions

- Whether a later change should persist derived TTM capex snapshots or continue deriving them on demand.
- Which official or licensed source should become the long-term primary China government-bond yield source ahead of the current AkShare adapter.
- Company-profile rollout is no longer an open question of this DCF change; the current product contract uses an all-A-share common core with evidence-triggered enhancements.
