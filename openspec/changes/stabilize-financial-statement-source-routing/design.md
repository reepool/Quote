## Context

The authoritative maintenance owner is `FinancialMaintenanceRepairRouter`, called by the financial disclosure incremental service. It currently invokes the bounded official validation helper for CNInfo data20, then uses local readiness to decide whether THS/Sina fallback is needed. The validation helper labels an instrument-period as failed whenever strict core readiness is incomplete, even when the three CNInfo endpoints returned parseable structured JSON and wrote numeric facts.

CNInfo data20 exposes a compact balance-sheet template. Live samples and the existing fact catalog show the equity row as `所有者权益`, mapped to `equity_total`; the endpoint does not expose a distinct `归属于母公司所有者权益` or a minority-interest row. CNInfo's separate `getMainIndicators` endpoint exposes `每股净资产`, but deriving parent equity from that metric and paid-in capital would depend on undocumented share-count, face-value, and rounding semantics. That is not a safe canonical derivation.

The Sina AkShare adapter calls an upstream function that performs a bare `requests.get(...).json()` with no caller-controlled timeout, response classification, retry, or pacing. The project already has source policy values for these controls, so the narrow fix is to enforce them at the Sina adapter boundary and leave the existing THS/Eastmoney fallback order intact.

## Goals / Non-Goals

**Goals:**

- Make official-source outcomes observable as transport success, structured parse success, strict readiness, and fallback-required counts.
- Preserve CNInfo numeric facts when they are valid, while only routing missing canonical facts to fallback.
- Keep `equity_total` distinct from `equity_parent`; derive the latter only from same-context total equity minus minority equity when both are present and numeric.
- Make Sina requests bounded and diagnosable with configured timeout, interval, retry, and backoff values.
- Keep current scheduler names, storage schema, source priority semantics, and fallback ownership.

**Non-Goals:**

- Do not promote `每股净资产 × 股本` to an official `equity_parent` derivation.
- Do not add a new CNInfo endpoint, database table, global HTTP framework, or audit event stream.
- Do not relax readiness gates or treat total owners' equity as parent-attributable equity.
- Do not change the official-source promotion policy or rewrite existing source facts.

## Decisions

### 1. Separate acquisition and readiness counters

The batch result will retain per-instrument-period strict readiness failures, but also expose counts for successfully fetched manifests, parsed numeric facts, and targets requiring missing-fact fallback. `FinancialMaintenanceRepairRouter` will use local readiness, not the batch `status` alone, to select fallback targets. Reporting will label these dimensions separately.

Alternative considered: treating every CNInfo batch with an incomplete core row as a transport failure. Rejected because it discards useful official facts and misleads operators.

### 2. Keep equity semantics strict

The CNInfo parser continues mapping `所有者权益` to `equity_total`. A narrow post-parse derivation may create `equity_parent` only when `equity_total` and `minority_equity` are present for the same instrument, report period, source file context, and unit. Otherwise the missing parent field remains eligible for fallback. `getMainIndicators` per-share net assets are diagnostic evidence only, not a canonical derivation input.

Alternative considered: multiply `每股净资产` by `实收资本（或股本）`. Rejected because the endpoint does not contractually expose the exact share-count basis, face-value assumptions, or rounding policy needed for an auditable parent-equity value.

### 3. Add a bounded Sina transport adapter

For `sina_report`, the provider will issue the existing endpoint request through a project session with configured timeout and interval, classify status/content type/body before JSON decoding, retry transient HTTP, empty, malformed-JSON, and rate-limit responses with bounded exponential backoff, and emit a compact diagnostic after final failure. The existing fallback loop will continue to the next configured statement interface.

Alternative considered: changing AkShare globally or replacing all statement providers. Rejected because it would widen ownership and affect unrelated AkShare consumers.

### 4. Align validation vocabulary

The official validation path will accept the production canonical required-fact list and use it when determining strict readiness. Legacy aliases may remain in low-level parser tests, but maintenance reports and source routing use `net_income_parent` and `equity_parent` consistently.

## Risks / Trade-offs

- [Risk] Sina remains an upstream third-party endpoint and can still be unavailable after retries → [Mitigation] retain THS/Eastmoney fallback and report final transport diagnostics without failing the whole run when fallback completes.
- [Risk] CNInfo official coverage remains partial for parent equity → [Mitigation] preserve `equity_total`, explicitly count the missing canonical field, and route only that field to fallback.
- [Risk] Additional retries increase run time during a provider outage → [Mitigation] cap retries, backoff, and per-request timeout using existing configuration.
- [Risk] Existing tests assert the old `failed=N/N` wording → [Mitigation] update only source-routing assertions to the new acquisition/readiness fields and retain strict unresolved-blocker behavior.

## Migration Plan

1. Add focused provider, parser/readiness, router, and report tests.
2. Deploy code and configuration-compatible behavior; no schema migration is required.
3. Run one isolated CNInfo validation for a bank/non-bank sample and one bounded Sina fallback smoke test.
4. Run the next manual financial incremental task and verify that CNInfo parse counts are non-zero, fallback counts reflect only missing facts, and unresolved blockers still degrade the run.
5. Roll back by reverting the code change; existing stored source facts remain compatible.

## Open Questions

- None blocking implementation. A future source-specific evidence change could revisit a documented CNInfo parent-equity endpoint if CNInfo publishes one with explicit semantics.

## Implementation Verification

- Focused unit tests: 81 passed across the Sina provider, CNInfo parser, repair router, incremental sync, and scheduler report suites.
- CNInfo bank sample (`000001.SZ`, `2026-06-30`): 24 official numeric facts parsed; revenue, parent net income, total assets, and total liabilities were present; only `equity_parent` required fallback.
- CNInfo non-bank sample (`600519.SH`, `2026-06-30`): 19 official numeric facts parsed with the same required-fact outcome; only `equity_parent` required fallback.
- Sina bounded smoke (`300540.SZ`, `2026-06-30`): the request completed in 1.471 seconds with 286 fields and no source error. The audit command returned `needs_review` because a single-source run cannot satisfy its cross-source promotion gate, not because Sina failed.
- The live CNInfo balance sheets exposed `所有者权益` as `equity_total` but no separate minority-equity row. Therefore no live sample could derive `equity_parent`; the tested derivation remains restricted to payloads containing both total and minority equity.
