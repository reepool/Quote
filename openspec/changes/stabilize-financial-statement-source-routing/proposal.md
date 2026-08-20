## Why

The daily financial disclosure job currently reports every CNInfo data20 target as an official-source failure even when all three official statement endpoints were fetched and parsed successfully but one strict canonical fact, usually parent-attributable equity, remains unavailable. The AkShare Sina fallback also emits repeated opaque JSON decode warnings during transient non-JSON responses because its configured timeout, retry, and pacing policy is not enforced at the actual Sina request boundary.

## What Changes

- Distinguish CNInfo transport, structured-response, parsing, and strict canonical-readiness outcomes instead of collapsing all incomplete targets into `failed`.
- Preserve successfully parsed CNInfo facts and invoke THS/Sina fallback only for canonical facts that remain missing or semantically ambiguous.
- Recognize CNInfo `所有者权益` as total owners' equity, not parent-attributable equity; derive `equity_parent` only when total equity and minority equity are both available under the same instrument, report period, source, and statement context.
- Align official batch validation with the canonical fact names required by the production maintenance job.
- Apply bounded timeout, pacing, retry, and response diagnostics to Sina statement requests while retaining a configured fallback source after Sina failure.
- Update daily reporting to show CNInfo request/parse success separately from full-readiness and fallback-required counts.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `official-financial-source-profiles`: Clarify partial official-source success, strict equity semantics, missing-fact fallback, and bounded Sina transport behavior.
- `financial-operations-scheduler`: Report source acquisition and parsing independently from canonical readiness and fallback completion.

## Impact

- Affected application service: `research/financial_statement_maintenance_repair.py`.
- Affected providers and validation: `research/providers/akshare_financial_statements.py` and official financial validation helpers.
- Affected reporting: financial disclosure incremental result metadata and Telegram formatter.
- No database schema, public API, scheduler identity, or financial canonical key changes.
- Existing official facts and fallback facts remain in the current financial storage model; no new audit or ingestion framework is introduced.
