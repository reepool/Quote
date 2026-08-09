## Why

The `business-profile-20260809112342618082` production-shadow batch proved that the LLM gateway and structured JSON contract can work reliably, but it also exposed local correctness and throughput defects that make further backfill unsafe: numeric reconciliation is hard-coded as successful, free-form model units are confused with governed units, Chinese source labels are translated into unstable `*_raw` identities, and repeated full storage initialization holds the single-writer path for most of the run. Separately, the same log stream reports AkShare/Sina response parsing failures as `invalid syntax` because the upstream adapter evaluates network text and the local exception log omits actionable classification.

## What Changes

- Require every business-profile LLM prompt and semantic conclusion to use Chinese, preserve source-native labels and units without translation, and leave canonical field mapping and normalization to deterministic program code.
- Replace the ambiguous unit string contract with source value/unit lineage plus a versioned programmatic unit parser, dimension catalog, scale grammar, exact `Decimal` conversion, and replayable unresolved-unit state. Unit resolution failures will not repeat a successful LLM request.
- Perform real row-level numeric reconciliation after unit normalization, including revenue/cost/gross-margin identities and precision-aware tolerances. Remove all unconditional reconciliation-success flags.
- Preserve distinct source-native labels, Chinese semantic summaries, and program-generated canonical identifiers so model wording cannot change cross-period entity identity.
- Persist the raw validated semantic envelope before downstream conversion, allowing catalog or conversion fixes to replay without another LLM call.
- Automatically quarantine and supersede affected shadow candidates, recover the 12 unit-blocked work items and inconsistent `600403.SH` rows, and reuse downloaded reports, selected sections, evidence, and valid semantic artifacts.
- Raise structured semantic worker concurrency from 2 to an initial configurable value of 4, while retaining shared-gateway admission, adaptive provider congestion controls, per-provider limits, and automatic reduction under pressure.
- Initialize research storage once before worker startup, remove per-work-item schema initialization, keep parsing/network/LLM work outside the write gate, query only referenced evidence during bundle persistence, and expose transaction latency and writer-duty metrics.
- Replace the HKEX factor path's evaluation of Sina response text with a safe bounded parser and classify upstream response/parser errors with exception type, source, symbol, response metadata, and DEBUG traceback.

## Capabilities

### New Capabilities

- `business-profile-production-readiness`: Governs Chinese-only semantic output, source/canonical field separation, deterministic units and numeric reconciliation, replayable semantic artifacts, safe recovery, bounded concurrency, and short-transaction backfill operation.

### Modified Capabilities

- `hkex-adjustment-factors`: Replaces unsafe upstream response evaluation with safe parsing and requires actionable parser-failure classification without changing the factor fallback contract.

## Impact

- Business-profile prompts, response schemas, structured extraction validation, semantic runtime conversion, unit catalogs, processing identities, candidate lineage, recovery, rollout configuration, queue metrics, and production runbook.
- `ResearchStorage` initialization lifecycle, business-profile write coordination, bundle persistence queries, and focused performance tests.
- HKEX AkShare/Sina adjustment-factor transport/parser diagnostics and its existing unit/integration tests.
- Existing immutable annual-report PDFs and selected-section assets remain reusable. Existing approved records are not destructively rewritten; affected shadow candidates are automatically made non-publishable and regenerated under the new identities.
