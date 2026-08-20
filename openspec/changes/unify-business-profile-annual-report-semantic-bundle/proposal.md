## Why

Company-profile semantic production currently selects annual-report evidence and invokes the LLM separately for `atomic_activities` and `named_relationships`. The two field families usually depend on the same management-discussion chapter, so full-market backfill repeats context construction, input tokens, and model latency while still omitting several common annual-report subsections needed for accurate business, supply-chain, and commodity analysis.

## What Changes

- Extend annual-report subsection recognition for industry context, products and applications, business models, operating analysis, orders, and major customers and suppliers inside the resolved management-discussion chapter.
- Build one bounded, deduplicated semantic evidence bundle per company, report, and document version while continuing to parse the shared PDF page asset only once.
- Request atomic activities and named relationships together in one schema-valid LLM JSON response, with Chinese semantic summaries, source-native values and units, and locally resolved evidence span identifiers.
- Persist and replay the joint semantic response by exact document, evidence, prompt, and schema identity so retries and the second field-family consumer do not call the LLM again.
- Keep field-family validation, governed-record persistence, rollout promotion, value-chain derivation, commodity mapping, unit conversion, and publication independent and program-owned.
- Preserve the current `/run business_profile_backfill` interface and active rollout phase; this change optimizes semantic phases without promoting them automatically.

## Capabilities

### New Capabilities

- `business-profile-annual-report-semantic-bundle`: Chapter-aware annual-report evidence packaging, one-request semantic extraction, exact replay, and independent downstream field-family consumption.

### Modified Capabilities

None.

## Impact

- Affected modules: annual-report disclosure templates, section selection, semantic extraction contracts, semantic artifact replay, semantic runtime, and focused business-profile tests.
- The authoritative annual-report input remains `research/announcement_assets`; no discovery, download, archive, or alternate asset path is added.
- No public command, scheduler contract, database schema, canonical record type, financial time semantics, or LLM gateway interface changes.
