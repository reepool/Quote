## Why

The completed manufacturing/materials research slice is usable, but its benchmark still exposes a small set of period and event-label discrepancies that matter for scalable normalization: duration facts can be represented with a point-in-time date, explicit cross-page completion timing can be lost, and Gold metadata can reject a source-correct subject or event without a documented contract decision. These issues should be resolved offline without reopening the four-report extraction loop or weakening source-native evidence rules.

## What Changes

- Distinguish duration periods from point-in-time dates so annual revenue, cost, margin, production, sales, capacity, and similar duration facts retain an annual period such as `2025`, while balance or inventory-at-date facts may retain `2025-12-31`.
- Preserve an expected completion or commissioning year as a source-native qualifier only when the supplied Evidence explicitly states it, including when the statement is on an approved continuation page; the qualifier does not replace the fact's reported period.
- Adjudicate the remaining manufacturing/materials Gold subject-strictness and event-type discrepancies against the original Evidence and frozen semantic contract. Runtime facts and Gold expectations are not rewritten merely to increase the score.
- Keep event matching closed and auditable: distinct event types or physical anchors remain non-matches unless a specific contract decision establishes an allowed equivalence.
- Validate the change with local fixtures, focused unit tests, and strict OpenSpec validation only. Do not launch another complete or targeted four-report LLM run.
- Preserve `research_slice_usable`, the disclosed Gold 14/24 baseline, `production_authorization=not_authorized`, and the closure of stage six and all legacy publication paths.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-common-semantic-model`: Define closed duration versus point-in-time period semantics and Evidence-bound expected-completion qualifiers.
- `company-profile-research-acceptance-policy`: Require explicit, auditable Gold metadata and event-type decisions without fuzzy equivalence or runtime-to-Gold backfilling.
- `manufacturing-materials-profile-semantic-adjudication`: Resolve the remaining sample-specific period, subject-strictness, qualifier, and event-label discrepancies from immutable Evidence without reopening live extraction.

## Impact

- Expected implementation surface is limited to `research/company_profile/` period/qualifier adaptation and benchmark logic, the approved manufacturing/materials Gold metadata or adjudication ledger, and directly related unit fixtures/tests.
- No model family, LLM schema, provider routing, evidence plan, PDF processing, database, scheduler, API, Telegram, legacy backfill, approved table, CommodityExposure, ValueChainRole, or DCF path is added or enabled.
- Historical run bundles, including `stage55-closure-four-20260907-a`, remain immutable and are not rescored in place; any offline evaluation artifact uses a new identity and clearly cites that bundle as input.
