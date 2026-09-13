## Why

The September 12 current-MVP shadow batch exposed seven reviewed semantic defects across
all four eligible models and twelve deadline failures whose Stage 5 traces discarded the
gateway attempt lineage. These are shared contract/adapter defects, not company-specific
exceptions, and they currently prevent a fast, trustworthy scale-validation path.

## What Changes

- Tighten the existing bounded workflow so BusinessOverview keeps substantive
  source-native business descriptions while rejecting metric-only, regulatory-boilerplate,
  and unsupported promotional/risk fragments.
- Require material-input and segment legal-empty results to cite their real chapter owner,
  not a page that merely contains incidental material, revenue, or industry words.
- Preserve BusinessEvent source-native labels by removing canonical English labels that do
  not occur in the cited source while retaining the evidenced canonical event semantics.
- Make a routed request reserve bounded time for another eligible pool member, and retain
  the complete safe gateway attempt/model lineage in failed Stage 5 traces.
- **BREAKING**: remove the obsolete executable `evidence-role-semantic-replay` and
  `current-mvp-semantic-replay` one-shot compatibility modes, validators, contracts, CLI
  options, fixtures, and tests. Their JSON remains immutable historical evidence but is no
  longer runnable against current implementation hashes.
- Add provider-free regression fixtures for the seven reviewed defects and controlled
  routed-deadline failover; do not rerun the twenty-report batch or open production paths.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-bounded-semantic-workflow`: strengthen substantive overview,
  legal-empty Evidence-owner, source-native label, and failed-trace lineage requirements.
- `common-llm-gateway`: reserve bounded failover time inside one execution deadline so an
  eligible second model can be attempted after a slow first source.
- `company-profile-manufacturing-materials-shadow-batch`: retire the obsolete Evidence-role
  replay execution compatibility while preserving archived audit artifacts.

## Impact

- Affects `research/company_profile/stage5_provider.py`, Stage 5 trace models, the existing
  common LLM routed client, the shadow-batch operator/service, and focused unit tests.
- Reuses the existing four-member logical pool, schemas, Evidence plan, provider-call
  owner, and report-local execution path; no new gateway, retry framework, field, or
  company-specific rule is introduced.
- Keeps historical bundles immutable and retains
  `production_authorization=not_authorized`; Stage 6, approved writers, scheduler/backfill,
  CommodityExposure, ValueChainRole, and DCF remain closed.
