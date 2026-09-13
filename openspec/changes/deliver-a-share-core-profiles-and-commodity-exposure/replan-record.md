# Replan and retirement record — 2026-09-13

Only planning/specification documents and test fixture paths changed. No runtime code, production configuration, provider request, database or original source bundle was changed.

## Retired active changes

- `define-company-profile-product-and-industry-contracts`: 0 unchecked historical tasks; retained under `openspec/changes/archive/2026-09-13-define-company-profile-product-and-industry-contracts`. Superseded does not mean passed.
- `define-company-profile-industry-research-method`: 0 unchecked historical tasks; retained under `openspec/changes/archive/2026-09-13-define-company-profile-industry-research-method`. Superseded does not mean passed.
- `repair-company-profile-segment-evidence-context-and-output-budget`: 0 unchecked historical tasks; retained under `openspec/changes/archive/2026-09-13-repair-company-profile-segment-evidence-context-and-output-budget`. Superseded does not mean passed.
- `validate-company-profile-segment-partition-normalization-live`: 0 unchecked historical tasks; retained under `openspec/changes/archive/2026-09-13-validate-company-profile-segment-partition-normalization-live`. Superseded does not mean passed.
- `retry-company-profile-shadow-segment-replay-after-operator-timeout`: 0 unchecked historical tasks; retained under `openspec/changes/archive/2026-09-13-retry-company-profile-shadow-segment-replay-after-operator-timeout`. Superseded does not mean passed.
- `replay-company-profile-shadow-segment-financial-repair`: 5 unchecked historical tasks; retained under `openspec/changes/archive/2026-09-13-replay-company-profile-shadow-segment-financial-repair`. Superseded does not mean passed.
- `validate-company-profile-shadow-routing-continuation-replay`: 0 unchecked historical tasks; retained under `openspec/changes/archive/2026-09-13-validate-company-profile-shadow-routing-continuation-replay`. Superseded does not mean passed.
- `repair-company-profile-shadow-evidence-routing-and-continuation`: 0 unchecked historical tasks; retained under `openspec/changes/archive/2026-09-13-repair-company-profile-shadow-evidence-routing-and-continuation`. Superseded does not mean passed.
- `validate-company-profile-shadow-owner-closure-replay`: 0 unchecked historical tasks; retained under `openspec/changes/archive/2026-09-13-validate-company-profile-shadow-owner-closure-replay`. Superseded does not mean passed.
- `repair-company-profile-scale-blockers-and-validate-expanded-cohort`: 4 unchecked historical tasks; retained under `openspec/changes/archive/2026-09-13-repair-company-profile-scale-blockers-and-validate-expanded-cohort`. Superseded does not mean passed.
- `repair-business-profile-replay-cleanup-and-annual-versioning`: 10 unchecked historical tasks; retained under `openspec/changes/archive/2026-09-13-repair-business-profile-replay-cleanup-and-annual-versioning`. Superseded does not mean passed.

## Removed invalid or duplicate material

Additional frozen legacy plans retired:

- `integrate-llm-business-profile-supply-chain`: 61 unchecked legacy tasks; archive `openspec/changes/archive/2026-09-13-integrate-llm-business-profile-supply-chain`; superseded, not passed.
- `build-a-share-business-profile-evidence-pipeline`: 4 unchecked legacy tasks; archive `openspec/changes/archive/2026-09-13-build-a-share-business-profile-evidence-pipeline`; superseded, not passed.
- `correct-business-profile-role-and-replay-integrity`: 1 unchecked legacy tasks; archive `openspec/changes/archive/2026-09-13-correct-business-profile-role-and-replay-integrity`; superseded, not passed.
- `repair-business-profile-publication-boundaries`: 0 unchecked legacy tasks; archive `openspec/changes/archive/2026-09-13-repair-business-profile-publication-boundaries`; superseded, not passed.
- `repair-business-profile-fact-integrity`: 0 unchecked legacy tasks; archive `openspec/changes/archive/2026-09-13-repair-business-profile-fact-integrity`; superseded, not passed.

- Deleted expanded-cohort-source-review-outcomes.v1.json, expanded-cohort-readiness-reviewed.v1.json and expanded-cohort-empirical-audit.v1.json: semantic precision/recurrence claims were withdrawn. Prior bytes remain recoverable from Git a3dc726/2813b9f. Original batch and AUDIT-CORRECTION.md remain.
- Deleted docs/development/company_profile_usable_mvp.md: manufacturing-only/50-company roadmap replaced by the all-A-share master requirement; export instructions retained in section 28.
- Dated Stage 5 summaries leave the current-product index. Historical Evidence and Gold retain original semantics.

## Current product authority

Master requirements → company-profile-a-share-delivery and shared semantic/acceptance specs → deliver-a-share-core-profiles-and-commodity-exposure/tasks.md. Planning complete does not mean implementation complete. All new implementation tasks remain unchecked. Pre-freeze business-profile changes describe legacy implementations and cannot resume old production. No old schema delta was replayed into current specs.

## Verification and review

- OpenSpec full strict validation: 166 passed, 0 failed; current change individually valid.
- Current implementation plan: 23 unchecked tasks, no implementation completion claimed.
- Related regression set: 123 passed initially; four archive-location failures were corrected in test fixture lookup only, then 1 + 3 targeted tests passed. Total 127 cases verified across these runs, not one newly repeated full-suite run.
- The last three tests waited on mounted PDF storage; a bounded repeat completed in 95.09 seconds. No LLM run or storage infrastructure change was introduced.
- Ten historical proof artifacts retain their expected content hashes after path relocation. Test proof paths are rebased in memory; archived proof bytes are unchanged.
- Ruff on the three affected test files and git diff whitespace checks passed.
- Review blocking findings: stale archived-file lookup in tests, corrected without changing expected source hashes. Existing LLM configuration/docs, PDF evaluation and unrelated untracked files remain outside this task.
