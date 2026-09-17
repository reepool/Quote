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

## Review follow-up — 2026-09-14

The review findings are accepted: core dimensions lacked an explicit mapping to the six ChapterTask values; Gold default-group conflict precedence was ambiguous; M3 needed a concrete derived-object contract; 29 completed legacy business-profile plans remained active. These are clarified in the existing change, with all 23 implementation tasks still unchecked. No runtime schema, matcher, production configuration or source bundle is changed in this follow-up.

The 2026-09-13 directory count is 19 profile archives, not 16 archives total: 16 were retired by the replan; the other three already existed from earlier work: fix-company-profile-common-mvp-blockers, validate-company-profile-common-fixes-on-fresh-cohort, validate-company-profile-current-mvp-shadow-batch. The original list of 16 is retained as the transaction scope, not a claim about every archive on that date.

### Remaining completed legacy plans retired

- `add-continuous-business-profile-backfill` → `openspec/changes/archive/2026-09-14-add-continuous-business-profile-backfill` (completed historical tasks; old deltas not reapplied).
- `automate-business-profile-semantic-production` → `openspec/changes/archive/2026-09-14-automate-business-profile-semantic-production` (completed historical tasks; old deltas not reapplied).
- `bind-business-profile-semantic-reuse-to-source-sections` → `openspec/changes/archive/2026-09-14-bind-business-profile-semantic-reuse-to-source-sections` (completed historical tasks; old deltas not reapplied).
- `complete-business-profile-production-automation` → `openspec/changes/archive/2026-09-14-complete-business-profile-production-automation` (completed historical tasks; old deltas not reapplied).
- `complete-business-profile-publication-and-reporting` → `openspec/changes/archive/2026-09-14-complete-business-profile-publication-and-reporting` (completed historical tasks; old deltas not reapplied).
- `complete-business-profile-structured-semantic-fallback` → `openspec/changes/archive/2026-09-14-complete-business-profile-structured-semantic-fallback` (completed historical tasks; old deltas not reapplied).
- `configure-business-profile-production-rollout` → `openspec/changes/archive/2026-09-14-configure-business-profile-production-rollout` (completed historical tasks; old deltas not reapplied).
- `establish-a-share-business-profile-governance` → `openspec/changes/archive/2026-09-14-establish-a-share-business-profile-governance` (completed historical tasks; old deltas not reapplied).
- `expose-business-profile-measurement-contract-and-replay-legacy-profile` → `openspec/changes/archive/2026-09-14-expose-business-profile-measurement-contract-and-replay-legacy-profile` (completed historical tasks; old deltas not reapplied).
- `fix-business-profile-backfill-recovery-observability` → `openspec/changes/archive/2026-09-14-fix-business-profile-backfill-recovery-observability` (completed historical tasks; old deltas not reapplied).
- `harden-business-profile-backfill-production-readiness` → `openspec/changes/archive/2026-09-14-harden-business-profile-backfill-production-readiness` (completed historical tasks; old deltas not reapplied).
- `harden-business-profile-end-to-end-integrity` → `openspec/changes/archive/2026-09-14-harden-business-profile-end-to-end-integrity` (completed historical tasks; old deltas not reapplied).
- `harden-business-profile-fact-identity-and-publication` → `openspec/changes/archive/2026-09-14-harden-business-profile-fact-identity-and-publication` (completed historical tasks; old deltas not reapplied).
- `harden-business-profile-llm-acceptance` → `openspec/changes/archive/2026-09-14-harden-business-profile-llm-acceptance` (completed historical tasks; old deltas not reapplied).
- `optimize-business-profile-annual-report-discovery` → `openspec/changes/archive/2026-09-14-optimize-business-profile-annual-report-discovery` (completed historical tasks; old deltas not reapplied).
- `optimize-business-profile-discovery-pagination` → `openspec/changes/archive/2026-09-14-optimize-business-profile-discovery-pagination` (completed historical tasks; old deltas not reapplied).
- `redesign-business-profile-llm-scheduling-and-verification` → `openspec/changes/archive/2026-09-14-redesign-business-profile-llm-scheduling-and-verification` (completed historical tasks; old deltas not reapplied).
- `repair-business-profile-automatic-publication-gaps` → `openspec/changes/archive/2026-09-14-repair-business-profile-automatic-publication-gaps` (completed historical tasks; old deltas not reapplied).
- `repair-business-profile-chapter-aware-extraction` → `openspec/changes/archive/2026-09-14-repair-business-profile-chapter-aware-extraction` (completed historical tasks; old deltas not reapplied).
- `repair-business-profile-frontier-bound-acquisition` → `openspec/changes/archive/2026-09-14-repair-business-profile-frontier-bound-acquisition` (completed historical tasks; old deltas not reapplied).
- `repair-business-profile-publication-gap-closure` → `openspec/changes/archive/2026-09-14-repair-business-profile-publication-gap-closure` (completed historical tasks; old deltas not reapplied).
- `repair-business-profile-recovery-and-stage-health` → `openspec/changes/archive/2026-09-14-repair-business-profile-recovery-and-stage-health` (completed historical tasks; old deltas not reapplied).
- `repair-business-profile-terminal-replay-identity` → `openspec/changes/archive/2026-09-14-repair-business-profile-terminal-replay-identity` (completed historical tasks; old deltas not reapplied).
- `replace-business-profile-llm-offsets-with-evidence-spans` → `openspec/changes/archive/2026-09-14-replace-business-profile-llm-offsets-with-evidence-spans` (completed historical tasks; old deltas not reapplied).
- `separate-business-profile-readiness-from-market-linking` → `openspec/changes/archive/2026-09-14-separate-business-profile-readiness-from-market-linking` (completed historical tasks; old deltas not reapplied).
- `separate-business-profile-semantic-synthesis-from-evidence` → `openspec/changes/archive/2026-09-14-separate-business-profile-semantic-synthesis-from-evidence` (completed historical tasks; old deltas not reapplied).
- `simplify-business-profile-async-production` → `openspec/changes/archive/2026-09-14-simplify-business-profile-async-production` (completed historical tasks; old deltas not reapplied).
- `stabilize-business-profile-unit-replay-and-pdf-benchmark` → `openspec/changes/archive/2026-09-14-stabilize-business-profile-unit-replay-and-pdf-benchmark` (completed historical tasks; old deltas not reapplied).
- `unify-business-profile-annual-report-semantic-bundle` → `openspec/changes/archive/2026-09-14-unify-business-profile-annual-report-semantic-bundle` (completed historical tasks; old deltas not reapplied).

### Implementation boundary

- M1 1.2: implement company_profile_common_core_mapping.v1 using existing overview/segment task and field IDs; new projected assessments are explicitly registered, not extra LLM fields.
- M1 1.4: implement fact-error-first / valid-default-policy-conflict-second Gold evaluation; current _subject_match_status still falls through to failed for different group/unclear subjects.
- M3 3.1: register company_profile_commodity_exposure.v1 derived/read schema before writer integration; current ObjectType has no commodity extraction object and ResearchBoundary remains a placeholder.
- No claim that old 742-record output or Gold scores prove these future behaviors. No legacy command is re-enabled by the retirement.

### Follow-up verification

- Full OpenSpec strict validation: 137 passed, 0 failed (166 minus 29 retired active plans).
- All 25 requirement blocks in the eight change specs match their main-spec counterparts; all 23 implementation tasks remain unchecked.
- Source inspection confirms exactly six ChapterTask values, no CommodityExposure extraction object, and the matcher implementation gap described above. These are implementation-status checks, not proof of new runtime behavior.
- No research/scripts/tests or production configuration were changed by this follow-up; existing unrelated config/LLM/PDF edits are excluded.
- No direct references to the 29 former active paths were found in research, scripts, tests, config or documentation. All retirement record links resolve. Historical artifacts are preserved and no old delta was reapplied.
- Document-only changes were validated with OpenSpec, requirement equality and reference checks; no new LLM run or duplicate semantic benchmark was performed.

## Content-based retirement follow-up — 2026-09-14

The further review identified legacy profile plans whose directory names did not contain business-profile. Accepted and resolved:

- add-contract-row-identity-and-adaptive-section-budgets → openspec/changes/archive/2026-09-14-add-contract-row-identity-and-adaptive-section-budgets (completed historical work, old deltas not synchronized).
- separate-business-activities-from-operating-facts → openspec/changes/archive/2026-09-14-separate-business-activities-from-operating-facts (completed historical work; Activity numeric compatibility is not part of the new contract).
- Removed harden-a-share-dcf-input-governance/specs/company-business-profile-governance/spec.md from the active DCF delta tree. Its industry-default company exposure and scoring rules are superseded; proposal/design/tasks now point to SUPERSEDED-PROFILE-SCOPE.md. The DCF financial-input delta is unchanged. The removed file was ignored/untracked locally, so it cannot be recovered from Git 9995519; its retired policy is summarized in the replacement notice.
- business-profile-selective-pdf-recovery main spec is explicitly historical implementation reference. New profile task semantics use the current delivery/workflow contracts; shared PDF APIs retain their own authority.

The previous 29-plan count is the prior retirement transaction, not all legacy content in the repository. This follow-up adds two archives (31 relevant Sep14 retirements in total). No claim is made that unrelated completed DCF or infrastructure plans are retired.

The three named missing features (core projection, Gold precedence, commodity schema) are specific contract implementation gaps, not the full implementation backlog. All 23 tasks, including discovery, persistence/resume, query, source review and publication, remain unchecked. This documentation cleanup does not implement M1 or authorize a new provider run.
