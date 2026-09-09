## Context

The completed four-report authority and the BaoSteel and Zhongnan Steel out-of-sample runs show that the bounded company-profile semantic contract can produce traceable research facts. They do not yet prove an operational path for a larger unseen cohort: sample admission is still tied to historical constants, Evidence page plans are hand-authored, and current run bundles are sized and interpreted as Stage 5 validation slices rather than a scale-oriented shadow batch.

The existing owners remain authoritative. Shared annual-report assets and PDF artifacts own document integrity; `BusinessProfileSectionSelector` owns governed section discovery; `ManufacturingMaterialsProfileSliceService` and `CompanyProfileSemanticService` own request preparation, semantic validation, verification, disposition, projection, and report status; the common LLM gateway owns transport behavior. This change connects those owners for exactly twenty unseen reports and adds only the manifest, automatic planning, batch coordination, and audit outputs needed to test scale readiness.

This is the first vertical slice toward high-quality production. It must establish empirical quality and review workload before any Stage 6 reset or production writer is designed.

## Goals / Non-Goals

**Goals:**

- Freeze exactly twenty previously unseen, locally valid manufacturing/materials annual reports with exchange and disclosure-shape diversity.
- Generate a six-core-chapter Evidence plan from existing immutable PDF artifacts and governed section rules without hand-authored company page maps.
- Execute each report through the existing bounded semantic owner with isolated identities, immutable outputs, typed failures, and one primary model policy.
- Measure report completion, Evidence traceability, critical semantic errors, human-review workload, latency, token use, and sampled source-level precision.
- Produce a review package containing the source text required to approve every blocker, caveat, or unresolved item.
- Keep all data research-only and use the batch result only to decide whether a later production-promotion design change is justified.

**Non-Goals:**

- Stage 6 legacy reset, approved-table writes, scheduler/backfill enablement, CommodityExposure, ValueChainRole, DCF, or production publication.
- A new annual-report downloader, PDF/OCR engine, LLM client, schema registry, confidence platform, data warehouse, or generic workflow framework.
- Changing the semantic object model, Evidence standard, frozen verifier blockers, Gold expectations, or historical four-report/OOS bundles.
- Running every report through three models, merging outputs from different models or runs, or repeatedly tuning failed reports until metrics pass.
- Claiming manufacturing/materials industry-wide readiness from twenty reports alone.

## Decisions

### 1. Freeze one manifest-bound cohort before Evidence generation

A shadow manifest will contain exactly twenty unique 2025 annual reports whose instrument/report identities are absent from the four-report authority, BaoSteel, Zhongnan Steel, Gold fixtures, targeted runs, adjudication ledgers, and earlier OOS manifests. Each row records the instrument, company, exchange, report period, local PDF path and SHA-256, source document identity, page count, business shape, disclosure shape, selection reason, and known limitations. The cohort will include SSE, SZSE, and BSE reports and will be committed before provider-bearing execution.

The selection reads existing local-valid annual-report assets and first-wave industry metadata. It does not download or replace documents. A deterministic selection receipt records the eligible population and exclusions so that a later runtime result cannot influence cohort membership.

### 2. Add an explicit shadow admission mode without weakening historical modes

`Stage5SampleManifest` and Evidence-plan loading will accept a new shadow schema/kind only when all twenty report identities, local paths, file hashes, report periods, and the manifest hash satisfy the frozen shadow contract. Historical four-report and OOS kinds continue to use their existing closed constants and limits.

Report bundle admission will be validated against the active manifest supplied to the application service rather than by globally adding the twenty instruments to `KNOWN_STAGE5_SAMPLES`. The Stage 5 report model remains the semantic report payload; a new batch result references per-report immutable results and their hashes. This avoids changing the meaning of `Stage5RunBundle`, its four-report success rule, or old bundles.

Alternative rejected: extending `KNOWN_STAGE5_SAMPLES` or changing `Stage5RunBundle.reports` to twenty. That would turn a completed validation contract into a rolling universe and could reinterpret historical success states.

### 3. Build Evidence plans from existing PDF artifacts and governed selectors

The automatic planner consumes the frozen manifest, existing content-addressed PDF page artifact, resolved manufacturing/materials disclosure templates, and `BusinessProfileSectionSelector`. It maps governed section families to the six existing chapter tasks:

- overview and Activity;
- segment facts or legal empty;
- operating quantities/capacity or legal empty;
- material and energy inputs or legal empty;
- counterparties/concentration or legal empty;
- business-regime event or explicit `not_applicable`.

Selected physical pages are split into continuous report-local request scopes and remain within the existing bounded page/request limits. The generated plan records selector reasons, section titles, source anchor terms, matched headers, units or footnotes when present, continuation pages, page hashes, plan version, manifest identity, and PDF hash. It never stores Gold values, expected semantic answers, subject decisions, or inferred facts.

If a required chapter cannot obtain governed, readable Evidence, planning emits a typed report/chapter failure before any provider call. It does not add guessed pages after seeing model output. All twenty plans and a provider-free preparation audit must be frozen before the semantic batch can start.

### 4. Reuse the existing semantic owner one report at a time

A `ManufacturingMaterialsShadowBatchService` will coordinate the cohort but delegate each report and scope to the existing `ManufacturingMaterialsProfileSliceService` / `CompanyProfileSemanticService` path. The operator remains a thin argument adapter. It must not duplicate extraction, verification, disposition, projection, or benchmark rules.

Each report receives a distinct immutable run identity beneath one batch identity. No request contains pages from another report, and no batch-wide LLM prompt is allowed. A failure in one report is recorded and does not erase already committed reports or cause their facts to be merged into a replacement run.

### 5. Use one primary model policy and bounded failure behavior

The shadow batch uses the previously measured Gemini logical profile as the single primary route because it completed the second OOS extract comparison and formal run with materially lower latency. The decision is local to this cohort, not a global model ranking. Existing gateway attempt/deadline handling remains unchanged.

A schema or transport failure may use only the existing bounded request behavior. Exhaustion produces a typed failed scope/report. The batch does not start a different-model replacement run or splice records across models. Any future mirrored model audit must be a separate, non-authoritative artifact and is outside this change.

### 6. Separate validation completion from scale-readiness gates

The one contracted batch completes whether the outcome is ready or hold. Scale readiness is a post-run audit and requires all of the following:

- exactly twenty frozen reports and provider-free plans;
- at least 95% report execution completion;
- at least 90% reports in `usable` or `usable_with_caveats`;
- 100% of accepted records traceable to immutable physical-page Evidence;
- zero source value/unit/header mutation, silent required-chapter omission, unsupported `consolidated_group` promotion, Evidence misbinding, or other frozen hold-level semantic error in the reviewed material;
- sampled accepted-record precision of at least 99%;
- median unresolved human-review workload no greater than two items per report and p90 no greater than five;
- every execution/preparation failure represented by an existing or explicitly batch-scoped typed code.

The review sample is frozen before execution: every blocker, caveat, unresolved item, and proposed adjudication, plus the first accepted record or legal-empty result in stable scope order for each core chapter of every report. Each review row includes source quote, physical page, Evidence identity, runtime target, disposition, usage restriction, and recommended decision. No post-output Gold set is authored.

If any gate fails, the batch readiness decision is `hold`; that honest outcome still completes the change and must not trigger targeted reruns or relaxed semantics inside this change.

### 7. Preserve research-only boundaries

Every result remains `accepted_for_review` and every projection remains a research fixture. The manifest, preparation output, per-report results, batch metrics, and review package retain `production_authorization=not_authorized`. The batch does not write production repositories or become visible to approved-data consumers.

A later change may design controlled promotion only if this batch meets its readiness gates and the promotion repository, rollback, and dry-run contracts are separately approved.

## Risks / Trade-offs

- **[Automatic selection misses an unusual disclosure]** → Record selector coverage and typed missing-Evidence failures; do not add post-output pages in this batch.
- **[Twenty reports are costly or slow]** → Keep requests report-local and scope-bounded, use the measured primary profile, record token/latency metrics, and stop after the one frozen batch.
- **[Runtime manifest admission weakens Stage 5 history]** → Restrict it to the new schema/kind and validate active-manifest identity at the application boundary; leave historical constants and bundle semantics unchanged.
- **[A single model creates model-specific blind spots]** → Treat conclusions as cohort/model specific and preserve full review material; do not pay for or merge a three-model run in this slice.
- **[A few provider failures distort semantic metrics]** → Report execution completion separately from reviewed precision and retain typed failures; do not classify missing output as a semantic success.
- **[Review gates become a new platform]** → Implement only deterministic batch aggregation and a flat review package over existing fields, not a generalized scoring or approval service.
