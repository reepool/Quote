## Context

The completed manufacturing/materials authority, `stage55-closure-four-20260907-a`, proves the bounded workflow can produce a research-usable slice for the four reports that informed its evidence plans, Gold, and adjudication decisions. Its later offline evaluation reached Gold 18/24 without changing the authority bundle. That result is intentionally in-sample: the current Stage 5 preparation and bundle contracts enumerate those four reports, so broader use must first show how the same semantic and acceptance rules behave on a report that did not shape them.

The authoritative owners remain unchanged. The existing company-profile preparation path owns Evidence construction, `CompanyProfileSemanticService.run_task` owns validation and disposition, the task-specific provider owns compact extract/verify adaptation, the research projection owns usage restrictions, and the isolated bundle store owns immutable output. This change adds a bounded research-validation mode around those owners; it does not create another extraction loop or production path.

## Goals / Non-Goals

**Goals:**

- Freeze one genuinely unseen manufacturing/materials annual report before any semantic output is generated.
- Exercise all six existing core chapter families with source-native Evidence and the current acceptance policy.
- Produce one immutable isolated run and a researcher-readable report that states whether the existing contract generalized, remained usable with caveats, or encountered a real blocker.
- Capture any genuinely new disclosure form or failure class with exact Evidence and a recommended next decision, without fixing it inside this change.
- Preserve the four-report authority, historical Gold results, and production non-authorization.

**Non-Goals:**

- Expanding the sample beyond one report or claiming statistical industry coverage.
- Revisiting the old four reports, changing their Gold expectations, or pursuing Gold 24/24.
- Creating a generic manifest platform, automatic annual-report discovery, new PDF/OCR/table infrastructure, confidence scoring, or a new data warehouse.
- Tuning prompts, schemas, validators, or acceptance rules in response to the out-of-sample result.
- Starting stage six, legacy backfill, approved-table writes, CommodityExposure, ValueChainRole, DCF, scheduler, API, Telegram, or production publication.

## Decisions

### 1. Freeze one independent sample before semantic execution

The change will select exactly one official, locally valid annual report within the existing manufacturing/materials boundary. Its issuer/report identity must be absent from the current four-report manifest, Gold annotations, targeted runs, and manual adjudication ledger. Selection records the business model, exchange, disclosure form, PDF hash, reason for inclusion, and known limitations before any extract or verify output exists.

A larger sample is deferred because the current decision is whether the workflow has a credible sample-out path, not whether it already supports population-scale rollout.

### 2. Reuse the same owners through an explicit validation manifest

The existing four-report mode remains the default and continues to reject extra reports. A separately identified out-of-sample validation manifest may admit only the one frozen sample and must call the same preparation, semantic, provider, projection, and bundle owners. A thin operator entry may select this manifest; it must not duplicate sample iteration, semantic validation, persistence, or benchmark logic.

The alternative of weakening `APPROVED_STAGE5_SAMPLES` globally is rejected because it would erase the completed slice boundary. A second service or script-owned loop is rejected because it would create a parallel semantic path.

### 3. Freeze a six-chapter Evidence plan before the model run

The new report receives a versioned Evidence plan for: overview plus Activity; segment or legal empty; operating quantity/capacity or legal empty; material/energy input or legal empty; counterparty/concentration or legal empty; and business regime checked as an event or explicit `not_applicable`. The plan preserves continuous physical pages, headers, units, footnotes, continuation links, PDF hash, and request scopes using existing PDF output. Missing or unreadable required context is recorded before provider invocation rather than guessed or silently omitted.

This is sample-specific preparation, not an automatic selector or a new parser capability.

### 4. Permit one operator-level complete run and stop

After manifest, Evidence, and focused offline contract checks pass, the operator may execute exactly one complete report run under a new run ID. Existing bounded gateway attempts and failover remain available inside that run, but the change does not permit targeted repair runs, result splicing, or a second complete run. A transport or execution failure is retained as an out-of-sample execution finding rather than used to justify timeout, schema, or provider changes in the same change.

### 5. Evaluate generalization without post-hoc Gold

The report status is calculated by the current six-chapter, frozen-blocker, verification, coverage, negative-guard, and subject-usage policy. No Gold set is authored after seeing runtime output. Before execution, the review rule is frozen to inspect every blocker, caveat, and proposed adjudication plus the first accepted or legal-empty result in stable request order for each core chapter. The handoff includes chapter coverage, accepted facts, legal empty results, caveats, blockers, usage restrictions, and source citations. Every item needing human adjudication includes the original source text, physical page, Evidence identity, runtime target, and a concrete recommended decision.

The final audit states whether this one sample supports reuse of the existing contract. A `hold` or `failed` result still completes the validation honestly; it does not automatically authorize rule changes.

### 6. Keep output research-only

All accepted records remain `accepted_for_review`, all projections remain isolated research fixtures, and every manifest and report retains `production_authorization=not_authorized`. No result from this validation is visible to production consumers.

## Risks / Trade-offs

- **[One sample cannot establish industry-wide accuracy]** → Describe the conclusion as one bounded out-of-sample result and use any later sample expansion in a separate change.
- **[A validation mode could weaken the frozen four-report boundary]** → Use an explicit manifest kind, reject mixed or multi-sample requests, and retain regression tests for the original mode.
- **[Manual Evidence preparation can introduce selection bias]** → Freeze the sample, full six-chapter checklist, page plan, and known gaps before semantic execution.
- **[A transient provider failure can make the result inconclusive]** → Preserve the typed execution outcome and stop; do not tune infrastructure or launch an uncontracted rerun.
- **[Review without new Gold can miss subtle errors]** → Require complete traceability plus chapter-level review material and explicit human-review items, while forbidding post-output Gold creation in this change.
