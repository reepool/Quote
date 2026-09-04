## 1. Baseline and four-report evidence plans

- [ ] 1.1 Record the stage-five baseline: stage-four common model/workflow specs are archived, old business-profile production remains frozen, and the only approved samples are 300750.SZ, 603659.SH, 920015.BJ, and 302132.SZ for the recorded 2025 report versions.
- [ ] 1.2 Implement a strict reader for the approved manufacturing/materials sample manifest that verifies sample ID, instrument, report period, PDF path/hash, package, and regime context and rejects any extra report.
- [ ] 1.3 Create versioned per-report evidence plans for the six frozen chapter tasks, including continuous PDF physical pages, sections, request scopes, headers, units, footnotes, and expected readable context; do not copy Gold semantic answers into the plans.
- [ ] 1.4 Build Evidence/PreparedEvidence from the approved plans through existing shared PDF or already structured table capabilities, with typed pre-provider failures for missing assets, hash mismatch, unreadable pages, lost headers/units, or incomplete continuation context.

## 2. Isolated run-bundle lifecycle

- [ ] 2.1 Define versioned stage-five run/report bundle models containing manifest identity, evidence plan and hashes, request scopes, provider call types, records, dispositions, coverage, human review items, research view, report status, benchmark, and `production_authorization=not_authorized`.
- [ ] 2.2 Implement a narrow file run-bundle store that requires an operator-supplied non-production output root, writes through a temporary path, atomically commits complete/held bundles, and never overwrites an existing run ID.
- [ ] 2.3 Implement failure cleanup that deletes uncommitted candidate/view artifacts and retains only a bounded `failed`/`non_reusable` diagnostic manifest; add an audit that reports and removes abandoned stage-five temporary paths without touching accepted/held bundles.

## 3. Common-gateway semantic provider

- [ ] 3.1 Implement a `SemanticProvider` adapter over the existing common LLM gateway using the stage-four extract/repair/verify Pydantic schemas and existing model-routing/configuration boundaries.
- [ ] 3.2 Map gateway unavailable, congestion, timeout, malformed JSON, schema failure, and request-identity mismatch into existing typed workflow outcomes without adding an unbounded retry or alternate semantic loop.
- [ ] 3.3 Add provider contract tests proving extract receives only one bounded request scope, repair is attempted at most once with writable pointers, verify is independent, and the adapter cannot publish, approve, choose packages, or write storage.

## 4. Single stage-five application owner

- [ ] 4.1 Implement `ManufacturingMaterialsProfileSliceService` as the only stage-five owner for sample iteration, request-scope execution, calls to `CompanyProfileSemanticService.run_task`, result aggregation, isolated persistence, and report/overall status.
- [ ] 4.2 Split top-five concentration/name coverage, ranking rows, contracts, related-party rows, and independently disclosed aggregate identities into separate request scopes; enforce that `not_disclosed` name coverage suppresses Relationships from the same scope while independent source scopes remain reviewable.
- [ ] 4.3 Reconstruct subject basis, Activity actor/source actor/source verb, period, unit, and anchors only from runtime Evidence; prohibit application imports or use of the stage-four Gold adapter and its default-filled fields.
- [ ] 4.4 Normalize review actions to `accept_for_research_review`, `reject`, `hold`, or `request_repair`, and ensure accepted research facts never receive production approved/reusable/publication state.
- [ ] 4.5 Aggregate accepted-for-review records into one researcher-readable view per report and calculate report/overall status without allowing average scores to override a frozen blocker.

## 5. Executable slice and regression coverage

- [ ] 5.1 Add a fake-provider single-report vertical test from evidence plan through isolated committed bundle and research view, including source-backed overview, segment measurements, operating quantity, legal empty, and Evidence links.
- [ ] 5.2 Add cross-report preparation and contract tests for all four samples, covering SSE/SZSE/BSE page coordinates, consolidation adjustment, processing direction, capacity kind, absent quantities, anonymous/aggregate counterparties, restructuring, and same-control comparison basis.
- [ ] 5.3 Execute the approved Gold and 19 negative cases only as post-run benchmark assertions; prove Gold values and `_adapt_observed_gold` defaults cannot enter request preparation or runtime candidates.
- [ ] 5.4 Add side-effect tests proving imports, fake runs, failed writes, and cleanup do not access old profile databases, approved tables, replay/publication paths, scheduler, Telegram, API, DCF, or freeze switches.

## 6. Controlled four-report execution

- [ ] 6.1 Add a thin local operator command requiring explicit sample manifest, isolated output root, provider route, budget, and run ID; support preparation-only and bounded semantic-run modes without adding scheduler/API/Telegram entry points.
- [ ] 6.2 Run preparation-only for all four reports and verify asset hashes, request scopes, evidence continuity, no Gold-derived semantic defaults, no provider calls, and no abandoned temporary artifacts.
- [ ] 6.3 After offline tests pass, run the bounded real-provider slice for all four reports into a new isolated run root; preserve every blocker, hold, failure manifest, provider call type, and research view without rerunning the old backfill.
- [ ] 6.4 Produce a Chinese review package comparing each report and chapter task with the frozen benchmark; obtain user decisions for unresolved facts and record research-only acceptance or hold without changing production authorization.

## 7. Acceptance, cleanup, and handoff

- [ ] 7.1 Re-run held scopes only with new run IDs after approved evidence/semantic corrections, then perform a final stage-five garbage audit: remove abandoned temporary/uncommitted outputs and list every retained pass/hold/failed diagnostic bundle.
- [ ] 7.2 Run stage-five unit/integration tests, stage-four shared regressions, projection comparison, Ruff, and `openspec validate slice-manufacturing-materials-company-profile --strict`; record exact commands and results.
- [ ] 7.3 Review only blockers affecting the four-report contract, isolation, result correctness, or proof of acceptance; record non-blocking parser/platform/generalization ideas without implementing them.
- [ ] 7.4 Verify all four reports have immutable audit bundles and an explicit pass/hold decision, old production remains frozen, `production_authorization=not_authorized`, and no stage-six reset or stage-eight backfill was started.
