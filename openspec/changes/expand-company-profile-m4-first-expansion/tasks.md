## 1. Scope review gate

- [x] 1.1 Independent review accepts this first-slice scope and does not enlarge it into industry packages or production
- [x] 1.2 Confirm implementation must not start until 1.1 is checked

Checked tasks below mean the control capability is implemented and tested. They do not mean an official first-expansion plan, mode, run, snapshot, or closure has been written.

## 2. First-expansion mode and immutable plan capability

- [x] 2.1 Implement and test persisting `first_expansion_mode` on the existing owner as `inactive` / `active` / `completed` without adding a published action
- [x] 2.2 Implement and test the immutable `company_profile_first_expansion_plan.v1` model, persist path, and validation for plan id or hash, `knowledge_cutoff`, registry identity, strata including `service`, and per-report `asset_id`, `report_id`, `report_period`, and `document_version`
- [x] 2.3 Implement and test refusal of enqueue or resume only while `active` if the snapshot is missing or the cutoff, registry identity, or report references drift
- [x] 2.4 Implement and test that ordinary `run`/`resume` stay unchanged while `inactive` or `completed`, that missing assets stay in the denominator, and that `scale_quality_claim_allowed=false`

## 3. Frozen work and independent snapshot capability

- [x] 3.1 Implement and test that an active published `run`/`resume` stays on the frozen report references and does not overwrite v1–v4 work JSON
- [x] 3.2 Implement and test that `resume` in `active` mode continues only that frozen work, and that after delivery later `run`/`resume` are idempotent
- [x] 3.3 Implement and test writing independent live-run and source-review snapshots that carry the same plan and report references and do not overwrite the two-company v1 baseline files

## 4. Operator closure v2 capability

- [x] 4.1 Implement and test generating `company_profile_operator_closure.v2` without changing or overwriting the v1 catalog, and prove historical v1 JSON still loads
- [x] 4.2 Implement and test that the replacement backlog is written only after a new source-review snapshot exists, uses post-execution wording, marks mode `completed`, and keeps the other five backlog item ids
- [x] 4.3 Implement and test that a post-completion `run` does not rerun the frozen sample or start another expansion
- [x] 4.4 Implement and test that production, DCF, trading, and the legacy writer remain unauthorized

## 5. Official first expansion, not yet executed

Do not check 5.6–5.12 until that official step has actually happened. Do not enlarge the sample or change ordinary live-run priority.

- [x] 5.1 Read-only re-probe of the official registry at `knowledge_cutoff=2026-09-17` completed. No plan, mode, snapshot, or closure was written
- [x] 5.2 The current two-company rule selected two manufacturing names and does not include `service`, although legal service candidates exist later in the priority
- [x] 5.3 Recorded immutable plan `cd8031cb5a440da19d7a7b8a41af0fdf` for `600004.SH` SSE service and `600006.SH` SSE manufacturing at `knowledge_cutoff=2026-09-17`, snapshot `universe_5a919396af823bb61d02a7894b330184`. Mode remains `inactive`. Did not activate
- [x] 5.4 Activated first expansion through `CompanyProfileTaskService.activate_first_expansion_from_registry()`. Mode is `active`, `plan_id=cd8031cb5a440da19d7a7b8a41af0fdf`, `work_ids=()`, `delivered=false`. Did not run, enqueue, or write an observation
- [x] 5.5 Executed one published `run` at `knowledge_cutoff=2026-09-17`. Returned `action=run`, `state=incomplete`. Enqueued and froze `bp-work-5d1215f542d18b4d16ddd500` (`600004.SH`) and `bp-work-97e7eed49d4b1e5cf477974f` (`600006.SH`). Did not resume, source-review, recount 4.1, or write closure v2
- [x] 5.6 Confirmed the independent live-run snapshot after controlled resume. Both frozen works completed with `delivered=true` and `supplement_incomplete=false`. Did not write source-review or closure v2
- [ ] 5.7 Independently review the official pages and write the source-review snapshot
- [ ] 5.8 Recalculate 4.1 from that source review. Do not presume 7/7
- [ ] 5.9 Judge and record any reusable interpretability gap. Do not add an extractor or `owned_page_facts=v5` in this change
- [ ] 5.10 Check that publication control is the legal control required before closure
- [ ] 5.11 Write operator closure v2 and set the mode to `completed`
- [ ] 5.12 After independent Review of the official execution, archiving this change is allowed. Do not authorize production or the next expansion

The former unchecked 3.4, 3.5, and 4.5 are this section. 5.6–5.12 stay unchecked.

## 7. Reserved service seat, not yet implemented

Contract review must pass before any code change. Do not change ordinary live-run `_STRATUM_PRIORITY`, the two-company budget, or the closed Shenwan table.

- [x] 7.1 Implement and test the first-expansion-only rule that reserves one `service` seat inside the two-company budget, fills the other seat from the remaining global priority, requires two different strata, and refuses when no legal `service` candidate exists

## 6. Retained history

These observations stay as history and are not overwritten:

- Pre-fix 2026-09-17 registry probe: 5564 names, available forms `{other: 5474, manufacturing: 1}`, `service_available_count=0`. That count is the baseline before the taxonomy parent-chain read, not a post-fix conclusion. The official registry has not been re-probed since `read-as-of-shenwan-l1-for-profile-strata` archived.
- The original two-company v1 live-run and source-review baseline files remain the retained 7/7 observation.
- Existing v1–v4 work JSON remains on disk.

The immutable plan is recorded and the mode is active. One published `run` froze `bp-work-5d1215f542d18b4d16ddd500` and `bp-work-97e7eed49d4b1e5cf477974f`. A later controlled resume of those same IDs delivered both companies. The live-run snapshot records `completed`, `delivered=true`, and `supplement_incomplete=false`. There is no source-review snapshot or closure v2. `scale_quality_claim_allowed` stays false and production stays `not_authorized`.

Capability repairs already tested, not official execution: after `1fe3133b`, pointer, immutable snapshot, and full plan must match, and closure v2 requires a delivered assessed full sample; after `3ec82b97`, active `run` keeps real enqueue accounting; after `23858958`, the sample stays at two companies; after `5fd681d9`, the code foundation was accepted and 3.4 / 3.5 / 4.5 stayed unchecked.
