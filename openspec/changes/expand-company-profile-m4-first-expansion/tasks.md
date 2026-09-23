## 1. Scope review gate

- [x] 1.1 Independent review accepts this first-slice scope and does not enlarge it into industry packages or production
- [x] 1.2 Confirm implementation must not start until 1.1 is checked

## 2. First-expansion mode and frozen plan

- [x] 2.1 Persist `first_expansion_mode` on the existing owner as `inactive` / `active` / `completed` without adding a published action
- [x] 2.2 Persist `company_profile_first_expansion_plan.v1` before activating the mode, with plan id or hash, `knowledge_cutoff`, registry identity, strata including `service`, and per-report `asset_id`, `report_id`, `report_period`, and `document_version`
- [x] 2.3 Refuse enqueue or resume only while `active` if the snapshot is missing or the cutoff, registry identity, or report references drift
- [x] 2.4 Keep ordinary `run`/`resume` unchanged while `inactive` or `completed`; keep missing assets in the denominator and `scale_quality_claim_allowed=false`

## 3. New-sample review snapshots

- [x] 3.1 Run the published `run`/`resume` path only on the frozen report references without overwriting v1–v4 work JSON
- [x] 3.2 Limit `resume` in `active` mode to that frozen work; after delivery, later `run`/`resume` are idempotent
- [x] 3.3 Write live-run and source-review snapshots that carry the same plan and report references and do not overwrite the two-company v1 baseline files
- [ ] 3.4 Independently review official pages and recount 4.1; do not presume 7/7
- [ ] 3.5 If a reusable interpretability gap appears, record it and stop; do not add an extractor or v5 in this change

Official 2026-09-17 registry probe: 5564 names, available forms `{other: 5474, manufacturing: 1}`, `service_available_count=0`. Existing classification path cannot occupy `service`. Stopped before activate/enqueue. Did not hard-code an instrument or change the Shenwan L1 table.

A-class contract repair after `1fe3133b`: runtime now requires pointer + immutable snapshot + full plan equality; observation snapshots must match `plan.reports`; current asset `asset_id`/`report_id`/`report_period`/`document_version` freeze and compare together; work ids persist before drain; resume marks delivery; closure v2 requires a delivered, assessed, full-sample review.

Active-run accounting repair after `3ec82b97`: `_run()` persists work ids after its own enqueue and before drain; active `run` keeps `enqueue=True` and returns real inserted/reused/work_ids; undelivered or failed rounds are not idle/completed.

Two-company cap repair after `23858958`: `record_first_expansion_plan()` samples only the existing two-company budget; if that sample cannot occupy `service`, activation is refused.

Contract lock after `5cfa40c3`: design and spec require the fixed two-company budget and refuse a public plan payload enlarged to three companies.

Code-slice acceptance at `5fd681d9`: independent recheck closed the OpenSpec two-company contract and the public three-company schema counterexample. This accepts the code foundation only. 3.4 / 3.5 / 4.5 stay unchecked. The change stays open. Official registry still has no legal `service` sample; the as-of Shenwan L1 read is a separate change and must not alter this slice's two-company budget.

## 4. Operator closure v2 and completion

- [x] 4.1 Publish `company_profile_operator_closure.v2` without changing or overwriting the v1 catalog; prove historical v1 JSON still loads
- [x] 4.2 Write the replacement backlog only after the new source-review snapshot exists, using post-execution wording, mark mode `completed`, and keep the other five backlog item ids
- [x] 4.3 Prove a post-completion `run` does not rerun the frozen sample or start another expansion
- [x] 4.4 Prove production, DCF, trading, and the legacy writer remain unauthorized
- [ ] 4.5 Stop for independent Review of the applied slice; do not authorize production or the next expansion
