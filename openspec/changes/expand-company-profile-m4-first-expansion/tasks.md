## 1. Scope review gate

- [ ] 1.1 Independent review accepts this first-slice scope and does not enlarge it into industry packages or production
- [ ] 1.2 Confirm implementation must not start until 1.1 is checked

## 2. First-expansion mode and frozen plan

- [ ] 2.1 Persist `first_expansion_mode` on the existing owner as `inactive` / `active` / `completed` without adding a published action
- [ ] 2.2 Persist `company_profile_first_expansion_plan.v1` before activating the mode, with plan id or hash, `knowledge_cutoff`, registry identity, strata including `service`, and per-report `asset_id`, `report_id`, `report_period`, and `document_version`
- [ ] 2.3 Refuse enqueue or resume only while `active` if the snapshot is missing or the cutoff, registry identity, or report references drift
- [ ] 2.4 Keep ordinary `run`/`resume` unchanged while `inactive` or `completed`; keep missing assets in the denominator and `scale_quality_claim_allowed=false`

## 3. New-sample review snapshots

- [ ] 3.1 Run the published `run`/`resume` path only on the frozen report references without overwriting v1–v4 work JSON
- [ ] 3.2 Limit `resume` in `active` mode to that frozen work; after delivery, later `run`/`resume` are idempotent
- [ ] 3.3 Write live-run and source-review snapshots that carry the same plan and report references and do not overwrite the two-company v1 baseline files
- [ ] 3.4 Independently review official pages and recount 4.1; do not presume 7/7
- [ ] 3.5 If a reusable interpretability gap appears, record it and stop; do not add an extractor or v5 in this change

## 4. Operator closure v2 and completion

- [ ] 4.1 Publish `company_profile_operator_closure.v2` without changing or overwriting the v1 catalog; prove historical v1 JSON still loads
- [ ] 4.2 Write the replacement backlog only after the new source-review snapshot exists, using post-execution wording, mark mode `completed`, and keep the other five backlog item ids
- [ ] 4.3 Prove a post-completion `run` does not rerun the frozen sample or start another expansion
- [ ] 4.4 Prove production, DCF, trading, and the legacy writer remain unauthorized
- [ ] 4.5 Stop for independent Review of the applied slice; do not authorize production or the next expansion
