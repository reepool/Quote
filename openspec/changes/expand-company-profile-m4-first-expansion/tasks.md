## 1. Scope review gate

- [ ] 1.1 Independent review accepts this first-slice scope and does not enlarge it into industry packages or production
- [ ] 1.2 Confirm implementation must not start until 1.1 is checked

## 2. Immutable a-priori expansion plan

- [ ] 2.1 Persist `company_profile_first_expansion_plan.v1` with plan id or content hash, selected instrument IDs, and strata before enqueue or drain
- [ ] 2.2 Prove the frozen strata include `service` and that `max_companies_this_round` equals the preselected reports
- [ ] 2.3 Refuse enqueue when the snapshot is missing or lacks `service`; keep missing assets in the denominator and `scale_quality_claim_allowed=false`

## 3. New-sample review snapshots

- [ ] 3.1 Run the published common-core path only on the frozen sample without overwriting v1–v4 work JSON
- [ ] 3.2 Write live-run and source-review snapshots that reference the same plan id or hash and do not overwrite the two-company v1 baseline files
- [ ] 3.3 Independently review official pages and recount 4.1; do not presume 7/7
- [ ] 3.4 If a reusable interpretability gap appears, record it and stop; do not add an extractor or v5 in this change

## 4. Operator closure v2 and boundaries

- [ ] 4.1 Publish `company_profile_operator_closure.v2` without changing or overwriting the v1 catalog; prove historical v1 JSON still loads
- [ ] 4.2 Write the replacement backlog only after the new source-review snapshot exists, using post-execution wording, and keep the other five backlog item ids
- [ ] 4.3 Prove production, DCF, trading, and the legacy writer remain unauthorized
- [ ] 4.4 Stop for independent Review of the applied slice; do not authorize production or the next expansion
