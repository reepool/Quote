## 1. Scope review gate

- [ ] 1.1 Independent review accepts this as-of Shenwan L1 read and does not enlarge it into a closed-table change, a hardcoded service sample, or first-expansion activation
- [ ] 1.2 Confirm implementation must not start until 1.1 is checked

## 2. Project the stored as-of L1 name

- [ ] 2.1 When an as-of history row has no top-level `sw_l1_name`, expose `classification.levels.sw_l1.industry_name` as the registry classification name
- [ ] 2.2 Keep an existing top-level `sw_l1_name` unchanged
- [ ] 2.3 Leave the disclosure form as `other` when that history row stores no Shenwan L1 name
- [ ] 2.4 Do not copy a later current membership name over an existing as-of row

## 3. Keep the closed table and first expansion untouched

- [ ] 3.1 Prove a stored service name receives `service` from the existing closed Shenwan L1 table without adding or renaming entries
- [ ] 3.2 Prove this change does not alter the two-company first-expansion budget or write a first-expansion plan, mode, observation, or closure
- [ ] 3.3 Keep production `not_authorized` and `scale_quality_claim_allowed=false`
