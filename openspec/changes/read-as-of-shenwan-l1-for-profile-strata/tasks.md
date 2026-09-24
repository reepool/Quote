## 1. Scope review gate

- [x] 1.1 Independent review accepts this as-of Shenwan L1 read and does not enlarge it into a closed-table change, a hardcoded service sample, or first-expansion activation
- [x] 1.2 Confirm implementation must not start until 1.1 is checked

## 2. Project the stored as-of L1 name

- [x] 2.1 When an as-of history row has no top-level `sw_l1_name`, resolve the L1 name from `taxonomy_system`, `taxonomy_version`, and `official_industry_code` by walking the existing `industry_taxonomy` parent chain
- [x] 2.2 Keep an existing top-level `sw_l1_name` unchanged
- [x] 2.3 Leave the disclosure form as `other` when that history row stores no Shenwan L1 name
- [x] 2.4 Do not copy a later current membership name over an existing as-of row

## 3. Keep the closed table and first expansion untouched

- [x] 3.1 Prove a stored service name receives `service` from the existing closed Shenwan L1 table without adding or renaming entries
- [x] 3.2 Prove this change does not alter the two-company first-expansion budget or write a first-expansion plan, mode, observation, or closure
- [x] 3.3 Keep production `not_authorized` and `scale_quality_claim_allowed=false`
