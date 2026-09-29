## 1. Scope review gate

- [x] 1.1 Independent review accepts this reconciliation: `replay/20260928` stays recall 26/36, accuracy 26/27, critical numeric errors 1, and gate false, with tasks 3.2 and 3.3 unchecked; archived Putailai `.2` stays recall 36/38, accuracy 36/36, critical numeric errors 0, and gate false, with 23 target facts correctly delivered and not written back into `.1`; archived CATL `.3` stays recall 38/38, accuracy 38/38, critical numeric errors 0, and a gate limited to the four frozen 2025 reports, plan `manufacturing_materials_stage4_operating_quantities.2026-09-28.3`, and `extract_operating_quantities`. The three observations stay independent. Do not check 1.1 until that review. Do not change Python, enqueue, replay, identity, publication, closure, mode, checkpoint, or any existing artifact, and do not start the six-chapter package, counterparties, scale quality, or production

## 2. Ledger update and archive, not yet authorized

- [ ] 2.1 After 1.1, record the failed 26/36 result in the original operating-quantity change design/tasks without checking tasks 3.2 or 3.3. Archive that change with `git mv` into a dated directory. Stop if the target already exists. Confirm the active directory is gone and the archive path remains traceable
- [ ] 2.2 Compare the four `.1` artifact hashes before and after the archive. Stop if any byte changes. Do not create a replay, and do not edit the archived `.2` or `.3` artifacts

## 3. Reconciliation close-out, not yet authorized

- [ ] 3.1 After 2.1 and 2.2, archive this reconciliation itself into a dated directory. Do not create a successor replay for the failed 26/36 observation
