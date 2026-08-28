## 1. Semantic contracts and source identity

- [ ] 1.1 Define the atomic-versus-derived layer contract in the existing business-profile schemas and add explicit internal-inventory and external-service classification fields without changing raw LLM values or units.
- [ ] 1.2 Extend operating-fact occurrence identity so source row/contract keys are derived from immutable evidence and used consistently in record ids, fact scopes, temporal keys, metadata, ambiguity groups, and publication lineage.
- [ ] 1.3 Add compatibility decoding for legacy activities and operating facts, preserving approved records when row/contract identity cannot be reconstructed reliably.

## 2. Role derivation and temporal integrity

- [ ] 2.1 Change role derivation so generic internal `stores` activities never produce `storage_provider`; require validated external-service scope and exact evidence for that role.
- [ ] 2.2 Group qualifying external-service activities by instrument, scope, role, report cohort/business regime, and rule version; produce or reuse one deterministic role while retaining sorted supporting activity/evidence lineage.
- [ ] 2.3 Align value-chain role record identity and temporal stable identity, and make derived-role publication reuse existing compatible approved/current roles without weakening temporal conflict checks.
- [ ] 2.4 Isolate one role/fact group’s evidence, identity, or temporal failure from unrelated derived roles, exposure facts, and publications; persist typed non-provider diagnostics.

## 3. Reuse, replace, and local replay

- [ ] 3.1 Propagate `result_policy` from the backfill application entry point through semantic reuse, atomic persistence, role derivation, exposure publication, promotion, and repair, with explicit policy diagnostics at each boundary.
- [ ] 3.2 Implement local upgrade/replay for legacy contract facts using persisted evidence and semantic artifacts; create row-aware successors only when occurrence identity is proven and preserve approved history otherwise.
- [ ] 3.3 Ensure reuse does not promote incompatible legacy candidates over approved records and replace creates normal governed successors without last-write-wins or evidence deletion.
- [ ] 3.4 Add origin and token accounting for `llm_extracted`, `semantic_reused`, `local_replayed`, and `program_derived`; count tokens only from actual gateway calls.

## 4. Historical audit and repair

- [ ] 4.1 Add one bounded business-profile integrity repair service with zero-write audit default, explicit instrument/apply scope, and existing repository/promotion owners as the only write path.
- [ ] 4.2 Audit inventory-derived storage roles, duplicate role identities, broad-identity contract conflicts, and incompatible reusable artifacts with stable ids, evidence, proposed action, and reason codes.
- [ ] 4.3 Apply only evidence-positive corrections transactionally per instrument/group; transition or delete unreferenced invalid machine-derived records when permitted, replay dependent publications, and hold unreconstructable cases.
- [ ] 4.4 Make repair idempotent, network/LLM-free, and explicit about `would_change`, `changed`, `unchanged`, `held`, and `failed`; prove audit performs zero writes.

## 5. Verification and rollout

- [ ] 5.1 Add focused tests for separate `成品酒`/`半成品酒` inventory facts, internal versus external storage semantics, multiple qualifying service supports, and aligned role identity/temporal behavior.
- [ ] 5.2 Add tests for the `601012.SH` two-contract pattern, zero-value preservation, legacy approved-plus-candidate reuse, occurrence reconstruction failure, and unrelated-row isolation.
- [ ] 5.3 Add end-to-end tests for `force=true result_policy=reuse`, `replace`, repeated runs, partial publication failure, origin/token reports, and deterministic non-congestion diagnostics.
- [ ] 5.4 Run audit and apply on a copied database for the affected cohort, verify API/current projections and backfill reports, then run audit again to prove idempotence before any production apply.
- [ ] 5.5 Complete a final review against the three-layer contract, production write-owner invariants, existing API compatibility, and strict OpenSpec validation; archive only after all acceptance tasks pass.
