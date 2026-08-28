## Context

The current pipeline uses one broad activity vocabulary for both source disclosures and derived value-chain capabilities. In particular, every approved `stores` activity maps to `storage_provider`. An annual report's separate inventory disclosures for `成品酒` and `半成品酒` are therefore correctly stored as two atomic activities but incorrectly converted into two external-service roles. The role record id includes its supporting activity while the temporal identity omits that activity, so the second role write also conflicts with the first.

Contract facts have the inverse problem. A completed change added `source_row_key` for new extraction, but historical semantic artifacts and persisted candidates can still have the old broad identity. With `force=true result_policy=reuse`, the runtime may reuse those records and promote `4.18 亿元` and `0 亿元` from two separate polysilicon contracts as though they were competing versions of one fact.

These failures occur after semantic extraction. The LLM can return source-supported activities and values, but program-side layering, identity, reuse, and publication contracts mishandle them. The current business-profile runtime, governed repository, promotion service, and exposure publisher remain the authoritative write chain. This change must correct that chain without adding another model, provider, queue, database, or repair implementation.

## Goals / Non-Goals

**Goals:**

- Preserve source disclosures at their natural granularity: inventory object, table row, contract occurrence, raw number, raw unit, and evidence.
- Distinguish an internal operating fact from an externally provided value-chain service before deriving a company capability.
- Make derived role record identity, temporal identity, aggregation scope, and support lineage agree.
- Carry contract/row identity through new extraction, legacy reuse, promotion, API current projections, and local repair.
- Make `reuse` and `replace` explicit end-to-end lifecycle policies rather than semantic-artifact-only choices.
- Repair locally reconstructable historical defects without LLM/network use and without deleting evidence or valid history.
- Isolate deterministic fact/role conflicts and account accurately for LLM tokens versus local work.

**Non-Goals:**

- Improve the PDF/OCR subsystem or report-section selection.
- Add Terra, Sol, or any other stronger-model dependency; model tiering remains an LLM gateway/team responsibility.
- Ask an LLM to normalize units, calculate totals, choose one valid contract row, or override temporal governance.
- Model every inventory accounting category or create a universal product/catalog ontology.
- Change shareholder acquisition, public routes, database locations, scheduler job ids, or the `/run business_profile_backfill` command shape.
- Build a generalized migration, provenance, validation, or workflow platform.

## Decisions

### 1. Use three explicit semantic layers

The authoritative flow is:

```text
immutable evidence
    -> atomic activities / operating facts
    -> derived scoped company capabilities
    -> exposure and publication consumers
```

Atomic records describe what the report says. They retain object or occurrence identity, so `成品酒` and `半成品酒` remain separate. Derived roles summarize an evidenced company capability and are not one-for-one copies of inventory objects. Exposure consumers use governed current records only.

The rejected alternative is to merge atomic records until temporal writes succeed. That loses legitimate disclosures and hides identity defects.

### 2. Split internal storage from external storage service

The canonical meaning of the existing `stores` action becomes internal holding/storage of an object. It never derives `storage_provider` by itself. The semantic schema will represent explicit externally supplied storage/warehousing service separately, using an external-service classification and evidence-backed service/counterparty context. Program code validates the closed classification and required exact-evidence linkage; unknown scope remains an atomic record and yields a reason-coded role gap rather than a role.

The classification is a closed program-facing contract, not a free-form model role:
`storage_semantics=internal_inventory|external_service|unknown`. The semantic response may provide Chinese source wording and raw service/recipient hints, but program code derives the enum from those fields and exact evidence. `external_service` additionally requires a non-empty service marker and a beneficiary scope of `third_party|customer|named_counterparty`; `internal_inventory` requires no recipient. Missing or contradictory fields resolve to `unknown`.

Only a governed external storage-service activity maps to `storage_provider`. This keeps linguistic interpretation in semantic extraction while leaving the executable derivation rule deterministic. A confidence score alone cannot satisfy the external-service gate.

The rejected alternatives are mapping every `stores` action, which is demonstrably false, and relying only on a hard-coded Chinese phrase list, which cannot cover annual-report language reliably.

### 3. Aggregate roles by business identity, retain support by lineage

A derived role's business identity is the instrument, optional business segment/scope, role, report cohort/business regime, and derivation-rule version. Supporting activity ids and evidence ids are lineage, not competing role identities. All qualifying supports in the same role group are sorted, deduplicated, and stored on one role record. The role record id and temporal stable identity use compatible business keys; evidence id is not used to manufacture parallel roles for one capability.

The implementation MUST make this key explicit in one shared helper used both by record-id generation and temporal validation. `evidence_id` and supporting activity ids MUST remain metadata only for a company/scoped capability role. A changed support set for the same report cohort updates lineage on the same role identity or creates a governed successor according to `replace`; it MUST NOT create a second overlapping role.

Object detail is never discarded because it remains in the atomic activities referenced by the role. If a future role is genuinely object-specific, object identity must be added to both record identity and temporal identity in the same change; this change does not create such roles.

### 4. Make contract facts occurrence-specific from evidence

Every new contract/table fact uses a program-derived occurrence key based on immutable document/table identity, one-based physical page, row ordinal or occurrence ordinal, ordered raw cells, and explicit contract reference when present. Raw values and raw units remain unchanged LLM/source outputs; normalization and aggregation remain programmatic.

The preferred source is deterministic table provenance (`table_id`, physical page, row ordinal, ordered cells). If the parser does not expose a row ordinal, the runtime MUST use a deterministic occurrence ordinal within the immutable evidence span and mark `occurrence_identity_quality=derived_from_evidence`; if even that is unavailable, it MUST mark the identity `unresolved` and hold only the ambiguous group. It MUST NOT use a model-generated id or a value-only hash as a substitute for row identity.

The occurrence key participates consistently in record id, fact scope, temporal stable identity, persisted metadata, ambiguity grouping, and publication lineage. Equal product labels therefore do not imply equal facts. If two source-supported contracts disclose `4.18 亿元` and `0 亿元`, both contract facts remain available. A company-level total can only be a separately typed program-derived aggregate with explicit inclusion rules.

For a legacy record, local repair first reconstructs the occurrence key from persisted evidence, exact semantic output, raw cells, value/unit, and occurrence order. When reconstruction is reliable, it creates row-aware successors and preserves the legacy row as history. When it is not reliable, the approved legacy record is preserved, conflicting candidates are held with a typed reason, and only that ambiguity group is blocked.

### 5. Apply result policy across the whole observation lifecycle

`reuse` means:

- use a compatible completed semantic artifact when its source and processing contracts match;
- locally upgrade/replay persisted rows when sufficient evidence exists;
- preserve approved and held history;
- reuse an existing compatible current derived record and merge deterministic support lineage;
- skip or hold an incompatible conflicting candidate with explicit diagnostics;
- call the configured semantic model only when no compatible reusable result can satisfy the requested family.

`replace` means a fresh semantic observation may create governed successors through normal promotion and temporal rules. It does not delete evidence or bypass review, and it does not use last-write-wins.

The policy is passed from the application entry point through semantic execution, persistence, role derivation, exposure publication, and repair. A run report records the applied policy at every family boundary.

### 6. Isolate failures by governed business group

Operating-fact occurrence groups and derived-role business groups are the smallest publication units. A deterministic conflict in one group becomes a held/skipped result plus a persisted reason code; unrelated groups continue. Provider failures retain existing retry/backoff classification, while identity, evidence, and temporal failures cannot be relabelled as gateway congestion.

This preserves the existing single publication owner. It changes transaction/error boundaries, not ownership.

### 7. Repair historical data through the same owners

One bounded business-profile integrity repair service supports `audit` and explicit `apply`; audit is the default. It detects at least:

- `storage_provider` roles derived only from internal inventory activities;
- multiple role ids representing one corrected role business identity;
- contract facts sharing a legacy broad identity despite distinct reconstructable occurrences;
- approved/candidate pairs blocked solely by missing row identity;
- semantic artifacts selected for reuse under an incompatible processing contract.

Apply delegates role derivation, temporal transitions, fact persistence, and publication to their existing domain owners. Invalid machine-derived roles that were never valid under the corrected semantics are removed only when they have no inbound lineage/review dependency; otherwise they are transitioned out of the current projection and dependent derived publications are replayed. Source evidence, valid atomic facts, valid approved history, and review decisions are never deleted. Unreconstructable rows are held, not guessed.

The operation is scoped by instrument, transactional per instrument/group, idempotent, and performs no network or LLM call. Reports contain stable ids, reasons, before/after state, and `would_change`, `changed`, `unchanged`, `held`, and `failed` counts.

### 8. Make token ownership observable

Each processed field family records one origin: `llm_extracted`, `semantic_reused`, `local_replayed`, or `program_derived`. The run aggregates prompt, completion, and total tokens from actual gateway calls only. Reuse, repair, role derivation, normalization, and publication report zero LLM tokens. No stronger-model route is selected unless the common LLM layer later supplies and explicitly configures one.

## Risks / Trade-offs

- [Corrected role counts decrease] -> Treat removal of false inventory-derived roles as a data correction and show before/after role reasons in repair reports.
- [External storage service wording is ambiguous] -> Keep the atomic activity, emit an external-service-scope gap, and fail closed on role derivation rather than infer from confidence.
- [Legacy contract rows lack row/page metadata] -> Preserve approved history, hold only the conflicting group, and require fresh extraction only when local reconstruction is impossible.
- [Changing temporal keys exposes existing invalid rows] -> Audit first on a copied database, migrate by instrument, and reuse existing successor/transition owners rather than weakening temporal validation.
- [Role support merging can hide object detail in the role projection] -> Keep complete sorted supporting activity/evidence ids and expose detail through the referenced atomic records.
- [Per-group isolation permits a partially complete report] -> Report completeness and held groups explicitly; do not mark a requested family fully complete while a required group is held.
- [Reuse compatibility becomes stricter] -> Prefer a local schema upgrade; invoke LLM only when the requested semantic family truly has no compatible reusable source.

## Migration Plan

1. Add the corrected future-write semantic classifications, identities, role grouping, and per-group failure behavior behind the existing business-profile owner.
2. Add focused tests using the `000858.SZ` inventory pattern and the `601012.SH` two-contract pattern, including `force=true result_policy=reuse` and repeated execution.
3. Run repair audit against a copied database for the affected cohort; verify every proposed role transition/deletion and contract successor against persisted evidence.
4. Apply repair to the copied database, rerun audit to prove idempotence, and compare approved/current API projections plus backfill reports.
5. Deploy future-write behavior, then apply bounded production repair by explicit instrument cohort before resuming broad rollout.
6. Remove any temporary migration adapter after the repaired cohort and tests prove that the normal backfill path alone maintains the corrected contract.

Rollback disables repair apply and reverts the code release. Existing source evidence and historical rows remain available; database restoration uses the normal pre-apply backup if a production repair cohort must be reverted.

## Open Questions

- No model-tier decision is required for this change. A future ambiguity-review route may use another model only after the LLM team exposes it through the existing gateway contract.
- Exact repair cohorts will be produced by audit; the first mandatory acceptance cohort includes `000858.SZ`, `002496.SZ`, `600276.SH`, `601318.SH`, and `601012.SH`.
