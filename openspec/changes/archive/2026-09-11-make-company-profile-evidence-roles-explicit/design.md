## Context

The prepared scope already binds Evidence to a checklist field through `PreparedEvidence.field_id`; entries without a binding are contextual. The provider catalog currently collapses those distinctions into a single `field_ids` list, so a model can mistake page context or a neighboring disclosure for proof of a requested field. The change addresses a concrete failure class observed in the authoritative shadow batch while preserving local validation as the source of truth.

## Goals / Non-Goals

**Goals:**

- Serialize deterministic Evidence roles and exact owning field IDs in every provider request.
- Give extract and verify workers explicit rules for legal-empty coverage and known cross-field confusions.
- Keep all existing schema, Evidence, subject, unit, and verification guards unchanged.
- Prove the payload and instruction behavior without consuming provider calls.

**Non-Goals:**

- No new Evidence model hierarchy, registry, confidence score, or downstream usage system.
- No relaxation of validator rules, Gold expectations, report status gates, or negative cases.
- No historical batch mutation, model substitution, timeout change, or external replay.

## Decisions

1. **Derive roles from existing bindings.** An entry with one or more prepared `field_id` bindings is `field_owner`; an entry with no bindings is `context_only`; an entry that is reused for both is `mixed`. This avoids changing immutable Evidence identities or introducing a parallel schema.

2. **Keep role metadata provider-facing only.** The catalog adds `field_owner_ids`, `context_only`, and `evidence_role` while the canonical Evidence and prepared-scope models remain unchanged. Local normalization and verification continue to resolve exact Evidence IDs and hashes.

3. **Use explicit negative instructions.** The request contract states that only field-owning Evidence may support observed or legal-empty coverage. Context-only Evidence can explain headers, relationships, or reconciliation but cannot close an unrelated field. Regime and segment cautions are added to the existing task/scope instructions rather than creating new enums.

4. **Test the contract without a provider.** Focused tests inspect compact extract/verify payloads and the assembled instruction text. Existing integration and batch tests remain unchanged and no live run is part of this change.

## Risks / Trade-offs

- **A genuinely shared Evidence span owns multiple fields** → represent it as `mixed` and retain all exact owning field IDs; the model may use it for each listed field but not for unlisted fields.
- **Older callers inspect only `field_ids`** → retain the existing key and add role metadata compatibly.
- **Instructions do not fix every semantic error** → validator and verifier remain authoritative; unresolved results stay typed rather than being accepted.
