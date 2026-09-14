## Context

The select stage may produce a new artifact when the PDF engine, normalized text, selector inputs, or parser identity changes. Under `result_policy=reuse`, the extract stage currently reuses governed record identifiers from an earlier completed semantic run but leaves the new select-stage artifact path on the output. Verify then loads the new artifact while the reused evidence records still contain the original section identifiers and hashes. This mixes two valid but incompatible evidence contexts and fails before an LLM request is created.

The authoritative chain is `selected artifact -> semantic extraction -> governed evidence/records -> verification -> promotion`. A reused semantic extraction must therefore reuse its source selected artifact as a unit. Existing database schemas already persist the required artifact path, artifact hash, document hash, evidence ids, and exact evidence spans.

## Goals / Non-Goals

**Goals:**

- Treat the selected artifact and governed semantic records as one reusable evidence context.
- Prove every persisted evidence span against the source artifact before reuse.
- Fall back to normal extraction on the current selected artifact when the source context is absent, corrupt, or incompatible.
- Prevent prior verification results and machine-rework failures from crossing evidence-context boundaries.
- Recover current open evidence-provenance failures through the normal backfill command without data deletion or a special migration.

**Non-Goals:**

- Relax exact-evidence validation or permit quote-only publication without a source section.
- Rewrite approved historical rows in place.
- Add a second repair command, database migration, or new persistence owner.
- Change PDF parsing, LLM prompts, public APIs, scheduling, or publication policy.

## Decisions

### Reuse the complete source evidence context

When a completed semantic family is considered for reuse, the runtime loads the `selected_artifact_path` recorded by that semantic run rather than combining its records with the current select-stage artifact. The reusable output replaces the selected artifact path/hash with the source values and records both the current selection and source selection in diagnostics.

This is preferred over remapping section identifiers because normalization changes can alter section boundaries and quotes. It is also preferred over always re-extracting because unchanged durable results remain reusable without another LLM call.

### Validate source context before accepting reuse

Reuse requires all of the following:

- the source artifact exists and its stored hash matches the run bundle hash;
- its document hash matches the currently selected annual-report document;
- every persisted evidence id exists;
- every exact evidence span resolves to a section in that artifact with matching section hash, page, quote hash, and quote content.

Failure rejects only that reuse candidate and continues to older candidates or fresh extraction. It does not create machine rework by itself because extraction on the current context is the automatic recovery path.

### Scope verification state to evidence context

Verification artifacts persist an evidence-context hash derived from the selected artifact hash and source document. Resume accepts prior verification and inherited machine rework only when that hash matches the current verify input. Legacy verify artifacts without the hash are treated as stale and recomputed. A successful verification or re-extraction resolves old evidence-provenance exceptions through the existing exception owner.

### Preserve governed history

Fresh extraction follows existing governed identity/version and terminal-row protections. This change does not overwrite approved evidence or weaken collision checks. If a new parser produces materially different facts, existing persistence rules continue to require distinct governed identities or reject the transaction.

## Risks / Trade-offs

- [A legacy semantic run lacks a selected artifact path] -> Reject reuse and perform fresh extraction; do not guess a section mapping.
- [The source artifact exists but its evidence metadata is incomplete] -> Reject reuse and log a bounded reason code identifying the failed evidence id.
- [Fresh extraction costs an additional LLM call] -> It occurs only when durable evidence cannot be proven against its original context; correctness takes priority over reuse.
- [Current task is already running with old code] -> The active run may finish degraded. Deployment requires a service restart; the next normal run replays failures through the corrected path.
