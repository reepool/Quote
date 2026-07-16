## Context

Checkpoint IDs and stored parameter hashes are currently calculated from every normalized command parameter. `resume` controls whether an existing checkpoint is loaded; it does not change the requested data or processing policy. Including it in identity makes the act of enabling resume select a different checkpoint. Existing production checkpoints were written with the old full-parameter hash and must remain usable after the correction.

## Goals / Non-Goals

**Goals:**
- Make `resume=false` and `resume=true` resolve to the same logical checkpoint identity.
- Preserve identity changes for parameters that alter data scope or processing policy.
- Discover and load compatible legacy checkpoints without manual IDs.
- Keep explicit checkpoint IDs authoritative.

**Non-Goals:**
- Changing database schemas or backfill stage behavior.
- Merging checkpoints with different data ranges, scopes, exchanges, chunk sizes, or repair policies.
- Automatically executing another production backfill.

## Decisions

1. Checkpoint parameter hashing will ignore only `resume`. Other parameters remain identity-sensitive because they can change the frozen universe, work partitioning, source access, or persisted results.
2. `resolve_id` will return the canonical identity-derived ID for new runs. When resume is requested and the canonical file is absent, it will scan checkpoint metadata for identity-compatible legacy files and select the most recently updated one.
3. `load` will validate both current hashes and legacy payloads by recomputing the normalized identity from stored parameters. A successfully loaded legacy payload will be migrated in memory to the current hash and saved atomically on the next stage update.
4. The scheduler will request legacy discovery only for non-dry-run resume operations. Explicit IDs bypass discovery and retain current precedence.

## Risks / Trade-offs

- [Multiple legacy checkpoints share one normalized identity] -> Select the most recently updated valid checkpoint and cover the ordering with tests.
- [Corrupt or unrelated checkpoint files exist] -> Ignore unreadable candidates during discovery; strict validation still occurs when the selected checkpoint is loaded.
- [Removing too many controls from identity could resume incompatible work] -> Exclude only `resume`; keep all data and processing policy parameters hashed.

## Migration Plan

Deploy the helper and scheduler changes without running a backfill. The next `resume=true` invocation can discover the latest compatible legacy checkpoint, then migrate its hash during normal atomic saves. Rollback restores the previous selection behavior; checkpoint JSON remains readable because no fields are removed.

## Open Questions

None.
