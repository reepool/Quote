## Why

The A-share historical backfill currently includes the `resume` execution switch in checkpoint identity. Changing a completed or interrupted run from `resume=false` to `resume=true` therefore creates a different checkpoint and repeats all chunks instead of reusing recorded progress.

## What Changes

- Define checkpoint identity from data-affecting backfill parameters while excluding the `resume` execution-control switch.
- Reuse compatible checkpoints created by earlier versions, including checkpoints whose stored hash included `resume`.
- Preserve explicit `checkpoint_id` precedence and keep data range, exchanges, scopes, chunking, and repair policy identity-sensitive.
- Add regression coverage for identity stability, legacy checkpoint discovery, and parameter mismatch protection.

## Capabilities

### New Capabilities
- `a-share-backfill-checkpoint-resume`: Stable and backward-compatible checkpoint selection for governed A-share historical backfills.

### Modified Capabilities

None.

## Impact

- Affects `utils/a_share_historical_backfill.py`, the scheduler backfill entry point, and focused unit tests.
- Does not change database schemas, stored market data, command syntax, or existing explicit checkpoint identifiers.
