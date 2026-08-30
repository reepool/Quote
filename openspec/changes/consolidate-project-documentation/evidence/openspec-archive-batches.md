# OpenSpec Archive Batches

**Snapshot date:** 2026-08-30
**Scope:** active (not already archived) OpenSpec changes only.

## Method

1. Read `openspec list --json`; this snapshot has 122 active change directories:
   104 `complete` and 18 not complete.
2. A status-complete candidate must have complete artifacts and no unchecked
   task. It is then scanned for its literal change name in all other
   non-archived change directories.
3. Every delta capability must already have a main
   `openspec/specs/<capability>/spec.md`. A difference between the delta and
   main spec is expected: archive must merge the delta rather than move the
   only copy of the requirement.
4. A batch is archived one change at a time through `openspec archive`, which
   validates and synchronizes the delta specs before moving the directory.

The literal-reference scan is a lower bound. A named baseline, a task
dependency, an active worktree overlap, or a missing durable spec blocks a
candidate even where the name is absent from the scan.

## Batch A: Archived

All four changes had complete artifact graphs and no unchecked tasks. Their
name scans found no references in another non-archived change. Each listed
main capability existed, and `openspec archive --yes` merged the delta before
moving the change on 2026-08-30.

| Change | Delta capabilities checked | Active cross-change reference scan | Decision |
|---|---|---|---|
| `harden-dce-proxy-browser-fallback` | `futures-market-data`, `futures-official-trading-calendar-backfill`, `official-futures-source-priority` | none | archived; 9 requirements added |
| `stabilize-financial-statement-source-routing` | `financial-operations-scheduler`, `official-financial-source-profiles` | none | archived; 5 requirements added |
| `retire-legacy-annual-report-assets` | `broker-annual-report-risk-control-source`, `data-storage-layout`, `research-data-engine`, `scheduler` | none | archived; 5 requirements added |
| `reconcile-provisional-futures-bars-and-write-semantics` | `futures-market-data`, `scheduler` | none | archived; 4 requirements added and 1 modified |

## Deferred Completed Candidates

| Change(s) | Blocker | Recheck condition |
|---|---|---|
| `correct-shareholder-exposure-supply-chain-semantics`, `harden-business-profile-end-to-end-integrity`, `harden-business-profile-fact-identity-and-publication` | Their delta capabilities have no corresponding main spec (3, 1, and 2 respectively). They are also in the currently active business-profile area. | Sync durable specifications and verify the profile worktree/change boundary. |
| `harden-a-share-daily-source-failover` | `a-share-daily-source-failover` has no main spec, even though its other delta capabilities do. | Create/sync the missing main spec, then rescan. |
| `triage-announcement-only-xdxr-candidates` | W6 explicitly names this change as its post-triage behavior baseline in proposal, design, task 1.5, and the corporate-action boundary spec. Its `announcement-only-xdxr-semantic-triage` main spec is also missing. | Keep live until W6 migrates its baseline reference or records an archive-safe replacement, and sync the durable spec. |
| Other completed changes with a missing main capability spec | The status list is not proof that the durable requirement survived archive. | Process in later bounded batches after main-spec synchronization and dependency scan. |

## Outcome

This record completes the baseline classification of completed changes and
creates a reviewable, four-change first batch. It does not authorize bulk
archiving the remaining status-complete backlog.
