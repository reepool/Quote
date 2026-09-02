# Business Profile 10.2 Cleanup Manifest

- Execution time: `2026-08-31`
- Database: `data/research.db`
- Mode: controlled local cleanup (`apply=true`, `cleanup_only=true`, `result_policy=reuse`)
- Network access: `false`
- LLM access: `false`
- Pre-cleanup backups:
  - `/tmp/business_profile_before_10_2_20260831.db`
  - `/tmp/business_profile_before_10_2_replay_cleanup_20260831.db`
  - `/tmp/business_profile_before_10_2_orphan_cleanup_20260831.db`
- Backup integrity: SQLite `PRAGMA integrity_check=ok` before each cleanup phase

## Deletion Manifest

Only confirmed non-reusable lifecycle descendants were deleted. IDs are represented
by a deterministic SHA-256 over the sorted deleted IDs; the full IDs remain
recoverable from the pre-cleanup backup files above.

| Table | Deleted | Deleted-ID SHA-256 |
| --- | ---: | --- |
| `business_profile_work_items` | 1 | `3be4efe22e381d51551268581e1119f604fb87a3ab25a431238bc2a61aa490d5` |
| `business_profile_semantic_runs` | 2 | `878ec3481086efe420dea503246ae6400844956d44b8dec900499d9cfd685153` |
| `business_profile_semantic_artifacts` | 1 | `425617f875cf7bc818a19c42b6f8cdf99529d878e4ff07c07b5cff63a6059439` |
| `company_business_activities` (candidate) | 3 | `5732588ea13a1037b74d31a3d181957bd9e0c6b976420644e911675fab396c9a` |
| `company_supply_chain_relationships` (candidate) | 20 | `0d209132c8646f557d27c2bf4c55119cc78d72651e54ae7131311d2653a421a4` |
| `company_commodity_exposure_facts` (candidate) | 152 | `383b2505c7d02a0a79405fdcbbc1e2153adad9c3d1c133f13b97a5931f53f850` |
| `business_profile_exceptions` (orphan candidate exceptions) | 152 | `b6def90afb5be82b4b1a06e0330e7e089023bf42e089c6dbd0c2363c179ea684` |

The 3 activity and 20 relationship candidates belonged to the failed
`002496.SZ` semantic run. The exposure-fact candidates were orphaned because
their referenced semantic run no longer existed. No approved record was a
deletion target.

## Preserved Invariants

- Approved activities: `264` before and after.
- Approved relationships: `7` before and after.
- Approved exposure facts: `74` before and after.
- Approved published exposures: `2` before and after.
- Evidence rows: `112` before and after.
- Review-audit rows were not deleted.
- `300750.SZ` retry-due verify work remains present for a later provider retry.

## Post-cleanup Verification

- Orphan candidate activities: `0`.
- Orphan candidate relationships: `0`.
- Orphan candidate exposure facts: `0`.
- Terminal-failure work items: `0`.
- Retired/orphan checkpoint files: `0`.
- Semantic runs with missing governed-record references: `0`.
- Cleanup-only audit rerun: `issue_counts={}`, `failed=0`.

The cleanup did not broaden into approved-history deletion, evidence deletion,
or automatic retry of provider-congestion work. The database remains ready for
the targeted replay gate in OpenSpec task 10.3.

## Follow-up Cleanup Before 10.3

After the first targeted replay, a read-only audit found legacy state that was
not reusable but was outside the original deletion manifest:

- `002415.SZ`: 17 orphan candidate exposure facts whose semantic run rows no
  longer existed.
- `300750.SZ`: 10 orphan candidate exposure facts and one legacy-occurrence
  semantic artifact (`bp-semantic-artifact-9eeaee85e02ed6a12d6cb185`).

The exact three findings were deleted transactionally with
`cleanup_only=true, apply=true`. No approved activity, operating fact, role,
exposure fact, evidence row, or review audit was deleted. `002496.SZ` retained
its `retry_due` work item because it represents a provider transport failure
that remains eligible for retry, not an unusable artifact.

Backup and integrity evidence:

- `/tmp/business_profile_before_10_3_cleanup_20260831_full.db`
- `PRAGMA quick_check=ok`
- Post-cleanup repair audit for `002415.SZ`, `002496.SZ`, and `300750.SZ`:
  `issue_counts={}`, with no execution-state deletion candidates.
