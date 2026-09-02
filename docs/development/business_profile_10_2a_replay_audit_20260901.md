# Business-profile 10.2a replay audit (2026-09-01)

This is a read-only audit of the failed operation
`business-profile-20260831214638661690`. It used the control artifact and
local `data/research.db`; SQLite was opened with `mode=ro`. No production rows,
checkpoint files, or artifacts were changed, and no network or LLM call was
made.

Command:

```text
/home/python/miniconda3/envs/Quote/bin/python scripts/research_business_profile_replay_audit.py \
  --output docs/development/business_profile_10_2a_replay_audit_20260901.json
```

The machine-readable manifest is
[`business_profile_10_2a_replay_audit_20260901.json`](business_profile_10_2a_replay_audit_20260901.json).

## Result

- Status: **blocked** for canonical migration review; all failed-operation
  candidate descendants are now physically cleaned.
- Requested IDs reconstructed from the three selected work items: **133**.
- Reconstructed dispositions: **61 `written`**, **72 `not_persisted`**.
- Candidate descendants found in the selected scope: **0** (the 17 completed
  publication-manifest candidates from the first audit were physically deleted
  by cleanup-only apply and the manifest was marked `cleaned`).
- Approved occurrence groups with an exact new source material: **3**
  operating-fact groups migrated transactionally on `300750.SZ`; their
  duplicate rows were physically deleted and immutable review audits written.
- Remaining approved occurrence groups requiring manual review: **7** activity
  groups with semantic-content conflicts on `300750.SZ`.
- Approved rows whose occurrence material is not in the current schema remain
  protected history and are not deletion targets.

The 52 rows are an unresolved canonical-identity set, not deletion targets.
Because their physical source material cannot be proven from the persisted
data, the audit intentionally does not select a canonical row, re-key any
identity, or delete an approved row. Every reconstructed mapping in the JSON
manifest is marked `inference: reconstructed`; no inference was written back
as original execution truth.

The failed-operation descendants are no longer blocking cleanup. The remaining
`canonical_rekey_material_unavailable` blocker is intentional: seven activity
groups contain different semantic fingerprints for one physical source row.
They require explicit review before any approved row can be removed. No
inference is used to choose a winner.
