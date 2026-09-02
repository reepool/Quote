# Business-profile garbage cleanup (2026-09-01)

After the 10.2a read-only audit, the confirmed non-reusable lifecycle state was
physically cleaned with the existing bounded `cleanup_only` path. This was a
write operation against the local research database and owned checkpoint root;
no approved record, evidence row, review audit, current work item, or
owner-backed publication candidate was selected for deletion.

## Deleted

- 1 terminal-failure work item: `bp-work-d0771460a217ff69dd122fe6`
- Its stale-scope checkpoint (6,063 bytes)
- 3 non-reusable semantic artifacts:
  - `bp-semantic-artifact-7c0f442822a71f01845ced0b`
  - `bp-semantic-artifact-4f3eb51aa4e5653ce1165c62`
  - `bp-semantic-artifact-ff300ce003e557da118fdbc2`
- Candidate descendants explicitly owned by those artifacts/runs:
  - 3 activities
  - 13 operating facts
  - 21 relationships

The deletion was transactional per instrument/artifact and returned zero
failures. A subsequent all-scope `cleanup_only` audit reports no remaining
obsolete work, failed semantic artifact, orphan checkpoint, or orphan candidate
finding.

The 17 candidate exposure facts owned by the completed 002415 publication
manifest were deliberately retained: they still have a durable publication
owner and are not proven garbage. The 10.2a identity blocker remains separate:
52 approved rows lack current `source_occurrence_material`, so no approved
history was deleted or re-keyed by this cleanup.

## Follow-up cleanup and migration (same day)

The completed publication manifest was subsequently audited as an invalid
terminal owner because it still contained unapproved descendants.  All 17
`002415.SZ` candidate exposure facts were physically deleted in one transaction
and the manifest was marked `cleaned`.  `002496.SZ` and `300750.SZ` had no
remaining cleanup-only findings.

The exact approved-occurrence migration then processed three
`300750.SZ` operating-fact duplicate groups.  Each group had identical
source-derived physical material and semantic fingerprint.  The retained
canonical IDs were updated with the current identity material and lineage hash;
three duplicate rows were physically deleted and three immutable
`system:occurrence_migration` review audits were written.  Seven activity groups
with semantic-content conflicts remain held for explicit review; they were not
deleted or auto-resolved.

Post-operation `cleanup_only` audit: `issue_counts={}` for all three selected
instruments.  The 10.2a migration audit still reports
`canonical_rekey_material_unavailable` solely for the seven held semantic
conflicts, with `candidate_descendants=0`.
