# Announcement Asset Backup And Restore

This document defines the version 1 ordering implemented by
`AnnouncementAssetBackupService` and `AnnouncementAssetRestoreService`.
It covers shared announcement attachments only. It does not authorize an
operator to overwrite the live catalog or remove a primary attachment.

## Backup ordering

1. Validate that the configured backup mount has an independent filesystem
   identity and expected source.
2. Enumerate the complete catalog-required blob set. This includes current
   effective blobs, active retention pins, pending/failed deletion predecessor
   and replacement blobs, and every indefinitely active recovery-manifest blob.
3. Reserve target capacity for missing or operator-repaired blobs, the online
   SQLite snapshot, and the recovery-journal/file-manifest artifacts.
4. Create a content-addressed online SQLite snapshot.
5. Revalidate every existing hash-named target. Ordinary runs preserve a
   mismatched target's path, bytes, and mtime. An authorized repair records the
   original evidence, moves it to quarantine, and republishes verified bytes.
6. Publish missing blobs atomically. A remount before publication or final
   watermark persistence fails closed. A complete uncommitted `.part` is
   reconciled after the expected mount returns.
7. Publish an immutable recovery-journal bundle in the backup failure domain.
   Its hash, catalog baseline sequence/watermark, terminal sequence/watermark,
   and source catalog generation are bound into the file manifest.
8. Publish the content-addressed file manifest, revalidate mount identity, then
   persist per-blob paired backup state. A file or manifest without that final
   catalog state does not satisfy deletion readiness.
9. Close eligible recovery pairs in manifest -> catalog snapshot -> append-only
   closure -> retention-pin CAS order. Version 1 performs no automatic backup
   blob, manifest, snapshot, journal, or recovery-manifest garbage collection.

## Isolated consumer rollback

Consumer rollback is not a live catalog restore. `reconstruct_legacy_paths`
reads only immutable `legacy_path_rollback` recovery entries, verifies their
paired file manifest and hash-qualified backup object, and defaults to an
isolated `root_override`. It can reconstruct from backup after the primary path
has been removed. It never registers recovery-only bytes as another canonical
shared attachment. Publishing directly to a live legacy root requires the
separate `publish_live=True` authorization after an isolated consumer-read
drill has passed.

## True data-loss restore gate

Before any live overwrite or service reopen, an operator must:

1. Freeze announcement-asset writes and record the declared snapshot RPO.
2. Select the mutually compatible application version, content-addressed
   catalog snapshot, file manifest, and independent recovery-journal bundle.
3. Run `verify_paired_restore`. It verifies mount/failure-domain identity,
   catalog and manifest hashes, the complete unsampled required blob set, the
   embedded journal chain and terminal watermark, and restored catalog lineage
   invariants. Caller-supplied journal declarations must exactly match the
   independently persisted pair.
4. Stage the verified catalog and files in an isolated root. Replay every
   accepted post-snapshot journal increment with `replay_recovery_journal`.
   Replay categorically rejects the live catalog path, verifies each row's
   pre-image hash, appends the journal entry in the same SQLite transaction as
   its changeset, and requires the declared terminal coverage watermark.
5. Re-run full blob, effective/no-winner, immutable decision, deletion/outbox,
   recovery-pair, retention-pin, and consumer-current-result reconciliation.
6. Rebuild and compare readiness projections. Re-enable reads first, then
   writes and consumer startup. Predecessor deletion is enabled last and only
   through its existing recovery-pair gate.

A missing middle journal entry, out-of-order entry, tail truncation, payload or
integrity-hash mutation, mismatched backup object, dangling decision edge, or
unclosed migrated recovery hint blocks reopen. Routine sampled drills are
additional evidence only and never replace this full enablement gate.
