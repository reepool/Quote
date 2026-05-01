# Quote Data Volume Migration Cleanup Plan

This document lists files and directories left by the `/dev/sda3` data-volume
migration that can be cleaned after the system has run normally for at least
three days.

Do not run these cleanup commands before the post-migration observation window
passes.

## Current Verified State

Migration completed on 2026-04-30 around 23:17 local time.

Current target layout:

- `/home/python/Quote/data` is mounted from `/dev/sda3`.
- `/home/python/Quote/data/PVE-Bak` is mounted from
  `192.168.188.88:/volume2/PVE-Bak`.
- `/home/python/Quote/data/QuoteBak` is mounted from
  `192.168.188.68:/export/HDD-2/QuoteBak`.
- `/home/python/sd` has been removed and should not reappear.
- `/home/python/Quote/data.rootfs.bak` is the old root-filesystem data backup.
- `/etc/fstab.quote-migration-20260430231708.bak` is the fstab rollback copy.

## Cleanup Gate

Clean only after all of the following are true:

1. At least three calendar days have passed since migration completion.
   For this migration, the earliest calendar time is after
   `2026-05-03 23:17` local time.
2. Quote services have restarted and stayed stable.
3. At least one successful daily data update has completed after the migration.
4. At least one database backup task or manual NAS write test has succeeded
   against `data/PVE-Bak/QuoteBak`.
5. The current data path still shows `/dev/sda3` and both NAS child mounts:

```bash
findmnt -R -T /home/python/Quote/data
df -h /home/python/Quote/data /home/python/Quote/data/PVE-Bak /home/python/Quote/data/QuoteBak
```

6. SQLite quick checks and core read smoke tests pass:

```bash
sqlite3 /home/python/Quote/data/quotes.db "PRAGMA quick_check;"
sqlite3 /home/python/Quote/data/research.db "PRAGMA quick_check;"
sqlite3 /home/python/Quote/data/quotes.db "SELECT time, instrument_id, close FROM daily_quotes ORDER BY time DESC LIMIT 1;"
sqlite3 /home/python/Quote/data/research.db "SELECT COUNT(*) FROM shareholder_snapshots; SELECT COUNT(*) FROM industry_memberships;"
```

## Cleanable File List

### Required Cleanup After Stability Window

| Path | Current size | Type | Reason | Cleanup condition |
| --- | ---: | --- | --- | --- |
| `/home/python/Quote/data.rootfs.bak` | about 11G | rollback data backup | Old root-filesystem copy of `data/`; no longer used after `/dev/sda3` is mounted at `data/` | Clean after the cleanup gate passes |

Contents observed in `data.rootfs.bak`:

```text
/home/python/Quote/data.rootfs.bak/quotes.db
/home/python/Quote/data.rootfs.bak/research.db
/home/python/Quote/data.rootfs.bak/market_data.db
/home/python/Quote/data.rootfs.bak/download_progress.json
/home/python/Quote/data.rootfs.bak/reports/
/home/python/Quote/data.rootfs.bak/backups/
/home/python/Quote/data.rootfs.bak/PVE-Bak/
/home/python/Quote/data.rootfs.bak/QuoteBak/
```

Cleanup command:

```bash
cd /home/python/Quote
rm -rf data.rootfs.bak
```

### Optional Cleanup After One Reboot Or Mount-A Test

| Path | Current size | Type | Reason | Cleanup condition |
| --- | ---: | --- | --- | --- |
| `/etc/fstab.quote-migration-20260430231708.bak` | about 1K | system rollback backup | Backup of the old fstab before the mount switch | Clean only after a reboot or `mount -a` verification confirms the new fstab is correct |

Cleanup command:

```bash
sudo rm -f /etc/fstab.quote-migration-20260430231708.bak
```

### Safe Build Artifact Cleanup

| Path | Type | Reason | Cleanup condition |
| --- | --- | --- | --- |
| `/home/python/Quote/scripts/__pycache__/quote_data_volume_preflight.cpython-311.pyc` | Python bytecode | Created by migration script validation | Can be removed any time |
| `/home/python/Quote/scripts/__pycache__/quote_data_volume_validate.cpython-311.pyc` | Python bytecode | Created by migration script validation | Can be removed any time |

Cleanup command:

```bash
rm -f scripts/__pycache__/quote_data_volume_preflight.cpython-311.pyc
rm -f scripts/__pycache__/quote_data_volume_validate.cpython-311.pyc
rmdir scripts/__pycache__ 2>/dev/null || true
```

## Keep For Now

Do not remove these during the three-day observation window:

| Path | Reason |
| --- | --- |
| `docs/development/quote_data_volume_migration.md` | Runbook and rollback procedure |
| `docs/development/quote_data_volume_cleanup.md` | This cleanup plan |
| `scripts/quote_data_volume_preflight.py` | Useful for future mount and database checks |
| `scripts/quote_data_volume_validate.py` | Useful for future post-reboot validation |
| `scripts/quote_data_volume_root_switch.sh` | One-time root switch script; keep until the migration OpenSpec change is archived |
| `openspec/changes/relocate-data-mount-to-sda3/` | Active OpenSpec change; archive through OpenSpec instead of deleting manually |

After the observation window and after OpenSpec archival, the one-time root
switch script may be removed if no longer needed:

```bash
rm -f scripts/quote_data_volume_root_switch.sh
```

Keep the preflight and validation scripts unless the team decides migration
tooling should not remain in the repository.

## Do Not Clean

These are production data paths after migration and must not be deleted:

```text
/home/python/Quote/data/quotes.db
/home/python/Quote/data/research.db
/home/python/Quote/data/download_progress.json
/home/python/Quote/data/reports/
/home/python/Quote/data/backups/
/home/python/Quote/data/filings/
/home/python/Quote/data/PVE-Bak/
/home/python/Quote/data/QuoteBak/
```

Do not delete SQLite `-wal` or `-shm` files from the active `data/` directory
while services may be running. If cleanup is needed, stop services first and
checkpoint through SQLite rather than deleting active runtime files directly.

## Final Post-Cleanup Check

After cleanup, verify:

```bash
test ! -e /home/python/Quote/data.rootfs.bak
test ! -e /home/python/sd
findmnt -R -T /home/python/Quote/data
sqlite3 /home/python/Quote/data/quotes.db "SELECT COUNT(*) FROM instruments;"
sqlite3 /home/python/Quote/data/research.db "SELECT COUNT(*) FROM industry_memberships;"
```
