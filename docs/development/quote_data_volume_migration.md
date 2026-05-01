# Quote Data Volume Migration Runbook

This runbook records the controlled migration that moves Quote local data from
the root filesystem to the dedicated `/dev/sda3` filesystem mounted at
`/home/python/Quote/data`.

## Target Layout

- Local data volume: `/dev/sda3` mounted at `/home/python/Quote/data`
- Local databases: `data/quotes.db`, `data/research.db`, future
  `data/financials.db`
- Future financial filing archive root: `data/filings/`
- NAS backup child mounts:
  - `192.168.188.88:/volume2/PVE-Bak` at `data/PVE-Bak`
  - `192.168.188.68:/export/HDD-2/QuoteBak` at `data/QuoteBak`

The application should keep using project-relative paths under `data/`.

## Preconditions

1. Stop Quote API, scheduler, Telegram task entry points, daily quote update,
   and any manual script that may write `quotes.db` or `research.db`.
2. Confirm no open SQLite handles:

```bash
lsof data_bak/quotes.db data_bak/quotes.db-wal data_bak/research.db data_bak/research.db-wal
```

3. Record current mount state, disk usage, and fstab:

```bash
findmnt -R -T /home/python/Quote/data_bak
findmnt -R -T /home/python/sd
lsblk -f /dev/sda3
df -h /home/python/Quote/data /home/python/Quote/data_bak /home/python/sd
cat /etc/fstab
```

4. Check database integrity before copying:

```bash
sqlite3 /home/python/Quote/data_bak/quotes.db "PRAGMA integrity_check;"
sqlite3 /home/python/Quote/data_bak/research.db "PRAGMA integrity_check;"
```

## Execution

The current operator-prepared source directory is
`/home/python/Quote/data_bak`, and `/home/python/Quote/data` is an empty mount
point directory. `/home/python/sd` is only the old existing mount point for
`/dev/sda3`; it is used as a temporary staging path only because a filesystem
must be accessed through a mount point before it can be remounted at
`/home/python/Quote/data`. It is not a target application path and should be
removed after the final switch.

1. Unmount NAS child mounts before copying local data:

```bash
sudo umount /home/python/Quote/data_bak/PVE-Bak
sudo umount /home/python/Quote/data_bak/QuoteBak
```

2. Checkpoint SQLite WAL files while all writers are stopped:

```bash
sqlite3 /home/python/Quote/data_bak/quotes.db "PRAGMA wal_checkpoint(TRUNCATE); PRAGMA integrity_check;"
sqlite3 /home/python/Quote/data_bak/research.db "PRAGMA wal_checkpoint(TRUNCATE); PRAGMA integrity_check;"
```

3. Clear `/dev/sda3` only after operator confirmation. If it is mounted at
   `/home/python/sd`, preserve the filesystem UUID and remove old contents:

```bash
find /home/python/sd -mindepth 1 -xdev -exec rm -rf -- {} +
```

4. Copy local Quote data only. Do not copy NAS contents:

```bash
rsync -aHAX --numeric-ids --one-file-system /home/python/Quote/data_bak/ /home/python/sd/
mkdir -p /home/python/sd/PVE-Bak /home/python/sd/QuoteBak /home/python/sd/filings
```

If `rsync` is not installed in the runtime environment, use GNU `cp` with the
same filesystem boundary rule:

```bash
cp -a -x /home/python/Quote/data_bak/. /home/python/sd/
mkdir -p /home/python/sd/PVE-Bak /home/python/sd/QuoteBak /home/python/sd/filings
```

5. Preserve the old root-filesystem copy for rollback:

```bash
mv /home/python/Quote/data_bak /home/python/Quote/data.rootfs.bak
mkdir -p /home/python/Quote/data
```

6. Back up and update `/etc/fstab`. Use the UUID shown by `lsblk -f /dev/sda3`.

```fstab
UUID=<sda3-uuid>  /home/python/Quote/data  ext4  defaults,noatime  0  2
192.168.188.88:/volume2/PVE-Bak  /home/python/Quote/data/PVE-Bak  nfs  defaults,_netdev,nofail,x-systemd.automount,x-systemd.requires-mounts-for=/home/python/Quote/data  0  0
192.168.188.68:/export/HDD-2/QuoteBak  /home/python/Quote/data/QuoteBak  nfs  defaults,_netdev,nofail,x-systemd.automount,x-systemd.requires-mounts-for=/home/python/Quote/data  0  0
```

7. Remount and verify:

```bash
sudo systemctl daemon-reload
sudo umount /home/python/sd
sudo mount /home/python/Quote/data
sudo mount /home/python/Quote/data/PVE-Bak
sudo mount /home/python/Quote/data/QuoteBak
rmdir /home/python/sd
findmnt -R -T /home/python/Quote/data
```

## Validation

1. Confirm `/home/python/Quote/data` is backed by `/dev/sda3` or its UUID.
2. Confirm both NAS paths are backed by their NFS filesystems.
3. Run post-migration integrity checks:

```bash
sqlite3 /home/python/Quote/data/quotes.db "PRAGMA integrity_check;"
sqlite3 /home/python/Quote/data/research.db "PRAGMA integrity_check;"
```

4. Run smoke checks for quote reads and research readiness using
   `scripts/quote_data_volume_validate.py`.
5. Dry-run or write-test the backup path under `data/PVE-Bak/QuoteBak`.

## Rollback

1. Stop services again.
2. Unmount child NAS mounts:

```bash
sudo umount /home/python/Quote/data/PVE-Bak
sudo umount /home/python/Quote/data/QuoteBak
```

3. Unmount the local data volume and restore the previous rootfs directory:

```bash
sudo umount /home/python/Quote/data
mv /home/python/Quote/data /home/python/Quote/data.empty-mountpoint
mv /home/python/Quote/data.rootfs.bak /home/python/Quote/data
```

4. Restore the saved `/etc/fstab`, reload systemd, and remount the NAS shares.
5. Keep `data.rootfs.bak` until at least one or two successful daily update
   cycles complete.

## Troubleshooting

- Missing NAS mount: confirm the local data volume is mounted first, then run
  `mount /home/python/Quote/data/PVE-Bak` and
  `mount /home/python/Quote/data/QuoteBak`.
- Failed `mount -a`: restore the previous fstab backup before rebooting.
- SQLite integrity failure: do not start services; roll back to
  `data.rootfs.bak` and inspect the copied database and source database.
- Backup writes into a local directory: stop the backup task and fix the NAS
  mount before running a full database backup.

## Cleanup

Do not clean migration leftovers immediately. Use
`docs/development/quote_data_volume_cleanup.md` after the system has run
normally for at least three days and at least one post-migration update/backup
cycle has succeeded.
