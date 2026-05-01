#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/home/python/Quote"
DEVICE="/dev/sda3"
OLD_STAGE="/home/python/sd"
DATA_DIR="${PROJECT_ROOT}/data"
ROOTFS_BACKUP="${PROJECT_ROOT}/data.rootfs.bak"
OPERATOR_BACKUP="${PROJECT_ROOT}/data_bak"
PVE_NAS="192.168.188.88:/volume2/PVE-Bak"
QUOTE_NAS="192.168.188.68:/export/HDD-2/QuoteBak"

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: this script must run as root because it edits /etc/fstab and changes mounts." >&2
  exit 1
fi

if [ ! -f "${OLD_STAGE}/quotes.db" ] || [ ! -f "${OLD_STAGE}/research.db" ]; then
  echo "ERROR: staged databases are missing under ${OLD_STAGE}; aborting." >&2
  exit 1
fi

if [ -e "${ROOTFS_BACKUP}" ]; then
  echo "ERROR: ${ROOTFS_BACKUP} already exists; inspect it before continuing." >&2
  exit 1
fi

if [ ! -d "${OPERATOR_BACKUP}" ]; then
  echo "ERROR: ${OPERATOR_BACKUP} is missing; expected current rootfs backup source." >&2
  exit 1
fi

if findmnt -T "${OPERATOR_BACKUP}/PVE-Bak" >/dev/null 2>&1; then
  umount "${OPERATOR_BACKUP}/PVE-Bak"
fi

if findmnt -T "${OPERATOR_BACKUP}/QuoteBak" >/dev/null 2>&1; then
  umount "${OPERATOR_BACKUP}/QuoteBak"
fi

SDA3_UUID="$(lsblk -no UUID "${DEVICE}" | head -n 1 | tr -d '[:space:]')"
if [ -z "${SDA3_UUID}" ]; then
  echo "ERROR: failed to read UUID for ${DEVICE}." >&2
  exit 1
fi

FSTAB_BACKUP="/etc/fstab.quote-migration-$(date +%Y%m%d%H%M%S).bak"
cp /etc/fstab "${FSTAB_BACKUP}"

awk '
  $2 == "/home/python/sd" { next }
  $2 == "/home/python/Quote/data/PVE-Bak" { next }
  $2 == "/home/python/Quote/data/QuoteBak" { next }
  { print }
' /etc/fstab > /etc/fstab.quote-migration.new

cat >> /etc/fstab.quote-migration.new <<EOF
UUID=${SDA3_UUID}  /home/python/Quote/data  ext4  defaults,noatime  0  2
${PVE_NAS}  /home/python/Quote/data/PVE-Bak  nfs  defaults,_netdev,nofail,x-systemd.automount,x-systemd.requires-mounts-for=/home/python/Quote/data  0  0
${QUOTE_NAS}  /home/python/Quote/data/QuoteBak  nfs  defaults,_netdev,nofail,x-systemd.automount,x-systemd.requires-mounts-for=/home/python/Quote/data  0  0
EOF

cp /etc/fstab.quote-migration.new /etc/fstab
rm -f /etc/fstab.quote-migration.new

mv "${OPERATOR_BACKUP}" "${ROOTFS_BACKUP}"
mkdir -p "${DATA_DIR}"

if findmnt -T "${OLD_STAGE}" >/dev/null 2>&1; then
  umount "${OLD_STAGE}"
fi

systemctl daemon-reload || true
mount "${DATA_DIR}"
mkdir -p "${DATA_DIR}/PVE-Bak" "${DATA_DIR}/QuoteBak" "${DATA_DIR}/filings"
mount "${DATA_DIR}/PVE-Bak" || true
mount "${DATA_DIR}/QuoteBak" || true

if [ -d "${OLD_STAGE}" ]; then
  rmdir "${OLD_STAGE}" 2>/dev/null || echo "WARN: ${OLD_STAGE} is not empty; leave it for manual inspection." >&2
fi

findmnt -R -T "${DATA_DIR}"
df -h "${DATA_DIR}" "${DATA_DIR}/PVE-Bak" "${DATA_DIR}/QuoteBak" || true

sqlite3 "${DATA_DIR}/quotes.db" "SELECT 'latest_quote', time, instrument_id, close FROM daily_quotes ORDER BY time DESC LIMIT 1;"
sqlite3 "${DATA_DIR}/research.db" "SELECT 'shareholder_snapshots', COUNT(*) FROM shareholder_snapshots; SELECT 'industry_memberships', COUNT(*) FROM industry_memberships;"

echo "fstab backup: ${FSTAB_BACKUP}"
echo "rootfs data backup: ${ROOTFS_BACKUP}"
