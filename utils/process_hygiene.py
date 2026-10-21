"""Detect and clean orphaned browser-session processes and stale profile dirs.

Headed browser sessions (nodriver Chrome, chromedriver, Xvfb) are owned by the
python process that spawned them. When the owner dies or a browser start fails
midway, the survivors are reparented to init: they serve nobody, hold memory,
and slow down later browser startups. The hourly system health check calls
`sweep_orphan_browser_artifacts`; the sweep never touches target processes that
still have a live parent.
"""

from __future__ import annotations

import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

import psutil

logger = logging.getLogger(__name__)

# Name fragments identifying browser-session helper processes. Matched by
# substring because Chrome ships as "chrome", "chrome_crashpad_handler",
# "chromium", and renderers share the main binary name.
TARGET_NAME_PARTS = (
    "chrome",
    "chromium",
    "chromedriver",
    "xvfb",
    "geckodriver",
    "phantomjs",
)

# nodriver creates its temp profile dirs with this prefix; the DCE browser
# client uses "uc_dce_" which matches it too.
PROFILE_DIR_PREFIX = "uc_"


def _is_target_process(name: str) -> bool:
    lowered = (name or "").lower()
    return any(part in lowered for part in TARGET_NAME_PARTS)


def classify_orphan_processes(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Split target-process rows into orphans (ppid==1) and live sessions.

    `rows` are psutil info dicts with pid, ppid, name, status, and rss bytes.
    Orphans are processes whose owning parent has died (reparented to init);
    live sessions keep their ppid so callers can report them by owner.
    """
    orphans: List[Dict[str, Any]] = []
    live_by_parent: Dict[str, int] = {}
    live_count = 0
    for row in rows:
        if not _is_target_process(str(row.get("name") or "")):
            continue
        ppid = int(row.get("ppid") or 0)
        if ppid <= 0:
            continue
        if ppid == 1:
            # zombie orphans have nothing left to kill; live sessions of a
            # dead owner are the reclamation targets
            if row.get("status") != psutil.STATUS_ZOMBIE:
                orphans.append(row)
            continue
        live_count += 1
        live_by_parent[str(ppid)] = live_by_parent.get(str(ppid), 0) + 1
    rss_freed_bytes = sum(int(row.get("rss_bytes") or 0) for row in orphans)
    return {
        "orphans": orphans,
        "live_target_count": live_count,
        "live_by_parent": live_by_parent,
        "rss_freed_bytes": rss_freed_bytes,
    }


def stale_profile_dirs(
    profile_dirs: Iterable[str],
    referenced_tokens: Set[str],
) -> List[str]:
    """Return profile dirs no running process references via --user-data-dir.

    nodriver removes its temp profiles on clean close only; crashed or killed
    sessions leave the dirs behind forever. `referenced_tokens` is the set of
    all argv tokens of running processes, so an exact-token comparison avoids
    prefix false positives between uc_dce_a and uc_dce_ab.
    """
    stale: List[str] = []
    for profile_dir in profile_dirs:
        marker = f"--user-data-dir={profile_dir}"
        if marker not in referenced_tokens:
            stale.append(profile_dir)
    return stale


def sweep_orphan_browser_artifacts() -> Dict[str, Any]:
    """Reclaim orphaned browser processes and unreferenced nodriver profiles."""
    rows: List[Dict[str, Any]] = []
    cmdline_tokens: Set[str] = set()
    for proc in psutil.process_iter(
        ["pid", "ppid", "name", "status", "memory_info", "cmdline"]
    ):
        info = proc.info or {}
        try:
            cmdline = info.get("cmdline") or []
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            cmdline = []
        for token in cmdline:
            if isinstance(token, str):
                cmdline_tokens.add(token)
        memory = info.get("memory_info")
        rows.append(
            {
                "pid": info.get("pid"),
                "ppid": info.get("ppid"),
                "name": info.get("name"),
                "status": info.get("status"),
                "rss_bytes": int(getattr(memory, "rss", 0) or 0),
                "_proc": proc,
            }
        )
    classification = classify_orphan_processes(rows)
    orphans = classification["orphans"]

    killed = 0
    for row in orphans:
        proc = row.get("_proc")
        try:
            proc.terminate()
            killed += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    if orphans:
        psutil.wait_procs(
            [row["_proc"] for row in orphans if row.get("_proc")], timeout=3.0
        )
        for row in orphans:
            proc = row.get("_proc")
            try:
                if proc.is_running():
                    proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

    temp_root = Path(tempfile.gettempdir())
    try:
        candidates = [
            str(entry)
            for entry in temp_root.glob(f"{PROFILE_DIR_PREFIX}*")
            if entry.is_dir()
        ]
    except OSError:
        candidates = []
    stale = stale_profile_dirs(candidates, cmdline_tokens)
    for profile_dir in stale:
        shutil.rmtree(profile_dir, ignore_errors=True)

    samples = [
        f"pid={row.get('pid')} name={row.get('name')} rss_mb={row.get('rss_bytes', 0) // (1024 * 1024)}"
        for row in orphans[:10]
    ]
    result = {
        "orphans_found": len(orphans),
        "orphans_killed": killed,
        "rss_freed_mb": round(
            classification["rss_freed_bytes"] / (1024 * 1024), 1
        ),
        "live_target_count": classification["live_target_count"],
        "live_by_parent": classification["live_by_parent"],
        "stale_profile_dirs_removed": len(stale),
        "samples": samples,
    }
    if orphans or stale:
        logger.warning("[ProcessHygiene] browser leak sweep result: %s", result)
    return result
