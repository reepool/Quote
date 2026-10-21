"""Shared nodriver session startup with Chrome leak reclamation.

nodriver's Browser.create uses a fixed ~3s DevTools connect budget that no
config exposes, and it registers the Browser object globally before that loop
succeeds. When Chrome's cold start outlives the budget (momentary load is
enough), the spawned Chrome tree stays alive with no owner reference. Every
start here therefore uses a unique user-data-dir; on failure the leaked
processes are reclaimed through that dir and the start is retried. Callers own
the returned dir and must reclaim it on close via reclaim_nodriver_user_data_dir.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)


def kill_browser_processes_for_user_data_dir(user_data_dir: str) -> int:
    """Terminate browser processes still bound to a nodriver profile directory.

    Returns the number of processes that actually received a termination
    signal; processes that vanished between the scan and the signal are not
    counted.
    """
    try:
        import psutil
    except ImportError:  # pragma: no cover - psutil ships with the runtime env
        return 0
    marker = f"--user-data-dir={user_data_dir}"
    targets = [
        proc
        for proc in psutil.process_iter(["cmdline"])
        if (proc.info or {}).get("cmdline") and marker in proc.info["cmdline"]
    ]
    signalled = 0
    for proc in targets:
        try:
            proc.terminate()
            signalled += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    if targets:
        psutil.wait_procs(targets, timeout=3.0)
        for proc in targets:
            try:
                if proc.is_running():
                    proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    return signalled


def reclaim_nodriver_user_data_dir(user_data_dir: str) -> int:
    """Stop stray browser processes for the dir and remove the dir itself."""
    signalled = kill_browser_processes_for_user_data_dir(user_data_dir)
    shutil.rmtree(user_data_dir, ignore_errors=True)
    return signalled


async def start_nodriver_with_reclaim(
    *,
    start_timeout_seconds: float,
    retry_attempts: int = 2,
    retry_backoff_seconds: float = 2.0,
    user_data_dir_prefix: str = "uc_nodriver_",
    start_logger: Optional[logging.Logger] = None,
    log_prefix: str = "[NodriverSession]",
    **start_kwargs: Any,
) -> Tuple[Any, str]:
    """Start nodriver with per-attempt leak reclamation.

    Returns (browser, user_data_dir). Raises the last start error once all
    attempts are exhausted; each failed attempt's leaked Chrome processes and
    profile dir are reclaimed before the next try.
    """
    try:
        import nodriver as uc
    except Exception as exc:  # pragma: no cover - optional dependency path
        raise RuntimeError("nodriver is required for headed browser sessions") from exc
    log = start_logger or logger
    total_attempts = 1 + max(0, int(retry_attempts))
    start_error: Optional[BaseException] = None
    for attempt in range(1, total_attempts + 1):
        user_data_dir = tempfile.mkdtemp(prefix=user_data_dir_prefix)
        try:
            browser = await asyncio.wait_for(
                uc.start(user_data_dir=user_data_dir, **start_kwargs),
                timeout=start_timeout_seconds,
            )
        except Exception as exc:
            start_error = exc
            reclaimed = reclaim_nodriver_user_data_dir(user_data_dir)
            log.warning(
                "%s browser start failed attempt=%s/%s reclaimed_processes=%s "
                "error=%s: %s",
                log_prefix,
                attempt,
                total_attempts,
                reclaimed,
                type(exc).__name__,
                exc,
            )
            if attempt < total_attempts:
                await asyncio.sleep(min(retry_backoff_seconds * attempt, 5.0))
            continue
        return browser, user_data_dir
    if start_error is None:  # pragma: no cover - loop always records the failure
        start_error = RuntimeError("nodriver start failed without error")
    raise start_error
