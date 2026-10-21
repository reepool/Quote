"""Tests for utils.process_hygiene orphan browser-process sweeping."""

import types

from utils.process_hygiene import (
    PROFILE_DIR_PREFIX,
    classify_orphan_processes,
    stale_profile_dirs,
    sweep_orphan_browser_artifacts,
)


def _row(pid, ppid, name, status="sleeping", rss_bytes=0):
    return {
        "pid": pid,
        "ppid": ppid,
        "name": name,
        "status": status,
        "rss_bytes": rss_bytes,
    }


def test_classify_separates_orphans_from_live_sessions():
    rows = [
        _row(11, 1, "chrome", rss_bytes=100 * 1024 * 1024),
        _row(12, 1, "chrome_crashpad_handler"),
        _row(13, 500, "chrome"),
        _row(14, 500, "Xvfb"),
        _row(15, 1, "python"),
        _row(16, 1, "chrome", status="zombie"),
    ]
    result = classify_orphan_processes(rows)
    assert [row["pid"] for row in result["orphans"]] == [11, 12]
    assert result["live_target_count"] == 2
    assert result["live_by_parent"] == {"500": 2}
    assert result["rss_freed_bytes"] == 100 * 1024 * 1024


def test_classify_ignores_non_target_processes():
    rows = [_row(21, 1, "python3"), _row(22, 1, "node"), _row(23, 900, "chromium")]
    result = classify_orphan_processes(rows)
    assert result["orphans"] == []
    assert result["live_target_count"] == 1


def test_stale_profile_dirs_requires_exact_token_reference():
    referenced = {
        "--user-data-dir=/tmp/uc_dce_abc",
        "--user-data-dir=/tmp/uc_keep",
    }
    stale = stale_profile_dirs(
        ["/tmp/uc_dce_abc", "/tmp/uc_dce_abcd", "/tmp/uc_old"],
        referenced,
    )
    assert stale == ["/tmp/uc_dce_abcd", "/tmp/uc_old"]


def test_sweep_kills_orphans_and_removes_stale_dirs(monkeypatch, tmp_path):
    class FakeProc:
        def __init__(self, pid, kill_should_fail=False):
            self.pid = pid
            self.terminated = False
            self.killed = False
            self._running = not kill_should_fail

        def terminate(self):
            self.terminated = True

        def is_running(self):
            return self._running

        def kill(self):
            self.killed = True
            self._running = False

    orphan = FakeProc(31)
    live = FakeProc(32)
    keep_dir = tmp_path / f"{PROFILE_DIR_PREFIX}keep"
    stale_dir = tmp_path / f"{PROFILE_DIR_PREFIX}stale"
    keep_dir.mkdir()
    stale_dir.mkdir()

    def fake_process_iter(attrs=None):
        def make(pid, ppid, name, proc):
            memory = types.SimpleNamespace(rss=50 * 1024 * 1024)
            info = {
                "pid": pid,
                "ppid": ppid,
                "name": name,
                "status": "sleeping",
                "memory_info": memory,
                "cmdline": ["chrome", f"--user-data-dir={keep_dir}"],
            }
            proc.info = info
            return proc

        return [
            make(31, 1, "chrome", orphan),
            make(32, 900, "chrome", live),
        ]

    def fake_wait_procs(procs, timeout=None):
        return [], procs

    monkeypatch.setattr("utils.process_hygiene.psutil.process_iter", fake_process_iter)
    monkeypatch.setattr("utils.process_hygiene.psutil.wait_procs", fake_wait_procs)
    monkeypatch.setattr("utils.process_hygiene.tempfile.gettempdir", lambda: str(tmp_path))

    result = sweep_orphan_browser_artifacts()

    assert result["orphans_killed"] == 1
    assert result["live_target_count"] == 1
    assert result["stale_profile_dirs_removed"] == 1
    assert keep_dir.exists()
    assert not stale_dir.exists()
    assert orphan.terminated
    assert not live.terminated
