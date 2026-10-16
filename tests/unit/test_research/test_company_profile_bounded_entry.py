"""Entry scope includes blocking source work; authorization precedes real calls."""

import subprocess
import sys
import time

import pytest

from research.company_profile.bounded_entry import digest, supervise, validate_binding


def test_whole_entry_deadline_kills_blocking_worker_and_its_child(tmp_path):
    child_pid = tmp_path / "child.pid"
    code = (
        "import subprocess,sys,time; from pathlib import Path; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
        f"Path({str(child_pid)!r}).write_text(str(p.pid)); time.sleep(60)"
    )
    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        supervise(
            [sys.executable, "-c", code],
            deadline=started + 0.5,
            log_path=tmp_path / "entry.log",
        )
    assert time.monotonic() - started < 3
    # A killed descendant may remain a zombie until the container reaps it.
    from pathlib import Path

    state = Path(f"/proc/{int(child_pid.read_text())}/stat")
    if state.exists():
        assert state.read_text().split()[2] in {"Z", "X"}


def test_real_binding_requires_price_feasibility_and_exact_A_authorization(tmp_path):
    source = tmp_path / "source.pdf"
    source.write_bytes(b"offline source")
    binding = {
        "file_hashes": {},
        "asset": {"archive_path": str(source), "content_hash": digest(source)},
        "max_elapsed_seconds": 900,
    }
    validate_binding(binding, real=False, authorization={}, binding_hash="frozen")
    with pytest.raises(ValueError, match="authorization"):
        validate_binding(binding, real=True, authorization={}, binding_hash="frozen")
    authorization = {
        "status": "authorized",
        "binding_sha256": "frozen",
        "document_version": digest(source),
        "attempt": "first",
        "approval_reference": "unit-test-only",
    }
    with pytest.raises(ValueError, match="price/context"):
        validate_binding(
            binding, real=True, authorization=authorization, binding_hash="frozen"
        )
    binding.update(price={"basis": "synthetic"}, resource_feasibility_passed=True)
    validate_binding(
        binding, real=True, authorization=authorization, binding_hash="frozen"
    )
    authorization["binding_sha256"] = "stale"
    with pytest.raises(ValueError, match="authorization"):
        validate_binding(
            binding, real=True, authorization=authorization, binding_hash="frozen"
        )


def test_changed_code_cannot_use_frozen_execution_binding(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"v1")
    binding = {"file_hashes": {str(source): digest(source)}}
    source.write_bytes(b"v2")
    with pytest.raises(ValueError, match="changed"):
        validate_binding(binding, real=False, authorization={}, binding_hash="test")
