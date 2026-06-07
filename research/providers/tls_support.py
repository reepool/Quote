"""TLS helpers for source providers with incomplete upstream certificate chains."""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path
from typing import Optional, Union

import certifi

from utils import dm_logger


VerifySetting = Union[bool, str]


def build_ca_bundle_with_extra_certificate(extra_certificate_path: Optional[str]) -> VerifySetting:
    """Return a requests ``verify`` setting that keeps certifi plus one extra CA cert."""
    if not extra_certificate_path:
        return True

    extra_path = Path(extra_certificate_path).expanduser()
    if not extra_path.is_absolute():
        cwd_path = Path.cwd() / extra_path
        project_path = Path(__file__).resolve().parents[2] / extra_path
        extra_path = cwd_path if cwd_path.exists() else project_path
    if not extra_path.exists():
        dm_logger.warning("[TLS] Extra CA certificate not found: %s", extra_path)
        return True

    certifi_path = Path(certifi.where())
    bundle_key = hashlib.sha256(
        f"{certifi_path}:{certifi_path.stat().st_mtime_ns}:"
        f"{extra_path}:{extra_path.stat().st_mtime_ns}".encode("utf-8")
    ).hexdigest()[:16]
    bundle_path = Path(tempfile.gettempdir()) / f"quote_ca_bundle_{bundle_key}.pem"
    if bundle_path.exists():
        return str(bundle_path)

    certifi_content = certifi_path.read_text(encoding="utf-8")
    extra_content = extra_path.read_text(encoding="utf-8").strip()
    bundle_path.write_text(
        certifi_content.rstrip() + "\n\n" + extra_content + "\n",
        encoding="utf-8",
    )
    return str(bundle_path)
