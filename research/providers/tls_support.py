"""Compatibility wrappers for provider TLS helpers."""

from __future__ import annotations

from utils.http_transport import (
    VerifySetting,
    build_ca_bundle_with_extra_certificate,
)

__all__ = ["VerifySetting", "build_ca_bundle_with_extra_certificate"]
