"""Typed validation for filesystem path segments owned by announcement assets."""

from __future__ import annotations

import re
from typing import Literal

PathSegmentKind = Literal["sha256", "identifier", "configured_name"]

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_ENCODED_PATH_ESCAPE_RE = re.compile(r"%(?:2f|5c|2e)", re.IGNORECASE)


def validate_path_segment(
    value: object,
    *,
    kind: PathSegmentKind,
    field_name: str,
) -> str:
    """Return one canonical segment or fail before path construction."""

    segment = str(value)
    if not segment or segment != segment.strip() or segment in {".", ".."}:
        raise ValueError(f"{field_name} contains an empty or traversal path segment")
    if "/" in segment or "\\" in segment:
        raise ValueError(f"{field_name} contains a path separator")
    if any(ord(character) < 32 or ord(character) == 127 for character in segment):
        raise ValueError(f"{field_name} contains a control-character path segment")
    if _ENCODED_PATH_ESCAPE_RE.search(segment):
        raise ValueError(f"{field_name} contains an encoded traversal path segment")
    if kind == "sha256":
        if not _SHA256_RE.fullmatch(segment):
            raise ValueError(
                f"{field_name} must be exactly 64 lowercase SHA-256 hexadecimal characters"
            )
        return segment
    if kind in {"identifier", "configured_name"}:
        if not _IDENTIFIER_RE.fullmatch(segment):
            raise ValueError(f"{field_name} contains a non-canonical path segment")
        return segment
    raise ValueError(f"unsupported path segment kind: {kind}")


def normalize_reason_segment(value: object, *, field_name: str) -> str:
    """Normalize display-like reason text while rejecting path-shaped input."""

    raw = str(value or "quarantine").strip()
    if any(token in raw for token in ("/", "\\", "..", "%")):
        raise ValueError(f"{field_name} contains an unsafe path segment")
    normalized = "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in raw
    )[:80]
    return validate_path_segment(
        normalized or "quarantine",
        kind="identifier",
        field_name=field_name,
    )
