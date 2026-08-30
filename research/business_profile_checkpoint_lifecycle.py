"""Owned-file operations for business-profile async checkpoints."""

from __future__ import annotations

from pathlib import Path


CHECKPOINT_FILE_PREFIX = "bp-work-"
CHECKPOINT_FILE_SUFFIX = ".json"


def owned_checkpoint_path(
    path: str | Path,
    *,
    checkpoint_root: str | Path,
) -> Path | None:
    """Return a normalized owned checkpoint path, or ``None`` if unsafe."""

    root = Path(checkpoint_root).resolve(strict=False)
    candidate = Path(path).resolve(strict=False)
    if candidate.parent != root:
        return None
    if not candidate.name.startswith(CHECKPOINT_FILE_PREFIX):
        return None
    if candidate.suffix != CHECKPOINT_FILE_SUFFIX:
        return None
    return candidate


def list_owned_checkpoint_files(checkpoint_root: str | Path) -> tuple[Path, ...]:
    """List only direct async work checkpoints; control files are excluded."""

    root = Path(checkpoint_root)
    if not root.is_dir():
        return ()
    return tuple(
        sorted(
            (
                candidate.resolve(strict=False)
                for candidate in root.glob(f"{CHECKPOINT_FILE_PREFIX}*{CHECKPOINT_FILE_SUFFIX}")
                if candidate.is_file()
            ),
            key=str,
        )
    )


def delete_owned_checkpoint_file(
    path: str | Path,
    *,
    checkpoint_root: str | Path,
) -> bool:
    """Delete one validated checkpoint and report whether a file was removed."""

    candidate = owned_checkpoint_path(path, checkpoint_root=checkpoint_root)
    if candidate is None:
        raise ValueError("checkpoint path is outside the owned async checkpoint root")
    if not candidate.exists():
        return False
    if not candidate.is_file():
        raise ValueError("owned checkpoint path is not a regular file")
    candidate.unlink()
    return True
