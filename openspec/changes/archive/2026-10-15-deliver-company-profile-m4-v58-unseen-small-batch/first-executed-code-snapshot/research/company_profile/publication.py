"""Research-publication scope switch for company-profile common-core.

This is the unique owner for company_profile_research_publication.v1. It
enables only the new-contract writer/reader for company facts and commodity
associations. Pause and rollback stop new official writes, keep saved
records, and never re-enable the legacy writer, DCF, or trading. Production
authorization stays not_authorized.
"""

from __future__ import annotations

import fcntl
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, model_validator

from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.runtime import (
    COMMON_CORE_STORAGE_NAMESPACE,
    COMMON_CORE_WRITER_NAME,
)

PUBLICATION_SCHEMA_VERSION = "company_profile_research_publication.v1"
PUBLICATION_READER = "company_profile_read_service.v1"
ENABLED_PUBLICATION_SCOPES = ("company_facts", "commodity_associations")
PublicationState = Literal["disabled", "enabled", "paused", "rolled_back"]
PublicationAction = Literal["enable", "pause", "resume", "rollback"]
_ALLOWED_NEXT: dict[PublicationState, frozenset[PublicationAction]] = {
    "disabled": frozenset({"enable"}),
    "enabled": frozenset({"pause", "rollback", "enable"}),
    "paused": frozenset({"resume", "rollback"}),
    "rolled_back": frozenset({"enable", "rollback"}),
}


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class CompanyProfilePublicationControl(_StrictModel):
    schema_version: Literal["company_profile_research_publication.v1"] = (
        PUBLICATION_SCHEMA_VERSION
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    state: PublicationState
    writer: Literal["company_profile_research_writer.v1"] = COMMON_CORE_WRITER_NAME
    reader: Literal["company_profile_read_service.v1"] = PUBLICATION_READER
    storage_namespace: Literal["company_profile_common_core.v1"] = (
        COMMON_CORE_STORAGE_NAMESPACE
    )
    declared_scopes: tuple[Literal["company_facts", "commodity_associations"], ...]
    active_scopes: tuple[Literal["company_facts", "commodity_associations"], ...]
    automatic_research_publication: bool
    allows_new_writes: bool
    retains_saved_records: Literal[True]
    legacy_writer_enabled: Literal[False]
    dcf_authorized: Literal[False]
    trading_authorized: Literal[False]
    price_sensitivity_authorized: Literal[False]

    @model_validator(mode="after")
    def _scope_switch_stays_bounded(self) -> CompanyProfilePublicationControl:
        if self.production_authorization != "not_authorized":
            raise ValueError("research publication cannot authorize production")
        if self.legacy_writer_enabled:
            raise ValueError("research publication cannot enable the legacy writer")
        if (
            self.dcf_authorized
            or self.trading_authorized
            or self.price_sensitivity_authorized
        ):
            raise ValueError("research publication cannot authorize DCF or trading")
        if self.declared_scopes != ENABLED_PUBLICATION_SCOPES:
            raise ValueError(
                "research publication can only declare company facts and "
                "commodity associations"
            )
        if self.writer != COMMON_CORE_WRITER_NAME:
            raise ValueError("research publication must keep the new-contract writer")
        if self.reader != PUBLICATION_READER:
            raise ValueError("research publication must keep the new-contract reader")
        if self.state == "enabled":
            if (
                not self.automatic_research_publication
                or not self.allows_new_writes
                or self.active_scopes != ENABLED_PUBLICATION_SCOPES
            ):
                raise ValueError("enabled publication must open the declared scopes")
        else:
            if self.automatic_research_publication or self.active_scopes:
                raise ValueError("inactive publication cannot keep scopes open")
            if self.state in {"paused", "rolled_back"} and self.allows_new_writes:
                raise ValueError("paused or rolled-back publication cannot accept writes")
            if self.state == "disabled" and not self.allows_new_writes:
                raise ValueError("disabled publication must keep research writes open")
        if not self.retains_saved_records:
            raise ValueError("publication rollback must retain saved records")
        return self


def default_publication_control() -> CompanyProfilePublicationControl:
    """Research writes stay available before the scope switch is opened."""

    return _control(state="disabled")


def record_publication_control(
    action: str,
    current: CompanyProfilePublicationControl | None = None,
) -> CompanyProfilePublicationControl:
    """Apply one publication-scope action without enabling legacy writers."""

    normalized = str(action or "").strip().lower()
    if normalized not in {"enable", "pause", "resume", "rollback"}:
        raise ValueError(f"unsupported research publication action {action!r}")
    current = current or default_publication_control()
    if normalized not in _ALLOWED_NEXT[current.state]:
        raise ValueError(
            f"cannot {normalized} research publication from {current.state}"
        )
    if normalized == "enable":
        return _control(state="enabled")
    if normalized == "pause":
        return _control(state="paused")
    if normalized == "resume":
        return _control(state="enabled")
    return _control(state="rolled_back")


def publication_allows_new_writes(
    control: CompanyProfilePublicationControl | None,
) -> bool:
    """Official run stays open until publication is paused or rolled back."""

    if control is None:
        return True
    return control.allows_new_writes


def persist_publication_control(
    control: CompanyProfilePublicationControl,
    root: str | Path,
) -> Path:
    """Write company_profile_research_publication.v1 next to live-run reports."""

    path = Path(root) / "reports" / f"{PUBLICATION_SCHEMA_VERSION}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with _publication_lock(root):
        tmp = path.with_suffix(".tmp")
        tmp.write_text(control.model_dump_json(indent=2), encoding="utf-8")
        tmp.replace(path)
    return path


def commit_research_profile_write(
    root: str | Path,
    write: Callable[[], None],
) -> bool:
    """Commit a profile write only if publication still allows it.

    The publication switch and the profile writer share one exclusive lock, so
    a completed pause or rollback cannot be followed by a new visible write.
    """

    with _publication_lock(root):
        if not publication_allows_new_writes(load_publication_control(root)):
            return False
        write()
        return True


def load_publication_control(
    root: str | Path,
) -> CompanyProfilePublicationControl | None:
    """Load the persisted publication switch, if the operator has set one."""

    path = Path(root) / "reports" / f"{PUBLICATION_SCHEMA_VERSION}.json"
    if not path.is_file():
        return None
    return CompanyProfilePublicationControl.model_validate_json(
        path.read_text(encoding="utf-8")
    )


def publication_schema_manifest() -> dict[str, Any]:
    """Register the publication-scope schema without authorizing production."""

    return {
        "schema_version": PUBLICATION_SCHEMA_VERSION,
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "writer": COMMON_CORE_WRITER_NAME,
        "reader": PUBLICATION_READER,
        "declared_scopes": list(ENABLED_PUBLICATION_SCOPES),
        "legacy_writer_enabled": False,
        "dcf_authorized": False,
        "trading_authorized": False,
        "control_schema": CompanyProfilePublicationControl.model_json_schema(),
    }


@contextmanager
def _publication_lock(root: str | Path) -> Iterator[None]:
    path = Path(root) / "reports" / f"{PUBLICATION_SCHEMA_VERSION}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _control(*, state: PublicationState) -> CompanyProfilePublicationControl:
    enabled = state == "enabled"
    disabled = state == "disabled"
    return CompanyProfilePublicationControl(
        state=state,
        declared_scopes=ENABLED_PUBLICATION_SCOPES,
        active_scopes=ENABLED_PUBLICATION_SCOPES if enabled else (),
        automatic_research_publication=enabled,
        allows_new_writes=enabled or disabled,
        retains_saved_records=True,
        legacy_writer_enabled=False,
        dcf_authorized=False,
        trading_authorized=False,
        price_sensitivity_authorized=False,
    )
