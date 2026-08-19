"""Business-neutral contracts for official announcement assets."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

ANNOUNCEMENT_SCHEMA_VERSION = "official_announcement.v1"
ATTACHMENT_SCHEMA_VERSION = "official_announcement_attachment.v1"
BLOB_SCHEMA_VERSION = "official_document_blob.v1"
ATTACHMENT_VERSION_SCHEMA_VERSION = "official_attachment_version.v3"
EFFECTIVE_ANNUAL_REPORT_SCHEMA_VERSION = "effective_annual_report.v2"
EFFECTIVE_DECISION_SCHEMA_VERSION = "official_annual_report_decision.v1"
OPERATION_SCHEMA_VERSION = "official_asset_operation.v2"
OPERATION_STAGE_SCHEMA_VERSION = "official_asset_operation_stage.v1"
OPERATION_SUBSCRIPTION_SCHEMA_VERSION = "official_asset_operation_subscription.v2"
RETENTION_PIN_SCHEMA_VERSION = "official_asset_retention_pin.v1"
DISCOVERY_STATE_SCHEMA_VERSION = "official_asset_discovery_state.v2"
BOOTSTRAP_RUN_SCHEMA_VERSION = "official_asset_bootstrap_run.v1"
CHANGE_EVENT_SCHEMA_VERSION = "official_asset_change_event.v1"
CANONICAL_FILING_PROJECTION_POLICY_VERSION = "canonical_source_filing.v1"
CLASSIFICATION_VOCABULARY_VERSION = "official_document_classification.v1"
ACQUISITION_POLICY_SCHEMA_VERSION = "official_document_acquisition_policy.v1"


class _StringEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class AssetAvailability(_StringEnum):
    LOCAL_VALID = "local_valid"
    METADATA_ONLY = "metadata_only"
    MISSING = "missing"
    AMBIGUOUS = "ambiguous"
    CORRUPT = "corrupt"
    SUPERSEDED = "superseded"
    BLOCKED = "blocked"


class IntegrityStatus(_StringEnum):
    UNCHECKED = "unchecked"
    VALID = "valid"
    MISSING = "missing"
    UNREADABLE = "unreadable"
    NOT_PDF = "not_pdf"
    SIZE_MISMATCH = "size_mismatch"
    HASH_MISMATCH = "hash_mismatch"
    QUARANTINED = "quarantined"


class AnnualReportVariant(_StringEnum):
    ORIGINAL = "original"
    CORRECTION = "correction"


class DocumentFamily(_StringEnum):
    """Stable document-family vocabulary shared by providers and consumers."""

    ANNUAL_REPORT = "annual_report"
    SEMIANNUAL_REPORT = "semiannual_report"
    QUARTERLY_REPORT = "quarterly_report"
    OTHER = "other"


class DocumentFamilyAcquisitionScope(_StringEnum):
    """Attachment acquisition scope for a document family.

    The scope is deliberately independent from the annual-report scheduler.  A
    future family can register a policy without teaching the V1 annual job to
    download that family's attachments.
    """

    METADATA_ONLY = "metadata_only"
    BOUNDED_EXPLICIT_UNIVERSE = "bounded_explicit_universe"
    FULL_MARKET = "independently_governed_full_market"


def _policy_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} must be non-empty")
    return text


def _policy_parameters(value: Any, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    # Validate that the policy remains JSON-round-trippable.  ``default=str``
    # is intentionally not used here: policy fingerprints must not silently
    # turn arbitrary Python objects into unstable text.
    try:
        encoded = json.dumps(
            dict(value),
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{field_name} must contain JSON values") from exc
    return json.loads(encoded)


@dataclass(frozen=True)
class DocumentFamilyEffectiveVersionPolicy:
    """Family-owned effective-version selection rules.

    ``precedence_rules`` and ``parameters`` are intentionally family-specific
    data rather than annual-report assumptions.  They are persisted in the
    acquisition-policy fingerprint so changing a future family's winner rules
    cannot silently reuse incompatible work.
    """

    policy_version: str
    precedence_rules: tuple[str, ...]
    tie_break: str = "stable_legal_identity"
    conflict_policy: str = "fail_closed"
    parameters: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _policy_text(self.policy_version, "effective_version_policy.policy_version")
        rules = tuple(
            dict.fromkeys(
                _policy_text(item, "effective_version_policy.precedence_rules")
                for item in self.precedence_rules
            )
        )
        if not rules:
            raise ValueError(
                "effective_version_policy.precedence_rules must be non-empty"
            )
        object.__setattr__(self, "precedence_rules", rules)
        object.__setattr__(
            self,
            "tie_break",
            _policy_text(self.tie_break, "effective_version_policy.tie_break"),
        )
        object.__setattr__(
            self,
            "conflict_policy",
            _policy_text(
                self.conflict_policy, "effective_version_policy.conflict_policy"
            ),
        )
        object.__setattr__(
            self,
            "parameters",
            _policy_parameters(self.parameters, "effective_version_policy.parameters"),
        )

    def normalized_mapping(self) -> dict[str, Any]:
        return {
            "policy_version": self.policy_version,
            "precedence_rules": list(self.precedence_rules),
            "tie_break": self.tie_break,
            "conflict_policy": self.conflict_policy,
            "parameters": dict(self.parameters),
        }

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, Any]
    ) -> DocumentFamilyEffectiveVersionPolicy:
        raw = dict(value)
        allowed = {
            "policy_version",
            "precedence_rules",
            "tie_break",
            "conflict_policy",
            "parameters",
        }
        unknown = sorted(set(raw) - allowed)
        if unknown:
            raise ValueError(
                "effective_version_policy contains unknown fields: "
                + ", ".join(unknown)
            )
        rules = raw.get("precedence_rules", ())
        if isinstance(rules, (str, bytes)) or not isinstance(rules, (list, tuple)):
            raise TypeError(
                "effective_version_policy.precedence_rules must be an array"
            )
        return cls(
            policy_version=_policy_text(
                raw.get("policy_version"), "effective_version_policy.policy_version"
            ),
            precedence_rules=tuple(rules),
            tie_break=raw.get("tie_break", "stable_legal_identity"),
            conflict_policy=raw.get("conflict_policy", "fail_closed"),
            parameters=raw.get("parameters", {}),
        )


@dataclass(frozen=True)
class DocumentFamilyRetentionPolicy:
    """Family-owned retention and replacement rules for raw attachments."""

    policy_version: str
    mode: str
    retain_metadata: bool = True
    retain_superseded_bytes: str = "governed_recovery_only"
    max_effective_per_instrument_period: int = 1
    parameters: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _policy_text(self.policy_version, "retention_policy.policy_version")
        object.__setattr__(
            self, "mode", _policy_text(self.mode, "retention_policy.mode")
        )
        if type(self.retain_metadata) is not bool:
            raise TypeError("retention_policy.retain_metadata must be a boolean")
        object.__setattr__(
            self,
            "retain_superseded_bytes",
            _policy_text(
                self.retain_superseded_bytes,
                "retention_policy.retain_superseded_bytes",
            ),
        )
        if type(self.max_effective_per_instrument_period) is not int:
            raise TypeError(
                "retention_policy.max_effective_per_instrument_period must be an integer"
            )
        if self.max_effective_per_instrument_period <= 0:
            raise ValueError(
                "retention_policy.max_effective_per_instrument_period must be positive"
            )
        object.__setattr__(
            self,
            "parameters",
            _policy_parameters(self.parameters, "retention_policy.parameters"),
        )

    def normalized_mapping(self) -> dict[str, Any]:
        return {
            "policy_version": self.policy_version,
            "mode": self.mode,
            "retain_metadata": self.retain_metadata,
            "retain_superseded_bytes": self.retain_superseded_bytes,
            "max_effective_per_instrument_period": self.max_effective_per_instrument_period,
            "parameters": dict(self.parameters),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DocumentFamilyRetentionPolicy:
        raw = dict(value)
        allowed = {
            "policy_version",
            "mode",
            "retain_metadata",
            "retain_superseded_bytes",
            "max_effective_per_instrument_period",
            "parameters",
        }
        unknown = sorted(set(raw) - allowed)
        if unknown:
            raise ValueError(
                "retention_policy contains unknown fields: " + ", ".join(unknown)
            )
        return cls(
            policy_version=_policy_text(
                raw.get("policy_version"), "retention_policy.policy_version"
            ),
            mode=_policy_text(raw.get("mode"), "retention_policy.mode"),
            retain_metadata=raw.get("retain_metadata", True),
            retain_superseded_bytes=raw.get(
                "retain_superseded_bytes", "governed_recovery_only"
            ),
            max_effective_per_instrument_period=raw.get(
                "max_effective_per_instrument_period", 1
            ),
            parameters=raw.get("parameters", {}),
        )


@dataclass(frozen=True)
class DocumentFamilyAcquisitionPolicy:
    """Neutral, versioned acquisition policy for any announcement family.

    A policy is metadata-only by default and is never implicitly consulted by
    annual-report V1 maintenance.  Production proactive acquisition is
    deliberately fail-closed for non-annual families; a future change can
    register a governed production policy explicitly.
    """

    document_family: str
    policy_version: str
    scope: DocumentFamilyAcquisitionScope
    effective_version_policy: DocumentFamilyEffectiveVersionPolicy
    retention_policy: DocumentFamilyRetentionPolicy
    proactive_enabled: bool = False
    explicit_universe: tuple[str, ...] = ()
    universe_policy_version: str | None = None
    governance_policy_version: str | None = None
    environment: str = "production"
    schema_version: str = ACQUISITION_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        family = normalize_document_family(self.document_family)
        if not family:
            raise ValueError("document_family must be non-empty")
        object.__setattr__(self, "document_family", family)
        _policy_text(self.policy_version, "acquisition_policy.policy_version")
        if not isinstance(self.scope, DocumentFamilyAcquisitionScope):
            try:
                object.__setattr__(
                    self, "scope", DocumentFamilyAcquisitionScope(str(self.scope))
                )
            except ValueError as exc:
                raise ValueError("unsupported acquisition_policy.scope") from exc
        if self.schema_version != ACQUISITION_POLICY_SCHEMA_VERSION:
            raise ValueError("unsupported acquisition_policy.schema_version")
        if not isinstance(
            self.effective_version_policy, DocumentFamilyEffectiveVersionPolicy
        ):
            object.__setattr__(
                self,
                "effective_version_policy",
                DocumentFamilyEffectiveVersionPolicy.from_mapping(
                    self.effective_version_policy
                ),
            )
        if not isinstance(self.retention_policy, DocumentFamilyRetentionPolicy):
            object.__setattr__(
                self,
                "retention_policy",
                DocumentFamilyRetentionPolicy.from_mapping(self.retention_policy),
            )
        if type(self.proactive_enabled) is not bool:
            raise TypeError("acquisition_policy.proactive_enabled must be a boolean")
        universe = tuple(
            sorted(
                dict.fromkeys(
                    _policy_text(item, "acquisition_policy.explicit_universe")
                    for item in self.explicit_universe
                )
            )
        )
        object.__setattr__(self, "explicit_universe", universe)
        for name in ("universe_policy_version", "governance_policy_version"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(
                    self, name, _policy_text(value, f"acquisition_policy.{name}")
                )
        environment = _policy_text(
            self.environment, "acquisition_policy.environment"
        ).lower()
        if environment not in {"production", "test", "development"}:
            raise ValueError(
                "acquisition_policy.environment must be production, test, or development"
            )
        object.__setattr__(self, "environment", environment)

        if self.scope is DocumentFamilyAcquisitionScope.METADATA_ONLY:
            if self.proactive_enabled or self.explicit_universe:
                raise ValueError(
                    "metadata_only policy cannot proactively acquire attachments"
                )
        elif self.scope is DocumentFamilyAcquisitionScope.BOUNDED_EXPLICIT_UNIVERSE:
            if not self.explicit_universe:
                raise ValueError(
                    "bounded explicit-universe policy requires a non-empty universe"
                )
            if not self.universe_policy_version:
                raise ValueError(
                    "bounded explicit-universe policy requires universe_policy_version"
                )
            if self.governance_policy_version:
                raise ValueError(
                    "bounded explicit-universe policy cannot set governance_policy_version"
                )
        else:
            if self.explicit_universe:
                raise ValueError("full-market policy cannot carry an explicit universe")
            if not self.universe_policy_version:
                raise ValueError("full-market policy requires universe_policy_version")
            if not self.governance_policy_version:
                raise ValueError(
                    "full-market policy requires governance_policy_version"
                )
        if (
            self.environment == "production"
            and family != DocumentFamily.ANNUAL_REPORT.value
            and self.proactive_enabled
        ):
            raise ValueError(
                "production proactive acquisition remains disabled for non-annual families"
            )

    def normalized_mapping(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "document_family": self.document_family,
            "policy_version": self.policy_version,
            "scope": self.scope.value,
            "proactive_enabled": self.proactive_enabled,
            "explicit_universe": list(self.explicit_universe),
            "universe_policy_version": self.universe_policy_version,
            "governance_policy_version": self.governance_policy_version,
            "environment": self.environment,
            "effective_version_policy": self.effective_version_policy.normalized_mapping(),
            "retention_policy": self.retention_policy.normalized_mapping(),
        }

    @property
    def fingerprint(self) -> str:
        payload = json.dumps(
            self.normalized_mapping(),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    @property
    def policy_fingerprint(self) -> str:
        """Explicit alias used by persisted work/configuration records."""

        return self.fingerprint

    def attachment_acquisition_allowed(self, instrument_id: str | None = None) -> bool:
        """Return whether this policy permits proactive bytes for one target."""

        if (
            not self.proactive_enabled
            or self.scope is DocumentFamilyAcquisitionScope.METADATA_ONLY
        ):
            return False
        if self.scope is DocumentFamilyAcquisitionScope.BOUNDED_EXPLICIT_UNIVERSE:
            return (
                instrument_id is not None
                and str(instrument_id).strip() in self.explicit_universe
            )
        # Even a governed full-market policy authorizes a concrete target, not
        # an unbounded caller operation with no instrument identity.
        return instrument_id is not None and bool(str(instrument_id).strip())

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DocumentFamilyAcquisitionPolicy:
        raw = dict(value)
        allowed = {
            "schema_version",
            "document_family",
            "policy_version",
            "scope",
            "proactive_enabled",
            "explicit_universe",
            "universe_policy_version",
            "governance_policy_version",
            "environment",
            "effective_version_policy",
            "retention_policy",
        }
        unknown = sorted(set(raw) - allowed)
        if unknown:
            raise ValueError(
                "acquisition_policy contains unknown fields: " + ", ".join(unknown)
            )
        universe = raw.get("explicit_universe", ())
        if isinstance(universe, (str, bytes)) or not isinstance(
            universe, (list, tuple)
        ):
            raise TypeError("acquisition_policy.explicit_universe must be an array")
        effective = raw.get("effective_version_policy")
        retention = raw.get("retention_policy")
        if not isinstance(effective, Mapping):
            raise TypeError(
                "acquisition_policy.effective_version_policy must be a mapping"
            )
        if not isinstance(retention, Mapping):
            raise TypeError("acquisition_policy.retention_policy must be a mapping")
        return cls(
            schema_version=raw.get("schema_version", ACQUISITION_POLICY_SCHEMA_VERSION),
            document_family=_policy_text(
                raw.get("document_family"), "acquisition_policy.document_family"
            ),
            policy_version=_policy_text(
                raw.get("policy_version"), "acquisition_policy.policy_version"
            ),
            scope=raw.get("scope"),
            proactive_enabled=raw.get("proactive_enabled", False),
            explicit_universe=tuple(universe),
            universe_policy_version=raw.get("universe_policy_version"),
            governance_policy_version=raw.get("governance_policy_version"),
            environment=raw.get("environment", "production"),
            effective_version_policy=DocumentFamilyEffectiveVersionPolicy.from_mapping(
                effective
            ),
            retention_policy=DocumentFamilyRetentionPolicy.from_mapping(retention),
        )


_DOCUMENT_FAMILY_ALIASES = {
    "annual": DocumentFamily.ANNUAL_REPORT.value,
    "annual_report": DocumentFamily.ANNUAL_REPORT.value,
    "annual_report_correction": DocumentFamily.ANNUAL_REPORT.value,
    "annual_correction": DocumentFamily.ANNUAL_REPORT.value,
    "correction": DocumentFamily.ANNUAL_REPORT.value,
    "correction_notice": DocumentFamily.ANNUAL_REPORT.value,
    "annual_report_notice": DocumentFamily.ANNUAL_REPORT.value,
    "semiannual": DocumentFamily.SEMIANNUAL_REPORT.value,
    "semiannual_report": DocumentFamily.SEMIANNUAL_REPORT.value,
    "half_year": DocumentFamily.SEMIANNUAL_REPORT.value,
    "quarterly": DocumentFamily.QUARTERLY_REPORT.value,
    "quarterly_report": DocumentFamily.QUARTERLY_REPORT.value,
}


def normalize_document_family(value: Any) -> str | None:
    """Normalize provider/legacy labels into the orthogonal family field."""

    text = str(value or "").strip().lower()
    if not text:
        return None
    return _DOCUMENT_FAMILY_ALIASES.get(text, text)


def normalize_annual_report_variant(
    value: Any,
    *,
    correction_evidence: bool = False,
) -> AnnualReportVariant | None:
    """Normalize legacy correction labels without making them a family."""

    text = str(value or "").strip().lower()
    if (
        text
        in {
            "correction",
            "annual_report_correction",
            "annual_correction",
            "revised",
            "revision",
        }
        or correction_evidence
    ):
        return AnnualReportVariant.CORRECTION
    if text in {"", "original", "annual", "annual_report"}:
        return AnnualReportVariant.ORIGINAL if text else None
    return None


class EffectiveDecisionState(_StringEnum):
    CURRENT = "current"
    PROVISIONAL = "provisional"
    AMBIGUOUS = "ambiguous"
    BLOCKED = "blocked"
    WITHDRAWN = "withdrawn"


class EffectiveDecisionKind(_StringEnum):
    INITIAL_ACTIVATION = "initial_activation"
    REPLACEMENT = "replacement"
    PROJECTION_UPDATE = "projection_update"
    MIGRATION_SNAPSHOT = "migration_snapshot"
    WITHDRAWN_WITHOUT_REPLACEMENT = "withdrawn_without_replacement"


class EnsureDisposition(_StringEnum):
    LOCAL_HIT = "local_hit"
    LOCAL_MISS = "local_miss"
    OPERATION_CREATED = "operation_created"
    OPERATION_REUSED = "operation_reused"


class OperationStatus(_StringEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    MISSING = "missing"
    FAILED = "failed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


class AssetRequestStatus(_StringEnum):
    ACTIVE = "active"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


ACTIVE_OPERATION_STATUSES = frozenset(
    {OperationStatus.QUEUED.value, OperationStatus.RUNNING.value}
)
TERMINAL_OPERATION_STATUSES = frozenset(
    status.value
    for status in OperationStatus
    if status.value not in ACTIVE_OPERATION_STATUSES
)


class OperationStage(_StringEnum):
    NOT_APPLICABLE = "not_applicable"
    DISCOVERING = "discovering"
    RECONCILING = "reconciling"
    DOWNLOADING = "downloading"
    VALIDATING = "validating"
    ACTIVATING = "activating"


class BatchOutcome(_StringEnum):
    SUCCESS = "success"
    PARTIAL = "partial"
    BLOCKED = "blocked"
    FAILED = "failed"


class ResultOrigin(_StringEnum):
    ADOPTED = "adopted"
    DOWNLOADED = "downloaded"
    REPAIRED = "repaired"


class CoverageStatus(_StringEnum):
    AVAILABLE = "available"
    CONFIRMED_MISSING = "confirmed_missing"
    INCOMPLETE = "incomplete"
    RETRYABLE = "retryable"
    BLOCKED = "blocked"


class ExpectedPeriodCoverage(_StringEnum):
    NOT_DUE = "not_due"
    CURRENT = "current"
    OVERDUE_MISSING = "overdue_missing"
    INCOMPLETE = "incomplete"


class ChangeEventType(_StringEnum):
    ADDED = "added"
    REPLACED = "replaced"
    REPAIRED = "repaired"
    WITHDRAWN = "withdrawn"
    DELETED = "deleted"


def canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def stable_id(prefix: str, *parts: Any) -> str:
    normalized = "\x1f".join(str(part or "").strip() for part in parts)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:32]}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_instrument_id(value: str) -> str:
    instrument_id = str(value or "").strip().upper()
    if not instrument_id:
        raise ValueError("instrument_id is required")
    return instrument_id


def normalize_source(value: str) -> str:
    source = str(value or "").strip().lower()
    if not source:
        raise ValueError("source is required")
    return source


def normalize_source_url(value: str) -> str:
    """Return a deterministic URL identity while retaining meaningful query data."""
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("source_url is required")
    parsed = urlsplit(raw)
    if parsed.scheme and parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("source_url must use http or https")
    hostname = (parsed.hostname or "").lower()
    port = parsed.port
    netloc = hostname
    if port and not (
        (parsed.scheme.lower() == "http" and port == 80)
        or (parsed.scheme.lower() == "https" and port == 443)
    ):
        netloc = f"{hostname}:{port}"
    if parsed.username or parsed.password:
        raise ValueError("source_url credentials are not allowed")
    path = parsed.path or "/"
    query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
    return urlunsplit((parsed.scheme.lower(), netloc, path, query, ""))


@dataclass(frozen=True)
class OfficialAnnouncement:
    announcement_id: str
    source: str
    source_announcement_id: str
    title: str
    instrument_id: str | None
    exchange: str | None
    published_at: str | None
    published_at_raw: str | None
    raw_payload_hash: str
    first_observed_at: str
    last_observed_at: str
    status: str = "observed"
    metadata: Mapping[str, Any] = field(default_factory=dict)
    source_category: str | None = None
    published_at_precision: str | None = None
    provider_diagnostics: Mapping[str, Any] = field(default_factory=dict)
    schema_version: str = ANNOUNCEMENT_SCHEMA_VERSION


@dataclass(frozen=True)
class OfficialAnnouncementAttachment:
    attachment_id: str
    announcement_id: str
    attachment_identity: str
    source_attachment_id: str | None
    source_url: str
    normalized_source_url: str
    name: str | None
    media_type: str | None
    content_length_hint: int | None
    first_observed_at: str
    last_observed_at: str
    metadata: Mapping[str, Any] = field(default_factory=dict)
    schema_version: str = ATTACHMENT_SCHEMA_VERSION


@dataclass(frozen=True)
class OfficialDocumentBlob:
    content_hash: str
    content_length: int
    canonical_path: str
    signature_status: str
    integrity_status: IntegrityStatus
    first_available_at: str
    last_verified_at: str | None
    schema_version: str = BLOB_SCHEMA_VERSION


@dataclass(frozen=True)
class OfficialAttachmentVersion:
    version_id: str
    attachment_id: str
    observation_key: str
    content_hash: str | None
    final_url: str | None
    retrieval_status: str
    integrity_status: IntegrityStatus
    attempt: int
    next_retry_at: str | None
    error_code: str | None
    observed_at: str
    version_available_at: str | None = None
    available_time_source: str = "first_observed"
    available_time_precision: str = "instant"
    first_observed_at: str | None = None
    last_observed_at: str | None = None
    response_evidence: Mapping[str, Any] = field(default_factory=dict)
    content_length_observed: int | None = None
    content_hash_observed: str | None = None
    lease_owner: str | None = None
    lease_generation: int | None = None
    max_attempts: int = 4
    temporary_path: str | None = None
    temporary_bytes: int | None = None
    quarantine_path: str | None = None
    visibility_state: str = "production"
    metadata: Mapping[str, Any] = field(default_factory=dict)
    schema_version: str = ATTACHMENT_VERSION_SCHEMA_VERSION


@dataclass(frozen=True)
class SourceFilingEvidence:
    """One legal filing identity in a proven equivalent evidence set."""

    source: str
    source_announcement_id: str
    attachment_id: str
    version_id: str
    content_hash: str | None = None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "source": self.source,
            "source_announcement_id": self.source_announcement_id,
            "attachment_id": self.attachment_id,
            "version_id": self.version_id,
            "content_hash": self.content_hash,
        }


def normalize_source_filing_evidence(
    values: tuple[SourceFilingEvidence, ...] | list[SourceFilingEvidence],
) -> tuple[SourceFilingEvidence, ...]:
    """Return a stable de-duplicated evidence set independent of discovery order."""

    unique = {
        (
            item.source,
            item.source_announcement_id,
            item.attachment_id,
            item.version_id,
            item.content_hash,
        ): item
        for item in values
    }
    return tuple(
        sorted(
            unique.values(),
            key=lambda item: (
                item.source,
                item.source_announcement_id,
                item.attachment_id,
                item.version_id,
                item.content_hash or "",
            ),
        )
    )


def source_filing_evidence_hash(values: tuple[SourceFilingEvidence, ...]) -> str:
    normalized = normalize_source_filing_evidence(values)
    payload = [item.as_dict() for item in normalized]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class EffectiveAnnualReport:
    asset_id: str
    instrument_id: str
    fiscal_year: int
    report_period: str
    announcement_id: str
    attachment_id: str
    version_id: str
    content_hash: str | None
    source: str
    source_announcement_id: str
    published_at: str | None
    variant: AnnualReportVariant
    classifier_version: str
    decision_state: EffectiveDecisionState
    availability: AssetAvailability
    predecessor_asset_id: str | None
    pending_candidate_id: str | None
    activated_at: str | None
    last_checked_at: str
    decision_reasons: tuple[str, ...] = ()
    visibility_state: str = "production"
    equivalent_source_filings: tuple[SourceFilingEvidence, ...] = ()
    canonical_projection_policy_version: str = (
        CANONICAL_FILING_PROJECTION_POLICY_VERSION
    )
    evidence_set_hash: str | None = None
    decision_evidence: Mapping[str, Any] = field(default_factory=dict)
    document_family: str = DocumentFamily.ANNUAL_REPORT.value
    schema_version: str = EFFECTIVE_ANNUAL_REPORT_SCHEMA_VERSION

    @property
    def is_full_report(self) -> bool:
        """Effective annual-report rows can only be complete report assets."""

        return True

    @property
    def classification_vocabulary_version(self) -> str:
        return CLASSIFICATION_VOCABULARY_VERSION


@dataclass(frozen=True)
class EffectiveAnnualReportDecision:
    """One immutable transition in an annual report's effective lineage."""

    decision_sequence: int
    decision_id: str
    instrument_id: str
    fiscal_year: int
    decision_kind: EffectiveDecisionKind
    predecessor_asset_id: str | None
    predecessor_source: str | None
    predecessor_source_announcement_id: str | None
    predecessor_announcement_id: str | None
    predecessor_attachment_id: str | None
    predecessor_version_id: str | None
    predecessor_content_hash: str | None
    replacement_asset_id: str | None
    replacement_source: str | None
    replacement_source_announcement_id: str | None
    replacement_announcement_id: str | None
    replacement_attachment_id: str | None
    replacement_version_id: str | None
    replacement_content_hash: str | None
    decision_state: EffectiveDecisionState
    classifier_version: str
    decision_policy_version: str
    decision_reasons: tuple[str, ...]
    decision_evidence: Mapping[str, Any]
    activated_at: str
    outbox_event_key: str
    created_at: str
    schema_version: str = EFFECTIVE_DECISION_SCHEMA_VERSION


@dataclass(frozen=True)
class AssetOperation:
    operation_id: str
    operation_type: str
    idempotency_key: str
    scope: Mapping[str, Any]
    policy_version: str
    owner: str | None
    status: OperationStatus
    stage: OperationStage | None
    outcome: BatchOutcome | None
    attempt: int
    next_retry_at: str | None
    lease_owner: str | None
    lease_generation: int
    lease_expires_at: str | None
    heartbeat_at: str | None
    progress: Mapping[str, Any]
    result_asset_id: str | None
    result_origin: ResultOrigin | None
    reason_code: str | None
    diagnostics: Mapping[str, Any]
    created_at: str
    started_at: str | None
    finished_at: str | None
    updated_at: str
    bounds: Mapping[str, Any] = field(default_factory=dict)
    checkpoint: Mapping[str, Any] = field(default_factory=dict)
    max_attempts: int = 1
    resume_generation: int = 0
    config_version: str | None = None
    stage_schema_version: str = OPERATION_STAGE_SCHEMA_VERSION
    schema_version: str = OPERATION_SCHEMA_VERSION


@dataclass(frozen=True)
class AssetOperationSubscription:
    """Caller-scoped view of a globally single-flight asset operation."""

    asset_request_id: str
    operation_id: str
    principal: str
    consumer: str | None
    idempotency_key: str
    request_fingerprint: str
    status: AssetRequestStatus
    created_at: str
    updated_at: str
    cancelled_at: str | None = None
    consumer_continuation_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    authorized_projection: Mapping[str, Any] = field(default_factory=dict)
    expires_at: str | None = None
    expired_at: str | None = None
    tombstone_until: str | None = None
    retention_policy_version: str = "asset_request_retention.v1"
    schema_version: str = OPERATION_SUBSCRIPTION_SCHEMA_VERSION


@dataclass(frozen=True)
class OfficialAssetChangeEvent:
    event_id: int | None
    event_key: str
    event_type: ChangeEventType
    instrument_id: str
    fiscal_year: int
    asset_id: str | None
    predecessor_asset_id: str | None
    content_hash: str | None
    payload: Mapping[str, Any]
    created_at: str
    trigger_origin: str = "unknown"
    dispatch_policy_version: str = "asset_change_event.v1"
    schema_version: str = CHANGE_EVENT_SCHEMA_VERSION


@dataclass(frozen=True)
class OfficialAssetDiscoveryState:
    source: str
    exchange: str
    category: str
    scope_key: str
    config_fingerprint: str
    item_cursor_kind: str | None
    item_cursor_value: str | None
    covered_until: str | None
    run_cutoff: str | None
    next_page: int | None
    status: str
    is_complete: bool
    gap_reason: str | None
    checkpoint: Mapping[str, Any]
    created_at: str
    updated_at: str
    lease_owner: str | None = None
    lease_expires_at: str | None = None
    lease_generation: int = 0
    state_version: int = 0
    schema_version: str = DISCOVERY_STATE_SCHEMA_VERSION


@dataclass(frozen=True)
class EnsureRequest:
    instrument_id: str | None = None
    fiscal_year: int | None = None
    source: str | None = None
    source_announcement_id: str | None = None
    filing_id: str | None = None
    attachment_id: str | None = None
    expected_content_hash: str | None = None
    observation_version: str | None = None
    allow_network: bool = False
    integrity_level: str = "hash"
    wait_seconds: float | None = None
    consumer: str | None = None
    principal: str | None = None
    idempotency_key: str | None = None
    consumer_continuation_id: str | None = None
    knowledge_cutoff: str | None = None

    def __post_init__(self) -> None:
        source_announcement_id = str(self.source_announcement_id or "").strip()
        filing_id = str(self.filing_id or "").strip()
        if source_announcement_id and filing_id and source_announcement_id != filing_id:
            raise ValueError(
                "filing_id conflicts with canonical source_announcement_id"
            )
        canonical_filing_id = source_announcement_id or filing_id
        object.__setattr__(
            self,
            "source_announcement_id",
            canonical_filing_id or None,
        )
        object.__setattr__(self, "filing_id", canonical_filing_id or None)
        period_scope = bool(self.instrument_id and self.fiscal_year is not None)
        filing_scope = bool(self.source and canonical_filing_id)
        if bool(self.source) != bool(canonical_filing_id):
            raise ValueError(
                "source and source_announcement_id/filing_id are all-or-none"
            )
        if period_scope == filing_scope:
            raise ValueError(
                "ensure request requires exactly one period or source-filing scope"
            )
        if self.fiscal_year is not None and filing_scope:
            raise ValueError("exact-filing scope cannot include fiscal_year")
        if self.fiscal_year is not None and not 1990 <= int(self.fiscal_year) <= 2200:
            raise ValueError("fiscal_year is outside supported bounds")
        if self.wait_seconds is not None and self.wait_seconds < 0:
            raise ValueError("wait_seconds cannot be negative")
        has_pin = bool(
            self.attachment_id or self.expected_content_hash or self.observation_version
        )
        if has_pin and not filing_scope:
            raise ValueError("attachment observation pins require exact-filing scope")
        if self.expected_content_hash:
            digest = str(self.expected_content_hash).strip().lower()
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise ValueError("expected_content_hash must be a SHA-256 digest")

    @property
    def normalized_scope(self) -> Mapping[str, Any]:
        if self.fiscal_year is not None:
            return {
                "instrument_id": normalize_instrument_id(self.instrument_id),
                "fiscal_year": int(self.fiscal_year or 0),
                "knowledge_cutoff": self.knowledge_cutoff,
            }
        return {
            "source": normalize_source(self.source or ""),
            "source_announcement_id": str(self.source_announcement_id or "").strip(),
            "instrument_id": (
                None
                if not self.instrument_id
                else normalize_instrument_id(self.instrument_id)
            ),
            "attachment_id": (
                None if self.attachment_id is None else str(self.attachment_id).strip()
            ),
            "expected_content_hash": (
                None
                if self.expected_content_hash is None
                else str(self.expected_content_hash).strip().lower()
            ),
            "observation_version": (
                None
                if self.observation_version is None
                else str(self.observation_version).strip()
            ),
            "knowledge_cutoff": self.knowledge_cutoff,
        }


@dataclass(frozen=True)
class EnsureResult:
    disposition: EnsureDisposition
    availability: AssetAvailability
    asset: EffectiveAnnualReport | None = None
    operation: AssetOperation | None = None
    asset_request: AssetOperationSubscription | None = None
    reason_code: str | None = None


@dataclass(frozen=True)
class FiscalYearSearchBounds:
    as_of: date
    listing_date: date
    candidate_upper_year: int
    disclosure_due_year: int
    earliest_search_year: int
    candidate_years: tuple[int, ...]
    policy_version: str
