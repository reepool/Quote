"""Governed attachment retrieval without business archive policy."""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests

from utils.date_utils import get_shanghai_time
from utils.http_transport import HttpTlsConfig, create_requests_session, request_get

from .models import AnnouncementAttachment, AnnouncementRetrievalResult


REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class AttachmentRetrievalPolicy:
    """Source-specific URL trust and bounded transport settings."""

    source: str
    artifact_base_url: str
    approved_hosts: Tuple[str, ...]
    headers: Dict[str, str] = field(default_factory=dict)
    request_timeout_seconds: float = 20.0
    request_interval_seconds: float = 0.2
    retry_attempts: int = 2
    retry_backoff_seconds: float = 0.5
    max_attachment_bytes: int = 50 * 1024 * 1024
    max_redirects: int = 3

    @classmethod
    def from_mapping(
        cls,
        source: str,
        value: Mapping[str, Any],
    ) -> "AttachmentRetrievalPolicy":
        source_name = str(source or "").strip().lower()
        base_url = str(value.get("artifact_base_url") or "").strip()
        hosts = tuple(
            str(item).strip().lower()
            for item in value.get("approved_attachment_hosts", ())
            if str(item).strip()
        )
        if not source_name or not base_url or not hosts:
            raise ValueError(
                "attachment policy requires source, artifact_base_url, and approved hosts"
            )
        headers = dict(value.get("artifact_headers") or {})
        referer = str(value.get("referer") or "").strip()
        if referer and "Referer" not in headers:
            headers["Referer"] = referer
        headers.setdefault(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        )
        headers.setdefault("Accept", "application/pdf,application/octet-stream,*/*")
        return cls(
            source=source_name,
            artifact_base_url=base_url,
            approved_hosts=hosts,
            headers=headers,
            request_timeout_seconds=max(
                1.0,
                float(value.get("request_timeout_seconds", 20.0)),
            ),
            request_interval_seconds=max(
                0.0,
                float(value.get("request_interval_seconds", 0.2)),
            ),
            retry_attempts=max(0, int(value.get("retry_attempts", 2))),
            retry_backoff_seconds=max(
                0.0,
                float(value.get("retry_backoff_seconds", 0.5)),
            ),
            max_attachment_bytes=max(
                1,
                int(value.get("max_attachment_bytes", 50 * 1024 * 1024)),
            ),
            max_redirects=max(0, int(value.get("max_redirects", 3))),
        )


class AnnouncementAttachmentRetriever:
    """Resolve and download official attachments under source trust policies."""

    def __init__(
        self,
        policies: Mapping[str, AttachmentRetrievalPolicy],
        *,
        sessions: Optional[Mapping[str, requests.Session]] = None,
    ) -> None:
        self.policies = {
            str(source).strip().lower(): policy for source, policy in policies.items()
        }
        self.sessions: Dict[str, requests.Session] = dict(sessions or {})

    @classmethod
    def from_provider_configs(
        cls,
        provider_configs: Mapping[str, Mapping[str, Any]],
    ) -> "AnnouncementAttachmentRetriever":
        policies = {
            str(source).strip().lower(): AttachmentRetrievalPolicy.from_mapping(
                str(source),
                value,
            )
            for source, value in provider_configs.items()
            if value.get("artifact_base_url")
            and value.get("approved_attachment_hosts")
            and bool(value.get("enabled", True))
        }
        return cls(policies)

    def resolve_attachment(
        self,
        source: str,
        attachment: AnnouncementAttachment,
    ) -> AnnouncementAttachment:
        policy = self._policy(source)
        resolved_url = attachment.resolved_url or urljoin(
            policy.artifact_base_url,
            attachment.source_url,
        )
        self._validate_url(policy, resolved_url)
        return AnnouncementAttachment(
            source_url=attachment.source_url,
            resolved_url=resolved_url,
            attachment_id=attachment.attachment_id,
            name=attachment.name,
            media_type=attachment.media_type,
            file_extension=attachment.file_extension,
            raw_metadata=attachment.raw_metadata,
        )

    def retrieve(
        self,
        source: str,
        attachment: AnnouncementAttachment,
        *,
        require_pdf: bool = False,
    ) -> AnnouncementRetrievalResult:
        source_name = str(source or "").strip().lower()
        policy = self._policy(source_name)
        started = time.monotonic()
        LOGGER.info(
            "announcement attachment retrieval started: source=%s host=%s require_pdf=%s max_bytes=%s retries=%s",
            source_name,
            urlparse(attachment.resolved_url or attachment.source_url).hostname,
            require_pdf,
            policy.max_attachment_bytes,
            policy.retry_attempts,
        )
        try:
            resolved_attachment = self.resolve_attachment(source_name, attachment)
        except Exception as exc:
            LOGGER.warning(
                "announcement attachment retrieval failed: source=%s attempts=0 elapsed=%.3f error=url_policy_failed:%s",
                source_name,
                time.monotonic() - started,
                type(exc).__name__,
            )
            return self._failed(
                source_name,
                attachment,
                f"url_policy_failed:{type(exc).__name__}:{exc}",
            )

        last_error: Optional[str] = None
        for attempt in range(policy.retry_attempts + 1):
            try:
                content, final_url, response_media_type, redirect_count = self._fetch_once(
                    policy,
                    resolved_attachment.resolved_url or resolved_attachment.source_url,
                )
                signature_status = (
                    "valid_pdf" if content.startswith(b"%PDF-") else "not_pdf"
                )
                if require_pdf and signature_status != "valid_pdf":
                    raise ValueError("invalid_pdf_signature")
                digest = hashlib.sha256(content).hexdigest()
                LOGGER.info(
                    "announcement attachment retrieval completed: source=%s status=success bytes=%s redirects=%s attempt=%s signature=%s elapsed=%.3f",
                    source_name,
                    len(content),
                    redirect_count,
                    attempt + 1,
                    signature_status,
                    time.monotonic() - started,
                )
                return AnnouncementRetrievalResult(
                    source=source_name,
                    attachment=resolved_attachment,
                    status="success",
                    content=content,
                    content_hash=digest,
                    content_length=len(content),
                    final_url=final_url,
                    response_media_type=response_media_type,
                    retrieved_at=get_shanghai_time().isoformat(),
                    signature_status=signature_status,
                    diagnostics={
                        "redirect_count": redirect_count,
                        "attempt": attempt + 1,
                        "media_type_mismatch": bool(
                            response_media_type
                            and "pdf" in response_media_type.lower()
                            and signature_status != "valid_pdf"
                        ),
                    },
                )
            except Exception as exc:
                last_error = f"{type(exc).__name__}:{exc}"
                if attempt >= policy.retry_attempts:
                    break
                if policy.retry_backoff_seconds > 0:
                    time.sleep(policy.retry_backoff_seconds * (attempt + 1))
        LOGGER.warning(
            "announcement attachment retrieval failed: source=%s attempts=%s elapsed=%.3f error=%s",
            source_name,
            policy.retry_attempts + 1,
            time.monotonic() - started,
            last_error or "attachment_retrieval_failed",
        )
        return self._failed(
            source_name,
            resolved_attachment,
            last_error or "attachment_retrieval_failed",
        )

    def _fetch_once(
        self,
        policy: AttachmentRetrievalPolicy,
        initial_url: str,
    ) -> tuple[bytes, str, Optional[str], int]:
        current_url = initial_url
        redirect_count = 0
        session = self.sessions.get(policy.source)
        if session is None:
            session = create_requests_session(
                tls_config=HttpTlsConfig(source_name=policy.source),
                headers=policy.headers,
            )
            self.sessions[policy.source] = session
        while True:
            self._validate_url(policy, current_url)
            response = request_get(
                current_url,
                tls_config=HttpTlsConfig(source_name=policy.source),
                session=session,
                headers=policy.headers,
                timeout=policy.request_timeout_seconds,
                allow_redirects=False,
                stream=True,
            )
            status_code = int(getattr(response, "status_code", 200))
            if status_code in REDIRECT_STATUSES:
                location = str((getattr(response, "headers", {}) or {}).get("Location") or "").strip()
                if not location:
                    raise ValueError("redirect_location_missing")
                redirect_count += 1
                if redirect_count > policy.max_redirects:
                    raise ValueError("redirect_limit_exceeded")
                current_url = urljoin(current_url, location)
                continue
            response.raise_for_status()
            headers = getattr(response, "headers", {}) or {}
            content_length = headers.get("Content-Length")
            if content_length not in (None, "") and int(content_length) > policy.max_attachment_bytes:
                raise ValueError("attachment_size_limit_exceeded")
            content = self._bounded_content(response, policy.max_attachment_bytes)
            if not content:
                raise ValueError("attachment_empty")
            media_type = str(headers.get("Content-Type") or "").split(";", 1)[0].strip() or None
            if policy.request_interval_seconds > 0:
                time.sleep(policy.request_interval_seconds)
            return content, current_url, media_type, redirect_count

    @staticmethod
    def _bounded_content(response: Any, limit: int) -> bytes:
        chunks = []
        total = 0
        iterator = getattr(response, "iter_content", None)
        if callable(iterator):
            for chunk in iterator(chunk_size=64 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if total > limit:
                    raise ValueError("attachment_size_limit_exceeded")
                chunks.append(bytes(chunk))
            return b"".join(chunks)
        content = bytes(getattr(response, "content", b"") or b"")
        if len(content) > limit:
            raise ValueError("attachment_size_limit_exceeded")
        return content

    def _policy(self, source: str) -> AttachmentRetrievalPolicy:
        policy = self.policies.get(str(source or "").strip().lower())
        if policy is None:
            raise ValueError(f"attachment retrieval policy is not configured: {source}")
        return policy

    @staticmethod
    def _validate_url(policy: AttachmentRetrievalPolicy, url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("attachment_url_invalid")
        hostname = parsed.hostname.lower()
        if hostname not in policy.approved_hosts:
            raise ValueError(f"attachment_host_not_approved:{hostname}")

    @staticmethod
    def _failed(
        source: str,
        attachment: AnnouncementAttachment,
        error: str,
    ) -> AnnouncementRetrievalResult:
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="failed",
            errors=(error,),
            retrieved_at=get_shanghai_time().isoformat(),
        )
