"""
Configured official structured financial filing provider.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests

from research.official_financial_source_profiles import source_profile_metadata
from utils.http_transport import HttpTlsConfig, create_requests_session

from .base import (
    BaseOfficialFinancialFilingProvider,
    FinancialFilingPayload,
    FinancialSourceFileManifest,
)


@dataclass(frozen=True)
class OfficialFilingResponseClassification:
    """Normalized classification for a candidate official filing response."""

    response_class: str
    artifact_kind: Optional[str] = None
    parser_candidate: Optional[str] = None
    readiness_blocker: Optional[str] = None
    is_structured: bool = False
    diagnostics: Dict[str, Any] = field(default_factory=dict)

    def as_probe_fields(self) -> Dict[str, Any]:
        """Return stable fields consumed by live probe output."""
        return {
            "response_class": self.response_class,
            "artifact_kind": self.artifact_kind,
            "parser_candidate": self.parser_candidate,
            "structured_payload": self.is_structured,
            "readiness_blocker": self.readiness_blocker,
            "classification_diagnostics": self.diagnostics,
        }


@dataclass(frozen=True)
class OfficialFilingArtifactCandidate:
    """One concrete filing artifact candidate resolved from official metadata."""

    url: str
    artifact_kind: Optional[str]
    parser_candidate: Optional[str]
    structured_payload: bool
    source_key: Optional[str] = None
    filing_id: Optional[str] = None
    title: Optional[str] = None
    report_period: Optional[str] = None
    report_type: Optional[str] = None
    metadata_json: Dict[str, Any] = field(default_factory=dict)

    def as_probe_fields(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "artifact_kind": self.artifact_kind,
            "parser_candidate": self.parser_candidate,
            "structured_payload": self.structured_payload,
            "source_key": self.source_key,
            "filing_id": self.filing_id,
            "title": self.title,
            "report_period": self.report_period,
            "report_type": self.report_type,
            "metadata_json": self.metadata_json,
        }


@dataclass(frozen=True)
class OfficialFilingContextResolution:
    """Resolved request context plus diagnostics for endpoint templating."""

    context: Dict[str, str]
    diagnostics: List[Dict[str, Any]] = field(default_factory=list)


def classify_official_filing_response(
    sample: bytes | str,
    *,
    content_type: Optional[str] = None,
    http_status: Optional[int] = None,
    url: Optional[str] = None,
) -> OfficialFilingResponseClassification:
    """Classify a candidate official filing response from headers and sample bytes."""
    raw = sample.encode("utf-8", errors="replace") if isinstance(sample, str) else bytes(sample or b"")
    stripped = raw.lstrip(b"\xef\xbb\xbf\r\n\t ")
    content_type_lower = (content_type or "").lower()
    text_full = stripped.decode("utf-8", errors="ignore")
    text_sample = text_full[:8192]
    text_lower = text_sample.lower()

    if http_status is not None and http_status in {401, 403, 407, 408, 409, 429, 451}:
        return OfficialFilingResponseClassification(
            response_class="blocked",
            readiness_blocker=f"http_{http_status}",
            diagnostics={"http_status": http_status, "url": url},
        )
    if not stripped:
        return OfficialFilingResponseClassification(
            response_class="empty",
            readiness_blocker="empty_response",
            diagnostics={"content_type": content_type, "http_status": http_status},
        )
    if _looks_blocked(text_lower):
        return OfficialFilingResponseClassification(
            response_class="blocked",
            readiness_blocker="blocked_or_verification_response",
            diagnostics={"content_type": content_type, "http_status": http_status},
        )
    if stripped.startswith(b"%PDF-") or "application/pdf" in content_type_lower:
        return OfficialFilingResponseClassification(
            response_class="pdf_only",
            artifact_kind="pdf",
            readiness_blocker="pdf_only_not_structured",
            diagnostics={"content_type": content_type, "http_status": http_status},
        )
    if stripped.startswith(b"PK\x03\x04") or "application/zip" in content_type_lower:
        return OfficialFilingResponseClassification(
            response_class="structured_payload",
            artifact_kind="xbrl_zip",
            parser_candidate="xbrl_numeric_facts.v1",
            is_structured=True,
            diagnostics={"content_type": content_type, "http_status": http_status},
        )
    if _looks_like_json(stripped, content_type_lower):
        return _classify_json_response(
            text_full,
            content_type=content_type,
            http_status=http_status,
        )
    if _looks_like_html(text_lower, content_type_lower):
        if _looks_like_inline_xbrl(text_lower):
            return OfficialFilingResponseClassification(
                response_class="structured_payload",
                artifact_kind="structured_html",
                parser_candidate="inline_xbrl_numeric_facts.v1",
                is_structured=True,
                diagnostics={"content_type": content_type, "http_status": http_status},
            )
        return OfficialFilingResponseClassification(
            response_class="html_manifest",
            readiness_blocker="manifest_ok_endpoint_missing",
            diagnostics={"content_type": content_type, "http_status": http_status},
        )
    if _looks_like_xbrl_xml(text_lower, content_type_lower):
        return OfficialFilingResponseClassification(
            response_class="structured_payload",
            artifact_kind="xbrl_xml",
            parser_candidate="xbrl_numeric_facts.v1",
            is_structured=True,
            diagnostics={"content_type": content_type, "http_status": http_status},
        )
    if http_status is not None and not 200 <= http_status < 400:
        return OfficialFilingResponseClassification(
            response_class="unsupported",
            readiness_blocker=f"http_{http_status}",
            diagnostics={"content_type": content_type, "http_status": http_status},
        )
    return OfficialFilingResponseClassification(
        response_class="unsupported",
        readiness_blocker="unsupported_response",
        diagnostics={"content_type": content_type, "http_status": http_status},
    )


def extract_official_filing_artifact_candidates(
    sample: bytes | str,
    *,
    content_type: Optional[str] = None,
    base_url: Optional[str] = None,
    max_candidates: int = 20,
) -> List[OfficialFilingArtifactCandidate]:
    """Extract concrete filing artifact candidates from official manifest payloads."""
    raw = sample.encode("utf-8", errors="replace") if isinstance(sample, str) else bytes(sample or b"")
    stripped = raw.lstrip(b"\xef\xbb\xbf\r\n\t ")
    content_type_lower = (content_type or "").lower()
    if not stripped:
        return []
    text_sample = stripped[:65536].decode("utf-8", errors="ignore")
    candidates: List[OfficialFilingArtifactCandidate] = []
    if _looks_like_json(stripped, content_type_lower):
        payload = _parse_json_or_jsonp(text_sample)
        if payload is not None:
            candidates.extend(
                _extract_json_artifact_candidates(payload, base_url=base_url)
            )
    elif _looks_like_html(text_sample.lower(), content_type_lower):
        candidates.extend(
            _extract_html_artifact_candidates(text_sample, base_url=base_url)
        )

    deduped: Dict[str, OfficialFilingArtifactCandidate] = {}
    for candidate in candidates:
        if candidate.url and candidate.url not in deduped:
            deduped[candidate.url] = candidate
        if len(deduped) >= max_candidates:
            break
    return list(deduped.values())


def resolve_official_filing_context(
    session: requests.Session,
    context: Dict[str, str],
    *,
    resolvers: List[Dict[str, Any]],
    timeout: float,
    user_agent: str = "QuoteResearch/official-financial-filing",
) -> OfficialFilingContextResolution:
    """Resolve optional endpoint template variables from configured metadata calls."""
    resolved = dict(context)
    diagnostics: List[Dict[str, Any]] = []
    for resolver in resolvers:
        if not isinstance(resolver, dict) or not bool(resolver.get("enabled", True)):
            continue
        kind = str(resolver.get("kind") or "").strip()
        key = str(resolver.get("key") or kind or "context_resolver")
        if kind != "json_row_template":
            diagnostics.append(
                {
                    "key": key,
                    "status": "unsupported",
                    "kind": kind,
                    "error": "unsupported_context_resolver_kind",
                }
            )
            continue
        try:
            before_keys = set(resolved)
            row = _fetch_context_resolver_row(
                session,
                resolver=resolver,
                context=resolved,
                timeout=timeout,
                user_agent=user_agent,
            )
            if row is None:
                diagnostics.append(
                    {
                        "key": key,
                        "status": "not_found",
                        "kind": kind,
                        "added_keys": [],
                    }
                )
                continue
            for output_key, template in (resolver.get("outputs") or {}).items():
                value = _format_context_template(str(template), resolved, row)
                if value:
                    resolved[str(output_key)] = value
            diagnostics.append(
                {
                    "key": key,
                    "status": "ok",
                    "kind": kind,
                    "added_keys": sorted(set(resolved) - before_keys),
                }
            )
        except Exception as exc:  # pragma: no cover - transport-specific
            diagnostics.append(
                {
                    "key": key,
                    "status": "error",
                    "kind": kind,
                    "error": str(exc),
                }
            )
    return OfficialFilingContextResolution(context=resolved, diagnostics=diagnostics)


def _classify_json_response(
    text_sample: str,
    *,
    content_type: Optional[str],
    http_status: Optional[int],
) -> OfficialFilingResponseClassification:
    payload = _parse_json_or_jsonp(text_sample)
    if payload is None:
        return OfficialFilingResponseClassification(
            response_class="unsupported",
            readiness_blocker="malformed_json",
            diagnostics={"content_type": content_type, "http_status": http_status},
        )

    key_paths = sorted(_collect_json_key_paths(payload))
    diagnostics = {
        "content_type": content_type,
        "http_status": http_status,
        "sample_key_paths": key_paths[:20],
    }
    if _json_has_structured_financial_facts(payload, key_paths):
        return OfficialFilingResponseClassification(
            response_class="structured_payload",
            artifact_kind="structured_json",
            parser_candidate="structured_financial_json.v1",
            is_structured=True,
            diagnostics=diagnostics,
        )
    return OfficialFilingResponseClassification(
        response_class="json_manifest",
        readiness_blocker="manifest_ok_endpoint_missing",
        diagnostics=diagnostics,
    )


def _looks_like_json(raw: bytes, content_type_lower: str) -> bool:
    stripped = raw.lstrip()
    text_prefix = stripped[:80].decode("utf-8", errors="ignore").lstrip()
    return (
        "json" in content_type_lower
        or stripped.startswith(b"{")
        or stripped.startswith(b"[")
        or bool(re.match(r"^[A-Za-z_$][\w$.]*\s*\(", text_prefix))
        or text_prefix.startswith("null(")
    )


def _parse_json_or_jsonp(text_sample: str) -> Optional[Any]:
    text = text_sample.lstrip("\ufeff\r\n\t ")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.match(r"^[A-Za-z_$][\w$.]*\s*\((.*)\)\s*;?\s*$", text, flags=re.S)
    if not match:
        match = re.match(r"^null\s*\((.*)\)\s*;?\s*$", text, flags=re.S)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def _fetch_context_resolver_row(
    session: requests.Session,
    *,
    resolver: Dict[str, Any],
    context: Dict[str, str],
    timeout: float,
    user_agent: str,
) -> Optional[Dict[str, Any]]:
    request_config = resolver.get("request") or resolver.get("request_config") or {}
    url = str(resolver.get("url") or resolver.get("endpoint_url") or "")
    if not url:
        return None
    method = str(request_config.get("method") or "GET").upper()
    headers = {
        "User-Agent": user_agent,
        **_format_mapping(request_config.get("headers") or {}, context),
    }
    kwargs: Dict[str, Any] = {
        "headers": headers,
        "timeout": timeout,
    }
    query_params = _format_mapping(
        request_config.get("query_params") or request_config.get("params") or {},
        context,
    )
    body_params = _format_mapping(
        request_config.get("body_params") or request_config.get("body") or {},
        context,
    )
    json_body = _format_mapping(request_config.get("json") or {}, context)
    if query_params:
        kwargs["params"] = query_params
    if body_params:
        kwargs["data"] = body_params
    if json_body:
        kwargs["json"] = json_body

    formatted_url = url.format_map(_SafeFormatDict(context))
    response = (
        session.get(formatted_url, **kwargs)
        if method == "GET"
        else session.request(method, formatted_url, **kwargs)
    )
    raise_for_status = getattr(response, "raise_for_status", None)
    if raise_for_status is not None:
        raise_for_status()
    payload = _parse_json_or_jsonp(str(getattr(response, "text", "") or ""))
    if payload is None:
        payload = _parse_json_or_jsonp(
            _response_body_bytes(response).decode(
                "utf-8",
                errors="ignore",
            )
        )
    rows = _extract_rows_by_path(payload, str(resolver.get("row_list_path") or ""))
    return _select_context_resolver_row(
        rows,
        row_match=resolver.get("row_match") or {},
        context=context,
    )


def _response_body_bytes(response: Any) -> bytes:
    content = getattr(response, "content", None)
    if content is not None:
        return bytes(content or b"")
    iter_content = getattr(response, "iter_content", None)
    if iter_content is None:
        return b""
    body = bytearray()
    for chunk in iter_content(chunk_size=4096):
        if chunk:
            body.extend(chunk)
    return bytes(body)


def _extract_rows_by_path(payload: Any, path: str) -> List[Dict[str, Any]]:
    current = payload
    for part in [item for item in path.split(".") if item]:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = None
        if current is None:
            break
    if isinstance(current, list):
        return [item for item in current if isinstance(item, dict)]
    if isinstance(current, dict):
        return [current]
    return []


def _select_context_resolver_row(
    rows: List[Dict[str, Any]],
    *,
    row_match: Dict[str, Any],
    context: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    match_items = [
        (str(key), _format_config_value(value, context))
        for key, value in row_match.items()
    ]
    if not match_items:
        return rows[0]
    for row in rows:
        if all(str(row.get(key) or "") == str(expected) for key, expected in match_items):
            return row
    return None


def _format_context_template(
    template: str,
    context: Dict[str, str],
    row: Dict[str, Any],
) -> str:
    values = {key: str(value) for key, value in context.items()}
    values.update({str(key): str(value) for key, value in row.items() if value is not None})
    return template.format_map(_SafeFormatDict(values)).strip()


def _looks_like_html(text_lower: str, content_type_lower: str) -> bool:
    return (
        "text/html" in content_type_lower
        or "<!doctype html" in text_lower[:200]
        or "<html" in text_lower[:200]
    )


def _looks_like_xbrl_xml(text_lower: str, content_type_lower: str) -> bool:
    if "xml" not in content_type_lower and not text_lower.startswith("<?xml"):
        return False
    return "<xbrl" in text_lower or ":xbrl" in text_lower


def _looks_like_inline_xbrl(text_lower: str) -> bool:
    return (
        "inline xbrl" in text_lower
        or "inlinexbrl" in text_lower
        or "ix:nonfraction" in text_lower
        or "ix:nonnumeric" in text_lower
    )


def _looks_blocked(text_lower: str) -> bool:
    blocked_markers = (
        "captcha",
        "access denied",
        "forbidden",
        "too many requests",
        "verify you are human",
        "访问过于频繁",
        "安全验证",
    )
    return any(marker in text_lower for marker in blocked_markers)


def _collect_json_key_paths(payload: Any, prefix: str = "") -> set[str]:
    paths: set[str] = set()
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_path = f"{prefix}.{key}" if prefix else str(key)
            paths.add(key_path)
            paths.update(_collect_json_key_paths(value, key_path))
    elif isinstance(payload, list):
        for item in payload[:5]:
            paths.update(_collect_json_key_paths(item, prefix))
    return paths


def _json_has_structured_financial_facts(
    payload: Any,
    key_paths: List[str],
) -> bool:
    markers = (
        "facts",
        "numericfacts",
        "financialstatements",
        "balancesheet",
        "incomestatement",
        "cashflowstatement",
        "cashflow",
    )
    normalized = [path.replace("_", "").replace("-", "").lower() for path in key_paths]
    if any(any(marker in path for marker in markers) for path in normalized):
        return True

    if isinstance(payload, dict):
        path = str(payload.get("path") or "")
        if path.startswith("/financialData/") and _json_has_cninfo_data20_records(payload):
            return True

        sql_id = str(payload.get("sqlId") or "").upper()
        if sql_id in {
            "COMMON_MAP_BASIC_COMPANYINFO_C",
            "COMMON_MAP_BALANCESHEET_C",
            "COMMON_MAP_INCOMESTATEMENT_C",
            "COMMON_MAP_CASHFLOW_C",
            "COMMON_MAP_GBJG_C",
            "COMMON_MAP_TOP10_C",
        }:
            result = payload.get("result")
            if isinstance(result, list) and any(isinstance(row, dict) and row for row in result):
                return True

        result = payload.get("result")
        if isinstance(result, list):
            for row in result[:5]:
                if isinstance(row, dict) and any(
                    re.match(r"^S\d{4}_\d{4}$", str(key)) for key in row
                ):
                    return True
    return False


def _json_has_cninfo_data20_records(payload: Dict[str, Any]) -> bool:
    data = payload.get("data")
    if not isinstance(data, dict):
        return False
    records = data.get("records")
    if not isinstance(records, list):
        return False
    for record in records[:3]:
        if not isinstance(record, dict):
            continue
        for bucket in ("year", "middle", "one", "three"):
            rows = record.get(bucket)
            if isinstance(rows, list) and any(
                isinstance(row, dict) and row.get("index") for row in rows[:5]
            ):
                return True
    return False


def _extract_json_artifact_candidates(
    payload: Any,
    *,
    base_url: Optional[str],
    parent: Optional[Dict[str, Any]] = None,
    source_key: Optional[str] = None,
) -> List[OfficialFilingArtifactCandidate]:
    candidates: List[OfficialFilingArtifactCandidate] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_path = f"{source_key}.{key}" if source_key else str(key)
            if isinstance(value, str) and _looks_like_artifact_url(value):
                candidates.append(
                    _build_artifact_candidate(
                        value,
                        base_url=base_url,
                        source_key=key_path,
                        metadata=payload,
                    )
                )
            elif isinstance(value, (dict, list)):
                candidates.extend(
                    _extract_json_artifact_candidates(
                        value,
                        base_url=base_url,
                        parent=payload,
                        source_key=key_path,
                    )
                )
    elif isinstance(payload, list):
        for item in payload:
            candidates.extend(
                _extract_json_artifact_candidates(
                    item,
                    base_url=base_url,
                    parent=parent,
                    source_key=source_key,
                )
            )
    return candidates


class _ArtifactLinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links: List[Dict[str, Optional[str]]] = []
        self._current_link: Optional[Dict[str, Optional[str]]] = None

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        attrs_dict = dict(attrs)
        url = attrs_dict.get("href") or attrs_dict.get("src")
        if not url:
            return
        self._current_link = {"url": url, "title": None}
        self.links.append(self._current_link)

    def handle_data(self, data: str) -> None:
        if self._current_link is None:
            return
        text = data.strip()
        if text:
            existing = self._current_link.get("title")
            self._current_link["title"] = f"{existing} {text}".strip() if existing else text

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a":
            self._current_link = None


def _extract_html_artifact_candidates(
    text_sample: str,
    *,
    base_url: Optional[str],
) -> List[OfficialFilingArtifactCandidate]:
    parser = _ArtifactLinkParser()
    parser.feed(text_sample)
    candidates: List[OfficialFilingArtifactCandidate] = []
    for link in parser.links:
        url = str(link.get("url") or "")
        if not _looks_like_artifact_url(url):
            continue
        candidates.append(
            _build_artifact_candidate(
                url,
                base_url=base_url,
                source_key="html.href",
                metadata={"title": link.get("title")},
            )
        )
    return candidates


def _build_artifact_candidate(
    raw_url: str,
    *,
    base_url: Optional[str],
    source_key: Optional[str],
    metadata: Dict[str, Any],
) -> OfficialFilingArtifactCandidate:
    resolved_url = urljoin(base_url or "", raw_url)
    artifact_kind, parser_candidate, structured_payload = _infer_artifact_from_url(
        resolved_url
    )
    return OfficialFilingArtifactCandidate(
        url=resolved_url,
        artifact_kind=artifact_kind,
        parser_candidate=parser_candidate,
        structured_payload=structured_payload,
        source_key=source_key,
        filing_id=_first_metadata_value(
            metadata,
            "announcementId",
            "filingId",
            "noticeId",
            "id",
        ),
        title=_first_metadata_value(
            metadata,
            "announcementTitle",
            "title",
            "secName",
        ),
        report_period=_first_metadata_value(
            metadata,
            "reportPeriod",
            "report_period",
            "fiscalPeriod",
        ),
        report_type=_first_metadata_value(
            metadata,
            "reportType",
            "report_type",
            "category",
        ),
        metadata_json={
            "artifact_extension": _artifact_extension(resolved_url),
        },
    )


def _looks_like_artifact_url(value: str) -> bool:
    if not value:
        return False
    path = urlparse(value).path.lower()
    return any(
        path.endswith(extension)
        for extension in (
            ".xml",
            ".xbrl",
            ".zip",
            ".json",
            ".pdf",
        )
    )


def _infer_artifact_from_url(url: str) -> tuple[Optional[str], Optional[str], bool]:
    extension = _artifact_extension(url)
    if extension in {".xml", ".xbrl"}:
        return "xbrl_xml", "xbrl_numeric_facts.v1", True
    if extension == ".zip":
        return "xbrl_zip", "xbrl_numeric_facts.v1", True
    if extension == ".json":
        return "structured_json", "structured_financial_json.v1", True
    if extension in {".html", ".htm"}:
        return "structured_html", "inline_xbrl_numeric_facts.v1", True
    if extension == ".pdf":
        return "pdf", None, False
    return None, None, False


def _artifact_extension(url: str) -> str:
    path = urlparse(url).path.lower()
    for extension in (".xbrl", ".xml", ".zip", ".json", ".html", ".htm", ".pdf"):
        if path.endswith(extension):
            return extension
    return ""


def _first_metadata_value(metadata: Dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        value = metadata.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


class ConfiguredOfficialFinancialFilingProvider(BaseOfficialFinancialFilingProvider):
    """Fetch official filing payloads from configured URL templates."""

    supported_modes = {"direct"}

    def __init__(
        self,
        *,
        source_name: str,
        source_config: Dict[str, Any],
        session: Optional[requests.Session] = None,
    ):
        self.source_name = source_name
        self.source_config = source_config
        self.manifest_url = str(source_config.get("manifest_url") or "")
        self.endpoint_url = str(source_config.get("endpoint_url") or "")
        self.endpoint_candidates = [
            candidate
            for candidate in source_config.get("endpoint_candidates", [])
            if isinstance(candidate, dict)
        ]
        self.context_resolvers = [
            resolver
            for resolver in source_config.get("context_resolvers", [])
            if isinstance(resolver, dict)
        ]
        self.timeout = float(source_config.get("request_timeout_seconds", 20.0))
        self.request_interval = float(source_config.get("request_interval_seconds", 0.0))
        self.tls_config = HttpTlsConfig(
            source_name=source_name,
            extra_ca_cert_path=source_config.get("extra_ca_cert_path"),
        )
        self.session = session or create_requests_session(tls_config=self.tls_config)

    async def fetch_financial_filings(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        report_periods: List[str],
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[FinancialFilingPayload]:
        """Fetch structured filing payloads for configured endpoint templates."""
        if not self.supports_mode(mode) or not (
            self.endpoint_url or self.manifest_url or self.endpoint_candidates
        ):
            return []
        target_instruments = [
            item
            for item in instruments
            if item.get("exchange") == exchange and item.get("type", "stock") == "stock"
        ]
        if limit is not None:
            target_instruments = target_instruments[:limit]
        if not target_instruments or not report_periods:
            return []

        return await asyncio.to_thread(
            self._fetch_sync,
            target_instruments,
            exchange,
            report_periods,
            mode,
        )

    def _fetch_sync(
        self,
        instruments: List[Dict[str, Any]],
        exchange: str,
        report_periods: List[str],
        mode: str,
    ) -> List[FinancialFilingPayload]:
        payloads: List[FinancialFilingPayload] = []
        for instrument in instruments:
            for report_period in report_periods:
                context = self._context(instrument, exchange, report_period)
                if self.context_resolvers:
                    context = resolve_official_filing_context(
                        self.session,
                        context,
                        resolvers=self.context_resolvers,
                        timeout=self.timeout,
                    ).context
                if self.endpoint_url:
                    payloads.append(
                        self._download_payload(
                            url=self.endpoint_url.format_map(_SafeFormatDict(context)),
                            instrument=instrument,
                            exchange=exchange,
                            report_period=report_period,
                            mode=mode,
                        )
                    )
                elif self.endpoint_candidates:
                    payloads.extend(
                        self._fetch_from_endpoint_candidates(
                            context=context,
                            instrument=instrument,
                            exchange=exchange,
                            report_period=report_period,
                            mode=mode,
                        )
                    )
                elif self.manifest_url:
                    payloads.extend(
                        self._fetch_from_manifest(
                            context=context,
                            instrument=instrument,
                            exchange=exchange,
                            report_period=report_period,
                            mode=mode,
                        )
                    )
                if self.request_interval > 0:
                    time.sleep(self.request_interval)
        return payloads

    def _fetch_from_manifest(
        self,
        *,
        context: Dict[str, str],
        instrument: Dict[str, Any],
        exchange: str,
        report_period: str,
        mode: str,
    ) -> List[FinancialFilingPayload]:
        manifest_url = self.manifest_url.format_map(_SafeFormatDict(context))
        response = self.session.get(
            manifest_url,
            timeout=self.timeout,
            headers={"User-Agent": "QuoteResearch/official-financial-filing"},
        )
        response.raise_for_status()
        content_type = response.headers.get("Content-Type")
        candidates = extract_official_filing_artifact_candidates(
            bytes(response.content or b""),
            content_type=content_type,
            base_url=manifest_url,
        )
        payloads: List[FinancialFilingPayload] = []
        for candidate in candidates:
            if not candidate.structured_payload:
                continue
            payloads.append(
                self._download_payload(
                    url=candidate.url,
                    instrument=instrument,
                    exchange=exchange,
                    report_period=report_period,
                    mode=mode,
                    candidate=candidate,
                )
            )
            break
        return payloads

    def _fetch_from_endpoint_candidates(
        self,
        *,
        context: Dict[str, str],
        instrument: Dict[str, Any],
        exchange: str,
        report_period: str,
        mode: str,
    ) -> List[FinancialFilingPayload]:
        payloads: List[FinancialFilingPayload] = []
        for candidate in self.endpoint_candidates:
            if not bool(candidate.get("enabled", False)):
                continue
            url = str(candidate.get("url") or candidate.get("endpoint_url") or "")
            if not url:
                continue
            continue_after_success = self._continue_after_candidate_success(candidate)
            response = self._send_candidate_request(candidate, url, context)
            response.raise_for_status()
            content = bytes(response.content or b"")
            content_type = response.headers.get("Content-Type")
            classification = classify_official_filing_response(
                content,
                content_type=content_type,
                http_status=getattr(response, "status_code", None),
                url=getattr(response, "url", None),
            )
            if classification.is_structured:
                payloads.append(
                    self._build_payload_from_response(
                        response=response,
                        content=content,
                        content_type=content_type,
                        classification=classification,
                        instrument=instrument,
                        exchange=exchange,
                        report_period=report_period,
                        mode=mode,
                        candidate_config=candidate,
                    )
                )
                if not continue_after_success:
                    break
                continue

            artifact_base_url = str(candidate.get("artifact_base_url") or response.url)
            artifacts = extract_official_filing_artifact_candidates(
                content,
                content_type=content_type,
                base_url=artifact_base_url,
            )
            candidate_payload_written = False
            for artifact in artifacts:
                if not artifact.structured_payload:
                    continue
                payloads.append(
                    self._download_payload(
                        url=artifact.url,
                        instrument=instrument,
                        exchange=exchange,
                        report_period=report_period,
                        mode=mode,
                        candidate=artifact,
                    )
                )
                candidate_payload_written = True
                break
            if candidate_payload_written and not continue_after_success:
                break
        return payloads

    def _continue_after_candidate_success(self, candidate: Dict[str, Any]) -> bool:
        if "continue_after_success" in candidate:
            return bool(candidate.get("continue_after_success"))
        return bool(
            self.source_config.get("fetch_all_endpoint_candidates")
            or self.source_config.get("collect_all_endpoint_candidates")
        )

    def _send_candidate_request(
        self,
        candidate: Dict[str, Any],
        url: str,
        context: Dict[str, str],
    ) -> requests.Response:
        request_config = candidate.get("request") or candidate.get("request_config") or {}
        method = str(request_config.get("method") or "GET").upper()
        headers = {
            "User-Agent": "QuoteResearch/official-financial-filing",
            **_format_mapping(request_config.get("headers") or {}, context),
        }
        kwargs: Dict[str, Any] = {
            "headers": headers,
            "timeout": self.timeout,
        }
        query_params = _format_mapping(
            request_config.get("query_params") or request_config.get("params") or {},
            context,
        )
        body_params = _format_mapping(
            request_config.get("body_params") or request_config.get("body") or {},
            context,
        )
        json_body = _format_mapping(request_config.get("json") or {}, context)
        if query_params:
            kwargs["params"] = query_params
        if body_params:
            kwargs["data"] = body_params
        if json_body:
            kwargs["json"] = json_body
        formatted_url = url.format_map(_SafeFormatDict(context))
        if method == "GET":
            return self.session.get(formatted_url, **kwargs)
        request = getattr(self.session, "request")
        return request(method, formatted_url, **kwargs)

    def _build_payload_from_response(
        self,
        *,
        response: requests.Response,
        content: bytes,
        content_type: Optional[str],
        classification: OfficialFilingResponseClassification,
        instrument: Dict[str, Any],
        exchange: str,
        report_period: str,
        mode: str,
        candidate_config: Dict[str, Any],
    ) -> FinancialFilingPayload:
        content_hash = hashlib.sha256(content).hexdigest()
        symbol = str(
            instrument.get("symbol")
            or str(instrument.get("instrument_id") or "").split(".")[0]
        )
        metadata_json = {
            "content_type": content_type,
            "endpoint_candidate_key": candidate_config.get("key"),
            "promotion_gate": candidate_config.get("promotion_gate"),
            "evidence": candidate_config.get("evidence") or {},
            "source_profile": source_profile_metadata(
                exchange,
                self.source_name,
                strict=False,
            ),
            **classification.as_probe_fields(),
        }
        manifest = FinancialSourceFileManifest(
            source=self.source_name,
            source_mode=mode,
            instrument_id=str(instrument.get("instrument_id") or ""),
            symbol=symbol,
            exchange=exchange,
            report_period=report_period,
            report_type=str(self.source_config.get("report_type") or ""),
            filing_id=str(candidate_config.get("key") or "") or None,
            source_url=str(getattr(response, "url", "") or ""),
            content_hash=content_hash,
            content_length=len(content),
            parser_version=str(
                self.source_config.get(
                    "parser_version",
                    "financial_structured_filing.v1",
                )
            ),
            status="downloaded",
            metadata_json=metadata_json,
        )
        return FinancialFilingPayload(
            manifest=manifest,
            content=content,
            text=response.text,
            content_type=content_type,
        )

    def _download_payload(
        self,
        *,
        url: str,
        instrument: Dict[str, Any],
        exchange: str,
        report_period: str,
        mode: str,
        candidate: Optional[OfficialFilingArtifactCandidate] = None,
    ) -> FinancialFilingPayload:
        response = self.session.get(
            url,
            timeout=self.timeout,
            headers={"User-Agent": "QuoteResearch/official-financial-filing"},
        )
        response.raise_for_status()
        content = bytes(response.content or b"")
        content_hash = hashlib.sha256(content).hexdigest()
        content_type = response.headers.get("Content-Type")
        classification = classify_official_filing_response(
            content,
            content_type=content_type,
            http_status=getattr(response, "status_code", None),
            url=url,
        )
        symbol = str(instrument.get("symbol") or str(instrument.get("instrument_id") or "").split(".")[0])
        metadata_json = {
            "content_type": content_type,
            "source_profile": source_profile_metadata(
                exchange,
                self.source_name,
                strict=False,
            ),
            **classification.as_probe_fields(),
        }
        if candidate is not None:
            metadata_json["artifact_candidate"] = candidate.as_probe_fields()
            if candidate.artifact_kind:
                metadata_json["artifact_kind"] = candidate.artifact_kind
            if candidate.parser_candidate:
                metadata_json["parser_candidate"] = candidate.parser_candidate
        manifest = FinancialSourceFileManifest(
            source=self.source_name,
            source_mode=mode,
            instrument_id=str(instrument.get("instrument_id") or ""),
            symbol=symbol,
            exchange=exchange,
            report_period=report_period,
            report_type=str(self.source_config.get("report_type") or ""),
            filing_id=None if candidate is None else candidate.filing_id,
            source_url=url,
            content_hash=content_hash,
            content_length=len(content),
            parser_version=str(
                self.source_config.get(
                    "parser_version",
                    "financial_structured_filing.v1",
                )
            ),
            status="downloaded",
            metadata_json=metadata_json,
        )
        return FinancialFilingPayload(
            manifest=manifest,
            content=content,
            text=response.text,
            content_type=content_type,
        )

    @staticmethod
    def _context(
        instrument: Dict[str, Any],
        exchange: str,
        report_period: str,
    ) -> Dict[str, str]:
        instrument_id = str(instrument.get("instrument_id") or "")
        symbol = str(instrument.get("symbol") or instrument_id.split(".")[0])
        return {
            "instrument_id": instrument_id,
            "symbol": symbol,
            "stockid": symbol,
            "exchange": exchange,
            "report_period": report_period,
            "report_year": _report_year(report_period),
            "report_type_id": _sse_report_type_id(report_period),
        }


class _SafeFormatDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _format_mapping(raw: Dict[str, Any], context: Dict[str, str]) -> Dict[str, Any]:
    formatted: Dict[str, Any] = {}
    for key, value in raw.items():
        formatted[str(key)] = _format_config_value(value, context)
    return formatted


def _format_config_value(value: Any, context: Dict[str, str]) -> Any:
    if isinstance(value, str):
        return value.format_map(_SafeFormatDict(context))
    if isinstance(value, list):
        return [_format_config_value(item, context) for item in value]
    if isinstance(value, dict):
        return _format_mapping(value, context)
    return value


def _report_year(report_period: str) -> str:
    value = str(report_period or "")
    return value[:4] if len(value) >= 4 and value[:4].isdigit() else value


def _sse_report_type_id(report_period: str) -> str:
    value = str(report_period or "").upper()
    if value.endswith("Q1") or value.endswith("-03-31"):
        return "4000"
    if value.endswith("Q2") or value.endswith("-06-30"):
        return "1000"
    if value.endswith("Q3") or value.endswith("-09-30"):
        return "4400"
    if value.endswith("Q4") or value.endswith("FY") or value.endswith("-12-31"):
        return "5000"
    return "5000"
