"""Shared HTTP/TLS helpers for project-owned upstream requests."""

from __future__ import annotations

import hashlib
import ssl
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Union

import certifi
import requests

from .logging_manager import dm_logger


VerifySetting = Union[bool, str]

SOURCE_DEFAULT_EXTRA_CA_CERT_PATHS = {
    "akshare_swsresearch_index_analysis": "config/certs/geotrust_g2_tls_cn_rsa4096_sha256_2022_ca1.crt",
    "swsresearch": "config/certs/geotrust_g2_tls_cn_rsa4096_sha256_2022_ca1.crt",
    "swsresearch_index_analysis_direct": "config/certs/geotrust_g2_tls_cn_rsa4096_sha256_2022_ca1.crt",
}


@dataclass(frozen=True)
class HttpTlsConfig:
    """TLS options for a self-managed upstream request path."""

    source_name: Optional[str] = None
    extra_ca_cert_path: Optional[str] = None
    allow_insecure: bool = False


def resolve_project_relative_path(path_value: str) -> Path:
    """Resolve a path from cwd first, then from the repository root."""
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    cwd_path = Path.cwd() / path
    if cwd_path.exists():
        return cwd_path
    return Path(__file__).resolve().parents[1] / path


def build_ca_bundle_with_extra_certificate(extra_certificate_path: Optional[str]) -> VerifySetting:
    """Return a requests verify setting that keeps certifi plus one extra CA cert."""
    if not extra_certificate_path:
        return True

    extra_path = resolve_project_relative_path(extra_certificate_path)
    if not extra_path.exists():
        dm_logger.warning("[HTTPTransport] Extra CA certificate not found: %s", extra_path)
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


def tls_config_from_source_config(
    source_name: str,
    source_config: Optional[Mapping[str, Any]],
    *,
    extra_ca_cert_path: Optional[str] = None,
    allow_insecure: Optional[bool] = None,
) -> HttpTlsConfig:
    """Build TLS config from a source config dict plus provider-local overrides."""
    source_tls = {}
    if isinstance(source_config, Mapping):
        tls_value = source_config.get("tls") or {}
        if isinstance(tls_value, Mapping):
            source_tls = dict(tls_value)

    return HttpTlsConfig(
        source_name=source_name,
        extra_ca_cert_path=extra_ca_cert_path
        or source_tls.get("extra_ca_cert_path"),
        allow_insecure=bool(
            source_tls.get("allow_insecure", False)
            if allow_insecure is None
            else allow_insecure
        ),
    )


def resolve_requests_verify(tls_config: Optional[HttpTlsConfig] = None) -> VerifySetting:
    """Resolve the requests ``verify`` value for a source."""
    if tls_config is not None and tls_config.allow_insecure:
        dm_logger.warning(
            "[HTTPTransport] Insecure TLS verification disabled for source=%s",
            tls_config.source_name or "unknown",
        )
        return False
    if tls_config is None:
        return True
    extra_ca_cert_path = tls_config.extra_ca_cert_path
    if not extra_ca_cert_path and tls_config.source_name:
        extra_ca_cert_path = SOURCE_DEFAULT_EXTRA_CA_CERT_PATHS.get(
            tls_config.source_name
        )
    return build_ca_bundle_with_extra_certificate(extra_ca_cert_path)


def create_requests_session(
    *,
    tls_config: Optional[HttpTlsConfig] = None,
    headers: Optional[Mapping[str, str]] = None,
) -> requests.Session:
    """Create a requests session with a project TLS policy attached."""
    session = requests.Session()
    if headers:
        session.headers.update(dict(headers))
    session.verify = resolve_requests_verify(tls_config)
    return session


def request_get(
    url: str,
    *,
    tls_config: Optional[HttpTlsConfig] = None,
    session: Optional[requests.Session] = None,
    **kwargs: Any,
) -> requests.Response:
    """GET using the shared TLS policy unless the caller provides a session."""
    verify = resolve_requests_verify(tls_config)
    if session is not None:
        previous_verify = session.verify
        session.verify = verify
        try:
            return session.get(url, **kwargs)
        finally:
            session.verify = previous_verify
    return requests.get(url, verify=verify, **kwargs)


def request_post(
    url: str,
    *,
    tls_config: Optional[HttpTlsConfig] = None,
    session: Optional[requests.Session] = None,
    **kwargs: Any,
) -> requests.Response:
    """POST using the shared TLS policy unless the caller provides a session."""
    verify = resolve_requests_verify(tls_config)
    if session is not None:
        previous_verify = session.verify
        session.verify = verify
        try:
            return session.post(url, **kwargs)
        finally:
            session.verify = previous_verify
    return requests.post(url, verify=verify, **kwargs)


def create_ssl_context(tls_config: Optional[HttpTlsConfig] = None) -> Optional[ssl.SSLContext]:
    """Create a urllib-compatible SSL context for the configured TLS policy."""
    verify = resolve_requests_verify(tls_config)
    if verify is True:
        return None
    if verify is False:
        context = ssl._create_unverified_context()
        return context
    return ssl.create_default_context(cafile=str(verify))


def urlopen_bytes(
    source_url: str,
    *,
    timeout_sec: float,
    user_agent: str,
    tls_config: Optional[HttpTlsConfig] = None,
) -> bytes:
    """Fetch bytes through urllib using shared TLS context rules."""
    request = urllib.request.Request(
        source_url,
        headers={"User-Agent": user_agent},
    )
    context = create_ssl_context(tls_config)
    with urllib.request.urlopen(request, timeout=timeout_sec, context=context) as response:
        return response.read()
