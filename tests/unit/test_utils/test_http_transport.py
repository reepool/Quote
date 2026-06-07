import ssl
from pathlib import Path

from utils.http_transport import (
    HttpTlsConfig,
    build_ca_bundle_with_extra_certificate,
    create_requests_session,
    create_ssl_context,
    resolve_project_relative_path,
    resolve_requests_verify,
    urlopen_bytes,
)


def test_extra_ca_bundle_resolves_project_relative_path_from_other_cwd(
    monkeypatch,
    tmp_path,
):
    cert_path = "config/certs/geotrust_g2_tls_cn_rsa4096_sha256_2022_ca1.crt"
    monkeypatch.chdir(tmp_path)

    bundle = build_ca_bundle_with_extra_certificate(cert_path)

    assert bundle is not True
    assert Path(str(bundle)).exists()
    resolved_cert_path = resolve_project_relative_path(cert_path)
    assert resolved_cert_path.is_absolute()
    assert resolved_cert_path.read_text().strip() in Path(str(bundle)).read_text()


def test_create_requests_session_sets_shared_verify_policy():
    cert_path = "config/certs/geotrust_g2_tls_cn_rsa4096_sha256_2022_ca1.crt"
    tls_config = HttpTlsConfig(source_name="swsresearch", extra_ca_cert_path=cert_path)

    session = create_requests_session(tls_config=tls_config)

    assert session.verify is not True
    assert Path(str(session.verify)).exists()


def test_swsresearch_source_uses_known_extra_ca_by_default():
    verify = resolve_requests_verify(HttpTlsConfig(source_name="swsresearch"))

    assert verify is not True
    assert Path(str(verify)).exists()


def test_create_ssl_context_uses_extra_ca_bundle():
    cert_path = "config/certs/geotrust_g2_tls_cn_rsa4096_sha256_2022_ca1.crt"
    tls_config = HttpTlsConfig(source_name="hkex", extra_ca_cert_path=cert_path)

    context = create_ssl_context(tls_config)

    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED


def test_urlopen_bytes_passes_shared_ssl_context(monkeypatch):
    captured = {}

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b"ok"

    def fake_urlopen(request, *, timeout, context):
        captured["timeout"] = timeout
        captured["context"] = context
        captured["headers"] = dict(request.header_items())
        return _Response()

    monkeypatch.setattr("utils.http_transport.urllib.request.urlopen", fake_urlopen)
    cert_path = "config/certs/geotrust_g2_tls_cn_rsa4096_sha256_2022_ca1.crt"

    payload = urlopen_bytes(
        "https://example.test/file",
        timeout_sec=3.0,
        user_agent="QuoteTest",
        tls_config=HttpTlsConfig(source_name="hkex", extra_ca_cert_path=cert_path),
    )

    assert payload == b"ok"
    assert captured["timeout"] == 3.0
    assert isinstance(captured["context"], ssl.SSLContext)
    assert captured["headers"]["User-agent"] == "QuoteTest"
