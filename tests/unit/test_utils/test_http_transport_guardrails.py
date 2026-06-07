from pathlib import Path


PRODUCTION_ROOTS = [
    Path("research"),
    Path("data_sources"),
    Path("utils"),
]

ALLOWLIST = {
    Path("utils/http_transport.py"),
}

THIRD_PARTY_SOURCE_BOUNDARY = {
    Path("data_sources/yfinance_source.py"),
}


def test_production_code_does_not_disable_tls_verification():
    offenders = []
    for root in PRODUCTION_ROOTS:
        for path in root.rglob("*.py"):
            if path in ALLOWLIST:
                continue
            text = path.read_text(encoding="utf-8")
            if "verify=False" in text or "verify = False" in text:
                offenders.append(str(path))

    assert offenders == []


def test_production_code_uses_shared_http_transport_for_new_direct_requests():
    banned_markers = [
        "requests.get(",
        "requests.post(",
        "requests.Session()",
        "urllib.request.urlopen(",
    ]
    offenders = []
    for root in PRODUCTION_ROOTS:
        for path in root.rglob("*.py"):
            if path in ALLOWLIST or path in THIRD_PARTY_SOURCE_BOUNDARY:
                continue
            text = path.read_text(encoding="utf-8")
            if any(marker in text for marker in banned_markers):
                offenders.append(str(path))

    assert offenders == []
