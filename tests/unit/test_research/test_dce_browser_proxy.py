from __future__ import annotations

import base64
import socket
import socketserver
import threading

import requests

from research.providers.dce_browser_proxy import DceBrowserProxyForwarder


class _FakeUpstreamHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data = bytearray()
        while b"\r\n\r\n" not in data:
            data.extend(self.request.recv(8192))
        self.server.requests.append(bytes(data))
        if data.startswith(b"CONNECT "):
            self.request.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            payload = self.request.recv(16)
            self.server.tunnel_payloads.append(payload)
            self.request.sendall(b"pong")
            return
        self.request.sendall(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
        )


def _start_upstream():
    server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), _FakeUpstreamHandler)
    server.daemon_threads = True
    server.requests = []
    server.tunnel_payloads = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def test_dce_browser_proxy_forwards_http_absolute_form_with_authentication():
    upstream, thread = _start_upstream()
    host, port = upstream.server_address
    forwarder = DceBrowserProxyForwarder(f"http://proxy-user:proxy-pass@{host}:{port}")
    forwarder.start()
    try:
        response = requests.get(
            "http://official.example/path?date=20260819",
            proxies={"http": forwarder.browser_proxy_url},
            timeout=2,
        )
    finally:
        forwarder.stop()
        upstream.shutdown()
        upstream.server_close()
        thread.join(timeout=2)

    assert response.text == "ok"
    request = upstream.requests[0]
    assert request.startswith(
        b"GET http://official.example/path?date=20260819 HTTP/1.1"
    )
    expected = base64.b64encode(b"proxy-user:proxy-pass")
    assert b"Proxy-Authorization: Basic " + expected in request


def test_dce_browser_proxy_forwards_https_connect_tunnel():
    upstream, thread = _start_upstream()
    host, port = upstream.server_address
    forwarder = DceBrowserProxyForwarder(f"http://proxy-user:proxy-pass@{host}:{port}")
    forwarder.start()
    proxy = socket.create_connection(
        ("127.0.0.1", int(forwarder.browser_proxy_url.rsplit(":", 1)[1])),
        timeout=2,
    )
    try:
        proxy.sendall(
            b"CONNECT official.example:443 HTTP/1.1\r\n"
            b"Host: official.example:443\r\n\r\n"
        )
        response = proxy.recv(4096)
        assert response.startswith(b"HTTP/1.1 200")
        proxy.sendall(b"ping")
        assert proxy.recv(4) == b"pong"
    finally:
        proxy.close()
        forwarder.stop()
        upstream.shutdown()
        upstream.server_close()
        thread.join(timeout=2)

    assert upstream.tunnel_payloads == [b"ping"]
    expected = base64.b64encode(b"proxy-user:proxy-pass")
    assert b"Proxy-Authorization: Basic " + expected in upstream.requests[0]
