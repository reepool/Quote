"""Credential-safe local proxy bridge for DCE's headed browser."""

from __future__ import annotations

import base64
import select
import socket
import socketserver
import threading
from typing import Optional, Tuple
from urllib.parse import unquote, urlsplit


_MAX_HEADER_BYTES = 64 * 1024


class _ForwardingServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address: Tuple[str, int], forwarder: "DceBrowserProxyForwarder"):
        self.forwarder = forwarder
        super().__init__(address, _ForwardingHandler)


class _ForwardingHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        try:
            self.server.forwarder._handle(self.request)  # type: ignore[attr-defined]
        except (ConnectionError, OSError, TimeoutError):
            return


class DceBrowserProxyForwarder:
    """Forward Chrome proxy traffic through one authenticated upstream lease."""

    def __init__(self, proxy_url: str, *, socket_timeout_seconds: float = 30.0):
        parsed = urlsplit(str(proxy_url or ""))
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or not parsed.port
        ):
            raise ValueError("DCE upstream proxy URL is invalid")
        self._upstream_host = parsed.hostname
        self._upstream_port = parsed.port
        self._socket_timeout_seconds = max(1.0, float(socket_timeout_seconds))
        self._proxy_authorization = ""
        if parsed.username is not None:
            credentials = f"{unquote(parsed.username)}:{unquote(parsed.password or '')}"
            token = base64.b64encode(credentials.encode("utf-8")).decode("ascii")
            self._proxy_authorization = f"Basic {token}"
        self._server: Optional[_ForwardingServer] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def browser_proxy_url(self) -> str:
        if self._server is None:
            raise RuntimeError("DCE browser proxy forwarder is not started")
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def start(self) -> None:
        if self._server is not None:
            return
        server = _ForwardingServer(("127.0.0.1", 0), self)
        thread = threading.Thread(
            target=server.serve_forever,
            name="dce-browser-proxy",
            daemon=True,
        )
        thread.start()
        self._server = server
        self._thread = thread

    def stop(self) -> None:
        server, thread = self._server, self._thread
        self._server = None
        self._thread = None
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)

    def _handle(self, client: socket.socket) -> None:
        client.settimeout(self._socket_timeout_seconds)
        initial = self._read_headers(client)
        if not initial:
            return
        first_line = initial.split(b"\r\n", 1)[0]
        parts = first_line.split(b" ", 2)
        if len(parts) != 3:
            return
        method = parts[0].upper()
        upstream = socket.create_connection(
            (self._upstream_host, self._upstream_port),
            timeout=self._socket_timeout_seconds,
        )
        try:
            upstream.settimeout(self._socket_timeout_seconds)
            upstream.sendall(self._with_proxy_authorization(initial))
            if method == b"CONNECT":
                response = self._read_headers(upstream)
                if response:
                    client.sendall(response)
                if not self._is_connect_success(response):
                    return
            self._relay(client, upstream)
        finally:
            upstream.close()

    def _with_proxy_authorization(self, headers: bytes) -> bytes:
        head, separator, tail = headers.partition(b"\r\n\r\n")
        lines = [
            line
            for line in head.split(b"\r\n")
            if not line.lower().startswith(b"proxy-authorization:")
        ]
        if self._proxy_authorization:
            lines.append(
                f"Proxy-Authorization: {self._proxy_authorization}".encode("ascii")
            )
        return b"\r\n".join(lines) + separator + tail

    @staticmethod
    def _read_headers(sock: socket.socket) -> bytes:
        data = bytearray()
        while b"\r\n\r\n" not in data and len(data) < _MAX_HEADER_BYTES:
            chunk = sock.recv(min(8192, _MAX_HEADER_BYTES - len(data)))
            if not chunk:
                break
            data.extend(chunk)
        if b"\r\n\r\n" not in data:
            return b""
        return bytes(data)

    @staticmethod
    def _is_connect_success(response: bytes) -> bool:
        first_line = response.split(b"\r\n", 1)[0]
        parts = first_line.split(b" ", 2)
        return len(parts) >= 2 and parts[1] == b"200"

    @staticmethod
    def _relay(left: socket.socket, right: socket.socket) -> None:
        sockets = [left, right]
        while sockets:
            readable, _, _ = select.select(sockets, [], [], 30.0)
            if not readable:
                return
            for source in readable:
                try:
                    data = source.recv(64 * 1024)
                except (ConnectionError, OSError):
                    return
                if not data:
                    return
                target = right if source is left else left
                target.sendall(data)
