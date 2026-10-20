"""Tiny HTTP server: /metrics (Prometheus) and /healthz (main loop liveness)."""

from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


class Liveness:
    def __init__(self, stale_after_s: float) -> None:
        self._stale_after_s = stale_after_s
        self._last_beat = time.monotonic()

    def beat(self) -> None:
        self._last_beat = time.monotonic()

    def healthy(self) -> bool:
        return time.monotonic() - self._last_beat < self._stale_after_s


def serve(port: int, liveness: Liveness) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
            if self.path.startswith("/metrics"):
                body, ctype, code = generate_latest(), CONTENT_TYPE_LATEST, 200
            elif self.path.startswith("/healthz"):
                ok = liveness.healthy()
                body, ctype, code = (b"ok" if ok else b"stale"), "text/plain", (200 if ok else 503)
            else:
                body, ctype, code = b"not found", "text/plain", 404
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:  # silence per-request logs
            return

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, name="http", daemon=True).start()
    return server
