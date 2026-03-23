from __future__ import annotations

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


WORKER_LIMIT = int(os.getenv("STUB_SERVER_WORKERS", "20"))
SEMAPHORE = threading.BoundedSemaphore(WORKER_LIMIT)


class DelayHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if not self.path.startswith("/delay/"):
            self.send_error(404)
            return
        try:
            delay_ms = int(self.path.rsplit("/", 1)[-1])
        except ValueError:
            self.send_error(400)
            return
        with SEMAPHORE:
            time.sleep(delay_ms / 1000.0)
            body = json.dumps({"ok": True, "delay_ms": delay_ms}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, *_args) -> None:
        return


def main() -> int:
    server = ThreadingHTTPServer(("0.0.0.0", 8080), DelayHandler)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
