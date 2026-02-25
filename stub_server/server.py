#!/usr/bin/env python3
"""Simple HTTP stub server for I/O-bound workload benchmarks.

Endpoints:
  GET /health        - Health check
  GET /delay/<ms>    - Responds after <ms> milliseconds
  GET /echo          - Echoes request headers as JSON
"""

import time
from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/delay/<int:ms>")
def delay(ms):
    capped = min(ms, 10000)  # Cap at 10 seconds
    time.sleep(capped / 1000.0)
    return jsonify({"delayed_ms": capped, "timestamp": time.time()})


@app.route("/echo")
def echo():
    return jsonify({
        "headers": dict(request.headers),
        "timestamp": time.time(),
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
