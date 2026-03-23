from __future__ import annotations

import json
import logging

from benchkit.metrics import MetricsStore
from benchkit.workloads import execute_message, make_error_record


LOGGER = logging.getLogger(__name__)


def handle_message(raw_message: str, metrics: MetricsStore, stub_server_url: str, system: str) -> None:
    message = json.loads(raw_message)
    run_id = message["run_id"]
    try:
        message["system"] = system
        record = execute_message(message, stub_server_url=stub_server_url)
        metrics.record_success(run_id, record)
    except Exception as exc:
        LOGGER.exception("worker failed for run %s", run_id)
        metrics.record_error(run_id, make_error_record(exc))
