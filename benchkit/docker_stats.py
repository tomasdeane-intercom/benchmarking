from __future__ import annotations

import csv
import re
import subprocess
import threading
import time
from pathlib import Path

from benchkit.host_compose import run_compose
from benchkit.timing import iso_z, utc_now


HEADER = [
    "timestamp",
    "container",
    "cpu_pct",
    "mem_usage_mb",
    "mem_limit_mb",
    "mem_pct",
    "net_in_mb",
    "net_out_mb",
]

SIZE_RE = re.compile(r"^\s*([0-9.]+)\s*([A-Za-z]+)?\s*$")
UNIT_SCALE_MB = {
    "B": 1 / 1024 / 1024,
    "KB": 1 / 1024,
    "KIB": 1 / 1024,
    "MB": 1.0,
    "MIB": 1.0,
    "GB": 1024.0,
    "GIB": 1024.0,
    "TB": 1024.0 * 1024.0,
    "TIB": 1024.0 * 1024.0,
}
DEFAULT_DOCKER_STATS_INTERVAL_SECONDS = 1.0


class DockerStatsCollector:
    def __init__(
        self,
        output_path: Path,
        *,
        compose_project: str | None = None,
        interval_seconds: float = DEFAULT_DOCKER_STATS_INTERVAL_SECONDS,
    ) -> None:
        self.output_path = output_path
        self.compose_project = compose_project
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(HEADER)
        self._thread = threading.Thread(target=self._run_loop, name="docker-stats", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_seconds + 1.0))

    def run_forever(self) -> None:
        self.start()
        try:
            while not self._stop.is_set():
                time.sleep(0.2)
        finally:
            self.stop()

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            rows = collect_stats_rows(self.compose_project)
            if rows:
                with self.output_path.open("a", encoding="utf-8", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerows(rows)
            self._stop.wait(self.interval_seconds)


def collect_stats_rows(compose_project: str | None = None) -> list[list[str]]:
    ids = _compose_container_ids(compose_project)
    if not ids:
        return []

    completed = subprocess.run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}}",
            *ids,
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        return []

    timestamp = iso_z(utc_now(), timespec="microseconds")
    rows: list[list[str]] = []
    for raw_line in completed.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(",", 4)
        if len(parts) != 5:
            continue
        name, cpu_raw, mem_raw, mem_pct, net_raw = parts
        if compose_project and not name.startswith(f"{compose_project}-"):
            continue
        mem_usage, mem_limit = _split_pair(mem_raw)
        net_in, net_out = _split_pair(net_raw)
        rows.append(
            [
                timestamp,
                name,
                _clean_percent(cpu_raw),
                f"{to_mb(mem_usage):.2f}",
                f"{to_mb(mem_limit):.2f}",
                _clean_percent(mem_pct),
                f"{to_mb(net_in):.2f}",
                f"{to_mb(net_out):.2f}",
            ]
        )
    return rows


def to_mb(value: str) -> float:
    match = SIZE_RE.match(value)
    if not match:
        return 0.0
    number = float(match.group(1))
    unit = (match.group(2) or "MB").upper()
    return number * UNIT_SCALE_MB.get(unit, 1.0)


def _compose_container_ids(compose_project: str | None = None) -> list[str]:
    if compose_project:
        completed = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                f"label=com.docker.compose.project={compose_project}",
                "--filter",
                "status=running",
                "--format",
                "{{.ID}}",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        return [line.strip() for line in completed.stdout.splitlines() if line.strip()]

    completed = run_compose(["ps", "-q"], capture_output=True, check=False)
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _split_pair(value: str) -> tuple[str, str]:
    left, _, right = value.partition("/")
    return left.strip(), right.strip()


def _clean_percent(value: str) -> str:
    return value.replace("%", "").strip() or "0"
