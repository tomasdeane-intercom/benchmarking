from __future__ import annotations

from datetime import UTC, datetime


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def iso_z(dt: datetime, *, timespec: str = "auto") -> str:
    return dt.astimezone(UTC).isoformat(timespec=timespec).replace("+00:00", "Z")


def iso_compact(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y%m%d_%H%M%S")


def parse_iso(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(UTC)
