from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from benchkit.paths import (
    RESULT_SCHEMA_PATH,
    STUB_CALIBRATION_SCHEMA_PATH,
    TRACE_SCHEMA_PATH,
)


class SchemaError(ValueError):
    pass


@lru_cache(maxsize=3)
def _load_schema(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


@lru_cache(maxsize=3)
def _validator(path: Path) -> Draft202012Validator:
    return Draft202012Validator(_load_schema(path))


def validate_result(data: dict[str, Any]) -> None:
    version = data.get("schema_version")
    if version == "3.5.0":
        _validate(_validator(RESULT_SCHEMA_PATH), data)
        return
    raise SchemaError(f"Unsupported result schema version: {version!r}")


def validate_trace_record(data: dict[str, Any]) -> None:
    _validate(_validator(TRACE_SCHEMA_PATH), data)


def validate_stub_calibration(data: dict[str, Any]) -> None:
    _validate(_validator(STUB_CALIBRATION_SCHEMA_PATH), data)


def load_json(path: Path) -> dict[str, Any] | list[Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _validate(validator: Draft202012Validator, data: Any) -> None:
    errors = sorted(validator.iter_errors(data), key=lambda err: list(err.path))
    if not errors:
        return
    first = errors[0]
    location = ".".join(str(part) for part in first.path) or "<root>"
    raise SchemaError(f"{location}: {first.message}")
