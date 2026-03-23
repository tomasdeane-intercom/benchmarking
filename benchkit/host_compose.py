from __future__ import annotations

import os
import shlex
import subprocess
import sys
from collections.abc import Sequence


def compose_base_command() -> list[str]:
    return shlex.split(os.getenv("COMPOSE", "docker compose"))


def run_compose(
    args: Sequence[str],
    *,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
    check: bool = True,
    timeout: float | None = None,
    stdin=None,
) -> subprocess.CompletedProcess[str]:
    command = [*compose_base_command(), *args]
    completed = subprocess.run(
        command,
        env=_merged_env(env),
        text=True,
        capture_output=capture_output,
        check=False,
        timeout=timeout,
        stdin=stdin,
    )
    if check and completed.returncode != 0:
        _raise(command, completed)
    return completed


def run_orchestrator(
    module: str,
    module_args: Sequence[str] | None = None,
    *,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
    check: bool = True,
    timeout: float | None = None,
    stdin=None,
) -> subprocess.CompletedProcess[str]:
    module_args = list(module_args or [])
    return run_compose(
        ["run", "--rm", "--no-deps", "orchestrator", "python", "-m", module, *module_args],
        env=env,
        capture_output=capture_output,
        check=check,
        timeout=timeout,
        stdin=stdin,
    )


def _merged_env(extra: dict[str, str] | None) -> dict[str, str]:
    env = os.environ.copy()
    if extra:
        env.update({key: str(value) for key, value in extra.items()})
    return env


def _raise(command: Sequence[str], completed: subprocess.CompletedProcess[str]) -> None:
    if completed.stderr:
        sys.stderr.write(completed.stderr)
    if completed.stdout and not completed.stderr:
        sys.stderr.write(completed.stdout)
    raise subprocess.CalledProcessError(
        completed.returncode,
        list(command),
        output=completed.stdout,
        stderr=completed.stderr,
    )
