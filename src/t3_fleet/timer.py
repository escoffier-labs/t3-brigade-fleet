from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Mapping

from .errors import FleetError, ValidationError

SETTLE_UNIT = "t3-fleet-settle-sweep"
_MAX_INTERVAL_MINUTES = 24 * 60

Runner = Callable[..., "subprocess.CompletedProcess[str]"]


def _quote(value: str) -> str:
    """One systemd ExecStart word: `%` is a specifier and `"`/`\\` need escapes."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    if not escaped or any(c in escaped for c in " \t'$;"):
        return f'"{escaped}"'
    return escaped


def _source_path() -> str | None:
    """The directory to put on PYTHONPATH when t3_fleet runs from a checkout.

    A user unit has none of the interactive shell's environment, so a source
    checkout would not import there. An installed copy needs nothing extra.
    """
    package = Path(__file__).resolve().parent
    root = package.parent
    if any(part in {"site-packages", "dist-packages"} for part in package.parts):
        return None
    return str(root)


def render_settle_units(
    *,
    config_path: Path,
    state_path: Path,
    interval_minutes: int,
    python: str | None = None,
) -> dict[str, str]:
    """The oneshot service and repeating timer that run `t3-fleet settle-sweep`.

    Nothing here involves a model: the timer starts one deterministic sweep
    that the controller config gates with `[settle] enabled`.
    """
    if not 1 <= interval_minutes <= _MAX_INTERVAL_MINUTES:
        raise ValidationError("interval minutes must be between 1 and 1440")
    exec_start = " ".join(
        _quote(word)
        for word in (
            python or sys.executable,
            "-m",
            "t3_fleet",
            "--config",
            str(config_path),
            "--state",
            str(state_path),
            "settle-sweep",
        )
    )
    source = _source_path()
    environment = f"Environment={_quote('PYTHONPATH=' + source)}\n" if source else ""
    service = (
        "[Unit]\n"
        "Description=t3-fleet settle sweep: settle finished fleet threads\n"
        "\n"
        "[Service]\n"
        "Type=oneshot\n"
        f"{environment}"
        f"ExecStart={exec_start}\n"
    )
    timer = (
        "[Unit]\n"
        f"Description=Run the t3-fleet settle sweep every {interval_minutes} minutes\n"
        "\n"
        "[Timer]\n"
        "OnBootSec=2min\n"
        f"OnUnitActiveSec={interval_minutes}min\n"
        "AccuracySec=30s\n"
        "\n"
        "[Install]\n"
        "WantedBy=timers.target\n"
    )
    return {f"{SETTLE_UNIT}.service": service, f"{SETTLE_UNIT}.timer": timer}


def user_unit_dir(environ: Mapping[str, str]) -> Path:
    root = environ.get("XDG_CONFIG_HOME")
    return (Path(root) if root else Path.home() / ".config") / "systemd" / "user"


def install_settle_timer(
    *,
    config_path: Path,
    state_path: Path,
    unit_dir: Path,
    interval_minutes: int,
    write: bool,
    enable: bool,
    settle_enabled: bool,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    """Render the settle units, write them when asked, and optionally enable the timer."""
    units = render_settle_units(
        config_path=config_path.resolve(),
        state_path=state_path.resolve(),
        interval_minutes=interval_minutes,
    )
    result: dict[str, Any] = {
        "schema": "t3-fleet.result.v1",
        "action": "install-settle-timer",
        "state": "rendered",
        "settle_enabled": settle_enabled,
        "interval_minutes": interval_minutes,
        "units": {name: str(unit_dir / name) for name in units},
    }
    if not write:
        result["content"] = units
        return result
    unit_dir.mkdir(parents=True, exist_ok=True)
    for name, text in units.items():
        path = unit_dir / name
        if not path.exists() or path.read_text(encoding="utf-8") != text:
            path.write_text(text, encoding="utf-8")
    result["state"] = "installed"
    commands = [
        ["systemctl", "--user", "daemon-reload"],
        ["systemctl", "--user", "enable", "--now", f"{SETTLE_UNIT}.timer"],
    ]
    if not enable:
        result["next"] = [" ".join(command) for command in commands]
        return result
    for command in commands:
        try:
            completed = runner(command, capture_output=True, text=True, timeout=30, check=False)
        except (OSError, subprocess.TimeoutExpired) as error:
            raise FleetError("systemctl could not be run") from error
        if completed.returncode != 0:
            raise FleetError("systemctl --user failed to enable the settle timer")
    result["state"] = "enabled"
    return result
