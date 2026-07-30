from __future__ import annotations

import ipaddress
import os
import re
import stat
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from .errors import ConfigError

_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
_SHELL_META = re.compile(r"[;&|`$<>\r\n]")
_CREDENTIAL_WORD = re.compile(r"(token|secret|password|credential|private[_-]?key)", re.I)
_TOP_KEYS = {
    "schema",
    "controller_host",
    "controller_id",
    "coordination_mode",
    "global_active_limit",
    "ssh_command",
    "state_dir",
    "hosts",
    "repositories",
}
_HOST_KEYS = {
    "enabled",
    "platform",
    "transport",
    "ssh_alias",
    "agent_command",
    "active_limit",
    "expected_hostname",
    "t3_url",
    "t3_command",
    "t3_base_dir",
    "agent_state_dir",
}
_REPO_KEYS = {"default_branch", "branch_prefix", "paths"}


@dataclass(frozen=True)
class HostConfig:
    host_id: str
    enabled: bool
    platform: str
    transport: str
    ssh_alias: str | None
    agent_command: tuple[str, ...]
    active_limit: int
    expected_hostname: str
    t3_url: str
    t3_command: tuple[str, ...]
    t3_base_dir: str | None
    agent_state_dir: str | None


@dataclass(frozen=True)
class RepositoryConfig:
    repository_id: str
    default_branch: str
    branch_prefix: str
    paths: dict[str, str]


@dataclass(frozen=True)
class FleetConfig:
    controller_host: str
    global_active_limit: int
    state_dir: str | None
    hosts: dict[str, HostConfig]
    repositories: dict[str, RepositoryConfig]
    controller_id: str | None = None
    coordination_mode: str = "single-controller"
    ssh_command: tuple[str, ...] = ("ssh",)


def _unknown_keys(data: dict[str, Any], allowed: set[str], label: str) -> None:
    unknown = sorted(set(data) - allowed)
    if not unknown:
        return
    if any(_CREDENTIAL_WORD.search(key) for key in unknown):
        raise ConfigError(f"{label} contains an embedded credential field")
    if label == "fleet config":
        raise ConfigError(f"unknown top-level key(s): {', '.join(unknown)}")
    raise ConfigError(f"{label} contains unknown key(s): {', '.join(unknown)}")


def _identifier(value: str, label: str) -> str:
    if not _ID.fullmatch(value):
        raise ConfigError(f"invalid {label}: {value!r}")
    return value


def _positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ConfigError(f"{label} must be a positive integer")
    return value


def _command(value: Any, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or not all(isinstance(item, str) and item for item in value):
        raise ConfigError(f"{label} must be a non-empty string array")
    if any(_SHELL_META.search(item) for item in value):
        raise ConfigError(f"{label} contains forbidden shell metacharacters")
    return tuple(value)


def _ssh_alias(value: Any, transport: str) -> str | None:
    if transport == "local":
        if value is not None:
            raise ConfigError("local host must not define ssh_alias")
        return None
    if not isinstance(value, str) or not value:
        raise ConfigError("ssh host requires ssh_alias")
    try:
        ipaddress.ip_address(value.strip("[]"))
    except ValueError:
        return value
    raise ConfigError("IP literals are forbidden in ssh_alias; use a reviewed SSH alias")


def _absolute_path(value: Any, platform: str, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{label} must be a non-empty path")
    path = PureWindowsPath(value) if platform == "windows" else PurePosixPath(value)
    if not path.is_absolute():
        raise ConfigError(f"{label} must be absolute for {platform}")
    return value


def _check_permissions(path: Path) -> None:
    if os.name == "nt":
        return
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise ConfigError("fleet config must be owner-only (mode 0600 or stricter)")


def load_config(path: str | Path) -> FleetConfig:
    config_path = Path(path)
    _check_permissions(config_path)
    with config_path.open("rb") as handle:
        raw = tomllib.load(handle)
    if not isinstance(raw, dict):
        raise ConfigError("fleet config must be a TOML table")
    _unknown_keys(raw, _TOP_KEYS, "fleet config")
    if raw.get("schema") != "t3-fleet.config.v1":
        raise ConfigError("unsupported or missing fleet config schema")

    hosts_raw = raw.get("hosts")
    if not isinstance(hosts_raw, dict) or not hosts_raw:
        raise ConfigError("hosts must be a non-empty table")
    hosts: dict[str, HostConfig] = {}
    for host_id, value in hosts_raw.items():
        _identifier(host_id, "host id")
        if not isinstance(value, dict):
            raise ConfigError(f"host {host_id} must be a table")
        _unknown_keys(value, _HOST_KEYS, f"host {host_id}")
        platform = value.get("platform")
        if platform not in {"linux", "windows"}:
            raise ConfigError(f"host {host_id} has unsupported platform")
        transport = value.get("transport")
        if transport not in {"local", "ssh"}:
            raise ConfigError(f"host {host_id} has unsupported transport")
        expected_hostname = value.get("expected_hostname")
        if not isinstance(expected_hostname, str) or not expected_hostname:
            raise ConfigError(f"host {host_id} requires expected_hostname")
        t3_command = value.get("t3_command", ["t3"])
        hosts[host_id] = HostConfig(
            host_id=host_id,
            enabled=value.get("enabled") is True,
            platform=platform,
            transport=transport,
            ssh_alias=_ssh_alias(value.get("ssh_alias"), transport),
            agent_command=_command(value.get("agent_command"), f"host {host_id} agent_command"),
            active_limit=_positive_int(value.get("active_limit"), f"host {host_id} active_limit"),
            expected_hostname=expected_hostname,
            t3_url=value.get("t3_url", "http://127.0.0.1:3773"),
            t3_command=_command(t3_command, f"host {host_id} t3_command"),
            t3_base_dir=value.get("t3_base_dir"),
            agent_state_dir=value.get("agent_state_dir"),
        )

    controller_host = raw.get("controller_host")
    if not isinstance(controller_host, str) or controller_host not in hosts:
        raise ConfigError("controller_host must name a configured host")

    coordination_mode = raw.get("coordination_mode", "single-controller")
    if not isinstance(coordination_mode, str):
        raise ConfigError("coordination_mode must be a string")
    if coordination_mode not in {"single-controller", "target-authoritative"}:
        raise ConfigError(f"unsupported coordination_mode: {coordination_mode!r}")

    controller_id = raw.get("controller_id")
    if controller_id is not None:
        if not isinstance(controller_id, str) or not controller_id:
            raise ConfigError("controller_id must be a non-empty string")
        _identifier(controller_id, "controller_id")
    if coordination_mode == "target-authoritative" and not controller_id:
        raise ConfigError("target-authoritative mode requires controller_id")

    global_active_limit = _positive_int(raw.get("global_active_limit"), "global_active_limit")
    if coordination_mode == "target-authoritative":
        derived_limit = sum(
            host.active_limit for host in hosts.values() if host.enabled
        )
        if global_active_limit != derived_limit:
            raise ConfigError(
                f"global_active_limit must equal the sum of enabled host active_limit ({derived_limit})"
            )

    repos_raw = raw.get("repositories")
    if not isinstance(repos_raw, dict) or not repos_raw:
        raise ConfigError("repositories must be a non-empty table")
    repositories: dict[str, RepositoryConfig] = {}
    for repo_id, value in repos_raw.items():
        _identifier(repo_id, "repository id")
        if not isinstance(value, dict):
            raise ConfigError(f"repository {repo_id} must be a table")
        _unknown_keys(value, _REPO_KEYS, f"repository {repo_id}")
        default_branch = value.get("default_branch")
        branch_prefix = value.get("branch_prefix")
        paths_raw = value.get("paths")
        if not isinstance(default_branch, str) or not default_branch:
            raise ConfigError(f"repository {repo_id} requires default_branch")
        if not isinstance(branch_prefix, str) or not branch_prefix.startswith("codex/"):
            raise ConfigError(f"repository {repo_id} branch_prefix must start with codex/")
        if not isinstance(paths_raw, dict) or not paths_raw:
            raise ConfigError(f"repository {repo_id} requires paths")
        paths: dict[str, str] = {}
        for host_id, path_value in paths_raw.items():
            if host_id not in hosts:
                raise ConfigError(f"repository {repo_id} path names unknown host {host_id}")
            paths[host_id] = _absolute_path(
                path_value,
                hosts[host_id].platform,
                f"repository {repo_id} path for {host_id}",
            )
        repositories[repo_id] = RepositoryConfig(
            repository_id=repo_id,
            default_branch=default_branch,
            branch_prefix=branch_prefix,
            paths=paths,
        )

    return FleetConfig(
        controller_host=controller_host,
        global_active_limit=global_active_limit,
        state_dir=raw.get("state_dir"),
        hosts=hosts,
        repositories=repositories,
        controller_id=controller_id,
        coordination_mode=coordination_mode,
        ssh_command=_command(raw.get("ssh_command", ["ssh"]), "ssh_command"),
    )
