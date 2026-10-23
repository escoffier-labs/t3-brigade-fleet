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
# Mirrors the Fleet Hub's own repository identity pattern. A canonical identity
# is a remote identity such as `github.com/example-org/example-repo`, never a
# URL with credentials in it and never a local alias.
_POLICY_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/@:-]{0,255}$")
# Credential syntax, not credential-sounding words: a repository named
# `tokenjuice` is fine, `user:pass@host/repo` is not.
_CREDENTIAL_SYNTAX = re.compile(r"(?i)(://|token=|password=|[^/]+:[^/@]+@)")
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
    "policy",
    "outbox",
    "settle",
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
    "max_concurrent_per_repository",
    "repository_lease_ttl_seconds",
}
_REPO_KEYS = {"default_branch", "branch_prefix", "paths", "policy_identity"}

# Per-host cap on concurrent active requests for one repository. Each writable
# request runs in its own target worktree, so the default (0) lets one
# repository use the host's whole active_limit. A positive value sets a lower
# explicit cap; 1 restores the original one-run-per-repository behavior.
DEFAULT_MAX_CONCURRENT_PER_REPOSITORY = 0
# Default repository lease lifetime. The fleet has no per-run timeout, so this
# bounds how long a stuck request can hold a repository slot on a host.
DEFAULT_REPOSITORY_LEASE_TTL_SECONDS = 6 * 60 * 60
_MIN_REPOSITORY_LEASE_TTL_SECONDS = 60
_POLICY_KEYS = {"enrollment", "workload"}
_POLICY_ENROLLMENTS = {"off", "required"}
_OUTBOX_KEYS = {
    "enabled",
    "max_records",
    "max_prompt_bytes",
    "max_intent_bytes",
    "retention_seconds",
    "max_attempts",
    "backoff_seconds",
    "max_backoff_seconds",
    "max_jobs_per_tick",
    "max_tick_seconds",
    "lease_seconds",
}
_SETTLE_KEYS = {"enabled", "grace_minutes", "unlink_pull_requests"}


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
    max_concurrent_per_repository: int = DEFAULT_MAX_CONCURRENT_PER_REPOSITORY
    repository_lease_ttl_seconds: int = DEFAULT_REPOSITORY_LEASE_TTL_SECONDS

    @property
    def repository_limit(self) -> int:
        """Effective per-repository cap: explicit value, else host capacity."""
        return effective_repository_limit(
            self.max_concurrent_per_repository,
            self.active_limit,
        )


def effective_repository_limit(configured: int, active_limit: int) -> int:
    if configured <= 0:
        return active_limit
    return min(configured, active_limit)


@dataclass(frozen=True)
class RepositoryConfig:
    """One allowlisted repository under two identities.

    `repository_id` is the local alias. It stays the request identity, the work
    id, the key for per-host checkout paths, and what the UI shows.
    `policy_identity` is the canonical remote identity, and it is the only thing
    the Fleet Hub is ever asked about, so one repository keeps one policy across
    machines whose checkouts are named differently.
    """

    repository_id: str
    default_branch: str
    branch_prefix: str
    paths: dict[str, str]
    policy_identity: str | None = None


@dataclass(frozen=True)
class PolicyConfig:
    """Fleet Hub policy enrollment for this controller and its targets.

    `off` keeps the pre-policy dispatch contract exactly as it was. `required`
    is the only enrolled value and it fails closed: without a reachable Brigade
    policy helper no request is journalled, dispatched, or partially launched.
    """

    enrollment: str = "off"
    workload: str = "linux-development"

    @property
    def enrolled(self) -> bool:
        return self.enrollment == "required"


POLICY_OFF = PolicyConfig()


@dataclass(frozen=True)
class OutboxConfig:
    """Bounds for the local ordinary-work outbox that `t3-fleet tick` drains.

    `enabled` is off by default and there is no implicit default-on path: the
    outbox only ever holds work a caller explicitly asked to queue, on a
    controller whose operator explicitly turned the queue on. Every other key
    is a bound, so a queue can never grow without limit, retry without limit, or
    hold a prompt on disk without limit.
    """

    enabled: bool = False
    max_records: int = 64
    max_prompt_bytes: int = 120_000
    max_intent_bytes: int = 16_384
    retention_seconds: int = 86_400
    max_attempts: int = 5
    backoff_seconds: int = 60
    max_backoff_seconds: int = 3_600
    max_jobs_per_tick: int = 4
    max_tick_seconds: int = 60
    lease_seconds: int = 120


OUTBOX_OFF = OutboxConfig()


@dataclass(frozen=True)
class SettleConfig:
    """Whether `t3-fleet settle-sweep` may settle finished fleet threads.

    Settling moves a thread out of the operator's active sidebar, so the sweep
    is opt-in: an absent table is the off state. `grace_minutes` is how long a
    request must have been terminal before its thread is touched, and
    `unlink_pull_requests` additionally detaches linked pull requests first so
    a PR watch cannot wake a lane that already finished.
    """

    enabled: bool = False
    grace_minutes: int = 10
    unlink_pull_requests: bool = False


SETTLE_OFF = SettleConfig()


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
    policy: PolicyConfig = POLICY_OFF
    outbox: OutboxConfig = OUTBOX_OFF
    settle: SettleConfig = SETTLE_OFF


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


def _non_negative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigError(f"{label} must be a non-negative integer")
    return value


def _lease_ttl(value: Any, label: str) -> int:
    ttl = _positive_int(value, label)
    if ttl < _MIN_REPOSITORY_LEASE_TTL_SECONDS:
        raise ConfigError(
            f"{label} must be at least {_MIN_REPOSITORY_LEASE_TTL_SECONDS} seconds"
        )
    return ttl


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
            max_concurrent_per_repository=_non_negative_int(
                value.get(
                    "max_concurrent_per_repository",
                    DEFAULT_MAX_CONCURRENT_PER_REPOSITORY,
                ),
                f"host {host_id} max_concurrent_per_repository",
            ),
            repository_lease_ttl_seconds=_lease_ttl(
                value.get(
                    "repository_lease_ttl_seconds",
                    DEFAULT_REPOSITORY_LEASE_TTL_SECONDS,
                ),
                f"host {host_id} repository_lease_ttl_seconds",
            ),
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
            policy_identity=_policy_identity(value.get("policy_identity"), repo_id),
        )

    policy = _policy(raw.get("policy"))
    if policy.enrolled:
        _require_canonical_identities(repositories)

    outbox = _outbox(raw.get("outbox"))
    settle = _settle(raw.get("settle"))

    return FleetConfig(
        controller_host=controller_host,
        global_active_limit=global_active_limit,
        state_dir=raw.get("state_dir"),
        hosts=hosts,
        repositories=repositories,
        controller_id=controller_id,
        coordination_mode=coordination_mode,
        ssh_command=_command(raw.get("ssh_command", ["ssh"]), "ssh_command"),
        policy=policy,
        outbox=outbox,
        settle=settle,
    )


def _policy_identity(value: Any, repo_id: str) -> str | None:
    """Validate the optional canonical remote identity for one repository.

    Nothing here parses a git URL or contacts a remote, and nothing here is
    Brigade's normalizer. The value is a reviewed configuration fact, checked
    only for being a canonical `host/owner/name` with no credential-bearing
    structure in it.

    The guard is structural, never lexical. A repository is allowed to be called
    `token-glace`, `secret-scanner`, or `password-vault`, because those are
    ordinary names; what is refused is a URL scheme, userinfo in the authority,
    a control, query, or fragment character, and a path segment that is empty or
    traverses. The equality check against the checkout a target actually holds
    stays the authority on identity; this only refuses input that could not be a
    canonical identity in the first place.
    """
    if value is None:
        return None
    if not isinstance(value, str) or not _POLICY_IDENTITY.fullmatch(value):
        raise ConfigError(f"repository {repo_id} policy_identity is not a canonical identity")
    if _CREDENTIAL_SYNTAX.search(value):
        raise ConfigError(f"repository {repo_id} policy_identity must not carry a credential")
    authority, separator, path = value.partition("/")
    if not separator or not authority or not path:
        raise ConfigError(
            f"repository {repo_id} policy_identity must be a canonical remote "
            "identity such as host/owner/name, not a local alias"
        )
    if "@" in authority:
        raise ConfigError(
            f"repository {repo_id} policy_identity must not carry a credential"
        )
    host, colon, port = authority.rpartition(":")
    if colon and not (host and port.isdigit()):
        # `host:owner/name` is scp-like remote syntax, not a canonical identity,
        # and it is the shape a credential is usually smuggled through.
        raise ConfigError(
            f"repository {repo_id} policy_identity must not carry a credential"
        )
    for segment in path.split("/"):
        if not segment or segment in {".", ".."}:
            raise ConfigError(
                f"repository {repo_id} policy_identity must not contain an empty "
                "or traversing path segment"
            )
    return value


def _is_canonical(value: str) -> bool:
    """A canonical identity names a host and a path, so it is never a bare alias."""
    head, separator, tail = value.partition("/")
    return bool(separator) and bool(head) and bool(tail)


def _require_canonical_identities(repositories: dict[str, RepositoryConfig]) -> None:
    """Under enrollment, every repository needs an identity the Hub agrees on.

    A local alias is not a canonical identity, so an enrolled controller with no
    explicit mapping has nothing truthful to ask the Fleet Hub about. That fails
    closed at load time rather than at dispatch time.
    """
    missing = sorted(
        repo_id
        for repo_id, repository in repositories.items()
        if repository.policy_identity is None and not _is_canonical(repo_id)
    )
    if missing:
        raise ConfigError(
            "policy enrollment requires policy_identity for repository(s): "
            + ", ".join(missing)
        )


def _policy(value: Any) -> PolicyConfig:
    if value is None:
        return POLICY_OFF
    if not isinstance(value, dict):
        raise ConfigError("policy must be a table")
    _unknown_keys(value, _POLICY_KEYS, "policy")
    enrollment = value.get("enrollment", "off")
    if enrollment not in _POLICY_ENROLLMENTS:
        raise ConfigError(
            "policy enrollment must be \"required\" or \"off\""
        )
    workload = value.get("workload", "linux-development")
    if not isinstance(workload, str) or not _ID.fullmatch(workload):
        raise ConfigError(f"invalid policy workload: {workload!r}")
    return PolicyConfig(enrollment=enrollment, workload=workload)


def _outbox(value: Any) -> OutboxConfig:
    """Read the local outbox bounds, all of which are positive integers.

    An absent table is the off state, which is also the default. There is no
    "unbounded" spelling for any of these: a missing key takes the default
    bound rather than removing it, so a queue cannot be widened by omission.
    """
    if value is None:
        return OUTBOX_OFF
    if not isinstance(value, dict):
        raise ConfigError("outbox must be a table")
    _unknown_keys(value, _OUTBOX_KEYS, "outbox")
    enabled = value.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ConfigError("outbox enabled must be a boolean")
    defaults = OUTBOX_OFF
    bounds = {
        key: _positive_int(value.get(key, getattr(defaults, key)), f"outbox {key}")
        for key in sorted(_OUTBOX_KEYS - {"enabled"})
    }
    if bounds["max_prompt_bytes"] > 120_000:
        # The outbox never stores a prompt the dispatch path itself would
        # refuse, so the queue can never become the wider input surface.
        raise ConfigError("outbox max_prompt_bytes exceeds the dispatch prompt limit")
    if bounds["backoff_seconds"] > bounds["max_backoff_seconds"]:
        raise ConfigError("outbox backoff_seconds exceeds max_backoff_seconds")
    return OutboxConfig(enabled=enabled, **bounds)


def _settle(value: Any) -> SettleConfig:
    if value is None:
        return SETTLE_OFF
    if not isinstance(value, dict):
        raise ConfigError("settle must be a table")
    _unknown_keys(value, _SETTLE_KEYS, "settle")
    for key in ("enabled", "unlink_pull_requests"):
        if not isinstance(value.get(key, False), bool):
            raise ConfigError(f"settle {key} must be a boolean")
    return SettleConfig(
        enabled=value.get("enabled", False),
        grace_minutes=_positive_int(
            value.get("grace_minutes", SETTLE_OFF.grace_minutes), "settle grace_minutes"
        ),
        unlink_pull_requests=value.get("unlink_pull_requests", False),
    )
