from __future__ import annotations

import os
import json
import stat
import tempfile
import unittest
from pathlib import Path

from t3_fleet.config import FleetConfig, load_config
from t3_fleet.errors import ConfigError


VALID = """
schema = "t3-fleet.config.v1"
controller_host = "controller"
global_active_limit = 3

[hosts.controller]
enabled = true
platform = "linux"
transport = "local"
agent_command = ["/opt/t3-fleet/bin/t3-fleet-agent"]
active_limit = 1
expected_hostname = "controller"

[hosts.linux-worker]
enabled = true
platform = "linux"
transport = "ssh"
ssh_alias = "linux-worker"
agent_command = ["/opt/t3-fleet/bin/t3-fleet-agent"]
active_limit = 2
expected_hostname = "linux-worker"

[hosts.windows-worker]
enabled = true
platform = "windows"
transport = "ssh"
ssh_alias = "windows-worker"
agent_command = ["powershell.exe", "-NoProfile", "-NonInteractive", "-File", "C:\\\\Users\\\\operator\\\\bin\\\\t3-fleet-agent.ps1"]
active_limit = 1
expected_hostname = "WINDOWS-WORKER"

[repositories.example-repo]
default_branch = "main"
branch_prefix = "codex/"

[repositories.example-repo.paths]
controller = "/srv/repos/example"
linux-worker = "/srv/repos/example"
windows-worker = "C:\\\\repos\\\\example"
"""


CANONICAL = "github.com/example-org/example-repo"

# The enrolled shape: the local alias stays `example-repo` for requests, work
# ids, path lookup, and the UI, while `policy_identity` carries the canonical
# remote identity every Fleet Hub call is made against.
ENROLLED = (
    VALID.replace(
        "[repositories.example-repo]\ndefault_branch",
        f'[repositories.example-repo]\npolicy_identity = "{CANONICAL}"\ndefault_branch',
    )
    + '\n[policy]\nenrollment = "required"\nworkload = "linux-development"\n'
)


class ConfigTests(unittest.TestCase):
    def write_config(self, text: str, *, mode: int = 0o600) -> Path:
        directory = Path(tempfile.mkdtemp())
        path = directory / "fleet.toml"
        path.write_text(text, encoding="utf-8")
        path.chmod(mode)
        self.addCleanup(directory.rmdir)
        self.addCleanup(lambda: path.unlink(missing_ok=True))
        return path

    def _load_example_config(self, text: str | None = None) -> FleetConfig:
        if text is None:
            text = (
                Path(__file__).resolve().parents[1] / "examples" / "fleet.example.toml"
            ).read_text(encoding="utf-8")
        return load_config(self.write_config(text))

    def test_t3_origin_is_retained_as_inert_text_at_config_boundary(self) -> None:
        foreign_origin = "https://unused.example.invalid"
        text = VALID.replace(
            "[hosts.linux-worker]",
            f'[hosts.linux-worker]\nt3_url = {json.dumps(foreign_origin)}',
        )
        config = load_config(self.write_config(text))
        self.assertEqual(config.hosts["linux-worker"].t3_url, foreign_origin)
        self.assertEqual(config.hosts["controller"].t3_url, "http://127.0.0.1:3773")  # content-guard: allow loopback-ipv4

    def test_t3_origin_must_be_a_string_at_config_boundary(self) -> None:
        for origin in [42, [], {}]:
            with self.subTest(origin=origin):
                value = json.dumps(origin) if origin != {} else "{}"
                text = VALID.replace('[hosts.controller]', f'[hosts.controller]\nt3_url = {value}')
                with self.assertRaises(ConfigError) as caught:
                    load_config(self.write_config(text))
                self.assertEqual(str(caught.exception), "invalid T3 origin")

    def test_peer_example_uses_derived_three_host_capacity(self) -> None:
        config = self._load_example_config()
        self.assertEqual(config.coordination_mode, "target-authoritative")
        self.assertEqual(config.controller_id, "controller-a")
        self.assertEqual(config.global_active_limit, 3)
        enabled = [host for host in config.hosts.values() if host.enabled]
        self.assertEqual(len(enabled), 3)
        self.assertEqual(sum(host.active_limit for host in enabled), 3)

    def test_peer_example_node_relative_transport_invariants(self) -> None:
        config = self._load_example_config()
        local = config.hosts[config.controller_host]
        self.assertEqual(local.transport, "local")
        self.assertIsNone(local.ssh_alias)
        for peer_id, peer in config.hosts.items():
            self.assertEqual(peer.expected_hostname, peer_id)
            if peer_id == config.controller_host:
                continue
            self.assertTrue(peer.enabled)
            self.assertEqual(peer.transport, "ssh")
            self.assertIsNotNone(peer.ssh_alias)

    def test_loads_allowlisted_hosts_and_repositories(self) -> None:
        config = load_config(self.write_config(VALID))
        self.assertEqual(config.controller_host, "controller")
        self.assertEqual(config.ssh_command, ("ssh",))
        self.assertEqual(config.hosts["linux-worker"].active_limit, 2)
        self.assertEqual(
            config.repositories["example-repo"].paths["windows-worker"],
            r"C:\repos\example",
        )

    def test_loads_custom_controller_ssh_command(self) -> None:
        text = VALID.replace(
            'global_active_limit = 3',
            'global_active_limit = 3\nssh_command = ["C:\\\\Program Files\\\\Git\\\\usr\\\\bin\\\\ssh.exe"]',
        )
        config = load_config(self.write_config(text))
        self.assertEqual(
            config.ssh_command,
            (r"C:\Program Files\Git\usr\bin\ssh.exe",),
        )

    def test_rejects_empty_controller_ssh_command(self) -> None:
        text = VALID.replace(
            "global_active_limit = 3",
            "global_active_limit = 3\nssh_command = []",
        )
        with self.assertRaisesRegex(ConfigError, "ssh_command"):
            load_config(self.write_config(text))

    def test_rejects_ip_literal_ssh_alias(self) -> None:
        text = VALID.replace('ssh_alias = "linux-worker"', 'ssh_alias = "100.64.0.9"')
        with self.assertRaisesRegex(ConfigError, "IP literals"):
            load_config(self.write_config(text))

    def test_rejects_embedded_credentials(self) -> None:
        text = VALID.replace(
            'expected_hostname = "linux-worker"',
            'expected_hostname = "linux-worker"\ntoken = "not-allowed"',
        )
        with self.assertRaisesRegex(ConfigError, "credential"):
            load_config(self.write_config(text))

    def test_rejects_shell_metacharacters(self) -> None:
        text = VALID.replace(
            'agent_command = ["/opt/t3-fleet/bin/t3-fleet-agent"]',
            'agent_command = ["/opt/t3-fleet/bin/t3-fleet-agent;whoami"]',
            1,
        )
        with self.assertRaisesRegex(ConfigError, "agent_command"):
            load_config(self.write_config(text))

    def test_rejects_unknown_top_level_key(self) -> None:
        text = VALID.replace("global_active_limit = 3", "global_active_limit = 3\nextra = true")
        with self.assertRaisesRegex(ConfigError, "unknown top-level"):
            load_config(self.write_config(text))

    def test_peer_mode_requires_controller_id_and_derived_capacity(self) -> None:
        text = VALID.replace(
            'controller_host = "controller"',
            'controller_host = "controller"\ncoordination_mode = "target-authoritative"\ncontroller_id = "controller-a"',
        ).replace("global_active_limit = 3", "global_active_limit = 4")
        self.assertEqual(load_config(self.write_config(text)).coordination_mode, "target-authoritative")
        invalid = text.replace("global_active_limit = 4", "global_active_limit = 3")
        with self.assertRaisesRegex(ConfigError, "sum of enabled host active_limit"):
            load_config(self.write_config(invalid))

    def test_single_controller_remains_the_default(self) -> None:
        config = load_config(self.write_config(VALID))
        self.assertEqual(config.coordination_mode, "single-controller")
        self.assertIsNone(config.controller_id)

    def test_rejects_invalid_identifier(self) -> None:
        text = VALID.replace("[hosts.linux-worker]", "[hosts.Bad_Host]")
        with self.assertRaisesRegex(ConfigError, "invalid host id"):
            load_config(self.write_config(text))

    def test_rejects_relative_repository_path(self) -> None:
        text = VALID.replace('linux-worker = "/srv/repos/example"', 'linux-worker = "repos/example"')
        with self.assertRaisesRegex(ConfigError, "absolute"):
            load_config(self.write_config(text))

    @unittest.skipIf(os.name == "nt", "POSIX permission semantics")
    def test_rejects_group_readable_config(self) -> None:
        path = self.write_config(VALID, mode=0o640)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o640)
        with self.assertRaisesRegex(ConfigError, "owner-only"):
            load_config(path)


class PolicyConfigTests(unittest.TestCase):
    def write_config(self, text: str, *, mode: int = 0o600) -> Path:
        directory = Path(tempfile.mkdtemp())
        path = directory / "fleet.toml"
        path.write_text(text, encoding="utf-8")
        path.chmod(mode)
        self.addCleanup(directory.rmdir)
        self.addCleanup(lambda: path.unlink(missing_ok=True))
        return path

    def test_policy_defaults_to_off_for_legacy_configs(self) -> None:
        config = load_config(self.write_config(VALID))
        self.assertEqual(config.policy.enrollment, "off")
        self.assertEqual(config.policy.workload, "linux-development")

    def test_explicit_required_enrollment_is_parsed(self) -> None:
        text = ENROLLED
        config = load_config(self.write_config(text))
        self.assertEqual(config.policy.enrollment, "required")
        self.assertEqual(config.policy.workload, "linux-development")

    def test_rejects_unsupported_enrollment_value(self) -> None:
        text = VALID + '\n[policy]\nenrollment = "advisory"\n'
        with self.assertRaisesRegex(ConfigError, "enrollment"):
            load_config(self.write_config(text))

    def test_rejects_unknown_policy_key(self) -> None:
        text = VALID + '\n[policy]\nenrollment = "off"\nhub_token = "nope"\n'
        with self.assertRaisesRegex(ConfigError, "credential"):
            load_config(self.write_config(text))

    def test_rejects_malformed_workload(self) -> None:
        text = ENROLLED.replace(
            'workload = "linux-development"', 'workload = "Linux Development"'
        )
        with self.assertRaisesRegex(ConfigError, "workload"):
            load_config(self.write_config(text))


class RepositoryPolicyIdentityTests(ConfigTests):
    """The local alias is the request identity; policy_identity is the canonical one."""

    def test_policy_identity_is_parsed_and_the_alias_is_unchanged(self) -> None:
        config = load_config(self.write_config(ENROLLED))
        repository = config.repositories["example-repo"]
        self.assertEqual(repository.repository_id, "example-repo")
        self.assertEqual(repository.policy_identity, CANONICAL)
        # The alias still drives path lookup and the durable request identity.
        self.assertEqual(repository.paths["linux-worker"], "/srv/repos/example")

    def test_policy_identity_is_optional_while_enrollment_is_off(self) -> None:
        config = load_config(self.write_config(VALID))
        self.assertIsNone(config.repositories["example-repo"].policy_identity)
        self.assertEqual(config.policy.enrollment, "off")

    def test_enrollment_without_a_canonical_identity_fails_closed(self) -> None:
        text = VALID + '\n[policy]\nenrollment = "required"\n'
        with self.assertRaisesRegex(ConfigError, "policy_identity"):
            load_config(self.write_config(text))

    def test_rejects_a_policy_identity_that_is_not_canonical(self) -> None:
        # A bare alias is a local name, not a canonical remote identity, so it
        # cannot stand in for one.
        text = ENROLLED.replace(f'"{CANONICAL}"', '"example-repo"')
        with self.assertRaisesRegex(ConfigError, "policy_identity"):
            load_config(self.write_config(text))

    def test_rejects_a_policy_identity_carrying_a_credential(self) -> None:
        for identity in (
            "operator:hunter2@github.com/example-org/example-repo",  # <!-- content-guard: allow pii/email -->
            "https://github.com/example-org/example-repo",
        ):
            with self.subTest(identity=identity):
                text = ENROLLED.replace(f'"{CANONICAL}"', f'"{identity}"')
                with self.assertRaisesRegex(ConfigError, "policy_identity"):
                    load_config(self.write_config(text))

    def test_accepts_a_repository_merely_named_like_a_credential(self) -> None:
        # The guard is structural, not lexical. A repository is allowed to be
        # called `token-glace` or `secret-scanner`; those are ordinary names.
        for identity in (
            "github.com/example-org/tokenjuice",
            "git.example.invalid/team/token-glace",
            "github.com/example-org/secret-scanner",
            "github.com/example-org/password-vault",
            "github.com/example-org/private-key-rotation",
            "git.example.invalid:7999/team/credential-store",
            "github.com/example-org/example.repo-2",
        ):
            with self.subTest(identity=identity):
                text = ENROLLED.replace(f'"{CANONICAL}"', f'"{identity}"')
                config = load_config(self.write_config(text))
                self.assertEqual(
                    config.repositories["example-repo"].policy_identity, identity
                )

    def test_rejects_structurally_credential_bearing_identities(self) -> None:
        # Every value here is an obviously fake placeholder. What is being
        # refused is the shape, not the string.
        for identity in (
            "operator:hunter2@github.com/example-org/example-repo",  # <!-- content-guard: allow pii/email -->
            "https://github.com/example-org/example-repo",
            "ssh://git@github.com/example-org/example-repo",
            "operator@github.com/example-org/example-repo",  # <!-- content-guard: allow pii/email -->
            "git@github.com:example-org/example-repo",
            "github.com:example-org/example-repo",
        ):
            with self.subTest(identity=identity):
                text = ENROLLED.replace(f'"{CANONICAL}"', f'"{identity}"')
                with self.assertRaisesRegex(ConfigError, "policy_identity"):
                    load_config(self.write_config(text))

    def test_rejects_unsafe_path_segments(self) -> None:
        for identity in (
            "github.com/example-org//example-repo",
            "github.com/../example-repo",
            "github.com/example-org/../../etc",
            "github.com/example-org/./example-repo",
            "github.com/example-org/example-repo/",
        ):
            with self.subTest(identity=identity):
                text = ENROLLED.replace(f'"{CANONICAL}"', f'"{identity}"')
                with self.assertRaisesRegex(ConfigError, "policy_identity"):
                    load_config(self.write_config(text))

    def test_rejects_control_query_and_fragment_characters(self) -> None:
        for identity in (
            "github.com/example-org/example-repo?token=placeholder",
            "github.com/example-org/example-repo#fragment",
            "github.com/example-org/example\trepo",
            "github.com/example-org/example repo",
            "github.com/example-org/example%2Frepo",
        ):
            with self.subTest(identity=identity):
                text = ENROLLED.replace(f'"{CANONICAL}"', f'"{identity}"')
                with self.assertRaisesRegex(ConfigError, "policy_identity"):
                    load_config(self.write_config(text))

    def test_rejects_a_non_string_policy_identity(self) -> None:
        text = ENROLLED.replace(f'"{CANONICAL}"', "7")
        with self.assertRaisesRegex(ConfigError, "policy_identity"):
            load_config(self.write_config(text))


class OutboxConfigTests(unittest.TestCase):
    """The local queue is off by default and every one of its knobs is a bound."""

    def write_config(self, text: str, *, mode: int = 0o600) -> Path:
        directory = Path(tempfile.mkdtemp())
        path = directory / "fleet.toml"
        path.write_text(text, encoding="utf-8")
        path.chmod(mode)
        self.addCleanup(directory.rmdir)
        self.addCleanup(lambda: path.unlink(missing_ok=True))
        return path

    def test_a_config_without_an_outbox_table_keeps_the_queue_off(self) -> None:
        config = load_config(self.write_config(VALID))
        self.assertFalse(config.outbox.enabled)
        self.assertEqual(config.outbox.max_records, 64)

    def test_the_queue_is_still_off_unless_it_is_turned_on(self) -> None:
        config = load_config(self.write_config(VALID + "\n[outbox]\nmax_records = 8\n"))
        self.assertFalse(config.outbox.enabled)
        self.assertEqual(config.outbox.max_records, 8)

    def test_the_bounds_are_read_and_the_rest_keep_their_defaults(self) -> None:
        config = load_config(
            self.write_config(
                VALID
                + "\n[outbox]\nenabled = true\nmax_records = 8\n"
                "retention_seconds = 600\nmax_attempts = 2\n"
            )
        )
        self.assertTrue(config.outbox.enabled)
        self.assertEqual(config.outbox.max_records, 8)
        self.assertEqual(config.outbox.retention_seconds, 600)
        self.assertEqual(config.outbox.max_attempts, 2)
        # An omitted key takes its default bound rather than removing one.
        self.assertEqual(config.outbox.max_prompt_bytes, 120_000)

    def test_an_unknown_outbox_key_is_refused(self) -> None:
        with self.assertRaisesRegex(ConfigError, "outbox"):
            load_config(
                self.write_config(VALID + "\n[outbox]\nenabled = true\nqueue_all = true\n")
            )

    def test_a_bound_can_never_be_zero_negative_or_unbounded(self) -> None:
        for line in ("max_records = 0", "max_attempts = -1", "retention_seconds = 0"):
            with self.subTest(line=line):
                with self.assertRaisesRegex(ConfigError, "positive integer"):
                    load_config(
                        self.write_config(VALID + f"\n[outbox]\nenabled = true\n{line}\n")
                    )

    def test_the_queue_never_accepts_a_prompt_the_dispatch_path_would_refuse(
        self,
    ) -> None:
        with self.assertRaisesRegex(ConfigError, "prompt limit"):
            load_config(
                self.write_config(
                    VALID + "\n[outbox]\nenabled = true\nmax_prompt_bytes = 200000\n"
                )
            )

    def test_a_backoff_above_its_own_ceiling_is_refused(self) -> None:
        with self.assertRaisesRegex(ConfigError, "backoff"):
            load_config(
                self.write_config(
                    VALID
                    + "\n[outbox]\nenabled = true\nbackoff_seconds = 900\n"
                    "max_backoff_seconds = 60\n"
                )
            )

    def test_enabled_must_be_a_boolean(self) -> None:
        with self.assertRaisesRegex(ConfigError, "boolean"):
            load_config(
                self.write_config(VALID + '\n[outbox]\nenabled = "yes"\n')
            )


class RepositoryConcurrencyConfigTests(unittest.TestCase):
    write_config = ConfigTests.write_config

    def test_repository_limit_defaults_to_host_capacity(self) -> None:
        config = load_config(self.write_config(VALID))
        worker = config.hosts["linux-worker"]
        self.assertEqual(worker.max_concurrent_per_repository, 0)
        self.assertEqual(worker.repository_limit, worker.active_limit)
        self.assertEqual(worker.repository_lease_ttl_seconds, 6 * 60 * 60)

    def test_explicit_repository_limit_and_ttl_are_loaded(self) -> None:
        text = VALID.replace(
            'active_limit = 2\nexpected_hostname = "linux-worker"',
            'active_limit = 2\nexpected_hostname = "linux-worker"\n'
            "max_concurrent_per_repository = 1\n"
            "repository_lease_ttl_seconds = 5400",
        )
        worker = load_config(self.write_config(text)).hosts["linux-worker"]
        self.assertEqual(worker.repository_limit, 1)
        self.assertEqual(worker.repository_lease_ttl_seconds, 5400)

    def test_repository_limit_zero_means_host_capacity_and_is_capped(self) -> None:
        for value, expected in (("0", 2), ("5", 2), ("2", 2)):
            with self.subTest(value=value):
                text = VALID.replace(
                    'expected_hostname = "linux-worker"',
                    f'expected_hostname = "linux-worker"\nmax_concurrent_per_repository = {value}',
                )
                worker = load_config(self.write_config(text)).hosts["linux-worker"]
                self.assertEqual(worker.repository_limit, expected)

    def test_invalid_repository_limit_and_ttl_are_rejected(self) -> None:
        cases = (
            ("max_concurrent_per_repository = -1", "max_concurrent_per_repository"),
            ("max_concurrent_per_repository = true", "max_concurrent_per_repository"),
            ('max_concurrent_per_repository = "2"', "max_concurrent_per_repository"),
            ("repository_lease_ttl_seconds = 0", "repository_lease_ttl_seconds"),
            ("repository_lease_ttl_seconds = 59", "repository_lease_ttl_seconds"),
        )
        for line, message in cases:
            with self.subTest(line=line):
                text = VALID.replace(
                    'expected_hostname = "linux-worker"',
                    f'expected_hostname = "linux-worker"\n{line}',
                )
                with self.assertRaisesRegex(ConfigError, message):
                    load_config(self.write_config(text))


class SettleConfigTests(unittest.TestCase):
    """Settling moves threads off the operator's sidebar, so it is opt-in."""

    def write_config(self, text: str) -> Path:
        directory = Path(tempfile.mkdtemp())
        path = directory / "fleet.toml"
        path.write_text(text, encoding="utf-8")
        path.chmod(0o600)
        self.addCleanup(directory.rmdir)
        self.addCleanup(lambda: path.unlink(missing_ok=True))
        return path

    def test_a_config_without_a_settle_table_keeps_settling_off(self) -> None:
        config = load_config(self.write_config(VALID))
        self.assertFalse(config.settle.enabled)
        self.assertEqual(config.settle.grace_minutes, 10)
        self.assertFalse(config.settle.unlink_pull_requests)

    def test_a_settle_table_is_still_off_unless_enabled(self) -> None:
        config = load_config(self.write_config(VALID + "\n[settle]\ngrace_minutes = 30\n"))
        self.assertFalse(config.settle.enabled)
        self.assertEqual(config.settle.grace_minutes, 30)

    def test_every_settle_key_is_read(self) -> None:
        config = load_config(
            self.write_config(
                VALID
                + "\n[settle]\nenabled = true\ngrace_minutes = 5\n"
                "unlink_pull_requests = true\n"
            )
        )
        self.assertTrue(config.settle.enabled)
        self.assertEqual(config.settle.grace_minutes, 5)
        self.assertTrue(config.settle.unlink_pull_requests)

    def test_invalid_settle_values_are_refused(self) -> None:
        for body in (
            "enabled = 1",
            "enabled = \"yes\"",
            "unlink_pull_requests = \"yes\"",
            "grace_minutes = 0",
            "grace_minutes = -3",
            "grace_minutes = true",
            "grace_minutes = 2.5",
            "unexpected = true",
        ):
            with self.subTest(body=body), self.assertRaises(ConfigError):
                load_config(self.write_config(VALID + f"\n[settle]\n{body}\n"))

    def test_settle_must_be_a_table(self) -> None:
        text = VALID.replace(
            'global_active_limit = 3', 'global_active_limit = 3\nsettle = true', 1
        )
        with self.assertRaisesRegex(ConfigError, "settle must be a table"):
            load_config(self.write_config(text))


if __name__ == "__main__":
    unittest.main()
