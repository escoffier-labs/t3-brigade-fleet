from __future__ import annotations

import os
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
agent_command = ["/home/operator/.local/bin/t3-fleet-agent"]
active_limit = 1
expected_hostname = "controller"

[hosts.linux-worker]
enabled = true
platform = "linux"
transport = "ssh"
ssh_alias = "linux-worker"
agent_command = ["/home/operator/.local/bin/t3-fleet-agent"]
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

    def test_peer_example_uses_derived_three_host_capacity(self) -> None:
        config = self._load_example_config()
        self.assertEqual(config.coordination_mode, "target-authoritative")
        self.assertEqual(config.controller_id, "node-a")
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
            'agent_command = ["/home/operator/.local/bin/t3-fleet-agent"]',
            'agent_command = ["/home/operator/.local/bin/t3-fleet-agent;whoami"]',
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


if __name__ == "__main__":
    unittest.main()
