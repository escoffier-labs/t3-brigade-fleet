from __future__ import annotations

import re
import tempfile
import tomllib
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLIC_DOCS = [
    REPO_ROOT / "README.md",
    REPO_ROOT / "SECURITY.md",
    REPO_ROOT / "CONTRIBUTING.md",
    REPO_ROOT / "docs" / "architecture.md",
    REPO_ROOT / "docs" / "quickstart.md",
    REPO_ROOT / "docs" / "recovery.md",
    REPO_ROOT / "docs" / "security-model.md",
]
EXAMPLE_CONFIGS = [
    REPO_ROOT / "examples" / "fleet.example.toml",
    REPO_ROOT / "examples" / "local.example.toml",
]
T3_CODE_URL = "https://github.com/pingdotgg/t3code"
PUBLIC_COMMANDS = (
    "doctor",
    "capacity",
    "submit",
    "status",
    "wait",
    "followup",
    "interrupt",
    "approve",
    "answer",
    "recover",
)
# Harness projection paths that .gitignore keeps local. Public docs must not
# reference them, because they do not exist in a clean checkout.
PROJECTION_PATHS = (
    ".claude/commands/",
    ".codex/skills/",
    ".opencode/commands/",
    ".cursor/rules/",
    ".cursor/skills/",
    ".aider/commands/",
    ".aider/skills/",
    ".brigade/commands/",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _all_public_text() -> str:
    return "\n".join(_read(path) for path in PUBLIC_DOCS + EXAMPLE_CONFIGS)


class SkillPackageTests(unittest.TestCase):
    def test_both_example_configs_parse_as_toml(self) -> None:
        for path in EXAMPLE_CONFIGS:
            with path.open("rb") as handle:
                data = tomllib.load(handle)
            self.assertEqual(data.get("schema"), "t3-fleet.config.v1", path)
            self.assertIn("controller_host", data, path)
            self.assertIn("hosts", data, path)
            self.assertIn("repositories", data, path)

    def test_example_configs_load_through_config_loader(self) -> None:
        from t3_fleet.config import load_config

        for path in EXAMPLE_CONFIGS:
            tmp_dir = Path(tempfile.mkdtemp())
            tmp = tmp_dir / "fleet.toml"
            tmp.write_text(_read(path), encoding="utf-8")
            tmp.chmod(0o600)
            self.addCleanup(lambda p=tmp, d=tmp_dir: (p.unlink(missing_ok=True), d.rmdir()))
            config = load_config(tmp)
            self.assertIn(config.controller_host, config.hosts)
            for host in config.hosts.values():
                self.assertTrue(host.expected_hostname)

    def test_public_docs_use_the_real_t3_code_link(self) -> None:
        text = _all_public_text()
        self.assertIn(T3_CODE_URL, text)
        # The old wrong link pointed T3 Code at the t3-brigade-fleet repo.
        self.assertNotIn(
            "https://github.com/escoffier-labs/t3-brigade-fleet)",
            text,
            "T3 Code must link to pingdotgg/t3code, not the t3-brigade-fleet repo",
        )

    def test_documented_terminal_states_match_models_and_schema(self) -> None:
        from t3_fleet.models import JOB_STATES, TERMINAL_STATES

        self.assertEqual(
            set(TERMINAL_STATES), {"succeeded", "failed", "interrupted"}
        )
        self.assertIn("unknown", JOB_STATES)
        self.assertNotIn("unknown", TERMINAL_STATES)

        for path in (REPO_ROOT / "docs" / "architecture.md", REPO_ROOT / "docs" / "recovery.md"):
            text = _read(path)
            # The retired state names must not appear as documented states.
            for retired in ("`completed`", "`error`"):
                self.assertNotIn(retired, text, f"{path} still documents {retired}")
            # The new terminal names must appear.
            for name in ("`succeeded`", "`failed`", "`interrupted`"):
                self.assertIn(name, text, f"{path} does not document {name}")
            self.assertIn("`unknown`", text, f"{path} does not mention unknown")

    def test_readme_lists_exactly_the_public_ten_commands(self) -> None:
        text = _read(REPO_ROOT / "README.md")
        section = self._public_commands_section(text)
        bullets = re.findall(r"^- `t3-fleet ([a-z][a-z-]*)`", section, re.MULTILINE)
        self.assertEqual(
            sorted(bullets),
            sorted(PUBLIC_COMMANDS),
            "Public commands section must list exactly the ten public commands",
        )
        # Each command appears as a bullet exactly once in the section.
        for command in PUBLIC_COMMANDS:
            self.assertEqual(bullets.count(command), 1, f"{command} listed once")

    def _public_commands_section(self, text: str) -> str:
        start = text.find("## Public commands")
        self.assertNotEqual(start, -1, "README has no Public commands section")
        rest = text[start:]
        next_header = re.search(r"\n## ", rest[3:])
        end = (3 + next_header.start()) if next_header else len(rest)
        return rest[:end]

    def test_readme_notes_answer_is_json_stdin_only(self) -> None:
        text = _read(REPO_ROOT / "README.md")
        self.assertIn("`answer` is JSON-stdin-only", text)
        # The old blanket claim that every command accepts flags is gone.
        self.assertNotIn(
            "Each command accepts flags or a single JSON object on standard input.",
            text,
        )

    def test_public_docs_do_not_recommend_full_access(self) -> None:
        for path in PUBLIC_DOCS:
            text = _read(path)
            # full-access may be mentioned only as a high-risk mode that is not
            # recommended. It must never be presented as a default or a
            # recommendation.
            self.assertNotIn("default to `full-access`", text, path)
            self.assertNotIn("default is `full-access`", text, path)
            self.assertNotIn("recommend `full-access`", text, path)
            self.assertNotIn("recommended mode is `full-access`", text, path)

    def test_public_docs_do_not_reference_harness_projection_paths(self) -> None:
        text = _all_public_text()
        for projection in PROJECTION_PATHS:
            self.assertNotIn(
                projection,
                text,
                f"public artifact references ignored harness path {projection}",
            )

    def test_example_configs_use_generic_roles_only(self) -> None:
        allowed_roles = {"controller-a", "target-a", "target-b"}
        for path in EXAMPLE_CONFIGS:
            data = tomllib.loads(_read(path))
            host_ids = set(data.get("hosts", {}).keys())
            self.assertTrue(host_ids, f"{path} has no hosts")
            self.assertTrue(
                host_ids <= allowed_roles,
                f"{path} uses non-generic host ids: {host_ids - allowed_roles}",
            )
            controller = data.get("controller_host")
            self.assertIn(controller, allowed_roles, path)
            for repo in data.get("repositories", {}).values():
                for host_id in repo.get("paths", {}):
                    self.assertIn(host_id, allowed_roles, path)

    def test_fleet_example_is_three_host_target_authoritative(self) -> None:
        data = tomllib.loads(_read(REPO_ROOT / "examples" / "fleet.example.toml"))
        self.assertEqual(data.get("coordination_mode"), "target-authoritative")
        self.assertIn(data.get("controller_id"), {"controller-a"})
        enabled = [h for h in data["hosts"].values() if h.get("enabled")]
        self.assertEqual(len(enabled), 3)
        self.assertEqual(
            data.get("global_active_limit"),
            sum(h.get("active_limit", 0) for h in enabled),
        )

    def test_local_example_is_single_local_host(self) -> None:
        data = tomllib.loads(_read(REPO_ROOT / "examples" / "local.example.toml"))
        self.assertEqual(data.get("coordination_mode"), "single-controller")
        hosts = data.get("hosts", {})
        self.assertEqual(len(hosts), 1, "local example must have exactly one host")
        host = next(iter(hosts.values()))
        self.assertEqual(host.get("transport"), "local")
        self.assertNotIn("ssh_alias", host, "local host must not set ssh_alias")


SKILL_DIR = REPO_ROOT / "skills" / "t3-fleet-dispatch"
SKILL_JSON = SKILL_DIR / "skill.json"
SKILL_MD = SKILL_DIR / "SKILL.md"
OPENAI_AGENT = SKILL_DIR / "agents" / "openai.yaml"
GENERIC_ROLES = {"controller-a", "target-a", "target-b"}
MUTATING_COMMANDS = (
    "submit",
    "followup",
    "interrupt",
    "approve",
    "answer",
    "recover",
)
PRIVATE_PATH_PREFIXES = ("/home/", "/Users/", "/mnt/", "/root/", "C:\\", "C:/")


def _skill_package_files() -> list[Path]:
    if not SKILL_DIR.is_dir():
        return []
    return sorted(path for path in SKILL_DIR.rglob("*") if path.is_file())


def _skill_package_text() -> str:
    return "\n".join(_read(path) for path in _skill_package_files())


class T3FleetDispatchSkillTests(unittest.TestCase):
    def test_skill_json_metadata_values(self) -> None:
        import json

        self.assertTrue(SKILL_JSON.is_file(), "skill.json must exist")
        data = json.loads(_read(SKILL_JSON))
        self.assertEqual(data.get("id"), "t3-fleet-dispatch")
        self.assertEqual(data.get("version"), "0.1.0")
        self.assertEqual(data.get("license"), "Apache-2.0")
        self.assertEqual(data.get("trust_level"), "public")
        self.assertEqual(data.get("required_tools"), ["t3-fleet"])
        self.assertEqual(
            set(data.get("supported_harnesses", [])),
            {"codex", "claude", "cursor", "openclaw"},
        )
        self.assertTrue(data.get("tests"), "skill.json tests must not be empty")

    def test_openai_agent_metadata(self) -> None:
        self.assertTrue(OPENAI_AGENT.is_file(), "agents/openai.yaml must exist")
        text = _read(OPENAI_AGENT)
        display = re.search(r'^display_name:\s*"([^"]+)"\s*$', text, re.MULTILINE)
        short = re.search(
            r'^short_description:\s*"([^"]+)"\s*$', text, re.MULTILINE
        )
        prompt_match = re.search(
            r'^default_prompt:\s*"([^"]+)"\s*$', text, re.MULTILINE
        )
        self.assertIsNotNone(display, "display_name must be quoted")
        self.assertIsNotNone(short, "short_description must be quoted")
        self.assertIsNotNone(prompt_match, "default_prompt must be quoted")
        self.assertTrue(25 <= len(short.group(1)) <= 64)
        prompt = prompt_match.group(1)
        self.assertIn("$t3-fleet-dispatch", prompt)
        self.assertEqual(prompt.count("."), 1)
        self.assertNotRegex(prompt, r"[?!]")

    def test_skill_md_command_set_matches_public_ten(self) -> None:
        self.assertTrue(SKILL_MD.is_file(), "SKILL.md must exist")
        found = set(
            re.findall(r"\bt3-fleet\s+([a-z][a-z-]*)\b", _read(SKILL_MD))
        )
        self.assertEqual(found, set(PUBLIC_COMMANDS))

    def test_mutating_commands_require_operator_approval(self) -> None:
        text = _read(SKILL_MD)
        for command in MUTATING_COMMANDS:
            self.assertTrue(
                self._command_co_occurs_with(text, command, "approval"),
                f"{command} must be gated by operator approval",
            )

    def test_approval_required_defaults_are_literal(self) -> None:
        text = _read(SKILL_MD)
        for command in ("submit", "followup"):
            self.assertTrue(
                self._command_co_occurs_with(text, command, "approval-required"),
                f"{command} must document approval-required",
            )

    def _command_co_occurs_with(self, text: str, command: str, needle: str) -> bool:
        lines = text.splitlines()
        for index, line in enumerate(lines):
            if re.search(rf"\b{re.escape(command)}\b", line):
                window = lines[max(0, index - 2) : index + 3]
                if any(needle.lower() in candidate.lower() for candidate in window):
                    return True
        return False

    def test_skill_package_has_no_private_guidance(self) -> None:
        text = _skill_package_text()
        self.assertNotIn("full-access", text)
        for prefix in PRIVATE_PATH_PREFIXES:
            self.assertNotIn(prefix, text, f"skill package includes {prefix}")
        for projection in PROJECTION_PATHS:
            self.assertNotIn(projection, text)

    def test_skill_package_examples_use_generic_hosts(self) -> None:
        text = _skill_package_text()
        hosts = set(re.findall(r"--host[= ]+([^\s\\\"']+)", text))
        self.assertTrue(
            hosts <= GENERIC_ROLES,
            f"skill package names non-generic hosts: {sorted(hosts - GENERIC_ROLES)}",
        )
        self.assertNotRegex(
            text,
            r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
            "skill package must not contain IP addresses",
        )

    def test_skill_package_has_no_inline_prompt_payload(self) -> None:
        text = _skill_package_text()
        self.assertNotRegex(text, r"(?m)^\s*prompt\s*:\s*[\"']")
        self.assertNotRegex(text, r"(?m)^```prompt\s*$")


if __name__ == "__main__":
    unittest.main()
