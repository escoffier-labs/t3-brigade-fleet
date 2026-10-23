from __future__ import annotations

import io
import json
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from t3_fleet import cli
from t3_fleet.config import SettleConfig
from t3_fleet.controller import FleetController
from t3_fleet.errors import HostUnavailable, T3HttpError, ValidationError
from t3_fleet.store import FleetStore
from t3_fleet.t3_api import T3Client, T3SessionIssuer
from t3_fleet.timer import SETTLE_UNIT, install_settle_timer, render_settle_units
from tests import test_agent as legacy
from tests.test_controller import config as controller_config
from tests.test_controller import two_host_peer_config
from tests.test_protocol_v2 import V2Server, thread
from tests.test_t3_api import FakeRunner

PR = {"host": "github.com", "repository": "example-org/example-repo", "number": 7}


class ProtocolV2SettleTranslationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = V2Server()
        self.client = T3Client(
            "http://127.0.0.1:3773",
            T3SessionIssuer(("t3",), runner=FakeRunner()),
            opener=self.server,
        )

    def dispatch(self, command: dict) -> dict:
        with patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc):
            return self.client.dispatch(command)

    def test_settle_is_translated_without_a_settled_at_the_caller_did_not_send(self) -> None:
        self.dispatch({"type": "thread.settle", "commandId": "cmd-1", "threadId": "thread-1"})
        self.assertEqual(
            self.server.commands,
            [{"type": "thread.settle", "commandId": "cmd-1", "threadId": "thread-1"}],
        )

    def test_settle_forwards_an_explicit_settled_at(self) -> None:
        self.dispatch({"type": "thread.settle", "commandId": "cmd-1", "threadId": "thread-1",
                       "settledAt": "2026-10-21T00:00:00Z"})
        self.assertEqual(self.server.commands[0]["settledAt"], "2026-10-21T00:00:00Z")

    def test_pull_request_unlink_is_translated_with_its_identity(self) -> None:
        self.dispatch({"type": "thread.pull-request.unlink", "commandId": "cmd-2",
                       "threadId": "thread-1", **PR})
        self.assertEqual(
            self.server.commands,
            [{"type": "thread.pull-request.unlink", "commandId": "cmd-2",
              "threadId": "thread-1", **PR}],
        )

    def test_other_commands_are_still_refused(self) -> None:
        with self.assertRaisesRegex(T3HttpError, "unsupported T3 protocol 2 dispatch command"):
            self.dispatch({"type": "thread.delete", "commandId": "c", "threadId": "thread-1"})
        self.assertEqual(self.server.commands, [])


class AgentSettleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = legacy.AgentTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.agent = self.fixture.agent
        self.server = V2Server()
        self.client = T3Client(
            "http://127.0.0.1:3773",
            T3SessionIssuer(("t3",), runner=FakeRunner()),
            opener=self.server,
        )
        self.agent.t3 = self.client
        self.fixture._seed_admitted_request()
        self.finish()
        self.request = {"action": "settle", "request_id": "req-12345678", "thread_id": "thread-1"}

    def finish(self, **changes) -> None:
        self.server.shell["threads"] = [dict(thread("completed"), **changes)]
        self.server.projection["thread"] = {**self.server.projection["thread"], **thread("completed"), **changes}
        self.server.projection["runs"][0]["status"] = "completed"

    def handle(self, **overrides) -> dict:
        with patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc):
            return self.agent.handle({**self.request, **overrides})

    def types(self) -> list[str]:
        return [command["type"] for command in self.server.commands]

    def test_a_finished_fleet_thread_is_settled(self) -> None:
        result = self.handle()
        self.assertEqual(self.types(), ["thread.settle"])
        self.assertEqual(self.server.commands[0]["threadId"], "thread-1")
        self.assertEqual((result["outcome"], result["settled"], result["state"]), ("settled", True, "succeeded"))
        self.assertEqual(result["sequence"], 10)

    def test_an_already_settled_thread_sends_nothing(self) -> None:
        for changes in ({"settledOverride": "settled"}, {"settledAt": "2026-10-21T00:00:00Z"}):
            with self.subTest(changes=changes):
                self.finish(**changes)
                result = self.handle()
                self.assertEqual(result["outcome"], "already_settled")
                self.assertFalse(result["settled"])
        self.assertEqual(self.server.commands, [])

    def test_an_operator_unsettled_thread_is_still_settleable(self) -> None:
        self.finish(settledOverride="active", settledAt=None)
        self.assertEqual(self.handle()["outcome"], "settled")

    def test_a_pinned_thread_is_refused(self) -> None:
        self.finish(pinnedAt="2026-10-20T00:00:00Z")
        result = self.handle()
        self.assertEqual((result["outcome"], result["reason"], result["settled"]), ("skipped", "pinned", False))
        self.assertEqual(self.server.commands, [])

    def test_a_thread_without_this_targets_receipt_is_refused(self) -> None:
        for overrides in ({"request_id": "req-unknown01"}, {"thread_id": "thread-other"}):
            with self.subTest(overrides=overrides):
                if "thread_id" in overrides:
                    self.server.shell["threads"].append(dict(thread("completed"), id="thread-other"))
                result = self.handle(**overrides)
                self.assertEqual((result["outcome"], result["reason"]), ("skipped", "not_fleet_thread"))
        self.assertEqual(self.server.commands, [])

    def test_live_work_is_left_alone(self) -> None:
        cases = {
            "running": lambda: self.server.shell.__setitem__("threads", [thread("running")]),
            "queued run": lambda: self.server.projection["runs"].append(
                {"id": "run-2", "threadId": "thread-1", "status": "queued"}),
            "pending request": lambda: self.finish(
                pendingRuntimeRequest={"id": "approval-1", "kind": "command", "createdAt": "2026-10-21T00:00:00Z"}),
            "background task": lambda: self.finish(pendingBackgroundTasks=[{"taskId": "task-1"}]),
        }
        for name, arrange in cases.items():
            with self.subTest(name=name):
                self.finish()
                arrange()
                result = self.handle()
                self.assertEqual((result["outcome"], result["reason"]), ("skipped", "not_terminal"))
        self.assertEqual(self.server.commands, [])

    def test_a_followup_in_flight_is_left_alone(self) -> None:
        with self.agent._request_lock("req-12345678"):
            result = self.handle()
        self.assertEqual((result["outcome"], result["reason"]), ("skipped", "busy"))
        store = self.agent._target_store()
        store.begin_turn("req-12345678", previous_turn_ref="turn-1")
        result = self.handle()
        self.assertEqual((result["outcome"], result["reason"]), ("skipped", "not_terminal"))
        self.assertEqual(self.server.commands, [])

    def test_a_deleted_or_archived_thread_is_reported_without_a_command(self) -> None:
        self.server.shell["threads"] = []
        self.assertEqual(self.handle()["outcome"], "thread_missing")
        self.server.shell["archivedThreads"] = [thread("completed")]
        self.assertEqual(self.handle()["outcome"], "archived")
        self.assertEqual(self.server.commands, [])

    def test_linked_pull_requests_are_unlinked_first_only_when_asked(self) -> None:
        self.finish(pullRequests=[{**PR, "url": "https://github.com/example-org/example-repo/pull/7"}],
                    linkedPullRequest={**PR})
        self.handle()
        self.assertEqual(self.types(), ["thread.settle"])
        self.server.commands.clear()
        result = self.handle(unlink_pull_requests=True)
        self.assertEqual(self.types(), ["thread.pull-request.unlink", "thread.settle"])
        self.assertEqual({k: self.server.commands[0][k] for k in PR}, PR)
        self.assertEqual(result["unlinked"], 1)

    def test_a_failed_unlink_does_not_settle(self) -> None:
        self.finish(pullRequests=[PR])

        def fail(url, token, method, command, timeout):
            self.server.commands.append(command)
            raise T3HttpError("T3 refused")

        with patch("t3_fleet.t3_api.websocket_rpc", fail), self.assertRaises(T3HttpError):
            self.agent.handle({**self.request, "unlink_pull_requests": True})
        self.assertEqual(self.types(), ["thread.pull-request.unlink"])

    def test_the_request_is_validated(self) -> None:
        for request in (
            {"action": "settle", "thread_id": "thread-1"},
            {"action": "settle", "request_id": "req-12345678"},
            {**self.request, "unlink_pull_requests": "yes"},
        ):
            with self.subTest(request=request), self.assertRaises(ValidationError):
                self.agent.handle(request)


class FakeSettleTransport:
    """Answers `settle` like a target: one outcome per thread, a live record of calls."""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.outcomes: dict[str, dict] = {}
        self.down: set[str] = set()

    def call(self, host, request):
        self.calls.append({"host": host.host_id, **request})
        if host.host_id in self.down:
            raise HostUnavailable("simulated outage")
        answer = self.outcomes.get(request["thread_id"], {"outcome": "settled", "sequence": 11})
        return {"schema": "t3-fleet.agent-result.v1", "request_id": request["request_id"],
                "thread_id": request["thread_id"], **answer}


class SettleSweepTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.now = 10_000.0
        self.clock = lambda: self.now
        self.store = FleetStore(Path(self.directory.name) / "fleet.db", clock=lambda: self.now)
        self.transport = FakeSettleTransport()
        self.settings = SettleConfig(enabled=True, grace_minutes=10)
        self.controller = self.build(controller_config())

    def build(self, config) -> FleetController:
        return FleetController(replace(config, settle=self.settings), self.store, self.transport,
                               wall_clock=lambda: self.now)

    def request(self, name: str, final: str | None, *, host: str = "linux-worker", thread_id: str | None = None):
        request_id = f"req-{name}"
        self.store.create_request(request_id, "d" * 64, host, "example-repo")
        thread = thread_id or f"thread-{name}"
        path = {
            None: [],
            "running": ["preflight", "dispatching", "running"],
            "needs_attention": ["preflight", "dispatching", "running", "needs_attention"],
        }.get(final, ["preflight", "dispatching", "running", final])
        for state in path:
            self.store.transition(request_id, state, thread_id=thread if state == "dispatching" else None)
        return request_id, thread

    def age(self, minutes: float) -> None:
        self.now += minutes * 60

    def test_disabled_by_default_makes_no_calls(self) -> None:
        self.settings = SettleConfig()
        controller = self.build(controller_config())
        self.request("done0001", "succeeded")
        self.age(60)
        result = controller.settle_sweep()
        self.assertEqual(result["state"], "disabled")
        self.assertEqual(self.transport.calls, [])

    def test_only_old_terminal_requests_are_asked_about(self) -> None:
        for name, final in (("done0001", "succeeded"), ("fail0001", "failed"), ("intr0001", "interrupted"),
                            ("run00001", "running"), ("attn0001", "needs_attention"), ("queued01", None)):
            self.request(name, final)
        self.age(9)
        self.assertEqual(self.controller.settle_sweep()["settled"], [])
        self.assertEqual(self.transport.calls, [])
        self.age(2)
        result = self.controller.settle_sweep()
        self.assertEqual(sorted(item["request_id"] for item in result["settled"]),
                         ["req-done0001", "req-fail0001", "req-intr0001"])
        self.assertEqual({c["action"] for c in self.transport.calls}, {"settle"})
        self.assertEqual(result["state"], "complete")

    def test_the_request_carries_the_unlink_setting(self) -> None:
        self.request("done0001", "succeeded")
        self.age(20)
        self.controller.settle_sweep()
        self.assertFalse(self.transport.calls[0]["unlink_pull_requests"])
        self.settings = SettleConfig(enabled=True, unlink_pull_requests=True)
        self.request("done0002", "succeeded")
        self.age(20)
        self.build(controller_config()).settle_sweep()
        self.assertTrue(self.transport.calls[-1]["unlink_pull_requests"])

    def test_a_second_sweep_makes_no_new_commands(self) -> None:
        self.request("done0001", "succeeded")
        self.age(20)
        first = self.controller.settle_sweep()
        calls = len(self.transport.calls)
        second = self.controller.settle_sweep()
        self.assertEqual(len(first["settled"]), 1)
        self.assertEqual((second["settled"], second["examined"]), ([], 0))
        self.assertEqual(len(self.transport.calls), calls)
        receipt = self.store.settle_receipt("req-done0001")
        self.assertEqual((receipt.outcome, receipt.thread_id, receipt.sequence), ("settled", "thread-done0001", 11))

    def test_a_followup_that_finishes_again_is_settled_again(self) -> None:
        request_id, _ = self.request("done0001", "succeeded")
        self.age(20)
        self.controller.settle_sweep()
        self.age(1)
        self.store.transition(request_id, "dispatching")
        self.store.transition(request_id, "running")
        self.store.transition(request_id, "succeeded")
        self.assertEqual(self.controller.settle_sweep()["settled"], [])
        self.age(20)
        self.assertEqual(len(self.controller.settle_sweep()["settled"]), 1)
        self.assertEqual(len(self.transport.calls), 2)

    def test_refusals_leave_no_receipt_and_are_retried(self) -> None:
        self.request("pinned01", "succeeded")
        self.request("live0001", "succeeded")
        self.request("other001", "succeeded")
        self.transport.outcomes = {
            "thread-pinned01": {"outcome": "skipped", "reason": "pinned"},
            "thread-live0001": {"outcome": "skipped", "reason": "not_terminal"},
            "thread-other001": {"outcome": "skipped", "reason": "not_fleet_thread"},
        }
        self.age(20)
        result = self.controller.settle_sweep()
        self.assertEqual(result["settled"], [])
        self.assertEqual(sorted((s["request_id"], s["reason"]) for s in result["skipped"]),
                         [("req-live0001", "not_terminal"), ("req-other001", "not_fleet_thread"),
                          ("req-pinned01", "pinned")])
        self.assertIsNone(self.store.settle_receipt("req-pinned01"))
        self.transport.outcomes = {}
        self.assertEqual(len(self.controller.settle_sweep()["settled"]), 3)

    def test_a_missing_or_archived_thread_is_final(self) -> None:
        self.request("gone0001", "failed")
        self.request("arch0001", "succeeded")
        self.transport.outcomes = {"thread-gone0001": {"outcome": "thread_missing"},
                                   "thread-arch0001": {"outcome": "archived"}}
        self.age(20)
        self.controller.settle_sweep()
        calls = len(self.transport.calls)
        self.assertEqual(self.controller.settle_sweep()["examined"], 0)
        self.assertEqual(len(self.transport.calls), calls)
        self.assertEqual(self.store.settle_receipt("req-gone0001").outcome, "thread_missing")

    def test_an_unavailable_host_is_skipped_and_retried_next_sweep(self) -> None:
        config = two_host_peer_config()
        controller = self.build(config)
        for name in ("one00001", "one00002", "one00003"):
            self.request(name, "succeeded", host="linux-worker")
        self.request("two00001", "succeeded", host="second-worker")
        self.transport.down = {"linux-worker"}
        self.age(20)
        result = controller.settle_sweep()
        self.assertEqual(result["state"], "partial")
        self.assertEqual(result["unavailable"], ["linux-worker"])
        self.assertEqual([item["request_id"] for item in result["settled"]], ["req-two00001"])
        # Two failures end the host for this sweep, so the third request is not called.
        self.assertEqual(sum(1 for c in self.transport.calls if c["host"] == "linux-worker"), 2)
        for name in ("one00001", "one00002", "one00003"):
            self.assertIsNone(self.store.settle_receipt(f"req-{name}"))
            self.assertEqual(self.store.get(f"req-{name}").state, "succeeded")
        self.transport.down = set()
        retry = controller.settle_sweep()
        self.assertEqual(len(retry["settled"]), 3)
        self.assertEqual(retry["state"], "complete")

    def test_a_host_removed_from_config_is_an_error_not_a_crash(self) -> None:
        self.request("done0001", "succeeded", host="retired-worker")
        self.request("done0002", "succeeded")
        self.age(20)
        result = self.controller.settle_sweep()
        self.assertEqual(result["errors"], [{"request_id": "req-done0001", "code": "invalid_request"}])
        self.assertEqual(len(result["settled"]), 1)
        self.assertEqual(result["state"], "partial")

    def test_a_response_for_another_thread_is_not_recorded(self) -> None:
        self.request("done0001", "succeeded")
        self.age(20)
        with patch.object(self.transport, "call", return_value={"outcome": "settled", "thread_id": "x", "request_id": "y"}):
            result = self.controller.settle_sweep()
        self.assertEqual(result["errors"], [{"request_id": "req-done0001", "code": "invalid_response"}])
        self.assertIsNone(self.store.settle_receipt("req-done0001"))

    def test_requests_without_a_thread_are_never_asked_about(self) -> None:
        request_id, _ = self.request("nothread1", None)
        self.age(20)
        self.assertEqual(self.controller.settle_sweep()["examined"], 0)


class SettleCliTests(unittest.TestCase):
    CONFIG = """
schema = "t3-fleet.config.v1"
controller_host = "controller"
global_active_limit = 1

[hosts.controller]
enabled = true
platform = "linux"
transport = "local"
agent_command = ["/opt/t3-fleet/bin/t3-fleet-agent"]
active_limit = 1
expected_hostname = "controller"

[repositories.example-repo]
default_branch = "main"
branch_prefix = "codex/"

[repositories.example-repo.paths]
controller = "/srv/repos/example"
"""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.config = self.root / "config.toml"
        self.config.write_text(self.CONFIG + "\n[settle]\nenabled = true\n", encoding="utf-8")
        self.config.chmod(0o600)
        self.state = self.root / "fleet.db"

    def run_cli(self, *argv: str, stdin: str = "") -> tuple[int, dict]:
        out = io.StringIO()
        code = cli.main(["--config", str(self.config), "--state", str(self.state), *argv],
                        stdin=io.StringIO(stdin), stdout=out, stderr=io.StringIO(), environ={})
        return code, json.loads(out.getvalue())

    def test_settle_sweep_runs_from_the_cli(self) -> None:
        code, result = self.run_cli("settle-sweep")
        self.assertEqual(code, 0)
        self.assertEqual((result["action"], result["state"], result["examined"]), ("settle-sweep", "complete", 0))

    def test_a_model_facing_invoke_cannot_run_operator_commands(self) -> None:
        for action in ("settle-sweep", "install-settle-timer"):
            with self.subTest(action=action):
                code, result = self.run_cli("invoke", stdin=json.dumps({"action": action}))
                self.assertEqual(code, 2)
                self.assertEqual(result["error"]["code"], "invalid_request")

    def test_install_writes_the_units_and_names_the_next_commands(self) -> None:
        units = self.root / "units"
        code, result = self.run_cli("install-settle-timer", "--dir", str(units))
        self.assertEqual(code, 0)
        self.assertEqual((result["state"], result["settle_enabled"]), ("installed", True))
        self.assertEqual(sorted(p.name for p in units.iterdir()),
                         [f"{SETTLE_UNIT}.service", f"{SETTLE_UNIT}.timer"])
        self.assertIn("systemctl --user enable --now", result["next"][1])
        service = (units / f"{SETTLE_UNIT}.service").read_text()
        self.assertIn("-m t3_fleet", service)
        self.assertIn(f"--config {self.config.resolve()}", service)
        self.assertTrue(service.rstrip().endswith("settle-sweep"))

    def test_print_renders_without_writing(self) -> None:
        units = self.root / "units"
        code, result = self.run_cli("install-settle-timer", "--dir", str(units), "--print")
        self.assertEqual((code, result["state"]), (0, "rendered"))
        self.assertFalse(units.exists())
        self.assertIn("OnUnitActiveSec=10min", result["content"][f"{SETTLE_UNIT}.timer"])

    def test_an_out_of_range_interval_is_refused(self) -> None:
        for minutes in ("0", "1441"):
            code, result = self.run_cli("install-settle-timer", "--print", "--interval-minutes", minutes)
            self.assertEqual((code, result["error"]["code"]), (2, "invalid_request"))


class TimerTests(unittest.TestCase):
    def test_exec_start_quotes_paths_and_escapes_specifiers(self) -> None:
        units = render_settle_units(config_path=Path("/srv/fleet cfg/50%/config.toml"),
                                    state_path=Path("/s/fleet.db"), interval_minutes=15,
                                    python="/usr/bin/python3")
        service = units[f"{SETTLE_UNIT}.service"]
        self.assertIn('"/srv/fleet cfg/50%%/config.toml"', service)
        self.assertIn("Type=oneshot", service)
        self.assertIn("OnUnitActiveSec=15min", units[f"{SETTLE_UNIT}.timer"])

    def test_enable_runs_systemctl_and_reports_failure(self) -> None:
        calls = []

        def runner(command, **kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, "", "")

        with tempfile.TemporaryDirectory() as directory:
            result = install_settle_timer(
                config_path=Path(directory) / "c.toml", state_path=Path(directory) / "s.db",
                unit_dir=Path(directory) / "u", interval_minutes=10, write=True, enable=True,
                settle_enabled=False, runner=runner)
            self.assertEqual(result["state"], "enabled")
            self.assertEqual([c[2] for c in calls], ["daemon-reload", "enable"])
            self.assertFalse(result["settle_enabled"])
            with self.assertRaises(Exception):
                install_settle_timer(
                    config_path=Path(directory) / "c.toml", state_path=Path(directory) / "s.db",
                    unit_dir=Path(directory) / "u", interval_minutes=10, write=True, enable=True,
                    settle_enabled=True,
                    runner=lambda command, **kw: subprocess.CompletedProcess(command, 1, "", ""))


if __name__ == "__main__":
    unittest.main()
