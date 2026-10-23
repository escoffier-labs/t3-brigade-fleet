from __future__ import annotations

import copy
import unittest
from pathlib import Path
from unittest.mock import patch

from t3_fleet.errors import PolicyRejected, T3HttpError, ValidationError
from t3_fleet.t3_api import T3Client, T3SessionIssuer
from tests import test_agent as legacy
from tests.test_t3_api import FakeResponse, FakeRunner


MODEL = {"instanceId": "codex", "model": "gpt-5.6-terra", "options": {"reasoningEffort": "high", "serviceTier": "priority"}}

NATIVE_MODEL = {**MODEL, "options": [{"id": key, "value": value} for key, value in MODEL["options"].items()]}


def thread(status="running"):
    return {"id": "thread-1", "projectId": "project-1", "status": status,
            "modelSelection": copy.deepcopy(NATIVE_MODEL), "runtimeMode": "full-access",
            "interactionMode": "default", "latestRunId": "run-1", "activeRunId": "run-1" if status in {"queued", "preparing", "starting", "running", "waiting"} else None,
            "pendingRuntimeRequest": None, "activityRunStatus": None,
            "pendingBackgroundTasks": [], "updatedAt": "2026-10-03T15:00:00Z"}


class V2Server:
    def __init__(self):
        self.requests = []
        self.commands = []
        self.descriptor = {"serverVersion": "0.0.46-nightly.20261003.2623", "orchestrationProtocolVersion": 2, "capabilities": {"threadSettlement": True}}
        self.shell = {"schemaVersion": 2, "snapshotSequence": 9, "projects": [{"id": "project-1", "workspaceRoot": "/srv/repos/example"}], "threads": [thread()], "archivedThreads": []}
        self.projection = {"thread": {**thread(), "branch": None, "worktreePath": None, "lineage": {"parentThreadId": None, "rootThreadId": "thread-1", "relationshipToParent": None}}, "runs": [{"id": "run-1", "threadId": "thread-1", "status": "running"}], "messages": [], "runtimeRequests": []}

    def __call__(self, request, timeout):
        self.requests.append(request)
        if request.full_url.endswith("/.well-known/t3/environment"):
            return FakeResponse(self.descriptor)
        if request.full_url.endswith("/shell"):
            return FakeResponse(self.shell)
        if "/threads/" in request.full_url:
            return FakeResponse({"snapshotSequence": 9, "projection": self.projection})
        raise AssertionError("removed v2 HTTP endpoint: " + request.full_url)

    def rpc(self, url, token, method, command, timeout):
        self.commands.append(copy.deepcopy(command))
        if command["type"] == "thread.create":
            self.projection["thread"].update(copy.deepcopy(command))
            selection = command["modelSelection"]
            if not isinstance(selection.get("options"), list):
                raise AssertionError("v2 dispatch requires native option arrays")
            self.projection["thread"]["id"] = command["threadId"]
            self.projection["thread"]["lineage"] = {"parentThreadId": None, "rootThreadId": command["threadId"], "relationshipToParent": None}
            self.projection["runs"] = []
        if command["type"] == "thread.metadata.update":
            self.projection["thread"].update(command)
        return {"sequence": 10}


class ProtocolV2Tests(unittest.TestCase):
    def setUp(self):
        self.fixture = legacy.AgentTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.agent = self.fixture.agent
        self.server = V2Server()
        # Start with the existing constructor. RPC is patched below once the
        # transport exists, so read regressions fail on the old protocol itself.
        self.runner = FakeRunner()
        self.client = T3Client("http://127.0.0.1:3773", T3SessionIssuer(("t3",), runner=self.runner), opener=self.server)
        self.agent.t3 = self.client

    def _seed_idle_no_turn_with_worktree(self):
        self.fixture._seed_admitted_request()
        self.server.shell["threads"] = [dict(thread("idle"), latestRunId=None, runCount=0)]
        self.server.projection["thread"] = dict(thread("idle"), latestRunId=None, runCount=0)
        self.server.projection["runs"] = []
        store = self.agent._target_store()
        store.reconcile("req-12345678", state="unknown", thread_id="thread-1")
        worktree = Path(self.agent._worktree_path("req-12345678"))
        worktree.mkdir(parents=True)
        (worktree / "retained.txt").write_text("retained work", encoding="utf-8")
        return worktree

    def test_reconcile_old_idle_no_turn_fails_without_touching_worktree(self):
        worktree = self._seed_idle_no_turn_with_worktree()
        self.agent._now = lambda: "2026-07-29T01:00:00Z"
        result = self.agent.handle({"action": "reconcile", "request_id": "req-12345678"})
        self.assertEqual(result["state"], "failed")
        self.assertEqual(self.agent._target_store().active_count(), 0)
        self.assertEqual([p.name for p in worktree.iterdir()], ["retained.txt"])
        self.assertEqual((worktree / "retained.txt").read_text(encoding="utf-8"), "retained work")
        self.assertEqual(self.fixture.git_calls, [])
        self.assertEqual(self.server.commands, [])

    def test_reconcile_young_idle_no_turn_preserves_capacity(self):
        self._seed_idle_no_turn_with_worktree()
        self.agent._now = lambda: "2026-07-28T02:00:00Z"
        result = self.agent.handle({"action": "reconcile", "request_id": "req-12345678"})
        self.assertEqual(result["state"], "unknown")
        self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_reconcile_old_no_turn_with_live_work_preserves_capacity(self):
        self._seed_idle_no_turn_with_worktree()
        self.agent._now = lambda: "2026-07-29T01:00:00Z"
        for changes in ({"status": "running"}, {"activeRunId": "new-run"},
                        {"activityRunStatus": "running"},
                        {"pendingBackgroundTasks": [{"taskId": "task-1"}]}):
            with self.subTest(changes=changes):
                self.server.shell["threads"] = [dict(thread("idle"), latestRunId=None, **changes)]
                result = self.agent.handle({"action": "reconcile", "request_id": "req-12345678"})
                self.assertEqual(result["state"], "running")
                self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_reconcile_old_idle_no_turn_with_detail_work_preserves_capacity(self):
        self._seed_idle_no_turn_with_worktree()
        self.agent._now = lambda: "2026-07-29T01:00:00Z"
        cases = (
            ("nodes", [{"id": "work-1", "status": "running"}]),
            ("subagents", [{"id": "work-1", "status": "running"}]),
        )
        for key, value in cases:
            for include_output in (False, True):
                with self.subTest(key=key, include_output=include_output):
                    self.server.projection[key] = value
                    result = self.agent.handle({
                        "action": "reconcile", "request_id": "req-12345678",
                        "include_output": include_output,
                    })
                    self.assertEqual(result["state"], "unknown")
                    self.assertEqual(self.agent._target_store().active_count(), 1)
                    self.server.projection.pop(key)

    def test_reconcile_old_nonidle_no_turn_preserves_capacity(self):
        self._seed_idle_no_turn_with_worktree()
        self.agent._now = lambda: "2026-07-29T01:00:00Z"
        self.server.shell["threads"][0]["status"] = "completed"
        result = self.agent.handle({"action": "reconcile", "request_id": "req-12345678"})
        self.assertEqual(result["state"], "unknown")
        self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_reconcile_locked_old_idle_no_turn_preserves_capacity(self):
        self._seed_idle_no_turn_with_worktree()
        self.agent._now = lambda: "2026-07-29T01:00:00Z"
        with self.agent._request_lock("req-12345678"):
            result = self.agent.handle({"action": "reconcile", "request_id": "req-12345678"})
        self.assertEqual(result["state"], "unknown")
        self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_v2_reads_negotiate_header_and_keep_projection(self):
        self.client.shell_snapshot()
        snapshot = self.client.thread_snapshot("thread-1")
        for request in self.server.requests:
            if "/api/orchestration/" in request.full_url:
                self.assertEqual(request.get_header("X-t3-orchestration-protocol"), "2")
        self.assertIn("projection", snapshot)
        self.assertEqual(snapshot["thread"]["runs"][0]["id"], "run-1")
        self.assertEqual(self.runner.calls[-1][-2:], ["revoke", "session-1"])

    def test_native_options_normalize_without_changing_projection(self):
        options = list(reversed(NATIVE_MODEL["options"])) + [{"id": "fastMode", "value": False}, {"id": "custom", "value": "actual"}]
        self.server.projection["thread"]["modelSelection"]["options"] = options
        snapshot = self.client.thread_snapshot("thread-1")
        self.assertEqual(snapshot["thread"]["modelSelection"]["options"], {**MODEL["options"], "fastMode": False, "custom": "actual"})
        self.assertEqual(snapshot["projection"]["thread"]["modelSelection"]["options"], options)

    def test_malformed_and_duplicate_options_are_rejected(self):
        for options in ([{"id": "reasoningEffort", "value": "high"}] * 2, [{"id": "x"}], [{"id": "", "value": True}], [{"id": " x", "value": True}], [{"id": "x", "value": 0}], [{"id": "x", "value": None}], [{"id": "x", "value": ""}], [{"id": "x", "value": " value "}], [{"id": "x", "value": True, "extra": True}], [False], None, {"x": 0}):
            with self.subTest(options=options):
                self.server.projection["thread"]["modelSelection"]["options"] = options
                with self.assertRaises(T3HttpError):
                    self.client.thread_snapshot("thread-1")

    def test_shell_defaults_preserve_all_options_and_null(self):
        project = self.server.shell["projects"][0]
        project["defaultModelSelection"] = {**NATIVE_MODEL, "options": NATIVE_MODEL["options"] + [{"id": "fastMode", "value": False}]}
        shell = self.client.shell_snapshot()
        self.assertEqual(shell["projects"][0]["defaultModelSelection"]["options"], {**MODEL["options"], "fastMode": False})
        observed = self.agent.handle({"action": "doctor", "repository": "example-repo"})
        self.assertEqual(observed["observed_project_default"]["options"], {**MODEL["options"], "fastMode": False})
        project["defaultModelSelection"] = None
        self.assertIsNone(self.client.shell_snapshot()["projects"][0]["defaultModelSelection"])
        self.assertIsNone(self.agent.handle({"action": "doctor", "repository": "example-repo"})["observed_project_default"])

    def test_reordered_options_match_but_changed_identity_blocks_continuation(self):
        self.fixture._seed_admitted_request()
        selection = self.server.projection["thread"]["modelSelection"]
        selection["options"].reverse()
        command = {"type": "thread.turn.start", "commandId": "cmd", "threadId": "thread-1", "modelSelection": MODEL, "runtimeMode": "full-access", "interactionMode": "default", "message": {"messageId": "msg", "text": "continue", "attachments": []}}
        with patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc):
            self.client.dispatch(command)
        for change in ({"model": "other"}, {"instanceId": "other"}, {"options": [{"id": "reasoningEffort", "value": "low"}, {"id": "serviceTier", "value": "priority"}]}, {"options": [{"id": "reasoningEffort", "value": "high"}, {"id": "serviceTier", "value": "standard"}]}, {"options": NATIVE_MODEL["options"] + [{"id": "fastMode", "value": False}]}):
            self.server.projection["thread"]["modelSelection"] = {**copy.deepcopy(NATIVE_MODEL), **change}
            with self.subTest(change=change):
                with self.assertRaises(T3HttpError):
                    self.client.dispatch(command)
                with self.assertRaises(PolicyRejected):
                    self.agent._validate_thread_model_admission(self.client.thread_snapshot("thread-1")["thread"], legacy.ADMISSION)

    def test_legacy_inbound_option_map_is_validated_and_kept(self):
        self.server.projection["thread"]["modelSelection"] = copy.deepcopy(MODEL)
        snapshot = self.client.thread_snapshot("thread-1")
        self.assertEqual(snapshot["thread"]["modelSelection"], MODEL)
        self.assertEqual(snapshot["projection"]["thread"]["modelSelection"], MODEL)

    def test_extra_native_options_block_prelaunch_and_approval(self):
        rpc = self.server.rpc
        def changed_binding(*args):
            result = rpc(*args)
            if args[3]["type"] == "thread.metadata.update":
                self.server.projection["thread"]["modelSelection"]["options"].append({"id": "fastMode", "value": False})
            return result
        with patch("t3_fleet.t3_api.websocket_rpc", changed_binding):
            with self.assertRaises(ValidationError):
                self.agent.handle(self.fixture.submit_request)
        self.assertNotIn("message.dispatch", [command["type"] for command in self.server.commands])
        self.server.commands.clear()
        self.server.projection["thread"] = thread()
        self.server.projection["thread"]["modelSelection"]["options"].append({"id": "fastMode", "value": False})
        self.server.projection["runtimeRequests"] = [{"id": "approval-1", "kind": "command", "status": "pending", "responseCapability": {"type": "live", "providerSessionId": "s"}}]
        with patch("t3_fleet.t3_api.websocket_rpc", rpc):
            with self.assertRaises(PolicyRejected):
                self.agent.handle({"action": "approve", "request_id": "req-12345678", "thread_id": "thread-1", "approval_id": "approval-1", "decision": "accept", "model_admission": legacy.ADMISSION})
        self.assertEqual(self.server.commands, [])

    def test_doctor_and_capacity_count_each_active_v2_state(self):
        for state in ("queued", "preparing", "starting", "running", "waiting"):
            self.server.shell["threads"] = [thread(state), thread("completed")]
            self.server.shell["threads"][1]["id"] = "thread-2"
            for action in ("doctor", "capacity"):
                with self.subTest(state=state, action=action):
                    result = self.agent.handle({"action": action})
                    self.assertEqual(result["state"], "eligible")
                    self.assertEqual(result["active"], 1)

    def test_completed_foreground_with_pending_work_still_holds_capacity(self):
        t = thread("completed")
        t["pendingBackgroundTasks"] = [{"taskId": "bg-1", "kind": "command"}]
        self.server.shell["threads"] = [t]
        self.assertEqual(self.agent.handle({"action": "capacity"})["active"], 1)
        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})
        self.assertEqual(result["state"], "running")
        self.assertEqual(result["pending_background_tasks"], t["pendingBackgroundTasks"])

    def test_v2_status_observes_terminal_output_for_current_run(self):
        self.server.shell["threads"] = [thread("completed")]
        self.server.projection["runs"][0]["status"] = "completed"
        self.server.projection["messages"] = [
            {"role": "assistant", "runId": "old-run", "text": "stale", "streaming": False},
            {"role": "assistant", "runId": "run-1", "text": "finished", "streaming": False},
            {"role": "assistant", "runId": "run-1", "text": "partial", "streaming": True}]
        result = self.agent.handle({"action": "status", "thread_id": "thread-1", "include_output": True})
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(result["output"], "finished")

    def test_v2_thread_projection_over_one_mib_stays_controllable(self):
        # A long thread's projection outgrows the general 1 MiB response cap.
        self.server.shell["threads"] = [thread("completed")]
        self.server.projection["runs"][0]["status"] = "completed"
        history = [
            {"role": "assistant", "runId": "old-run", "text": "y" * 65_536, "streaming": False}
            for _ in range(40)
        ]
        self.server.projection["messages"] = history + [
            {"role": "assistant", "runId": "run-1", "text": "finished", "streaming": False}]
        snapshot = self.client.thread_snapshot("thread-1")
        self.assertEqual(len(snapshot["thread"]["messages"]), 41)
        result = self.agent.handle({"action": "status", "thread_id": "thread-1", "include_output": True})
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(result["output"], "finished")

    def test_v2_thread_projection_keeps_an_upper_bound(self):
        from t3_fleet.t3_api import MAX_THREAD_RESPONSE_BYTES

        self.server.projection["messages"] = [
            {"role": "assistant", "runId": "run-1", "text": "z" * (MAX_THREAD_RESPONSE_BYTES + 1), "streaming": False}]
        with self.assertRaisesRegex(T3HttpError, "exceeded"):
            self.client.thread_snapshot("thread-1")

    def test_waiting_runtime_request_needs_attention(self):
        t = thread("waiting")
        t["pendingRuntimeRequest"] = {"id": "req-1", "kind": "user_input", "createdAt": "2026-10-03T15:00:00Z"}
        self.server.shell["threads"] = [t]
        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})
        self.assertEqual(result["state"], "needs_attention")
        self.assertEqual(result["pending_runtime_request"], t["pendingRuntimeRequest"])

    def test_readiness_rejects_malformed_or_unknown_v2_state(self):
        mutations = [{"status": "surprise"}, {"pendingBackgroundTasks": None}, {"pendingRuntimeRequest": {}}, {"activeRunId": 2}, {"activityRunStatus": "surprise"}]
        for mutation in mutations:
            self.server.shell["threads"] = [{**thread(), **mutation}]
            for action in ("doctor", "capacity", "status"):
                with self.subTest(mutation=mutation, action=action):
                    with self.assertRaises((ValidationError, T3HttpError)):
                        self.agent.handle({"action": action, "thread_id": "thread-1"})

    def test_capacity_shares_descriptor_compatibility_gate(self):
        for version, protocol in (("0.0.47", 2), ("0.2.0", 2), ("0.1", 2), ("0.0.46", 3), ("0.0.46", True), ("0.0.46", None)):
            self.server.descriptor.update(serverVersion=version, orchestrationProtocolVersion=protocol)
            with self.subTest(version=version, protocol=protocol):
                with self.assertRaises(T3HttpError):
                    self.client.shell_snapshot()

    def test_readiness_accepts_0_1_versions_with_protocol_v2(self):
        for version in (
            "0.1.0",
            "0.1.1-nightly.20261017.3234",
            "0.1.1-preview.20261016.3231",
            "0.1.12",
            "0.1.12+build.7",
            "0.1.12-rc.1+build.7",
        ):
            self.server.descriptor["serverVersion"] = version
            for action in ("doctor", "capacity"):
                with self.subTest(version=version, action=action):
                    self.server.requests.clear()
                    self.agent.t3 = T3Client(
                        "http://127.0.0.1:3773",
                        T3SessionIssuer(("t3",), runner=self.runner),
                        opener=self.server,
                    )
                    self.assertEqual(self.agent.handle({"action": action})["state"], "eligible")
                    self.assertTrue(any(
                        request.full_url.endswith("/shell")
                        and request.get_header("X-t3-orchestration-protocol") == "2"
                        for request in self.server.requests
                    ))

    def test_readiness_rejects_outside_or_malformed_0_1_versions(self):
        # Use the agent's protocol boundary so the transport cannot mask a
        # too-permissive readiness gate.
        self.agent.t3 = self.fixture.fake_t3
        self.fixture.fake_t3.shell_snapshot = lambda: copy.deepcopy(self.server.shell)
        for version in ("0.2.0", "1.0.0", "0.1", "0.1.1-", "0.1.1+", "0.0.41"):
            self.fixture.fake_t3.descriptor = lambda: {
                **self.server.descriptor, "serverVersion": version,
            }
            for action in ("doctor", "capacity"):
                with self.subTest(version=version, action=action):
                    self.assertEqual(self.agent.handle({"action": action})["state"], "incompatible")

    def test_0_1_readiness_requires_v2_descriptor_and_shell(self):
        self.server.descriptor["serverVersion"] = "0.1.12"
        self.server.shell["threads"] = []
        for protocol, schema in ((1, 2), (2, 1), (1, 1)):
            self.server.descriptor["orchestrationProtocolVersion"] = protocol
            self.server.shell["schemaVersion"] = schema
            for action in ("doctor", "capacity"):
                with self.subTest(protocol=protocol, schema=schema, action=action):
                    # Check readiness independently of transport validation.
                    self.agent.t3 = self.fixture.fake_t3
                    self.fixture.fake_t3.descriptor = lambda: copy.deepcopy(self.server.descriptor)
                    self.fixture.fake_t3.shell_snapshot = lambda: copy.deepcopy(self.server.shell)
                    self.assertEqual(self.agent.handle({"action": action})["state"], "incompatible")
                    # A new real client must also reject the mismatched contract.
                    self.agent.t3 = T3Client(
                        "http://127.0.0.1:3773",
                        T3SessionIssuer(("t3",), runner=self.runner),
                        opener=self.server,
                    )
                    with self.assertRaises(T3HttpError):
                        self.agent.handle({"action": action})

    def test_v2_dispatch_creates_binds_and_launches_authorized_thread(self):
        with patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc, create=True):
            result = self.agent.handle(self.fixture.submit_request)
        self.assertEqual(result["state"], "running")
        create, bind, start = self.server.commands
        self.assertEqual(create["createdBy"], "agent")
        self.assertEqual(create["creationSource"], "server")
        self.assertEqual(bind["type"], "thread.metadata.update")
        self.assertTrue(bind["expectedEmpty"])
        self.assertEqual(start["type"], "message.dispatch")
        self.assertEqual(start["dispatchMode"], {"type": "start_immediately"})
        self.assertEqual(start["modelSelection"], NATIVE_MODEL)
        self.assertEqual(start["text"], "private prompt text")
        self.assertEqual(self.server.projection["thread"]["worktreePath"], self.agent._worktree_path("req-12345678"))
        self.assertTrue(any("worktree" in call for call in self.fixture.git_calls))

    def test_unknown_v2_dispatch_is_explicitly_rejected(self):
        with patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc, create=True):
            with self.assertRaises(T3HttpError):
                self.client.dispatch({"type": "thread.session.stop", "threadId": "thread-1", "commandId": "cmd"})
        self.assertEqual(self.server.commands, [])

    def test_installed_cohorts_and_legacy_validation_are_preserved(self):
        self.agent.t3 = self.fixture.fake_t3
        for version in ("0.0.43-nightly.1", "0.0.45+local.2"):
            self.fixture.fake_t3.server_version = version
            self.assertEqual(self.agent.handle({"action": "doctor"})["state"], "eligible")
        self.fixture.fake_t3.thread["session"] = {"status": "surprise"}
        for action in ("doctor", "capacity"):
            with self.assertRaises(ValidationError):
                self.agent.handle({"action": action})

    def test_submit_rejects_acknowledged_but_wrong_worktree_binding(self):
        rpc = self.server.rpc
        def wrong_binding(*args):
            result = rpc(*args)
            if args[3]["type"] == "thread.metadata.update":
                self.server.projection["thread"]["worktreePath"] = "/wrong/path"
            return result
        with patch("t3_fleet.t3_api.websocket_rpc", wrong_binding):
            with self.assertRaises((T3HttpError, ValidationError)):
                self.agent.handle(self.fixture.submit_request)
        self.assertNotIn("message.dispatch", [c["type"] for c in self.server.commands])

    def test_submit_rejects_non_root_thread_before_launch(self):
        rpc = self.server.rpc
        def child_binding(*args):
            result = rpc(*args)
            self.server.projection["thread"]["lineage"]["parentThreadId"] = "parent-1"
            return result
        with patch("t3_fleet.t3_api.websocket_rpc", child_binding):
            with self.assertRaises((T3HttpError, ValidationError)):
                self.agent.handle(self.fixture.submit_request)
        self.assertNotIn("message.dispatch", [c["type"] for c in self.server.commands])

    def test_runtime_responses_translate_and_enforce_resumability(self):
        self.fixture._seed_admitted_request()
        self.server.projection["runtimeRequests"] = [{"id": "approval-1", "kind": "command", "status": "pending", "responseCapability": {"type": "live", "providerSessionId": "s"}}]
        with patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc):
            result = self.agent.handle({"action": "approve", "request_id": "req-12345678", "thread_id": "thread-1", "approval_id": "approval-1", "decision": "accept", "model_admission": legacy.ADMISSION})
        self.assertEqual(result["sequence"], 10)
        self.assertEqual(self.server.commands[-1]["type"], "runtime-request.respond")
        self.assertEqual(self.server.commands[-1]["decision"], "accept")
        self.server.projection["runtimeRequests"][0].update(id="input-1", kind="user_input")
        with patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc):
            self.agent.handle({"action": "answer", "request_id": "req-12345678", "thread_id": "thread-1", "input_id": "input-1", "answers": {"q": ["yes"]}, "model_admission": legacy.ADMISSION})
        self.assertEqual(self.server.commands[-1]["answers"], {"q": ["yes"]})
        self.server.projection["runtimeRequests"][0]["responseCapability"] = {"type": "not_resumable", "reason": "expired"}
        count = len(self.server.commands)
        with self.assertRaises(ValidationError):
            self.agent.handle({"action": "answer", "request_id": "req-12345678", "thread_id": "thread-1", "input_id": "input-1", "answers": {"q": "yes"}, "model_admission": legacy.ADMISSION})
        self.assertEqual(len(self.server.commands), count)

    def test_interrupt_cancels_queue_and_observes_actual_state(self):
        self.server.projection["runs"].append({"id": "run-2", "threadId": "thread-1", "status": "queued"})
        with patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc):
            result = self.agent.handle({"action": "interrupt", "thread_id": "thread-1"})
        self.assertEqual([c["type"] for c in self.server.commands], ["run.interrupt", "queued-run.cancel"])
        self.assertEqual(result["state"], "running")
        self.assertTrue(self.server.commands[0]["holdQueue"])

    def test_followup_launches_exact_model_and_rejects_mode_change(self):
        self.fixture._seed_admitted_request()
        self.server.shell["threads"] = [thread("completed")]
        self.server.projection["runs"][0]["status"] = "completed"
        request = {"action": "followup", "request_id": "req-12345678", "thread_id": "thread-1", "prompt": "continue", "runtime_mode": "full-access", "model_admission": legacy.ADMISSION}
        with patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc):
            result = self.agent.handle(request)
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.server.commands[-1]["modelSelection"], NATIVE_MODEL)
        request["runtime_mode"] = "approval-required"
        with self.assertRaises(T3HttpError):
            self.agent.handle(request)

    def test_terminal_detail_cannot_supply_stale_output_or_terminal_success(self):
        self.server.shell["threads"] = [thread("completed")]
        self.server.projection["runs"][0]["status"] = "running"
        self.server.projection["messages"] = [{"role": "assistant", "runId": "old", "text": "old output", "streaming": False}]
        result = self.agent.handle({"action": "status", "thread_id": "thread-1", "include_output": True})
        self.assertEqual(result["state"], "unknown")
        self.assertNotIn("output", result)

    def test_shared_gate_rejects_descriptor_schema_mismatch(self):
        self.agent.t3 = self.fixture.fake_t3
        self.fixture.fake_t3.server_version = "0.0.46"
        for action in ("doctor", "capacity"):
            self.assertEqual(self.agent.handle({"action": action})["state"], "incompatible")

    def test_runtime_acknowledgement_keeps_unresolved_pending_request_visible(self):
        self.fixture._seed_admitted_request()
        pending = {"id": "approval-1", "kind": "command", "createdAt": "2026-10-03T15:00:00Z"}
        self.server.shell["threads"][0].update(status="waiting", pendingRuntimeRequest=pending)
        self.server.projection["runtimeRequests"] = [{**pending, "status": "pending", "responseCapability": {"type": "live", "providerSessionId": "s"}}]
        with patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc):
            result = self.agent.handle({"action": "approve", "request_id": "req-12345678", "thread_id": "thread-1", "approval_id": "approval-1", "decision": "accept", "model_admission": legacy.ADMISSION})
        self.assertEqual(result["state"], "needs_attention")
        self.assertEqual(result["pending_runtime_request"], pending)
        pending.update(id="input-1", kind="user_input")
        self.server.projection["runtimeRequests"][0].update(id="input-1", kind="user_input")
        with patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc):
            result = self.agent.handle({"action": "answer", "request_id": "req-12345678", "thread_id": "thread-1", "input_id": "input-1", "answers": {"q": "yes"}, "model_admission": legacy.ADMISSION})
        self.assertEqual(result["state"], "needs_attention")
        self.assertEqual(result["pending_runtime_request"], pending)

    def test_v2_enrolled_launch_preserves_hub_order_and_exact_native_model(self):
        fixture = legacy.TargetLaunchOrderingTests("runTest")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.agent.t3 = self.client
        rpc = self.server.rpc
        def recorded_rpc(*args):
            fixture.order.append(args[3]["type"])
            return rpc(*args)
        with patch("t3_fleet.t3_api.websocket_rpc", recorded_rpc):
            result = fixture.agent.handle(fixture.submit_request)
        self.assertEqual(result["state"], "running")
        self.assertEqual(fixture.order, ["route", "prepare", "thread.create", "thread.metadata.update", "acknowledge", "admit:launch", "message.dispatch"])
        self.assertEqual(self.server.commands[-1]["modelSelection"]["model"], legacy.NATIVE_MODEL)
        self.assertIn(fixture.policy.instructions, self.server.commands[-1]["text"])
        self.assertEqual(len(fixture.policy.ack_calls), 1)

    def test_newer_detail_work_prevents_stale_terminal_success_and_output(self):
        for evidence in ("run", "runtime", "background", "node"):
            with self.subTest(evidence=evidence):
                self.server.shell["threads"] = [thread("completed")]
                self.server.projection["runs"] = [{"id": "run-1", "threadId": "thread-1", "status": "completed"}]
                self.server.projection["runtimeRequests"] = []
                self.server.projection["providerThreads"] = []
                self.server.projection["nodes"] = []
                self.server.projection["messages"] = [{"role": "assistant", "runId": "run-1", "text": "stale result", "streaming": False}]
                if evidence == "run":
                    self.server.projection["runs"].append({"id": "run-2", "threadId": "thread-1", "status": "queued"})
                elif evidence == "runtime":
                    self.server.projection["runtimeRequests"] = [{"id": "approval-2", "kind": "permission", "status": "pending", "createdAt": "2026-10-03T15:01:00Z", "responseCapability": {"type": "live", "providerSessionId": "s"}}]
                elif evidence == "background":
                    self.server.projection["providerThreads"] = [{"id": "pt-1", "appThreadId": "thread-1", "pendingBackgroundTasks": [{"taskId": "bg-1", "kind": "command"}]}]
                else:
                    self.server.projection["nodes"] = [{"id": "n-1", "status": "waiting"}]
                result = self.agent.handle({"action": "status", "thread_id": "thread-1", "include_output": True})
                self.assertNotIn(result["state"], {"succeeded", "failed", "interrupted"})
                self.assertNotIn("output", result)
                if evidence == "runtime":
                    self.assertTrue(result["needs_attention"])
                if evidence == "background":
                    self.assertEqual(result["pending_background_tasks"][0]["taskId"], "bg-1")

    def test_stale_plan_nodes_that_do_not_count_for_the_run_keep_success_and_output(self):
        # T3 0.0.46 leaves a run's todo_list plan node "running" after the run
        # completes and marks it countsForRun=false. Observed on a protocol-2 target:
        # it hid every final report from `status --include-output`.
        self.server.shell["threads"] = [thread("completed")]
        self.server.projection["runs"] = [{"id": "run-1", "threadId": "thread-1", "status": "completed"}]
        self.server.projection["runtimeRequests"] = []
        self.server.projection["providerThreads"] = []
        self.server.projection["nodes"] = [
            {"id": "plan-1", "runId": "run-1", "kind": "todo_list", "status": "running", "countsForRun": False},
            {"id": "tool-1", "runId": "run-1", "kind": "command", "status": "completed", "countsForRun": True},
        ]
        self.server.projection["messages"] = [{"role": "assistant", "runId": "run-1", "text": "final report", "streaming": False}]
        result = self.agent.handle({"action": "status", "thread_id": "thread-1", "include_output": True})
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(result["output"], "final report")
        # A node that counts for its run still holds the request open.
        self.server.projection["nodes"][0]["countsForRun"] = True
        result = self.agent.handle({"action": "status", "thread_id": "thread-1", "include_output": True})
        self.assertNotIn(result["state"], {"succeeded", "failed", "interrupted"})
        self.assertNotIn("output", result)

    def test_permission_and_mcp_approval_kinds_use_decision_contract(self):
        self.fixture._seed_admitted_request()
        for kind in ("permission", "mcp-elicitation"):
            self.server.projection["runtimeRequests"] = [{"id": "approval-1", "kind": kind, "status": "pending", "responseCapability": {"type": "live", "providerSessionId": "s"}}]
            with self.subTest(kind=kind), patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc):
                self.agent.handle({"action": "approve", "request_id": "req-12345678", "thread_id": "thread-1", "approval_id": "approval-1", "decision": "accept", "model_admission": legacy.ADMISSION})
                self.assertEqual(self.server.commands[-1]["type"], "runtime-request.respond")
                self.assertEqual(self.server.commands[-1]["decision"], "accept")
                self.server.projection["runtimeRequests"][0]["responseCapability"] = {"type": "not_resumable", "reason": "expired"}
                with self.assertRaises(ValidationError):
                    self.agent.handle({"action": "approve", "request_id": "req-12345678", "thread_id": "thread-1", "approval_id": "approval-1", "decision": "accept", "model_admission": legacy.ADMISSION})

    def test_malformed_projection_and_pending_state_fail_closed(self):
        mutations = [{"status": []}, {"responseCapability": {"type": []}}, {"responseCapability": {"type": "live"}}, {"kind": "surprise"}]
        for mutation in mutations:
            self.server.projection["runtimeRequests"] = [{"id": "r", "kind": "command", "status": "pending", "responseCapability": {"type": "live", "providerSessionId": "s"}, **mutation}]
            with self.subTest(mutation=mutation):
                with self.assertRaises(T3HttpError):
                    self.client.thread_snapshot("thread-1")
        self.server.shell["threads"][0]["pendingRuntimeRequest"] = {"id": "r", "kind": "surprise", "createdAt": "now"}
        with self.assertRaises(ValidationError):
            self.agent.handle({"action": "capacity"})


class WebSocketRpcTests(unittest.TestCase):
    def test_real_upgrade_masked_request_fragmented_exit_and_ping(self):
        import base64
        import hashlib
        import json
        import socket
        import struct
        import threading
        from t3_fleet import t3_api

        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        self.addCleanup(listener.close)
        observed = []
        errors = []

        def server():
            try:
                conn, _ = listener.accept()
                with conn:
                    conn.settimeout(3)
                    request = b""
                    while not request.endswith(b"\r\n\r\n"):
                        request += conn.recv(1)
                    observed.append(request.decode())
                    key = next(line.split(": ", 1)[1] for line in request.decode().split("\r\n") if line.startswith("Sec-WebSocket-Key:"))
                    accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest())
                    conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + b"\r\n\r\n")
                    def receive():
                        def exact(n):
                            b = b""
                            while len(b) < n:
                                b += conn.recv(n-len(b))
                            return b
                        a, b = exact(2)
                        self.assertTrue(b & 128)
                        n = b & 127
                        if n == 126:
                            n = struct.unpack("!H", exact(2))[0]
                        elif n == 127:
                            n = struct.unpack("!Q", exact(8))[0]
                        mask = exact(4)
                        data = exact(n)
                        return a & 15, bytes(v ^ mask[i % 4] for i, v in enumerate(data))
                    opcode, data = receive()
                    observed.append(json.loads(data))
                    conn.sendall(b"\x89\x02hi")
                    self.assertEqual(receive(), (10, b"hi"))
                    exit_data = json.dumps({"_tag": "Exit", "requestId": "1", "exit": {"_tag": "Success", "value": {"sequence": 7}}}).encode()
                    half = len(exit_data)//2
                    conn.sendall(bytes([1, half]) + exit_data[:half] + bytes([128, len(exit_data)-half]) + exit_data[half:])
            except BaseException as error:
                errors.append(error)
        worker = threading.Thread(target=server, daemon=True)
        worker.start()
        result = t3_api.websocket_rpc(f"http://127.0.0.1:{listener.getsockname()[1]}/", "local-token", "orchestration.dispatchCommand", {"type": "thread.archive", "threadId": "t", "commandId": "c"}, 3)
        worker.join(4)
        if errors:
            raise errors[0]
        self.assertEqual(result, {"sequence": 7})
        self.assertIn("GET /ws?orchestrationProtocol=2 HTTP/1.1", observed[0])
        self.assertIn("Authorization: Bearer local-token", observed[0])
        self.assertEqual(observed[1], {"_tag": "Request", "id": "1", "tag": "orchestration.dispatchCommand", "payload": {"type": "thread.archive", "threadId": "t", "commandId": "c"}, "headers": []})


class TransportBoundsTests(unittest.TestCase):
    def fake_socket(self, data_frames=b"", *, bad_accept=False, timeout=False, comma_connection=False):
        import base64
        import hashlib
        import io
        import socket
        class Connection:
            closed = False
            buffer = io.BytesIO()
            def settimeout(self, timeout):
                pass
            def sendall(self, data):
                if data.startswith(b"GET "):
                    key = next(line.split(b": ")[1] for line in data.split(b"\r\n") if line.startswith(b"Sec-WebSocket-Key:"))
                    accept = base64.b64encode(hashlib.sha1(key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest())
                    if bad_accept:
                        accept = b"invalid"
                    connection_header = b"keep-alive, Upgrade" if comma_connection else b"Upgrade"
                    self.buffer = io.BytesIO(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: " + connection_header + b"\r\nSec-WebSocket-Accept: " + accept + b"\r\n\r\n" + data_frames)
            def recv(self, n):
                if timeout:
                    raise socket.timeout("secret-token")
                return self.buffer.read(n)
            def close(self):
                self.closed = True
        return Connection()

    def test_transport_rejects_failure_bad_ack_and_oversized_frames(self):
        import json
        import struct
        from t3_fleet.t3_api import websocket_rpc
        failures = [b"\x81\x7f" + struct.pack("!Q", 1024*1024+1), b"\x81\x01{", b"\x88\x00"]
        payload = json.dumps({"_tag": "Exit", "requestId": "1", "exit": {"_tag": "Failure", "cause": "secret-token"}}).encode()
        failures.append(bytes([129, len(payload)]) + payload)
        for frames in failures:
            connection = self.fake_socket(frames)
            with self.subTest(frames=frames[:2]), patch("t3_fleet.t3_rpc.socket.create_connection", return_value=connection):
                with self.assertRaises(T3HttpError) as caught:
                    websocket_rpc("http://localhost:3773", "secret-token", "method", {}, 1)
                self.assertNotIn("secret-token", str(caught.exception))
                self.assertTrue(connection.closed)

    def test_upgrade_rejection_and_timeout_close_socket_and_revoke(self):
        for options in ({"bad_accept": True}, {"timeout": True}):
            connection = self.fake_socket(**options)
            server = V2Server()
            runner = FakeRunner()
            client = T3Client("http://localhost:3773", T3SessionIssuer(("t3",), runner=runner), opener=server)
            with self.subTest(options=options), patch("t3_fleet.t3_rpc.socket.create_connection", return_value=connection):
                with self.assertRaises(T3HttpError):
                    client.dispatch({"type": "thread.archive", "threadId": "thread-1", "commandId": "cmd"})
                self.assertTrue(connection.closed)
                self.assertEqual(runner.calls[-1][-2:], ["revoke", "session-1"])

    def test_standard_connection_token_list_is_accepted(self):
        import json
        from t3_fleet.t3_api import websocket_rpc
        data = json.dumps({"_tag": "Exit", "requestId": "1", "exit": {"_tag": "Success", "value": {"sequence": 8}}}).encode()
        connection = self.fake_socket(bytes([129, len(data)]) + data, comma_connection=True)
        with patch("t3_fleet.t3_rpc.socket.create_connection", return_value=connection):
            self.assertEqual(websocket_rpc("http://localhost", "token", "method", {}, 1), {"sequence": 8})

    def test_oversized_request_never_connects(self):
        from t3_fleet.t3_api import websocket_rpc
        with patch("t3_fleet.t3_rpc.socket.create_connection") as connect:
            with self.assertRaises(T3HttpError):
                websocket_rpc("http://localhost", "secret-token", "method", {"text": "x"*(1024*1024)}, 1)
            connect.assert_not_called()



# The shape T3 0.0.46 actually returns, read from a live thread on 2026-10-10.
LIVE_INPUT_ID = (
    "runtime-request:provider:codex:provider-turn:provider-turn%3Aprovider%3Acodex%3A"
    "native-turn%3A01a12344-6485-7411-ba41-da5b1ebe31bc:native-request:async%3A"
    "call_942f2b61d827483a84f0a1ab7bdb9167:a12915a8-08c3-4455-9f41-781c3a4ee966"
)


class PendingRequestPayloadTests(unittest.TestCase):
    """A needs_attention status names the request and what it asks."""

    def setUp(self):
        self.fixture = legacy.AgentTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.agent = self.fixture.agent
        self.server = V2Server()
        self.client = T3Client("http://127.0.0.1:3773", T3SessionIssuer(("t3",), runner=FakeRunner()), opener=self.server)
        self.agent.t3 = self.client
        self.pending = {"id": LIVE_INPUT_ID, "kind": "user_input", "createdAt": "2026-10-10T00:45:38.356Z"}
        self.server.shell["threads"][0].update(status="waiting", pendingRuntimeRequest=dict(self.pending))
        self.server.projection["runtimeRequests"] = [
            {**self.pending, "status": "pending", "responseCapability": {"type": "message"}, "resolvedAt": None}
        ]
        self.server.projection["turnItems"] = [
            {"id": "turn-item:other", "type": "command_execution", "input": "ls"},
            {
                "id": "turn-item:question", "type": "user_input_request", "requestId": LIVE_INPUT_ID,
                "status": "waiting", "responseMode": "message",
                "questions": [{"id": "0", "header": "Question", "question": "Which isolation route?", "options": []}],
            },
        ]

    def test_status_carries_the_question_and_keeps_the_raw_pending_record(self):
        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})
        self.assertEqual(result["state"], "needs_attention")
        self.assertEqual(result["pending_runtime_request"], self.pending)
        self.assertEqual(result["pending_request"], {
            "id": LIVE_INPUT_ID, "kind": "user_input", "created_at": "2026-10-10T00:45:38.356Z",
            "respond_with": "answer",
            "questions": [{"id": "0", "header": "Question", "question": "Which isolation route?", "options": []}],
        })
        bounded = [r.full_url for r in self.server.requests if r.full_url.endswith("/bounded")]
        self.assertEqual(len(bounded), 1)

    def test_include_output_path_also_carries_the_question(self):
        result = self.agent.handle({"action": "status", "thread_id": "thread-1", "include_output": True})
        self.assertEqual(result["state"], "needs_attention")
        self.assertEqual(result["pending_request"]["questions"][0]["question"], "Which isolation route?")

    def test_question_payload_is_bounded_and_unknown_fields_dropped(self):
        item = self.server.projection["turnItems"][1]
        item["questions"] = [
            {"id": str(index), "header": "h" * 500, "question": "q" * 5000, "secret": "drop me",
             "multiSelect": True,
             "options": [{"label": f"o{n}", "description": "d" * 900, "value": "v", "x": 1} for n in range(12)]}
            for index in range(7)
        ]
        payload = self.agent.handle({"action": "status", "thread_id": "thread-1"})["pending_request"]
        self.assertEqual(len(payload["questions"]), 5)
        self.assertTrue(payload["questions_truncated"])
        first = payload["questions"][0]
        self.assertEqual(set(first), {"id", "header", "question", "options", "multi_select"})
        self.assertEqual(len(first["header"]), 200)
        self.assertEqual(len(first["question"]), 2000)
        self.assertEqual(len(first["options"]), 10)
        self.assertEqual(set(first["options"][0]), {"label", "description"})
        self.assertEqual(len(first["options"][0]["description"]), 500)

    def test_approval_request_carries_a_bounded_summary(self):
        approval = {"id": "approval-1", "kind": "command", "createdAt": "2026-10-10T00:00:00Z"}
        self.server.shell["threads"][0]["pendingRuntimeRequest"] = approval
        self.server.projection["turnItems"] = [{
            "id": "turn-item:approval", "type": "approval_request", "requestId": "approval-1",
            "requestKind": "command", "prompt": "rm -rf build " + "x" * 3000, "appName": "codex",
            "options": [{"decision": "accept", "label": "Allow", "warning": "careful"}, {"decision": "decline", "label": "Deny"}],
        }]
        payload = self.agent.handle({"action": "status", "thread_id": "thread-1"})["pending_request"]
        self.assertEqual(payload["respond_with"], "approve")
        self.assertNotIn("questions", payload)
        self.assertEqual(payload["approval"]["request_kind"], "command")
        self.assertEqual(len(payload["approval"]["summary"]), 2000)
        self.assertNotIn("prompt", payload["approval"])
        self.assertEqual(payload["approval"]["app_name"], "codex")
        self.assertEqual(payload["approval"]["options"], [
            {"decision": "accept", "label": "Allow", "warning": "careful"},
            {"decision": "decline", "label": "Deny"},
        ])

    def test_unreadable_detail_still_exposes_the_answerable_id(self):
        server = self.server

        def opener(request, timeout):
            if request.full_url.endswith("/bounded"):
                import urllib.error
                raise urllib.error.URLError("down")
            return server(request, timeout)

        self.client._opener = opener
        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})
        self.assertEqual(result["state"], "needs_attention")
        self.assertEqual(result["pending_request"]["id"], LIVE_INPUT_ID)
        self.assertNotIn("questions", result["pending_request"])

    def test_no_pending_request_means_no_detail_fetch(self):
        self.server.shell["threads"] = [thread("running")]
        self.server.projection["runtimeRequests"] = []
        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})
        self.assertNotIn("pending_request", result)
        self.assertFalse(any(r.full_url.endswith("/bounded") for r in self.server.requests))

    def test_peer_reconcile_passes_the_pending_request_through(self):
        self.fixture._seed_admitted_request()
        result = self.agent.handle({"action": "reconcile", "request_id": "req-12345678"})
        self.assertEqual(result["state"], "needs_attention")
        self.assertEqual(result["pending_request"]["id"], LIVE_INPUT_ID)
        self.assertEqual(result["pending_request"]["questions"][0]["id"], "0")

    def test_exposed_id_round_trips_into_answer(self):
        self.fixture._seed_admitted_request()
        exposed = self.agent.handle({"action": "reconcile", "request_id": "req-12345678"})["pending_request"]["id"]
        self.assertIn(exposed, self.agent._open_user_input_ids(self.client.thread_snapshot("thread-1")["thread"]))
        with patch("t3_fleet.t3_api.websocket_rpc", self.server.rpc):
            self.agent.handle({"action": "answer", "request_id": "req-12345678", "thread_id": "thread-1",
                               "input_id": exposed, "answers": {"0": "bubblewrap"}, "model_admission": legacy.ADMISSION})
        command = self.server.commands[-1]
        self.assertEqual(command["type"], "runtime-request.respond")
        self.assertEqual(command["requestId"], LIVE_INPUT_ID)
        self.assertEqual(command["answers"], {"0": "bubblewrap"})

    def test_legacy_v1_status_exposes_the_open_user_input_request(self):
        self.agent.t3 = self.fixture.fake_t3
        self.fixture.fake_t3.pending_input = True
        self.fixture.fake_t3.thread["activities"] = [
            {"kind": "user-input.requested", "createdAt": "2026-07-28T00:59:00Z",
             "payload": {"requestId": "input-1", "questions": [{"id": "q1", "header": "H", "question": "Proceed?", "options": [{"label": "Yes", "description": "go"}]}]}},
        ]
        result = self.agent.handle({"action": "status", "thread_id": "thread-1"})
        self.assertEqual(result["state"], "needs_attention")
        self.assertEqual(result["pending_request"], {
            "id": "input-1", "kind": "user_input", "created_at": "2026-07-28T00:59:00Z", "respond_with": "answer",
            "questions": [{"id": "q1", "header": "H", "question": "Proceed?", "options": [{"label": "Yes", "description": "go"}]}],
        })


if __name__ == "__main__":
    unittest.main()
