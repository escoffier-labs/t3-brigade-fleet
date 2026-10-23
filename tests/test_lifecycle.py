"""Target-owned event boundary, crash recovery, and observation fencing."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import urllib.error
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from t3_fleet.errors import TargetBusy, T3HttpError, UnknownState, ValidationError
from tests import test_agent as legacy
from tests.test_agent import _claim_request


class ReportingTests(unittest.TestCase):
    def setUp(self):
        cache = patch("t3_fleet.reporting._REPORT_SUPPORTED", None, create=True)
        cache.start()
        self.addCleanup(cache.stop)

    def event(self):
        return {
            "run_id": "t3f:req-12345678",
            "sequence": 1,
            "state": "run.started",
            "ts": "2026-10-17T14:00:00Z",
            "context": {"schema": "brigade.t3_execution_context.v1"},
        }

    def test_invalid_event_is_rejected_but_transport_failures_retry(self):
        from t3_fleet.reporting import report_event

        invalid = subprocess.CompletedProcess([], 2, stdout="", stderr="private detail")
        supported = subprocess.CompletedProcess([], 0, stdout="report help")
        with patch(
            "t3_fleet.reporting._bounded_run", side_effect=[invalid, supported]
        ) as run:
            with self.assertLogs("t3_fleet.reporting", level="WARNING") as logs:
                self.assertEqual(report_event(self.event()), "rejected")
            self.assertEqual(
                logs.output, ["WARNING:t3_fleet.reporting:T3 lifecycle event rejected"]
            )
            self.assertEqual(run.call_count, 2)
        for failure in (
            subprocess.CompletedProcess([], 1, stdout=""),
            subprocess.TimeoutExpired("brigade", 8),
        ):
            with (
                self.subTest(failure=failure),
                patch(
                    "t3_fleet.reporting._bounded_run",
                    side_effect=failure if isinstance(failure, Exception) else None,
                    return_value=failure,
                ) as run,
            ):
                self.assertEqual(report_event(self.event()), "unavailable")
                self.assertEqual(report_event(self.event()), "unavailable")
                self.assertEqual(run.call_count, 2)

    def test_local_invalid_frozen_context_is_rejected_without_helper_io(self):
        from t3_fleet.reporting import report_event

        event = self.event()
        event["context"]["work_id"] = "relative/path"
        with patch("t3_fleet.reporting._bounded_run") as run:
            self.assertEqual(report_event(event), "rejected")
            run.assert_not_called()

    def test_old_helper_is_detected_and_cached(self):
        from t3_fleet.reporting import report_event

        with patch(
            "t3_fleet.reporting._bounded_run",
            return_value=subprocess.CompletedProcess([], 2, stdout=""),
        ) as run:
            self.assertEqual(report_event(self.event()), "unavailable")
            first = run.call_count
            self.assertEqual(first, 2)
            self.assertEqual(report_event(self.event()), "unavailable")
            self.assertEqual(run.call_count, first)

    @unittest.skipIf(
        os.name == "nt", "POSIX executable fixture; Windows argv is tested separately"
    )
    def test_fake_brigade_receives_only_bounded_context_on_stdin(self):
        from t3_fleet.reporting import report_event

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "brigade"
            executable.write_text(
                "#!" + sys.executable + "\n"
                "import json, os, sys\n"
                'data = {"argv": sys.argv[1:], "context": json.load(sys.stdin)}\n'
                'open(os.environ["EVENT_CAPTURE"], "w").write(json.dumps(data))\n'
                "args = sys.argv\n"
                'print(json.dumps({"result": "queued", "run_id": args[args.index("--run-id")+1], "sequence": int(args[args.index("--sequence")+1]), "digest": "a"*64}))\n',
                encoding="utf-8",
            )
            executable.chmod(0o700)
            event = {
                "run_id": "t3f:req-12345678",
                "sequence": 1,
                "state": "run.started",
                "ts": "2026-10-17T14:00:00Z",
                "harness": "t3-fleet",
                "repo": "example-repo",
                "context": {"schema": "brigade.t3_execution_context.v1"},
            }
            with patch.dict(
                os.environ, {"PATH": directory, "EVENT_CAPTURE": str(root / "capture")}
            ):
                self.assertEqual(report_event(event), "queued")
            received = json.loads((root / "capture").read_text())
            self.assertIn("--context-stdin", received["argv"])
            self.assertNotIn("--node-id", received["argv"])
            self.assertNotIn("--exit-status", received["argv"])
            self.assertEqual(received["context"], event["context"])

    def test_missing_old_failed_or_malformed_helper_is_unavailable(self):
        from t3_fleet.reporting import report_event

        event = {
            "run_id": "t3f:req-12345678",
            "sequence": 1,
            "state": "external.completed",
            "ts": "2026-10-17T14:00:00Z",
            "harness": "t3-fleet",
            "context": {"schema": "brigade.t3_execution_context.v1"},
        }
        for reply in (
            FileNotFoundError(),
            subprocess.TimeoutExpired("brigade", 3),
            subprocess.CompletedProcess([], 2, stdout="unknown subcommand"),
            subprocess.CompletedProcess([], 1, stdout='{"result":"queued"}'),
            subprocess.CompletedProcess([], 0, stdout="{}"),
        ):
            with (
                self.subTest(reply=reply),
                patch(
                    "t3_fleet.reporting._bounded_run",
                    side_effect=reply if isinstance(reply, Exception) else None,
                    return_value=reply,
                ),
            ):
                self.assertEqual(report_event(event), "unavailable")

    def test_windows_subprocess_uses_list2cmdline(self):
        from t3_fleet.reporting import report_event

        event = {
            "run_id": "t3f:req-12345678",
            "sequence": 1,
            "state": "run.started",
            "ts": "2026-10-17T14:00:00Z",
            "harness": "t3-fleet",
            "context": {"schema": "brigade.t3_execution_context.v1"},
        }
        with (
            patch("t3_fleet.reporting.os.name", "nt"),
            patch("t3_fleet.reporting._bounded_run") as run,
        ):
            run.return_value = subprocess.CompletedProcess([], 2, stdout="")
            report_event(event)
            self.assertIsInstance(run.call_args_list[0].args[0], str)
            self.assertEqual(
                run.call_args_list[0].args[0],
                subprocess.list2cmdline(
                    [
                        "brigade",
                        "fleet",
                        "events",
                        "report",
                        "--run-id",
                        event["run_id"],
                        "--state",
                        event["state"],
                        "--ts",
                        event["ts"],
                        "--sequence",
                        "1",
                        "--harness",
                        "t3-fleet",
                        "--context-stdin",
                        "--json",
                    ]
                ),
            )


class ContextTests(unittest.TestCase):
    def test_host_names_containing_sk_are_not_credentials(self):
        from t3_fleet.reporting import validate_context

        for prefix in ("desk", "kiosk", "disk"):
            context = {
                "schema": "brigade.t3_execution_context.v1",
                "target_host": prefix + "-example-12345678-1234-1234-1234-123456789abc",
            }
            self.assertEqual(validate_context(context), context)
        with self.assertRaises(ValidationError):
            validate_context(
                {
                    "schema": "brigade.t3_execution_context.v1",
                    "work_id": "sk-" + "a" * 24,
                }
            )

    def test_context_credential_matching_has_hub_case_and_length_boundaries(self):
        from t3_fleet.reporting import validate_context

        for text in ("sk-" + "a" * 16, "SK-" + "a" * 24, "GHP_" + "a" * 24, "AKIA" + "a" * 16):
            context = {"schema": "brigade.t3_execution_context.v1", "work_id": text}
            with self.subTest(text=text):
                self.assertEqual(validate_context(context), context)

    def test_context_matches_hub_text_and_credential_rules(self):
        from t3_fleet.reporting import validate_context

        for text in (
            "relative/path",
            "relative\\path",
            "encoded%20text",
            "op:reference",
            "passwd=",
            "credential:",
            "Basic abc",
            "Authorization: value",
            "Cookie: value",
            "xoxb-" + "a" * 16,
            "ASIA" + "A" * 16,
            "AIza" + "a" * 32,
            "glpat-" + "a" * 20,
            "eyJ" + "a" * 10 + "." + "a" * 12 + "." + "a" * 12,
            "ghp_" + "a" * 16,
        ):
            for field in ("work_id", "initiator", "policy"):
                value = (
                    {"kind": "agent", "label": text}
                    if field == "initiator"
                    else {"session_id": text}
                    if field == "policy"
                    else text
                )
                with (
                    self.subTest(text=text, field=field),
                    self.assertRaises(ValidationError),
                ):
                    validate_context(
                        {"schema": "brigade.t3_execution_context.v1", field: value}
                    )
        with self.assertRaises(ValidationError):
            validate_context(
                {
                    "schema": "brigade.t3_execution_context.v1",
                    "issue_url": "https://github.com/.owner/repo/issues/1",
                }
            )

    def test_context_rejects_unknown_controls_bounds_and_secrets(self):
        from t3_fleet.reporting import validate_context

        for changes in (
            {"prompt": "no"},
            {"work_id": "x" * 129},
            {"work_id": "/tmp/private/file"},
            {"work_id": "C:\\private\\file"},
            {"work_id": "line\nbreak"},
            {"issue_url": "https://example.com/issues/1"},
            {"initiator": {"kind": "root", "label": "hello"}},
            {"policy": {"token": "hidden"}},
            {"work_id": "ghp_" + "a" * 36},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                validate_context(
                    {"schema": "brigade.t3_execution_context.v1", **changes}
                )
        self.assertEqual(
            validate_context({"schema": "brigade.t3_execution_context.v1"}),
            {"schema": "brigade.t3_execution_context.v1"},
        )


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.fixture = legacy.AgentTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.agent = self.fixture.agent
        self.t3 = self.fixture.fake_t3
        self.events = []
        self.reporting = patch("t3_fleet.agent.report_event", side_effect=self.record)
        self.reporting.start()
        self.addCleanup(self.reporting.stop)
        self.request = _claim_request(self.fixture.submit_request, 1)
        self.request["execution_context"] = {
            "schema": "brigade.t3_execution_context.v1",
            "controller_id": "controller-a",
            "origin_node": "origin-a",
            "initiator": {"kind": "harness", "label": "codex"},
            "issue_url": "https://github.com/example/project/issues/12",
            "work_id": "work-12",
        }
        self.result = "delivered"

    def record(self, event):
        self.events.append(event)
        return self.result

    def test_request_lock_contention_is_unknown_and_reads_do_not_lock(self):
        self.agent.handle(self.request)
        with self.agent._request_lock(self.request["request_id"]):
            with patch.object(self.agent, "_monotonic", side_effect=[0, 10]):
                with self.assertRaises(UnknownState):
                    self.agent.handle(self.request)
            with patch.object(
                self.agent, "_request_lock", side_effect=AssertionError("read locked")
            ):
                result = self.agent.handle(
                    dict(self.request, action="lookup", thread_id="thread-1")
                )
                self.assertEqual(result["state"], "running")
            # status and recover only try the lock; contention reports the
            # stored state instead of raising or settling.
            for action in ("status", "recover"):
                result = self.agent.handle(
                    dict(self.request, action=action, thread_id="thread-1")
                )
                self.assertEqual(result["state"], "running")
        self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_status_during_in_flight_launch_cannot_fail_it_or_free_capacity(self):
        self.agent.handle(self.request)
        # T3 reports a terminal turn while the dispatcher still holds the lock.
        self.t3.thread["latestTurn"] = {"state": "error", "turnId": "run-1"}
        self.t3.thread["session"] = {"status": "ready"}
        with self.agent._request_lock(self.request["request_id"]):
            for action in ("status", "recover"):
                result = self.agent.handle(
                    dict(self.request, action=action, thread_id="thread-1")
                )
                self.assertEqual(result["state"], "running")
        store = self.agent._target_store()
        self.assertEqual(store.lookup(self.request["request_id"]).state, "running")
        self.assertEqual(store.active_count(), 1)

    def test_status_keeps_old_turn_unresolved_after_followup_crashed(self):
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        store = self.agent._target_store()
        # A crash after begin_turn leaves launching set and dispatch unfinished.
        store.begin_turn(self.request["request_id"], previous_turn_ref="old-run")
        result = self.agent.handle(
            dict(self.request, action="status", thread_id="thread-1")
        )
        self.assertEqual(result["state"], "unknown")
        self.assertEqual(store.active_count(), 1)
        self.agent._now = lambda: "2026-07-28T01:01:00Z"
        result = self.agent.handle(
            dict(self.request, action="status", thread_id="thread-1")
        )
        self.assertEqual(result["state"], "unknown")
        self.assertEqual(store.active_count(), 1)

    def test_reporting_and_settlement_do_not_hold_request_lock(self):
        def outside(event):
            with self.agent._request_lock(self.request["request_id"], timeout=0):
                self.events.append(event)
            return "delivered"

        with patch("t3_fleet.agent.report_event", side_effect=outside):
            self.agent.handle(self.request)
        self.assertEqual(len(self.events), 1)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "run-1"}
        self.t3.thread["session"] = {"status": "ready"}
        settled = []

        def settle(receipt, state):
            with self.agent._request_lock(receipt.request_id, timeout=0):
                settled.append(state)

        with patch.object(self.agent, "_settle_reservation", side_effect=settle):
            result = self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(settled, ["succeeded"])
        self.assertEqual(result["results"][0]["state"], "succeeded")

    def test_snapshot_failure_skips_pass_without_changing_receipts(self):
        self.agent.handle(self.request)
        before = self.agent._target_store().lookup(self.request["request_id"])
        with patch.object(
            self.t3, "shell_snapshot", side_effect=RuntimeError("snapshot failed")
        ):
            result = self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(result["examined"], 0)
        self.assertEqual(self.agent._target_store().lookup(before.request_id), before)
        self.assertEqual(len(self.events), 1)

    def test_rejected_event_advances_sequence_without_retry(self):
        self.result = "rejected"
        self.agent.handle(self.request)
        receipt = self.agent._target_store().lookup(self.request["request_id"])
        self.assertEqual(receipt.lifecycle.get("rejected_sequence"), 1)
        self.result = "delivered"
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "new-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual([e["sequence"] for e in self.events], [1, 2])
        self.assertEqual(self.events[-1]["state"], "external.completed")

    def test_direct_submit_does_not_apply_claim_capacity_limit(self):
        from dataclasses import replace

        self.agent.policy = replace(self.agent.policy, active_limit=1)
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "completed"}
        self.t3.thread["session"] = {"status": "ready"}
        self.assertEqual(self.agent.handle({"action": "doctor"})["state"], "eligible")
        result = self.agent.handle(
            dict(self.fixture.submit_request, request_id="req-87654321")
        )
        self.assertEqual(result["state"], "running")
        with self.assertRaises(TargetBusy):
            self.agent.handle(_claim_request(self.fixture.submit_request, 3))

    def test_direct_submit_retry_digest_ignores_refreshed_policy_metadata(self):
        first = dict(self.fixture.submit_request, policy={"version": 1})
        self.agent.handle(first)
        result = self.agent.handle(dict(first, policy={"version": 2}))
        self.assertEqual(result["thread_id"], "thread-1")
        self.assertEqual(
            sum(c["type"] == "thread.create" for c in self.t3.dispatched), 1
        )

    def test_submit_store_error_does_not_replace_original_dispatch_error(self):
        original = RuntimeError("original dispatch error")
        store = self.agent._target_store()
        with (
            patch.object(self.agent, "_create_worktree", side_effect=original),
            patch.object(store, "reconcile", side_effect=OSError("journal failed")),
        ):
            with self.assertRaises(RuntimeError) as raised:
                self.agent.handle(self.fixture.submit_request)
        self.assertIs(raised.exception, original)

    def test_definitive_followup_rejection_restores_prior_terminal(self):
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        error = T3HttpError("T3 rejected command")
        error.__cause__ = urllib.error.HTTPError(
            "https://example.invalid", 400, "rejected", {}, None
        )
        with (
            patch.object(self.t3, "dispatch", side_effect=error),
            self.assertRaises(T3HttpError),
        ):
            self.agent.handle(
                dict(
                    self.request,
                    action="followup",
                    thread_id="thread-1",
                    prompt="continue",
                )
            )
        self.assertEqual(
            self.agent._target_store().lookup(self.request["request_id"]).state,
            "succeeded",
        )
        self.assertEqual(self.agent._target_store().active_count(), 0)

    def test_ambiguous_followup_failure_cannot_succeed_from_old_turn(self):
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        with (
            patch.object(
                self.t3, "dispatch", side_effect=RuntimeError("lost response")
            ),
            self.assertRaises(RuntimeError),
        ):
            self.agent.handle(
                dict(
                    self.request,
                    action="followup",
                    thread_id="thread-1",
                    prompt="continue",
                )
            )
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.agent._target_store().active_count(), 1)
        self.agent._now = lambda: "2026-07-28T01:01:00Z"
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(
            self.agent._target_store().lookup(self.request["request_id"]).state,
            "unknown",
        )
        self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_queued_followup_grace_starts_when_previous_run_finishes(self):
        for number, (action, new_identity, previous_id) in enumerate(
            ((action, new, previous) for action in ("observe", "reconcile", "status")
             for new in (False, True) for previous in ("old-run", None)), start=1
        ):
            with self.subTest(action=action, new_identity=new_identity, previous_id=previous_id):
                fixture = legacy.AgentTests("runTest")
                fixture.setUp()
                self.addCleanup(fixture.doCleanups)
                self.agent = fixture.agent
                self.t3 = fixture.fake_t3
                request = _claim_request(fixture.submit_request, number)
                self.agent.handle(request)
                self.t3.thread["latestTurn"] = {"state": "running", "turnId": previous_id}
                self.t3.thread["session"] = {"status": "running"}
                self.agent.handle({"action": "observe", "max": 1})
                with (
                    patch.object(self.t3, "dispatch", side_effect=RuntimeError("lost response")),
                    self.assertRaises(RuntimeError),
                ):
                    self.agent.handle(dict(
                        request, action="followup", thread_id="thread-1", prompt="continue"
                    ))
                poll = dict(request, action=action, thread_id="thread-1", max=1)
                self.agent._now = lambda: "2026-07-28T01:01:00Z"
                self.agent.handle(poll)
                self.assertEqual(self.agent._target_store().active_count(), 1)
                self.t3.thread["latestTurn"] = {"state": "completed", "turnId": previous_id}
                self.t3.thread["session"] = {"status": "ready"}
                self.agent.handle(poll)
                receipt = self.agent._target_store().lookup(request["request_id"])
                self.assertEqual(receipt.state, "unknown")
                self.assertEqual(self.agent._target_store().active_count(), 1)
                # Reload the persisted deadline and ensure another poll cannot extend it.
                self.agent._store = None
                self.agent._now = lambda: "2026-07-28T01:01:29Z"
                self.agent.handle(poll)
                self.assertEqual(self.agent._target_store().active_count(), 1)
                if new_identity:
                    self.t3.thread["latestTurn"] = {"state": "running", "turnId": "new-run"}
                    self.t3.thread["session"] = {"status": "running"}
                    self.agent.handle(poll)
                    self.agent._now = lambda: "2026-07-28T01:02:00Z"
                    self.agent.handle(poll)
                    self.assertEqual(self.agent._target_store().active_count(), 1)
                    self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "new-run"}
                    self.t3.thread["session"] = {"status": "ready"}
                else:
                    self.agent._now = lambda: "2026-07-28T01:01:30Z"
                self.agent.handle(poll)
                receipt = self.agent._target_store().lookup(request["request_id"])
                # Previous or absent identity never proves this followup ran.
                self.assertEqual(receipt.state, "succeeded" if new_identity else "unknown")
                self.assertEqual(self.agent._target_store().active_count(), 0 if new_identity else 1)
                if previous_id and not new_identity:
                    self.assertFalse(receipt.lifecycle["waiting_for_previous_turn"])
                    self.assertEqual(receipt.lifecycle["launch_deadline"], "2026-07-28T01:01:30Z")
                elif not previous_id and not new_identity:
                    self.assertFalse(receipt.lifecycle["waiting_for_previous_turn"])
                    self.assertEqual(receipt.lifecycle["launch_deadline"], "2026-07-28T01:01:30Z")

    def test_stale_observation_cannot_start_new_epoch_launch_grace(self):
        self.agent.handle(self.request)
        store = self.agent._target_store()
        old = store.begin_turn(self.request["request_id"], previous_turn_ref="old-run")
        current = store.begin_turn(self.request["request_id"], previous_turn_ref="next-run")
        updated = store.reconcile(
            self.request["request_id"], state="unknown", thread_id="thread-1",
            expected_version=old.observation_version,
            launch_grace_started_at="2026-07-28T01:01:00Z",
        )
        self.assertFalse(updated.observation_applied)
        self.assertEqual(store.lookup(self.request["request_id"]), current)

    def test_new_live_identity_ends_ambiguous_launch_grace(self):
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        with (
            patch.object(
                self.t3, "dispatch", side_effect=RuntimeError("lost response")
            ),
            self.assertRaises(RuntimeError),
        ):
            self.agent.handle(
                dict(
                    self.request,
                    action="followup",
                    thread_id="thread-1",
                    prompt="continue",
                )
            )
        # A pending event must not delay recording evidence of the new identity.
        self.result = "unavailable"
        self.agent.handle({"action": "observe", "max": 1})
        self.t3.thread["latestTurn"] = {"state": "running", "turnId": "new-run"}
        self.t3.thread["session"] = {"status": "running"}
        self.agent.handle({"action": "observe", "max": 1})
        self.agent._now = lambda: "2026-07-28T01:01:00Z"
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.agent._target_store().active_count(), 1)
        self.assertEqual(
            self.agent._target_store().lookup(self.request["request_id"]).state,
            "unknown",
        )

    def test_inflight_followup_cannot_expire_grace_from_concurrent_reads(self):
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        def dispatch(command):
            self.agent._now = lambda: "2026-07-28T01:01:00Z"
            for action in ("status", "reconcile", "recover"):
                result = self.agent.handle(dict(self.request, action=action, thread_id="thread-1"))
                self.assertNotIn(result["state"], {"succeeded", "failed", "interrupted"})
                self.assertEqual(self.agent._target_store().active_count(), 1)
            return {"sequence": 9}
        with patch.object(self.t3, "dispatch", side_effect=dispatch):
            self.agent.handle(dict(self.request, action="followup", thread_id="thread-1", prompt="continue"))
        self.assertEqual(self.agent._target_store().lookup(self.request["request_id"]).state, "running")
        self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_confirmed_followup_fences_old_interrupt_with_fresh_thread_timestamp(self):
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "interrupted", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        self.agent.handle(dict(self.request, action="followup", thread_id="thread-1", prompt="continue"))
        self.t3.thread["updatedAt"] = "2026-07-28T01:00:01Z"
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.agent._target_store().lookup(self.request["request_id"]).state, "unknown")
        self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_confirmed_no_id_followup_keeps_timestamp_fence_after_grace(self):
        self.agent.handle(self.request)
        self.agent.handle(dict(self.request, action="followup", thread_id="thread-1", prompt="continue"))
        self.t3.thread["latestTurn"] = {"state": "completed"}
        self.t3.thread["session"] = {"status": "ready"}
        self.t3.thread["updatedAt"] = "2026-07-27T00:00:00Z"
        self.agent._now = lambda: "2026-07-28T01:01:00Z"
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.agent._target_store().lookup(self.request["request_id"]).state, "unknown")
        self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_uncertain_rpc_failure_does_not_restore_terminal_state(self):
        from tests.test_protocol_v2 import TransportBoundsTests
        from t3_fleet.t3_api import websocket_rpc

        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        payload = json.dumps({"_tag": "Exit", "requestId": "1", "exit": {"_tag": "Failure", "cause": {"_tag": "Die"}}}).encode()
        connection = TransportBoundsTests().fake_socket(bytes([129, len(payload)]) + payload)
        with patch("t3_fleet.t3_rpc.socket.create_connection", return_value=connection):
            with self.assertRaises(T3HttpError) as caught:
                websocket_rpc("http://localhost", "test-token", "method", {}, 1)
        with patch.object(self.t3, "dispatch", side_effect=caught.exception), self.assertRaises(T3HttpError):
            self.agent.handle(dict(self.request, action="followup", thread_id="thread-1", prompt="continue"))
        self.assertEqual(self.agent._target_store().lookup(self.request["request_id"]).state, "unknown")
        self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_observed_identity_stays_fenced_when_followup_detail_is_stale(self):
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "running", "turnId": "current-run"}
        self.t3.thread["session"] = {"status": "running"}
        self.agent.handle({"action": "observe", "max": 1})
        store = self.agent._target_store()
        # T3 detail may lag the identity already recorded by observation.
        store.begin_turn(self.request["request_id"], previous_turn_ref="stale-run")
        store.confirm_turn(self.request["request_id"])
        self.agent.handle({"action": "observe", "max": 1})
        self.assertNotEqual(self.events[-1]["context"]["turn_ref"], "current-run")
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "current-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(store.lookup(self.request["request_id"]).state, "unknown")
        self.assertEqual(store.active_count(), 1)

    def test_new_turn_identity_settles_despite_clock_skew(self):
        self.agent.handle(self.request)
        self.agent.handle(
            dict(
                self.request, action="followup", thread_id="thread-1", prompt="continue"
            )
        )
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "new-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.t3.thread["updatedAt"] = "2026-07-27T00:00:00Z"
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.events[-1]["state"], "external.completed")
        self.assertEqual(self.agent._target_store().active_count(), 0)

    def test_explicit_interruption_does_not_attribute_old_turn_to_new_epoch(self):
        self.agent.handle(self.request)
        store = self.agent._target_store()
        receipt = store.begin_turn(self.request["request_id"], previous_turn_ref="old-run")
        self.t3.thread["latestTurn"] = {"state": "interrupted", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.t3.thread["updatedAt"] = "2026-07-28T01:00:01Z"
        # Interrupt intent does not attribute the previous turn to the current epoch.
        store.mark_interrupt_requested(receipt.thread_id, "2026-07-28T01:00:01Z")
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(store.lookup(self.request["request_id"]).state, "unknown")
        self.assertEqual(store.active_count(), 1)

    def test_old_interrupt_with_fresh_thread_timestamp_keeps_capacity_in_grace(self):
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "interrupted", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})

        def lost_ack(command):
            # A valid shell can retain the old turn while thread metadata moves.
            self.t3.thread["updatedAt"] = "2026-07-28T01:00:01Z"
            raise RuntimeError("lost dispatch acknowledgement")

        with patch.object(self.t3, "dispatch", side_effect=lost_ack):
            with self.assertRaises(RuntimeError):
                self.agent.handle(dict(self.request, action="followup", thread_id="thread-1", prompt="continue"))
        store = self.agent._target_store()
        self.assertEqual(store.lookup(self.request["request_id"]).state, "unknown")
        self.assertEqual(store.active_count(), 1)
        # Still before the persisted launch deadline: no interrupt from this
        # target, so a fresh timestamp alone must not free capacity.
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(
            (store.lookup(self.request["request_id"]).state, store.active_count()),
            ("unknown", 1),
        )

    def test_exact_replay_one_thread_one_run_and_context_on_every_event(self):
        first = self.agent.handle(self.request)
        self.agent.handle(self.request)
        self.assertEqual(
            sum(c["type"] == "thread.create" for c in self.t3.dispatched), 1
        )
        self.assertEqual([e["state"] for e in self.events], ["run.started"])
        self.assertEqual(self.events[0]["run_id"], "t3f:" + self.request["request_id"])
        self.t3.pending_input = True
        self.agent.handle({"action": "observe", "max": 2})
        self.assertEqual(self.events[-1]["state"], "run.paused")
        self.assertEqual(self.events[-1]["context"]["thread_id"], first["thread_id"])
        self.assertEqual(self.events[-1]["context"]["work_id"], "work-12")
        self.assertNotIn("private prompt", json.dumps(self.events))

    def test_restart_retries_terminal_with_identical_digest_inputs(self):
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "run-1"}
        self.t3.thread["session"] = {"status": "ready"}
        self.result = "unavailable"
        self.agent.handle({"action": "observe", "max": 1})
        terminal = self.events[-1]
        self.assertEqual(terminal["state"], "external.completed")
        self.assertNotIn("exit_status", terminal)
        self.agent._store = None  # restart: reconstruct the store from disk
        self.result = "queued"
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.events[-1], terminal)
        count = len(self.events)
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(len(self.events), count)

    def test_unknown_does_not_free_capacity_and_failure_does(self):
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = None
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 2})
        self.assertEqual(self.events[-1]["state"], "external.unknown")
        self.assertEqual(self.agent._target_store().active_count(), 1)
        self.t3.thread["latestTurn"] = {"state": "error", "turnId": "run-1"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 2})
        self.assertEqual(self.events[-1]["state"], "external.failed")
        self.assertEqual(self.agent._target_store().active_count(), 0)

    def test_snapshot_is_once_and_one_failure_does_not_abort_batch(self):
        self.agent.handle(self.request)
        with patch.object(
            self.t3, "shell_snapshot", wraps=self.t3.shell_snapshot
        ) as snapshot:
            observed = self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(snapshot.call_count, 1)
        self.assertEqual(observed["examined"], 1)
        for limit in (0, 101, True):
            with self.subTest(limit=limit), self.assertRaises(ValidationError):
                self.agent.handle({"action": "observe", "max": limit})

    def test_stale_observer_cannot_complete_new_followup_epoch(self):
        self.agent.handle(self.request)
        store = self.agent._target_store()
        stale = store.lookup(self.request["request_id"])
        store.reconcile(stale.request_id, state="succeeded", thread_id=stale.thread_id)
        store.reserve_followup(stale.request_id, active_limit=2, reservation_ttl=60)
        newer = store.lookup(stale.request_id)
        store.reconcile(
            stale.request_id,
            state="succeeded",
            thread_id=stale.thread_id,
            expected_version=stale.observation_version,
        )
        self.assertEqual(store.lookup(stale.request_id).state, "dispatching")
        self.assertGreater(newer.observation_version, stale.observation_version)

    def test_reserved_followup_cannot_finish_from_old_snapshot(self):
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        self.agent.handle(
            dict(self.request, action="reserve_followup", thread_id="thread-1")
        )
        for _ in range(2):
            self.agent.handle({"action": "observe", "max": 1})
            self.assertEqual(self.agent._target_store().active_count(), 1)
            self.assertNotEqual(self.events[-1]["state"], "external.completed")

    def test_abandoned_followup_reservation_expires_without_starting_a_turn(self):
        self.agent.handle(self.request)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        store = self.agent._target_store()
        clock = [1000.0]
        store._clock = lambda: clock[0]
        store.reserve_followup(self.request["request_id"], active_limit=2, reservation_ttl=60)
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(store.active_count(), 1)
        clock[0] += 61
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(store.lookup(self.request["request_id"]).state, "succeeded")
        self.assertEqual(store.active_count(), 0)
        self.assertEqual(self.events[-1]["state"], "external.completed")

    def test_first_followup_of_pre_lifecycle_receipt_fences_no_id_terminal(self):
        self.agent.handle(self.request)
        store = self.agent._target_store()
        store.reconcile(self.request["request_id"], state="succeeded", thread_id="thread-1")
        # Simulate a receipt written before lifecycle bookkeeping existed.
        with store._connect() as connection:
            connection.execute("UPDATE target_requests SET lifecycle_json = NULL")
        store.reserve_followup(self.request["request_id"], active_limit=2, reservation_ttl=60)
        store.begin_turn(self.request["request_id"])
        self.t3.thread["latestTurn"] = {"state": "completed"}
        self.t3.thread["session"] = {"status": "ready"}
        self.t3.thread["updatedAt"] = "2026-07-27T00:00:00Z"
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(store.lookup(self.request["request_id"]).state, "unknown")
        self.assertEqual(store.active_count(), 1)
        self.agent._now = lambda: "2026-07-28T01:01:00Z"
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(store.lookup(self.request["request_id"]).state, "unknown")
        self.assertEqual(store.active_count(), 1)

    def test_dispatch_response_loss_eventually_accepts_new_turn_completion(self):
        self.t3.fail_after = 2
        with self.assertRaises(RuntimeError):
            self.agent.handle(self.request)
        self.t3.fail_after = None
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "new-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.events[-1]["state"], "external.completed")

    def test_busy_legacy_submit_leaves_no_orphan_receipt(self):
        with (
            patch.object(
                self.agent,
                "_create_worktree",
                side_effect=TargetBusy("busy", reason="repository-busy"),
            ),
            self.assertRaises(TargetBusy),
        ):
            self.agent.handle(self.fixture.submit_request)
        self.assertIsNone(self.agent._target_store().lookup("req-12345678"))
        self.assertEqual(
            self.agent.handle({"action": "observe", "max": 1})["examined"], 0
        )

    def test_bad_turn_metadata_does_not_block_other_reports(self):
        self.agent.handle(self.request)
        second = _claim_request(self.fixture.submit_request, 2)
        self.agent.handle(second)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "valid-run"}
        self.t3.thread["session"] = {"status": "ready"}
        original = self.agent._status

        def bad_one(thread_id, **kwargs):
            result = original(thread_id, **kwargs)
            if thread_id == "thread-1":
                result["latest_run_id"] = "x" * 129
            return result

        with patch.object(self.agent, "_status", side_effect=bad_one):
            result = self.agent.handle({"action": "observe", "max": 2})
        self.assertEqual(result["examined"], 2)
        self.assertTrue(
            any(
                e["run_id"] == "t3f:" + second["request_id"]
                and e["state"] == "external.unknown"
                for e in self.events
            )
        )

    def test_heartbeat_is_due_at_300_seconds(self):
        self.agent.handle(self.request)
        self.agent._now = lambda: "2026-07-28T01:04:59Z"
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(len(self.events), 1)
        self.agent._now = lambda: "2026-07-28T01:05:00Z"
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.events[-1]["state"], "run.dispatch.observed")
        self.assertEqual(self.events[-1]["sequence"], 2)

    def test_followup_from_terminal_legacy_receipt_advances_epoch(self):
        request = dict(
            self.fixture.submit_request,
            execution_context=self.request["execution_context"],
        )
        self.agent.handle(request)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "old-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle(
            {
                "action": "status",
                "request_id": request["request_id"],
                "thread_id": "thread-1",
            }
        )
        first_ref = self.events[-1]["context"]["turn_ref"]
        self.agent.handle(
            dict(request, action="followup", thread_id="thread-1", prompt="continue")
        )
        self.assertEqual(self.events[-1]["state"], "run.started")
        self.assertNotEqual(self.events[-1]["context"]["turn_ref"], first_ref)
        # The previous terminal remains visible until T3 projects the new run.
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.events[-1]["state"], "external.unknown")
        self.assertEqual(self.agent._target_store().active_count(), 1)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "new-run"}
        self.t3.thread["session"] = {"status": "ready"}
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.events[-1]["state"], "external.completed")
        self.assertEqual(self.events[-1]["context"]["turn_ref"], "new-run")

    def test_legacy_no_turn_interrupt_remains_unresolved_after_followup(self):
        self.agent.handle(self.request)
        self.agent._now = lambda: "2026-07-28T02:00:00Z"
        self.agent.handle(
            dict(self.request, action="followup", thread_id="thread-1", prompt="continue")
        )
        self.t3.thread["session"] = {"status": "stopped"}
        self.t3.thread["latestTurn"] = {"state": "interrupted"}
        self.t3.thread["session"] = {"status": "ready"}
        self.t3.thread["updatedAt"] = "2026-07-27T00:00:00Z"
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.events[-1]["state"], "external.unknown")
        self.assertEqual(self.agent._target_store().active_count(), 1)

    def test_fenced_terminal_keeps_current_epoch_identity_on_every_status_path(self):
        for action in ("observe", "reconcile", "status"):
            with self.subTest(action=action):
                store = self.agent._target_store()
                if store.lookup(self.request["request_id"]) is None:
                    self.agent.handle(self.request)
                # Remember the older identity before advancing again. A distinct
                # new turn can have a skewed clock, but known history stays fenced.
                store.begin_turn(self.request["request_id"], previous_turn_ref="older-turn")
                store.confirm_turn(self.request["request_id"])
                store.begin_turn(self.request["request_id"], previous_turn_ref="previous")
                store.confirm_turn(self.request["request_id"])
                self.t3.thread["latestTurn"] = {
                    "state": "completed", "turnId": "older-turn"
                }
                self.t3.thread["session"] = {"status": "ready"}
                self.t3.thread["updatedAt"] = "2026-07-28T01:00:00Z"
                self.agent.handle({
                    "action": action, "max": 1,
                    "request_id": self.request["request_id"], "thread_id": "thread-1",
                })
                receipt = store.lookup(self.request["request_id"])
                self.assertEqual(receipt.state, "unknown")
                self.assertEqual(store.active_count(), 1)
                self.assertEqual(
                    self.events[-1]["context"]["turn_ref"],
                    f"epoch:{receipt.lifecycle['epoch']}",
                )

    def test_persisted_terminal_is_reported_if_restart_precedes_event_creation(self):
        self.agent.handle(self.request)
        store = self.agent._target_store()
        store.reconcile(
            self.request["request_id"], state="interrupted", thread_id="thread-1"
        )
        # Crash between durable terminal state and lifecycle event creation.
        self.agent._store = None
        self.agent._now = lambda: "2026-07-28T02:00:00Z"
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.events[-1]["state"], "external.canceled")
        self.assertEqual(self.events[-1]["ts"], "2026-07-28T01:00:00Z")

    def test_stale_status_cannot_label_new_epoch_with_previous_run(self):
        self.agent.handle(self.request)
        store = self.agent._target_store()
        original = self.t3.shell_snapshot

        def racing_snapshot():
            snapshot = original()
            snapshot["threads"][0]["latestTurn"] = {
                "state": "completed",
                "turnId": "old-run",
            }
            store.reconcile(
                self.request["request_id"], state="succeeded", thread_id="thread-1"
            )
            store.reserve_followup(
                self.request["request_id"], active_limit=2, reservation_ttl=60
            )
            store.begin_turn(self.request["request_id"], previous_turn_ref="old-run")
            return snapshot

        with patch.object(self.t3, "shell_snapshot", side_effect=racing_snapshot):
            self.agent.handle(
                {"action": "reconcile", "request_id": self.request["request_id"]}
            )
        self.assertEqual(store.lookup(self.request["request_id"]).state, "dispatching")
        self.assertNotEqual(self.events[-1]["context"]["turn_ref"], "old-run")

    def test_one_reporting_failure_does_not_abort_other_receipts(self):
        self.agent.handle(self.request)
        second = _claim_request(self.fixture.submit_request, 2)
        self.agent.handle(second)
        self.t3.thread["latestTurn"] = {"state": "completed", "turnId": "finished"}
        self.t3.thread["session"] = {"status": "ready"}
        self.result = "unavailable"
        batch = self.agent.handle({"action": "observe", "max": 2})
        self.assertEqual(batch["examined"], 2)
        self.assertEqual(
            {r["request_id"] for r in batch["results"]},
            {self.request["request_id"], second["request_id"]},
        )

    def test_prelaunch_failure_is_durable_and_reported(self):
        self.fixture.git_fail_after = 0
        with self.assertRaises(UnknownState):
            self.agent.handle(self.request)
        self.assertEqual(self.events[-1]["state"], "external.failed")
        self.assertNotIn("exit_status", self.events[-1])


class V2LifecycleTests(unittest.TestCase):
    def setUp(self):
        from tests import test_protocol_v2 as protocol

        self.fixture = protocol.ProtocolV2Tests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.agent = self.fixture.agent
        self.server = self.fixture.server
        self.fixture.fixture._seed_admitted_request()
        self.events = []
        reporter = patch(
            "t3_fleet.agent.report_event",
            side_effect=lambda event: self.events.append(event) or "delivered",
        )
        reporter.start()
        self.addCleanup(reporter.stop)

    def test_background_runtime_attention_and_terminal_mappings(self):
        t = self.server.shell["threads"][0]
        t.update(
            status="completed",
            activeRunId=None,
            pendingBackgroundTasks=[{"taskId": "task-1"}],
        )
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.events[-1]["state"], "run.dispatch.observed")
        t["pendingRuntimeRequest"] = {
            "id": "request-1",
            "kind": "user_input",
            "createdAt": "2026-10-03T15:00:00Z",
        }
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.events[-1]["state"], "run.paused")
        t.update(
            pendingBackgroundTasks=[], pendingRuntimeRequest=None, status="cancelled"
        )
        self.agent.handle({"action": "observe", "max": 1})
        self.assertEqual(self.events[-1]["state"], "external.canceled")
        self.assertEqual(self.agent._target_store().active_count(), 0)


class UpgradeTests(unittest.TestCase):
    def test_null_context_exact_replay_and_old_terminal_history(self):
        from t3_fleet.target_store import TargetStore
        from tests.test_target_store import TargetStoreTests

        with tempfile.TemporaryDirectory() as directory:
            store = TargetStore(Path(directory) / "target.db")
            claim = TargetStoreTests("runTest").claim("req-12345678")
            store.claim(claim, active_limit=2, reservation_ttl=30)
            from dataclasses import replace

            store.claim(
                replace(
                    claim,
                    execution_context={"schema": "brigade.t3_execution_context.v1"},
                ),
                active_limit=2,
                reservation_ttl=30,
            )
            store.reconcile(claim.request_id, state="succeeded", thread_id=None)
            self.assertEqual(
                store.observation_batch(target_id="linux-worker", maximum=2), []
            )

    def test_context_upgrade_replay_merges_without_rewriting_pending_event(self):
        from dataclasses import replace

        from t3_fleet.errors import IdempotencyConflict
        from t3_fleet.target_store import TargetStore
        from tests.test_target_store import TargetStoreTests

        with tempfile.TemporaryDirectory() as directory:
            store = TargetStore(Path(directory) / "target.db")
            schema_only = {"schema": "brigade.t3_execution_context.v1"}
            claim = replace(
                TargetStoreTests("runTest").claim("req-12345678"),
                execution_context=schema_only,
            )
            receipt = store.claim(claim, active_limit=2, reservation_ttl=30)
            original = store.lifecycle_event(
                claim.request_id, expected_version=receipt.observation_version,
                state=receipt.state, observed_at="2026-07-28T01:00:00Z",
            )
            enriched = {**schema_only, "controller_id": "controller-a", "work_id": "work-a"}
            receipt = store.claim(
                replace(claim, execution_context=enriched),
                active_limit=2, reservation_ttl=30,
            )
            self.assertEqual(receipt.execution_context, enriched)
            # Older controllers may replay after the upgrade, too.
            for context in (schema_only, None, enriched):
                receipt = store.claim(
                    replace(claim, execution_context=context),
                    active_limit=2, reservation_ttl=30,
                )
                self.assertEqual(receipt.execution_context, enriched)
            replay = store.lifecycle_event(
                claim.request_id, expected_version=receipt.observation_version,
                state=receipt.state, observed_at="2026-07-28T02:00:00Z",
            )
            self.assertEqual(replay, original)
            store.acknowledge_event(claim.request_id, original["sequence"])
            event = store.lifecycle_event(
                claim.request_id, expected_version=receipt.observation_version,
                state=receipt.state, observed_at="2026-07-28T02:00:00Z",
            )
            self.assertEqual(event["sequence"], original["sequence"] + 1)
            self.assertEqual(event["context"]["controller_id"], "controller-a")
            with self.assertRaises(IdempotencyConflict):
                store.claim(
                    replace(claim, execution_context={
                        **enriched, "work_id": "work-b",
                    }), active_limit=2, reservation_ttl=30,
                )

    def test_replay_keeps_original_derived_controller_identity(self):
        from dataclasses import replace

        from t3_fleet.target_store import TargetStore
        from tests.test_target_store import TargetStoreTests

        with tempfile.TemporaryDirectory() as directory:
            store = TargetStore(Path(directory) / "target.db")
            context = {
                "schema": "brigade.t3_execution_context.v1",
                "controller_id": "controller-a", "origin_node": "origin-a",
            }
            claim = replace(
                TargetStoreTests("runTest").claim("req-12345678"),
                execution_context=context,
            )
            store.claim(claim, active_limit=2, reservation_ttl=30)
            replay = store.claim(replace(claim, execution_context={
                **context, "controller_id": "controller-b", "origin_node": "origin-b",
            }), active_limit=2, reservation_ttl=30)
            self.assertEqual(replay.execution_context, context)

    def test_derived_context_does_not_change_caller_digest(self):
        from t3_fleet.controller import FleetController

        request = {"host": "linux-worker", "seat": "coder", "title": "bounded work"}
        before = FleetController._digest(
            FleetController._caller_payload(request, None), "prompt"
        )
        request["execution_context"] = {
            "schema": "brigade.t3_execution_context.v1",
            "controller_id": "controller-a",
        }
        self.assertEqual(
            FleetController._digest(
                FleetController._caller_payload(request, None), "prompt"
            ),
            before,
        )


class SubmitContextTests(unittest.TestCase):
    def test_cli_validates_context_and_carries_flags(self):
        from tests import test_cli as cli_tests

        fixture = cli_tests.CliTests("runTest")
        application = Mock()
        application.controller.submit.return_value = {"state": "running"}
        argv = [
            "submit",
            "--host",
            "linux-worker",
            "--repository",
            "example-repo",
            "--source-revision",
            legacy.FULL_SHA,
            "--title",
            "bounded work",
            "--seat",
            "coder",
            "--initiator-kind",
            "harness",
            "--initiator-label",
            "codex",
            "--work-id",
            "work-1",
            "--issue-url",
            "https://github.com/example/project/pull/1",
        ]
        code, _, _ = fixture.invoke(argv, "prompt", application=application)
        self.assertEqual(code, 0)
        self.assertEqual(
            application.controller.submit.call_args.args[0]["work_id"], "work-1"
        )
        application.reset_mock()
        code, _, _ = fixture.invoke(
            argv + ["--initiator-label", "x" * 129], "prompt", application=application
        )
        self.assertEqual(code, 2)
        application.controller.submit.assert_not_called()

    def test_controller_both_dispatch_paths_include_origin_context(self):
        from tests import test_controller as controllers

        for peer in (False, True):
            with self.subTest(peer=peer), tempfile.TemporaryDirectory() as directory:
                from t3_fleet.controller import FleetController
                from t3_fleet.store import FleetStore

                transport = controllers.FakeTransport()
                store = FleetStore(Path(directory) / "controller.db")
                controller = FleetController(
                    controllers.peer_config() if peer else controllers.config(),
                    store,
                    transport,
                    admission=legacy.FakeAdmission(),
                )
                controller.submit(
                    {
                        "request_id": "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
                        if peer
                        else "req-12345678",
                        "host": "linux-worker",
                        "repository": "example-repo",
                        "source_revision": legacy.FULL_SHA,
                        "title": "bounded work",
                        "seat": "coder",
                        "initiator_kind": "agent",
                        "initiator_label": "agent:example",
                    },
                    prompt="prompt",
                )
                call = next(
                    c for c in transport.calls if c["action"] in {"claim", "submit"}
                )
                self.assertEqual(
                    call["execution_context"]["initiator"]["label"], "agent:example"
                )
                self.assertEqual(
                    call["execution_context"]["origin_node"], "linux-worker"
                )


class EnrolledReportingTests(legacy._AgentPolicyFixture, unittest.TestCase):
    def test_required_enrollment_reports_target_policy_and_launch_seat(self):
        self.agent = self.build(enrollment="required")
        events = []
        request = dict(
            self.submit_request,
            action="claim",
            request_id=legacy.PEER_REQUEST_ID,
            digest="d" * 64,
        )
        with patch(
            "t3_fleet.agent.report_event",
            side_effect=lambda event: events.append(event) or "queued",
        ):
            self.agent.handle(request)
        self.assertEqual(events[0]["state"], "run.started")
        self.assertEqual(events[0]["seat"], "coder")
        self.assertEqual(events[0]["repo_identity"], legacy.CANONICAL_REPO)
        self.assertEqual(
            events[0]["context"]["policy"]["session_id"],
            request["fleet_policy"]["session_id"],
        )
        self.assertEqual(events[0]["context"]["target_host"], "linux-worker")


if __name__ == "__main__":
    unittest.main()
