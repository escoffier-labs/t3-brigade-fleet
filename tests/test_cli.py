from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from t3_fleet.cli import _POLICY_REASON_MESSAGES
from t3_fleet.errors import (
    POLICY_REASONS,
    HostUnavailable,
    PolicyRejected,
    ValidationError,
)


FULL_SHA = "0123456789abcdef0123456789abcdef01234567"


class CliTests(unittest.TestCase):
    def invoke(
        self,
        argv: list[str],
        stdin_text: str = "",
        *,
        application: Mock | None = None,
    ) -> tuple[int, str, str]:
        from t3_fleet import cli

        stdout = io.StringIO()
        stderr = io.StringIO()
        app = application or Mock()
        app.close = Mock()
        with patch.object(cli, "_build_application", return_value=app):
            exit_code = cli.main(
                argv,
                stdin=io.StringIO(stdin_text),
                stdout=stdout,
                stderr=stderr,
                environ={},
            )
        return exit_code, stdout.getvalue(), stderr.getvalue()

    def test_submit_accepts_json_stdin_and_emits_one_json_result(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.submit.return_value = {
            "schema": "t3-fleet.result.v1",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "state": "running",
            "thread_id": "thread-1",
            "needs_attention": False,
        }
        payload = {
            "request_id": "req-12345678",
            "host": "linux-worker",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "prompt": "private prompt",
            "seat": "coder",
        }

        exit_code, stdout, stderr = self.invoke(
            ["submit"],
            json.dumps(payload),
            application=application,
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(stdout.strip().splitlines()), 1)
        self.assertEqual(json.loads(stdout)["thread_id"], "thread-1")
        self.assertEqual(stderr, "")
        submitted_request = application.controller.submit.call_args.args[0]
        self.assertNotIn("prompt", submitted_request)
        self.assertEqual(
            application.controller.submit.call_args.kwargs["prompt"],
            "private prompt",
        )

    def test_submit_accepts_versioned_json_envelope(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.submit.return_value = {
            "schema": "t3-fleet.result.v1",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "state": "running",
            "thread_id": "thread-1",
            "needs_attention": False,
        }
        payload = {
            "schema": "t3-fleet.request.v1",
            "action": "submit",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "prompt": "private prompt",
            "seat": "coder",
        }

        exit_code, _, _ = self.invoke(
            ["submit"],
            json.dumps(payload),
            application=application,
        )

        self.assertEqual(exit_code, 0)
        application.controller.submit.assert_called_once()

    def test_invoke_extracts_submit_action_from_json_envelope(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.submit.return_value = {
            "schema": "t3-fleet.result.v1",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "state": "running",
            "thread_id": "thread-1",
            "needs_attention": False,
        }
        payload = {
            "schema": "t3-fleet.request.v1",
            "action": "submit",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "prompt": "private prompt",
            "seat": "coder",
        }

        exit_code, stdout, stderr = self.invoke(
            ["invoke"],
            json.dumps(payload),
            application=application,
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout)["thread_id"], "thread-1")
        self.assertEqual(stderr, "")
        application.controller.submit.assert_called_once()
        self.assertEqual(
            application.controller.submit.call_args.kwargs["prompt"],
            "private prompt",
        )

    def test_invoke_routes_answer_fields_without_argv_answers(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.answer.return_value = {
            "schema": "t3-fleet.result.v1",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "state": "running",
            "thread_id": "thread-1",
            "needs_attention": False,
        }
        payload = {
            "schema": "t3-fleet.request.v1",
            "action": "answer",
            "request_id": "req-12345678",
            "input_id": "input-1",
            "answers": {"question-1": ["Option A"]},
        }

        exit_code, stdout, stderr = self.invoke(
            ["invoke"],
            json.dumps(payload),
            application=application,
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout)["state"], "running")
        self.assertEqual(stderr, "")
        application.controller.answer.assert_called_once_with(
            "req-12345678",
            "input-1",
            {"question-1": ["Option A"]},
        )

    def test_submit_human_flags_read_prompt_from_stdin_not_argv(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.submit.return_value = {
            "schema": "t3-fleet.result.v1",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "state": "running",
            "thread_id": "thread-1",
            "needs_attention": False,
        }

        exit_code, _, _ = self.invoke(
            [
                "submit",
                "--request-id",
                "req-12345678",
                "--host",
                "linux-worker",
                "--repository",
                "example-repo",
                "--source-revision",
                FULL_SHA,
                "--title",
                "Fix issue 123",
            ],
            "private prompt",
            application=application,
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            application.controller.submit.call_args.kwargs["prompt"],
            "private prompt",
        )

    def test_submit_reads_prompt_file_without_placing_content_in_arguments(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.submit.return_value = {
            "schema": "t3-fleet.result.v1",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "state": "running",
            "thread_id": "thread-1",
            "needs_attention": False,
        }
        with tempfile.TemporaryDirectory() as directory:
            prompt_path = Path(directory) / "prompt.txt"
            prompt_path.write_text("private file prompt", encoding="utf-8")
            payload = {
                "request_id": "req-12345678",
                "host": "linux-worker",
                "repository": "example-repo",
                "source_revision": FULL_SHA,
                "title": "Fix issue 123",
                "prompt_file": str(prompt_path),
            }
            exit_code, _, _ = self.invoke(
                ["submit"],
                json.dumps(payload),
                application=application,
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            application.controller.submit.call_args.kwargs["prompt"],
            "private file prompt",
        )

    def test_raw_prompt_option_is_rejected_without_echoing_secret(self) -> None:
        secret = "do-not-print-this"
        exit_code, stdout, stderr = self.invoke(["submit", "--prompt", secret])

        self.assertEqual(exit_code, 2)
        result = json.loads(stdout)
        self.assertEqual(result["error"]["code"], "invalid_request")
        self.assertNotIn(secret, stdout)
        self.assertNotIn(secret, stderr)
        self.assertEqual(stderr.strip(), "t3-fleet: invalid_request")

    def test_prompt_limit_matches_target_agent_contract(self) -> None:
        from t3_fleet import cli

        request = {"prompt": "x" * 120_001}
        with self.assertRaisesRegex(ValidationError, "prompt exceeds"):
            cli._read_prompt(request, "", human=False)

    def test_followup_rejects_invalid_json_modes_before_controller_mutation(
        self,
    ) -> None:
        for field, value in (
            ("runtime_mode", "unrestricted"),
            ("interaction_mode", "unplanned"),
        ):
            with self.subTest(field=field):
                application = Mock()
                application.close = Mock()
                payload = {
                    "schema": "t3-fleet.request.v1",
                    "action": "followup",
                    "request_id": "req-12345678",
                    "prompt": "next prompt",
                    field: value,
                }

                exit_code, stdout, stderr = self.invoke(
                    ["invoke"],
                    json.dumps(payload),
                    application=application,
                )

                self.assertEqual(exit_code, 2)
                self.assertEqual(
                    json.loads(stdout)["error"]["code"],
                    "invalid_request",
                )
                self.assertEqual(stderr.strip(), "t3-fleet: invalid_request")
                application.controller.followup.assert_not_called()

    def test_fleet_error_uses_stable_exit_code_and_redacted_json(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.status.side_effect = HostUnavailable(
            "worker unavailable"
        )

        exit_code, stdout, stderr = self.invoke(
            ["status", "--request-id", "req-12345678"],
            application=application,
        )

        self.assertEqual(exit_code, 3)
        self.assertEqual(json.loads(stdout)["error"]["code"], "host_unavailable")
        self.assertEqual(stderr.strip(), "t3-fleet: host_unavailable")

    def test_a_typed_policy_reason_reaches_the_user_as_json(self) -> None:
        """A denial the operator can act on must survive the transport.

        Every policy denial shares one error code, so without a typed reason a
        stale digest, a wrong machine, and a resumed request that lost its
        reservation all read as the same generic refusal and the operator
        cannot tell which one to fix.
        """
        application = Mock()
        application.close = Mock()
        application.controller.status.side_effect = PolicyRejected(
            "Fleet Hub model admission was denied",
            reason="continuation-admission-required",
        )

        exit_code, stdout, _ = self.invoke(
            ["status", "--request-id", "req-12345678"], application=application
        )

        payload = json.loads(stdout)
        self.assertEqual(exit_code, 3)
        self.assertEqual(payload["error"]["code"], "policy_rejected")
        self.assertEqual(
            payload["error"]["reason"], "continuation-admission-required"
        )
        self.assertNotEqual(
            payload["error"]["message"], "fleet model admission was denied"
        )

    def test_every_allowlisted_reason_has_its_own_operator_sentence(self) -> None:
        # A reason that reaches the user without a sentence of its own would
        # print the generic denial and tell the operator nothing.
        self.assertEqual(set(_POLICY_REASON_MESSAGES), set(POLICY_REASONS))
        for reason in sorted(POLICY_REASONS):
            with self.subTest(reason=reason):
                application = Mock()
                application.close = Mock()
                application.controller.status.side_effect = PolicyRejected(
                    "denied", reason=reason
                )
                _, stdout, _ = self.invoke(
                    ["status", "--request-id", "req-12345678"],
                    application=application,
                )
                payload = json.loads(stdout)
                self.assertEqual(payload["error"]["reason"], reason)
                self.assertEqual(
                    payload["error"]["message"], _POLICY_REASON_MESSAGES[reason]
                )

    def test_a_policy_denial_without_a_reason_is_unchanged(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.status.side_effect = PolicyRejected("denied")

        exit_code, stdout, _ = self.invoke(
            ["status", "--request-id", "req-12345678"], application=application
        )

        payload = json.loads(stdout)
        self.assertEqual(exit_code, 3)
        self.assertEqual(payload["error"]["code"], "policy_rejected")
        self.assertNotIn("reason", payload["error"])
        self.assertEqual(
            payload["error"]["message"], "fleet model admission was denied"
        )

    def test_an_unallowlisted_reason_never_reaches_the_user(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.status.side_effect = PolicyRejected(
            "denied", reason="/srv/operator/private/path"
        )

        _, stdout, _ = self.invoke(
            ["status", "--request-id", "req-12345678"], application=application
        )

        payload = json.loads(stdout)
        self.assertNotIn("reason", payload["error"])
        self.assertNotIn("/srv/operator", stdout)

    def test_unexpected_error_does_not_expose_exception_text(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.status.side_effect = RuntimeError(
            "private prompt leaked by dependency"
        )

        exit_code, stdout, stderr = self.invoke(
            ["status", "--request-id", "req-12345678"],
            application=application,
        )

        self.assertEqual(exit_code, 1)
        self.assertEqual(json.loads(stdout)["error"]["code"], "internal_error")
        self.assertNotIn("private prompt", stdout)
        self.assertNotIn("private prompt", stderr)

    def test_unknown_state_error_includes_redacted_request_id_after_journal(self) -> None:
        from t3_fleet.errors import UnknownState

        application = Mock()
        application.close = Mock()
        unknown = UnknownState(
            "dispatch outcome is unknown for request req-12345678; run recover",
            detail="target capacity is exhausted at 1 active requests",
        )
        unknown.request_id = "req-12345678"
        application.controller.submit.side_effect = unknown

        exit_code, stdout, stderr = self.invoke(
            [
                "submit",
                "--request-id",
                "req-12345678",
                "--host",
                "linux-worker",
                "--repository",
                "example-repo",
                "--source-revision",
                FULL_SHA,
                "--title",
                "Fix issue 123",
            ],
            "private prompt",
            application=application,
        )

        self.assertEqual(exit_code, 5)
        payload = json.loads(stdout)
        self.assertEqual(payload["schema"], "t3-fleet.error.v1")
        self.assertEqual(payload["error"]["code"], "unknown_state")
        self.assertEqual(
            payload["error"]["detail"],
            "target capacity is exhausted at 1 active requests",
        )
        self.assertEqual(payload["request_id"], "req-12345678")
        self.assertNotIn("private prompt", stdout)
        self.assertNotIn("private prompt", stderr)
        self.assertEqual(stderr.strip(), "t3-fleet: unknown_state")

    def test_unknown_state_error_omits_request_id_when_not_journaled(self) -> None:
        from t3_fleet.errors import UnknownState

        application = Mock()
        application.close = Mock()
        application.controller.status.side_effect = UnknownState(
            "status outcome is unknown for request req-12345678; run recover"
        )

        exit_code, stdout, _ = self.invoke(
            ["status", "--request-id", "req-12345678"],
            application=application,
        )

        self.assertEqual(exit_code, 5)
        payload = json.loads(stdout)
        self.assertEqual(payload["schema"], "t3-fleet.error.v1")
        self.assertEqual(payload["error"]["code"], "unknown_state")
        self.assertNotIn("request_id", payload)

    def test_recover_by_target_routes_target_hint_without_request_id(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.recover.return_value = {
            "schema": "t3-fleet.result.v1",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "state": "running",
            "thread_id": "thread-1",
            "needs_attention": False,
        }

        exit_code, stdout, _ = self.invoke(
            ["recover", "--target", "linux-worker"],
            "",
            application=application,
        )

        self.assertEqual(exit_code, 0)
        application.controller.recover.assert_called_once_with(
            None,
            include_output=False,
            target_hint="linux-worker",
        )
        self.assertEqual(json.loads(stdout)["request_id"], "req-12345678")

    def test_recover_without_id_or_target_is_invalid_request(self) -> None:
        exit_code, stdout, _ = self.invoke(["recover"], "")
        self.assertEqual(exit_code, 2)
        payload = json.loads(stdout)
        self.assertEqual(payload["schema"], "t3-fleet.error.v1")
        self.assertEqual(payload["error"]["code"], "invalid_request")
        self.assertNotIn("request_id", payload)

    def test_routes_every_controller_subcommand(self) -> None:
        application = Mock()
        application.close = Mock()
        result = {
            "schema": "t3-fleet.result.v1",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "state": "running",
            "thread_id": "thread-1",
            "needs_attention": False,
        }
        application.doctor.return_value = result
        application.capacity.return_value = result
        application.controller.status.return_value = result
        application.controller.wait.return_value = result
        application.controller.followup.return_value = result
        application.controller.interrupt.return_value = result
        application.controller.approve.return_value = result
        application.controller.answer.return_value = result
        application.controller.recover.return_value = result

        cases = [
            (["doctor"], "", application.doctor),
            (["capacity"], "", application.capacity),
            (
                ["status", "--request-id", "req-12345678"],
                "",
                application.controller.status,
            ),
            (
                ["wait", "--request-id", "req-12345678", "--timeout", "1"],
                "",
                application.controller.wait,
            ),
            (
                ["followup", "--request-id", "req-12345678"],
                "next prompt",
                application.controller.followup,
            ),
            (
                ["interrupt", "--request-id", "req-12345678"],
                "",
                application.controller.interrupt,
            ),
            (
                [
                    "approve",
                    "--request-id",
                    "req-12345678",
                    "--approval-id",
                    "approval-1",
                    "--decision",
                    "accept",
                ],
                "",
                application.controller.approve,
            ),
            (
                ["answer"],
                json.dumps(
                    {
                        "request_id": "req-12345678",
                        "input_id": "input-1",
                        "answers": {"question-1": ["Option A"]},
                    }
                ),
                application.controller.answer,
            ),
            (
                ["recover", "--request-id", "req-12345678"],
                "",
                application.controller.recover,
            ),
            (
                [
                    "recover",
                    "--request-id",
                    "req-12345678",
                    "--target",
                    "linux-worker",
                ],
                "",
                application.controller.recover,
            ),
        ]

        for argv, stdin_text, method in cases:
            with self.subTest(command=argv[0]):
                exit_code, stdout, _ = self.invoke(
                    argv,
                    stdin_text,
                    application=application,
                )
                self.assertEqual(exit_code, 0)
                self.assertEqual(len(stdout.strip().splitlines()), 1)
                method.assert_called()

    def test_recover_json_stdin_accepts_optional_target_hint(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.recover.return_value = {
            "schema": "t3-fleet.result.v1",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "state": "running",
            "thread_id": "thread-1",
            "needs_attention": False,
        }
        stdin_text = json.dumps(
            {
                "action": "recover",
                "request_id": "req-12345678",
                "target": "linux-worker",
            }
        )
        exit_code, stdout, _ = self.invoke(
            ["recover"],
            stdin_text,
            application=application,
        )
        self.assertEqual(exit_code, 0)
        application.controller.recover.assert_called_once_with(
            "req-12345678",
            include_output=False,
            target_hint="linux-worker",
        )
        self.assertEqual(len(stdout.strip().splitlines()), 1)

    def test_recover_rejects_empty_target_hint(self) -> None:
        for argv, stdin_text in (
            (["recover", "--request-id", "req-12345678", "--target", ""], ""),
            (
                ["recover"],
                json.dumps(
                    {
                        "action": "recover",
                        "request_id": "req-12345678",
                        "target": "",
                    }
                ),
            ),
        ):
            with self.subTest(argv=argv):
                exit_code, stdout, _ = self.invoke(argv, stdin_text)
                self.assertEqual(exit_code, 2)
                payload = json.loads(stdout.strip())
                self.assertEqual(payload["schema"], "t3-fleet.error.v1")
                self.assertEqual(payload["error"]["code"], "invalid_request")

    def test_doctor_without_flags_does_not_read_interactive_stdin(self) -> None:
        from t3_fleet import cli

        class InteractiveInput(io.StringIO):
            def isatty(self) -> bool:
                return True

            def read(self, size: int = -1) -> str:
                raise AssertionError("interactive input must not be read")

        application = Mock()
        application.close = Mock()
        application.doctor.return_value = {
            "schema": "t3-fleet.result.v1",
            "action": "doctor",
            "state": "eligible",
            "hosts": [],
        }
        stdout = io.StringIO()
        with patch.object(cli, "_build_application", return_value=application):
            exit_code = cli.main(
                ["doctor"],
                stdin=InteractiveInput(),
                stdout=stdout,
                stderr=io.StringIO(),
                environ={},
            )

        self.assertEqual(exit_code, 0)
        application.doctor.assert_called_once_with({})

    def test_doctor_propagates_target_active_count_and_limit(self) -> None:
        from t3_fleet import cli

        host = Mock(host_id="linux-worker", enabled=True)
        config = Mock(
            hosts={"linux-worker": host},
            repositories={"example-repo": Mock()},
        )
        transport = Mock()
        transport.call.return_value = {
            "state": "eligible",
            "server_version": "0.0.30",
            "project_registered": True,
            "active": 1,
            "limit": 2,
            "observed_project_default": {
                "instanceId": "stale-instance",
                "model": "gpt-5.6-luna",
            },
            "approved_model_admission": {
                "roster_revision": 42,
                "roster_digest": "sha256:" + "a" * 64,
                "seat": "coder",
                "provider": "openai",
                "model": "gpt-5.6-terra",
                "reasoning": "high",
                "provider_instance_id": "codex",
                "service_tier": "priority",
                "source": "hub",
                "expires_at": "2026-08-30T14:00:00Z",
            },
            "admission_source": "hub",
            "admission_cache_age_seconds": 12.5,
            "project_default_drift": True,
        }
        application = cli._Application(
            config=config,
            store=Mock(),
            transport=transport,
            controller=Mock(),
        )

        result = application.doctor(
            {"host": "linux-worker", "repository": "example-repo"}
        )

        self.assertEqual(result["state"], "eligible")
        self.assertEqual(result["hosts"][0]["active"], 1)
        self.assertEqual(result["hosts"][0]["limit"], 2)
        self.assertEqual(
            result["hosts"][0]["approved_model_admission"]["model"],
            "gpt-5.6-terra",
        )
        self.assertTrue(result["hosts"][0]["project_default_drift"])

    def test_doctor_aggregate_eligible_when_mix_of_eligible_and_at_capacity(self) -> None:
        from t3_fleet import cli

        host_a = Mock(host_id="worker-a", enabled=True)
        host_b = Mock(host_id="worker-b", enabled=True)
        config = Mock(
            hosts={"worker-a": host_a, "worker-b": host_b},
            repositories={},
        )
        transport = Mock()

        def call(host, _request):
            if host.host_id == "worker-a":
                return {"state": "eligible"}
            return {"state": "at_capacity"}

        transport.call.side_effect = call
        application = cli._Application(
            config=config,
            store=Mock(),
            transport=transport,
            controller=Mock(),
        )

        result = application.doctor({})

        self.assertEqual(result["state"], "eligible")
        self.assertEqual(len(result["hosts"]), 2)
        states = {item["host"]: item["state"] for item in result["hosts"]}
        self.assertEqual(states, {"worker-a": "eligible", "worker-b": "at_capacity"})

    def test_doctor_continues_after_unavailable_host(self) -> None:
        from t3_fleet import cli

        host_a = Mock(host_id="worker-a", enabled=True)
        host_b = Mock(host_id="worker-b", enabled=True)
        config = Mock(
            hosts={"worker-a": host_a, "worker-b": host_b},
            repositories={},
        )
        transport = Mock()
        transport.call.side_effect = [
            HostUnavailable("private transport detail"),
            {
                "state": "eligible",
                "server_version": "0.0.34",
                "active": 0,
                "limit": 1,
            },
        ]
        application = cli._Application(
            config=config,
            store=Mock(),
            transport=transport,
            controller=Mock(),
        )

        result = application.doctor({})

        self.assertEqual(result["state"], "blocked_offline")
        self.assertEqual(
            result["hosts"],
            [
                {
                    "host": "worker-a",
                    "state": "blocked_offline",
                    "server_version": None,
                },
                {
                    "host": "worker-b",
                    "state": "eligible",
                    "server_version": "0.0.34",
                    "active": 0,
                    "limit": 1,
                },
            ],
        )
        self.assertNotIn("private transport detail", repr(result))

    def test_capacity_continues_after_unavailable_host(self) -> None:
        from t3_fleet import cli

        host_a = Mock(
            host_id="worker-a", enabled=True, active_limit=1, repository_limit=1
        )
        host_b = Mock(
            host_id="worker-b", enabled=True, active_limit=1, repository_limit=1
        )
        config = Mock(
            hosts={"worker-a": host_a, "worker-b": host_b},
            global_active_limit=2,
        )
        store = Mock()
        store.circuit_is_open.return_value = False
        transport = Mock()
        transport.call.side_effect = [
            HostUnavailable("private transport detail"),
            {"state": "eligible", "active": 0},
        ]
        application = cli._Application(
            config=config,
            store=store,
            transport=transport,
            controller=Mock(),
        )

        result = application.capacity({})

        self.assertEqual(result["state"], "blocked_offline")
        self.assertEqual(
            result["hosts"][0],
            {
                "host": "worker-a",
                "state": "blocked_offline",
                "active": None,
                "limit": 1,
                "repository_limit": 1,
                "circuit_open": False,
            },
        )
        self.assertEqual(result["hosts"][1]["state"], "eligible")
        self.assertNotIn("private transport detail", repr(result))


class BrigadeInvokeCliTests(unittest.TestCase):
    call_id = "call-0123456789abcdef"
    run_id = "run-fedcba9876543210"

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / ".brigade" / "tools").mkdir(parents=True)

    def write_call(
        self,
        args: dict,
        *,
        call_id: str | None = None,
        run_id: str | None = None,
        status: str = "running",
    ) -> str:
        record = {
            "id": call_id or self.call_id,
            "run_id": run_id or self.run_id,
            "status": status,
            "tool_id": "t3-fleet",
            "family": "script",
            "command": "t3-fleet brigade-invoke",
            "args": args,
            "arguments": {},
            "blockers": [],
            "approval_fingerprint": "approval-fingerprint",
            "contract": {
                "approval_mode": "always",
                "approval_required": True,
            },
        }
        text = json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n"
        (self.root / ".brigade" / "tools" / "calls.jsonl").write_text(
            text,
            encoding="utf-8",
        )
        return text

    def invoke(
        self,
        argv: list[str],
        *,
        application: Mock | None = None,
        environ: dict[str, str] | None = None,
        stdin_text: str = "",
    ) -> tuple[int, str, str]:
        from t3_fleet import cli

        app = application or Mock()
        app.close = Mock()
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch.object(cli, "_build_application", return_value=app),
            patch.object(cli.Path, "cwd", return_value=self.root),
        ):
            exit_code = cli.main(
                argv,
                stdin=io.StringIO(stdin_text),
                stdout=stdout,
                stderr=stderr,
                environ=environ or {},
            )
        return exit_code, stdout.getvalue(), stderr.getvalue()

    def valid_environ(self) -> dict[str, str]:
        return {
            "BRIGADE_TOOL_CALL_ID": self.call_id,
            "BRIGADE_TOOL_RUN_ID": self.run_id,
        }

    def test_brigade_invoke_reads_matching_call_and_protected_prompt_file(self) -> None:
        secret = "prompt-content-must-not-enter-brigade-records"  # content-guard: allow api-key-assignment
        prompt_file = self.root / "protected-prompt.txt"
        prompt_file.write_text(secret, encoding="utf-8")
        call_text = self.write_call(
            {
                "schema": "t3-fleet.request.v1",
                "action": "submit",
                "request_id": "req-12345678",
                "host": "linux-worker",
                "repository": "example-repo",
                "source_revision": FULL_SHA,
                "title": "Fix issue 123",
                "prompt_file": str(prompt_file),
                "seat": "coder",
            }
        )
        application = Mock()
        application.close = Mock()
        application.controller.submit.return_value = {
            "schema": "t3-fleet.result.v1",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "state": "running",
            "thread_id": "thread-1",
            "needs_attention": False,
        }

        argv = ["brigade-invoke"]
        exit_code, stdout, stderr = self.invoke(
            argv,
            application=application,
            environ=self.valid_environ(),
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout)["state"], "running")
        self.assertEqual(stderr, "")
        self.assertEqual(argv, ["brigade-invoke"])
        self.assertNotIn(secret, call_text)
        self.assertNotIn(secret, stdout)
        self.assertNotIn(secret, stderr)
        submitted_request = application.controller.submit.call_args.args[0]
        self.assertNotIn("prompt", submitted_request)
        self.assertEqual(
            application.controller.submit.call_args.kwargs["prompt"],
            secret,
        )

    def test_brigade_invoke_rejects_missing_or_mismatched_context(self) -> None:
        self.write_call(
            {
                "schema": "t3-fleet.request.v1",
                "action": "doctor",
            }
        )
        cases = (
            {},
            {
                "BRIGADE_TOOL_CALL_ID": self.call_id,
                "BRIGADE_TOOL_RUN_ID": "run-0000000000000000",
            },
        )

        for environ in cases:
            with self.subTest(environ=environ):
                exit_code, stdout, stderr = self.invoke(
                    ["brigade-invoke"],
                    environ=environ,
                )
                self.assertEqual(exit_code, 2)
                self.assertEqual(
                    json.loads(stdout)["error"]["code"],
                    "invalid_request",
                )
                self.assertEqual(stderr.strip(), "t3-fleet: invalid_request")

    def test_brigade_invoke_rejects_inline_prompt_without_echoing_it(self) -> None:
        secret = "stored-inline-prompt-must-not-run"  # content-guard: allow api-key-assignment
        self.write_call(
            {
                "schema": "t3-fleet.request.v1",
                "action": "submit",
                "request_id": "req-12345678",
                "host": "linux-worker",
                "repository": "example-repo",
                "source_revision": FULL_SHA,
                "title": "Fix issue 123",
                "prompt": secret,
                "seat": "coder",
            }
        )

        exit_code, stdout, stderr = self.invoke(
            ["brigade-invoke"],
            environ=self.valid_environ(),
        )

        self.assertEqual(exit_code, 2)
        self.assertEqual(json.loads(stdout)["error"]["code"], "invalid_request")
        self.assertNotIn(secret, stdout)
        self.assertNotIn(secret, stderr)

    def test_brigade_invoke_rejects_command_arguments(self) -> None:
        exit_code, stdout, stderr = self.invoke(
            ["brigade-invoke", "request-data-is-not-an-argument"],
            environ=self.valid_environ(),
        )

        self.assertEqual(exit_code, 2)
        self.assertEqual(json.loads(stdout)["error"]["code"], "invalid_request")
        self.assertNotIn("request-data-is-not-an-argument", stdout)
        self.assertNotIn("request-data-is-not-an-argument", stderr)


class AgentMainTests(unittest.TestCase):
    class LineWithoutEof:
        def readline(self, size: int = -1) -> str:
            return '{"action":"doctor"}\n'

        def read(self, size: int = -1) -> str:
            raise AssertionError("agent must not wait for stdin EOF")

    def test_agent_reads_one_json_request_and_writes_one_json_response(self) -> None:
        from t3_fleet import agent

        fake = Mock()
        fake.handle.return_value = {
            "schema": "t3-fleet.agent-result.v1",
            "host": "linux-worker",
            "state": "eligible",
            "needs_attention": False,
        }
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.object(agent, "_build_agent", return_value=fake):
            exit_code = agent.main(
                [],
                stdin=self.LineWithoutEof(),
                stdout=stdout,
                stderr=stderr,
                environ={},
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(stdout.getvalue().strip().splitlines()), 1)
        self.assertEqual(json.loads(stdout.getvalue())["state"], "eligible")
        fake.handle.assert_called_once_with({"action": "doctor"})

    def test_agent_validation_error_is_redacted_and_stable(self) -> None:
        from t3_fleet import agent

        fake = Mock()
        fake.handle.side_effect = ValidationError("invalid private payload")
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.object(agent, "_build_agent", return_value=fake):
            exit_code = agent.main(
                [],
                stdin=io.StringIO('{"action":"submit","prompt":"secret"}'),
                stdout=stdout,
                stderr=stderr,
                environ={},
            )

        self.assertEqual(exit_code, 2)
        self.assertEqual(
            json.loads(stdout.getvalue())["error"]["code"],
            "invalid_request",
        )
        self.assertNotIn("secret", stdout.getvalue())
        self.assertNotIn("secret", stderr.getvalue())
        self.assertEqual(stderr.getvalue().strip(), "t3-fleet-agent: invalid_request")

    def test_agent_host_error_includes_safe_detail(self) -> None:
        from t3_fleet import agent

        fake = Mock()
        fake.handle.side_effect = HostUnavailable(
            "target capacity is exhausted at 1 active requests"
        )
        stdout = io.StringIO()
        with patch.object(agent, "_build_agent", return_value=fake):
            exit_code = agent.main(
                [],
                stdin=io.StringIO('{"action":"claim","prompt":"private prompt"}'),
                stdout=stdout,
                stderr=io.StringIO(),
                environ={},
            )

        self.assertEqual(exit_code, 3)
        self.assertEqual(
            json.loads(stdout.getvalue())["error"]["detail"],
            "target capacity is exhausted at 1 active requests",
        )
        self.assertNotIn("private prompt", stdout.getvalue())

    def test_agent_omits_detail_that_reflects_request_content(self) -> None:
        from t3_fleet import agent

        private_prompt = "private-prompt-content"
        fake = Mock()
        fake.handle.side_effect = HostUnavailable(f"target failed: {private_prompt}")
        stdout = io.StringIO()
        with patch.object(agent, "_build_agent", return_value=fake):
            exit_code = agent.main(
                [],
                stdin=io.StringIO(
                    json.dumps({"action": "claim", "prompt": private_prompt})
                ),
                stdout=stdout,
                stderr=io.StringIO(),
                environ={},
            )

        self.assertEqual(exit_code, 3)
        self.assertNotIn("detail", json.loads(stdout.getvalue())["error"])
        self.assertNotIn(private_prompt, stdout.getvalue())

    def test_agent_unexpected_error_emits_closed_internal_envelope(self) -> None:
        from t3_fleet import agent

        fake = Mock()
        fake.handle.side_effect = RuntimeError("failure at /private/worktree")
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.object(agent, "_build_agent", return_value=fake):
            exit_code = agent.main(
                [],
                stdin=io.StringIO('{"action":"doctor"}'),
                stdout=stdout,
                stderr=stderr,
                environ={},
            )

        self.assertEqual(exit_code, 1)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "schema": "t3-fleet.agent-error.v1",
                "error": {
                    "code": "internal_error",
                    "message": "internal target error",
                },
            },
        )
        self.assertEqual(stderr.getvalue().strip(), "t3-fleet-agent: internal_error")
        self.assertNotIn("/private/worktree", stdout.getvalue() + stderr.getvalue())


class DefaultPathTests(unittest.TestCase):
    def test_environment_overrides_config_and_state_paths(self) -> None:
        from t3_fleet import cli

        environ = {
            "T3_FLEET_CONFIG": "/private/fleet.toml",
            "T3_FLEET_STATE": "/private/fleet.db",
        }

        self.assertEqual(
            cli._platform_config_path(environ),
            Path("/private/fleet.toml"),
        )
        self.assertEqual(
            cli._platform_state_path(environ),
            Path("/private/fleet.db"),
        )


class PowerShellLauncherContractTests(unittest.TestCase):
    def test_launcher_redacts_missing_python_and_native_launch_failures(self) -> None:
        script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "t3-fleet-agent.ps1"
        ).read_text(encoding="utf-8")

        self.assertIn(
            '{"schema":"t3-fleet.agent-error.v1","error":{"code":"invalid_config",'
            '"message":"target configuration is invalid"}}',
            script,
        )
        self.assertIn(
            '{"schema":"t3-fleet.agent-error.v1","error":{"code":"internal_error",'
            '"message":"internal target error"}}',
            script,
        )
        self.assertIn('Write-AgentError "invalid_config"', script)
        self.assertIn('Write-AgentError "internal_error"', script)
        self.assertIn('$ErrorActionPreference = "Continue"', script)
        self.assertIn(
            "$ErrorActionPreference = $SavedErrorActionPreference",
            script,
        )
        self.assertNotIn("2>$null", script)
        self.assertIn("2>&1", script)
        self.assertIn("System.Management.Automation.ErrorRecord", script)
        self.assertNotIn("Write-Error", script)

    def test_launcher_passes_through_policy_conflict_and_unknown_codes(self) -> None:
        script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "t3-fleet-agent.ps1"
        ).read_text(encoding="utf-8")

        self.assertIn('"idempotency_conflict",', script)
        self.assertIn('"policy_rejected",', script)
        self.assertIn('"policy_revision_conflict",', script)
        self.assertIn('"unknown_state",', script)
        # A busy decline must reach the controller as itself, or Windows
        # targets would turn it into internal_error and an unknown outcome.
        self.assertIn('"target_busy",', script)
        self.assertIn("exit $AgentExitCode", script)


class CliFleetPolicyTests(unittest.TestCase):
    def invoke(
        self,
        argv: list[str],
        stdin_text: str,
        *,
        application: Mock,
    ) -> tuple[int, str, str]:
        from t3_fleet import cli

        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.object(cli, "_build_application", return_value=application):
            exit_code = cli.main(
                argv,
                stdin=io.StringIO(stdin_text),
                stdout=stdout,
                stderr=stderr,
                environ={},
            )
        return exit_code, stdout.getvalue(), stderr.getvalue()

    def application(self) -> Mock:
        application = Mock()
        application.close = Mock()
        application.controller.submit.return_value = {
            "schema": "t3-fleet.result.v1",
            "request_id": "req-12345678",
            "host": "second-worker",
            "state": "running",
            "thread_id": "thread-1",
            "needs_attention": False,
        }
        return application

    def test_human_submit_forwards_auto_host_without_a_seat(self) -> None:
        application = self.application()
        exit_code, _, _ = self.invoke(
            [
                "submit",
                "--host",
                "auto",
                "--repository",
                "example-repo",
                "--source-revision",
                FULL_SHA,
                "--title",
                "Fix issue 123",
            ],
            "private prompt",
            application=application,
        )
        self.assertEqual(exit_code, 0)
        request = application.controller.submit.call_args.args[0]
        self.assertEqual(request["host"], "auto")
        self.assertNotIn("seat", request)
        self.assertNotIn("override_reason", request)

    def test_human_submit_forwards_the_override_reason(self) -> None:
        application = self.application()
        exit_code, _, _ = self.invoke(
            [
                "submit",
                "--host",
                "linux-worker",
                "--repository",
                "example-repo",
                "--source-revision",
                FULL_SHA,
                "--title",
                "Fix issue 123",
                "--override-reason",
                "operator pinned the controller host",
            ],
            "private prompt",
            application=application,
        )
        self.assertEqual(exit_code, 0)
        request = application.controller.submit.call_args.args[0]
        self.assertEqual(
            request["override_reason"], "operator pinned the controller host"
        )

    def test_human_submit_forwards_a_seat_override_under_auto_selection(self) -> None:
        application = self.application()
        exit_code, _, _ = self.invoke(
            [
                "submit",
                "--host",
                "auto",
                "--repository",
                "example-repo",
                "--source-revision",
                FULL_SHA,
                "--title",
                "Fix issue 123",
                "--seat",
                "muse13",
                "--override-reason",
                "operator pinned the review seat",
            ],
            "private prompt",
            application=application,
        )
        self.assertEqual(exit_code, 0)
        request = application.controller.submit.call_args.args[0]
        self.assertEqual(request["host"], "auto")
        self.assertEqual(request["seat"], "muse13")
        self.assertEqual(
            request["override_reason"], "operator pinned the review seat"
        )

    def test_json_submit_accepts_the_override_reason_field(self) -> None:
        application = self.application()
        payload = {
            "schema": "t3-fleet.request.v1",
            "action": "submit",
            "request_id": "req-12345678",
            "host": "linux-worker",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "prompt": "private prompt",
            "seat": "coder",
            "override_reason": "operator pinned the controller host",
        }
        exit_code, _, _ = self.invoke(
            ["submit"], json.dumps(payload), application=application
        )
        self.assertEqual(exit_code, 0)
        request = application.controller.submit.call_args.args[0]
        self.assertEqual(
            request["override_reason"], "operator pinned the controller host"
        )


class CliWorkloadAndDelegationTests(unittest.TestCase):
    """The two new public flags, and the denials they can produce."""

    invoke = CliFleetPolicyTests.invoke
    application = CliFleetPolicyTests.application

    def test_human_submit_forwards_an_explicit_workload(self) -> None:
        application = self.application()
        exit_code, _, _ = self.invoke(
            [
                "submit",
                "--host",
                "auto",
                "--repository",
                "example-repo",
                "--source-revision",
                FULL_SHA,
                "--title",
                "Fix issue 123",
                "--workload",
                "windows-development",
            ],
            "private prompt",
            application=application,
        )
        self.assertEqual(exit_code, 0)
        request = application.controller.submit.call_args.args[0]
        self.assertEqual(request["workload"], "windows-development")
        # Stating the workload is not an override.
        self.assertNotIn("override_reason", request)

    def test_an_omitted_workload_is_not_invented_by_the_client(self) -> None:
        application = self.application()
        self.invoke(
            [
                "submit",
                "--host",
                "auto",
                "--repository",
                "example-repo",
                "--source-revision",
                FULL_SHA,
                "--title",
                "Fix issue 123",
            ],
            "private prompt",
            application=application,
        )
        request = application.controller.submit.call_args.args[0]
        self.assertNotIn("workload", request)

    def test_human_submit_forwards_only_the_delegation_id(self) -> None:
        application = self.application()
        exit_code, _, _ = self.invoke(
            [
                "submit",
                "--delegation-id",
                "del-8f2c1a",
                "--title",
                "Fix issue 123",
            ],
            "private prompt",
            application=application,
        )
        self.assertEqual(exit_code, 0)
        request = application.controller.submit.call_args.args[0]
        self.assertEqual(request["delegation_id"], "del-8f2c1a")
        for absent in ("host", "repository", "source_revision", "seat"):
            self.assertNotIn(absent, request)

    def test_json_submit_accepts_the_delegation_and_workload_fields(self) -> None:
        application = self.application()
        payload = {
            "schema": "t3-fleet.request.v1",
            "action": "submit",
            "delegation_id": "del-8f2c1a",
            "title": "Fix issue 123",
            "prompt": "private prompt",
            "workload": "windows-development",
        }
        exit_code, _, _ = self.invoke(
            ["submit"], json.dumps(payload), application=application
        )
        self.assertEqual(exit_code, 0)
        request = application.controller.submit.call_args.args[0]
        self.assertEqual(request["delegation_id"], "del-8f2c1a")
        self.assertEqual(request["workload"], "windows-development")

    def test_there_is_no_public_consumer_or_adapter_flag(self) -> None:
        for flag in ("--consumer", "--adapter", "--model-plan"):
            with self.subTest(flag=flag):
                application = self.application()
                exit_code, stdout, _ = self.invoke(
                    ["submit", flag, "brigade-run", "--title", "Fix issue 123"],
                    "private prompt",
                    application=application,
                )
                self.assertEqual(exit_code, 2)
                self.assertEqual(
                    json.loads(stdout)["error"]["code"], "invalid_request"
                )
                application.controller.submit.assert_not_called()

    def test_a_json_consumer_field_is_refused_as_an_unsupported_field(self) -> None:
        application = self.application()
        payload = {
            "schema": "t3-fleet.request.v1",
            "action": "submit",
            "delegation_id": "del-8f2c1a",
            "title": "Fix issue 123",
            "prompt": "private prompt",
            "consumer": "brigade-run",
        }
        exit_code, stdout, _ = self.invoke(
            ["submit"], json.dumps(payload), application=application
        )
        self.assertEqual(exit_code, 2)
        self.assertEqual(json.loads(stdout)["error"]["code"], "invalid_request")

    def test_a_caller_supplied_receipt_is_never_authority(self) -> None:
        for field in ("delegation", "delegation_receipt", "adapter"):
            with self.subTest(field=field):
                application = self.application()
                payload = {
                    "schema": "t3-fleet.request.v1",
                    "action": "submit",
                    "delegation_id": "del-8f2c1a",
                    "title": "Fix issue 123",
                    "prompt": "private prompt",
                    field: {"consumer": "brigade-run", "adapter": "t3-fleet"},
                }
                exit_code, stdout, _ = self.invoke(
                    ["submit"], json.dumps(payload), application=application
                )
                self.assertEqual(exit_code, 2)
                self.assertEqual(
                    json.loads(stdout)["error"]["code"], "invalid_request"
                )
                application.controller.submit.assert_not_called()

    def test_every_delegation_denial_has_its_own_public_sentence(self) -> None:
        from t3_fleet.errors import PolicyRejected

        for reason in (
            "delegation-not-enrolled",
            "delegation-invalid",
            "delegation-conflict",
            "delegation-repository-unmatched",
        ):
            with self.subTest(reason=reason):
                application = self.application()
                application.controller.submit.side_effect = PolicyRejected(
                    "internal detail that must not be echoed", reason=reason
                )
                exit_code, stdout, _ = self.invoke(
                    [
                        "submit",
                        "--delegation-id",
                        "del-8f2c1a",
                        "--title",
                        "Fix issue 123",
                    ],
                    "private prompt",
                    application=application,
                )
                self.assertEqual(exit_code, 3)
                error = json.loads(stdout)["error"]
                self.assertEqual(error["code"], "policy_rejected")
                self.assertEqual(error["reason"], reason)
                self.assertNotIn("internal detail", error["message"])

    def test_a_delegation_refusal_is_never_read_as_an_unsupported_flag(self) -> None:
        """Brigade's transport sniffs stderr to decide if this client is too old.

        It treats "unrecognized arguments", or any mention of the flag spelling
        `--delegation-id`, as "this client does not accept the flag" and falls
        back. A real refusal must therefore stay a structured refusal on stdout
        and must never put either string on stderr, or a policy denial would be
        silently downgraded into an undelegated dispatch.
        """
        from t3_fleet.errors import PolicyRejected

        for reason in (
            "delegation-not-enrolled",
            "delegation-invalid",
            "delegation-conflict",
            "delegation-repository-unmatched",
        ):
            with self.subTest(reason=reason):
                application = self.application()
                application.controller.submit.side_effect = PolicyRejected(
                    "adopting a fleet delegation requires policy enrollment",
                    reason=reason,
                )
                exit_code, stdout, stderr = self.invoke(
                    [
                        "submit",
                        "--delegation-id",
                        "del-8f2c1a",
                        "--title",
                        "Fix issue 123",
                    ],
                    "private prompt",
                    application=application,
                )
                self.assertEqual(exit_code, 3)
                self.assertEqual(json.loads(stdout)["error"]["reason"], reason)
                self.assertNotIn("--delegation-id", stderr)
                self.assertNotIn("unrecognized arguments", stderr)

    def test_the_flag_brigade_dispatches_with_is_accepted_as_one_argv(self) -> None:
        """The exact submit argv the merged Brigade transport sends."""
        application = self.application()
        exit_code, _, stderr = self.invoke(
            [
                "submit",
                "--delegation-id",
                "del-8f2c1a",
                "--source-revision",
                FULL_SHA,
                "--title",
                "Fix issue 123",
                "--runtime-mode",
                "full-access",
                "--interaction-mode",
                "default",
            ],
            "private prompt",
            application=application,
        )
        self.assertEqual(exit_code, 0)
        self.assertNotIn("unrecognized arguments", stderr)
        request = application.controller.submit.call_args.args[0]
        self.assertEqual(request["delegation_id"], "del-8f2c1a")
        self.assertEqual(request["source_revision"], FULL_SHA)
        self.assertEqual(request["runtime_mode"], "full-access")
        self.assertEqual(request["interaction_mode"], "default")
        # No host and no repository: the receipt names both.
        self.assertIsNone(request.get("host"))
        self.assertIsNone(request.get("repository"))

    def test_a_delegated_result_projection_keeps_the_handoff(self) -> None:
        request_id = "second-worker-controller-a-12345678-1234-1234-1234-123456789abc"
        application = self.application()
        application.controller.submit.return_value = {
            "schema": "t3-fleet.result.v1",
            "request_id": request_id,
            "host": "second-worker",
            "state": "succeeded",
            "thread_id": "thread-1",
            "needs_attention": False,
            "disposition": "pending-handoff",
            "handoff": {
                "target_machine": "second-worker",
                "repository": "git.example.invalid/team/repository",
                "branch": f"codex/{request_id}",
                "source_revision": FULL_SHA,
                "request_id": request_id,
                "worktree_ref": f"t3-fleet:worktree:{request_id}",
            },
        }
        exit_code, stdout, _ = self.invoke(
            ["submit", "--delegation-id", "del-8f2c1a", "--title", "Fix issue 123"],
            "private prompt",
            application=application,
        )
        self.assertEqual(exit_code, 0)
        result = json.loads(stdout)
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(result["disposition"], "pending-handoff")
        self.assertEqual(
            result["handoff"]["worktree_ref"], f"t3-fleet:worktree:{request_id}"
        )


class TickCliTests(unittest.TestCase):
    """`tick` takes nothing and reports one redacted machine-readable result."""

    def invoke(
        self,
        argv: list[str],
        stdin_text: str = "",
        *,
        application: Mock | None = None,
    ) -> tuple[int, str, str]:
        from t3_fleet import cli

        stdout = io.StringIO()
        stderr = io.StringIO()
        app = application or Mock()
        app.close = Mock()
        with patch.object(cli, "_build_application", return_value=app):
            exit_code = cli.main(
                argv,
                stdin=io.StringIO(stdin_text),
                stdout=stdout,
                stderr=stderr,
                environ={},
            )
        return exit_code, stdout.getvalue(), stderr.getvalue()

    def application(self) -> Mock:
        app = Mock()
        app.close = Mock()
        app.controller.tick.return_value = {
            "schema": "t3-fleet.result.v1",
            "action": "tick",
            "state": "eligible",
            "examined": 1,
            "truncated": False,
            "dispatched": [
                {
                    "job_id": "job-" + "0" * 32,
                    "job_state": "dispatched",
                    "queue_disposition": "dispatched",
                    "queue_reason": None,
                    "execution_state": "dispatched",
                    "attempts": 1,
                    "request_id": "req-12345678",
                    "parent_request_id": None,
                    "host": "linux-worker",
                    "repository": "example-repo",
                    "payload_purged": True,
                }
            ],
            "blocked": [],
            "held": [],
            "expired": [],
            "reservations": [],
            "last_tick_at": 1000.0,
            "last_failure_at": None,
            "last_failure_reason": None,
            "counts": {
                "queued": 0,
                "dispatched": 1,
                "held": 0,
                "failed": 0,
                "expired": 0,
            },
        }
        return app

    def test_tick_emits_one_json_result_and_takes_no_arguments(self) -> None:
        application = self.application()
        exit_code, stdout, stderr = self.invoke(["tick"], application=application)
        self.assertEqual(exit_code, 0)
        self.assertEqual(len(stdout.strip().splitlines()), 1)
        result = json.loads(stdout)
        self.assertEqual(result["action"], "tick")
        self.assertEqual(result["dispatched"][0]["request_id"], "req-12345678")
        self.assertEqual(stderr, "")
        application.controller.tick.assert_called_once_with()

    def test_tick_refuses_a_request_field(self) -> None:
        for argv in (
            ["tick", "--request-id", "req-12345678"],
            ["tick", "--repository", "example-repo"],
        ):
            with self.subTest(argv=argv):
                exit_code, stdout, _ = self.invoke(argv, application=self.application())
                self.assertEqual(exit_code, 2)
                self.assertEqual(json.loads(stdout)["error"]["code"], "invalid_request")

    def test_tick_routes_through_invoke_like_every_other_action(self) -> None:
        application = self.application()
        exit_code, stdout, _ = self.invoke(
            ["invoke"],
            json.dumps({"schema": "t3-fleet.request.v1", "action": "tick"}),
            application=application,
        )
        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout)["action"], "tick")

    def test_a_tick_on_a_disabled_queue_is_an_invalid_request(self) -> None:
        application = self.application()
        application.controller.tick.side_effect = ValidationError(
            "the local fleet outbox is disabled on this controller"
        )
        exit_code, stdout, _ = self.invoke(["tick"], application=application)
        self.assertEqual(exit_code, 2)
        self.assertEqual(json.loads(stdout)["error"]["code"], "invalid_request")

    def test_a_queued_submit_is_reported_without_a_thread(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.submit.return_value = {
            "schema": "t3-fleet.result.v1",
            "action": "submit",
            "state": "pending_autotick",
            "needs_attention": False,
            "queue_reason": "capacity-exhausted",
            "job_id": "job-" + "0" * 32,
            "job_state": "queued",
            "queue_disposition": "queued-blocked",
            "execution_state": "never-dispatched",
            "attempts": 0,
            "request_id": None,
            "parent_request_id": None,
            "host": "auto",
            "repository": "example-repo",
            "payload_purged": False,
        }
        payload = {
            "request_id": "req-12345678",
            "host": "auto",
            "repository": "example-repo",
            "source_revision": FULL_SHA,
            "title": "Fix issue 123",
            "prompt": "private prompt",
            "seat": "coder",
            "allow_queue": True,
        }
        exit_code, stdout, _ = self.invoke(
            ["submit"], json.dumps(payload), application=application
        )
        self.assertEqual(exit_code, 0)
        result = json.loads(stdout)
        self.assertEqual(result["state"], "pending_autotick")
        self.assertEqual(result["execution_state"], "never-dispatched")
        self.assertNotIn("thread_id", result)
        self.assertIs(
            application.controller.submit.call_args.args[0]["allow_queue"], True
        )

    def test_the_queue_flag_is_accepted_on_the_human_command_line(self) -> None:
        application = Mock()
        application.close = Mock()
        application.controller.submit.return_value = {
            "schema": "t3-fleet.result.v1",
            "state": "pending_autotick",
            "needs_attention": False,
        }
        with tempfile.TemporaryDirectory() as directory:
            prompt_file = Path(directory) / "prompt.txt"
            prompt_file.write_text("private prompt", encoding="utf-8")
            exit_code, _, _ = self.invoke(
                [
                    "submit",
                    "--request-id",
                    "req-12345678",
                    "--host",
                    "auto",
                    "--repository",
                    "example-repo",
                    "--source-revision",
                    FULL_SHA,
                    "--title",
                    "Fix issue 123",
                    "--seat",
                    "coder",
                    "--prompt-file",
                    str(prompt_file),
                    "--allow-queue",
                ],
                application=application,
            )
        self.assertEqual(exit_code, 0)
        self.assertIs(
            application.controller.submit.call_args.args[0]["allow_queue"], True
        )
