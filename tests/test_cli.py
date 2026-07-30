from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from t3_fleet.errors import HostUnavailable, ValidationError


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
            "dispatch outcome is unknown for request req-12345678; run recover"
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
        self.assertIn("2>$null", script)
        self.assertNotIn("Write-Error", script)

    def test_launcher_passes_through_conflict_and_unknown_codes(self) -> None:
        script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "t3-fleet-agent.ps1"
        ).read_text(encoding="utf-8")

        self.assertIn('"idempotency_conflict",', script)
        self.assertIn('"unknown_state",', script)
        self.assertIn("exit $AgentExitCode", script)
