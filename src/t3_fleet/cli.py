from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, TextIO

from .config import FleetConfig, load_config
from .controller import FleetController
from .errors import ConfigError, FleetError, ValidationError
from .store import FleetStore
from .transport import FleetTransport

_MAX_INPUT_BYTES = 1024 * 1024
_MAX_BRIGADE_CALLS_BYTES = 4 * 1024 * 1024
_MAX_PROMPT_CHARACTERS = 120_000
_BRIGADE_ID = re.compile(r"^(?:call|run)-[a-z0-9]{16,64}$")
_APPROVAL_DECISIONS = ("accept", "acceptForSession", "decline", "cancel")
_RUNTIME_MODES = ("approval-required", "auto-accept-edits", "auto", "full-access")
_INTERACTION_MODES = ("default", "plan")

_PUBLIC_ERROR_MESSAGES = {
    "invalid_config": "fleet configuration is invalid",
    "invalid_request": "fleet request is invalid",
    "host_unavailable": "fleet host is unavailable",
    "idempotency_conflict": "fleet request conflicts with an existing request",
    "unknown_state": "fleet request outcome is unknown; run recover",
    "t3_auth_failed": "target authentication failed",
    "t3_http_failed": "target T3 request failed",
    "internal_error": "internal fleet error",
}


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValidationError("invalid command arguments")


@dataclass
class _Application:
    config: FleetConfig
    store: FleetStore
    transport: FleetTransport
    controller: FleetController

    def close(self) -> None:
        return None

    def _hosts(self, host_id: str | None) -> list[Any]:
        if host_id is not None:
            try:
                host = self.config.hosts[host_id]
            except KeyError as error:
                raise ValidationError(f"host is not allowlisted: {host_id}") from error
            return [host]
        return [host for host in self.config.hosts.values() if host.enabled]

    def doctor(self, request: dict[str, Any]) -> dict[str, Any]:
        repository = request.get("repository")
        if repository is not None and repository not in self.config.repositories:
            raise ValidationError("repository is not allowlisted")
        results = []
        for host in self._hosts(request.get("host")):
            if not host.enabled:
                results.append({"host": host.host_id, "state": "disabled"})
                continue
            response = self.transport.call(
                host,
                {
                    "action": "doctor",
                    **({"repository": repository} if repository is not None else {}),
                },
            )
            results.append(
                {
                    "host": host.host_id,
                    "state": response.get("state", "blocked_offline"),
                    "server_version": response.get("server_version"),
                    **(
                        {"project_registered": response["project_registered"]}
                        if "project_registered" in response
                        else {}
                    ),
                    **(
                        {"active": response["active"]}
                        if "active" in response
                        else {}
                    ),
                    **(
                        {"limit": response["limit"]}
                        if "limit" in response
                        else {}
                    ),
                }
            )
        online_states = {"eligible", "at_capacity"}
        overall = (
            "eligible"
            if results and all(item["state"] in online_states for item in results)
            else "blocked_offline"
        )
        return {
            "schema": "t3-fleet.result.v1",
            "action": "doctor",
            "state": overall,
            "hosts": results,
        }

    def capacity(self, request: dict[str, Any]) -> dict[str, Any]:
        results = []
        for host in self._hosts(request.get("host")):
            if not host.enabled:
                results.append(
                    {
                        "host": host.host_id,
                        "state": "disabled",
                        "active": 0,
                        "limit": host.active_limit,
                        "circuit_open": self.store.circuit_is_open(host.host_id),
                    }
                )
                continue
            response = self.transport.call(host, {"action": "capacity"})
            results.append(
                {
                    "host": host.host_id,
                    "state": response.get("state", "blocked_offline"),
                    "active": response.get("active"),
                    "limit": host.active_limit,
                    "circuit_open": self.store.circuit_is_open(host.host_id),
                }
            )
        return {
            "schema": "t3-fleet.result.v1",
            "action": "capacity",
            "state": "eligible",
            "global_limit": self.config.global_active_limit,
            "hosts": results,
        }


def _platform_config_path(environ: Mapping[str, str]) -> Path:
    override = environ.get("T3_FLEET_CONFIG")
    if override:
        return Path(override).expanduser()
    if os.name == "nt":
        root = environ.get("APPDATA")
        return (
            Path(root) if root else Path.home() / "AppData" / "Roaming"
        ) / "t3-fleet" / "config.toml"
    if sys.platform == "darwin":
        return (
            Path.home()
            / "Library"
            / "Application Support"
            / "t3-fleet"
            / "config.toml"
        )
    root = environ.get("XDG_CONFIG_HOME")
    return (
        Path(root) if root else Path.home() / ".config"
    ) / "t3-fleet" / "config.toml"


def _platform_state_path(environ: Mapping[str, str]) -> Path:
    override = environ.get("T3_FLEET_STATE")
    if override:
        return Path(override).expanduser()
    if os.name == "nt":
        root = environ.get("LOCALAPPDATA")
        return (
            Path(root) if root else Path.home() / "AppData" / "Local"
        ) / "t3-fleet" / "fleet.db"
    if sys.platform == "darwin":
        return (
            Path.home()
            / "Library"
            / "Application Support"
            / "t3-fleet"
            / "fleet.db"
        )
    root = environ.get("XDG_STATE_HOME")
    return (
        Path(root) if root else Path.home() / ".local" / "state"
    ) / "t3-fleet" / "fleet.db"


def _build_application(
    config_path: Path,
    state_path: Path | None,
    environ: Mapping[str, str] | None = None,
) -> _Application:
    try:
        config = load_config(config_path)
    except OSError as error:
        raise ConfigError("fleet configuration could not be read") from error
    if state_path is None:
        state_path = (
            Path(config.state_dir).expanduser() / "fleet.db"
            if config.state_dir
            else _platform_state_path(environ if environ is not None else os.environ)
        )
    store = FleetStore(state_path)
    transport = FleetTransport(ssh_command=config.ssh_command)
    return _Application(
        config=config,
        store=store,
        transport=transport,
        controller=FleetController(config, store, transport),
    )


def _parser() -> _Parser:
    parser = _Parser(prog="t3-fleet")
    parser.add_argument("--config")
    parser.add_argument("--state")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("invoke")
    commands.add_parser("brigade-invoke")

    doctor = commands.add_parser("doctor")
    doctor.add_argument("--host")
    doctor.add_argument("--repository")

    capacity = commands.add_parser("capacity")
    capacity.add_argument("--host")

    submit = commands.add_parser("submit")
    _add_submit_flags(submit)

    status = commands.add_parser("status")
    status.add_argument("--request-id")
    status.add_argument("--include-output", action="store_true", default=None)

    wait = commands.add_parser("wait")
    wait.add_argument("--request-id")
    wait.add_argument("--timeout", type=float)
    wait.add_argument("--include-output", action="store_true", default=None)

    followup = commands.add_parser("followup")
    followup.add_argument("--request-id")
    followup.add_argument("--prompt-file")
    followup.add_argument("--runtime-mode", choices=_RUNTIME_MODES)
    followup.add_argument("--interaction-mode", choices=_INTERACTION_MODES)

    interrupt = commands.add_parser("interrupt")
    interrupt.add_argument("--request-id")

    approve = commands.add_parser("approve")
    approve.add_argument("--request-id")
    approve.add_argument("--approval-id")
    approve.add_argument("--decision", choices=_APPROVAL_DECISIONS)

    commands.add_parser("answer")

    recover = commands.add_parser("recover")
    recover.add_argument("--request-id")
    recover.add_argument("--include-output", action="store_true", default=None)
    recover.add_argument("--target")
    return parser


def _brigade_request(environ: Mapping[str, str]) -> dict[str, Any]:
    call_id = environ.get("BRIGADE_TOOL_CALL_ID")
    run_id = environ.get("BRIGADE_TOOL_RUN_ID")
    if (
        not isinstance(call_id, str)
        or not _BRIGADE_ID.fullmatch(call_id)
        or not isinstance(run_id, str)
        or not _BRIGADE_ID.fullmatch(run_id)
    ):
        raise ValidationError("Brigade tool-call context is invalid")
    calls_path = Path.cwd() / ".brigade" / "tools" / "calls.jsonl"
    try:
        if calls_path.stat().st_size > _MAX_BRIGADE_CALLS_BYTES:
            raise ValidationError("Brigade tool-call journal exceeds the size limit")
        lines = calls_path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ValidationError("Brigade tool-call journal is unavailable") from error
    matches: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValidationError("Brigade tool-call journal is invalid") from error
        if (
            isinstance(record, dict)
            and record.get("id") == call_id
            and record.get("run_id") == run_id
        ):
            matches.append(record)
    if len(matches) != 1:
        raise ValidationError("Brigade tool-call context does not match one call")
    record = matches[0]
    contract = record.get("contract")
    if (
        record.get("status") != "running"
        or record.get("tool_id") != "t3-fleet"
        or record.get("family") != "script"
        or record.get("command") != "t3-fleet brigade-invoke"
        or record.get("blockers") != []
        or not isinstance(record.get("approval_fingerprint"), str)
        or not record["approval_fingerprint"]
        or not isinstance(contract, dict)
        or contract.get("approval_mode") != "always"
        or contract.get("approval_required") is not True
    ):
        raise ValidationError("Brigade tool-call authorization is invalid")
    request = record.get("args")
    if not isinstance(request, dict):
        raise ValidationError("Brigade tool-call arguments must be an object")
    if "prompt" in request:
        raise ValidationError("Brigade tool calls cannot store inline prompts")
    action = request.get("action")
    if action in {"submit", "followup"} and not isinstance(
        request.get("prompt_file"), str
    ):
        raise ValidationError("Brigade mutating prompts require prompt_file")
    return request


def _add_submit_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--request-id")
    parser.add_argument("--host")
    parser.add_argument("--repository")
    parser.add_argument("--source-revision")
    parser.add_argument("--title")
    parser.add_argument("--prompt-file")
    parser.add_argument("--model-instance")
    parser.add_argument("--model")
    parser.add_argument("--runtime-mode", choices=_RUNTIME_MODES)
    parser.add_argument("--interaction-mode", choices=_INTERACTION_MODES)
    parser.add_argument("--run-setup-script", action="store_true", default=None)
    parser.add_argument("--allow-queue", action="store_true", default=None)


def _read_stdin(stdin: TextIO) -> str:
    value = stdin.read(_MAX_INPUT_BYTES + 1)
    if len(value.encode("utf-8")) > _MAX_INPUT_BYTES:
        raise ValidationError("standard input exceeds the size limit")
    return value


def _json_request(text: str, *, allow_empty: bool = False) -> dict[str, Any]:
    if not text.strip():
        if allow_empty:
            return {}
        raise ValidationError("JSON input is required")
    try:
        value = json.loads(text)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValidationError("standard input must contain one JSON object") from error
    if not isinstance(value, dict):
        raise ValidationError("standard input must contain one JSON object")
    return value


def _check_action(request: dict[str, Any], command: str) -> None:
    schema = request.pop("schema", "t3-fleet.request.v1")
    if schema != "t3-fleet.request.v1":
        raise ValidationError("unsupported request schema")
    action = request.pop("action", command)
    if action != command:
        raise ValidationError("JSON action does not match the selected command")


def _only(request: dict[str, Any], allowed: set[str]) -> dict[str, Any]:
    unknown = set(request) - allowed
    if unknown:
        raise ValidationError("request contains unsupported fields")
    return request


def _read_prompt(request: dict[str, Any], stdin_text: str, *, human: bool) -> str:
    prompt_file = request.pop("prompt_file", None)
    prompt = request.pop("prompt", None)
    if prompt_file is not None and prompt is not None:
        raise ValidationError("provide prompt text through one input source")
    if prompt_file is not None:
        if not isinstance(prompt_file, str) or not prompt_file:
            raise ValidationError("prompt_file must be a non-empty path")
        try:
            prompt = Path(prompt_file).read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise ValidationError("prompt file could not be read") from error
    elif human:
        prompt = stdin_text
    if not isinstance(prompt, str) or not prompt:
        raise ValidationError(
            "prompt must be supplied through standard input or a prompt file"
        )
    if len(prompt) > _MAX_PROMPT_CHARACTERS:
        raise ValidationError("prompt exceeds the size limit")
    return prompt


def _submit_request(args: argparse.Namespace, stdin: TextIO) -> tuple[dict[str, Any], str]:
    identity_fields = (
        "request_id",
        "host",
        "repository",
        "source_revision",
        "title",
        "model_instance",
        "model",
        "runtime_mode",
        "interaction_mode",
        "run_setup_script",
        "allow_queue",
    )
    human = any(getattr(args, key) is not None for key in identity_fields)
    stdin_text = _read_stdin(stdin)
    if human:
        request = {
            key: getattr(args, key)
            for key in identity_fields
            if getattr(args, key) is not None
        }
        if "model_instance" in request or "model" in request:
            instance_id = request.pop("model_instance", None)
            model = request.pop("model", None)
            if not instance_id or not model:
                raise ValidationError("--model-instance and --model must be used together")
            request["model_selection"] = {"instanceId": instance_id, "model": model}
        if args.prompt_file is not None:
            request["prompt_file"] = args.prompt_file
    else:
        request = _json_request(stdin_text)
        _check_action(request, "submit")
        if args.prompt_file is not None:
            if "prompt_file" in request:
                raise ValidationError("prompt file was provided twice")
            request["prompt_file"] = args.prompt_file
    _only(
        request,
        {
            "request_id",
            "host",
            "repository",
            "source_revision",
            "title",
            "prompt",
            "prompt_file",
            "model_selection",
            "runtime_mode",
            "interaction_mode",
            "base_branch",
            "run_setup_script",
            "allow_queue",
        },
    )
    prompt = _read_prompt(request, stdin_text, human=human)
    return request, prompt


def _command_request(
    args: argparse.Namespace,
    stdin: TextIO,
    fields: tuple[str, ...],
    *,
    allow_empty: bool = False,
) -> dict[str, Any]:
    human = any(getattr(args, key, None) is not None for key in fields)
    if human:
        request = {
            key: getattr(args, key)
            for key in fields
            if getattr(args, key, None) is not None
        }
    else:
        if allow_empty and stdin.isatty():
            request = {}
        else:
            request = _json_request(_read_stdin(stdin), allow_empty=allow_empty)
        _check_action(request, args.command)
    return _only(request, set(fields))


def _dispatch(
    application: _Application,
    args: argparse.Namespace,
    stdin: TextIO,
) -> dict[str, Any]:
    command = args.command
    if command == "invoke":
        stdin_text = _read_stdin(stdin)
        request = _json_request(stdin_text)
        schema = request.get("schema", "t3-fleet.request.v1")
        if schema != "t3-fleet.request.v1":
            raise ValidationError("unsupported request schema")
        action = request.get("action")
        if not isinstance(action, str) or not action or action == "invoke":
            raise ValidationError("JSON action is required")
        routed_args = _parser().parse_args([action])
        return _dispatch(application, routed_args, io.StringIO(stdin_text))
    if command == "submit":
        request, prompt = _submit_request(args, stdin)
        return application.controller.submit(request, prompt=prompt)
    if command == "followup":
        human = (
            args.request_id is not None
            or args.runtime_mode is not None
            or args.interaction_mode is not None
        )
        stdin_text = _read_stdin(stdin)
        if human:
            request = {
                key: getattr(args, key)
                for key in ("request_id", "runtime_mode", "interaction_mode")
                if getattr(args, key) is not None
            }
            if args.prompt_file is not None:
                request["prompt_file"] = args.prompt_file
        else:
            request = _json_request(stdin_text)
            _check_action(request, command)
            if args.prompt_file is not None:
                request["prompt_file"] = args.prompt_file
        _only(
            request,
            {"request_id", "prompt", "prompt_file", "runtime_mode", "interaction_mode"},
        )
        prompt = _read_prompt(request, stdin_text, human=human)
        request_id = _required(request, "request_id")
        runtime_mode = request.get("runtime_mode", "approval-required")
        if runtime_mode not in _RUNTIME_MODES:
            raise ValidationError("unsupported runtime mode")
        interaction_mode = request.get("interaction_mode", "default")
        if interaction_mode not in _INTERACTION_MODES:
            raise ValidationError("unsupported interaction mode")
        return application.controller.followup(
            request_id,
            prompt=prompt,
            runtime_mode=runtime_mode,
            interaction_mode=interaction_mode,
        )
    if command == "doctor":
        return application.doctor(
            _command_request(args, stdin, ("host", "repository"), allow_empty=True)
        )
    if command == "capacity":
        return application.capacity(
            _command_request(args, stdin, ("host",), allow_empty=True)
        )
    if command == "status":
        request = _command_request(args, stdin, ("request_id", "include_output"))
        return application.controller.status(
            _required(request, "request_id"),
            include_output=bool(request.get("include_output", False)),
        )
    if command == "wait":
        request = _command_request(
            args,
            stdin,
            ("request_id", "timeout", "include_output"),
        )
        timeout = request.get("timeout", 3600.0)
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise ValidationError("timeout must be a number")
        return application.controller.wait(
            _required(request, "request_id"),
            timeout=float(timeout),
            include_output=bool(request.get("include_output", False)),
        )
    if command == "interrupt":
        request = _command_request(args, stdin, ("request_id",))
        return application.controller.interrupt(_required(request, "request_id"))
    if command == "approve":
        request = _command_request(
            args,
            stdin,
            ("request_id", "approval_id", "decision"),
        )
        decision = _required(request, "decision")
        if decision not in _APPROVAL_DECISIONS:
            raise ValidationError("unsupported approval decision")
        return application.controller.approve(
            _required(request, "request_id"),
            _required(request, "approval_id"),
            decision,
        )
    if command == "answer":
        request = _command_request(
            args,
            stdin,
            ("request_id", "input_id", "answers"),
        )
        answers = request.get("answers")
        if not isinstance(answers, dict):
            raise ValidationError("answers must be an object")
        return application.controller.answer(
            _required(request, "request_id"),
            _required(request, "input_id"),
            answers,
        )
    if command == "recover":
        request = _command_request(args, stdin, ("request_id", "include_output", "target"))
        target_hint = request.get("target")
        if target_hint is not None and (
            not isinstance(target_hint, str) or not target_hint
        ):
            raise ValidationError("target must be a non-empty string")
        request_id = request.get("request_id")
        if request_id is not None and (
            not isinstance(request_id, str) or not request_id
        ):
            raise ValidationError("request_id must be a non-empty string")
        if request_id is None and target_hint is None:
            raise ValidationError("request_id must be a non-empty string")
        return application.controller.recover(
            request_id,
            include_output=bool(request.get("include_output", False)),
            target_hint=target_hint,
        )
    raise ValidationError("unsupported command")


def _required(request: dict[str, Any], key: str) -> str:
    value = request.get(key)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{key} must be a non-empty string")
    return value


def _emit(stream: TextIO, value: dict[str, Any]) -> None:
    stream.write(json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n")


def _error_result(error: FleetError) -> dict[str, Any]:
    result: dict[str, Any] = {
        "schema": "t3-fleet.error.v1",
        "error": {
            "code": error.code,
            "message": _PUBLIC_ERROR_MESSAGES.get(
                error.code,
                _PUBLIC_ERROR_MESSAGES["internal_error"],
            ),
        },
    }
    request_id = getattr(error, "request_id", None)
    if isinstance(request_id, str) and request_id:
        result["request_id"] = request_id
    return result


def main(
    argv: list[str] | None = None,
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    environ: Mapping[str, str] | None = None,
) -> int:
    stdin = stdin or sys.stdin
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    environ = environ if environ is not None else os.environ
    application: _Application | None = None
    try:
        args = _parser().parse_args(argv)
        config_path = (
            Path(args.config).expanduser()
            if args.config
            else _platform_config_path(environ)
        )
        state_path = (
            Path(args.state).expanduser()
            if args.state
            else Path(environ["T3_FLEET_STATE"]).expanduser()
            if environ.get("T3_FLEET_STATE")
            else None
        )
        application = _build_application(config_path, state_path, environ)
        if args.command == "brigade-invoke":
            request = _brigade_request(environ)
            result = _dispatch(
                application,
                _parser().parse_args(["invoke"]),
                io.StringIO(json.dumps(request, separators=(",", ":"))),
            )
        else:
            result = _dispatch(application, args, stdin)
        _emit(stdout, result)
        return 0
    except FleetError as error:
        _emit(stdout, _error_result(error))
        stderr.write(f"t3-fleet: {error.code}\n")
        return error.exit_code
    except Exception:
        _emit(stdout, _error_result(FleetError("internal error")))
        stderr.write("t3-fleet: internal_error\n")
        return 1
    finally:
        if application is not None:
            application.close()
