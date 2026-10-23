from __future__ import annotations

import contextlib
import json
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator
from typing import Any

from .errors import T3AuthError, T3HttpError
from .t3_rpc import websocket_rpc
from .t3_origin import validate_t3_origin

MAX_RESPONSE_BYTES = 1024 * 1024
MAX_SHELL_RESPONSE_BYTES = 16 * 1024 * 1024
# One thread's projection carries its whole message, run, and activity history,
# so a long thread outgrows the general 1 MiB cap long before the shell does.
MAX_THREAD_RESPONSE_BYTES = 16 * 1024 * 1024
_THREAD_PATH_PREFIX = "/api/orchestration/threads/"
_SUPPORTED_LEGACY = re.compile(r"0\.0\.(?:3[0-9]|40|43|45)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?")
_SUPPORTED_V2 = re.compile(r"(?:0\.0\.46|0\.1\.(?:0|[1-9][0-9]*))(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?")

Runner = Callable[[list[str], float], subprocess.CompletedProcess[str]]
Opener = Callable[..., Any]


def normalize_model_selection(value: object) -> dict[str, Any]:
    """Validate v2 selections without losing or coercing option identity."""
    def valid_string(item: object) -> bool:
        return isinstance(item, str) and bool(item) and item == item.strip()

    if not isinstance(value, dict) or any(not valid_string(value.get(key)) for key in ("instanceId", "model")):
        raise T3HttpError("invalid T3 v2 model selection")
    selection = dict(value)
    if "options" not in selection:
        return selection
    options = selection["options"]
    if isinstance(options, dict):
        entries = list(options.items())
    elif isinstance(options, list):
        if any(not isinstance(item, dict) or set(item) != {"id", "value"} for item in options):
            raise T3HttpError("invalid T3 v2 model options")
        entries = [(item["id"], item["value"]) for item in options]
    else:
        raise T3HttpError("invalid T3 v2 model options")
    normalized: dict[str, Any] = {}
    for key, item in entries:
        if not valid_string(key) or key in normalized or not (type(item) is bool or valid_string(item)):
            raise T3HttpError("invalid or duplicate T3 v2 model option")
        normalized[key] = item
    selection["options"] = normalized
    return selection


def _native_model_selection(value: object) -> dict[str, Any]:
    selection = normalize_model_selection(value)
    if "options" in selection:
        selection["options"] = [{"id": key, "value": item} for key, item in selection["options"].items()]
    return selection


def _response_limit(path: str) -> int:
    if path == "/api/orchestration/shell":
        return MAX_SHELL_RESPONSE_BYTES
    if path.startswith(_THREAD_PATH_PREFIX):
        rest = path[len(_THREAD_PATH_PREFIX):]
        if "/" not in rest or (rest.count("/") == 1 and rest.endswith("/bounded")):
            return MAX_THREAD_RESPONSE_BYTES
    return MAX_RESPONSE_BYTES


def _run(argv: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        shell=False,
    )


class T3SessionIssuer:
    def __init__(
        self,
        t3_command: tuple[str, ...],
        *,
        base_dir: str | None = None,
        runner: Runner = _run,
    ) -> None:
        if not t3_command:
            raise T3AuthError("T3 command is empty")
        self._command = list(t3_command)
        self._base_dir = base_dir
        self._runner = runner

    def _base_args(self) -> list[str]:
        return ["--base-dir", self._base_dir] if self._base_dir else []

    @contextlib.contextmanager
    def session(self) -> Iterator[str]:
        issue_argv = [
            *self._command,
            "auth",
            "session",
            "issue",
            "--ttl",
            "2m",
            "--label",
            "t3-fleet-agent",
            "--subject",
            "t3-fleet-agent",
            "--json",
            *self._base_args(),
        ]
        try:
            completed = self._runner(issue_argv, 15)
        except (OSError, subprocess.SubprocessError) as error:
            raise T3AuthError("could not issue a target-local T3 session") from error
        if completed.returncode != 0:
            raise T3AuthError("T3 rejected target-local session issuance")
        try:
            payload = json.loads(completed.stdout)
            session_id = payload["sessionId"]
            token = payload["token"]
            if not isinstance(session_id, str) or not session_id:
                raise ValueError("missing session id")
            if not isinstance(token, str) or not token:
                raise ValueError("missing token")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            raise T3AuthError("T3 returned an invalid session response") from error

        try:
            yield token
        finally:
            revoke_argv = [
                *self._command,
                "auth",
                "session",
                "revoke",
                session_id,
                *self._base_args(),
            ]
            try:
                self._runner(revoke_argv, 15)
            except (OSError, subprocess.SubprocessError):
                pass


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class T3Client:
    def __init__(
        self,
        base_url: str,
        issuer: T3SessionIssuer,
        *,
        opener: Opener | None = None,
    ) -> None:
        try:
            self._base_url = validate_t3_origin(base_url).rstrip("/") + "/"
        except ValueError:
            raise T3HttpError("invalid T3 origin") from None
        self._issuer = issuer
        self._opener = (
            opener
            if opener is not None
            else urllib.request.build_opener(
                urllib.request.ProxyHandler({}), _NoRedirect()
            ).open
        )
        self._protocol: int | None = None

    def _request(
        self,
        path: str,
        *,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
        authenticated: bool = True,
        timeout: float = 30,
    ) -> Any:
        if not path.startswith("/"):
            raise T3HttpError("T3 API path must be absolute")
        body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        if body is not None and len(body) > MAX_RESPONSE_BYTES:
            raise T3HttpError("T3 API request exceeded size limit")
        response_limit = _response_limit(path)
        protocol = self._protocol if path.startswith("/api/orchestration/") else None
        session = self._issuer.session() if authenticated else contextlib.nullcontext(None)
        with session as token:
            headers = {"Accept": "application/json"}
            if protocol == 2:
                headers["x-t3-orchestration-protocol"] = "2"
            if body is not None:
                headers["Content-Type"] = "application/json"
            if token is not None:
                headers["Authorization"] = f"Bearer {token}"
            url = urllib.parse.urljoin(self._base_url, path.lstrip("/"))
            request = urllib.request.Request(url, data=body, headers=headers, method=method)
            try:
                with self._opener(request, timeout=timeout) as response:
                    raw = response.read(response_limit + 1)
            except urllib.error.HTTPError as error:
                raise T3HttpError(f"T3 API returned HTTP {error.code} for {path}") from error
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                raise T3HttpError(f"T3 API request failed for {path}") from error
            if len(raw) > response_limit:
                raise T3HttpError(f"T3 API response exceeded {response_limit} bytes")
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                raise T3HttpError(f"T3 API returned invalid JSON for {path}") from error

    def descriptor(self) -> dict[str, Any]:
        return self._request(
            "/.well-known/t3/environment",
            authenticated=False,
            timeout=5,
        )

    def get_json(self, path: str) -> Any:
        return self._request(path)

    def post_json(self, path: str, payload: dict[str, Any]) -> Any:
        return self._request(path, method="POST", payload=payload)

    def _orchestration_protocol(self) -> int:
        if self._protocol is not None:
            return self._protocol
        descriptor = self.descriptor()
        if not isinstance(descriptor, dict) or not isinstance(descriptor.get("serverVersion"), str):
            raise T3HttpError("T3 returned an invalid environment descriptor")
        version = descriptor["serverVersion"]
        protocol = descriptor.get("orchestrationProtocolVersion")
        if _SUPPORTED_V2.fullmatch(version) and type(protocol) is int and protocol == 2:
            self._protocol = 2
        elif _SUPPORTED_LEGACY.fullmatch(version) and (protocol is None or type(protocol) is int and protocol == 1):
            self._protocol = 1
        else:
            raise T3HttpError("unsupported T3 orchestration version")
        return self._protocol

    def snapshot(self) -> dict[str, Any]:
        if self._orchestration_protocol() == 2:
            raise T3HttpError("full snapshot is unsupported by T3 orchestration protocol 2")
        return self._request("/api/orchestration/snapshot")

    def shell_snapshot(self) -> dict[str, Any]:
        protocol = self._orchestration_protocol()
        result = self._request("/api/orchestration/shell")
        if protocol == 2 and (not isinstance(result, dict) or type(result.get("schemaVersion")) is not int or result["schemaVersion"] != 2):
            raise T3HttpError("T3 returned a mismatched orchestration shell schema")
        if protocol == 2:
            result = dict(result)
            for key in ("threads", "archivedThreads", "projects"):
                if key not in result:
                    continue
                if not isinstance(result[key], list) or any(not isinstance(item, dict) for item in result[key]):
                    raise T3HttpError("invalid T3 v2 shell records")
                field = "defaultModelSelection" if key == "projects" else "modelSelection"
                result[key] = [
                    {**item, field: normalize_model_selection(item[field])}
                    if field in item and item[field] is not None else dict(item)
                    for item in result[key]
                ]
        return result

    def thread_snapshot(self, thread_id: str) -> dict[str, Any]:
        protocol = self._orchestration_protocol()
        encoded = urllib.parse.quote(thread_id, safe="")
        result = self._request(f"/api/orchestration/threads/{encoded}")
        if protocol == 1:
            return result
        if not isinstance(result, dict) or type(result.get("snapshotSequence")) is not int or result["snapshotSequence"] < 0:
            raise T3HttpError("invalid T3 v2 thread snapshot")
        projection = result.get("projection")
        if not isinstance(projection, dict) or not isinstance(projection.get("thread"), dict) or projection["thread"].get("id") != thread_id:
            raise T3HttpError("invalid T3 v2 thread projection")
        for key in ("runs", "messages", "runtimeRequests"):
            if not isinstance(projection.get(key), list) or any(not isinstance(item, dict) for item in projection[key]):
                raise T3HttpError("invalid T3 v2 thread projection records")
        for run in projection["runs"]:
            if not isinstance(run.get("id"), str) or not run["id"] or not isinstance(run.get("status"), str) or run["status"] not in {"preparing", "queued", "starting", "running", "waiting", "completed", "interrupted", "failed", "cancelled", "rolled_back"}:
                raise T3HttpError("invalid T3 v2 run")
        for request in projection["runtimeRequests"]:
            capability = request.get("responseCapability")
            if (not isinstance(request.get("id"), str) or not request["id"]
                or not isinstance(request.get("status"), str) or request["status"] not in {"pending", "resolved", "expired", "cancelled"}
                or not isinstance(request.get("kind"), str) or request["kind"] not in {"command", "file-read", "file-change", "mcp-elicitation", "permission", "dynamic_tool_call", "user_input", "auth_refresh"}
                or not isinstance(capability, dict) or not isinstance(capability.get("type"), str) or capability["type"] not in {"live", "message", "not_resumable"}):
                raise T3HttpError("invalid T3 v2 runtime request")
            key = "providerSessionId" if capability["type"] == "live" else "reason" if capability["type"] == "not_resumable" else None
            if key is not None and (not isinstance(capability.get(key), str) or not capability[key]):
                raise T3HttpError("invalid T3 v2 runtime request capability")
        for key in ("providerThreads", "nodes", "subagents"):
            if key in projection and (not isinstance(projection[key], list) or any(not isinstance(item, dict) for item in projection[key])):
                raise T3HttpError("invalid T3 v2 activity records")
        for provider_thread in projection.get("providerThreads", []):
            background = provider_thread.get("pendingBackgroundTasks", [])
            if not isinstance(background, list) or any(not isinstance(item, dict) or not isinstance(item.get("taskId"), str) or not item["taskId"] for item in background):
                raise T3HttpError("invalid T3 v2 provider background work")
        # Keep the native projection and expose its records at the established
        # thread boundary. No v1 session/latestTurn or activities are invented.
        detail = {**projection["thread"], **{key: projection[key] for key in ("runs", "messages", "runtimeRequests")},
                  **{key: projection[key] for key in ("providerThreads", "nodes", "subagents") if key in projection}}
        detail["modelSelection"] = normalize_model_selection(detail.get("modelSelection"))
        return {**result, "thread": detail}

    def pending_request_item(self, thread_id: str, request_id: str) -> dict[str, Any] | None:
        """The turn item that carries a pending request's question or prompt.

        The thread detail lists runtime requests without their content. T3
        keeps the questions on a `user_input_request` turn item and an
        approval's prompt on an `approval_request` item, which only the
        bounded thread snapshot returns. Protocol 1 has no such route.
        """
        if self._orchestration_protocol() != 2:
            return None
        encoded = urllib.parse.quote(thread_id, safe="")
        result = self._request(f"/api/orchestration/threads/{encoded}/bounded")
        projection = result.get("projection") if isinstance(result, dict) else None
        if (
            not isinstance(projection, dict)
            or not isinstance(projection.get("thread"), dict)
            or projection["thread"].get("id") != thread_id
        ):
            raise T3HttpError("invalid T3 v2 bounded thread projection")
        items: list[Any] = []
        if isinstance(projection.get("turnItems"), list):
            items.extend(projection["turnItems"])
        if isinstance(projection.get("visibleTurnItems"), list):
            items.extend(
                entry.get("item") for entry in projection["visibleTurnItems"] if isinstance(entry, dict)
            )
        for item in items:
            if (
                isinstance(item, dict)
                and item.get("requestId") == request_id
                and item.get("type") in {"user_input_request", "approval_request"}
            ):
                return item
        return None

    def _rpc_command(self, command: dict[str, Any]) -> dict[str, Any]:
        with self._issuer.session() as token:
            result = websocket_rpc(self._base_url, token, "orchestration.dispatchCommand", command, 30)
        if not isinstance(result, dict) or type(result.get("sequence")) is not int or result["sequence"] < 0:
            raise T3HttpError("invalid T3 dispatch acknowledgement")
        return result

    def dispatch(self, command: dict[str, Any]) -> dict[str, Any]:
        if self._orchestration_protocol() == 1:
            return self._request("/api/orchestration/dispatch", method="POST", payload=command)
        kind = command.get("type")
        common = {"commandId": command["commandId"], "threadId": command["threadId"]}
        if kind == "thread.create":
            translated = {"type": kind, **common, "createdBy": "agent", "creationSource": "server",
                          **{key: command[key] for key in ("projectId", "title", "modelSelection", "runtimeMode", "interactionMode", "branch", "worktreePath")}}
        elif kind == "thread.meta.update":
            translated = {"type": "thread.metadata.update", **common,
                          "branch": command["branch"], "worktreePath": command["worktreePath"],
                          "expectedWorktreePath": None, "expectedEmpty": True}
        elif kind == "thread.turn.start":
            detail = self.thread_snapshot(command["threadId"])["thread"]
            if any(detail.get(key) != command[key] for key in ("runtimeMode", "interactionMode", "modelSelection")):
                raise T3HttpError("T3 v2 continuation mode or model changes are unsupported")
            message = command["message"]
            translated = {"type": "message.dispatch", **common, "createdBy": "agent", "creationSource": "server",
                          "messageId": message["messageId"], "text": message["text"],
                          "attachments": message["attachments"], "modelSelection": command["modelSelection"],
                          "dispatchMode": {"type": "start_immediately"}}
        elif kind in {"thread.approval.respond", "thread.user-input.respond"}:
            translated = {"type": "runtime-request.respond", **common, "requestId": command["requestId"]}
            key = "decision" if kind == "thread.approval.respond" else "answers"
            translated[key] = command[key]
        elif kind == "thread.archive":
            translated = {"type": kind, **common}
        elif kind == "thread.settle":
            translated = {"type": kind, **common}
            if "settledAt" in command:
                translated["settledAt"] = command["settledAt"]
        elif kind == "thread.pull-request.unlink":
            translated = {"type": kind, **common,
                          **{key: command[key] for key in ("host", "repository", "number")}}
        elif kind in {"run.interrupt", "queued-run.cancel"}:
            translated = {"type": kind, **common, "runId": command["runId"]}
            if kind == "run.interrupt":
                translated["holdQueue"] = True
        else:
            raise T3HttpError("unsupported T3 protocol 2 dispatch command")
        if "modelSelection" in translated:
            translated["modelSelection"] = _native_model_selection(translated["modelSelection"])
        return self._rpc_command(translated)
