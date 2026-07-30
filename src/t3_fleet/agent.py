from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, TextIO

from .config import FleetConfig, load_config
from .errors import ConfigError, FleetError, UnknownState, ValidationError
from .t3_api import T3Client, T3SessionIssuer
from .target_store import TargetClaim, TargetReceipt, TargetStore, receipt_payload

_REQUEST_ID = re.compile(r"^[a-z0-9][a-z0-9-]{7,63}$")
_FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
_PEER_REQUEST_ID = re.compile(
    r"^[a-z0-9][a-z0-9-]{0,63}-[a-z0-9][a-z0-9-]{0,63}-"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_RUNTIME_MODES = {"approval-required", "auto-accept-edits", "auto", "full-access"}
_INTERACTION_MODES = {"default", "plan"}
_APPROVAL_DECISIONS = {"accept", "acceptForSession", "decline", "cancel"}
# Anchored SemVer-like validation for the staged cohort: base must be exactly
# 0.0.30, 0.0.31, or 0.0.32, optionally followed by a non-empty dot-separated
# ASCII alphanumeric/hyphen prerelease (after '-') and/or build (after '+')
# section. Rejects empty suffixes, whitespace, punctuation, and outside-cohort
# bases.
_SUPPORTED_T3_VERSION = re.compile(
    r"^(?:0\.0\.30|0\.0\.31|0\.0\.32)"
    r"(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)
_MISSING_THREAD_GRACE_SECONDS = 30
_RESERVATION_SECONDS = 24 * 60 * 60
# OrchestrationLatestTurnState schema is exactly running/interrupted/completed/error.
# A session.status in starting/running with a terminal latestTurn.state is a stale
# provider session (e.g. after thread.turn.interrupt + thread.session.stop) that
# no longer represents live work and must not consume a host slot.
_TERMINAL_TURN_STATES = {"interrupted", "completed", "error"}

GitRunner = Callable[[list[str], float], subprocess.CompletedProcess[str]]
_WORKTREE_TIMEOUT = 30
_WORKTREE_CLEANUP_TIMEOUT = 15


def _git_run(argv: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        shell=False,
    )


class T3Protocol(Protocol):
    def descriptor(self) -> dict[str, Any]: ...

    def snapshot(self) -> dict[str, Any]: ...

    def shell_snapshot(self) -> dict[str, Any]: ...

    def thread_snapshot(self, thread_id: str) -> dict[str, Any]: ...

    def dispatch(self, command: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True)
class RepositoryPolicy:
    path: str
    default_branch: str
    branch_prefix: str


@dataclass(frozen=True)
class AgentPolicy:
    host_id: str
    expected_hostname: str
    platform: str
    repositories: dict[str, RepositoryPolicy]
    state_dir: Path
    active_limit: int
    coordination_mode: str = "single-controller"
    target_store_path: Path | None = None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _uuid() -> str:
    return str(uuid.uuid4())


def _system_platform() -> str:
    return sys.platform


class T3Agent:
    def __init__(
        self,
        policy: AgentPolicy,
        t3: T3Protocol,
        *,
        hostname: Callable[[], str] = socket.gethostname,
        platform: Callable[[], str] = _system_platform,
        uuid_factory: Callable[[], str] = _uuid,
        now: Callable[[], str] = _utc_now,
        git_runner: GitRunner = _git_run,
    ) -> None:
        self.policy = policy
        self.t3 = t3
        self._hostname = hostname
        self._platform = platform
        self._uuid = uuid_factory
        self._now = now
        self._git_runner = git_runner
        self._store: TargetStore | None = None

    def _worktree_path(self, request_id: str) -> str:
        return str(self.policy.state_dir / "worktrees" / request_id)

    def _create_worktree(
        self,
        *,
        repository_path: str,
        worktree_path: str,
        branch: str,
        source_revision: str,
        default_branch: str,
    ) -> None:
        Path(worktree_path).parent.mkdir(parents=True, exist_ok=True)
        argv = [
            "git",
            "-C",
            repository_path,
            "worktree",
            "add",
            "-b",
            branch,
            worktree_path,
            source_revision,
        ]
        try:
            completed = self._git_runner(argv, _WORKTREE_TIMEOUT)
        except (OSError, subprocess.SubprocessError) as error:
            raise UnknownState("target worktree creation failed") from error
        if completed.returncode != 0:
            raise UnknownState("target worktree creation failed")
        config_argv = [
            "git",
            "-C",
            repository_path,
            "config",
            f"branch.{branch}.gh-merge-base",
            default_branch,
        ]
        try:
            completed = self._git_runner(config_argv, _WORKTREE_TIMEOUT)
        except (OSError, subprocess.SubprocessError) as error:
            self._remove_worktree(
                repository_path=repository_path,
                worktree_path=worktree_path,
            )
            raise UnknownState("target worktree configuration failed") from error
        if completed.returncode != 0:
            self._remove_worktree(
                repository_path=repository_path,
                worktree_path=worktree_path,
            )
            raise UnknownState("target worktree configuration failed")

    def _remove_worktree(self, *, repository_path: str, worktree_path: str) -> None:
        argv = [
            "git",
            "-C",
            repository_path,
            "worktree",
            "remove",
            "--force",
            worktree_path,
        ]
        try:
            self._git_runner(argv, _WORKTREE_CLEANUP_TIMEOUT)
        except (OSError, subprocess.SubprocessError):
            pass

    def _check_identity(self) -> None:
        live = self._hostname()
        if live.casefold() != self.policy.expected_hostname.casefold():
            raise ValidationError(f"host identity mismatch for {self.policy.host_id}")
        live_platform = self._platform().casefold()
        platform_matches = (
            self.policy.platform == "linux" and live_platform.startswith("linux")
        ) or (
            self.policy.platform == "windows" and live_platform.startswith("win")
        )
        if not platform_matches:
            raise ValidationError(f"platform mismatch for {self.policy.host_id}")

    @staticmethod
    def _required_string(request: dict[str, Any], key: str) -> str:
        value = request.get(key)
        if not isinstance(value, str) or not value:
            raise ValidationError(f"{key} must be a non-empty string")
        return value

    def _repository(self, request: dict[str, Any]) -> tuple[str, RepositoryPolicy]:
        repository_id = self._required_string(request, "repository")
        try:
            return repository_id, self.policy.repositories[repository_id]
        except KeyError as error:
            raise ValidationError(f"repository is not allowlisted: {repository_id}") from error

    def _project(
        self,
        repository: RepositoryPolicy,
        *,
        shell: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        snapshot = shell if shell is not None else self.t3.shell_snapshot()
        projects = [
            project
            for project in snapshot.get("projects", [])
            if project.get("workspaceRoot") == repository.path
        ]
        if len(projects) != 1:
            raise ValidationError("T3 project mapping requires exactly one matching workspace root")
        return projects[0]

    def _write_receipt(
        self,
        *,
        action: str,
        request_id: str | None,
        thread_id: str | None,
        sequence: int | None,
        prompt: str | None,
        outcome: str,
    ) -> None:
        self.policy.state_dir.mkdir(parents=True, exist_ok=True)
        key = request_id or thread_id or self._uuid()
        safe_key = self._safe_key(key)
        receipt = {
            "schema": "t3-fleet.agent-receipt.v1",
            "action": action,
            "request_id": request_id,
            "thread_id": thread_id,
            "sequence": sequence,
            "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest() if prompt is not None else None,
            "prompt_bytes": len(prompt.encode()) if prompt is not None else 0,
            "outcome": outcome,
            "created_at": self._now(),
        }
        destination = self.policy.state_dir / f"{safe_key}-{action}.json"
        temporary = destination.with_suffix(".tmp")
        temporary.write_text(json.dumps(receipt, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, destination)

    @staticmethod
    def _safe_key(value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9-]", "-", value)[:80]

    def _submit_receipt(self, request_id: str) -> dict[str, Any]:
        if not _REQUEST_ID.fullmatch(request_id):
            raise ValidationError("request_id must match the fleet request-id pattern")
        receipt_path = self.policy.state_dir / f"{self._safe_key(request_id)}-submit.json"
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValidationError(f"target receipt not found for request {request_id}") from error
        if (
            not isinstance(receipt, dict)
            or receipt.get("request_id") != request_id
            or not isinstance(receipt.get("thread_id"), str)
            or not receipt["thread_id"]
        ):
            raise ValidationError(f"target receipt is invalid for request {request_id}")
        return receipt

    @staticmethod
    def _age_seconds(created_at: str, now: str) -> float:
        try:
            created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            current = datetime.fromisoformat(now.replace("Z", "+00:00"))
        except (TypeError, ValueError) as error:
            raise ValidationError("target receipt has an invalid timestamp") from error
        return max(0.0, (current - created).total_seconds())

    @staticmethod
    def _thread_detail(snapshot: Any) -> dict[str, Any] | None:
        if not isinstance(snapshot, dict):
            return None
        thread = snapshot.get("thread")
        if not isinstance(thread, dict):
            return None
        return thread

    def _status(self, thread_id: str, *, include_output: bool = False) -> dict[str, Any]:
        shell = self.t3.shell_snapshot()
        matches = [thread for thread in shell.get("threads", []) if thread.get("id") == thread_id]
        if len(matches) != 1:
            raise ValidationError(f"T3 thread not found: {thread_id}")
        thread = matches[0]
        pending = bool(thread.get("hasPendingApprovals") or thread.get("hasPendingUserInput"))
        latest = thread.get("latestTurn") or {}
        latest_state = latest.get("state")
        session = thread.get("session") or {}
        session_state = session.get("status")
        detail: dict[str, Any] | None = None
        if latest_state == "running" and session_state not in {"starting", "running"}:
            detail = self._thread_detail(self.t3.thread_snapshot(thread_id))
        if latest_state == "interrupted" or session_state == "stopped":
            state = "interrupted"
        elif pending:
            state = "needs_attention"
        elif latest_state == "completed":
            state = "succeeded"
        elif latest_state == "running" and session_state not in {"starting", "running"}:
            state = "unknown"
            if detail is not None:
                detail_latest = detail.get("latestTurn")
                shell_turn_id = latest.get("turnId")
                if (
                    isinstance(detail_latest, dict)
                    and detail_latest.get("state") == "completed"
                    and isinstance(shell_turn_id, str)
                    and shell_turn_id
                    and shell_turn_id == detail_latest.get("turnId")
                ):
                    state = "succeeded"
        elif latest_state == "error" or session_state == "error":
            state = "failed"
        elif latest_state is None and session_state not in {"starting", "running"}:
            state = "unknown"
        else:
            state = "running"
        result: dict[str, Any] = {
            "schema": "t3-fleet.agent-result.v1",
            "host": self.policy.host_id,
            "state": state,
            "thread_id": thread_id,
            "needs_attention": state == "needs_attention",
            "updated_at": thread.get("updatedAt"),
        }
        if include_output:
            if detail is None:
                detail = self._thread_detail(self.t3.thread_snapshot(thread_id))
            messages = [
                message.get("text", "")
                for message in (detail or {}).get("messages", [])
                if message.get("role") == "assistant"
                and not message.get("streaming")
                and isinstance(message.get("text"), str)
            ]
            if messages:
                result["output"] = messages[-1][-16_384:]
        return result

    @dataclass(frozen=True)
    class _SubmitEnvelope:
        repository_id: str
        repository: RepositoryPolicy
        project: dict[str, Any]
        source_revision: str
        title: str
        prompt: str
        runtime_mode: str
        interaction_mode: str
        model_selection: dict[str, Any]

    def _prepare_submit_envelope(
        self,
        request: dict[str, Any],
    ) -> _SubmitEnvelope:
        repository_id, repository = self._repository(request)
        source_revision = self._required_string(request, "source_revision")
        if not _FULL_SHA.fullmatch(source_revision):
            raise ValidationError("source_revision must be a full lowercase commit SHA")
        title = self._required_string(request, "title")
        prompt = self._required_string(request, "prompt")
        if len(prompt) > 120_000:
            raise ValidationError("prompt exceeds T3 input limit")
        runtime_mode = request.get("runtime_mode", "approval-required")
        interaction_mode = request.get("interaction_mode", "default")
        if runtime_mode not in _RUNTIME_MODES:
            raise ValidationError("unsupported runtime_mode")
        if interaction_mode not in _INTERACTION_MODES:
            raise ValidationError("unsupported interaction_mode")
        if request.get("run_setup_script", False):
            raise ValidationError(
                "run_setup_script is unavailable through the T3 HTTP dispatch fallback"
            )
        project = self._project(repository)
        model_selection = request.get("model_selection") or project.get("defaultModelSelection")
        if not isinstance(model_selection, dict):
            raise ValidationError("no T3 model selection is configured for this project")
        return self._SubmitEnvelope(
            repository_id=repository_id,
            repository=repository,
            project=project,
            source_revision=source_revision,
            title=title,
            prompt=prompt,
            runtime_mode=runtime_mode,
            interaction_mode=interaction_mode,
            model_selection=model_selection,
        )

    def _submit(
        self,
        request: dict[str, Any],
        *,
        envelope: _SubmitEnvelope | None = None,
        on_thread_allocated: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        request_id = self._required_string(request, "request_id")
        if not (
            _REQUEST_ID.fullmatch(request_id)
            or _PEER_REQUEST_ID.fullmatch(request_id)
        ):
            raise ValidationError("invalid request_id")
        if envelope is None:
            envelope = self._prepare_submit_envelope(request)

        command_id = self._uuid()
        thread_id = self._uuid()
        if on_thread_allocated is not None:
            on_thread_allocated(thread_id)
        message_id = self._uuid()
        created_at = self._now()
        branch = f"{envelope.repository.branch_prefix}{request_id}"
        worktree_path = self._worktree_path(request_id)
        create_command = {
            "type": "thread.create",
            "commandId": self._uuid(),
            "threadId": thread_id,
            "projectId": envelope.project["id"],
            "title": envelope.title,
            "modelSelection": envelope.model_selection,
            "runtimeMode": envelope.runtime_mode,
            "interactionMode": envelope.interaction_mode,
            "branch": None,
            "worktreePath": None,
            "createdAt": created_at,
        }
        update_command = {
            "type": "thread.meta.update",
            "commandId": self._uuid(),
            "threadId": thread_id,
            "branch": branch,
            "worktreePath": worktree_path,
        }
        turn_command = {
            "type": "thread.turn.start",
            "commandId": command_id,
            "threadId": thread_id,
            "message": {
                "messageId": message_id,
                "role": "user",
                "text": envelope.prompt,
                "attachments": [],
            },
            "modelSelection": envelope.model_selection,
            "runtimeMode": envelope.runtime_mode,
            "interactionMode": envelope.interaction_mode,
            "createdAt": created_at,
        }
        self._write_receipt(
            action="submit",
            request_id=request_id,
            thread_id=thread_id,
            sequence=None,
            prompt=envelope.prompt,
            outcome="dispatching",
        )
        created_worktree = False
        turn_dispatch_started = False
        try:
            self.t3.dispatch(create_command)
            self._create_worktree(
                repository_path=envelope.repository.path,
                worktree_path=worktree_path,
                branch=branch,
                source_revision=envelope.source_revision,
                default_branch=envelope.repository.default_branch,
            )
            created_worktree = True
            self.t3.dispatch(update_command)
            turn_dispatch_started = True
            response = self.t3.dispatch(turn_command)
        except Exception:
            if created_worktree and not turn_dispatch_started:
                self._remove_worktree(
                    repository_path=envelope.repository.path,
                    worktree_path=worktree_path,
                )
            self._write_receipt(
                action="submit",
                request_id=request_id,
                thread_id=thread_id,
                sequence=None,
                prompt=envelope.prompt,
                outcome="unknown",
            )
            raise
        sequence = response.get("sequence")
        result = {
            "schema": "t3-fleet.agent-result.v1",
            "host": self.policy.host_id,
            "request_id": request_id,
            "state": "running",
            "thread_id": thread_id,
            "sequence": sequence,
            "needs_attention": False,
            "updated_at": created_at,
        }
        self._write_receipt(
            action="submit",
            request_id=request_id,
            thread_id=thread_id,
            sequence=sequence,
            prompt=envelope.prompt,
            outcome="accepted",
        )
        return result

    def _target_store(self) -> TargetStore:
        if self._store is None:
            path = self.policy.target_store_path or (self.policy.state_dir / "target.db")
            self._store = TargetStore(path, now=self._now)
        return self._store

    def _validate_claim_request_id(self, request_id: str) -> None:
        if not _PEER_REQUEST_ID.fullmatch(request_id):
            raise ValidationError("claim request_id must be a target-qualified peer id")
        if not request_id.startswith(f"{self.policy.host_id}-"):
            raise ValidationError("request_id target prefix does not match this target")

    def _receipt_or_migrate(self, request_id: str) -> TargetReceipt | None:
        store = self._target_store()
        receipt = store.lookup(request_id)
        if receipt is not None:
            return receipt
        legacy_path = self.policy.state_dir / f"{self._safe_key(request_id)}-submit.json"
        if not legacy_path.exists():
            return None
        repositories = self.policy.repositories
        inferred = next(iter(repositories)) if len(repositories) == 1 else None
        return store.migrate_legacy_receipt(
            legacy_path,
            target_id=self.policy.host_id,
            repository=inferred,
            source_revision=None,
        )

    def _claim(self, request: dict[str, Any]) -> dict[str, Any]:
        request_id = self._required_string(request, "request_id")
        self._validate_claim_request_id(request_id)
        digest = self._required_string(request, "digest")
        if not _SHA256.fullmatch(digest):
            raise ValidationError("digest must be a lowercase sha256 hex digest")
        envelope = self._prepare_submit_envelope(request)
        store = self._target_store()
        receipt = store.claim(
            TargetClaim(
                request_id=request_id,
                digest=digest,
                target_id=self.policy.host_id,
                repository=envelope.repository_id,
                source_revision=envelope.source_revision,
                prompt_sha256=hashlib.sha256(envelope.prompt.encode()).hexdigest(),
                prompt_bytes=len(envelope.prompt.encode()),
            ),
            active_limit=self.policy.active_limit,
            reservation_ttl=_RESERVATION_SECONDS,
        )
        if receipt.thread_id is not None:
            return receipt_payload(receipt)
        try:
            self._submit(
                request,
                envelope=envelope,
                on_thread_allocated=lambda thread_id: store.attach_thread(request_id, thread_id),
            )
        except Exception:
            current = store.lookup(request_id)
            if current is not None and current.thread_id is None:
                store.release(request_id)
            raise
        confirmed = store.lookup(request_id)
        if confirmed is None:
            raise UnknownState(f"claim outcome is unknown for request {request_id}")
        return receipt_payload(confirmed)

    def _lookup(self, request: dict[str, Any]) -> dict[str, Any]:
        request_id = self._required_string(request, "request_id")
        receipt = self._receipt_or_migrate(request_id)
        if receipt is None:
            return {"schema": "t3-fleet.agent-result.v1", "state": "not_found"}
        return receipt_payload(receipt)

    def _reconcile(self, request: dict[str, Any]) -> dict[str, Any]:
        request_id = self._required_string(request, "request_id")
        store = self._target_store()
        receipt = self._receipt_or_migrate(request_id)
        if receipt is None:
            return {"schema": "t3-fleet.agent-result.v1", "state": "not_found"}
        if receipt.thread_id is None:
            return receipt_payload(receipt)
        try:
            result = self._status(
                receipt.thread_id,
                include_output=bool(request.get("include_output", False)),
            )
        except ValidationError as error:
            if not str(error).startswith("T3 thread not found:"):
                raise
            age = self._age_seconds(receipt.created_at, self._now())
            state = "failed" if age >= _MISSING_THREAD_GRACE_SECONDS else "unknown"
            updated = store.reconcile(request_id, state=state, thread_id=receipt.thread_id)
            return receipt_payload(updated)
        updated = store.reconcile(request_id, state=result["state"], thread_id=receipt.thread_id)
        payload = receipt_payload(updated)
        if "output" in result:
            payload["output"] = result["output"]
        return payload

    def _reserve_followup(self, request: dict[str, Any]) -> dict[str, Any]:
        request_id = self._required_string(request, "request_id")
        receipt = self._target_store().reserve_followup(
            request_id,
            active_limit=self.policy.active_limit,
            reservation_ttl=_RESERVATION_SECONDS,
        )
        return receipt_payload(receipt)

    def _release(self, request: dict[str, Any]) -> dict[str, Any]:
        request_id = self._required_string(request, "request_id")
        self._target_store().release(request_id)
        return {
            "schema": "t3-fleet.agent-result.v1",
            "request_id": request_id,
            "state": "released",
        }

    def _followup(self, request: dict[str, Any]) -> dict[str, Any]:
        request_id = self._required_string(request, "request_id")
        thread_id = self._required_string(request, "thread_id")
        prompt = self._required_string(request, "prompt")
        runtime_mode = request.get("runtime_mode", "approval-required")
        interaction_mode = request.get("interaction_mode", "default")
        if runtime_mode not in _RUNTIME_MODES:
            raise ValidationError("unsupported runtime_mode")
        if interaction_mode not in _INTERACTION_MODES:
            raise ValidationError("unsupported interaction_mode")
        created_at = self._now()
        command = {
            "type": "thread.turn.start",
            "commandId": self._uuid(),
            "threadId": thread_id,
            "message": {
                "messageId": self._uuid(),
                "role": "user",
                "text": prompt,
                "attachments": [],
            },
            "runtimeMode": runtime_mode,
            "interactionMode": interaction_mode,
            "createdAt": created_at,
        }
        response = self.t3.dispatch(command)
        self._write_receipt(
            action="followup",
            request_id=request_id,
            thread_id=thread_id,
            sequence=response.get("sequence"),
            prompt=prompt,
            outcome="accepted",
        )
        return {
            "schema": "t3-fleet.agent-result.v1",
            "host": self.policy.host_id,
            "request_id": request_id,
            "state": "running",
            "thread_id": thread_id,
            "sequence": response.get("sequence"),
            "needs_attention": False,
            "updated_at": created_at,
        }

    def _interrupt(self, request: dict[str, Any]) -> dict[str, Any]:
        thread_id = self._required_string(request, "thread_id")
        created_at = self._now()
        # T3 cannot settle a turn while open approval.requested activities remain
        # unresolved: thread.turn.interrupt plus thread.session.stop leave the
        # turn running with a stale pending marker. The operator already
        # authorized stopping the active turn, so cancel every currently open
        # approval before interrupting. Never accept the requested tool action.
        snapshot = self.t3.thread_snapshot(thread_id).get("thread", {})
        for approval_id in sorted(self._open_approval_ids(snapshot)):
            self.t3.dispatch(
                {
                    "type": "thread.approval.respond",
                    "commandId": self._uuid(),
                    "threadId": thread_id,
                    "requestId": approval_id,
                    "decision": "cancel",
                    "createdAt": created_at,
                }
            )
        # T3 ProjectionPipeline's "thread.turn-interrupt-requested" handler
        # returns early when event.payload.turnId is undefined, leaving
        # latestTurn projected as running. Extract the active orchestration
        # turn id from the same snapshot (latestTurn.turnId) and include it
        # so the projection settles the turn. Omit turnId when the snapshot
        # does not expose one, preserving the prior command shape.
        interrupt_command: dict[str, Any] = {
            "type": "thread.turn.interrupt",
            "commandId": self._uuid(),
            "threadId": thread_id,
            "createdAt": created_at,
        }
        latest_turn = snapshot.get("latestTurn")
        if isinstance(latest_turn, dict):
            turn_id = latest_turn.get("turnId")
            if isinstance(turn_id, str) and turn_id:
                interrupt_command["turnId"] = turn_id
        response = self.t3.dispatch(interrupt_command)
        stop_response = self.t3.dispatch(
            {
                "type": "thread.session.stop",
                "commandId": self._uuid(),
                "threadId": thread_id,
                "createdAt": created_at,
            }
        )
        return {
            "schema": "t3-fleet.agent-result.v1",
            "host": self.policy.host_id,
            "state": "running",
            "thread_id": thread_id,
            "sequence": stop_response.get("sequence", response.get("sequence")),
            "needs_attention": False,
            "updated_at": created_at,
        }

    @staticmethod
    def _count_active_threads(shell: dict[str, Any]) -> int:
        # Count threads whose session is starting/running as active, except when
        # latestTurn.state is terminal (interrupted/completed/error): a terminal
        # turn with a stale-running provider session no longer holds capacity.
        # Sessions with no latestTurn, or a running/latestTurn.state=running, still
        # count. This is shared by doctor and capacity so they never disagree.
        active = 0
        for thread in shell.get("threads", []):
            session = thread.get("session") or {}
            if session.get("status") not in {"starting", "running"}:
                continue
            latest_turn = thread.get("latestTurn")
            if isinstance(latest_turn, dict):
                if latest_turn.get("state") in _TERMINAL_TURN_STATES:
                    continue
            active += 1
        return active

    @staticmethod
    def _open_approval_ids(thread: dict[str, Any]) -> set[str]:
        open_ids: set[str] = set()
        for activity in thread.get("activities", []):
            payload = activity.get("payload")
            if not isinstance(payload, dict):
                continue
            request_id = payload.get("requestId")
            if not isinstance(request_id, str):
                continue
            kind = activity.get("kind")
            if kind == "approval.requested":
                open_ids.add(request_id)
            elif kind == "approval.resolved":
                open_ids.discard(request_id)
            elif kind == "provider.approval.respond.failed":
                detail = str(payload.get("detail", "")).lower()
                if "stale pending approval request" in detail or "unknown pending approval request" in detail:
                    open_ids.discard(request_id)
        return open_ids

    def _approve(self, request: dict[str, Any]) -> dict[str, Any]:
        thread_id = self._required_string(request, "thread_id")
        approval_id = self._required_string(request, "approval_id")
        decision = self._required_string(request, "decision")
        if decision not in _APPROVAL_DECISIONS:
            raise ValidationError("unsupported approval decision")
        detail = self.t3.thread_snapshot(thread_id).get("thread", {})
        if approval_id not in self._open_approval_ids(detail):
            raise ValidationError("approval request is not currently pending")
        created_at = self._now()
        response = self.t3.dispatch(
            {
                "type": "thread.approval.respond",
                "commandId": self._uuid(),
                "threadId": thread_id,
                "requestId": approval_id,
                "decision": decision,
                "createdAt": created_at,
            }
        )
        return {
            "schema": "t3-fleet.agent-result.v1",
            "host": self.policy.host_id,
            "state": "running",
            "thread_id": thread_id,
            "sequence": response.get("sequence"),
            "needs_attention": False,
            "updated_at": created_at,
        }

    @staticmethod
    def _open_user_input_ids(thread: dict[str, Any]) -> set[str]:
        open_ids: set[str] = set()
        for activity in thread.get("activities", []):
            payload = activity.get("payload")
            if not isinstance(payload, dict):
                continue
            request_id = payload.get("requestId")
            if not isinstance(request_id, str):
                continue
            kind = activity.get("kind")
            if kind == "user-input.requested":
                open_ids.add(request_id)
            elif kind == "user-input.resolved":
                open_ids.discard(request_id)
            elif kind == "provider.user-input.respond.failed":
                detail = str(payload.get("detail", "")).lower()
                if (
                    "stale pending user-input request" in detail
                    or "unknown pending user-input request" in detail
                    or "unknown pending user input request" in detail
                    or "unknown pending codex user input request" in detail
                ):
                    open_ids.discard(request_id)
        return open_ids

    def _answer(self, request: dict[str, Any]) -> dict[str, Any]:
        thread_id = self._required_string(request, "thread_id")
        input_id = self._required_string(request, "input_id")
        answers = request.get("answers")
        if (
            not isinstance(answers, dict)
            or not answers
            or not all(isinstance(key, str) and key for key in answers)
        ):
            raise ValidationError("answers must be a non-empty object")
        detail = self.t3.thread_snapshot(thread_id).get("thread", {})
        if input_id not in self._open_user_input_ids(detail):
            raise ValidationError("user-input request is not currently pending")
        created_at = self._now()
        response = self.t3.dispatch(
            {
                "type": "thread.user-input.respond",
                "commandId": self._uuid(),
                "threadId": thread_id,
                "requestId": input_id,
                "answers": answers,
                "createdAt": created_at,
            }
        )
        return {
            "schema": "t3-fleet.agent-result.v1",
            "host": self.policy.host_id,
            "state": "running",
            "thread_id": thread_id,
            "sequence": response.get("sequence"),
            "needs_attention": False,
            "updated_at": created_at,
        }

    def handle(self, request: dict[str, Any]) -> dict[str, Any]:
        self._check_identity()
        action = request.get("action")
        if action == "doctor":
            descriptor = self.t3.descriptor()
            shell = self.t3.shell_snapshot()
            server_version = descriptor.get("serverVersion")
            capabilities = descriptor.get("capabilities")
            version_compatible = isinstance(server_version, str) and bool(
                _SUPPORTED_T3_VERSION.fullmatch(server_version)
            )
            compatible = (
                version_compatible
                and isinstance(capabilities, dict)
                and capabilities.get("threadSettlement") is True
            )
            active = self._count_active_threads(shell)
            if not compatible:
                state = "incompatible"
            elif active >= self.policy.active_limit:
                state = "at_capacity"
            else:
                state = "eligible"
            result = {
                "schema": "t3-fleet.agent-result.v1",
                "host": self.policy.host_id,
                "state": state,
                "server_version": server_version,
                "project_count": len(shell.get("projects", [])),
                "active": active,
                "limit": self.policy.active_limit,
                "needs_attention": False,
            }
            if "repository" in request:
                _, repository = self._repository(request)
                self._project(repository, shell=shell)
                result["project_registered"] = True
            return result
        if action == "capacity":
            shell = self.t3.shell_snapshot()
            active = self._count_active_threads(shell)
            return {
                "schema": "t3-fleet.agent-result.v1",
                "host": self.policy.host_id,
                "state": "eligible",
                "active": active,
                "needs_attention": False,
            }
        if action == "submit":
            return self._submit(request)
        if action == "status":
            return self._status(
                self._required_string(request, "thread_id"),
                include_output=bool(request.get("include_output", False)),
            )
        if action == "recover":
            thread_id = request.get("thread_id")
            receipt: dict[str, Any] | None = None
            if thread_id is None:
                receipt = self._submit_receipt(
                    self._required_string(request, "request_id")
                )
                thread_id = receipt["thread_id"]
            elif not isinstance(thread_id, str) or not thread_id:
                raise ValidationError("thread_id must be a non-empty string")
            try:
                result = self._status(
                    thread_id,
                    include_output=bool(request.get("include_output", False)),
                )
            except ValidationError as error:
                if request.get("thread_id") is not None or not str(error).startswith(
                    "T3 thread not found:"
                ):
                    raise
                created_at = receipt.get("created_at")
                if (
                    not isinstance(created_at, str)
                    or self._age_seconds(created_at, self._now())
                    < _MISSING_THREAD_GRACE_SECONDS
                ):
                    return {
                        "schema": "t3-fleet.agent-result.v1",
                        "host": self.policy.host_id,
                        "state": "unknown",
                        "thread_id": thread_id,
                        "needs_attention": False,
                        "updated_at": created_at,
                    }
                return {
                    "schema": "t3-fleet.agent-result.v1",
                    "host": self.policy.host_id,
                    "state": "failed",
                    "thread_id": thread_id,
                    "needs_attention": False,
                    "updated_at": created_at,
                }
            if (
                receipt is not None
                and result["state"] == "unknown"
                and receipt.get("outcome") == "unknown"
                and isinstance(receipt.get("created_at"), str)
                and self._age_seconds(receipt["created_at"], self._now())
                >= _MISSING_THREAD_GRACE_SECONDS
            ):
                result["state"] = "failed"
            return result
        if action == "followup":
            return self._followup(request)
        if action == "interrupt":
            return self._interrupt(request)
        if action == "approve":
            return self._approve(request)
        if action == "answer":
            return self._answer(request)
        if action == "claim":
            return self._claim(request)
        if action == "lookup":
            return self._lookup(request)
        if action == "reconcile":
            return self._reconcile(request)
        if action == "reserve_followup":
            return self._reserve_followup(request)
        if action == "release":
            return self._release(request)
        raise ValidationError("unsupported target-agent action")


_MAX_AGENT_INPUT_BYTES = 1024 * 1024

_AGENT_ERROR_MESSAGES = {
    "invalid_config": "target configuration is invalid",
    "invalid_request": "target request is invalid",
    "host_unavailable": "target is unavailable",
    "idempotency_conflict": "target request conflicts with an existing request",
    "unknown_state": "target request outcome is unknown",
    "t3_auth_failed": "target authentication failed",
    "t3_http_failed": "target T3 request failed",
    "internal_error": "internal target error",
}


def _agent_state_root(environ: Mapping[str, str]) -> Path:
    override = environ.get("T3_FLEET_AGENT_STATE")
    if override:
        return Path(override).expanduser()
    if os.name == "nt":
        root = environ.get("LOCALAPPDATA")
        return (
            Path(root) if root else Path.home() / "AppData" / "Local"
        ) / "t3-fleet" / "agent"
    if sys.platform == "darwin":
        return (
            Path.home()
            / "Library"
            / "Application Support"
            / "t3-fleet"
            / "agent"
        )
    root = environ.get("XDG_STATE_HOME")
    return (
        Path(root) if root else Path.home() / ".local" / "state"
    ) / "t3-fleet" / "agent"


def _agent_config_path(environ: Mapping[str, str]) -> Path:
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


def _select_local_host(config: FleetConfig, environ: Mapping[str, str]) -> str:
    selected = environ.get("T3_FLEET_HOST")
    if selected:
        if selected not in config.hosts:
            raise ConfigError("configured target host is not allowlisted")
        return selected
    live = socket.gethostname().casefold()
    matches = [
        host.host_id
        for host in config.hosts.values()
        if host.expected_hostname.casefold() == live
    ]
    if len(matches) != 1:
        raise ConfigError("target host identity does not match exactly one configured host")
    return matches[0]


def _build_agent(environ: Mapping[str, str]) -> T3Agent:
    try:
        config = load_config(_agent_config_path(environ))
    except OSError as error:
        raise ConfigError("target configuration could not be read") from error
    host_id = _select_local_host(config, environ)
    host = config.hosts[host_id]
    if not host.enabled:
        raise ConfigError("configured target host is disabled")
    repositories = {
        repository_id: RepositoryPolicy(
            path=repository.paths[host_id],
            default_branch=repository.default_branch,
            branch_prefix=repository.branch_prefix,
        )
        for repository_id, repository in config.repositories.items()
        if host_id in repository.paths
    }
    state_dir = (
        Path(host.agent_state_dir).expanduser()
        if host.agent_state_dir
        else _agent_state_root(environ) / host_id
    )
    issuer = T3SessionIssuer(host.t3_command, base_dir=host.t3_base_dir)
    return T3Agent(
        AgentPolicy(
            host_id=host_id,
            expected_hostname=host.expected_hostname,
            platform=host.platform,
            repositories=repositories,
            state_dir=state_dir,
            active_limit=host.active_limit,
        ),
        T3Client(host.t3_url, issuer),
    )


def _agent_emit(stream: TextIO, value: dict[str, Any]) -> None:
    stream.write(json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n")


def _agent_error(code: str) -> dict[str, Any]:
    return {
        "schema": "t3-fleet.agent-error.v1",
        "error": {
            "code": code,
            "message": _AGENT_ERROR_MESSAGES.get(
                code,
                _AGENT_ERROR_MESSAGES["internal_error"],
            ),
        },
    }


def main(
    argv: list[str] | None = None,
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    environ: Mapping[str, str] | None = None,
) -> int:
    argv = sys.argv[1:] if argv is None else argv
    stdin = stdin or sys.stdin
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    environ = environ if environ is not None else os.environ
    try:
        if argv:
            raise ValidationError("target adapter does not accept command arguments")
        raw = stdin.readline(_MAX_AGENT_INPUT_BYTES + 2)
        payload = raw[:-1] if raw.endswith("\n") else raw
        if len(payload.encode("utf-8")) > _MAX_AGENT_INPUT_BYTES:
            raise ValidationError("target request exceeds the size limit")
        try:
            request = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValidationError("target request must be one JSON object") from error
        if not isinstance(request, dict):
            raise ValidationError("target request must be one JSON object")
        result = _build_agent(environ).handle(request)
        _agent_emit(stdout, result)
        return 0
    except FleetError as error:
        _agent_emit(stdout, _agent_error(error.code))
        stderr.write(f"t3-fleet-agent: {error.code}\n")
        return error.exit_code
    except Exception:
        _agent_emit(stdout, _agent_error("internal_error"))
        stderr.write("t3-fleet-agent: internal_error\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
