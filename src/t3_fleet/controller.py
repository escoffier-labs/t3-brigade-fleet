from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from typing import Any, Callable, Protocol

from .config import FleetConfig, HostConfig, RepositoryConfig
from .errors import HostUnavailable, IdempotencyConflict, UnknownState, ValidationError
from .models import TERMINAL_STATES
from .store import (
    TRANSITIONS,
    FleetStore,
    RequestRecord,
    _receipt_string,
    _receipt_time,
)

_LEASE_SECONDS = 24 * 60 * 60
_MISSING_RECEIPT_GRACE_SECONDS = 30
_WAIT_INTERVALS = (1, 2, 5, 10, 15)

_PEER_REQUEST_ID = re.compile(
    r"^[a-z0-9][a-z0-9-]{0,63}-[a-z0-9][a-z0-9-]{0,63}-"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


class TransportProtocol(Protocol):
    def call(self, host: HostConfig, request: dict[str, Any]) -> dict[str, Any]: ...


class FleetController:
    def __init__(
        self,
        config: FleetConfig,
        store: FleetStore,
        transport: TransportProtocol,
        *,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        uuid_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
    ) -> None:
        self.config = config
        self.store = store
        self.transport = transport
        self._sleep = sleep
        self._monotonic = monotonic
        self._wall_clock = wall_clock
        self._uuid = uuid_factory

    def _validate_peer_request_id(
        self,
        request_id: Any,
        host_id: str,
    ) -> str:
        if not isinstance(request_id, str) or not request_id:
            raise ValidationError("request_id must be a non-empty string")
        if len(request_id) > 166:
            raise ValidationError("peer request_id exceeds maximum length of 166 characters")
        if not _PEER_REQUEST_ID.fullmatch(request_id):
            raise ValidationError("request_id must be a valid peer request id")
        if not request_id.startswith(f"{host_id}-"):
            raise ValidationError("request_id target prefix does not match selected host")
        return request_id

    def _target(
        self,
        host_id: str,
        repository_id: str,
    ) -> tuple[HostConfig, RepositoryConfig]:
        try:
            host = self.config.hosts[host_id]
        except KeyError as error:
            raise ValidationError(f"host is not allowlisted: {host_id}") from error
        if not host.enabled:
            raise HostUnavailable(f"host is disabled: {host_id}")
        try:
            repository = self.config.repositories[repository_id]
        except KeyError as error:
            raise ValidationError(f"repository is not allowlisted: {repository_id}") from error
        if host_id not in repository.paths:
            raise ValidationError(
                f"repository {repository_id} is not configured on host {host_id}"
            )
        return host, repository

    @staticmethod
    def _required(request: dict[str, Any], key: str) -> str:
        value = request.get(key)
        if not isinstance(value, str) or not value:
            raise ValidationError(f"{key} must be a non-empty string")
        return value

    @staticmethod
    def _digest(request: dict[str, Any], prompt: str) -> str:
        encoded_request = json.dumps(
            request,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        digest = hashlib.sha256()
        digest.update(encoded_request)
        digest.update(b"\0")
        digest.update(prompt.encode("utf-8"))
        return digest.hexdigest()

    @staticmethod
    def _result(record: RequestRecord, response: dict[str, Any] | None = None) -> dict[str, Any]:
        response = response or {}
        result: dict[str, Any] = {
            "schema": "t3-fleet.result.v1",
            "request_id": record.request_id,
            "host": record.host,
            "state": record.state,
            "thread_id": record.thread_id,
            "needs_attention": record.state == "needs_attention",
        }
        for key in ("updated_at", "output", "sequence"):
            if key in response:
                result[key] = response[key]
        return result

    def _call(
        self,
        host: HostConfig,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        return self.transport.call(host, request)

    def _record_or_discover(
        self,
        request_id: str,
        *,
        target_hint: str | None = None,
    ) -> RequestRecord:
        try:
            return self.store.get(request_id)
        except ValidationError as error:
            if not str(error).startswith("unknown request id:"):
                raise
        if self.config.coordination_mode != "target-authoritative":
            raise ValidationError(f"unknown request id: {request_id}")
        matches = self._lookup_targets(request_id, target_hint=target_hint)
        if not matches:
            raise ValidationError(f"unknown request id: {request_id}")
        if len(matches) > 1:
            raise IdempotencyConflict(
                f"request {request_id} exists on more than one target"
            )
        return self.store.import_receipt(matches[0])

    @staticmethod
    def _is_complete_receipt(response: dict[str, Any]) -> bool:
        try:
            for key in (
                "request_id",
                "request_digest",
                "target_id",
                "repository",
                "state",
            ):
                _receipt_string(response, key)
            _receipt_time(response.get("created_at"), "created_at")
            _receipt_time(response.get("updated_at"), "updated_at")
        except ValidationError:
            return False
        prompt_bytes = response.get("prompt_bytes")
        if (
            isinstance(prompt_bytes, bool)
            or not isinstance(prompt_bytes, int)
            or prompt_bytes < 0
        ):
            return False
        thread_id = response.get("thread_id")
        if thread_id is not None and not isinstance(thread_id, str):
            return False
        source_revision = response.get("source_revision")
        if source_revision is not None and not isinstance(source_revision, str):
            return False
        prompt_sha256 = response.get("prompt_sha256")
        if prompt_sha256 is not None and not isinstance(prompt_sha256, str):
            return False
        return True

    def _lookup_targets(
        self,
        request_id: str,
        *,
        target_hint: str | None = None,
    ) -> list[dict[str, Any]]:
        if target_hint is not None:
            host_ids = [target_hint]
        elif _PEER_REQUEST_ID.fullmatch(request_id):
            qualified = [
                host_id
                for host_id in self.config.hosts
                if request_id.startswith(f"{host_id}-")
            ]
            host_ids = (
                [max(qualified, key=len)]
                if qualified
                else [
                    host_id
                    for host_id, host in self.config.hosts.items()
                    if host.enabled
                ]
            )
        else:
            host_ids = [
                host_id
                for host_id, host in self.config.hosts.items()
                if host.enabled
            ]
        matches: list[dict[str, Any]] = []
        for host_id in host_ids:
            host = self.config.hosts.get(host_id)
            if host is None:
                raise ValidationError(f"host is not allowlisted: {host_id}")
            if not host.enabled:
                raise HostUnavailable(f"host is disabled: {host_id}")
            response = self._call(
                host,
                {"action": "lookup", "request_id": request_id},
            )
            if response.get("state") == "not_found":
                continue
            if response.get("request_id") != request_id:
                raise ValidationError("target lookup returned the wrong request id")
            matches.append(response)
        return matches

    def _ensure_capacity(
        self,
        record: RequestRecord,
        host: HostConfig,
    ) -> None:
        global_active, host_active = self.store.active_counts(
            host=record.host,
            exclude_request_id=record.request_id,
        )
        if (
            global_active >= self.config.global_active_limit
            or host_active >= host.active_limit
        ):
            raise HostUnavailable(
                f"dispatch capacity is exhausted for host {record.host}"
            )

    def _raise_unknown_after_mutation(
        self,
        record: RequestRecord,
        *,
        action: str,
        error: Exception | None = None,
    ) -> None:
        if record.state != "unknown":
            if record.state in TERMINAL_STATES:
                self.store.transition(record.request_id, "unknown")
            else:
                self.store.mark_transport_failure(record.request_id)
        self.store.record_host_failure(record.host)
        unknown = UnknownState(
            f"{action} outcome is unknown for request {record.request_id}; run recover",
            request_id=record.request_id,
        )
        if error is None:
            raise unknown
        raise unknown from error

    def _transition_from_response(
        self,
        record: RequestRecord,
        response: dict[str, Any],
    ) -> RequestRecord:
        state = response.get("state")
        if not isinstance(state, str):
            raise ValidationError("target response omitted state")
        thread_id = response.get("thread_id")
        if thread_id is not None and not isinstance(thread_id, str):
            raise ValidationError("target response contains an invalid thread id")
        if state == record.state:
            return record
        if state not in TRANSITIONS.get(record.state, frozenset()):
            raise ValidationError(f"invalid target state transition {record.state} -> {state}")
        return self.store.transition(record.request_id, state, thread_id=thread_id)

    def _refresh_from_target_response(
        self,
        record: RequestRecord,
        response: dict[str, Any],
        *,
        peer_mode: bool,
    ) -> RequestRecord:
        if peer_mode and self._is_complete_receipt(response):
            return self.store.import_receipt(response)
        return self._transition_from_response(record, response)

    def submit(self, request: dict[str, Any], *, prompt: str) -> dict[str, Any]:
        if not isinstance(prompt, str) or not prompt:
            raise ValidationError("prompt must be a non-empty string")
        if request.get("run_setup_script", False):
            raise ValidationError(
                "run_setup_script is unavailable through the T3 HTTP dispatch fallback"
            )
        if self.config.coordination_mode == "target-authoritative":
            host_id = self._required(request, "host")
            repository_id = self._required(request, "repository")
            host, repository = self._target(host_id, repository_id)
            provided_id = request.get("request_id")
            if provided_id is None:
                request = dict(request)
                request_id = f"{host_id}-{self.config.controller_id}-{self._uuid()}"
                request["request_id"] = request_id
            else:
                request_id = self._validate_peer_request_id(provided_id, host_id)
        else:
            request_id = self._required(request, "request_id")
            host_id = self._required(request, "host")
            repository_id = self._required(request, "repository")
            host, repository = self._target(host_id, repository_id)
        digest = self._digest(request, prompt)
        record = self.store.create_request(
            request_id,
            digest,
            host_id,
            repository_id,
            prompt_bytes=len(prompt.encode("utf-8")),
        )
        if record.state not in {"queued", "blocked_offline"}:
            return self._result(record)
        if self.store.circuit_is_open(host_id):
            if record.state == "queued":
                record = self.store.transition(request_id, "blocked_offline")
            raise HostUnavailable(f"host circuit is open: {record.host}")

        record = self.store.transition(request_id, "preflight")
        if self.config.coordination_mode == "target-authoritative":
            record = self.store.transition(request_id, "dispatching")
            agent_request = {
                key: value
                for key, value in request.items()
                if key
                in {
                    "request_id",
                    "repository",
                    "source_revision",
                    "title",
                    "model_selection",
                    "runtime_mode",
                    "interaction_mode",
                }
            }
            agent_request["action"] = "claim"
            agent_request["digest"] = digest
            agent_request["prompt"] = prompt
            try:
                response = self._call(host, agent_request)
            except HostUnavailable as error:
                self._raise_unknown_after_mutation(
                    record,
                    action="claim",
                    error=error,
                )
            self.store.record_host_success(host_id)
            state = response.get("state")
            thread_id = response.get("thread_id")
            if (
                state not in {"running", "needs_attention"}
                or not isinstance(thread_id, str)
                or not thread_id
            ):
                self._raise_unknown_after_mutation(record, action="claim")
            record = self.store.import_receipt(response)
            return self._result(record, response)

        try:
            doctor = self._call(host, {"action": "doctor", "repository": repository_id})
        except HostUnavailable:
            self.store.mark_transport_failure(request_id)
            self.store.record_host_failure(host_id)
            raise
        if doctor.get("state") != "eligible":
            self.store.mark_transport_failure(request_id)
            raise HostUnavailable(f"host is not eligible: {host_id}")

        try:
            self._ensure_capacity(record, host)
        except HostUnavailable:
            self.store.transition(request_id, "queued")
            raise

        if not self.store.acquire_lease(
            host_id,
            repository_id,
            request_id,
            ttl=_LEASE_SECONDS,
        ):
            self.store.transition(request_id, "queued")
            raise HostUnavailable(f"repository is busy on host {host_id}")

        record = self.store.transition(request_id, "dispatching")
        agent_request = {
            key: value
            for key, value in request.items()
            if key
            in {
                "request_id",
                "repository",
                "source_revision",
                "title",
                "model_selection",
                "runtime_mode",
                "interaction_mode",
                "run_setup_script",
            }
        }
        agent_request["action"] = "submit"
        agent_request["prompt"] = prompt
        try:
            response = self._call(host, agent_request)
        except HostUnavailable as error:
            self._raise_unknown_after_mutation(
                record,
                action="dispatch",
                error=error,
            )
        self.store.record_host_success(host_id)
        state = response.get("state")
        thread_id = response.get("thread_id")
        if (
            state not in {"running", "needs_attention"}
            or not isinstance(thread_id, str)
            or not thread_id
        ):
            self._raise_unknown_after_mutation(record, action="dispatch")
        record = self.store.transition(
            request_id,
            state,
            thread_id=thread_id,
        )
        return self._result(record, response)

    def status(self, request_id: str, *, include_output: bool = False) -> dict[str, Any]:
        record = self._record_or_discover(request_id)
        if record.thread_id is None:
            return self._result(record)
        host, _ = self._target(record.host, record.repository)
        peer_mode = self.config.coordination_mode == "target-authoritative"
        action = "reconcile" if peer_mode else "status"
        request: dict[str, Any] = {
            "action": action,
            "request_id": record.request_id,
        }
        if not peer_mode or record.thread_id is not None:
            request["thread_id"] = record.thread_id
        request["include_output"] = include_output
        try:
            response = self._call(host, request)
        except HostUnavailable:
            if record.state not in TERMINAL_STATES and record.state != "unknown":
                record = self.store.mark_transport_failure(request_id)
            self.store.record_host_failure(record.host)
            return self._result(record)
        self.store.record_host_success(record.host)
        record = self._refresh_from_target_response(
            record,
            response,
            peer_mode=peer_mode,
        )
        if not peer_mode:
            if record.state in TERMINAL_STATES:
                self.store.release_lease(record.host, record.repository, record.request_id)
            elif record.state in {"running", "needs_attention", "unknown"}:
                if not self.store.acquire_lease(
                    record.host,
                    record.repository,
                    record.request_id,
                    ttl=_LEASE_SECONDS,
                ):
                    raise UnknownState(
                        f"request {request_id} lost its repository lease; run recover"
                    )
        return self._result(record, response)

    def followup(
        self,
        request_id: str,
        *,
        prompt: str,
        runtime_mode: str = "approval-required",
        interaction_mode: str = "default",
    ) -> dict[str, Any]:
        if not prompt:
            raise ValidationError("prompt must be a non-empty string")
        record = self._record_or_discover(request_id)
        if record.thread_id is None:
            raise ValidationError("request has no target thread")
        host, _ = self._target(record.host, record.repository)
        peer_mode = self.config.coordination_mode == "target-authoritative"
        if record.state in TERMINAL_STATES:
            if peer_mode:
                try:
                    response = self._call(
                        host,
                        {
                            "action": "reserve_followup",
                            "request_id": request_id,
                            "thread_id": record.thread_id,
                        },
                    )
                except HostUnavailable as error:
                    self._raise_unknown_after_mutation(
                        record,
                        action="reserve_followup",
                        error=error,
                    )
                self.store.record_host_success(record.host)
                record = self._refresh_from_target_response(
                    record,
                    response,
                    peer_mode=True,
                )
            else:
                record = self.store.reserve_followup(
                    request_id,
                    global_limit=self.config.global_active_limit,
                    host_limit=host.active_limit,
                    ttl=_LEASE_SECONDS,
                )
        elif record.state != "running":
            raise ValidationError(f"cannot follow up request in state {record.state}")
        try:
            response = self._call(
                host,
                {
                    "action": "followup",
                    "request_id": request_id,
                    "thread_id": record.thread_id,
                    "prompt": prompt,
                    "runtime_mode": runtime_mode,
                    "interaction_mode": interaction_mode,
                },
            )
        except HostUnavailable as error:
            self._raise_unknown_after_mutation(
                record,
                action="follow-up",
                error=error,
            )
        self.store.record_host_success(record.host)
        if record.state == "dispatching":
            record = self.store.transition(request_id, "running")
        return self._result(record, response)

    def interrupt(self, request_id: str) -> dict[str, Any]:
        record = self._record_or_discover(request_id)
        if record.thread_id is None:
            raise ValidationError("request has no target thread")
        if record.state in TERMINAL_STATES:
            raise ValidationError(f"cannot interrupt request in state {record.state}")
        host, _ = self._target(record.host, record.repository)
        try:
            response = self._call(
                host,
                {"action": "interrupt", "thread_id": record.thread_id},
            )
        except HostUnavailable as error:
            self._raise_unknown_after_mutation(
                record,
                action="interrupt",
                error=error,
            )
        self.store.record_host_success(record.host)
        return self._result(record, response)

    def approve(self, request_id: str, approval_id: str, decision: str) -> dict[str, Any]:
        record = self._record_or_discover(request_id)
        if record.thread_id is None:
            raise ValidationError("request has no target thread")
        if record.state in TERMINAL_STATES:
            raise ValidationError(f"cannot approve request in state {record.state}")
        host, _ = self._target(record.host, record.repository)
        try:
            response = self._call(
                host,
                {
                    "action": "approve",
                    "thread_id": record.thread_id,
                    "approval_id": approval_id,
                    "decision": decision,
                },
            )
        except HostUnavailable as error:
            self._raise_unknown_after_mutation(
                record,
                action="approval",
                error=error,
            )
        self.store.record_host_success(record.host)
        record = self._transition_from_response(record, response)
        return self._result(record, response)

    def answer(
        self,
        request_id: str,
        input_id: str,
        answers: dict[str, Any],
    ) -> dict[str, Any]:
        if not input_id:
            raise ValidationError("input_id must be a non-empty string")
        if (
            not isinstance(answers, dict)
            or not answers
            or not all(isinstance(key, str) and key for key in answers)
        ):
            raise ValidationError("answers must be a non-empty object")
        record = self._record_or_discover(request_id)
        if record.thread_id is None:
            raise ValidationError("request has no target thread")
        if record.state in TERMINAL_STATES:
            raise ValidationError(f"cannot answer request in state {record.state}")
        host, _ = self._target(record.host, record.repository)
        try:
            response = self._call(
                host,
                {
                    "action": "answer",
                    "thread_id": record.thread_id,
                    "input_id": input_id,
                    "answers": answers,
                },
            )
        except HostUnavailable as error:
            self._raise_unknown_after_mutation(
                record,
                action="user-input response",
                error=error,
            )
        self.store.record_host_success(record.host)
        record = self._transition_from_response(record, response)
        return self._result(record, response)

    def recover(
        self,
        request_id: str | None,
        *,
        include_output: bool = False,
        target_hint: str | None = None,
    ) -> dict[str, Any]:
        if not request_id:
            if target_hint is None:
                raise ValidationError("request_id must be a non-empty string")
            matches = self.store.unknown_requests_for_host(target_hint)
            if len(matches) != 1:
                raise ValidationError(
                    f"target {target_hint} has {len(matches)} unknown requests; provide request_id"
                )
            request_id = matches[0]
        record = self._record_or_discover(request_id, target_hint=target_hint)
        host, _ = self._target(record.host, record.repository)
        peer_mode = self.config.coordination_mode == "target-authoritative"
        action = "reconcile" if peer_mode else "recover"
        recovery_request: dict[str, Any] = {
            "action": action,
            "request_id": request_id,
            "include_output": include_output,
        }
        if record.thread_id is not None:
            recovery_request["thread_id"] = record.thread_id
        try:
            response = self._call(
                host,
                recovery_request,
            )
        except HostUnavailable:
            self.store.record_host_failure(record.host)
            raise
        self.store.record_host_success(record.host)
        if peer_mode and response.get("state") == "not_found":
            if record.state == "failed":
                return self._result(record, response)
            if record.state == "unknown":
                if (
                    self._wall_clock() - record.updated_at
                    >= _MISSING_RECEIPT_GRACE_SECONDS
                ):
                    record = self.store.transition(record.request_id, "failed")
                return self._result(record, response)
        record = self._refresh_from_target_response(
            record,
            response,
            peer_mode=peer_mode,
        )
        if not peer_mode and record.state in TERMINAL_STATES:
            self.store.release_lease(record.host, record.repository, record.request_id)
        return self._result(record, response)

    def wait(
        self,
        request_id: str,
        *,
        timeout: float,
        include_output: bool = False,
    ) -> dict[str, Any]:
        if timeout < 0:
            raise ValidationError("timeout must not be negative")
        started = self._monotonic()
        attempt = 0
        while True:
            result = self.status(request_id, include_output=include_output)
            if result["state"] in TERMINAL_STATES or result["state"] in {
                "needs_attention",
                "unknown",
                "blocked_offline",
            }:
                return result
            elapsed = self._monotonic() - started
            remaining = timeout - elapsed
            if remaining <= 0:
                return result
            interval = _WAIT_INTERVALS[min(attempt, len(_WAIT_INTERVALS) - 1)]
            self._sleep(min(interval, remaining))
            attempt += 1
