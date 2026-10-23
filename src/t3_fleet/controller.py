from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, NoReturn, Protocol

from .config import FleetConfig, HostConfig, RepositoryConfig
from .errors import (
    MISSING_RECEIPT_GRACE_SECONDS,
    bounded_target_error,
    FleetError,
    HostUnavailable,
    IdempotencyConflict,
    PolicyRejected,
    PolicyRevisionConflict,
    TargetBusy,
    UnknownState,
    ValidationError,
)
from .models import TERMINAL_STATES, bounded_pending_request
from .outbox import OutboxJob, OutboxStore
from .policy import (
    CONSUMER as POLICY_CONSUMER,
)
from .policy import (
    DELEGATION_CONSUMER,
    BrigadeFleetPolicy,
    FleetPolicyProtocol,
    decision_expiry,
    delegation_stamp,
    load_delegation_receipt,
    policy_stamp,
)
from .reporting import launch_context
from .store import (
    TRANSITIONS,
    FleetStore,
    PolicySessionRecord,
    RequestRecord,
    _receipt_string,
    _receipt_time,
)
from .transport import (
    BrigadeModelAdmission,
    normalize_admission,
    normalize_model_plan,
)

AUTO_HOST = "auto"

_MAX_DECISION_LIFETIME = timedelta(hours=24)
_MISSING_RECEIPT_GRACE_SECONDS = MISSING_RECEIPT_GRACE_SECONDS
_WAIT_INTERVALS = (1, 2, 5, 10, 15)
# A host that fails this many settle calls in a row is skipped for the rest of
# the sweep, so one dead target cannot hold the sweep for a call timeout each.
_SETTLE_HOST_FAILURE_LIMIT = 2
# Outcomes after which a thread has nothing left to settle. Anything else
# (pinned, still running, not ours) is retried on the next sweep.
_SETTLE_DONE = frozenset({"settled", "already_settled", "thread_missing", "archived"})
_SETTLE_REFUSALS = frozenset(
    {"pinned", "not_terminal", "not_fleet_thread", "ambiguous_thread", "busy"}
)

_PEER_REQUEST_ID = re.compile(
    r"^[a-z0-9][a-z0-9-]{0,63}-[a-z0-9][a-z0-9-]{0,63}-"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
# A workload is a public, bounded identifier and nothing else: it states what
# kind of work this is so the central route can judge eligibility. It never
# ranks machines locally and it never widens a data rule.
_WORKLOAD = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
_DELEGATION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
# One adoption is one execution, so the local execution identity is derived
# from the delegation rather than minted fresh. A replay of the same adoption
# lands on the same request id and is answered from the durable receipt.
_DELEGATION_NAMESPACE = uuid.UUID("6f6b9b6e-0f6a-5a3f-9a5c-2b1d8f0c4e77")
# The frozen shape of a delegated result handoff. It is target-owned metadata
# about work the target still holds, never a claim that the source checkout
# changed and never a path this controller could have guessed.
_HANDOFF_FIELDS = (
    "target_machine",
    "repository",
    "branch",
    "source_revision",
    "request_id",
    "worktree_ref",
)


class TransportProtocol(Protocol):
    def call(self, host: HostConfig, request: dict[str, Any]) -> dict[str, Any]: ...


class TargetStoreProtocol(Protocol):
    """The narrow slice of the local target store `tick` is allowed to read.

    Renewal is a target act, so this is deliberately read-only: `tick` finds
    what this machine already owns and asks the Hub to keep it, and it has no
    way to claim, release, or rewrite a target receipt from the source side.
    """

    def active_reservations(self, *, target_id: str) -> list[Any]: ...


class AdmissionProtocol(Protocol):
    def admit(self, request: dict[str, Any]) -> dict[str, Any]: ...

    def plan(self, request: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True)
class _PolicyPlan:
    """One resolved Fleet Hub policy session and its routing decision.

    `delegation_id` is set only when this session was adopted from a parent
    delegation rather than routed here. It changes which consumer the Hub is
    asked under and it scopes the model plan, but it never changes what this
    controller is allowed to decide for itself, which is nothing.
    """

    session_id: str
    stamp: dict[str, Any]
    machine: str
    seat: str
    host: HostConfig
    repository: RepositoryConfig
    delegation_id: str | None = None

    @property
    def consumer(self) -> str:
        return POLICY_CONSUMER if self.delegation_id is None else DELEGATION_CONSUMER


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
        admission: AdmissionProtocol | None = None,
        policy: FleetPolicyProtocol | None = None,
        outbox: OutboxStore | None = None,
        target_store: TargetStoreProtocol | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.transport = transport
        self._sleep = sleep
        self._monotonic = monotonic
        self._wall_clock = wall_clock
        self._uuid = uuid_factory
        self._admission = admission or BrigadeModelAdmission()
        self._policy = policy or BrigadeFleetPolicy()
        # Both are optional. Without an outbox this controller behaves exactly
        # as it did before the queue existed; without a target store `tick`
        # drains work but renews nothing, because this machine is then not the
        # target that owns any reservation.
        self._outbox = outbox
        self._target_store = target_store

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
            raise HostUnavailable(
                f"host is disabled: {host_id}", reason="host-ineligible"
            )
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
        for key, value in (("target_error", record.target_error), ("resolution", record.resolution)):
            if value is not None:
                result[key] = value
        for key in ("updated_at", "output", "sequence"):
            if key in response:
                result[key] = response[key]
        if record.state == "needs_attention":
            # What `answer` (input_id) or `approve` (approval_id) needs. Rebuilt
            # and re-bounded here rather than relayed, so a target can never
            # widen it. An older target only sends the raw T3 record.
            pending = bounded_pending_request(
                response.get("pending_request")
                if response.get("pending_request") is not None
                else response.get("pending_runtime_request")
            )
            if pending is not None:
                result["pending_request"] = pending
        handoff = FleetController._delegated_handoff(record, response)
        if handoff is not None:
            # The execution state above stays the real one. The disposition is
            # an additional statement that the target still holds writable work
            # for this adoption and is waiting for review, not a completion
            # claim and not an assertion that anything was applied here.
            result["disposition"] = "pending-handoff"
            result["handoff"] = handoff
        if record.admission is not None:
            result["admission"] = record.admission
        if record.plan is not None:
            # Reported under its own key so an operator can never read a plan as
            # proof that a launch was authorized.
            result["model_plan"] = record.plan
        return result

    @staticmethod
    def _delegated_handoff(
        record: RequestRecord,
        response: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Re-validate the target's handoff before it reaches an operator.

        The field set is frozen with the T3 coordinator, so a document with a
        missing, extra, or wrongly typed field is refused rather than partially
        forwarded. The disposition is only meaningful on a terminal state, and
        only the target may state it: this controller never invents one from a
        local diff or an empty worktree.
        """
        if response.get("disposition") is None and response.get("handoff") is None:
            return None
        if response.get("disposition") != "pending-handoff":
            raise ValidationError("target reported an unsupported result disposition")
        if record.state not in TERMINAL_STATES:
            raise ValidationError(
                "target reported a completion disposition for a request that is "
                "not terminal"
            )
        handoff = response.get("handoff")
        if not isinstance(handoff, dict) or set(handoff) != set(_HANDOFF_FIELDS):
            raise ValidationError("target reported an unsupported handoff shape")
        for key in _HANDOFF_FIELDS:
            value = handoff[key]
            if not isinstance(value, str) or not value or len(value) > 256:
                raise ValidationError(f"target handoff {key} is invalid")
        if not _FULL_SHA.fullmatch(handoff["source_revision"]):
            raise ValidationError("target handoff source_revision is invalid")
        if handoff["request_id"] != record.request_id:
            raise ValidationError("target handoff names another request")
        if handoff["target_machine"] != record.host:
            raise ValidationError("target handoff names another machine")
        return {key: handoff[key] for key in _HANDOFF_FIELDS}

    def _call(
        self,
        host: HostConfig,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        request = {**request, "host": host.host_id}
        response = self.transport.call(host, request)
        if (
            not isinstance(response, dict)
            or response.get("schema") != "t3-fleet.agent-result.v1"
            or not any(key in response for key in ("host", "target_id"))
            or any(response[key] != host.host_id for key in ("host", "target_id") if key in response)
            or ("request_id" in request and response.get("request_id") != request["request_id"])
        ):
            # A reply cannot attest that this target accepted or rejected the
            # mutation. Use the same recovery/capacity path as a lost reply.
            raise HostUnavailable("target returned an invalid or mismatched response")
        return response

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
                raise HostUnavailable(
                f"host is disabled: {host_id}", reason="host-ineligible"
            )
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

    def refresh_submit_health(self, host_id: str) -> None:
        """Refresh local blockers from read-only, identity-checked target receipts.

        An eligible health projection alone never proves that a lost submit
        did not run. Only the matching request receipt can clear that blocker.
        """
        requests = self.store.blocking_requests(host_id)
        if not isinstance(requests, list):
            return
        host = self.config.hosts[host_id]
        for request_id in requests:
            record = self.store.get(request_id)
            try:
                response = self._call(host, {"action": "lookup", "request_id": request_id})
                if not isinstance(response, dict) or not self._is_complete_receipt(response):
                    continue
                peer_mode = self.config.coordination_mode == "target-authoritative"
                digest_matches = response.get("request_digest") == record.digest
                # Old single-controller journals did not retain the prompt
                # hash. Their exact owned thread is the available identity.
                legacy_matches = (
                    not peer_mode and record.thread_id is not None
                    and response.get("thread_id") == record.thread_id
                    and isinstance(response.get("request_digest"), str)
                    and response["request_digest"].startswith("legacy:")
                )
                if (
                    response.get("request_id") != request_id
                    or response.get("target_id") != record.host
                    or response.get("repository") != record.repository
                    or not (digest_matches or legacy_matches)
                    or (record.thread_id is not None and response.get("thread_id") != record.thread_id)
                    or (record.source_revision is not None and response.get("source_revision") != record.source_revision)
                    or (record.prompt_sha256 is not None and response.get("prompt_sha256") != record.prompt_sha256)
                ):
                    continue
                if (not peer_mode and record.state in TERMINAL_STATES
                        and record.resolution is not None and response.get("state") != record.state):
                    record = self.store.transition(request_id, "unknown")
                updated = self._refresh_from_target_response(record, response, peer_mode=peer_mode)
                if not peer_mode and updated.state in TERMINAL_STATES:
                    self.store.release_lease(updated.host, updated.repository, request_id)
            except FleetError:
                # An unavailable lookup never converts an unresolved mutation
                # into an eligible host. Recovery retains its original deadline.
                continue

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
                f"dispatch capacity is exhausted for host {record.host}",
                reason="capacity-exhausted",
            )

    def _raise_unknown_after_mutation(
        self,
        record: RequestRecord,
        *,
        action: str,
        error: Exception | None = None,
    ) -> None:
        target_error = bounded_target_error(getattr(error, "target_error", None))
        if target_error is not None and record.thread_id is None:
            self.store.record_diagnostics(record.request_id, target_error=target_error)
            self._fail_unstarted_admission(record)
            failure = HostUnavailable("target preflight rejected", detail=target_error["detail"])
            failure.request_id = record.request_id
            raise failure from error
        if record.state != "unknown":
            if record.state in TERMINAL_STATES:
                self.store.transition(record.request_id, "unknown")
            else:
                self.store.mark_transport_failure(record.request_id)
        self.store.record_diagnostics(
            record.request_id, target_error=getattr(error, "target_error", None),
            resolution=record.resolution,
        )
        self.store.record_host_failure(record.host)
        unknown = UnknownState(
            f"{action} outcome is unknown for request {record.request_id}; run recover",
            request_id=record.request_id,
            detail=error.detail if error is not None else None,
        )
        if error is None:
            raise unknown
        raise unknown from error

    def _raise_not_delivered(
        self,
        record: RequestRecord,
        *,
        action: str,
        error: HostUnavailable,
    ) -> NoReturn:
        """The target failed before sending anything to T3.

        The target answered, so the host is reachable, and it said nothing was
        delivered, so the request keeps its state and the same call can be
        retried once the cause is fixed. No unknown state, no recover.
        """
        self.store.record_host_success(record.host)
        detail = (
            f"target failed before delivering the {action}; nothing was sent to "
            "T3 and the request is unchanged, so the same call can be retried"
            + (f"; target reported: {error.detail}" if error.detail else "")
        )
        raise HostUnavailable(detail, detail=detail, not_started=True) from error

    def _requeue_declined(self, record: RequestRecord, error: TargetBusy) -> NoReturn:
        """Return a request the target declined unstarted to the queue.

        TargetBusy is the target's own answer that it created nothing for this
        request, so it is a pre-launch block like local capacity exhaustion,
        not a lost dispatch. The caller gets a HostUnavailable it may queue.
        """
        self.store.record_host_success(record.host)
        self.store.transition(record.request_id, "queued")
        raise HostUnavailable(
            f"target declined the request before starting it: {record.host}",
            reason=error.reason or "repository-busy",
            detail=error.detail,
        ) from error

    def _fail_unstarted_admission(self, record: RequestRecord) -> None:
        if record.thread_id is not None:
            return
        if record.state not in TERMINAL_STATES:
            self.store.transition(record.request_id, "failed")
        self.store.release_lease(record.host, record.repository, record.request_id)

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
            record = self.store.import_receipt(response)
        else:
            record = self._transition_from_response(record, response)
        self.store.record_diagnostics(
            record.request_id, target_error=response.get("target_error"),
            resolution=response.get("resolution"),
        )
        return self.store.get(record.request_id)

    def _policy_now(self) -> datetime:
        return datetime.fromtimestamp(self._wall_clock(), UTC)

    def _new_session_id(self) -> str:
        """A policy session identity that leaks no request or machine name."""
        return "t3f-" + self._uuid().replace("-", "")

    def _policy_target(
        self,
        machine: str,
        repository_id: str,
    ) -> tuple[HostConfig, RepositoryConfig]:
        """Re-check a centrally selected machine against this controller's allowlist.

        The Hub decides routing; the controller still refuses to dispatch to a
        machine it does not know, has disabled, or has no checkout for.
        """
        host = self.config.hosts.get(machine)
        if host is None:
            raise PolicyRejected(f"Fleet Hub selected a machine that is not allowlisted: {machine}")
        if not host.enabled:
            raise PolicyRejected(f"Fleet Hub selected a disabled machine: {machine}")
        repository = self.config.repositories[repository_id]
        if machine not in repository.paths:
            raise PolicyRejected(
                f"repository {repository_id} is not configured on machine {machine}"
            )
        return host, repository

    def _canonical_identity(self, repository_id: str) -> str:
        """The canonical identity this controller is configured to use for a repo.

        An enrolled controller with no configured mapping has nothing truthful
        to ask the Hub, so it refuses rather than falling back to the alias.
        Loading the config already enforces this; the dispatch path re-checks it
        because a fallback here would be a privacy bypass, not a convenience.
        """
        identity = self.config.repositories[repository_id].policy_identity
        if identity is None:
            raise PolicyRejected(
                "repository has no canonical policy identity configured",
                reason="policy-repository-mismatch",
            )
        return identity

    def _check_decision(
        self,
        decision: dict[str, Any],
        envelope: dict[str, Any],
    ) -> dict[str, Any]:
        if (
            decision["policy_version"] != envelope["version"]
            or decision["policy_digest"] != envelope["digest"]
        ):
            raise PolicyRevisionConflict(
                "fleet policy changed between resolve and route"
            )
        expiry = decision_expiry(decision)
        now = self._policy_now()
        if expiry <= now:
            raise PolicyRejected("fleet policy decision has expired")
        if expiry > now + _MAX_DECISION_LIFETIME:
            raise PolicyRejected(
                "fleet policy decision window exceeds the supported bound"
            )
        return decision

    def _reusable_decision(
        self,
        session: PolicySessionRecord,
        machine: str | None,
        seat: str | None,
    ) -> dict[str, Any] | None:
        """Reuse a live reservation instead of taking a second one for one request.

        A reservation may only be reused for the placement it was taken for, so
        an explicitly named machine and an explicitly named seat must both match
        the stored decision. A negotiated override never inherits capacity that
        was reserved for a different seat.
        """
        decision = session.decision
        if decision is None:
            return None
        if (
            decision["policy_version"] != session.policy_version
            or decision["policy_digest"] != session.policy_digest
        ):
            return None
        if machine is not None and decision["machine"] != machine:
            return None
        if seat is not None and decision["seat"] != seat:
            return None
        if decision_expiry(decision) <= self._policy_now():
            return None
        return decision

    def _plan_policy(
        self,
        request: dict[str, Any],
        override_reason: Any,
    ) -> _PolicyPlan:
        """Resolve policy and obtain a routing decision before anything mutates.

        Nothing is journalled, admitted, or dispatched until the Fleet Hub has
        both resolved the policy and reserved a machine, so an unreachable or
        refusing Hub fails closed with no half-launched request.
        """
        repository_id = self._required(request, "repository")
        if repository_id not in self.config.repositories:
            raise ValidationError(f"repository is not allowlisted: {repository_id}")
        # The caller names a local alias. Everything the Fleet Hub is asked is
        # asked about the canonical identity this controller is configured with,
        # never about a replacement the caller supplied.
        repo_identity = self._canonical_identity(repository_id)
        requested_host = self._required(request, "host")
        # Naming a seat is an override in exactly the way naming a machine is:
        # it narrows what the Hub may choose, so it is negotiated rather than
        # assumed, and it carries the same operator justification. Central
        # selection stays the default: `auto` with no seat asks for nothing.
        caller_seat = request.get("seat")
        if caller_seat is not None and (
            not isinstance(caller_seat, str) or not caller_seat
        ):
            raise ValidationError("seat must be a non-empty string")
        if requested_host == AUTO_HOST and caller_seat is None:
            if override_reason is not None:
                raise ValidationError(
                    "override_reason applies only to an explicit host or seat"
                )
            machine: str | None = None
        else:
            if not isinstance(override_reason, str) or not override_reason:
                raise ValidationError(
                    "an explicit host or seat requires override_reason while "
                    "policy enrollment is required"
                )
            machine = None if requested_host == AUTO_HOST else requested_host
        origin = self.config.controller_host
        workload = self._requested_workload(request)
        existing = None
        request_id = request.get("request_id")
        if isinstance(request_id, str) and request_id:
            existing = self.store.policy_session_for_request(request_id)
        session_id = existing.session_id if existing is not None else self._new_session_id()
        envelope = self._policy.resolve(
            consumer=POLICY_CONSUMER,
            repository=repo_identity,
            session_id=session_id,
            origin=origin,
        )
        session = self.store.record_policy_envelope(
            session_id,
            consumer=POLICY_CONSUMER,
            repository=repo_identity,
            origin=origin,
            workload=workload,
            envelope=envelope,
        )
        decision = self._reusable_decision(session, machine, caller_seat)
        if decision is None:
            decision = self._check_decision(
                self._policy.route(
                    consumer=POLICY_CONSUMER,
                    repository=repo_identity,
                    session_id=session_id,
                    origin=origin,
                    workload=workload,
                    machine=machine,
                    seat=caller_seat,
                    override_reason=override_reason,
                ),
                envelope,
            )
            self.store.record_policy_decision(session_id, decision)
        selected = decision["machine"]
        if machine is not None and selected != machine:
            raise PolicyRejected(
                "Fleet Hub routed the request to a machine the caller did not name"
            )
        host, repository = self._policy_target(selected, repository_id)
        seat = decision["seat"]
        if caller_seat is not None and caller_seat != seat:
            raise PolicyRejected(
                "Fleet Hub routed the request to a seat the caller did not name"
            )
        return _PolicyPlan(
            session_id=session_id,
            stamp=policy_stamp(
                session_id=session_id,
                origin=origin,
                workload=workload,
                repo_identity=repo_identity,
                envelope=envelope,
                decision=decision,
            ),
            machine=selected,
            seat=seat,
            host=host,
            repository=repository,
        )

    def _requested_workload(self, request: dict[str, Any]) -> str:
        """The workload intent this submit carries, or the configured default.

        The workload is input to central route eligibility. It is forwarded to
        the Hub and stamped onto the request; it never reorders candidates here
        and it is not an override, so it needs no operator justification. A
        request that names none inherits the workload this controller is
        configured for, which is the pre-existing behaviour.
        """
        workload = request.get("workload")
        if workload is None:
            return self.config.policy.workload
        if not isinstance(workload, str) or not _WORKLOAD.fullmatch(workload):
            raise ValidationError("workload must be a bounded lowercase identifier")
        return workload

    @staticmethod
    def _delegation_id(request: dict[str, Any]) -> str | None:
        """The only public field that opens the adoption path.

        There is deliberately no consumer or adapter flag beside it: both come
        from the receipt the Fleet Hub authors, and a caller-supplied receipt is
        not authority however well formed it is.
        """
        delegation_id = request.get("delegation_id")
        if delegation_id is None:
            return None
        if not isinstance(delegation_id, str) or not _DELEGATION_ID.fullmatch(
            delegation_id
        ):
            raise ValidationError("delegation_id must be a bounded identifier")
        return delegation_id

    @staticmethod
    def _caller_payload(
        request: dict[str, Any],
        override_reason: Any,
    ) -> dict[str, Any]:
        """The caller's own request, including its host, seat, and override intent."""
        payload = dict(request)
        payload.pop("model_admission", None)
        payload.pop("model_plan", None)
        # Derived metadata is not caller identity. Raw attribution flags above
        # remain in the digest, and pre-upgrade replay/outbox digests stay valid.
        payload.pop("execution_context", None)
        if override_reason is not None:
            payload["override_reason"] = override_reason
        return payload

    def _durable_replay(
        self,
        request: dict[str, Any],
        digest: str,
    ) -> RequestRecord | None:
        """Return the durable receipt for a request that already got this far.

        The same request id carrying the same payload is one request, not two.
        Once it has been dispatched, replaying it returns what happened; it must
        not resolve policy, take a second reservation, or route again, however
        far the policy has moved since. A different payload under the same id is
        still a conflict, and a request that never started still retries.
        """
        request_id = request.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            return None
        try:
            record = self.store.get(request_id)
        except ValidationError as error:
            if str(error).startswith("unknown request id:"):
                return None
            raise
        if record.digest != digest:
            raise IdempotencyConflict(
                f"request {request_id} already exists with a different payload digest"
            )
        if record.state in {"queued", "blocked_offline"}:
            return None
        return record

    def _delegated_repository(
        self,
        request: dict[str, Any],
        receipt: dict[str, Any],
    ) -> str:
        """Resolve the receipt's canonical identity to exactly one local alias.

        The parent names the repository the only way it truthfully can, by its
        canonical identity, and never has to know what this machine calls it. An
        optional `repository` may narrow to a configured alias, and it must be
        one whose canonical identity is the receipt's. No match, more than one
        match, and a narrowed alias that maps elsewhere are all refusals: there
        is no nearest-match and no alias guessing.
        """
        identity = receipt["repo_identity"]
        matches = sorted(
            repository_id
            for repository_id, repository in self.config.repositories.items()
            if repository.policy_identity == identity
        )
        supplied = request.get("repository")
        if supplied is not None:
            if not isinstance(supplied, str) or not supplied:
                raise ValidationError("repository must be a non-empty string")
            if supplied not in self.config.repositories:
                raise ValidationError(f"repository is not allowlisted: {supplied}")
            if supplied not in matches:
                raise PolicyRejected(
                    "the named repository is not the one this delegation was created for",
                    reason="delegation-repository-unmatched",
                )
            return supplied
        if not matches:
            raise PolicyRejected(
                "no configured repository matches the delegated canonical identity",
                reason="delegation-repository-unmatched",
            )
        if len(matches) > 1:
            raise PolicyRejected(
                "more than one configured repository matches the delegated "
                "canonical identity",
                reason="delegation-repository-unmatched",
            )
        return matches[0]

    def _delegated_request_id(self, machine: str, delegation_id: str) -> str:
        """The one stable execution identity this adoption may ever have.

        It is derived from the delegation, so a replay computes the same id and
        is answered from the durable receipt instead of opening a second
        execution. Nothing about the prompt or the title is in it: a different
        context under the same adoption is a digest conflict on this id, which
        is exactly what it should be.
        """
        controller = self.config.controller_id or self.config.controller_host
        derived = uuid.uuid5(_DELEGATION_NAMESPACE, delegation_id)
        request_id = f"{machine}-{controller}-{derived}"
        if not _PEER_REQUEST_ID.fullmatch(request_id):
            raise ValidationError(
                "the delegated execution identity is not a valid request id"
            )
        return request_id

    @staticmethod
    def _confirm_delegated(
        request: dict[str, Any],
        key: str,
        expected: str,
    ) -> None:
        """A supplied option may confirm the receipt and may never contradict it."""
        supplied = request.get(key)
        if supplied is None:
            return
        if not isinstance(supplied, str) or not supplied:
            raise ValidationError(f"{key} must be a non-empty string")
        if supplied != expected:
            raise PolicyRejected(
                f"the supplied {key} conflicts with this delegation",
                reason="delegation-conflict",
            )

    def _adopt_delegation(
        self,
        request: dict[str, Any],
        *,
        delegation_id: str,
        override_reason: Any,
    ) -> _PolicyPlan:
        """Adopt an existing parent decision instead of selecting a new one.

        Nothing here routes and nothing here reserves. The decision, the
        reservation, the session, the seat, the workload, the target machine and
        the immutable source revision already exist; this reads them live from
        the Fleet Hub, checks that the local configuration can honour them, and
        writes down what it adopted. The parent's consumer stays the policy
        consumer and `t3-fleet` stays the execution adapter the receipt names,
        so this client never becomes a second defaults layer.
        """
        if not self.config.policy.enrolled:
            raise PolicyRejected(
                "adopting a fleet delegation requires policy enrollment",
                reason="delegation-not-enrolled",
            )
        if override_reason is not None:
            raise ValidationError(
                "override_reason does not apply to an adopted delegation; the "
                "placement decision already exists"
            )
        if request.get("request_id") is not None:
            raise ValidationError(
                "an adopted request derives its own execution identity"
            )
        receipt = load_delegation_receipt(
            self._policy.delegation(delegation_id=delegation_id),
            now=self._policy_now(),
        )
        if receipt["delegation_id"] != delegation_id:
            raise PolicyRejected(
                "the fleet policy adapter answered for another delegation",
                reason="delegation-invalid",
            )
        repository_id = self._delegated_repository(request, receipt)
        machine = receipt["target_machine"]
        self._confirm_delegated(request, "host", machine)
        self._confirm_delegated(request, "seat", receipt["seat"])
        self._confirm_delegated(request, "workload", receipt["workload"])
        self._confirm_delegated(
            request, "source_revision", receipt["source_revision"]
        )
        identity = self._canonical_identity(repository_id)
        if identity != receipt["repo_identity"]:
            raise PolicyRejected(
                "the configured canonical identity is not the delegated one",
                reason="delegation-repository-unmatched",
            )
        host, repository = self._policy_target(machine, repository_id)
        session_id = receipt["session_id"]
        origin = receipt["origin_node"]
        envelope = self._policy.resolve(
            consumer=DELEGATION_CONSUMER,
            repository=identity,
            session_id=session_id,
            origin=origin,
            decision_id=receipt["decision_id"],
        )
        if (
            envelope["version"] != receipt["policy_version"]
            or envelope["digest"] != receipt["policy_digest"]
        ):
            raise PolicyRevisionConflict(
                "fleet policy changed after this delegation was created"
            )
        self.store.record_policy_envelope(
            session_id,
            consumer=DELEGATION_CONSUMER,
            repository=identity,
            origin=origin,
            workload=receipt["workload"],
            envelope=envelope,
        )
        decision = self._check_decision(
            {
                "decision_id": receipt["decision_id"],
                "policy_version": receipt["policy_version"],
                "policy_digest": receipt["policy_digest"],
                "machine": machine,
                "seat": receipt["seat"],
                "reason": "adopted fleet policy delegation",
                "candidates": [],
                "reservation_id": receipt["reservation_id"],
                "expires_at": receipt["expires_at"],
            },
            envelope,
        )
        self.store.record_policy_decision(session_id, decision)
        request["host"] = machine
        request["seat"] = receipt["seat"]
        request["repository"] = repository_id
        request["source_revision"] = receipt["source_revision"]
        request["request_id"] = self.store.record_delegation(
            delegation_id,
            request_id=self._delegated_request_id(machine, delegation_id),
            parent_request_id=receipt["parent_request_id"],
        )
        return _PolicyPlan(
            session_id=session_id,
            stamp=delegation_stamp(receipt),
            machine=machine,
            seat=receipt["seat"],
            host=host,
            repository=repository,
            delegation_id=delegation_id,
        )

    @staticmethod
    def _model_credential(record: RequestRecord) -> dict[str, Any]:
        """What this controller may relay to the target for a continuation.

        Under enrollment it is the plan, because the controller never held an
        admission for this request: the target holds the admission it was
        granted at launch and revalidates it against this plan. Off enrollment
        it stays the controller admission the pre-policy contract used.
        """
        if record.plan is not None:
            return {"model_plan": record.plan}
        return {"model_admission": record.admission}

    def _record_reservation_state(
        self,
        record: RequestRecord,
        response: dict[str, Any] | None,
    ) -> None:
        """Record the reservation state the authenticated target reported.

        Reservation ownership is fenced by the target's node identity, so this
        controller never renews or releases one itself: it relays status and
        writes down what came back. Silence, a transport failure, or a reply
        about another decision all leave the capacity counted, and a released
        reservation is never resurrected.
        """
        if not self.config.policy.enrolled or not isinstance(response, dict):
            return
        reservation = response.get("fleet_reservation")
        if not isinstance(reservation, dict) or reservation.get("state") != "released":
            return
        session = self.store.policy_session_for_request(record.request_id)
        if session is None or session.decision is None:
            return
        if session.reservation_state != "active":
            return
        decision = session.decision
        if (
            reservation.get("reservation_id") != decision["reservation_id"]
            or reservation.get("decision_id") != decision["decision_id"]
        ):
            return
        self.store.set_reservation_state(session.session_id, "released")

    def _dispatch_submit(
        self,
        request: dict[str, Any],
        *,
        prompt: str,
        resume_request_id: str | None = None,
        on_request_id: Callable[[str, str], None] | None = None,
    ) -> dict[str, Any]:
        """One dispatch attempt. This is the whole pre-queue submit path.

        `resume_request_id` is the execution identity a previous attempt for
        this same work already named. It is deliberately not part of the
        payload digest: the identity of the work is the caller's own request,
        and reusing the id is what makes a replayed attempt land on the durable
        record rather than mint a second execution.

        `on_request_id` is called with the request id and the selected host the
        moment both are known and before anything on a target can be mutated,
        so a caller that must survive a crash mid-dispatch has the identity
        written down first.
        """
        if not isinstance(prompt, str) or not prompt:
            raise ValidationError("prompt must be a non-empty string")
        if request.get("run_setup_script", False):
            raise ValidationError(
                "run_setup_script is unavailable through the T3 HTTP dispatch fallback"
            )
        if "model_selection" in request:
            raise ValidationError("caller model_selection is forbidden; use a logical seat")
        # A plan and an admission are both obtained from the Fleet Hub by this
        # controller. Accepting either from a caller would let the caller name
        # the model, so they are refused rather than silently overwritten.
        for forbidden in ("model_admission", "model_plan"):
            if forbidden in request:
                raise ValidationError(
                    f"caller {forbidden} is forbidden; the Fleet Hub decides the model"
                )
        request = dict(request)
        request["execution_context"] = launch_context(
            request,
            controller_id=self.config.controller_id or self.config.controller_host,
            origin_node=self.config.controller_host,
        )
        override_reason = request.pop("override_reason", None)
        delegation_id = self._delegation_id(request)
        # Validated here so a malformed workload is refused before anything is
        # journalled, whether or not this controller is enrolled.
        self._requested_workload(request)
        # The digest is taken from what the caller asked for, before central
        # routing rewrites the host and seat. Hub-chosen placement is ephemeral
        # policy metadata that moves between otherwise identical submissions,
        # so binding it into the identity would turn every policy change into a
        # false idempotency conflict. The explicit workload intent and the
        # delegation are the caller's own and stay in it.
        digest = self._digest(self._caller_payload(request, override_reason), prompt)
        if delegation_id is not None:
            # One adoption is one execution. A replay is answered from the
            # durable receipt before the Fleet Hub is asked anything at all, so
            # a duplicate submit can neither route again nor reserve again.
            binding = self.store.delegation_binding(delegation_id)
            if binding is not None:
                replay = self._durable_replay({"request_id": binding[0]}, digest)
                if replay is not None:
                    return self._result(replay)
        replay = self._durable_replay(request, digest)
        if replay is not None:
            return self._result(replay)
        plan: _PolicyPlan | None = None
        if delegation_id is not None:
            plan = self._adopt_delegation(
                request,
                delegation_id=delegation_id,
                override_reason=override_reason,
            )
            host_id = plan.machine
            host = plan.host
            repository = plan.repository
            repository_id = repository.repository_id
        elif self.config.policy.enrolled:
            try:
                plan = self._plan_policy(request, override_reason)
            except HostUnavailable as error:
                # Everything this call talks to besides the local allowlist is
                # the Fleet Hub, so an otherwise unclassified block here is the
                # Hub being unreachable rather than a target being busy.
                if error.reason is not None:
                    raise
                raise HostUnavailable(
                    error.message, reason="hub-unreachable", detail=error.detail
                ) from error
            host_id = plan.machine
            host = plan.host
            repository = plan.repository
            repository_id = repository.repository_id
            request["host"] = host_id
            request["seat"] = plan.seat
        else:
            if override_reason is not None:
                raise ValidationError(
                    "override_reason requires fleet policy enrollment"
                )
            if request.get("workload") is not None:
                raise ValidationError(
                    "workload requires fleet policy enrollment"
                )
            host_id = self._required(request, "host")
            repository_id = self._required(request, "repository")
            host, repository = self._target(host_id, repository_id)
        if plan is not None and plan.delegation_id is not None:
            # Derived from the delegation above, never minted here and never
            # supplied by the caller, in either coordination mode.
            request_id = self._required(request, "request_id")
        elif self.config.coordination_mode == "target-authoritative":
            provided_id = request.get("request_id")
            if provided_id is not None:
                request_id = self._validate_peer_request_id(provided_id, host_id)
            elif resume_request_id is not None:
                # A replayed attempt for work that already named an identity.
                # Reusing it is what makes the replay land on the durable record
                # rather than start a second execution, so it is kept even
                # though the route was taken again.
                #
                # A target-qualified id belongs to one machine. If this attempt
                # planned onto a different one, the previous attempt's execution
                # cannot be reconciled from here and cannot be proved to have
                # never started, so this is an unknown outcome rather than a
                # malformed request. Re-routing under a fresh id here is exactly
                # the second execution the replay exists to prevent.
                try:
                    request_id = self._validate_peer_request_id(
                        resume_request_id, host_id
                    )
                except ValidationError as error:
                    raise UnknownState(
                        "a previous attempt named an execution on another "
                        "machine; run recover",
                        request_id=resume_request_id,
                    ) from error
                request["request_id"] = request_id
            else:
                request_id = f"{host_id}-{self.config.controller_id}-{self._uuid()}"
                request["request_id"] = request_id
        else:
            request_id = self._required(request, "request_id")
        if on_request_id is not None:
            on_request_id(request_id, host_id)
        seat = self._required(request, "seat")
        admission: dict[str, Any] | None = None
        model_plan: dict[str, Any] | None = None
        if plan is not None:
            # Enrolled dispatch plans, it does not admit. Admission is a launch
            # act and it belongs to the target that runs the turn, immediately
            # before the turn starts and under proof of an acknowledged policy.
            model_plan = normalize_model_plan(
                self._admission.plan(
                    {
                        "consumer": plan.consumer,
                        "seat": seat,
                        **(
                            {"delegation_id": plan.delegation_id}
                            if plan.delegation_id is not None
                            else {}
                        ),
                    }
                )
            )
            request["model_plan"] = model_plan
        else:
            admission = normalize_admission(self._admission.admit({
                "consumer": "t3-fleet", "phase": "controller", "request_id": request_id,
                "seat": seat,
            }))
            request["model_admission"] = admission
        record = self.store.create_request(
            request_id,
            digest,
            host_id,
            repository_id,
            prompt_bytes=len(prompt.encode("utf-8")),
            admission=admission,
            plan=model_plan,
            replan=resume_request_id is not None,
        )
        if plan is not None:
            self.store.link_policy_request(plan.session_id, request_id)
        if record.state not in {"queued", "blocked_offline"}:
            return self._result(record)
        if self.store.circuit_is_open(host_id):
            if record.state == "queued":
                record = self.store.transition(request_id, "blocked_offline")
            raise HostUnavailable(
                f"host circuit is open: {record.host}",
                reason="host-circuit-open",
            )

        self.refresh_submit_health(host_id)
        if self.store.blocking_resolution(host_id) is not None:
            if record.state == "queued":
                self.store.transition(request_id, "blocked_offline")
            raise HostUnavailable("host has an unresolved submit path", reason="host-ineligible")
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
                    "seat",
                    "model_admission",
                    "model_plan",
                    "runtime_mode",
                    "interaction_mode",
                }
            }
            agent_request["execution_context"] = request["execution_context"]
            agent_request["action"] = "claim"
            agent_request["digest"] = digest
            agent_request["prompt"] = prompt
            if plan is not None:
                agent_request["fleet_policy"] = plan.stamp
            try:
                response = self._call(host, agent_request)
            except (PolicyRejected, PolicyRevisionConflict):
                self._fail_unstarted_admission(record)
                raise
            except TargetBusy as error:
                self._requeue_declined(record, error)
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
        except HostUnavailable as error:
            self.store.mark_transport_failure(request_id)
            self.store.record_host_failure(host_id)
            raise HostUnavailable(
                error.message, reason="host-offline", detail=error.detail
            ) from error
        if doctor.get("state") != "eligible":
            self.store.mark_transport_failure(request_id)
            raise HostUnavailable(
                f"host is not eligible: {host_id}", reason="host-ineligible"
            )

        try:
            self._ensure_capacity(record, host)
        except HostUnavailable:
            self.store.transition(request_id, "queued")
            raise

        if not self.store.acquire_lease(
            host_id,
            repository_id,
            request_id,
            ttl=host.repository_lease_ttl_seconds,
            limit=host.repository_limit,
        ):
            self.store.transition(request_id, "queued")
            raise HostUnavailable(
                f"repository is busy on host {host_id} "
                f"({host.repository_limit} concurrent request limit)",
                reason="repository-busy",
            )

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
                "seat",
                "model_admission",
                "model_plan",
                "runtime_mode",
                "interaction_mode",
                "run_setup_script",
            }
        }
        agent_request["execution_context"] = request["execution_context"]
        agent_request["digest"] = digest
        agent_request["action"] = "submit"
        agent_request["prompt"] = prompt
        if plan is not None:
            agent_request["fleet_policy"] = plan.stamp
        try:
            response = self._call(host, agent_request)
        except (PolicyRejected, PolicyRevisionConflict):
            self._fail_unstarted_admission(record)
            raise
        except TargetBusy as error:
            self.store.release_lease(record.host, record.repository, record.request_id)
            self._requeue_declined(record, error)
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

    # -- the local ordinary-work queue ---------------------------------

    def _queue_intent(self, request: dict[str, Any]) -> bool:
        """Whether this caller explicitly asked for the work to be queued.

        There is no implicit default-on path and no config that queues on the
        caller's behalf. `allow_queue` absent means the old behaviour exactly:
        a pre-launch block fails the submit. `allow_queue` false is an explicit
        refusal and is honoured as one even where the queue is available.
        """
        allow_queue = request.get("allow_queue")
        if allow_queue is None:
            return False
        if not isinstance(allow_queue, bool):
            raise ValidationError("allow_queue must be a boolean")
        if not allow_queue:
            return False
        if self._outbox is None or not self.config.outbox.enabled:
            raise ValidationError("the local fleet outbox is disabled on this controller")
        if request.get("delegation_id") is not None:
            # An adopted delegation is one execution that already exists
            # centrally. Deferring it locally would make it two.
            raise ValidationError("an adopted delegation cannot be queued locally")
        return True

    @staticmethod
    def _queued_result(job: OutboxJob, reason: str | None) -> dict[str, Any]:
        """The truthful answer for work that was accepted but not launched.

        `pending_autotick` is deliberately not one of the dispatch states. The
        work has not started, nothing was reserved for it, and the only thing
        that will move it is a later `tick`, so a reader cannot mistake it for
        a request that is queued on a target.
        """
        return {
            "schema": "t3-fleet.result.v1",
            "action": "submit",
            "state": "pending_autotick",
            "needs_attention": False,
            "queue_reason": reason,
            **job.redacted(),
        }

    def submit(
        self,
        request: dict[str, Any],
        *,
        prompt: str,
    ) -> dict[str, Any]:
        """Dispatch this work, or queue it locally when the caller allowed that.

        Queueing is only ever reached from a pre-launch block. Every transport
        loss that happens after a target could have accepted the work is turned
        into an unknown state by the dispatch path itself, so a `HostUnavailable`
        arriving here provably left no execution started and the same work may
        be replayed later without risking a second one.

        The authorization for that later attempt is the one this call already
        had. Deferring the same work does not need a second approval, and it is
        never widened: what is stored is the caller's own request, unchanged.
        """
        queue_allowed = self._queue_intent(request)
        try:
            return self._dispatch_submit(request, prompt=prompt)
        except HostUnavailable as error:
            if not queue_allowed or self._outbox is None:
                raise
            # The caller's request exactly as it arrived. The dispatch path
            # works on its own copy, so a minted execution identity never
            # reaches this dict and there is nothing here to strip; a
            # caller-supplied id is caller intent and stays.
            intent = dict(request)
            job = self._outbox.enqueue(
                digest=self._digest(
                    self._caller_payload(intent, intent.get("override_reason")),
                    prompt,
                ),
                intent=intent,
                prompt=prompt,
                host=request.get("host"),
                repository=request.get("repository"),
            )
            return self._queued_result(job, error.reason)

    def _target_owned_reservations(self) -> list[dict[str, Any]]:
        """Renew the reservations this machine holds as the target that runs them.

        Only a target may move its own reservation, so this walks the local
        target receipts and renews the ones this machine actually owns. A
        request whose outcome is unknown is renewed too and stays counted: the
        fleet never frees capacity it cannot prove was returned. Nothing here
        releases, and nothing here revives a reservation already handed back.
        """
        store = self._target_store
        if store is None:
            return []
        host_id = self.config.controller_host
        results: list[dict[str, Any]] = []
        for receipt in store.active_reservations(target_id=host_id):
            policy = receipt.policy
            if policy is None:
                continue
            entry = {
                "request_id": receipt.request_id,
                "state": receipt.state,
                "reservation_state": receipt.reservation_state,
                "renewed": False,
            }
            try:
                reply = self._policy.renew_reservation(
                    reservation_id=policy["reservation_id"],
                    decision_id=policy["decision_id"],
                    session_id=policy["session_id"],
                )
            except (HostUnavailable, PolicyRejected, PolicyRevisionConflict):
                # A refusal means the reservation is gone and a helper that
                # cannot answer means nothing is known. Neither is a reason to
                # release local capacity or to resurrect what was released.
                results.append(entry)
                continue
            entry["renewed"] = reply.get("state") == "active"
            results.append(entry)
        return results

    def tick(self) -> dict[str, Any]:
        """Drain a bounded slice of the local queue and renew what this machine owns.

        One owner at a time. The tick lease is taken first, so a second tick
        that overlaps this one reports that it is busy rather than racing it,
        and every write inside the drain additionally carries the per-job lease
        generation it claimed. A tick that is slow enough to lose its lease
        cannot overwrite what a newer tick already decided.

        Each job is revalidated from scratch against live policy, the live
        roster, and live capacity, because the dispatch path it goes back
        through is the same one an interactive submit uses. Nothing here trusts
        the machine a previous attempt was routed to.
        """
        outbox = self._outbox
        if outbox is None or not self.config.outbox.enabled:
            raise ValidationError("the local fleet outbox is disabled on this controller")
        owner = f"tick-{self._uuid()}"
        if not outbox.acquire_tick(owner):
            status = outbox.tick_status()
            return {
                "schema": "t3-fleet.result.v1",
                "action": "tick",
                "state": "busy",
                "examined": 0,
                "dispatched": [],
                "blocked": [],
                "held": [],
                "expired": [],
                "reservations": [],
                "truncated": False,
                **status,
            }
        limits = self.config.outbox
        buckets: dict[str, list[dict[str, Any]]] = {
            "dispatched": [],
            "blocked": [],
            "held": [],
            "expired": [],
        }
        examined = 0
        truncated = False
        failure_reason: str | None = None
        try:
            expired_before = {job.job_id for job in outbox.jobs(state="expired")}
            outbox.purge_expired()
            for job in outbox.jobs(state="expired"):
                if job.job_id not in expired_before:
                    buckets["expired"].append({**job.redacted(), "queue_reason": None})
            deadline = self._monotonic() + limits.max_tick_seconds
            for candidate in outbox.ready(limit=limits.max_jobs_per_tick):
                if self._monotonic() >= deadline:
                    truncated = True
                    break
                claimed = outbox.claim(candidate.job_id, owner)
                if claimed is None:
                    continue
                job, generation = claimed
                examined += 1
                bucket, entry = self._drain_job(job, generation, owner)
                buckets[bucket].append(entry)
        except Exception as error:  # noqa: BLE001 - recorded, then re-raised
            failure_reason = (
                error.code if isinstance(error, FleetError) else "internal_error"
            )
            outbox.finish_tick(owner, failure_reason=failure_reason)
            raise
        reservations = self._target_owned_reservations()
        outbox.finish_tick(owner)
        return {
            "schema": "t3-fleet.result.v1",
            "action": "tick",
            "state": "eligible",
            "examined": examined,
            "truncated": truncated,
            "reservations": reservations,
            **{key: value for key, value in buckets.items()},
            **outbox.tick_status(),
        }

    def _drain_job(
        self,
        job: OutboxJob,
        generation: int,
        owner: str,
    ) -> tuple[str, dict[str, Any]]:
        """One fenced dispatch attempt for one queued job.

        The stored intent is replayed exactly as the caller wrote it. The only
        thing carried over from a previous attempt is the execution identity it
        named, and that exists so a replay lands on the same durable request
        rather than starting a second one.
        """
        outbox = self._outbox
        assert outbox is not None
        payload = outbox.payload(job.job_id)
        if payload is None:
            # Retention already took the payload, so there is nothing left to
            # replay. This says only that this client stopped holding the
            # prompt; it is not a statement about any execution.
            updated = outbox.mark_failed(
                job.job_id, generation, owner, disposition="failed-invalid"
            )
            return "blocked", {**updated.redacted(), "queue_reason": None}
        request = dict(payload.intent)
        recorded: dict[str, str | None] = {"request_id": job.request_id, "host": None}

        def remember(request_id: str, host_id: str) -> None:
            recorded["request_id"] = request_id
            recorded["host"] = host_id
            outbox.record_execution_identity(
                job.job_id, generation, owner, request_id=request_id, host=host_id
            )

        try:
            result = self._dispatch_submit(
                request,
                prompt=payload.prompt,
                resume_request_id=job.request_id,
                on_request_id=remember,
            )
        except UnknownState as error:
            updated = outbox.mark_held(
                job.job_id,
                generation,
                owner,
                request_id=error.request_id or recorded["request_id"],
                host=recorded["host"],
            )
            return "held", {**updated.redacted(), "queue_reason": None}
        except HostUnavailable as error:
            updated = outbox.reschedule(
                job.job_id, generation, owner, host=recorded["host"]
            )
            return "blocked", {**updated.redacted(), "queue_reason": error.reason}
        except PolicyRejected as error:
            updated = outbox.mark_failed(
                job.job_id, generation, owner, disposition="failed-policy"
            )
            return "blocked", {**updated.redacted(), "queue_reason": error.reason}
        except PolicyRevisionConflict:
            updated = outbox.mark_failed(
                job.job_id, generation, owner, disposition="failed-policy"
            )
            return "blocked", {**updated.redacted(), "queue_reason": None}
        except IdempotencyConflict:
            updated = outbox.mark_failed(
                job.job_id, generation, owner, disposition="failed-conflict"
            )
            return "blocked", {**updated.redacted(), "queue_reason": None}
        except ValidationError:
            updated = outbox.mark_failed(
                job.job_id, generation, owner, disposition="failed-invalid"
            )
            return "blocked", {**updated.redacted(), "queue_reason": None}
        state = result.get("state")
        request_id = result.get("request_id") or recorded["request_id"]
        host_id = result.get("host") or recorded["host"]
        if state in {"running", "needs_attention", "succeeded"}:
            updated = outbox.mark_dispatched(
                job.job_id,
                generation,
                owner,
                request_id=request_id,
                host=host_id,
            )
            return "dispatched", {**updated.redacted(), "queue_reason": None}
        # The dispatch path returned without an error and without a started
        # turn, which is exactly the shape a replayed attempt takes when a
        # previous one may already have been accepted. It is held, not retried.
        updated = outbox.mark_held(
            job.job_id, generation, owner, request_id=request_id, host=host_id
        )
        return "held", {**updated.redacted(), "queue_reason": None}

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
                # The request still holds its repository whether or not its
                # lease row outlived the lease lifetime, so renew it without a
                # limit check instead of failing a healthy request.
                self.store.renew_lease(
                    record.host,
                    record.repository,
                    record.request_id,
                    ttl=host.repository_lease_ttl_seconds,
                )
        self._record_reservation_state(record, response)
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
                            **self._model_credential(record),
                        },
                    )
                except (PolicyRejected, PolicyRevisionConflict):
                    raise
                except TargetBusy as error:
                    # The target reserved nothing, so the record keeps its
                    # terminal state and the caller may retry.
                    self.store.record_host_success(record.host)
                    raise HostUnavailable(
                        f"target declined the follow-up before starting it: {record.host}",
                        reason=error.reason,
                        detail=error.detail,
                    ) from error
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
                    ttl=host.repository_lease_ttl_seconds,
                    repository_limit=host.repository_limit,
                )
        elif record.state == "needs_attention":
            raise ValidationError(
                "cannot follow up request in state needs_attention: it is waiting on "
                "a pending request. Read its id from pending_request in `t3-fleet "
                "status`, then use `t3-fleet answer` (input_id) for user input or "
                "`t3-fleet approve` (approval_id) for an approval"
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
                    **self._model_credential(record),
                },
            )
        except (PolicyRejected, PolicyRevisionConflict):
            raise
        except HostUnavailable as error:
            self._raise_unknown_after_mutation(
                record,
                action="follow-up",
                error=error,
            )
        self.store.record_host_success(record.host)
        if record.state == "dispatching":
            record = self.store.transition(request_id, "running")
        if response.get("state") in {"running", "needs_attention"}:
            self.store.record_diagnostics(request_id, resolution=response.get("resolution"))
            record = self.store.get(request_id)
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

    def settle_sweep(self) -> dict[str, Any]:
        """Settle the T3 threads of fleet requests that finished a while ago.

        Reads only this controller's ledger and asks each target to settle: the
        target re-checks the live thread (terminal, not pinned, one of its own
        receipts), so the ledger never has to be right about anything but "this
        request finished before the grace period". A receipt is written per
        thread that reached a final answer; everything else, including an
        unreachable host, is simply asked again on the next sweep.
        """
        settings = self.config.settle
        result: dict[str, Any] = {
            "schema": "t3-fleet.result.v1",
            "action": "settle-sweep",
            "state": "disabled" if not settings.enabled else "complete",
            "examined": 0,
            "settled": [],
            "skipped": [],
            "unavailable": [],
            "errors": [],
        }
        if not settings.enabled:
            return result
        cutoff = self._wall_clock() - settings.grace_minutes * 60
        failures: dict[str, int] = {}
        unavailable: set[str] = set()
        for record in self.store.settle_candidates(
            states=TERMINAL_STATES, terminal_before=cutoff
        ):
            if record.host in unavailable or record.thread_id is None:
                continue
            result["examined"] += 1
            try:
                host, _ = self._target(record.host, record.repository)
                response = self._call(
                    host,
                    {
                        "action": "settle",
                        "request_id": record.request_id,
                        "thread_id": record.thread_id,
                        "unlink_pull_requests": settings.unlink_pull_requests,
                    },
                )
            except HostUnavailable:
                failures[record.host] = failures.get(record.host, 0) + 1
                if failures[record.host] >= _SETTLE_HOST_FAILURE_LIMIT:
                    unavailable.add(record.host)
                result["skipped"].append(
                    {"request_id": record.request_id, "reason": "host_unavailable"}
                )
                continue
            except ValidationError:
                result["errors"].append(
                    {"request_id": record.request_id, "code": "invalid_request"}
                )
                continue
            failures[record.host] = 0
            outcome = response.get("outcome")
            if (
                response.get("request_id") != record.request_id
                or response.get("thread_id") != record.thread_id
                or not isinstance(outcome, str)
            ):
                result["errors"].append(
                    {"request_id": record.request_id, "code": "invalid_response"}
                )
                continue
            if outcome not in _SETTLE_DONE:
                reason = response.get("reason")
                result["skipped"].append(
                    {
                        "request_id": record.request_id,
                        "reason": reason if reason in _SETTLE_REFUSALS else "refused",
                    }
                )
                continue
            sequence = response.get("sequence")
            unlinked = response.get("unlinked")
            receipt = self.store.record_settle(
                record,
                outcome=outcome,
                sequence=sequence if type(sequence) is int else None,
                unlinked=unlinked if type(unlinked) is int and unlinked >= 0 else 0,
            )
            result["settled"].append(
                {
                    "request_id": record.request_id,
                    "host": record.host,
                    "thread_id": record.thread_id,
                    "outcome": receipt.outcome,
                    "unlinked": receipt.unlinked,
                }
            )
        result["unavailable"] = sorted(unavailable)
        if result["unavailable"] or result["errors"]:
            result["state"] = "partial"
        return result

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
                    "request_id": request_id,
                    "thread_id": record.thread_id,
                    **self._model_credential(record),
                    "approval_id": approval_id,
                    "decision": decision,
                },
            )
        except (PolicyRejected, PolicyRevisionConflict):
            raise
        except HostUnavailable as error:
            if error.not_started:
                self._raise_not_delivered(record, action="approval", error=error)
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
                    "request_id": request_id,
                    "thread_id": record.thread_id,
                    **self._model_credential(record),
                    "input_id": input_id,
                    "answers": answers,
                },
            )
        except (PolicyRejected, PolicyRevisionConflict):
            raise
        except HostUnavailable as error:
            if error.not_started:
                self._raise_not_delivered(record, action="user-input response", error=error)
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
        if record.state == "failed" and record.target_error is not None and record.thread_id is None:
            return self._result(record)
        host, _ = self._target(record.host, record.repository)
        peer_mode = self.config.coordination_mode == "target-authoritative"
        action = "reconcile" if peer_mode else "recover"
        recovery_request: dict[str, Any] = {
            "action": action,
            "contain": True,
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
                self._record_reservation_state(record, response)
                return self._result(record, response)
            if record.state == "unknown":
                if (
                    self._wall_clock() - record.updated_at
                    >= _MISSING_RECEIPT_GRACE_SECONDS
                ):
                    record = self.store.transition(record.request_id, "failed")
                self._record_reservation_state(record, response)
                return self._result(record, response)
        record = self._refresh_from_target_response(
            record,
            response,
            peer_mode=peer_mode,
        )
        if not peer_mode and record.state in TERMINAL_STATES:
            self.store.release_lease(record.host, record.repository, record.request_id)
        self._record_reservation_state(record, response)
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
