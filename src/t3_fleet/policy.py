"""Fleet Hub policy authority, reached only through the Brigade CLI.

The Hub is authoritative for policy resolution, machine and seat selection, and
session acknowledgement. This module is the one narrow subprocess adapter that
speaks to it. It never opens a Hub HTTP connection, never ranks candidates
locally, and never passes prompt text, repository content, or credentials to
the helper: every argument is an identifier, a version, or a digest.
"""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Protocol

from .errors import (
    HostUnavailable,
    PolicyRejected,
    PolicyRevisionConflict,
    ValidationError,
)

POLICY_SCHEMA = "brigade.fleet_policy.v1"
ROUTE_SCHEMA = "brigade.fleet_route.v1"
ACK_SCHEMA = "brigade.fleet_policy_session.v1"
RESERVATION_SCHEMA = "brigade.fleet_reservation.v1"
DELEGATION_SCHEMA = "brigade.fleet_policy_delegation.v1"
STAMP_SCHEMA = "t3-fleet.fleet-policy.v1"

CONSUMER = "t3-fleet"

# A delegated request is executed under the parent's policy consumer, with
# `t3-fleet` named only as the execution adapter. This client never invents a
# third consumer layer and never lets a caller choose either value: both come
# from a receipt the Fleet Hub authored.
DELEGATION_CONSUMER = "brigade-run"
DELEGATION_ADAPTER = "t3-fleet"

# The stable marker a target raises when a resumed request can no longer prove
# it holds live central admission. It is not a new protocol: it says the old
# reservation is gone and a new one has to be negotiated before this request
# can run again. Over the wire the target error envelope normalizes it to the
# `policy_rejected` code, so the exact string is the target-local contract.
CONTINUATION_ADMISSION_REQUIRED = "continuation-admission-required"

_MAX_RESPONSE_BYTES = 256 * 1024
_MAX_CLOCK_SKEW = timedelta(seconds=30)
_MAX_DECISION_LIFETIME = timedelta(hours=24)
_MAX_ROLES = 64
_MAX_EXECUTION_KEYS = 32
_MAX_DATA_KEYS = 32
_MAX_SOURCE_LEAVES = 256
_MAX_SOURCE_CHAIN = 32
_MAX_CANDIDATES = 64
_MAX_TEXT = 512
# Prepared turn instructions are longer than an identifier but still bounded:
# they are canonical text the Hub authored for this exact selection, not a
# free channel back into the provider turn.
_MAX_INSTRUCTIONS = 8192
_MAX_ACK_AGE = timedelta(minutes=10)

_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_SEAT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_DECISION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
# An immutable git commit, exactly as the parent pinned it. Nothing here
# shortens it, resolves it, or guesses it from a local branch.
_SOURCE_REVISION = re.compile(r"^[0-9a-f]{40}$")
_LEAF = re.compile(r"^[a-z0-9_]+(?:\.[a-z0-9_]+){0,7}$")
_REASON = re.compile(r"^[A-Za-z0-9 ,.'()_-]{1,200}$")
# A repository argument is either the canonical remote identity the controller
# is configured with, or the absolute checkout path a target actually holds.
# Brigade normalizes an existing path to the canonical identity on its side, so
# both forms address the same repository; neither may carry shell syntax.
_REPO_ARGUMENT = re.compile(r"^[A-Za-z0-9/\\][A-Za-z0-9._/\\@:+-]{0,255}$")
_REPO_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/@:-]{0,255}$")
# A provider, native model, or provider instance id as the roster spells it.
# Delimiters that real model names use are allowed and are never translated;
# shell syntax and whitespace are not.
_SELECTION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+-]{0,127}$")
# Mirrors the Fleet Hub's own credential guard. It matches credential *syntax*
# rather than credential-sounding words, so a repository legitimately named
# something like `tokenjuice` is not refused while `user:pass@host/x` is.
_CREDENTIAL_SYNTAX = re.compile(r"(?i)(://|token=|password=|[^/]+:[^/@]+@)")
_CREDENTIAL_WORD = re.compile(r"(token|secret|password|credential|private[_-]?key)", re.I)

_DENIAL_CODES = frozenset(
    {
        "auth-failed",
        "policy_denied",
        "policy_rejected",
        "denied",
        "forbidden",
        "not_enrolled",
        "no_candidate",
        "no_eligible_machine",
        "capability_denied",
        "reservation_denied",
    }
)
_CONFLICT_CODES = frozenset(
    {
        "revision-conflict",
        "policy_version_conflict",
        "policy_revision_conflict",
        "version_conflict",
        "stale_policy",
        "stale_decision",
    }
)

Runner = Callable[..., subprocess.CompletedProcess[str]]


class FleetPolicyProtocol(Protocol):
    def resolve(
        self,
        *,
        consumer: str,
        repository: str,
        session_id: str,
        origin: str,
        decision_id: str | None = None,
    ) -> dict[str, Any]: ...

    def route(
        self,
        *,
        consumer: str,
        repository: str,
        session_id: str,
        origin: str,
        workload: str,
        machine: str | None = None,
        seat: str | None = None,
        override_reason: str | None = None,
        decision_id: str | None = None,
    ) -> dict[str, Any]: ...

    def prepare(
        self,
        *,
        consumer: str,
        repository: str,
        session_id: str,
        origin: str,
        provider: str,
        model: str,
        instance_id: str,
        decision_id: str,
        reasoning: str | None = None,
        delegation_id: str | None = None,
    ) -> dict[str, Any]: ...

    def acknowledge(
        self,
        *,
        session_id: str,
        consumer: str,
        repository: str,
        version: int,
        digest: str,
        context_hash: str | None = None,
    ) -> dict[str, Any]: ...

    def delegation(self, *, delegation_id: str) -> dict[str, Any]: ...

    def renew_reservation(
        self,
        *,
        reservation_id: str,
        decision_id: str,
        session_id: str,
    ) -> dict[str, Any]: ...

    def release_reservation(
        self,
        *,
        reservation_id: str,
        decision_id: str,
        session_id: str,
    ) -> dict[str, Any]: ...


def _identifier(value: object, label: str, pattern: re.Pattern[str] = _ID) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValidationError(f"fleet policy {label} is invalid")
    return value


def _repository_argument(value: object, label: str = "repository") -> str:
    """Validate a `--repo` argument without deciding what it means.

    Resolving a checkout path to a canonical identity is the root's job, done by
    its own identity resolver. This client only refuses an argument that is
    unbounded, carries a credential, or could be read as shell syntax.
    """
    if not isinstance(value, str) or not _REPO_ARGUMENT.fullmatch(value):
        raise ValidationError(f"fleet policy {label} is invalid")
    if _CREDENTIAL_SYNTAX.search(value):
        raise ValidationError(f"fleet policy {label} must not carry a credential")
    return value


def _repo_identity(value: object, label: str = "repo identity") -> str:
    """Validate a canonical repository identity, never a checkout path."""
    if not isinstance(value, str) or not _REPO_IDENTITY.fullmatch(value):
        raise ValidationError(f"fleet policy {label} is invalid")
    if _CREDENTIAL_SYNTAX.search(value):
        raise ValidationError(f"fleet policy {label} must not carry a credential")
    return value


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValidationError(f"fleet policy {label} must be a positive integer")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise ValidationError(f"fleet policy {label} must be a sha256 digest")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or len(value) > _MAX_TEXT:
        raise ValidationError(f"fleet policy {label} is invalid")
    return value


def _optional_text(value: object, label: str) -> str | None:
    """Bounded text, or null. Brigade uses null for "no layer set this"."""
    if value is None:
        return None
    return _text(value, label)


def _known_keys(value: object, allowed: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"fleet policy {label} must be an object")
    unknown = set(value) - allowed
    if unknown:
        raise ValidationError(f"fleet policy {label} contains unsupported fields")
    return value


def _scalar(value: object, label: str) -> Any:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, str) and len(value) <= _MAX_TEXT:
        return value
    raise ValidationError(f"fleet policy {label} must be a bounded scalar")


def _expiry(value: object, label: str) -> tuple[str, datetime]:
    if not isinstance(value, str) or len(value) > 64:
        raise ValidationError(f"fleet policy {label} must be an ISO 8601 instant")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValidationError(f"fleet policy {label} must be an ISO 8601 instant") from error
    if parsed.tzinfo is None:
        raise ValidationError(f"fleet policy {label} requires a timezone offset")
    return value, parsed.astimezone(UTC)


def normalize_policy_envelope(value: object) -> dict[str, Any]:
    """Validate one `brigade.fleet_policy.v1` document from the Brigade CLI."""
    envelope = _known_keys(
        value,
        {"schema", "version", "digest", "effective", "sources", "ack_required"},
        "envelope",
    )
    if envelope.get("schema") != POLICY_SCHEMA:
        raise ValidationError("fleet policy envelope has an unsupported schema")
    if envelope.get("ack_required") is not True:
        raise ValidationError("fleet policy envelope must require acknowledgement")
    effective = _known_keys(
        envelope.get("effective"), {"roles", "execution", "data"}, "effective policy"
    )
    roles_raw = effective.get("roles")
    if not isinstance(roles_raw, dict) or len(roles_raw) > _MAX_ROLES:
        raise ValidationError("fleet policy roles must be a bounded object")
    roles = {
        _identifier(role, "role"): _identifier(seat, "seat", _SEAT)
        for role, seat in roles_raw.items()
    }
    execution_raw = effective.get("execution")
    if not isinstance(execution_raw, dict) or len(execution_raw) > _MAX_EXECUTION_KEYS:
        raise ValidationError("fleet policy execution must be a bounded object")
    execution = {
        _identifier(key, "execution key"): _scalar(item, f"execution.{key}")
        for key, item in execution_raw.items()
    }
    if "machine" in execution and execution["machine"] is not None:
        _identifier(execution["machine"], "execution machine")
    data_raw = effective.get("data")
    if not isinstance(data_raw, dict) or len(data_raw) > _MAX_DATA_KEYS:
        raise ValidationError("fleet policy data must be a bounded object")
    data = {
        _identifier(key, "data key"): _scalar(item, f"data.{key}")
        for key, item in data_raw.items()
    }
    if not isinstance(data.get("allow_training"), bool):
        raise ValidationError("fleet policy data requires a boolean allow_training")
    return {
        "version": _positive_int(envelope.get("version"), "version"),
        "digest": _digest(envelope.get("digest"), "digest"),
        "roles": roles,
        "execution": execution,
        "data": data,
        "sources": _normalize_sources(envelope.get("sources")),
        "ack_required": True,
    }


def normalize_prepared_session(value: object) -> dict[str, Any]:
    """Validate one decision-scoped `session prepare` reply.

    A prepared session is the ordinary policy envelope plus the facts a plain
    resolve cannot carry: the canonical repository the Hub resolved for itself,
    the exact selection it is preparing, the instructions it wants on the
    policy-bearing turn, and the digest of that whole prepared context. The
    envelope half is handed to the same strict reader a plain resolve uses, so
    `resolve` does not have to be widened to tolerate the extra keys.

    `selected.model` is the canonical roster identity. `selected.launch_model`
    is the one optional field this parser permits, and it is the exact native
    provider identity the turn is launched on. It is validated as a selection
    and never derived: no delimiter is translated and no alias is inferred in
    either direction. When it is absent the canonical identity is used verbatim.

    `context_hash` is optional here for compatibility parsing. An enrolled
    preparation requires it at the point of use, where a missing or malformed
    digest fails closed before anything is applied, acknowledged, or launched.
    """
    if not isinstance(value, dict):
        raise ValidationError("fleet policy prepared session must be an object")
    required_extra = {"repo_identity", "selected", "instructions"}
    extra = required_extra | {"context_hash"}
    missing = required_extra - set(value)
    if missing:
        raise ValidationError("fleet policy prepared session is incomplete")
    envelope = normalize_policy_envelope(
        {key: item for key, item in value.items() if key not in extra}
    )
    selected = _known_keys(
        value.get("selected"),
        {"seat", "provider", "model", "reasoning", "instance_id", "launch_model"},
        "prepared selection",
    )
    if {"seat", "provider", "model", "reasoning", "instance_id"} - set(selected):
        raise ValidationError("fleet policy prepared selection is incomplete")
    instructions = value.get("instructions")
    if (
        not isinstance(instructions, str)
        or not instructions
        or len(instructions) > _MAX_INSTRUCTIONS
    ):
        raise ValidationError("fleet policy prepared instructions are invalid")
    launch_model = selected.get("launch_model")
    context_hash = value.get("context_hash")
    return {
        **envelope,
        **(
            {"context_hash": _digest(context_hash, "prepared context hash")}
            if context_hash is not None
            else {}
        ),
        "repo_identity": _repo_identity(value.get("repo_identity")),
        "selected": {
            "seat": _identifier(selected.get("seat"), "prepared seat", _SEAT),
            "provider": _selection(selected.get("provider"), "prepared provider"),
            "model": _selection(selected.get("model"), "prepared model"),
            "reasoning": _selection(selected.get("reasoning"), "prepared reasoning"),
            "instance_id": _selection(
                selected.get("instance_id"), "prepared instance id"
            ),
            **(
                {
                    "launch_model": _selection(
                        launch_model, "prepared launch model"
                    )
                }
                if launch_model is not None
                else {}
            ),
        },
        "instructions": instructions,
    }


def prepared_launch_model(prepared: dict[str, Any]) -> str:
    """The exact native model a prepared session says the turn is launched on.

    An explicit `selected.launch_model` is authoritative. When the Hub named
    none, the canonical identity is used verbatim; nothing is inferred from a
    delimiter or a slug in either direction.
    """
    selected = prepared["selected"]
    return selected.get("launch_model", selected["model"])


def prepared_context(prepared: dict[str, Any]) -> dict[str, Any]:
    """The canonical policy document half of a prepared session."""
    return policy_context(prepared)


def _selection(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SELECTION.fullmatch(value):
        raise ValidationError(f"fleet policy {label} is invalid")
    return value


def _normalize_sources(value: object) -> dict[str, Any]:
    """Validate the per-leaf provenance Brigade's resolver actually emits.

    A leaf is `{layer, value, chain}`. The resolved `value` is required and is
    carried through unchanged, because it is not always recoverable from the
    chain: when a hard constraint denies an override, the chain still shows the
    requested value while `value` holds the one the fleet is governed by. The
    winning `layer` is null for a path no layer ever set. Unknown fields, in the
    leaf or in a chain step, are still refused.
    """
    if value is None:
        return {}
    if not isinstance(value, dict) or len(value) > _MAX_SOURCE_LEAVES:
        raise ValidationError("fleet policy sources must be a bounded object")
    sources: dict[str, Any] = {}
    for leaf, entry in value.items():
        if not isinstance(leaf, str) or not _LEAF.fullmatch(leaf):
            raise ValidationError("fleet policy sources use dotted leaf keys")
        record = _known_keys(entry, {"layer", "value", "chain"}, f"source {leaf}")
        if {"layer", "value", "chain"} - set(record):
            raise ValidationError(f"fleet policy source {leaf} is incomplete")
        chain_raw = record.get("chain")
        if not isinstance(chain_raw, list) or len(chain_raw) > _MAX_SOURCE_CHAIN:
            raise ValidationError("fleet policy source chain must be a bounded array")
        chain = []
        for step in chain_raw:
            item = _known_keys(step, {"layer", "action", "value"}, "source chain step")
            if {"layer", "action", "value"} - set(item):
                raise ValidationError("fleet policy source chain step is incomplete")
            chain.append(
                {
                    "layer": _text(item.get("layer"), "source layer"),
                    "action": _text(item.get("action"), "source action"),
                    "value": _scalar(item.get("value"), "source value"),
                }
            )
        sources[leaf] = {
            "layer": _optional_text(record.get("layer"), "source layer"),
            "value": _scalar(record.get("value"), f"source {leaf} value"),
            "chain": chain,
        }
    return sources


def normalize_route_decision(value: object) -> dict[str, Any]:
    """Validate one `brigade.fleet_route.v1` decision from the Brigade CLI."""
    decision = _known_keys(
        value,
        {
            "schema",
            "decision_id",
            "policy_version",
            "policy_digest",
            "selected",
            "reason",
            "candidates",
            "reservation_id",
            "expires_at",
        },
        "route decision",
    )
    if decision.get("schema") != ROUTE_SCHEMA:
        raise ValidationError("fleet route decision has an unsupported schema")
    selected = _known_keys(decision.get("selected"), {"machine", "seat"}, "selection")
    expires_at, _ = _expiry(decision.get("expires_at"), "decision expires_at")
    return {
        "decision_id": _identifier(
            decision.get("decision_id"), "decision id", _DECISION_ID
        ),
        "policy_version": _positive_int(decision.get("policy_version"), "policy version"),
        "policy_digest": _digest(decision.get("policy_digest"), "policy digest"),
        "machine": _identifier(selected.get("machine"), "selected machine"),
        "seat": _identifier(selected.get("seat"), "selected seat", _SEAT),
        "reason": _text(decision.get("reason"), "decision reason"),
        "candidates": _normalize_candidates(decision.get("candidates")),
        "reservation_id": _identifier(
            decision.get("reservation_id"), "reservation id", _DECISION_ID
        ),
        "expires_at": expires_at,
    }


def _normalize_candidates(value: object) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > _MAX_CANDIDATES:
        raise ValidationError("fleet route candidates must be a bounded array")
    candidates: list[dict[str, Any]] = []
    for entry in value:
        if isinstance(entry, str):
            candidates.append({"machine": _identifier(entry, "candidate machine")})
            continue
        item = _known_keys(
            entry, {"machine", "seat", "reason", "eligible"}, "route candidate"
        )
        candidate: dict[str, Any] = {
            "machine": _identifier(item.get("machine"), "candidate machine")
        }
        if item.get("seat") is not None:
            candidate["seat"] = _identifier(item["seat"], "candidate seat", _SEAT)
        if item.get("reason") is not None:
            candidate["reason"] = _text(item["reason"], "candidate reason")
        if item.get("eligible") is not None:
            if not isinstance(item["eligible"], bool):
                raise ValidationError("fleet route candidate eligible must be boolean")
            candidate["eligible"] = item["eligible"]
        candidates.append(candidate)
    return candidates


def decision_identity(decision: dict[str, Any]) -> dict[str, Any]:
    """The immutable part of a decision, excluding the ephemeral reservation window.

    `expires_at`, the human `reason`, and the candidate list all move between
    otherwise identical replies, so an idempotency comparison must ignore them.
    """
    return {
        key: decision[key]
        for key in (
            "decision_id",
            "policy_version",
            "policy_digest",
            "machine",
            "seat",
            "reservation_id",
        )
    }


def decision_expiry(decision: dict[str, Any]) -> datetime:
    _, parsed = _expiry(decision.get("expires_at"), "decision expires_at")
    return parsed


def require_fresh_decision(
    decision: dict[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    """Reject an expired decision and an implausibly long reservation window."""
    expiry = decision_expiry(decision)
    current = now.astimezone(UTC)
    if expiry <= current:
        raise PolicyRejected("fleet policy decision has expired")
    if expiry > current + _MAX_DECISION_LIFETIME + _MAX_CLOCK_SKEW:
        raise PolicyRejected("fleet policy decision window exceeds the supported bound")
    return decision


_DELEGATION_FIELDS = {
    "schema",
    "delegation_id",
    "launch_authorized",
    "consumer",
    "adapter",
    "parent_request_id",
    "decision_id",
    "reservation_id",
    "session_id",
    "repo_identity",
    "seat",
    "workload",
    "policy_version",
    "policy_digest",
    "origin_node",
    "target_machine",
    "target_node",
    "source_revision",
    "created_at",
    "expires_at",
}


def normalize_delegation_receipt(value: object) -> dict[str, Any]:
    """Validate one `brigade.fleet_policy_delegation.v1` receipt.

    The receipt is the whole authority for an adopted request: it names the
    parent consumer, the execution adapter, the decision and reservation that
    already exist, the canonical repository, the target machine and node, the
    seat, the workload, and the immutable source revision. Every key is
    mandatory and nothing else is tolerated, so a receipt that has grown an
    extra identity field is refused rather than partially believed.

    `launch_authorized` is stated as false by the Hub and re-checked here. A
    receipt is identity and reservation adoption; it is never permission to
    start a provider turn, and a document that claimed otherwise would be an
    admission wearing a delegation's schema.
    """
    receipt = _known_keys(value, _DELEGATION_FIELDS, "delegation receipt")
    missing = _DELEGATION_FIELDS - set(receipt)
    if missing:
        raise ValidationError("fleet policy delegation receipt is incomplete")
    if receipt.get("schema") != DELEGATION_SCHEMA:
        raise ValidationError("fleet policy delegation has an unsupported schema")
    if receipt.get("launch_authorized") is not False:
        raise ValidationError(
            "fleet policy delegation must not claim launch authorization"
        )
    if receipt.get("consumer") != DELEGATION_CONSUMER:
        raise ValidationError("fleet policy delegation names another consumer")
    if receipt.get("adapter") != DELEGATION_ADAPTER:
        raise ValidationError("fleet policy delegation names another adapter")
    source_revision = receipt.get("source_revision")
    if not isinstance(source_revision, str) or not _SOURCE_REVISION.fullmatch(
        source_revision
    ):
        raise ValidationError(
            "fleet policy delegation requires a full lowercase source revision"
        )
    created_at, _ = _expiry(receipt.get("created_at"), "delegation created_at")
    expires_at, _ = _expiry(receipt.get("expires_at"), "delegation expires_at")
    return {
        "schema": DELEGATION_SCHEMA,
        "delegation_id": _identifier(
            receipt.get("delegation_id"), "delegation id", _DECISION_ID
        ),
        "launch_authorized": False,
        "consumer": DELEGATION_CONSUMER,
        "adapter": DELEGATION_ADAPTER,
        "parent_request_id": _identifier(
            receipt.get("parent_request_id"), "parent request id", _DECISION_ID
        ),
        "decision_id": _identifier(
            receipt.get("decision_id"), "decision id", _DECISION_ID
        ),
        "reservation_id": _identifier(
            receipt.get("reservation_id"), "reservation id", _DECISION_ID
        ),
        "session_id": _identifier(receipt.get("session_id"), "session id"),
        "repo_identity": _repo_identity(receipt.get("repo_identity")),
        "seat": _identifier(receipt.get("seat"), "seat", _SEAT),
        "workload": _identifier(receipt.get("workload"), "workload"),
        "policy_version": _positive_int(receipt.get("policy_version"), "policy version"),
        "policy_digest": _digest(receipt.get("policy_digest"), "policy digest"),
        "origin_node": _identifier(receipt.get("origin_node"), "origin node"),
        "target_machine": _identifier(receipt.get("target_machine"), "target machine"),
        "target_node": _identifier(receipt.get("target_node"), "target node"),
        "source_revision": source_revision,
        "created_at": created_at,
        "expires_at": expires_at,
    }


def require_fresh_delegation(
    receipt: dict[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    """Reject a delegation that has lapsed, or one whose window is implausible.

    The expiry is bounded by the active reservation the parent already holds,
    so the same 24 hour ceiling a decision gets applies here. A `created_at`
    from the future is a clock or a forgery problem either way and is refused
    beyond the bounded skew this client already tolerates.
    """
    _, created = _expiry(receipt.get("created_at"), "delegation created_at")
    _, expiry = _expiry(receipt.get("expires_at"), "delegation expires_at")
    current = now.astimezone(UTC)
    if created > current + _MAX_CLOCK_SKEW:
        raise PolicyRejected("fleet policy delegation was created in the future")
    if expiry <= current:
        raise PolicyRejected("fleet policy delegation has expired")
    if expiry > current + _MAX_DECISION_LIFETIME + _MAX_CLOCK_SKEW:
        raise PolicyRejected(
            "fleet policy delegation window exceeds the supported bound"
        )
    if expiry <= created:
        raise PolicyRejected("fleet policy delegation window is not positive")
    return receipt


def load_delegation_receipt(value: object, *, now: datetime) -> dict[str, Any]:
    """Re-validate a delegation receipt at the point of use.

    The adapter validates what the Brigade CLI returned; this validates it
    again where it is about to be believed, mirroring `load_stored_envelope`
    and `load_stored_prepared`. A newer, buggy, or hostile helper therefore
    cannot widen what an adoption is allowed to mean by returning a shape only
    one layer ever checked.
    """
    return require_fresh_delegation(normalize_delegation_receipt(value), now=now)


def delegation_stamp(receipt: dict[str, Any]) -> dict[str, Any]:
    """Build the target stamp for an adopted request, entirely from the receipt.

    Nothing local is substituted. `origin` is the receipt's canonical
    `origin_node` rather than this controller's own alias, because the Hub
    derives any original alias on its side and a guess here would address the
    wrong node. There is no route call and no new reservation: the decision and
    the reservation this stamp names already existed when the parent created
    the delegation.
    """
    return {
        "schema": STAMP_SCHEMA,
        "session_id": receipt["session_id"],
        "origin": receipt["origin_node"],
        "workload": receipt["workload"],
        "repo_identity": receipt["repo_identity"],
        "version": receipt["policy_version"],
        "digest": receipt["policy_digest"],
        "decision_id": receipt["decision_id"],
        "reservation_id": receipt["reservation_id"],
        "machine": receipt["target_machine"],
        "seat": receipt["seat"],
        "expires_at": receipt["expires_at"],
        "delegation_id": receipt["delegation_id"],
    }


def policy_stamp(
    *,
    session_id: str,
    origin: str,
    workload: str,
    repo_identity: str,
    envelope: dict[str, Any],
    decision: dict[str, Any],
    delegation_id: str | None = None,
) -> dict[str, Any]:
    """Build the safe metadata block the controller hands to the target.

    `repo_identity` is the canonical identity the Hub was asked about, not the
    caller's local alias, so the target can check that the repository it is
    configured for is the one this decision was taken for. `delegation_id` is
    set only for an adopted request, and it is a pointer: the target fetches the
    receipt itself rather than believing anything this stamp restates about it.
    """
    return {
        **(
            {"delegation_id": _identifier(delegation_id, "delegation id", _DECISION_ID)}
            if delegation_id is not None
            else {}
        ),
        "schema": STAMP_SCHEMA,
        "session_id": session_id,
        "origin": origin,
        "workload": workload,
        "repo_identity": _repo_identity(repo_identity),
        "version": envelope["version"],
        "digest": envelope["digest"],
        "decision_id": decision["decision_id"],
        "reservation_id": decision["reservation_id"],
        "machine": decision["machine"],
        "seat": decision["seat"],
        "expires_at": decision["expires_at"],
    }


def normalize_policy_stamp(value: object) -> dict[str, Any]:
    """Validate the policy block that arrived on the target adapter wire."""
    stamp = _known_keys(
        value,
        {
            "schema",
            "session_id",
            "origin",
            "workload",
            "repo_identity",
            "version",
            "digest",
            "decision_id",
            "reservation_id",
            "machine",
            "seat",
            "expires_at",
            "delegation_id",
        },
        "stamp",
    )
    if stamp.get("schema") != STAMP_SCHEMA:
        raise ValidationError("fleet policy stamp has an unsupported schema")
    expires_at, _ = _expiry(stamp.get("expires_at"), "stamp expires_at")
    delegation_id = stamp.get("delegation_id")
    return {
        # A stamp without this key is an ordinary request. A stamp with it is a
        # pointer to a receipt the target still fetches and validates itself.
        **(
            {
                "delegation_id": _identifier(
                    delegation_id, "delegation id", _DECISION_ID
                )
            }
            if delegation_id is not None
            else {}
        ),
        "schema": STAMP_SCHEMA,
        "session_id": _identifier(stamp.get("session_id"), "session id"),
        "origin": _identifier(stamp.get("origin"), "origin"),
        "workload": _identifier(stamp.get("workload"), "workload"),
        "repo_identity": _repo_identity(stamp.get("repo_identity")),
        "version": _positive_int(stamp.get("version"), "version"),
        "digest": _digest(stamp.get("digest"), "digest"),
        "decision_id": _identifier(stamp.get("decision_id"), "decision id", _DECISION_ID),
        "reservation_id": _identifier(
            stamp.get("reservation_id"), "reservation id", _DECISION_ID
        ),
        "machine": _identifier(stamp.get("machine"), "machine"),
        "seat": _identifier(stamp.get("seat"), "seat", _SEAT),
        "expires_at": expires_at,
    }


def require_fresh_stamp(stamp: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    _, expiry = _expiry(stamp.get("expires_at"), "stamp expires_at")
    current = now.astimezone(UTC)
    if expiry <= current:
        raise PolicyRejected("fleet policy decision has expired")
    if expiry > current + _MAX_DECISION_LIFETIME + _MAX_CLOCK_SKEW:
        raise PolicyRejected("fleet policy decision window exceeds the supported bound")
    return stamp


def policy_context(envelope: dict[str, Any]) -> dict[str, Any]:
    """The canonical policy document a target renders into its own turn prefix.

    It is built from the envelope this machine resolved for itself, never from
    anything the caller supplied, so a controller cannot dictate the rules the
    provider is told to follow.
    """
    return {
        "version": envelope["version"],
        "digest": envelope["digest"],
        "effective": {
            "roles": envelope["roles"],
            "execution": envelope["execution"],
            "data": envelope["data"],
        },
        "sources": envelope["sources"],
    }


def policy_receipt(
    stamp: dict[str, Any],
    *,
    acknowledged: bool,
    context_hash: str | None = None,
) -> dict[str, Any]:
    """Safe metadata for a durable receipt: identifiers, version, digest only.

    `context_hash` is the digest of the exact prepared context this target
    applied. It is persisted with the request so a later boundary compares the
    snapshot that actually ran rather than a policy version, which two different
    preparations can share.
    """
    return {
        **(
            {"delegation_id": stamp["delegation_id"]}
            if "delegation_id" in stamp
            else {}
        ),
        **({"context_hash": context_hash} if context_hash is not None else {}),
        "session_id": stamp["session_id"],
        "workload": stamp["workload"],
        "repo_identity": stamp["repo_identity"],
        "version": stamp["version"],
        "digest": stamp["digest"],
        "decision_id": stamp["decision_id"],
        "reservation_id": stamp["reservation_id"],
        "machine": stamp["machine"],
        "seat": stamp["seat"],
        "acknowledged": acknowledged,
    }


def normalize_policy_receipt(value: object) -> dict[str, Any]:
    """Re-validate the durable policy receipt a target keeps for lifecycle calls.

    The reservation verbs are fenced by three identifiers, so the record they
    are read from is validated on the way out of local storage as strictly as
    it was on the way in.
    """
    record = _known_keys(
        value,
        {
            "session_id",
            "workload",
            "repo_identity",
            "version",
            "digest",
            "decision_id",
            "reservation_id",
            "machine",
            "seat",
            "acknowledged",
            "delegation_id",
            "context_hash",
        },
        "policy receipt",
    )
    if not isinstance(record.get("acknowledged"), bool):
        raise ValidationError("fleet policy receipt requires a boolean acknowledged")
    identity = record.get("repo_identity")
    delegation_id = record.get("delegation_id")
    context_hash = record.get("context_hash")
    return {
        **(
            {
                "delegation_id": _identifier(
                    delegation_id, "delegation id", _DECISION_ID
                )
            }
            if delegation_id is not None
            else {}
        ),
        **(
            {"context_hash": _digest(context_hash, "context hash")}
            if context_hash is not None
            else {}
        ),
        "session_id": _identifier(record.get("session_id"), "session id"),
        "workload": _identifier(record.get("workload"), "workload"),
        # Optional on the way in: a receipt written before canonical identity
        # landed still reads back. The continuation path is where a receipt
        # without one fails closed, rather than becoming unreadable here.
        "repo_identity": None if identity is None else _repo_identity(identity),
        "version": _positive_int(record.get("version"), "version"),
        "digest": _digest(record.get("digest"), "digest"),
        "decision_id": _identifier(
            record.get("decision_id"), "decision id", _DECISION_ID
        ),
        "reservation_id": _identifier(
            record.get("reservation_id"), "reservation id", _DECISION_ID
        ),
        "machine": _identifier(record.get("machine"), "machine"),
        "seat": _identifier(record.get("seat"), "seat", _SEAT),
        "acknowledged": record["acknowledged"],
    }


def normalize_session_ack(
    value: object,
    *,
    session_id: str,
    consumer: str,
    repository: str,
    version: int,
    digest: str,
    now: datetime,
    context_hash: str | None = None,
) -> dict[str, Any]:
    """Validate one `brigade.fleet_policy_session.v1` acknowledgement.

    The Hub confirms that the exact policy this session loaded is the current
    one and that the consumer applied it. A bare `{"acknowledged": true}` is
    not that confirmation, so it fails closed rather than being trusted.

    `context_hash` is mandatory in the reply and is the frozen final field of
    this contract. When this client acknowledged a prepared context, the Hub
    must echo that exact digest: a missing key, a null, or any other digest is
    a refusal rather than a weaker success. A null is meaningful only for a
    legacy resolve-only session that loaded no prepared context and therefore
    cannot launch, which is the one case this client sends no digest for.
    """
    ack = _known_keys(
        value,
        {
            "schema",
            "session_id",
            "consumer",
            "repo_identity",
            "version",
            "digest",
            "state",
            "loaded_at",
            "applied",
            "context_hash",
        },
        "session acknowledgement",
    )
    if ack.get("schema") != ACK_SCHEMA:
        raise HostUnavailable("fleet policy adapter returned an invalid acknowledgement")
    missing = {
        "session_id",
        "consumer",
        "repo_identity",
        "version",
        "digest",
        "state",
        "loaded_at",
        "applied",
        "context_hash",
    } - set(ack)
    if missing:
        raise HostUnavailable(
            "fleet policy adapter returned an incomplete acknowledgement"
        )
    loaded_at, loaded = _expiry(ack.get("loaded_at"), "acknowledgement loaded_at")
    current = now.astimezone(UTC)
    if loaded > current + _MAX_CLOCK_SKEW or loaded < current - _MAX_ACK_AGE:
        raise PolicyRejected("fleet policy acknowledgement timestamp is not current")
    if ack["applied"] is not True:
        raise PolicyRejected("fleet policy session acknowledgement was refused")
    if ack["state"] != "current":
        raise PolicyRejected(
            "fleet policy session is not current at the Fleet Hub"
        )
    if ack["session_id"] != session_id:
        raise PolicyRejected("fleet policy acknowledged a different session")
    if ack["consumer"] != consumer:
        raise PolicyRejected("fleet policy acknowledged a different consumer")
    if ack["repo_identity"] != repository:
        raise PolicyRejected("fleet policy acknowledged a different repository")
    if ack["version"] != version or isinstance(ack["version"], bool):
        raise PolicyRejected("fleet policy acknowledged a different policy version")
    if ack["digest"] not in {digest, digest.split(":", 1)[1]}:
        raise PolicyRejected("fleet policy acknowledged a different policy digest")
    acknowledged_context = _acknowledged_context_hash(ack["context_hash"], context_hash)
    return {
        "schema": ACK_SCHEMA,
        "session_id": session_id,
        "consumer": consumer,
        "repo_identity": repository,
        "version": version,
        "digest": digest,
        "state": "current",
        "loaded_at": loaded_at,
        "applied": True,
        "context_hash": acknowledged_context,
    }


def _acknowledged_context_hash(
    acknowledged: object,
    expected: str | None,
) -> str | None:
    """Compare the acknowledged prepared context against the one that was sent.

    An enrolled session applied one exact prepared context and named its digest
    on the way in, so the only acknowledgement it may accept is one that names
    the same digest back. Accepting a null there would let an applied, enforced
    session be acknowledged as though it had loaded nothing, which is the whole
    fact this field exists to carry.
    """
    if expected is None:
        # Legacy resolve-only: nothing was prepared, so there is nothing to
        # echo. The key is still mandatory; only its value may be null, and a
        # digest arriving for a context this client never applied is a refusal.
        if acknowledged is None:
            return None
        raise PolicyRejected(
            "fleet policy acknowledged a prepared context this session never applied"
        )
    if acknowledged is None:
        raise PolicyRejected(
            "fleet policy acknowledged no prepared context for an applied session"
        )
    # Compared as the exact string that was sent, not reparsed into a shape.
    # `expected` is already a validated `sha256:<64 hex>` digest, so an echo
    # that drops the prefix, changes the case, or names another context is one
    # refusal rather than three differently-classified parse failures.
    if acknowledged != expected:
        raise PolicyRejected(
            "fleet policy acknowledged a different prepared context"
        )
    return expected


def normalize_reservation(
    value: object,
    *,
    verb: str,
    reservation_id: str,
    decision_id: str,
    now: datetime,
) -> dict[str, Any]:
    """Validate one `brigade.fleet_reservation.v1` lifecycle reply.

    Fencing and ownership are decided at the Hub. The client only refuses a
    reply that answers for another reservation, another decision, or a state
    the requested verb cannot produce.
    """
    record = _known_keys(
        value,
        {"schema", "reservation_id", "decision_id", "state", "expires_at"},
        "reservation",
    )
    if record.get("schema") != RESERVATION_SCHEMA:
        raise HostUnavailable("fleet reservation adapter returned an invalid reply")
    if {"reservation_id", "decision_id", "state", "expires_at"} - set(record):
        raise HostUnavailable("fleet reservation adapter returned an incomplete reply")
    expires_at, expiry = _expiry(record.get("expires_at"), "reservation expires_at")
    if record["reservation_id"] != reservation_id:
        raise PolicyRejected("fleet reservation reply names another reservation")
    if record["decision_id"] != decision_id:
        raise PolicyRejected("fleet reservation reply names another decision")
    state = record["state"]
    if state not in {"active", "released"}:
        raise HostUnavailable("fleet reservation adapter returned an unknown state")
    if verb == "renew":
        if state != "active":
            raise PolicyRejected("fleet reservation could not be renewed")
        current = now.astimezone(UTC)
        if expiry <= current:
            raise PolicyRejected("fleet reservation renewal returned an expired window")
        if expiry > current + _MAX_DECISION_LIFETIME + _MAX_CLOCK_SKEW:
            raise PolicyRejected(
                "fleet reservation window exceeds the supported bound"
            )
    elif state != "released":
        raise PolicyRejected("fleet reservation is still held after release")
    return {
        "schema": RESERVATION_SCHEMA,
        "reservation_id": reservation_id,
        "decision_id": decision_id,
        "state": state,
        "expires_at": expires_at,
    }


def _override_reason(value: str) -> str:
    if not isinstance(value, str) or not _REASON.fullmatch(value):
        raise ValidationError("fleet policy override reason is invalid")
    if _CREDENTIAL_WORD.search(value):
        raise ValidationError("fleet policy override reason must not carry a credential")
    return value


def _bounded_run(argv: list[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryFile(mode="w+b") as stdout:
        completed = subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
            check=False,
        )
        stdout.seek(0)
        output = stdout.read(_MAX_RESPONSE_BYTES + 1)
    if len(output) > _MAX_RESPONSE_BYTES:
        raise HostUnavailable("fleet policy adapter response exceeds the size limit")
    try:
        decoded = output.decode("utf-8")
    except UnicodeDecodeError as error:
        raise HostUnavailable("fleet policy adapter returned invalid JSON") from error
    return subprocess.CompletedProcess(argv, completed.returncode, stdout=decoded, stderr="")


class BrigadeFleetPolicy:
    """Narrow subprocess adapter for Fleet Hub policy, never Hub HTTP."""

    def __init__(
        self,
        *,
        runner: Runner | None = None,
        timeout: float = 15,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._runner = runner
        self._timeout = timeout
        self._now = now

    def resolve(
        self,
        *,
        consumer: str,
        repository: str,
        session_id: str,
        origin: str,
        decision_id: str | None = None,
    ) -> dict[str, Any]:
        """Resolve the canonical policy, optionally fenced to one decision.

        The controller resolves plainly: it has no decision yet. A target passes
        the decision it was stamped with, so the Hub answers for that exact
        decision and can compare the repository this call names against the one
        the decision was taken for. `repository` is a canonical identity from a
        controller and the actual checkout path from a target; Brigade
        normalizes the path to the canonical identity on its side.
        """
        argv = [
            "brigade",
            "fleet",
            "policy",
            "resolve",
            "--consumer",
            self._session_consumer(consumer),
            "--repo",
            _repository_argument(repository),
            "--session-id",
            _identifier(session_id, "session id"),
            "--origin",
            _identifier(origin, "origin"),
        ]
        if decision_id is not None:
            argv.extend(
                ["--decision-id", _identifier(decision_id, "decision id", _DECISION_ID)]
            )
        argv.append("--json")
        response = self._invoke(argv)
        try:
            return normalize_policy_envelope(response)
        except ValidationError as error:
            raise HostUnavailable(
                "fleet policy adapter returned an invalid policy envelope"
            ) from error

    def route(
        self,
        *,
        consumer: str,
        repository: str,
        session_id: str,
        origin: str,
        workload: str,
        machine: str | None = None,
        seat: str | None = None,
        override_reason: str | None = None,
        decision_id: str | None = None,
    ) -> dict[str, Any]:
        argv = [
            "brigade",
            "fleet",
            "policy",
            "route",
            "--consumer",
            self._session_consumer(consumer),
            "--repo",
            _repository_argument(repository),
            "--session-id",
            _identifier(session_id, "session id"),
            "--origin",
            _identifier(origin, "origin"),
            "--workload",
            _identifier(workload, "workload"),
        ]
        if machine is not None:
            argv.extend(["--machine", _identifier(machine, "machine")])
        if seat is not None:
            argv.extend(["--seat", _identifier(seat, "seat", _SEAT)])
        if override_reason is not None:
            argv.extend(["--override-reason", _override_reason(override_reason)])
        if decision_id is not None:
            argv.extend(
                ["--decision-id", _identifier(decision_id, "decision id", _DECISION_ID)]
            )
        argv.append("--json")
        response = self._invoke(argv)
        try:
            decision = normalize_route_decision(response)
        except ValidationError as error:
            raise HostUnavailable(
                "fleet policy adapter returned an invalid route decision"
            ) from error
        return require_fresh_decision(decision, now=self._now())

    def prepare(
        self,
        *,
        consumer: str,
        repository: str,
        session_id: str,
        origin: str,
        provider: str,
        model: str,
        instance_id: str,
        decision_id: str,
        reasoning: str | None = None,
        delegation_id: str | None = None,
    ) -> dict[str, Any]:
        """Prepare one decision for one exact native selection.

        This is the decision-scoped call, so the Hub retrieves the overrides and
        origin it already validated for that decision rather than believing the
        origin an arbitrary caller passes now. `repository` is this target's
        actual configured checkout path, and `model` is the native model the
        provider is really about to be launched on, so the reply is a statement
        about the run that is about to happen and not about a request shape.

        For an adopted request the call is also delegation-scoped, so the Hub
        validates the delegation before it chooses the adapter binding. The
        adapter is the receipt's, never this client's opinion.
        """
        argv = [
            "brigade",
            "fleet",
            "policy",
            "session",
            "prepare",
            "--consumer",
            self._consumer(consumer, delegated=delegation_id is not None),
            "--repo",
            _repository_argument(repository),
            "--session-id",
            _identifier(session_id, "session id"),
            "--origin",
            _identifier(origin, "origin"),
            "--provider",
            _selection(provider, "provider"),
            "--model",
            _selection(model, "model"),
            "--instance-id",
            _selection(instance_id, "instance id"),
        ]
        if reasoning is not None:
            argv.extend(["--reasoning", _selection(reasoning, "reasoning")])
        argv.extend(
            ["--decision-id", _identifier(decision_id, "decision id", _DECISION_ID)]
        )
        if delegation_id is not None:
            argv.extend(
                [
                    "--delegation-id",
                    _identifier(delegation_id, "delegation id", _DECISION_ID),
                ]
            )
        argv.append("--json")
        response = self._invoke(argv)
        try:
            return normalize_prepared_session(response)
        except ValidationError as error:
            raise HostUnavailable(
                "fleet policy adapter returned an invalid prepared session"
            ) from error

    def acknowledge(
        self,
        *,
        session_id: str,
        consumer: str,
        repository: str,
        version: int,
        digest: str,
        context_hash: str | None = None,
    ) -> dict[str, Any]:
        """Acknowledge the exact prepared context this session applied.

        `context_hash` is the digest the Hub returned with the preparation, sent
        back verbatim. It is never derived here and never rebuilt from a
        timestamp: the Hub compares it under its own transaction, so a
        same-version preparation with a different override or model cannot have
        an older applied context acknowledged in its place.
        """
        session_id = _identifier(session_id, "session id")
        digest = _digest(digest, "digest")
        argv = [
            "brigade",
            "fleet",
            "policy",
            "session",
            "acknowledge",
            "--session-id",
            session_id,
            "--consumer",
            self._session_consumer(consumer),
            "--repo",
            _repository_argument(repository),
            "--version",
            str(_positive_int(version, "version")),
            "--digest",
            digest,
        ]
        if context_hash is not None:
            # The flag name on the acknowledge verb stays `--context-hash`. Only
            # the launch admission spells it `--policy-context-hash`, because
            # there it sits beside the rest of the policy proof.
            context_hash = _digest(context_hash, "context hash")
            argv.extend(["--context-hash", context_hash])
        argv.append("--json")
        response = self._invoke(argv)
        return self._check_acknowledgement(
            response,
            session_id=session_id,
            consumer=consumer,
            repository=repository,
            version=version,
            digest=digest,
            context_hash=context_hash,
        )

    @staticmethod
    def _consumer(value: str, *, delegated: bool) -> str:
        """Pair the consumer with the delegation; neither may travel alone.

        An ordinary call is always `t3-fleet`. A delegation-scoped call is
        always the parent's `brigade-run`, taken from a validated receipt.
        Checking them together is what stops either from being used to smuggle
        the other past this boundary. There is no caller-facing consumer flag
        and no third layer: `t3-fleet` is named as the execution adapter on the
        delegation, never as a second defaults layer under the parent.
        """
        expected = DELEGATION_CONSUMER if delegated else CONSUMER
        if value != expected:
            raise ValidationError("unsupported fleet policy consumer")
        return value

    @staticmethod
    def _session_consumer(value: str) -> str:
        """Either contracted consumer, for a call that carries no delegation flag.

        `resolve`, `route`, and `acknowledge` are session and decision scoped
        rather than delegation scoped, so an adopted session names the parent
        consumer here with no `--delegation-id` beside it. The value is still
        never caller-chosen: it comes from a validated receipt or is fixed.
        """
        if value not in {CONSUMER, DELEGATION_CONSUMER}:
            raise ValidationError("unsupported fleet policy consumer")
        return value

    def _check_acknowledgement(
        self,
        response: dict[str, Any],
        *,
        session_id: str,
        consumer: str,
        repository: str,
        version: int,
        digest: str,
        context_hash: str | None = None,
    ) -> dict[str, Any]:
        try:
            return normalize_session_ack(
                response,
                session_id=session_id,
                consumer=consumer,
                repository=repository,
                version=version,
                digest=digest,
                now=self._now(),
                context_hash=context_hash,
            )
        except ValidationError as error:
            raise HostUnavailable(
                "fleet policy adapter returned an invalid acknowledgement"
            ) from error

    def delegation(self, *, delegation_id: str) -> dict[str, Any]:
        """Read one delegation receipt live, from the authenticated Brigade CLI.

        This is the only way an adopted request gets its authority. There is no
        last-known-good copy, no HTTP path, and no way for a caller to hand this
        client a receipt: a locally authored document is not authority however
        well formed it is. The Hub revalidates the current policy and the live
        reservation as it answers, so an unknown, expired, released, stale, or
        ambiguous delegation is a refusal here rather than a stale success.
        """
        argv = [
            "brigade",
            "fleet",
            "policy",
            "delegation",
            "show",
            "--delegation-id",
            _identifier(delegation_id, "delegation id", _DECISION_ID),
            "--json",
        ]
        response = self._invoke(argv)
        try:
            receipt = normalize_delegation_receipt(response)
        except ValidationError as error:
            raise HostUnavailable(
                "fleet policy adapter returned an invalid delegation receipt"
            ) from error
        if receipt["delegation_id"] != delegation_id:
            raise PolicyRejected("fleet policy returned another delegation")
        return require_fresh_delegation(receipt, now=self._now())

    def renew_reservation(
        self,
        *,
        reservation_id: str,
        decision_id: str,
        session_id: str,
    ) -> dict[str, Any]:
        """Extend the current reservation generation; never resurrect an expired one."""
        return self._reservation("renew", reservation_id, decision_id, session_id)

    def release_reservation(
        self,
        *,
        reservation_id: str,
        decision_id: str,
        session_id: str,
    ) -> dict[str, Any]:
        """Hand the reservation back after a durable terminal local outcome."""
        return self._reservation("release", reservation_id, decision_id, session_id)

    def _reservation(
        self,
        verb: str,
        reservation_id: str,
        decision_id: str,
        session_id: str,
    ) -> dict[str, Any]:
        reservation_id = _identifier(reservation_id, "reservation id", _DECISION_ID)
        decision_id = _identifier(decision_id, "decision id", _DECISION_ID)
        session_id = _identifier(session_id, "session id")
        argv = [
            "brigade",
            "fleet",
            "policy",
            "reservation",
            verb,
            "--reservation-id",
            reservation_id,
            "--decision-id",
            decision_id,
            "--session-id",
            session_id,
            "--json",
        ]
        response = self._invoke(argv)
        try:
            return normalize_reservation(
                response,
                verb=verb,
                reservation_id=reservation_id,
                decision_id=decision_id,
                now=self._now(),
            )
        except ValidationError as error:
            raise HostUnavailable(
                "fleet reservation adapter returned an invalid reply"
            ) from error

    def _invoke(self, argv: list[str]) -> dict[str, Any]:
        try:
            if self._runner is None:
                completed = _bounded_run(argv, timeout=self._timeout)
            else:
                completed = self._runner(
                    argv,
                    input=None,
                    capture_output=True,
                    text=True,
                    timeout=self._timeout,
                    check=False,
                )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise HostUnavailable("fleet policy adapter could not be reached") from error
        if len(completed.stdout.encode("utf-8")) > _MAX_RESPONSE_BYTES:
            raise HostUnavailable("fleet policy adapter response exceeds the size limit")
        payload = self._payload(completed.stdout)
        if completed.returncode != 0:
            raise self._failure(completed.returncode, payload)
        if payload is None:
            raise HostUnavailable("fleet policy adapter returned invalid JSON")
        return payload

    @staticmethod
    def _payload(stdout: str) -> dict[str, Any] | None:
        try:
            value = json.loads(stdout)
        except (TypeError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    @staticmethod
    def _failure(returncode: int, payload: dict[str, Any] | None) -> Exception:
        code = None
        if isinstance(payload, dict):
            error = payload.get("error")
            if isinstance(error, dict) and isinstance(error.get("code"), str):
                code = error["code"]
        if code in _DENIAL_CODES or returncode == 3:
            return PolicyRejected("Fleet Hub policy denied the request")
        if code in _CONFLICT_CODES or returncode == 4:
            return PolicyRevisionConflict("Fleet Hub policy version changed")
        if returncode == 2:
            return ValidationError("fleet policy adapter rejected its invocation")
        return HostUnavailable("fleet policy adapter failed")


def load_stored_envelope(value: object) -> dict[str, Any]:
    """Re-validate a normalized envelope read back from the local journal."""
    if not isinstance(value, dict):
        raise ValidationError("stored fleet policy envelope must be an object")
    try:
        return normalize_policy_envelope(
            {
                "schema": POLICY_SCHEMA,
                "version": value["version"],
                "digest": value["digest"],
                "effective": {
                    "roles": value["roles"],
                    "execution": value["execution"],
                    "data": value["data"],
                },
                "sources": value.get("sources"),
                "ack_required": True,
            }
        )
    except KeyError as error:
        raise ValidationError("stored fleet policy envelope is incomplete") from error


def load_stored_prepared(value: object) -> dict[str, Any]:
    """Re-validate a normalized prepared session on the way out of the adapter.

    The adapter validates what the Brigade CLI returned; this re-validates it at
    the point of use, so a newer, buggy, or hostile helper cannot widen what
    reaches the provider turn by returning a shape nobody checked again.
    """
    if not isinstance(value, dict):
        raise ValidationError("stored fleet policy prepared session must be an object")
    allowed = {
        "version",
        "digest",
        "roles",
        "execution",
        "data",
        "sources",
        "ack_required",
        "repo_identity",
        "selected",
        "instructions",
        "context_hash",
    }
    if set(value) - allowed:
        raise ValidationError(
            "stored fleet policy prepared session contains unsupported fields"
        )
    context_hash = value.get("context_hash")
    try:
        return normalize_prepared_session(
            {
                "schema": POLICY_SCHEMA,
                "version": value["version"],
                "digest": value["digest"],
                "effective": {
                    "roles": value["roles"],
                    "execution": value["execution"],
                    "data": value["data"],
                },
                "sources": value.get("sources"),
                "ack_required": True,
                "repo_identity": value["repo_identity"],
                "selected": value["selected"],
                "instructions": value["instructions"],
                **({"context_hash": context_hash} if context_hash is not None else {}),
            }
        )
    except KeyError as error:
        raise ValidationError(
            "stored fleet policy prepared session is incomplete"
        ) from error


def load_stored_decision(value: object) -> dict[str, Any]:
    """Re-validate a normalized decision read back from the local journal."""
    if not isinstance(value, dict):
        raise ValidationError("stored fleet policy decision must be an object")
    try:
        return normalize_route_decision(
            {
                "schema": ROUTE_SCHEMA,
                "decision_id": value["decision_id"],
                "policy_version": value["policy_version"],
                "policy_digest": value["policy_digest"],
                "selected": {"machine": value["machine"], "seat": value["seat"]},
                "reason": value["reason"],
                "candidates": value.get("candidates"),
                "reservation_id": value["reservation_id"],
                "expires_at": value["expires_at"],
            }
        )
    except KeyError as error:
        raise ValidationError("stored fleet policy decision is incomplete") from error
