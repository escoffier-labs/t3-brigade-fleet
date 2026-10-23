from __future__ import annotations

import re
from datetime import datetime


class FleetError(Exception):
    """Base error with a stable machine-facing code and process exit code."""

    code = "internal_error"
    exit_code = 1

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail
        self.target_error: dict[str, str] | None = None


class ConfigError(FleetError):
    code = "invalid_config"
    exit_code = 2


class ValidationError(FleetError):
    code = "invalid_request"
    exit_code = 2


# Why a dispatch could not start, for the one narrow case where that
# distinction has to survive: a queued outbox job reporting truthfully why it is
# still waiting. Like the policy reasons this is an allowlist rather than free
# text, so a helper message, a host name, or a path can never ride out on it.
BLOCK_REASONS = frozenset(
    {
        # Every eligible seat is already running something.
        "capacity-exhausted",
        # The target answered, and it is not in a state to take work.
        "host-ineligible",
        # The target did not answer at all.
        "host-offline",
        # This client stopped calling a target that keeps failing.
        "host-circuit-open",
        # Another request holds this repository on this host.
        "repository-busy",
        # The Fleet Hub could not be reached to plan or route.
        "hub-unreachable",
    }
)


class HostUnavailable(FleetError):
    code = "host_unavailable"
    exit_code = 3

    def __init__(
        self,
        message: str,
        *,
        reason: str | None = None,
        detail: str | None = None,
        not_started: bool = False,
    ) -> None:
        super().__init__(message, detail=detail)
        # An unrecognized reason is dropped rather than raised, for the same
        # rule the policy reasons follow: a wrong slug must never change what
        # kind of failure this is, and must never be forwarded on the
        # assumption that some later layer will filter it.
        self.reason = reason if reason in BLOCK_REASONS else None
        # True only when the target itself reported that it failed before it
        # sent anything to T3. A caller may then keep the request's state
        # instead of treating the call as a lost mutation. Anything else, a
        # lost connection included, stays ambiguous.
        self.not_started = not_started is True


class TargetBusy(HostUnavailable):
    """A target declined work before it changed anything for the request.

    The target answered, so this is not a lost transport. It raises this only
    before it creates a T3 thread, a worktree, or a branch, and after it has
    released the claim's slot. The same request can therefore be queued or
    retried without risking a second execution. Older controllers do not know
    the code and fall back to their generic target-failure handling.
    """

    code = "target_busy"
    exit_code = 3


class IdempotencyConflict(FleetError):
    code = "idempotency_conflict"
    exit_code = 4


class PolicyRevisionConflict(FleetError):
    """The Fleet Hub roster changed while a request was being admitted."""

    code = "policy_revision_conflict"
    exit_code = 4

    def __init__(self, message: str, *, request_id: str | None = None) -> None:
        super().__init__(message)
        self.request_id = request_id


# Every fleet policy denial shares one wire code, so the code alone cannot tell
# an operator which denial happened. These slugs are the whole vocabulary that
# is allowed to cross a target, a transport, or the CLI boundary: an allowlist
# rather than free text, so a message, a path, or a helper's stderr can never
# ride out on this field.
POLICY_REASONS = frozenset(
    {
        "continuation-admission-required",
        "policy-repository-mismatch",
        "policy-machine-mismatch",
        "policy-seat-mismatch",
        "policy-decision-superseded",
        "policy-digest-stale",
        "model-admission-denied",
        # The Fleet Hub replayed an admission whose freshness window passed.
        "model-admission-expired",
        # The Fleet Hub prepared or admitted a selection other than the one
        # this request planned and applied.
        "policy-selection-mismatch",
        "policy-plan-mismatch",
        # Adoption of a parent delegation. `not-enrolled` is the config
        # refusal, `invalid` is the receipt itself, `conflict` is caller
        # context that contradicts the receipt, and `repository-unmatched` is
        # a canonical identity this machine cannot resolve to exactly one
        # configured alias.
        "delegation-not-enrolled",
        "delegation-invalid",
        "delegation-conflict",
        "delegation-repository-unmatched",
    }
)


class PolicyRejected(FleetError):
    """The authoritative fleet roster denied a model admission."""

    code = "policy_rejected"
    exit_code = 3

    def __init__(
        self,
        message: str,
        *,
        reason: str | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(message, detail=detail)
        # An unrecognized reason is dropped rather than raised: a wrong slug
        # must never turn a denial into a different failure, and must never be
        # forwarded on the assumption that some other layer will filter it.
        self.reason = reason if reason in POLICY_REASONS else None


class UnknownState(FleetError):
    code = "unknown_state"
    exit_code = 5

    def __init__(
        self,
        message: str,
        *,
        request_id: str | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(message, detail=detail)
        self.request_id = request_id


class T3AuthError(FleetError):
    code = "t3_auth_failed"
    exit_code = 3


class T3HttpError(FleetError):
    code = "t3_http_failed"
    exit_code = 3


MISSING_RECEIPT_GRACE_SECONDS = 30
UNCONFIRMED_TERMINAL_SECONDS = 300


TARGET_ERROR_CODES = frozenset({
    "invalid_config", "invalid_request", "host_unavailable", "t3_auth_failed",
    "t3_http_failed", "internal_error", "policy_rejected", "policy_revision_conflict",
})


def bounded_target_error(value: object) -> dict[str, str] | None:
    """Revalidate diagnostics at every public boundary, without forwarding stderr."""
    if (
        not isinstance(value, dict)
        or not isinstance(value.get("code"), str)
        or value["code"] not in TARGET_ERROR_CODES
    ):
        return None
    detail = value.get("detail")
    if not isinstance(detail, str) or not detail or len(detail) > 512:
        detail = "target preflight rejected; details redacted"
    if re.search(r"token|secret|password|credential|private[_-]?key|[/\\]", detail, re.I):
        detail = "target preflight rejected; details redacted"
    return {"code": value["code"], "detail": " ".join(detail.split())}


def bounded_resolution(value: object) -> dict[str, object] | None:
    if (
        not isinstance(value, dict)
        or not isinstance(value.get("reason"), str)
        or value["reason"] not in {"terminal-unconfirmed", "submit-unresolved", "host-circuit-open"}
    ):
        return None
    result = {"reason": value["reason"]}
    if type(value.get("blocking")) is bool:
        result["blocking"] = value["blocking"]
    deadline = value.get("deadline")
    if isinstance(deadline, str) and len(deadline) <= 40:
        try:
            parsed = datetime.fromisoformat(deadline.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                result["deadline"] = deadline
        except ValueError:
            pass
    return result
