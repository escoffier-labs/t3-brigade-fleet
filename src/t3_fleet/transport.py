from __future__ import annotations

import json
import re
import shlex
import subprocess
import tempfile
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from .config import HostConfig
from .errors import (
    bounded_target_error,
    BLOCK_REASONS,
    POLICY_REASONS,
    HostUnavailable,
    PolicyRejected,
    PolicyRevisionConflict,
    TargetBusy,
    ValidationError,
)

_MAX_RESPONSE_BYTES = 1024 * 1024
_MAX_ADMISSION_CLOCK_SKEW = timedelta(seconds=30)
_MAX_ADMISSION_LIFETIME = timedelta(seconds=900)

MODEL_PLAN_SCHEMA = "brigade.fleet_model_plan.v1"


Runner = Callable[..., subprocess.CompletedProcess[str]]

_ADMISSION_STRING_FIELDS = (
    "seat",
    "provider",
    "model",
    "reasoning",
    "provider_instance_id",
    "expires_at",
)
_ADMISSION_FIELDS = frozenset(
    (
        *_ADMISSION_STRING_FIELDS,
        "roster_revision",
        "roster_digest",
        "source",
    )
)
_ROSTER_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
# Bounded identifiers for the policy proof carried on a launch admission. They
# are the same shapes `policy.py` validates; they are restated here because the
# admission adapter must never depend on a policy document to check its own
# arguments.
_PROOF_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_PROOF_DECISION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
# The canonical repository identity the launch is charged against, and the
# digest of the prepared context that was applied and acknowledged for it.
# Both shapes are restated here for the same reason as the ids above: this
# adapter checks its own arguments and never borrows a policy document to do it.
_PROOF_REPO_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/@:-]{0,255}$")
_PROOF_CREDENTIAL_SYNTAX = re.compile(r"(?i)(://|token=|password=|[^/]+:[^/@]+@)")

# A plan is planning, never launch authorization, so it is parsed by its own
# strict reader. Widening the admission parser to accept it would make a planned
# seat indistinguishable from an admitted one at every later comparison.
_PLAN_ENVELOPE_FIELDS = frozenset(
    (
        "schema",
        "launch_authorized",
        "consumer",
        "roster_revision",
        "roster_digest",
        "seat",
        "provider",
        "model",
        "reasoning",
        "binding",
        "source",
        "expires_at",
    )
)
_PLAN_BINDING_FIELDS = frozenset(("instance_id", "service_tier", "model"))
_PLAN_STRING_FIELDS = (
    "seat",
    "provider",
    "model",
    "reasoning",
    "provider_instance_id",
    "launch_model",
    "expires_at",
)
_PLAN_RECORD_FIELDS = frozenset(
    (
        *_PLAN_STRING_FIELDS,
        "state",
        "roster_revision",
        "roster_digest",
        "service_tier",
        "launch_model_explicit",
        "source",
    )
)
# The canonical seat identity a plan fixes, plus the exact native model the
# provider will be launched on. `expires_at` moves between otherwise identical
# replies, and `state` is a label rather than identity, so neither is compared.
_PLAN_IDENTITY_FIELDS = (
    "roster_revision",
    "roster_digest",
    "seat",
    "provider",
    "model",
    "reasoning",
    "provider_instance_id",
    "service_tier",
    "launch_model",
    "launch_model_explicit",
)
# What a launch admission and the plan it was taken against must agree on. The
# native launch model is compared separately and only when the admission's
# binding named one (`launch_model`); an admission without it is proved against
# the prepared selection and the provider instead.
_PLAN_ADMISSION_FIELDS = (
    "roster_revision",
    "roster_digest",
    "seat",
    "provider",
    "model",
    "reasoning",
    "provider_instance_id",
    "service_tier",
)


_DELEGATION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
# The two consumers this adapter may name. An ordinary request is `t3-fleet`;
# a delegated one runs under the parent's consumer with `t3-fleet` named on the
# receipt as the execution adapter. Nothing else is accepted, and a caller has
# no flag that reaches either value.
_CONSUMER = "t3-fleet"
_DELEGATION_CONSUMER = "brigade-run"


def _delegation_argument(request: dict[str, Any], label: str) -> list[str]:
    """Validate the consumer against the delegation, and render the flag.

    A delegation and the parent consumer travel together: one without the other
    is a malformed request rather than a weaker one, so neither can be used to
    smuggle the other past this boundary.
    """
    delegation_id = request.get("delegation_id")
    consumer = request["consumer"]
    if delegation_id is None:
        if consumer != _CONSUMER:
            raise ValidationError(f"unsupported {label} consumer")
        return []
    if consumer != _DELEGATION_CONSUMER:
        raise ValidationError(f"a delegated {label} runs under the parent consumer")
    if not isinstance(delegation_id, str) or not _DELEGATION_ID.fullmatch(delegation_id):
        raise ValidationError(f"{label} requires a valid delegation_id")
    return ["--delegation-id", delegation_id]


def normalize_admission(value: object) -> dict[str, Any]:
    """Validate the safe, immutable result returned by Brigade's CLI adapter."""
    if not isinstance(value, dict):
        raise ValidationError("model admission must be an object")
    if set(value) - _ADMISSION_FIELDS - {"service_tier", "launch_model"}:
        raise ValidationError("model admission contains unsupported fields")
    normalized: dict[str, Any] = {}
    revision = value.get("roster_revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise ValidationError("model admission requires integer roster_revision")
    normalized["roster_revision"] = revision
    digest = value.get("roster_digest")
    if not isinstance(digest, str) or not _ROSTER_DIGEST.fullmatch(digest):
        raise ValidationError("model admission requires sha256 roster_digest")
    normalized["roster_digest"] = digest
    source = value.get("source")
    if source not in {"hub", "lkg"}:
        raise ValidationError("model admission requires authoritative source")
    normalized["source"] = source
    for key in _ADMISSION_STRING_FIELDS:
        item = value.get(key)
        if not isinstance(item, str) or not item:
            raise ValidationError(f"model admission requires {key}")
        normalized[key] = item
    expires_at = normalized["expires_at"]
    try:
        parsed_expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValidationError("model admission has invalid expires_at") from error
    if parsed_expiry.tzinfo is None:
        raise ValidationError("model admission has invalid expires_at")
    tier = value.get("service_tier")
    if tier is not None:
        if not isinstance(tier, str) or not tier:
            raise ValidationError("model admission has invalid service_tier")
        normalized["service_tier"] = tier
    # Present only when the binding named an explicit native model. The
    # canonical `model` above stays the identity; this is what the provider is
    # launched on, and its absence means the canonical model verbatim.
    if "launch_model" in value:
        normalized["launch_model"] = _launch_model(value["launch_model"], "model admission")
    return normalized


def _launch_model(value: object, label: str) -> str:
    """One explicit native launch model, validated the same way for plan and admission."""
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{label} binding has an invalid model")
    return value


def normalize_model_plan(value: object) -> dict[str, Any]:
    """Validate a normalized model plan read back from the wire or local storage.

    A plan says which seat a launch *would* use. It is not admission and it
    never becomes admission by being re-read, so `state` stays `planned` and a
    record that claims anything else is refused rather than coerced.
    """
    if not isinstance(value, dict):
        raise ValidationError("model plan must be an object")
    if set(value) - _PLAN_RECORD_FIELDS:
        raise ValidationError("model plan contains unsupported fields")
    if value.get("state") != "planned":
        raise ValidationError("model plan must record the planned state")
    normalized: dict[str, Any] = {"state": "planned"}
    revision = value.get("roster_revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise ValidationError("model plan requires integer roster_revision")
    normalized["roster_revision"] = revision
    digest = value.get("roster_digest")
    if not isinstance(digest, str) or not _ROSTER_DIGEST.fullmatch(digest):
        raise ValidationError("model plan requires sha256 roster_digest")
    normalized["roster_digest"] = digest
    # Planning is Hub-only. A last-known-good roster can describe what the fleet
    # used to run; it cannot be the basis for choosing what to launch now.
    if value.get("source") != "hub":
        raise ValidationError("model plan requires an authoritative hub source")
    normalized["source"] = "hub"
    for key in _PLAN_STRING_FIELDS:
        item = value.get(key)
        if not isinstance(item, str) or not item:
            raise ValidationError(f"model plan requires {key}")
        normalized[key] = item
    explicit = value.get("launch_model_explicit")
    if not isinstance(explicit, bool):
        raise ValidationError("model plan requires a boolean launch_model_explicit")
    if not explicit and normalized["launch_model"] != normalized["model"]:
        raise ValidationError(
            "model plan launch_model must be the canonical model unless the "
            "binding named one explicitly"
        )
    normalized["launch_model_explicit"] = explicit
    expires_at = normalized["expires_at"]
    try:
        parsed_expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValidationError("model plan has invalid expires_at") from error
    if parsed_expiry.tzinfo is None:
        raise ValidationError("model plan has invalid expires_at")
    tier = value.get("service_tier")
    if tier is not None:
        if not isinstance(tier, str) or not tier:
            raise ValidationError("model plan has invalid service_tier")
        normalized["service_tier"] = tier
    return normalized


def _normalize_model_plan_envelope(value: object) -> dict[str, Any]:
    """Validate one `brigade.fleet_model_plan.v1` reply from Brigade's CLI.

    `binding.model`, when present, is the explicit native model the provider is
    launched on; `model` stays the canonical model identity either way. Nothing
    here infers an alias or rewrites a delimiter: an absent `binding.model`
    means the canonical model is used verbatim.
    """
    if not isinstance(value, dict):
        raise ValidationError("model plan envelope must be an object")
    if set(value) - _PLAN_ENVELOPE_FIELDS:
        raise ValidationError("model plan envelope contains unsupported fields")
    if value.get("schema") != MODEL_PLAN_SCHEMA:
        raise ValidationError("model plan envelope has unsupported schema")
    # Stated by the Hub and re-checked here: a plan that claimed authorization
    # would be an admission wearing a plan's schema.
    if value.get("launch_authorized") is not False:
        raise ValidationError("model plan envelope must not claim launch authorization")
    if value.get("consumer") != "t3-fleet":
        raise ValidationError("model plan envelope names another consumer")
    binding = value.get("binding")
    if not isinstance(binding, dict) or set(binding) - _PLAN_BINDING_FIELDS:
        raise ValidationError("model plan envelope has invalid binding")
    launch_model = binding.get("model")
    explicit = launch_model is not None
    if explicit:
        launch_model = _launch_model(launch_model, "model plan")
    record = {
        "state": "planned",
        "roster_revision": value.get("roster_revision"),
        "roster_digest": value.get("roster_digest"),
        "seat": value.get("seat"),
        "provider": value.get("provider"),
        "model": value.get("model"),
        "reasoning": value.get("reasoning"),
        "provider_instance_id": binding.get("instance_id"),
        "launch_model": launch_model if explicit else value.get("model"),
        "launch_model_explicit": explicit,
        "source": value.get("source"),
        "expires_at": value.get("expires_at"),
    }
    service_tier = binding.get("service_tier")
    if service_tier is not None:
        record["service_tier"] = service_tier
    return normalize_model_plan(record)


def plan_identity(value: object) -> dict[str, Any]:
    """The immutable seat and native selection a plan fixes."""
    plan = normalize_model_plan(value)
    return {key: plan[key] for key in _PLAN_IDENTITY_FIELDS if key in plan}


def require_fresh_plan(
    value: object,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate one plan's bounded central freshness window.

    The bound is the same fifteen minutes an admission gets: a plan may not
    outlive the window the fleet would have admitted under, so a stale plan
    cannot be carried to a launch that happens much later.
    """
    plan = normalize_model_plan(value)
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        raise ValidationError("model plan freshness clock must be timezone-aware")
    current = current.astimezone(UTC)
    expiry = datetime.fromisoformat(plan["expires_at"].replace("Z", "+00:00"))
    if expiry.utcoffset() != timedelta(0):
        raise ValidationError("model plan has invalid expires_at UTC offset")
    expiry = expiry.astimezone(UTC)
    if expiry <= current:
        raise ValidationError("model plan expires_at is not in the future")
    if expiry > current + _MAX_ADMISSION_LIFETIME + _MAX_ADMISSION_CLOCK_SKEW:
        raise ValidationError("model plan freshness window exceeds fifteen minutes")
    return plan


def admission_matches_plan(admission: dict[str, Any], plan: dict[str, Any]) -> bool:
    """Whether a launch admission is for the seat identity that was planned."""
    if not all(admission.get(key) == plan.get(key) for key in _PLAN_ADMISSION_FIELDS):
        return False
    # Launch model against launch model. An admission that names no native
    # model makes no claim here and is proved against the prepared selection.
    launch_model = admission.get("launch_model")
    return launch_model is None or launch_model == plan.get("launch_model")


def admission_identity(value: object) -> dict[str, Any]:
    """Return the immutable policy identity, excluding the freshness deadline."""
    normalized = normalize_admission(value)
    return {key: item for key, item in normalized.items() if key != "expires_at"}


def require_fresh_admission(
    value: object,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate one admission's independent, bounded freshness window."""
    admission = normalize_admission(value)
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        raise ValidationError("model admission freshness clock must be timezone-aware")
    current = current.astimezone(UTC)
    expiry = datetime.fromisoformat(admission["expires_at"].replace("Z", "+00:00"))
    if expiry.utcoffset() != timedelta(0):
        raise ValidationError("model admission has invalid expires_at UTC offset")
    expiry = expiry.astimezone(UTC)
    if expiry <= current:
        raise ValidationError("model admission expires_at is not in the future")
    if expiry > current + _MAX_ADMISSION_LIFETIME + _MAX_ADMISSION_CLOCK_SKEW:
        raise ValidationError("model admission freshness window exceeds fifteen minutes")
    return admission


def _normalize_admission_envelope(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError("model admission envelope must be an object")
    allowed = {
        "schema",
        "state",
        "source",
        "roster_revision",
        "roster_digest",
        "seat",
        "provider",
        "model",
        "reasoning",
        "binding",
        "expires_at",
    }
    if set(value) - allowed:
        raise ValidationError("model admission envelope contains unsupported fields")
    if value.get("schema") != "brigade.model_admission.v1":
        raise ValidationError("model admission envelope has unsupported schema")
    if value.get("state") != "authoritative":
        raise ValidationError("model admission envelope is not authoritative")
    binding = value.get("binding")
    if not isinstance(binding, dict) or set(binding) - _PLAN_BINDING_FIELDS:
        raise ValidationError("model admission envelope has invalid binding")
    normalized = {
        "roster_revision": value.get("roster_revision"),
        "roster_digest": value.get("roster_digest"),
        "seat": value.get("seat"),
        "provider": value.get("provider"),
        "model": value.get("model"),
        "reasoning": value.get("reasoning"),
        "provider_instance_id": binding.get("instance_id"),
        "source": value.get("source"),
        "expires_at": value.get("expires_at"),
    }
    service_tier = binding.get("service_tier")
    if service_tier is not None:
        normalized["service_tier"] = service_tier
    # `binding.model` is the explicit native model; `model` stays canonical.
    if "model" in binding and binding["model"] is not None:
        normalized["launch_model"] = binding["model"]
    return normalize_admission(normalized)


_ADAPTER_REASON = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


def _adapter_failure_reason(stdout: object) -> str | None:
    """The Brigade CLI's own failure slug, when it printed a safe one.

    `brigade fleet models` reports a failure as `{"error": "<slug>"}` or
    `{"reason": "<slug>"}`. Only a short lowercase slug is kept, so a message,
    a path, or a token can never ride out on it.
    """
    try:
        payload = json.loads(stdout) if isinstance(stdout, str) else None
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    for key in ("reason", "error"):
        value = payload.get(key)
        if isinstance(value, dict):
            value = value.get("code")
        if isinstance(value, str) and _ADAPTER_REASON.fullmatch(value):
            return value
    return None


class BrigadeModelAdmission:
    """Narrow subprocess adapter for Fleet Hub model admission, never Hub HTTP."""

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

    def plan(self, request: dict[str, Any]) -> dict[str, Any]:
        """Ask the Fleet Hub which seat a launch would use. Read-only planning.

        This is explicit planning and never launch authorization: the reply
        states `launch_authorized: false`, this client re-checks it, and the
        record it returns stays labelled `planned` everywhere it is stored or
        relayed. Nothing here reserves capacity or permits a provider turn.
        """
        for key in ("consumer", "seat"):
            if not isinstance(request.get(key), str) or not request[key]:
                raise ValidationError(f"model plan requires {key}")
        if set(request) - {"consumer", "seat", "delegation_id"}:
            raise ValidationError("model plan request contains unsupported fields")
        delegation_id = _delegation_argument(request, "model plan")
        argv = [
            "brigade",
            "fleet",
            "models",
            "plan",
            "--consumer",
            request["consumer"],
            "--seat",
            request["seat"],
        ]
        argv.extend(delegation_id)
        argv.append("--json")
        response = self._run(argv, denial="Fleet Hub model planning was denied")
        try:
            plan = _normalize_model_plan_envelope(response)
        except ValidationError as error:
            raise HostUnavailable("model plan adapter returned an invalid plan") from error
        try:
            return require_fresh_plan(plan, now=self._now())
        except ValidationError as error:
            raise PolicyRejected("Fleet Hub model plan freshness is invalid") from error

    def admit(self, request: dict[str, Any]) -> dict[str, Any]:
        for key in ("consumer", "phase", "request_id", "seat"):
            if not isinstance(request.get(key), str) or not request[key]:
                raise ValidationError(f"model admission requires {key}")
        delegation = _delegation_argument(request, "model admission")
        if delegation and request["phase"] != "launch":
            # Only the launch admission is delegation-scoped. Planning is the
            # source's act and the target admits once, immediately before the
            # turn; a delegation on any other phase is a malformed request.
            raise ValidationError(
                "only a launch admission may carry a fleet policy delegation"
            )
        if request["phase"] not in {"controller", "target", "launch"}:
            raise ValidationError("unsupported model admission phase")
        expect_revision = request.get("expect_revision")
        expect_digest = request.get("expect_digest")
        if request["phase"] in {"target", "launch"}:
            if (
                isinstance(expect_revision, bool)
                or not isinstance(expect_revision, int)
                or expect_revision < 0
            ):
                raise ValidationError(
                    f"{request['phase']} admission requires expect_revision"
                )
            if not isinstance(expect_digest, str) or not _ROSTER_DIGEST.fullmatch(
                expect_digest
            ):
                raise ValidationError(
                    f"{request['phase']} admission requires expect_digest"
                )
        elif expect_revision is not None or expect_digest is not None:
            raise ValidationError("controller admission cannot pin a roster revision")
        proof = self._launch_proof(request)
        argv = [
            "brigade",
            "fleet",
            "models",
            "admit",
            "--consumer",
            request["consumer"],
            "--seat",
            request["seat"],
            "--request-id",
            request["request_id"],
            "--phase",
            request["phase"],
        ]
        if request["phase"] in {"target", "launch"}:
            argv.extend(
                [
                    "--expect-revision",
                    str(expect_revision),
                    "--expect-digest",
                    expect_digest,
                ]
            )
        argv.extend(proof)
        argv.extend(delegation)
        argv.append("--json")
        response = self._run(
            argv,
            denial="Fleet Hub model admission was denied",
            request_id=request["request_id"],
        )
        try:
            admission = _normalize_admission_envelope(response)
        except ValidationError as error:
            raise HostUnavailable("model admission adapter returned invalid admission") from error
        now = self._now()
        expiry = datetime.fromisoformat(admission["expires_at"].replace("Z", "+00:00"))
        if expiry <= now:
            raise PolicyRejected(
                "Fleet Hub model admission has expired",
                reason="model-admission-expired",
            )
        try:
            return require_fresh_admission(admission, now=now)
        except ValidationError as error:
            raise PolicyRejected("Fleet Hub model admission freshness is invalid") from error

    @staticmethod
    def _launch_proof(request: dict[str, Any]) -> list[str]:
        """The policy proof a real launch admission must carry, or nothing.

        A launch is the moment fleet capacity is actually spent, so the Hub is
        told which acknowledged policy session and routing decision paid for it,
        for which canonical repository, under which exact prepared context.
        Only the launch phase may carry this, and it must carry all of it: a
        partial proof is a malformed request rather than a weaker one.

        The repository is the canonical identity the policy was verified under,
        never a checkout path, and the context hash is the digest the Hub
        prepared and this client acknowledged, forwarded verbatim. Neither is
        derived here, so a launch cannot claim a context it did not apply.
        """
        keys = (
            "policy_session_id",
            "policy_version",
            "policy_digest",
            "decision_id",
            "policy_repo_identity",
            "policy_context_hash",
        )
        supplied = [key for key in keys if request.get(key) is not None]
        if request["phase"] != "launch":
            if supplied:
                raise ValidationError(
                    "only a launch admission may carry a fleet policy proof"
                )
            return []
        if len(supplied) != len(keys):
            raise ValidationError("launch admission requires a complete fleet policy proof")
        session_id = request["policy_session_id"]
        if not isinstance(session_id, str) or not _PROOF_ID.fullmatch(session_id):
            raise ValidationError("launch admission requires a valid policy_session_id")
        version = request["policy_version"]
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise ValidationError("launch admission requires a positive policy_version")
        policy_digest = request["policy_digest"]
        if not isinstance(policy_digest, str) or not _ROSTER_DIGEST.fullmatch(policy_digest):
            raise ValidationError("launch admission requires a sha256 policy_digest")
        decision_id = request["decision_id"]
        if not isinstance(decision_id, str) or not _PROOF_DECISION_ID.fullmatch(decision_id):
            raise ValidationError("launch admission requires a valid decision_id")
        repo_identity = request["policy_repo_identity"]
        if not isinstance(repo_identity, str) or not _PROOF_REPO_IDENTITY.fullmatch(
            repo_identity
        ):
            raise ValidationError(
                "launch admission requires a canonical policy_repo_identity"
            )
        if _PROOF_CREDENTIAL_SYNTAX.search(repo_identity):
            raise ValidationError(
                "launch admission policy_repo_identity must not carry a credential"
            )
        context_hash = request["policy_context_hash"]
        if not isinstance(context_hash, str) or not _ROSTER_DIGEST.fullmatch(context_hash):
            raise ValidationError(
                "launch admission requires a sha256 policy_context_hash"
            )
        return [
            "--policy-session-id",
            session_id,
            "--policy-version",
            str(version),
            "--policy-digest",
            policy_digest,
            "--decision-id",
            decision_id,
            "--repo",
            repo_identity,
            "--policy-context-hash",
            context_hash,
        ]

    def _run(
        self,
        argv: list[str],
        *,
        denial: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """One bounded Brigade CLI call, with the shared refusal vocabulary."""
        label = "model admission" if request_id is not None else "model plan"
        try:
            if self._runner is None:
                completed = _bounded_run(argv, input=None, timeout=self._timeout)
            else:
                completed = self._runner(
                    argv,
                    input=None,
                    capture_output=True, text=True, timeout=self._timeout, check=False,
                )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise HostUnavailable(f"{label} adapter could not be reached") from error
        if completed.returncode == 3:
            raise PolicyRejected(denial)
        if completed.returncode == 4:
            raise PolicyRevisionConflict(
                "Fleet Hub roster revision changed", request_id=request_id
            )
        if completed.returncode == 2:
            raise ValidationError(f"{label} adapter rejected its invocation")
        if completed.returncode != 0:
            hub_reason = _adapter_failure_reason(completed.stdout)
            if hub_reason == "admission-expired":
                # The Hub replays an admission by its idempotency key, so a
                # replay after the fifteen-minute window returns this. It is a
                # policy answer about freshness, not an unreachable Hub.
                raise PolicyRejected(
                    "Fleet Hub model admission has expired",
                    reason="model-admission-expired",
                )
            if hub_reason is not None:
                raise HostUnavailable(f"{label} adapter failed: {hub_reason}")
            raise HostUnavailable(f"{label} adapter failed")
        if len(completed.stdout.encode("utf-8")) > _MAX_RESPONSE_BYTES:
            raise HostUnavailable(f"{label} adapter response exceeds the size limit")
        try:
            response = json.loads(completed.stdout)
        except (TypeError, json.JSONDecodeError) as error:
            raise HostUnavailable(f"{label} adapter returned invalid JSON") from error
        if not isinstance(response, dict):
            raise HostUnavailable(f"{label} adapter returned invalid JSON")
        return response


def _bounded_run(
    argv: list[str],
    *,
    input: str | None,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    with (
        tempfile.TemporaryFile(mode="w+b") as stdin,
        tempfile.TemporaryFile(mode="w+b") as stdout,
    ):
        if input is not None:
            stdin.write(input.encode("utf-8"))
            stdin.seek(0)
        completed = subprocess.run(
            argv,
            stdin=stdin if input is not None else subprocess.DEVNULL,
            stdout=stdout,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
            check=False,
        )
        stdout.seek(0)
        output = stdout.read(_MAX_RESPONSE_BYTES + 1)
    if len(output) > _MAX_RESPONSE_BYTES:
        raise HostUnavailable("target adapter response exceeds the size limit")
    try:
        decoded = output.decode("utf-8")
    except UnicodeDecodeError as error:
        raise HostUnavailable("target adapter returned invalid JSON") from error
    return subprocess.CompletedProcess(argv, completed.returncode, stdout=decoded, stderr="")


def _invoke(
    argv: list[str],
    request: dict[str, Any],
    *,
    runner: Runner | None,
    timeout: float,
) -> dict[str, Any]:
    payload = json.dumps(request, separators=(",", ":"), sort_keys=True) + "\n"
    try:
        if runner is None:
            completed = _bounded_run(argv, input=payload, timeout=timeout)
        else:
            completed = runner(
                argv,
                input=payload,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise HostUnavailable("target adapter could not be reached") from error
    if completed.returncode != 0:
        detail = None
        try:
            failure = json.loads(completed.stdout)
        except (json.JSONDecodeError, TypeError):
            failure = None
        if isinstance(failure, dict) and failure.get("schema") == "t3-fleet.agent-error.v1":
            error_payload = failure.get("error")
            if isinstance(error_payload, dict):
                target_error = bounded_target_error(error_payload.get("target_error"))
                if (target_error is not None and request.get("action") in {"submit", "claim"}
                        and target_error["code"] == error_payload.get("code")):
                    error = HostUnavailable("target preflight rejected", detail=target_error["detail"])
                    error.target_error = target_error
                    raise error
                if error_payload.get("code") == "policy_revision_conflict":
                    raise PolicyRevisionConflict("Fleet Hub roster revision changed")
                if error_payload.get("code") == "policy_rejected":
                    # Re-validated against the allowlist rather than trusted: a
                    # target that is newer, buggy, or hostile cannot widen what
                    # crosses this boundary.
                    reason = error_payload.get("reason")
                    raise PolicyRejected(
                        "Fleet Hub model admission was denied",
                        reason=reason if reason in POLICY_REASONS else None,
                    )
                candidate = error_payload.get("detail")
                if isinstance(candidate, str) and 0 < len(candidate) <= 512:
                    detail = candidate
                if error_payload.get("not_started") is True:
                    # The target reported that it failed before it sent
                    # anything to T3, so nothing was delivered and the request
                    # keeps its state. Only the exact boolean counts; an older
                    # target never sends it and stays ambiguous.
                    if error_payload.get("code") == "invalid_request":
                        raise ValidationError(
                            "target refused the request before changing anything"
                            + (f": {detail}" if detail else ""),
                            detail=detail,
                        )
                    raise HostUnavailable(
                        "target failed before changing anything",
                        detail=detail,
                        not_started=True,
                    )
                if error_payload.get("code") == "target_busy":
                    # The target answered that it started nothing, so this is
                    # a pre-launch block, not a lost dispatch. The reason is
                    # re-validated against the allowlist, like policy reasons.
                    reason = error_payload.get("reason")
                    raise TargetBusy(
                        "target declined the request before starting it",
                        reason=reason if reason in BLOCK_REASONS else None,
                        detail=detail,
                    )
        raise HostUnavailable("target adapter failed", detail=detail)
    if len(completed.stdout.encode("utf-8")) > _MAX_RESPONSE_BYTES:
        raise HostUnavailable("target adapter response exceeds the size limit")
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise HostUnavailable("target adapter returned invalid JSON") from error
    if not isinstance(result, dict):
        raise HostUnavailable("target adapter returned invalid JSON")
    return result


class LocalTransport:
    def __init__(
        self,
        *,
        runner: Runner | None = None,
        timeout: float = 45,
    ) -> None:
        self._runner = runner
        self._timeout = timeout

    def call(self, host: HostConfig, request: dict[str, Any]) -> dict[str, Any]:
        return _invoke(
            list(host.agent_command),
            request,
            runner=self._runner,
            timeout=self._timeout,
        )


class SshTransport:
    def __init__(
        self,
        *,
        ssh_command: tuple[str, ...] = ("ssh",),
        runner: Runner | None = None,
        timeout: float = 45,
    ) -> None:
        self._ssh_command = ssh_command
        self._runner = runner
        self._timeout = timeout

    def call(self, host: HostConfig, request: dict[str, Any]) -> dict[str, Any]:
        if host.ssh_alias is None:
            raise HostUnavailable("SSH transport requires a configured alias")
        if host.platform == "windows":
            remote_command = subprocess.list2cmdline(list(host.agent_command))
        else:
            remote_command = shlex.join(host.agent_command)
        argv = [
            *self._ssh_command,
            "-o",
            "BatchMode=yes",
            "--",
            host.ssh_alias,
            remote_command,
        ]
        return _invoke(
            argv,
            request,
            runner=self._runner,
            timeout=self._timeout,
        )


class FleetTransport:
    def __init__(
        self,
        *,
        local: LocalTransport | None = None,
        ssh: SshTransport | None = None,
        ssh_command: tuple[str, ...] = ("ssh",),
    ) -> None:
        self._local = local or LocalTransport()
        self._ssh = ssh or SshTransport(ssh_command=ssh_command)

    def call(self, host: HostConfig, request: dict[str, Any]) -> dict[str, Any]:
        if host.transport == "local":
            return self._local.call(host, request)
        if host.transport == "ssh":
            return self._ssh.call(host, request)
        raise HostUnavailable(f"unsupported transport for host {host.host_id}")
