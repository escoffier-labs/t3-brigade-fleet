from __future__ import annotations

from dataclasses import dataclass
from typing import Any


JOB_STATES = frozenset(
    {
        "queued",
        "preflight",
        "dispatching",
        "running",
        "needs_attention",
        "succeeded",
        "failed",
        "interrupted",
        "blocked_offline",
        "unknown",
    }
)

TERMINAL_STATES = frozenset({"succeeded", "failed", "interrupted"})


@dataclass(frozen=True)
class FleetResult:
    request_id: str
    host: str
    state: str
    thread_id: str | None = None
    needs_attention: bool = False
    updated_at: str | None = None
    output: str | None = None

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema": "t3-fleet.result.v1",
            "request_id": self.request_id,
            "host": self.host,
            "state": self.state,
            "thread_id": self.thread_id,
            "needs_attention": self.needs_attention,
            "updated_at": self.updated_at,
        }
        if self.output is not None:
            result["output"] = self.output
        return result


# The pending request a needs_attention result names, so a caller can answer
# or approve it. The target builds it from T3 and the controller rebuilds it
# from what the target sent, through this one function, so every field is
# re-validated and re-bounded at each boundary and nothing unknown survives.
PENDING_ID_MAX = 1024
PENDING_TEXT_MAX = 2000
PENDING_LABEL_MAX = 200
PENDING_DESCRIPTION_MAX = 500
PENDING_MAX_QUESTIONS = 5
PENDING_MAX_OPTIONS = 10
USER_INPUT_REQUEST_KINDS = frozenset({"user_input"})
# T3 protocol 2 approval kinds, plus `approval` for a protocol 1 approval
# whose activity does not name its kind.
APPROVAL_REQUEST_KINDS = frozenset(
    {"command", "file-read", "file-change", "permission", "mcp-elicitation", "approval"}
)


def _short(value: object, limit: int) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value[:limit]


def _identifier(value: object, limit: int) -> str | None:
    # Identifiers are echoed back to T3, so they are never truncated: an
    # oversized one is dropped instead of being turned into a wrong one.
    if not isinstance(value, str) or not value or len(value) > limit:
        return None
    return value


def _first(value: dict[str, Any], *keys: str) -> object:
    for key in keys:
        if key in value:
            return value[key]
    return None


def _bounded_question(value: object) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    question_id = _identifier(value.get("id"), 128)
    if question_id is None:
        return None
    result: dict[str, Any] = {"id": question_id}
    header = _short(value.get("header"), PENDING_LABEL_MAX)
    if header is not None:
        result["header"] = header
    text = _short(value.get("question"), PENDING_TEXT_MAX)
    if text is not None:
        result["question"] = text
    options: list[dict[str, str]] = []
    raw_options = value.get("options")
    for option in raw_options if isinstance(raw_options, list) else []:
        if len(options) == PENDING_MAX_OPTIONS:
            break
        if not isinstance(option, dict):
            continue
        label = _short(option.get("label"), PENDING_LABEL_MAX)
        if label is None:
            continue
        bounded = {"label": label}
        description = _short(option.get("description"), PENDING_DESCRIPTION_MAX)
        if description is not None:
            bounded["description"] = description
        options.append(bounded)
    result["options"] = options
    multi_select = _first(value, "multi_select", "multiSelect")
    if isinstance(multi_select, bool):
        result["multi_select"] = multi_select
    return result


def _bounded_approval(value: object) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    result: dict[str, Any] = {}
    request_kind = _short(_first(value, "request_kind", "requestKind"), 64)
    if request_kind is not None:
        result["request_kind"] = request_kind
    # T3 calls this the approval's prompt: the provider's description of
    # what it wants to do. It is never the task prompt, which stays private.
    summary = _short(_first(value, "summary", "prompt"), PENDING_TEXT_MAX)
    if summary is not None:
        result["summary"] = summary
    app_name = _short(_first(value, "app_name", "appName"), PENDING_LABEL_MAX)
    if app_name is not None:
        result["app_name"] = app_name
    options: list[dict[str, str]] = []
    raw_options = value.get("options")
    for option in raw_options if isinstance(raw_options, list) else []:
        if len(options) == PENDING_MAX_OPTIONS:
            break
        if not isinstance(option, dict):
            continue
        label = _short(option.get("label"), PENDING_LABEL_MAX)
        decision = _short(option.get("decision"), 64)
        if label is None or decision is None:
            continue
        bounded = {"decision": decision, "label": label}
        warning = _short(option.get("warning"), PENDING_DESCRIPTION_MAX)
        if warning is not None:
            bounded["warning"] = warning
        options.append(bounded)
    if options:
        result["options"] = options
    return result or None


def bounded_pending_request(value: object) -> dict[str, Any] | None:
    """A validated, bounded pending request, or None when it is unusable.

    Accepts the T3 shape (`createdAt`, `requestKind`, `appName`,
    `multiSelect`) and this module's own output, so applying it twice is a
    no-op. `respond_with` names the fleet command that resolves the request;
    it is absent for kinds the fleet cannot respond to.
    """
    if not isinstance(value, dict):
        return None
    request_id = _identifier(value.get("id"), PENDING_ID_MAX)
    kind = _identifier(value.get("kind"), 64)
    if request_id is None or kind is None:
        return None
    result: dict[str, Any] = {"id": request_id, "kind": kind}
    created_at = _identifier(_first(value, "created_at", "createdAt"), 64)
    if created_at is not None:
        result["created_at"] = created_at
    if kind in USER_INPUT_REQUEST_KINDS:
        result["respond_with"] = "answer"
        questions = value.get("questions")
        if isinstance(questions, list):
            bounded = [
                question
                for question in (_bounded_question(item) for item in questions)
                if question is not None
            ]
            result["questions"] = bounded[:PENDING_MAX_QUESTIONS]
            if len(bounded) > PENDING_MAX_QUESTIONS or value.get("questions_truncated") is True:
                result["questions_truncated"] = True
    elif kind in APPROVAL_REQUEST_KINDS:
        result["respond_with"] = "approve"
        approval = _bounded_approval(value.get("approval"))
        if approval is not None:
            result["approval"] = approval
    return result
