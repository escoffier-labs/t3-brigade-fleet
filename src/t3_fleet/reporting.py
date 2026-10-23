"""Bounded, target-local Brigade CLI reporting. No Hub credentials cross this API."""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from typing import Any

from .errors import FleetError, ValidationError
from .transport import _bounded_run

SCHEMA = "brigade.t3_execution_context.v1"
HEARTBEAT_SECONDS = 300
REPORT_TIMEOUT = 8
_LOG = logging.getLogger(__name__)
_REPORT_SUPPORTED: bool | None = None
_CONTEXT_KEYS = frozenset(
    {
        "schema",
        "request_id",
        "thread_id",
        "turn_ref",
        "controller_id",
        "origin_node",
        "target_host",
        "initiator",
        "work_id",
        "issue_url",
        "policy",
    }
)
_SECRET = re.compile(
    r"(?i)(?:token|secret|password|passwd|api[_-]?key|credential)\s*[:=]|\b(?:bearer|basic)\s+\S|\b(?:authorization|cookie)\s*:"
)
_CREDENTIAL_RES = (
    re.compile(r"gh[pousr]_[A-Za-z0-9]{16,}"),
    re.compile(r"github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"xox[abopsr]-[A-Za-z0-9-]{10,}"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{20,}"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bASIA[0-9A-Z]{16}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{30,}"),
    re.compile(r"\bglpat-[0-9A-Za-z_-]{16,}"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),
    re.compile(
        r"(?:api[_-]?key|secret|password|passwd|access[_-]?token|auth[_-]?token|token)"
        r"[\"']?\s*[:=]\s*[\"']?[A-Za-z0-9_\-./+=]{16,}",
        re.IGNORECASE,
    ),
    re.compile(r"//[^\s/@]+:[^\s/@]+@"),
)
_ISSUE = re.compile(
    r"https://github\.com/[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*/(?:issues|pull)/[1-9][0-9]*"
)


def validate_context(value: Any) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or set(value) - _CONTEXT_KEYS
        or value.get("schema") != SCHEMA
    ):
        raise ValidationError("invalid T3 execution context schema or fields")
    for key, item in value.items():
        if key in {"initiator", "policy"}:
            allowed = (
                {"kind", "label"}
                if key == "initiator"
                else {"session_id", "decision_id", "reservation_id", "delegation_id"}
            )
            if not isinstance(item, dict) or set(item) - allowed:
                raise ValidationError("invalid T3 execution context object")
            if key == "initiator" and "kind" in item and item["kind"] not in {
                "agent",
                "harness",
                "human",
                "unknown",
            }:
                raise ValidationError("invalid reported initiator kind")
            strings = item.values()
        else:
            strings = (item,)
        for text in strings:
            if (
                not isinstance(text, str)
                or len(text) > (512 if key == "issue_url" else 128)
                or not all(c.isprintable() for c in text)
                or _SECRET.search(text)
                or any(pattern.search(text) for pattern in _CREDENTIAL_RES)
                or (
                    key != "issue_url"
                    and (any(c in text for c in ("/", "\\", "%")) or text.startswith("op:"))
                )
            ):
                raise ValidationError(
                    "invalid or secret-like T3 execution context value"
                )
        if key == "issue_url" and not _ISSUE.fullmatch(item):
            raise ValidationError(
                "issue_url must be a canonical GitHub issue or pull URL"
            )
    if (
        len(
            json.dumps(
                value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode()
        )
        > 4096
    ):
        raise ValidationError("T3 execution context exceeds 4 KiB")
    return json.loads(json.dumps(value))


def launch_context(
    request: dict[str, Any], *, controller_id: str, origin_node: str
) -> dict[str, Any]:
    return validate_context(
        {
            "schema": SCHEMA,
            "controller_id": controller_id,
            "origin_node": origin_node,
            "initiator": {
                "kind": request.get("initiator_kind", "unknown"),
                "label": request.get("initiator_label", "unknown"),
            },
            **{key: request[key] for key in ("issue_url", "work_id") if key in request},
        }
    )


def report_event(event: dict[str, Any]) -> str:
    """Return delivered/queued only after Brigade confirms durable acceptance.

    Helper failures are deliberately generic: stdout/stderr may contain secrets
    from an older helper or its environment and must never enter agent results.
    """
    global _REPORT_SUPPORTED
    try:
        context = validate_context(event["context"])
        if _REPORT_SUPPORTED is False:
            return "unavailable"
        argv = ["brigade", "fleet", "events", "report"]
        for field in (
            "run_id",
            "state",
            "ts",
            "sequence",
            "repo",
            "repo_identity",
            "seat",
            "harness",
            "exit_status",
        ):
            if field in event:
                argv.extend(["--" + field.replace("_", "-"), str(event[field])])
        argv.extend(["--context-stdin", "--json"])
        command = subprocess.list2cmdline(argv) if os.name == "nt" else argv
        reply = _bounded_run(
            command,
            input=json.dumps(context, sort_keys=True, separators=(",", ":")),
            timeout=REPORT_TIMEOUT,
        )
        if reply.returncode == 2:
            # argparse also uses exit 2 when an older helper lacks this command.
            # Probe only on rejection, and remember a conclusive result locally.
            if _REPORT_SUPPORTED is None:
                probe = ["brigade", "fleet", "events", "report", "--help"]
                help_reply = _bounded_run(
                    subprocess.list2cmdline(probe) if os.name == "nt" else probe,
                    input=None,
                    timeout=REPORT_TIMEOUT,
                )
                if help_reply.returncode in {0, 2}:
                    _REPORT_SUPPORTED = help_reply.returncode == 0
            if _REPORT_SUPPORTED is True:
                _LOG.warning("T3 lifecycle event rejected")
                return "rejected"
        if reply.returncode == 0:
            result = json.loads(reply.stdout)
            if (
                isinstance(result, dict)
                and result.get("result") in {"delivered", "queued"}
                and result.get("run_id") == event["run_id"]
                and type(result.get("sequence")) is int
                and result["sequence"] == event["sequence"]
                and isinstance(result.get("digest"), str)
                and re.fullmatch(r"[0-9a-f]{64}", result["digest"])
            ):
                return result["result"]
    except ValidationError:
        # A frozen event from an older validator is invalid permanently too.
        _LOG.warning("T3 lifecycle event rejected")
        return "rejected"
    except (
        OSError,
        subprocess.TimeoutExpired,
        FleetError,
        ValueError,
        TypeError,
        KeyError,
    ):
        pass
    _LOG.warning("T3 lifecycle reporting unavailable")
    return "unavailable"
