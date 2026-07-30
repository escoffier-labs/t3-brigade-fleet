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
