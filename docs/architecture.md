# Architecture

This document describes the layers of `t3-fleet` and the boundary each one owns. It is a reference, not a tutorial. For the walkthrough see `docs/quickstart.md`.

## Layers

`t3-fleet` is split into four layers that run in two processes.

```
workstation                       target host
---------                         -----------
controller CLI                    agent
  |- config                       |- T3 Code client
  |- store                        |- target store
  |- transport --------------------->|
  |- controller
```

- **Config** (`t3_fleet.config.py`) loads and validates the allowlisted fleet config. It rejects unknown keys, embedded credential fields, IP literals in `ssh_alias`, and config files that are not mode `0600` on Unix.
- **Store** (`t3_fleet.store.py`) is the controller-side state. It tracks in-flight requests, capacity counters, and circuit breakers per host.
- **Transport** (`t3_fleet.transport.py`) carries a request from the controller to a target. It supports `local` and `ssh`. It does not carry prompt text in Brigade tool calls.
- **Controller** (`t3_fleet.controller.py`) is the policy layer. It decides which host a request may run on, enforces capacity, records outcomes, and exposes the public commands.

The agent (`t3_fleet.agent.py`) runs on each target. It receives a request from the transport, talks to the local T3 Code instance, and writes a target-side receipt. The target store (`t3_fleet.target_store.py`) holds those receipts so the controller can reconcile later.

## Process model

The controller runs on your workstation. The agent runs on the target host. For a `local` host the agent is a subprocess. For an `ssh` host the controller invokes the agent over SSH using a reviewed alias.

The controller never opens a direct connection to T3 Code on a remote host. It always goes through the agent. This keeps the trust boundary at the agent process, not at the model API.

## Request lifecycle

1. `submit` validates the request, reserves a slot, and asks the agent to start a T3 Code session.
2. `status` reads the current state of the request from the store.
3. `wait` blocks until the request reaches a terminal state or the timeout elapses.
4. `followup`, `interrupt`, `approve`, and `answer` drive a live session.
5. `recover` reconciles a request whose outcome is unknown. See `docs/recovery.md`.

A request reaches one of these terminal states: `completed`, `interrupted`, `error`, or `unknown`. `unknown` is not a success state. It means the controller cannot confirm the outcome and `recover` is required.

## Coordination modes

The config supports two coordination modes.

- `single-controller` assumes one controller owns the fleet. This is the default.
- `target-authoritative` makes the target store the source of truth. The controller reconciles against target-side receipts. This mode requires a `controller_id` and a `global_active_limit` equal to the sum of enabled host limits.

Use `target-authoritative` when more than one controller may dispatch to the same fleet. Use `single-controller` otherwise.

## Schemas

The wire contract is JSON. Two schemas are published.

- `schemas/request.schema.json` defines `t3-fleet.request.v1`, the request envelope.
- `schemas/result.schema.json` defines `t3-fleet.result.v1`, the result envelope, and `t3-fleet.error.v1`, the error envelope.

The CLI accepts either flags or a single JSON object on standard input. The JSON form is required for Brigade tool calls. Prompt text is accepted only through `prompt` in JSON or through `prompt_file`. The CLI rejects inline prompts in Brigade tool calls.

## Error codes

Errors return a stable machine-facing code and a process exit code. The public message is generic. The detailed message stays on stderr for the operator.

- `invalid_config` (exit 2): the fleet config is invalid.
- `invalid_request` (exit 2): the request is invalid.
- `host_unavailable` (exit 3): an allowlisted host is unreachable.
- `idempotency_conflict` (exit 4): the request conflicts with an existing request.
- `unknown_state` (exit 5): the request outcome is unknown. Run `recover`.
- `t3_auth_failed` (exit 3): target authentication failed.
- `t3_http_failed` (exit 3): a target T3 request failed.
- `internal_error` (exit 1): an internal fleet error.

## What t3-fleet does not own

- T3 Code owns the session, the model, and the working tree. `t3-fleet` does not interpret session contents.
- Brigade owns dispatch and verification receipts when `t3-fleet` is invoked through Brigade. `t3-fleet` does not grade its own work.
- The human reviewer owns approval decisions. `t3-fleet` enforces that a decision is required. It does not make the decision.
