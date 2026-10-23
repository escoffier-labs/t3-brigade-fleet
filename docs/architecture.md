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

- **Config** (`src/t3_fleet/config.py`) loads and validates the allowlisted fleet config. It rejects unknown keys, embedded credential fields, IP literals in `ssh_alias`, and config files that are not mode `0600` on Unix.
- **Store** (`src/t3_fleet/store.py`) is the controller-side state. It tracks in-flight requests, capacity counters, and circuit breakers per host.
- **Transport** (`src/t3_fleet/transport.py`) carries a request from the controller to a target. It supports `local` and `ssh`. Prompt text travels to the target over protected
  standard input and is excluded from Brigade tool-call journals.
- **Controller** (`src/t3_fleet/controller.py`) is the policy layer. It enforces allowlists and capacity, obtains Brigade authority, records
  outcomes, and exposes the dispatch commands. Enrolled host selection belongs
  to the Hub, never local ranking.

The agent (`src/t3_fleet/agent.py`) runs on each target. It receives a request from the transport, talks to the local T3 Code instance, and writes a target-side receipt. The target store (`src/t3_fleet/target_store.py`) holds those receipts so the controller can reconcile later.

## Process model

The controller runs on your workstation. The agent runs on the target host. For a `local` host the agent is a subprocess. For an `ssh` host the controller invokes the agent over SSH using a reviewed alias.

The controller never opens a direct connection to T3 Code on a remote host. It always goes through the agent. This keeps the trust boundary at the agent process, not at the model API.

## Request lifecycle

1. `submit` validates the request and obtains Brigade authority before launch.
   With enrollment off, it obtains model admission for an explicit logical seat.
   With enrollment required, the Hub routes and reserves, the controller plans,
   and the target prepares, applies, acknowledges, and admits the launch.
2. `status` refreshes current target evidence and reconciles the controller store.
3. `wait` blocks until the request reaches a terminal state or the timeout elapses.
4. `followup`, `interrupt`, `approve`, and `answer` drive a live session.
5. `recover` reconciles a request whose outcome is unknown. See `docs/recovery.md`.

A request reaches one of these terminal states: `succeeded`, `failed`, or `interrupted`. A request that the controller cannot confirm reaches `unknown`. `unknown` is non-terminal: the slot stays reserved and `recover` is required to reconcile it.

## Coordination modes

The config supports two coordination modes.

- `single-controller` assumes one controller owns the fleet. This is the default.
- `target-authoritative` makes the target store the source of truth. The controller reconciles against target-side receipts. This mode requires a `controller_id` and a `global_active_limit` equal to the sum of enabled host limits.

Use `target-authoritative` when more than one controller may dispatch to the same fleet. Use `single-controller` otherwise.

## Schemas

The wire contract is JSON. Two schemas are published.

- `schemas/request.schema.json` defines `t3-fleet.request.v1`, the request envelope.
- `schemas/result.schema.json` defines `t3-fleet.result.v1`, the result envelope, and `t3-fleet.error.v1`, the error envelope.

The CLI accepts either flags or a single JSON object on standard input for most commands. `answer` is JSON-stdin-only: it has no flags and reads a single JSON object with `request_id`, `input_id`, and `answers`. The catalog
runner uses `brigade-invoke` with a matching approved tool-call journal record.
It does not accept arbitrary request JSON from stdin or command arguments.
`invoke` is a separate JSON integration path. Both reject operator-only
`settle-sweep` and `install-settle-timer` actions. Prompt text is accepted only through `prompt` in JSON or through `prompt_file`. The CLI rejects inline prompts in Brigade tool calls.

## Error codes

Errors return a stable machine-facing code and a process exit code. Public messages and stderr are redacted. Allowlisted reasons and bounded typed
preflight details can explain a refusal without forwarding helper output.

- `invalid_config` (exit 2): the fleet config is invalid.
- `invalid_request` (exit 2): the request is invalid.
- `host_unavailable` (exit 3): an allowlisted host is unreachable.
- `target_busy` (exit 3): the target refused before creating work.
- `policy_rejected` (exit 3): admission or policy denied the action.
- `policy_revision_conflict` (exit 4): roster authority changed.
- `idempotency_conflict` (exit 4): the request conflicts with an existing request.
- `unknown_state` (exit 5): the request outcome is unknown. Run `recover`.
- `t3_auth_failed` (exit 3): target authentication failed.
- `t3_http_failed` (exit 3): a target T3 request failed.
- `internal_error` (exit 1): an internal fleet error.

## Admission, concurrency, and background work

`policy.py` parses closed, bounded Brigade envelopes. `reporting.py` emits
bounded lifecycle evidence and retries transport failures without exposing
prompts. Model plans and launch admissions remain separate. A plan never
proves launch authorization. Policy enrollment off still requires model
admission. The Python package invokes the external Brigade CLI and does not
ship enrollment credentials or directly call the Hub.

A host's `active_limit` bounds all active work. The effective per-repository
cap is `max_concurrent_per_repository` when positive, bounded by the host limit.
With the default `0`, one repository can use the host's capacity. Each request
gets an isolated target worktree. Request-scoped lease keys permit concurrency
without losing request identity. The lease TTL defaults to 21600 seconds.
Unknown outcomes retain capacity and must satisfy target reconciliation rules.

`outbox.py` owns an optional bounded controller-local queue. Explicitly opted-in
pre-launch blocks can defer the original intent. Tick revalidates authority
before each attempt and holds unknown acceptance. Retention limits payload
duration in live records, never execution duration. Tick also renews reservations
this node owns as a target, so long runs need no originating CLI polling loop.

The optional operator-only settlement sweep handles fleet threads terminal past
a grace period. It does not contain live work. `timer.py` renders or installs
an operator-requested timer, and no model-facing action can invoke it.
See [deployment](deployment.md) and [recovery](recovery.md) for operational limits.

The T3 adapter supports protocol 1 and protocol 2. Protocol 2 uses native model
option arrays, run identity, and bounded RPC replies. Version acceptance also
requires the matching descriptor and shell contracts. A desktop version alone
is insufficient evidence of compatibility.

## What t3-fleet does not own

- T3 Code owns the session, the model, and the working tree. `t3-fleet` does not interpret session contents.
- Brigade owns dispatch and verification receipts when `t3-fleet` is invoked through Brigade. `t3-fleet` does not grade its own work.
- The human reviewer owns approval decisions. `t3-fleet` enforces that a decision is required. It does not make the decision.
