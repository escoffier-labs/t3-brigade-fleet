# T3 Fleet Dispatch

Project this as one allowlisted Brigade `script` tool. It invokes the local
`t3-fleet` client only. The client uses private configuration for its permitted
transport and `target-authoritative` peer coordination.

```toml
[[tool]]
id = "t3-fleet"
name = "T3 Fleet"
family = "script"
enabled = true
description = "Dispatch and manage allowlisted T3 Code tasks through the local client."
source_path = "tools/t3-fleet-dispatch.md"
command = "t3-fleet brigade-invoke"
input_schema_path = "schemas/tool-call.input.schema.json"
output_schema_path = "schemas/tool-call.output.schema.json"
timeout = 86500
permissions = ["read-files", "write-files", "run-process", "network"]
effects = ["filesystem", "process", "network"]
approval_mode = "always"
supported_harnesses = ["codex", "claude", "cursor", "openclaw"]
```

The catalog runner reads `BRIGADE_TOOL_CALL_ID` and `BRIGADE_TOOL_RUN_ID`, then
loads one matching authorized record from `.brigade/tools/calls.jsonl`. It
accepts no command arguments and does not read request JSON from standard
input. Submit and follow-up calls must use a protected `prompt_file`. Inline
prompt text is excluded from the planner schema and rejected if it appears in
the call journal. The client prints one result object to stdout and diagnostics
to stderr.

The public action surface is exactly `doctor`, `capacity`, `submit`, `status`,
`wait`, `followup`, `interrupt`, `approve`, `answer`, `recover`, and `tick`.
`tick` carries no request fields at all: it drains work a caller already
submitted and already authorized, so there is nothing on it to name, narrow, or
re-approve. The input
schema fixes those names and the result schema fixes normalized task states and
the redacted error envelope. Both schema roots set `additionalProperties:
false`. Do not copy either schema into a projection.

In peer mode, `submit` omits `request_id`. The local client generates the
qualified ID returned to the caller. Treat it as the cross-controller handle.
Each submit names a logical Fleet Hub `seat`. The controller and target resolve
and confirm the exact approved provider, model, reasoning, binding, revision,
and digest through Brigade before mutation. Never pass a provider/model
override. T3 project defaults are never dispatch authority.
`recover --target <allowlisted-target-id>` is only a lookup hint. No public
projection constructs connection commands, service endpoints, internal actions,
worktree paths, or private state locations.

## Fleet policy routing

When the controller is enrolled (`[policy] enrollment = "required"`), routing
authority is the Fleet Hub, reached only through the Brigade CLI. The client
never opens a Hub connection and never ranks machines locally.

- `--host auto` with no `--seat` asks the Hub to select both the machine and
  the seat. That is the default path and it needs no justification.
- Naming an explicit host or an explicit seat while enrolled is an override and
  requires `--override-reason` with a short operator justification. Both are
  forwarded to the same central route, which stays the authority: a decision
  that lands on a machine or a seat the caller did not name is refused. A seat
  override never ranks models locally and never relaxes a data rule.
- `--override-reason` with neither an explicit host nor an explicit seat, or
  with enrollment `off`, is rejected. Enrollment `off` keeps the pre-policy
  contract unchanged.
- `--workload <kind>` states what kind of work the request is. It is input to
  central route eligibility, not a local machine-ranking override: it is
  forwarded, stamped, and part of the request identity, and it never reorders
  candidates here. It is not an override and needs no reason, and machine and
  seat overrides are unchanged by it. Omitting it inherits the controller's
  configured `[policy] workload`. An unenrolled controller refuses an explicit
  workload rather than dropping it, and is otherwise unchanged.
- Enrollment fails closed. If the Brigade helper is missing, times out, returns
  an unparsable or oversized reply, or denies the request, nothing is
  journalled, admitted, or dispatched.
- A repository is named by its local alias in every request, while policy is
  resolved against the canonical remote identity configured as
  `policy_identity`. Under enrollment that mapping is required, and a controller
  and a target that disagree about it is a hard denial rather than a fallback.
- Every policy denial shares the `policy_rejected` code and adds a fixed
  `reason` naming which denial it was, so a lapsed continuation, a stale
  decision, and a wrong machine are distinguishable without reading logs. See
  the contract reference for the vocabulary.

Limits worth knowing before scripting against it:

- Policy is resolved on every submit that reaches the Hub. A live decision is
  reused for the same request instead of taking a second reservation; an
  unstarted retry after an expired decision or a new policy version produces a
  fresh decision. A reusable decision has to match the machine and the seat the
  caller explicitly named, so an override never inherits capacity reserved for
  a different placement.
- Resubmitting a request that already dispatched returns its durable receipt
  without resolving policy, routing, or taking a second reservation, however far
  the policy has moved since. The same request id with a different payload is
  still a conflict. Request identity is the caller's own payload, including the
  host, seat, and override reason it asked for; the Hub-chosen machine and seat
  are excluded from it.
- Decision windows are bounded. A reservation that claims to last longer than
  24 hours is refused.
- The target resolves the canonical policy from its own node identity, refuses a
  policy that drifted from the controller's stamp, and renders the canonical
  effective rules, their provenance, and the Hub's prepared instructions into
  the first turn before the provider sees any data. Those rules come from the
  target's own resolution, not from the caller, and they ride in ordinary turn
  text because T3 exposes no higher-priority channel here.
- Only the target renews or releases a reservation. It renews while it observes
  a live run and releases only on its own durable terminal outcome; the
  controller relays status and records what the target reported. An `unknown`
  state or a transport failure keeps the reservation counted.
- `followup`, `approve`, and `answer` are admitted again before they resume
  anything. The target revalidates its own durable policy receipt, the current
  canonical policy version and digest, the same routing decision, and the live
  reservation, and only a granted renewal admits the action. A reservation that
  was released, expired, or never recorded fails closed; resuming then needs a
  newly negotiated reservation, which this client does not invent. An
  unreachable helper keeps the reservation held and still refuses to resume.
  `interrupt`, `status`, and `wait` are unchanged.
- The target acknowledges the session only after the canonical rules and the
  locally selected model are applied to the thread, and takes the launch
  admission only after that.

### Planning is not launch authorization

Under enrollment, choosing a model and being allowed to run it are separate
acts taken by different machines:

- The controller **plans**. After the Hub routes the request, the controller
  asks Brigade which seat a launch would use. The reply says
  `launch_authorized: false`, it is Hub-only with no last-known-good fallback,
  and the client re-checks both. The plan travels as `model_plan` and is
  recorded as `state: planned` everywhere, so it can never be read as proof
  that a launch was permitted. A caller may not supply a plan or an admission;
  both are refused rather than overwritten.
- The target **prepares and then admits**. It replays the route fenced to the
  stamped decision, asks the Hub to prepare that decision for the exact native
  selection it is about to run, applies the real model metadata and the prepared
  instructions, acknowledges the policy, and only then takes a real launch
  admission, immediately before the turn starts. If the policy proof, the
  binding, the roster revision, the selected target, or the reservation fails,
  no turn starts, the pre-turn cleanup runs, and a durable failure receipt is
  written.
- The canonical model identity is what the fleet admits; the native model the
  provider runs is the plan's explicit binding when it has one, and the
  canonical model verbatim when it does not. The prepared reply states both:
  `selected.model` is canonical and the optional `selected.launch_model` is the
  exact native provider identity. No alias is inferred, no delimiter is
  translated, and an absent mapping is never fabricated.
- The prepared reply also carries a `context_hash` for the exact context it
  prepared. The target forwards it verbatim on the acknowledgement
  (`--context-hash`). It is optional for compatibility parsing and mandatory for
  an enrolled prepared launch: a missing or malformed digest fails closed before
  anything is applied, acknowledged, or launched, and there is no version-only
  fallback. Two preparations of one policy version can differ, so an older
  applied context can never acknowledge a replacement.
- The launch admission carries the complete policy proof or it is malformed:
  the session, version, digest, and decision, plus `--repo <canonical verified
  policy identity>` and `--policy-context-hash <the exact prepared digest>`.
  Both are forwarded verbatim and neither is derived at that boundary. The
  acknowledge verb keeps its own `--context-hash` spelling; only the launch
  admission spells it `--policy-context-hash`.
- The acknowledgement reply is the unchanged session envelope plus one mandatory
  `context_hash` key. An enrolled session that applied a prepared context must
  see its exact prepared SHA-256 echoed back; a missing key, a null, a different
  digest, or an unknown extra key is a refusal. A null means only a legacy
  resolve-only session that loaded no context, and such a receipt is not
  launchable.
- A successful launch binds the policy session and the decision to one request
  id. An exact retry is idempotent; a different request id cannot reuse the
  proof. An approval or an answer for the execution already running reuses the
  same request and reservation generation instead of claiming another launch.
- **Known limitation:** once the reservation is terminal, released, or expired,
  a new follow-up stays `continuation-admission-required` until a fresh affinity
  protocol exists. This client does not invent a resurrection path.

### Adopting a parent delegation

A parent Brigade run that already holds a decision and a live reservation may
hand it to this fleet. `--delegation-id <id>` is the only public flag that opens
that path; there is no consumer or adapter flag, and creation is always the
parent's act.

- The policy consumer stays the parent's `brigade-run` and `t3-fleet` is the
  execution adapter the receipt names. This client never applies its own
  consumer defaults as a second layer under the parent, and ordinary requests
  stay on the fixed `t3-fleet` consumer.
- The authority is a live `brigade.fleet_policy_delegation.v1` receipt read
  through `brigade fleet policy delegation show`. It states
  `launch_authorized: false`: identity and reservation adoption, never provider
  admission. A caller-supplied receipt is not authority, there is no
  last-known-good copy, and there is no HTTP path. Every field is mandatory,
  unknown fields are refused, and the receipt is re-validated at each point of
  use.
- Adoption requires `[policy] enrollment = "required"` on the controller and the
  target. Enrollment `off` refuses instead of falling back.
- The receipt supplies the placement, session, decision, reservation, seat,
  workload, canonical repository, and immutable source revision. A supplied
  `host`, `repository`, `seat`, `workload`, or `source_revision` may only
  confirm it; `override_reason` and a caller `request_id` are refused.
- The canonical repository identity is resolved to exactly one configured local
  alias, so the parent never needs local alias knowledge. `--repository` may
  narrow an ambiguous identity to a matching alias. No match, an ambiguous
  match, and a mismatched alias all refuse.
- Nothing routes again and nothing reserves again. The target validates the same
  decision with `--decision-id`, prepares under `--delegation-id` against its
  actual configured checkout, and verifies the immutable source revision against
  the receipt. There is no local `HEAD` substitution and no unapproved branch
  fetch.
- One adoption is one execution. The execution identity is derived from the
  delegation and bound to it durably, with the parent request id kept
  separately, so a duplicate submit replays the durable receipt without a second
  route, reservation, or model execution. Different context under the same
  adoption conflicts.

## Deferring blocked work

`submit` accepts `allow_queue`. On a controller with the local outbox turned on,
a submit that meets a **pre-launch** capacity or offline block is stored locally
instead of failing, and answers `"state": "pending_autotick"` with a `job_id`, a
`queue_disposition`, and a `queue_reason`. `pending_autotick` is not a dispatch
state: nothing started and nothing was reserved.

`allow_queue` is opt-in per call. Absent, a block fails the call as before;
`false` is an explicit refusal and is honoured as one; a controller without the
queue enabled refuses it outright; and it is never valid beside `delegation_id`,
because an adopted delegation is one execution that already exists centrally. A
transport lost after a target could have accepted the work stays `unknown_state`
and is never queued for a second run.

`tick` drains a bounded slice of that queue under a single-owner lease, and
renews the reservations this machine holds as the target that runs them. Each
job is revalidated against live policy, roster, privacy, and capacity through
the ordinary dispatch path, and a retry re-routes rather than replaying the
machine a failed route picked. Its result reports `dispatched`, `blocked`,
`held`, and `expired` as arrays of redacted records, plus `examined`,
`truncated`, `reservations`, `counts`, and `last_tick_at`, which is what
separates a queue pending an autotick from one a tick has drained.

The queued prompt and request never leave the controller's own owner-only outbox
file: not to the Fleet Hub, not into a status payload, a receipt, an error, or a
tick result. The payload is purged on a known acceptance, on any terminal
disposition, and on retention expiry. Retention bounds how long this client
stores a prompt and nothing else; an expiry is never proof that an execution
stopped. Work whose acceptance is unknown is held, never retried, and its
capacity is never freed speculatively.

An external scoped timer may call the local `t3-fleet tick` periodically. This
tool starts no daemon, installs no service, and adds no cron entry.

`t3-fleet settle-sweep` and `t3-fleet install-settle-timer` are operator
commands, not tool actions: JSON `invoke` rejects them, and they are not in the
request schema. The sweep settles the T3 threads of fleet requests that have
been terminal past `[settle] grace_minutes`, only when the controller config sets
`[settle] enabled = true`. The timer is installed by an operator, never by an
agent.

## Approval and effects

| Command | Approval | Effects |
| --- | --- | --- |
| `t3-fleet doctor` | Read-only policy | process, network |
| `t3-fleet capacity` | Read-only policy | filesystem |
| `t3-fleet status` | Read-only policy | filesystem, process, network |
| `t3-fleet wait` | Read-only policy | filesystem, process, network |
| `t3-fleet submit` | Operator authorization | filesystem, process, network |
| `t3-fleet followup` | Operator authorization | filesystem, process, network |
| `t3-fleet approve` | Authorization naming the specific action being approved | filesystem, process, network |
| `t3-fleet answer` | Operator authorization | filesystem, process, network |
| `t3-fleet interrupt` | Operator authorization | filesystem, process, network |
| `t3-fleet settle-sweep` | Operator command, not a tool action | filesystem, process, network |
| `t3-fleet install-settle-timer` | Operator command, not a tool action | filesystem |
| `t3-fleet tick` | Prior approval for the queued work, catalog gate still applies | filesystem, process, network |
| `t3-fleet recover` | Operator authorization | filesystem, process, network |

Reuse current-session authorization for the exact action. Ask only for missing
authorization or a new boundary. For `approve`, authorization must name the
specific action being approved.

Keep the catalog entry at `approval_mode = "always"` until Brigade supports
per-action approval modes for one script entry. A wrapper may show read-only
calls as low risk, but it must not bypass the catalog gate. Recovery is
mutating because it updates controller state, although it never resubmits work.
More controllers do not increase provider quota.

## Delegated result handoff

`state` is always the real execution state. A terminal delegated execution whose
writable target worktree was retained may additionally report
`disposition: "pending-handoff"` and a frozen
`handoff: {target_machine, repository, branch, source_revision, request_id,
worktree_ref}`. `worktree_ref` is an opaque target-owned request reference, not
a resolvable path and not copied files. No patch is applied automatically and no
source checkout is claimed to have changed: the work is pending review,
verification, and handoff. A pre-launch failure omits the handoff, and a running
or `unknown` request never claims a completion disposition.

## Runtime mode policy

`runtime_mode` is an in-session T3 runtime permission scope, not a fleet
orchestration approval. Keep it separate from the Brigade catalog
`approval_mode = "always"` and operator authorization for `submit`, `followup`,
`interrupt`, `approve`, `answer`, and `recover`. Reuse current-session
authorization for the exact action.

Public submits and follow-ups use `runtime_mode = "approval-required"`.
This remains independent of model admission and operator approval.
A `needs_attention` result from `status`,
`wait`, `answer`, or `approve` carries `pending_request`. Use its `id` unchanged
and its `respond_with`: `approve` takes `approval_id`, and `answer` takes
`input_id`. No other state carries `pending_request`. For `approve`,
authorization must name the specific action being approved.
Preserve the package's `approval-required` default in every public projection.
