# Command contract

Use the local `t3-fleet` client. The client selects permitted local or peer
transport from private configuration. Public callers never construct connection
details, service endpoints, internal actions, or private state locations.
Peer mode uses `target-authoritative` coordination.

## Commands

The operator command surface includes the following commands. `invoke` and
`brigade-invoke` serve integration plumbing. Do not use them to bypass routing
or the approved tool runner's gates.

| Command | Purpose | Mutates state |
| --- | --- | --- |
| `t3-fleet doctor` | Check controller state, target identity, project mapping, version, `active`, `limit`, and `repository_limit`. Scope with `--host` and `--repository`. | No orchestration mutation |
| `t3-fleet capacity` | Read host eligibility, `active`, `limit`, `repository_limit`, and circuit state. Scope with `--host`. | No |
| `t3-fleet submit` | Journal and start a new T3 thread. | Yes, approval required |
| `t3-fleet status` | Read the current state for one request. | Controller reconciliation only |
| `t3-fleet wait` | Poll until terminal, attention, or timeout. | Controller reconciliation only |
| `t3-fleet followup` | Start another turn on an existing thread. | Yes, approval required |
| `t3-fleet interrupt` | Request interruption of the active turn. | Yes, approval required |
| `t3-fleet approve` | Respond to one current approval request. | Yes, approval required |
| `t3-fleet answer` | Respond to one current user-input request. | Yes, approval required |
| `t3-fleet recover` | Recover an `unknown` request without resubmission. | Yes, approval required |
| `t3-fleet tick` | Drain a bounded slice of the local outbox and renew this machine's own reservations. | Yes, under the approval the queued submits already had |
| `t3-fleet install-settle-timer` | Operator-only timer installation, never a model-driven action. | Operator authorization |
| `t3-fleet settle-sweep` | Operator and timer only (rejected by JSON `invoke`). Settle the T3 threads of fleet requests terminal past `[settle] grace_minutes`. Off unless `[settle] enabled = true`. | Operator-installed timer. Not a tool action |

## T3 version gate

The client accepts T3 base versions 0.0.30-0.0.40, 0.0.43, 0.0.45, 0.0.46,
and 0.1.x, with optional prerelease/build suffixes. Protocol 2 applies to
0.0.46 and 0.1.x, and requires descriptor `orchestrationProtocolVersion` 2
and shell `schemaVersion` 2. The other accepted bases use protocol 1.
Check `t3-fleet doctor` before dispatch or fleet host updates. T3 0.2+ needs
verification and a gate change first.

## Submit envelope

Choose one of two explicit paths:

- **Enrolled policy path:** `[policy] enrollment = "required"`. Prefer
  `--host auto` and omit `seat` so the Hub selects placement. An explicit host
  or seat needs an override reason and the same live admission, never a bypass.
- **Legacy path:** enrollment `off`. Supply the configured allowlisted host,
  repository, and logical seat, with Brigade model admission still required.
  This is not a fallback after enrolled policy denial.

Resolve `source_revision` to an immutable full 40-character commit and confirm
the repository alias maps to the exact intended T3 project and workspace on the
selected target. Do not guess a project from its title or default model. In
peer mode, omit `request_id` so the local client generates a qualified ID.
Retain the Hub-selected seat and returned request ID as execution evidence.

The example uses a placeholder logical seat. Resolve an admitted seat from the
operator's current roster before use. For attached app tools, discover live
capabilities as described in [orchestration.md](orchestration.md).

```json
{
  "action": "submit",
  "host": "target-a",
  "repository": "example-repo",
  "source_revision": "0123456789abcdef0123456789abcdef01234567",
  "title": "Fix issue 123",
  "seat": "example-seat",
  "prompt_file": "<protected-prompt-file>",
  "runtime_mode": "approval-required",
  "interaction_mode": "default"
}
```

Require operator authorization before `submit`. Reuse current-session approval
for exactly authorized work rather than asking again. Do not provide executable
commands, worktree paths, or private locations in a request. Writable work runs
in the target-managed isolated worktree, never its canonical checkout.
`seat` is a logical Fleet Hub roster key. The controller and target resolve and
confirm its exact provider, model, reasoning, binding, roster revision, and
digest through Brigade before mutation. Caller model overrides and T3 project
defaults are never dispatch authority.

`--run-setup-script` is opt-in and executes the project's configured setup
script. Review that setup and its side effects within the task's authorization
before enabling it. Do not add the flag blindly to repair readiness. A backend
that cannot execute setup must refuse it, not silently ignore it.

`workload` is optional and states what kind of work this is. It is input to
central route eligibility, not a local machine-ranking override: it is forwarded
to the Hub, stamped onto the request, and included in the request identity, and
it never reorders candidates on this machine or relaxes a data rule. Omitting it
inherits the controller's configured `[policy] workload`. It is not an override
and does not need `override_reason`. Machine and seat overrides are unchanged.
An unenrolled controller refuses an explicit workload rather than silently
dropping it, and behaves exactly as before when none is given.

`runtime_mode` is an in-session T3 permission scope. Public submissions and
follow-ups use `approval-required`. Operator approval, Brigade model admission,
and the catalog's tool-runner approval gate remain required independently.
A `needs_attention` result supplies `pending_request` for the current approval
or user question. Use its ID unchanged and obtain the operator's exact response.

## Fleet policy routing

An enrolled controller (`[policy] enrollment = "required"`) treats the Fleet
Hub as the routing authority, reached only through the Brigade CLI. Enrollment
`off` still requires Brigade model admission for an explicit logical seat.

### Repository identity

A repository has two identities and they are not interchangeable.

- The **local alias** (`[repositories.example-repo]`) is what a caller names,
  what the request and work ID use, what per-host checkout paths are keyed on,
  and what the UI shows.
- The **canonical identity** (`policy_identity`) is the remote identity the
  Fleet Hub is asked about, so one repository keeps one policy across machines
  whose checkouts are named or located differently.

```toml
[repositories.example-repo]
policy_identity = "github.com/example-org/example-repo"
default_branch = "main"
branch_prefix = "codex/"

[repositories.example-repo.paths]
target-a = "/srv/repos/example"
```

Under `enrollment = "required"` every repository needs an explicit
`policy_identity`. A local alias is not a canonical identity, so a missing
mapping fails closed at config load rather than at dispatch. Enrollment `off`
does not need one and behaves exactly as before.

Callers never supply a canonical identity. A caller names the alias, and the
controller substitutes the configured canonical identity for every resolve,
route, and acknowledge. The target independently checks the same thing from its
own side: it asks Brigade to resolve using the checkout path it actually holds
(`resolve --repo <actual checkout path> --decision-id <id>`), which Brigade
normalizes with its own identity resolver and compares against the repository
the decision was stored against. This client never parses a git URL and never
accepts the alias as a fallback. A controller and a target that disagree about
which repository this is produce a hard denial before any data reaches a
provider.

Ask for central selection by setting the submit envelope's `host` to the
literal value `auto` instead of an allowlisted host ID:

```sh
t3-fleet submit --host auto --repository example-repo \
  --source-revision 0123456789abcdef0123456789abcdef01234567 \
  --title "Fix issue 123" --prompt-file <protected-prompt-file>
```

Under central selection, omit `seat` and let the resolved policy supply it.
Naming an explicit host or an explicit seat is an override: both require
`override_reason` (`--override-reason "<why>"`), both are forwarded to the same
central route, and a decision that lands on a machine or a seat the caller did
not name is refused rather than followed. A seat override asks the Hub for a
seat. It never ranks models locally and never relaxes a data rule.

```sh
t3-fleet submit --host auto --seat <current-seat> \
  --override-reason "operator pinned the review seat" \
  --repository example-repo \
  --source-revision 0123456789abcdef0123456789abcdef01234567 \
  --title "Fix issue 123" --prompt-file <protected-prompt-file>
```

Limits:

- Enrollment fails closed. An unreachable, slow, oversized, or denying helper
  leaves nothing journalled, admitted, or dispatched.
- Policy is resolved on every submit that reaches the Hub. A live decision is
  reused for the same request rather than taking a second reservation. A reused
  decision must match the machine and the seat the caller explicitly named.
- Resubmitting a request that already dispatched returns its durable receipt.
  Policy is not resolved or routed again and no second reservation is taken,
  even if the policy version changed or the decision window elapsed. The same
  request id carrying a different payload is still refused as a conflict, and a
  request that never started still retries.
- The request identity is the caller's own payload, including the host, seat,
  and override reason the caller asked for. The machine and seat the Hub chose
  are ephemeral and are excluded from it.
- An unstarted retry, a new policy version, or an expired decision produces a
  fresh decision and a fresh reservation.
- A reservation window longer than 24 hours is refused.
- The target validates its route before it prepares anything. The stamp is a
  pointer, so the target first replays the route fenced to the stamped decision
  id, and only then asks the Hub to prepare that same decision for the exact
  native selection it is about to run. It refuses a policy that drifted from the
  controller's stamp and renders the canonical effective rules, their
  provenance, the prepared instructions, and the machine and seat the Hub route
  actually selected into the first turn. The rules the model is given are the
  ones the target resolved, never anything the caller supplied. This is ordinary
  turn text. T3 exposes no higher-priority channel here.
- Only the target renews or releases a reservation, fenced by reservation id,
  decision id, and session id. It renews while it observes a live run and
  releases only on its own durable terminal outcome. The controller relays
  status and records the state the target reported. An `unknown` state, a
  transport failure, or a refused helper all keep the reservation counted, and
  a released reservation is never resurrected.
- The target acknowledges the session only after the canonical rules and the
  locally selected model are applied, and takes the launch admission only after
  that. See "Planning, preparing, launching" below.
- `followup`, `approve`, and `answer` are admitted again before they resume
  anything. The target checks its own durable policy receipt, the current
  canonical version and digest, the same routing decision, and the live
  reservation first, and only then re-takes the launch admission under this
  request's own id. Only a granted renewal admits the action. A continuation resolves
  under the recorded decision id, exactly as a first dispatch does, and never
  re-resolves on origin alone. A released, expired, or missing reservation fails
  closed, and resuming then needs a newly negotiated reservation rather than a
  revived one. A policy receipt written before canonical identity existed cannot
  prove which repository it was admitted for, so it fails closed too. An
  unreachable helper leaves the reservation held and still refuses. `interrupt`,
  `status`, and `wait` keep their existing behavior.

### Adopting a parent delegation

A parent Brigade run that already holds a routing decision and a live
reservation can hand that decision to this fleet instead of having a second one
made. The parent creates the delegation. T3 never does. Adoption is opened by
exactly one public flag:

```sh
t3-fleet submit --delegation-id del-8f2c1a --title "Fix issue 123" \
  --prompt-file <protected-prompt-file>
```

There is deliberately no consumer or adapter flag. The policy consumer stays the
parent's `brigade-run` and `t3-fleet` is the execution adapter the receipt
names, so this client never inserts its own defaults as a second layer under the
parent. Ordinary requests are unaffected and stay on the fixed `t3-fleet`
consumer.

The authority is a `brigade.fleet_policy_delegation.v1` receipt, read live
through the authenticated Brigade CLI:

```sh
brigade fleet policy delegation show --delegation-id <id> --json
```

The receipt states `launch_authorized: false`. It is identity and reservation
adoption, never provider admission: a real launch still needs the current
decision-scoped preparation, the applied context, the acknowledgement, the
launch admission, and target execution. A caller cannot supply a receipt. A
locally authored document is not authority regardless of its form. There is
no last-known-good copy, and there is no HTTP path. Every field is mandatory,
unknown fields are refused, timestamps and the reservation-bounded expiry are
checked, and the receipt is re-validated at each point of use rather than
believed once.

Everything about the placement comes from the receipt: the consumer, the
adapter, the parent request id, the decision, the reservation, the session, the
canonical repository, the seat, the workload, the origin node, the target
machine and node, and the immutable 40-character source revision. A supplied
`host`, `repository`, `seat`, `workload`, or `source_revision` may only confirm
the receipt. Anything that contradicts it is refused. `override_reason` does not
apply, because the placement decision already exists, and a caller-supplied
`request_id` is refused, because the execution identity is derived from the
delegation.

Adoption requires `[policy] enrollment = "required"` on both the controller and
the target. Enrollment `off` refuses rather than falling back to the pre-policy
contract.

Repository identity is resolved locally, so the parent never needs to know what
this machine calls a repository. The controller finds the single configured
repository whose `policy_identity` is the receipt's canonical identity. An
optional `--repository` may narrow to a configured alias whose identity is that
same one. No match, more than one match, and a narrowed alias that maps
elsewhere are all refusals. There is no nearest-match and no alias guessing.
`origin_node` on the receipt is the canonical origin identity and is used
verbatim, because the Hub derives any original alias on its side.

What adoption does *not* do:

- It never calls `route` and never allocates a second reservation. The
  controller adopts the decision the receipt names and records it.
- It never substitutes a local `HEAD` for the delegated source revision, never
  transfers uncommitted context, and never fetches or deploys an unapproved
  remote branch. A dirty source is blocked upstream as
  `remote-context-unavailable`. The target verifies the exact commit exists in
  its actual configured checkout using the existing source verification, and it
  compares that commit against the receipt rather than against a branch.
- It never lets this adapter's own consumer defaults become a new layer.

The target validates the adoption independently. `delegation show` is
authenticated to the owning origin or the selected target, so the target reads
the receipt itself under its own node identity rather than trusting the stamp,
which carries only the delegation id. It refuses a receipt for another machine,
another repository, another seat, a stamped context the receipt does not hold,
or a source revision other than the delegated one. It then validates the same
decision with `--decision-id` rather than routing again, performs the
decision-scoped and delegation-scoped preparation against its actual configured
checkout, applies the context, acknowledges, and passes the same launch proof
the current implementation already does. `models plan`, `policy session
prepare`, and the launch `models admit` each carry `--delegation-id`, and the
Hub validates the delegation before it selects the adapter binding.

One adoption is one execution. The local execution identity is derived from the
delegation and bound to it in durable storage, with the parent's own request id
kept separately, so a duplicate submit is answered from the durable receipt
before the Fleet Hub is asked anything: no second delegation read, no second
route, no second reservation, and no second model execution. Different context
under the same adoption is a conflict rather than a second run. Once dispatched,
a durable retry returns the prior result. An expired or released delegation is
never read as permission to launch again. Transport ambiguity stays `unknown`
and the capacity stays held.

Continuation is unchanged in kind. An approval or an answer for the same active
reserved generation revalidates and reuses it without another reservation or
launch claim. A new follow-up after a terminal, released, or expired reservation
stays `continuation-admission-required` until a fresh affinity decision exists.

### Reading a policy denial

Every fleet policy denial carries the `policy_rejected` code, so the JSON error
also carries a fixed `reason` naming which denial it was. The reason is drawn
from a closed vocabulary and is never free text, a path, or helper output:

Before any retry suggested below, establish that the request never launched
and that the same work remains authorized. Recover ambiguous acceptance under
the original qualified request ID. Never use a new submit to probe it.

| `reason` | What to do |
| --- | --- |
| `continuation-admission-required` | Admission for this request lapsed. It needs a newly negotiated reservation before it can resume. |
| `policy-repository-mismatch` | The controller and the target disagree about the canonical repository, or one has no `policy_identity`. Fix the config on both sides. |
| `policy-machine-mismatch` | The decision names another machine. |
| `policy-seat-mismatch` | The decision names another seat, or the seat does not match the confirmed model admission. |
| `policy-decision-superseded` | The Hub replaced this routing decision. Re-submit to get a fresh one. |
| `policy-digest-stale` | The decision is no longer current, or the model plan is past its window. Re-submit. |
| `model-admission-denied` | The roster denied the model admission, or the request carried no model plan. |
| `policy-selection-mismatch` | The Hub prepared a provider, native model, instance, or reasoning effort other than the one that was planned and applied. |
| `policy-plan-mismatch` | The launch admission granted a seat or model other than the one that was planned. |
| `delegation-not-enrolled` | Adoption needs `[policy] enrollment = "required"` on the controller and the target. |
| `delegation-invalid` | The delegation is unknown, expired, released, or not this caller's to adopt. |
| `delegation-conflict` | A supplied option, a stamped field, or a dispatched source revision contradicts the receipt. |
| `delegation-repository-unmatched` | No single configured repository matches the delegated canonical identity, or the named alias maps elsewhere. |

## Planning, preparing, launching

Under enrollment, choosing a model and being allowed to run it are two separate
acts, taken by two different machines, in this order:

1. **Plan (controller).** After the Hub has routed the request, the controller
   runs `brigade fleet models plan --consumer t3-fleet --seat <seat> --json`.
   This is read-only planning. The reply states `launch_authorized: false`, it
   is Hub-only with no last-known-good fallback, and this client re-checks both
   before accepting it. The plan crosses to the target as `model_plan` and is
   recorded as `state: planned` everywhere it is stored or reported, so a plan
   can never be read as proof that a launch was permitted.
2. **Route, then prepare (target).** The target replays the route fenced to the
   stamped decision, then runs
   `brigade fleet policy session prepare --consumer t3-fleet --repo <actual
   checkout path> --session-id <id> --origin <original origin> --provider
   <driver> --model <native model> --instance-id <id> [--reasoning <effort>]
   --decision-id <id> --json`. The reply is the ordinary resolve envelope plus
   the canonical `repo_identity`, the `selected` seat and native selection, the
   `instructions` the Hub wants on the policy-bearing turn, and the
   `context_hash` of that whole prepared context. `selected.model` is the
   canonical roster identity. The optional `selected.launch_model` is the exact
   native provider identity the turn is launched on, and it is the only extra
   field the strict parser permits. When it is absent the canonical identity is
   used verbatim. Nothing is inferred from a delimiter or a slug in either
   direction, and an absent mapping is never fabricated. The target refuses
   anything that is not the repository it holds, the seat the route chose, or
   the exact canonical and native selection it planned.
3. **Apply, acknowledge, then admit (target).** The target creates the thread
   with the real model metadata, applies the prepared instructions, acknowledges
   the policy through the existing session acknowledge contract with
   `--context-hash <the digest the preparation returned>`, and only then
   runs `brigade fleet models admit --consumer t3-fleet --seat <seat>
   --request-id <id> --phase launch --expect-revision <planned revision>
   --expect-digest <planned digest> --policy-session-id <id> --policy-version
   <n> --policy-digest <sha256:...> --decision-id <id> --repo <canonical
   verified policy identity> --policy-context-hash <the exact prepared digest>
   --json`, immediately before `thread.turn.start`. This ordering is the point: the old flow admitted
   a model before it could say what policy it had applied, so the admission
   could never carry proof of the policy it was admitted under.

The launch proof is complete or it is malformed. There is no partial form. Its
last two fields are frozen: `--repo` is the canonical repository identity the
policy was verified under, never a checkout path and never carrying a
credential, and `--policy-context-hash` is the exact prepared digest this target
acknowledged, forwarded verbatim. Neither is derived at the admission boundary.
Note the spelling: the acknowledge verb keeps `--context-hash`, and only the
launch admission spells it `--policy-context-hash`, because there it sits beside
the rest of the proof.

The acknowledgement reply is the same `brigade.fleet_policy_session.v1`
envelope it always was - `schema`, `session_id`, `consumer`, `repo_identity`,
`version`, `digest`, `state`, `loaded_at`, `applied` - plus one mandatory
`context_hash` key. The key is always present. For an enrolled session that
applied a prepared context, its value must equal the exact prepared SHA-256 that
was sent: a missing key, a null, a different digest, or an unknown extra key is
a refusal rather than a weaker success. A null is meaningful only for a legacy
resolve-only session that loaded no prepared context, and such a receipt is not
launchable: a continuation whose stored receipt carries no context hash is
`continuation-admission-required` rather than resumed on trust.

`context_hash` identifies the exact context that was applied. It is a bounded
digest the Hub produced from the effective policy, its sources, the
instructions, and the canonical and native model identity. It is optional for
compatibility parsing and mandatory for an enrolled prepared launch: a missing
or malformed digest fails closed before anything is applied, acknowledged, or
launched, and there is no version-only fallback on this path. It is forwarded
verbatim, never derived on this side and never rebuilt from a timestamp, and it
is persisted with the prepared snapshot. Two preparations of one policy version
can differ in override or model, so the Hub compares the digest under its own
transaction and an older applied context can never acknowledge a replacement.

The launch reply is the `brigade.model_admission.v1` envelope. Its binding
accepts `instance_id`, `service_tier`, and an optional `model`. Any other key is
refused. The canonical model identity is what the admission carries as `model`
and what the ledger and receipts keep. A `binding.model` is the explicit native
model the provider is launched on. It is exposed on the admission as
`launch_model`, and the thread is created and every turn is dispatched with it.
It must equal the plan's launch model and the prepared selection. When the
binding names no model, the canonical model is used verbatim. Nothing here
infers an alias or translates a delimiter.

If the policy proof, the binding, the roster revision, the selected target, or
the reservation fails, no provider turn starts, the existing pre-turn cleanup
runs, and a durable failure receipt is written.

Lifecycle:

- A successful launch binds the policy session and the routing decision to one
  request id. An exact retry of that request is idempotent. A different request
  id cannot reuse the proof.
- An approval or an answer for the execution that is already running
  revalidates the current policy, target, and live reservation, then re-takes
  the launch admission under the same request id. It never claims a second
  launch and never takes a second reservation.
- Once the reservation is terminal, released, or expired, a new follow-up is
  `continuation-admission-required` until a fresh affinity protocol exists.
  **This is a known limitation.** This client does not invent a resurrection
  path for capacity the fleet has already taken back.
- `interrupt`, `status`, and `wait` are unchanged.

Server-side enforcement is Brigade's: a launch requires a current acknowledged
exact selection plus an active target-owned reservation, and an older client
that cannot present that proof is denied while authoritative policy is active.

## Deferring blocked work: `--allow-queue` and `tick`

There is no resident scheduler. `wait` polls one request, and a durable request
record holds only a digest and a prompt size, so a submit that met a capacity or
offline block used to lose the work outright.

`--allow-queue` on a submit changes only that. When a **pre-launch** block stops
the dispatch, the caller's own request and prompt are stored in the
controller's local outbox and the submit answers:

```json
{
  "action": "submit",
  "state": "pending_autotick",
  "job_id": "job-<32 hex>",
  "job_state": "queued",
  "queue_disposition": "queued-blocked",
  "queue_reason": "capacity-exhausted",
  "execution_state": "never-dispatched",
  "request_id": null
}
```

`pending_autotick` is deliberately not a dispatch state. Nothing started, no
capacity was reserved, and only a later `t3-fleet tick` will move it.

Rules, all of which are refusals rather than preferences:

- The flag is opt-in per submit. Absent, a block fails the submit exactly as
  before. `"allow_queue": false` is an explicit refusal and is honoured as one.
  A controller with no `[outbox] enabled = true` refuses the flag outright.
- `--delegation-id` is never queued. An adopted delegation is one execution that
  already exists centrally. Deferring it locally would make it two.
- A lost transport after a target could have accepted the work is an
  `unknown_state`, not a block, and it is never queued for a second run.
- A target that answers it is busy (`repository-busy` or `capacity-exhausted`)
  has declined before creating a thread, worktree, or branch. That answer is a
  block like any other: the request is marked `queued`, and it may be queued
  or retried with the same request ID.
- The approval the original submit had is the approval the deferred attempt
  runs under. Deferring the same work asks for no second approval and widens
  nothing: what is replayed is the caller's own request, unchanged.

A job carries three separate identities and they are never collapsed: `job_id`
is this controller's own work identity, minted before any machine is chosen.
`request_id` is the target-qualified execution identity and exists only once a
dispatch attempt named one. `parent_request_id` belongs to delegated work, which
never enters this queue at all.

`t3-fleet tick` takes no arguments. It:

1. takes a single-owner tick lease, so an overlapping tick reports `"state":
   "busy"` rather than racing it.
2. applies retention, then drains a bounded number of ready jobs, each under a
   per-job lease generation that fences every write it makes.
3. revalidates each job from scratch through the ordinary dispatch path, so live
   policy, the live roster, privacy, seat, consumer, and live capacity are all
   re-checked, and a retry re-routes rather than replaying the machine a failed
   route picked.
4. renews the reservations this machine holds **as the target that runs them**,
   so long work no longer depends on an origin CLI staying in `wait`. A source
   never renews another target's reservation, `tick` only ever renews, and an
   execution whose outcome is unknown stays counted and held.

Outcomes are reported in `dispatched`, `blocked`, `held`, and `expired`, each an
array of redacted records, with `examined`, `truncated`, `reservations`,
`counts`, `last_tick_at`, and `last_failure_reason` beside them. `last_tick_at`
is what tells a queue that is *pending* an autotick from one a tick has actually
drained.

`queue_disposition` is a closed vocabulary: `queued-blocked`, `queued-retry`,
`dispatched`, `held-acceptance-unknown`, `failed-policy`, `failed-invalid`,
`failed-conflict`, `failed-attempts`, `expired-retention`. A still-blocked job
backs off and stays queued within a bounded attempt budget. A policy, privacy,
or seat refusal is terminal. Unknown acceptance is `held`: never retried, never
freed speculatively, and reconciled against the same durable target request.

Privacy and retention:

- The prompt and the stored request live only in the controller's own outbox
  file, in an owner-only directory. They are never sent to the Fleet Hub and
  never appear in a status payload, a receipt, an error, or a tick result.
- The payload is purged on a known acceptance, on any terminal disposition, and
  on retention expiry. A held job keeps its payload, because that is the local
  record of what the identity was asked to do.
- Retention bounds how long this client stores a prompt and says nothing else.
  An expired record reports `execution_state: never-dispatched` only when no
  drain attempt ever reached a target. A held record keeps `unknown`. **A
  retention expiry is never proof that an execution stopped.**

A separate scoped timer may invoke the local `t3-fleet tick` periodically. That
is an external scheduler calling the public command. This client starts no
daemon, installs no service, and adds no cron entry of its own.

## Delegated result handoff

`state` always remains the real execution state. A terminal delegated execution
whose writable target worktree was retained may also report:

```json
{
  "state": "succeeded",
  "disposition": "pending-handoff",
  "handoff": {
    "target_machine": "target-a",
    "repository": "git.example.invalid/team/repository",
    "branch": "codex/<request id>",
    "source_revision": "<40 lowercase hex>",
    "request_id": "<request id>",
    "worktree_ref": "<opaque target-owned request reference>"
  }
}
```

The field set is frozen with the T3 coordinator. `worktree_ref` is an opaque
reference the target owns, not a path a reader may resolve and not a copied
tree. Nothing was applied anywhere else: no patch is applied automatically, and
no claim is made that a source checkout changed. Retained target work is
explicitly pending review, verification, and handoff.

A pre-launch failure omits the handoff, because its worktree was cleaned up. A
running or `unknown` request never carries a completion disposition. An
undelegated request never carries one at all. The source side preserves the
target provenance and this disposition and must not read an empty local diff as
"implemented".

## Attention and recovery

Require operator authorization for `followup`, `approve`, `answer`,
`interrupt`, or `recover`. Reuse current-session authorization for the exact
action. For `approve`, authorization must name the specific action being
approved. Use `approve` only when the current snapshot
contains the matching approval ID. Use `answer` only when it contains the
matching user-input ID.

A `needs_attention` result from `status`, `wait`, `answer`, or `approve`
carries `pending_request`, and no other state does:

| Field | Meaning |
| --- | --- |
| `id` | The T3 runtime request ID, up to 1024 characters and never truncated. Pass it unchanged. |
| `kind` | The T3 request kind, such as `user_input`, `command`, `file-change`, or `permission`. |
| `created_at` | When T3 opened the request, when known. |
| `respond_with` | `answer` (pass `id` as `input_id`) or `approve` (pass `id` as `approval_id`). Absent for kinds the fleet cannot respond to. |
| `questions` | For `user_input`: up to 5 questions, each with `id`, `header`, `question` (up to 2000 characters), `options` (up to 10, each `label` and optional `description`), and optional `multi_select`. `questions_truncated` is `true` when T3 asked more. |
| `approval` | For approval kinds, when T3 has it: `request_kind`, `summary` (the provider's description of the action, up to 2000 characters), `app_name`, and `options` (`decision`, `label`, optional `warning`). |

Every string is bounded and unknown fields are dropped, first by the target and
again by the controller. When the target cannot read the question text, the
result still carries `id`, `kind`, and `respond_with`. Answer with the question
IDs as keys, for example:

```bash
t3-fleet answer < "$ANSWER_JSON"
```

`followup` refuses a `needs_attention` request and points to `answer` and
`approve`. A continuation re-confirms the request's model admission with the
Fleet Hub. When the stored admission has outlived its fifteen-minute window,
the target re-confirms it under a per-request continuation key pinned to the
same roster revision and digest, so a question that waited for hours can still
be answered. When `answer` or `approve` fails before the target sent anything
to T3, the error says so in `detail`, the request keeps its state, and the same
call can be retried. Only a failure that may have reached T3 moves the request
to `unknown`.

`recover --target <allowlisted-target-id>` without a request ID resolves only
when the local controller journal contains exactly one unknown request for that
target. Zero or multiple matches fail closed. A target is always a lookup hint:
it does not select an execution destination or override an explicit qualified
handle.

## Result handling

Expect `schema: "t3-fleet.result.v1"` for successful calls and retain the
returned qualified `request_id` as the cross-controller handle. A rejected call
uses the closed `t3-fleet.error.v1` envelope with a stable error code and a
redacted message. An `unknown_state` error includes `request_id` when the
controller already journaled the request. Retain that handle for recovery.
Treat the normalized state as authoritative:

- `queued`, `preflight`, or `dispatching`: check again later.
- `running`: use `t3-fleet status` or bounded `t3-fleet wait`.
- `needs_attention`: inspect the current request. Use an exactly authorized
  response or obtain missing operator input.
- `succeeded`: fetch fresh status evidence before reporting completion.
- `failed` or `interrupted`: report the observed state and available safe output.
- `blocked_offline`: do not force dispatch. Read per-host state and diagnose
  transport, authenticated protocol compatibility, and project mapping
  separately. The aggregate state alone does not prove a machine is down.
- `unknown`: run `t3-fleet recover`. Do not submit the original request again.

Doctor host entries may report `incompatible` or `at_capacity` alongside
`eligible`, `disabled`, or `blocked_offline`. Use their `active`, `limit`, and
`repository_limit` fields to distinguish host and per-repository capacity from
connectivity or version failures. A target can refuse work with
`repository-busy` or `capacity-exhausted` before it creates a thread, worktree,
or branch. Preserve a returned `target_busy` refusal rather than treating it
as accepted work. Retry only known pre-launch refusals under the same request
ID, within the original authorization.
Readiness does not grant Hub admission. See
[orchestration.md](orchestration.md) for scoped diagnosis and canary evidence.

`t3-fleet wait` timing out leaves the task running. `t3-fleet interrupt` does
not delete or archive its thread. Recovery never replays the original prompt.

More controllers do not increase provider quota.
