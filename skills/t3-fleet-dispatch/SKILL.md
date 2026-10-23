---
name: t3-fleet-dispatch
license: Apache-2.0
description: Route T3 child tasks or separate sessions, and dispatch, monitor, control, or recover work through the configured local `t3-fleet` client. Use for T3 orchestration and fleet tasks needing attention or recovery.
---

# T3 Fleet Dispatch

Use attached T3 app tools for local orchestration and app operations. For
cross-host fleet dispatch, use the local `t3-fleet` client only. It uses
this controller's private configuration to select the permitted connection.
Never construct connection commands, service endpoints, internal actions,
worktree paths, or private state locations.
Peer mode uses `target-authoritative` coordination.

The operator commands include `doctor`, `capacity`, `submit`, `status`,
`wait`, `followup`, `interrupt`, `approve`, `answer`, `recover`, and `tick`.
`invoke` and `brigade-invoke` are integration plumbing, not alternate routing.

Check the [T3 version gate](references/contract.md#t3-version-gate) before
dispatch or fleet host updates. The client accepts 0.0.30-0.0.40, 0.0.43,
0.0.45, 0.0.46, and 0.1.x, with optional prerelease/build suffixes. Protocol 2
applies to 0.0.46 and 0.1.x. T3 0.2+ needs a gate change first.

## Choose the orchestration surface

Discover `orchestrator_capabilities` before choosing a provider, model, or
options. Respect its child eligibility, constraints, features, and batch limit.

| Work requested | Route and handle |
| --- | --- |
| Same-provider child with a model the native tool supports | Native subagent and native lifecycle handle |
| Model unsupported by native tools, cross-provider child, or explicitly T3-owned child | `delegate_task`, then `task_status` / `task_cancel` using `taskId` |
| Explicitly requested separate/new top-level session | `t3_thread_launch` with workspace binding, retain `threadId` and `runId` |
| Requested batch of top-level sessions sharing this checkout | `create_threads`, retain each thread/run pair |
| Cross-host fleet work | Local `t3-fleet`, retain qualified request ID and any Brigade delegation ID separately |

Brigade-wired substantial implementation still follows local `brigade run`
policy. App tools do not bypass routing, admission, privacy, or authorization.
Child prompts are self-contained: T3 delegation does not copy parent history.
Assign file ownership, coordinate writers, and specify outputs and verification.
Async completion wakes the parent. Finish independent work, then yield if only
waiting. Do not busy-poll or spawn watchers. Use `watch_pull_request` for PR
events and `unwatch_pull_request` before handing back. A completed turn with
`workState: waiting_for_children` is still ongoing. Read the result before
claiming completion. Preserve handles after a timeout, which does not cancel.

Read only the references needed for the task:

- [orchestration.md](references/orchestration.md): discovery fallback, child
  dispatch, workspace binding, context transfer, PR tracking, and readiness.
- [supervision.md](references/supervision.md): paginated reads, run control,
  queues, user questions, and explicitly requested recurring schedules.
- [app-surfaces.md](references/app-surfaces.md): projects, environment settings,
  preview/browser, devices, and attachment upload then delivery.
- [contract.md](references/contract.md): cross-host client requests and controls.

Reuse current-session authorization for the exact action. Tool availability or
runtime mode does not authorize a new boundary. Persistent schedules require a
request for recurring work. Link every PR created or worked on, including all
stack layers, and verify the association list. Linking grants no publication or
merge authority.

## Operator approval

`submit`, `followup`, `interrupt`, `approve`, `answer`, and `recover` require
operator approval for the exact action and scope. Reuse that approval when it
already exists in this session. Never infer approval from runtime mode.
For `submit` and `followup`, use `approval-required`.

## Public commands

| Command | Use |
| --- | --- |
| `t3-fleet doctor` | Scoped readiness checks, without launch authorization. |
| `t3-fleet capacity` | Host and per-repository capacity. |
| `t3-fleet submit` | Start authorized work, optionally allowing a local queue. |
| `t3-fleet status` | Read current evidence and pending requests. |
| `t3-fleet wait` | Wait for terminal, attention, or timeout. |
| `t3-fleet followup` | Resume an admitted request with approval. |
| `t3-fleet interrupt` | Interrupt the named request with approval. |
| `t3-fleet approve` | Return the operator's specific approval decision. |
| `t3-fleet answer` | Answer a current input request with approval, JSON stdin only. |
| `t3-fleet recover` | Reconcile unknown acceptance with approval. |
| `t3-fleet tick` | Drain previously authorized queued work and renew local reservations. |

## Admission prerequisites

Install and configure the external Brigade CLI on the controller and targets.
Use a logical seat from the operator's current admitted roster. The package
contains no roster, credentials, or provider fallback. Even `[policy]
enrollment = "off"` requires Brigade model admission. A missing helper, denied
seat, or stale policy fails closed before launch. Read
[the deployment prerequisites](../../docs/deployment.md) before real dispatch.

## Dispatch

1. Run `t3-fleet doctor` before the first dispatch in this session. Use
   `--host target-a --repository <repository-id>` to scope it to the
   intended work. Stop if the selected host or repository is ineligible. An
   aggregate `blocked_offline` does not decide every host's eligibility.
2. Run `t3-fleet capacity` before dispatch when capacity may have changed.
   `--host target-a` avoids unrelated offline or claimed hosts.
   Check host `limit` and per-repository `repository_limit`. Readiness and
   capacity do not prove Hub model admission.
3. Resolve the repository, allowlisted host, immutable 40-character source
   revision, exact configured project/workspace identity, title,
   logical Fleet Hub `seat`, and protected prompt input.
   Never provide a provider/model override or trust a T3 project default as
   authority. Respect configured capacity.
   Writable work runs in the target's isolated worktree, never its canonical
   checkout.
   On an enrolled controller, prefer `--host auto` and omit `seat`: the Fleet
   Hub selects the machine and the seat. Naming an explicit host or an explicit
   `--seat` is an override and needs `--override-reason "<why>"`. The request
   still goes through the same central route, which refuses a machine or seat
   the caller did not name. `--workload <kind>` states what kind of work this
   is as input to that central route. It is not an override and needs no
   reason, and omitting it inherits the controller's configured workload. See
   [references/contract.md](references/contract.md) for the enrollment and
   reservation limits.
   To run work a parent Brigade run already routed and reserved, pass
   `t3-fleet submit --delegation-id <id>` with only a title and a prompt. The
   placement, repository, seat, workload, and immutable source revision all come
   from the delegation receipt. Supplied options may only confirm it. Adoption
   needs `[policy] enrollment = "required"`, and one delegation is one
   execution.
4. In peer mode, omit `request_id` from `submit`. The local client generates
   the qualified request ID. Treat that returned qualified ID as the
   cross-controller handle for later commands.
5. Put prompt text on standard input or in a protected prompt file. Never put
   prompt text, credentials, endpoints, or executable paths on the command
   line.
6. For `submit` and `followup`, use `approval-required`. Keep runtime
   permissions within the reviewed task. Admission never replaces approval.
7. Require operator authorization for `submit`, using existing current-session
   authorization for the exact work. Ask only for an unauthorized action or a
   real boundary such as destructive work, new dependencies, production impact,
   spending, external publication, or material scope expansion.
8. When the work is fine to run later and the controller has the local queue
   turned on, add `--allow-queue`. A submit that then meets a capacity or
   offline block is stored locally and answers `"state": "pending_autotick"`
   with a `job_id`. A later `t3-fleet tick` launches the same work with no
   second submit and no second approval. Absent, or with `"allow_queue": false`,
   a block fails the submit exactly as before. Never combine it with
   `--delegation-id`: an adopted delegation is one execution that already
   exists centrally.

Read [references/contract.md](references/contract.md) before constructing an
exact request or handling a non-terminal result.

## Monitor and control

- Use `t3-fleet status` for current evidence. Use `t3-fleet wait` when bounded
  polling is useful.
- Claim completion only after a fresh status result says `succeeded`. Report
  `failed`, `interrupted`, or `needs_attention` as observed.
- Use `approve` only for an approval request and `answer` only for a user-input
  request from the current snapshot, using its `pending_request` ID and
  `respond_with`. Use current-session authorization when it covers the exact
  response. For `approve`, authorization must name the specific action being
  approved. Ask for missing user input or an unauthorized
  boundary. Do not guess an answer or treat runtime mode as authorization.
- `followup`, `approve`, `answer`, `interrupt`, and `recover` require operator
  authorization, which may already exist for that exact action. Admission and
  tool-runner approval gates still apply.
- On an enrolled fleet, `followup`, `approve`, and `answer` are admitted again
  before they resume anything: the target revalidates policy and renews the
  reservation first. If the reservation was released, expired, or was never
  recorded, the action fails closed and the work needs a newly negotiated
  reservation rather than a resumed one. Treat that as a real refusal, not a
  transient error to retry.
- A terminal delegated result may add `disposition: "pending-handoff"` and a
  `handoff` object beside its real `state`. That means the target still holds a
  writable worktree awaiting review. Nothing was applied to any source checkout
  and `worktree_ref` is an opaque target-owned reference, not a path. Report it
  as work pending handoff, never as work already merged or applied.
- Use `t3-fleet recover` when state is `unknown`. `recover --target
  <allowlisted-target-id>` without a request ID resolves only when this
  controller journal has exactly one unknown request for that target. Zero or
  multiple matches fail closed. A supplied target is always a lookup hint. It
  does not direct execution or override an explicit request ID. Never resubmit
  the original request to guess whether the first dispatch ran.
- A terminal projection without matching request, thread, and turn identity
  remains unresolved. Doctor and capacity show `resolution.reason` and its
  deadline. At a blocking timeout the host stays blocked until
  `t3-fleet recover --request-id <id>` confirms containment or matching identity
  evidence arrives. Only explicit operator recovery requests containment.
  Passive status, observation, doctor, capacity, and settle never interrupt,
  cancel, or stop target work. A starting or running session retains capacity
  and clears any terminal-resolution clock. Queued work starts launch grace
  only after the previous run has a terminal projection. Uncorroborated terminal
  evidence remains unresolved after grace, with a bounded resolution deadline.

## Draining the queue

- `t3-fleet tick` takes no arguments. It drains a bounded slice of the local
  queue and renews the reservations this machine holds as the target that runs
  them, so long work does not depend on an origin CLI staying in `wait`.
- One tick owns the drain at a time. An overlapping one reports `"state":
  "busy"` instead of racing it. Each job is revalidated against live policy,
  roster, privacy, and capacity, and a retry re-routes rather than replaying the
  machine that already could not run it.
- Read `last_tick_at` to tell a queue that is pending an autotick from one a
  tick has actually drained. Read `queue_disposition` for why a job is where it
  is. `held-acceptance-unknown` means a target may already have the work, so it
  is never retried and its capacity is never freed.
- An expired job means this client stopped holding the prompt. It is never
  evidence that an execution stopped.
- A scoped external timer may call this command periodically. Do not create a
  daemon, service, or cron entry for it.
- `t3-fleet settle-sweep` (settle threads of long-finished fleet requests) and
  `t3-fleet install-settle-timer` are operator commands. JSON `invoke` rejects
  them. Do not run or install them unless the operator asks.

## Boundaries

- Do not push, merge, open a pull request, or delete a worktree unless the task
  separately authorizes that action.
- Do not infer success from transport success, elapsed time, or a task's last
  message. Require status evidence.
- Do not expose host addresses, private paths, auth material, or prompt text in
  logs, summaries, or receipts. A queued prompt stays in the controller's own
  owner-only outbox file. It never reaches the Fleet Hub, a status payload, a
  receipt, an error, or a tick result.
- Do not queue work to consume quota. Only an explicit submission a caller
  already made and already authorized may be deferred. There is no backlog,
  issue, or deck intake.
- More controllers do not increase provider quota.
