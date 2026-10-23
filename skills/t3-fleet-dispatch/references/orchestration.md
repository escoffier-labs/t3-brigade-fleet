# Attached T3 orchestration and readiness

## Discover and choose a route

Call `orchestrator_capabilities` for the live provider instances, models,
model options, inherited settings, constraints, and feature flags. Select real
`providerInstanceId`, model IDs, and option IDs/values from that response.
Check `canRunChildTask` and cross-provider constraints before delegation.
Respect the returned `maxBatchThreads` rather than a remembered limit.

A missing tool-catalog entry alone does not establish unavailability. Optional
servers can attach lazily. Make one bounded direct call with the known name,
using the harness spelling. Claude Code exposes `mcp__t3-code__...`.
Other harnesses may use `mcp__t3_code__...` [unverified]. Verify the spelling
in the current harness before concluding the tools are unavailable.
A worker's absent catalog is not evidence that its parent's tools
are unavailable.

If tools remain absent and `T3_ACP_MCP_NODE` is exposed, use the host-documented
ACP fallback:

```sh
ELECTRON_RUN_AS_NODE=1 "$T3_ACP_MCP_NODE" ${T3_ACP_MCP_ENTRYPOINT:+"$T3_ACP_MCP_ENTRYPOINT"} acp-mcp-call orchestrator_capabilities '{}'
```

`T3_ACP_MCP_ENTRYPOINT` is unset for a standalone executable. For delegation,
use the same transport with `acp-mcp-call delegate_task` and one safely quoted,
serialized JSON object containing `task`, catalog-selected
`target: {providerInstanceId, model}`, `mode: "async"`, and a stable
`clientRequestId`. Resolve placeholders before use and shell-quote JSON as a
single argument. This is supported T3 transport, not an ordinary shell worker
substitute. It remains gated by live capabilities and tool availability.
If neither route is exposed, report the blocker. Do not guess flags, private
endpoints, or credentials. This documented shape is not live execution proof.

Prefer native subagents for same-provider work only when the native tool
supports the selected live model. Use native handles and lifecycle tools.
Choose `delegate_task` for a selected model native tools cannot run, a
cross-provider child, or explicitly T3-owned children. `create_threads` and
`t3_thread_launch` create ordinary top-level conversations, not child tasks.
Brigade-wired substantial implementation must still use local `brigade run`
policy. A callable app capability does not replace admission or privacy rules.

## Dispatch a child with a usable contract

T3 children run only the supplied `task` prompt, without parent history. Include
the goal, relevant facts and paths, constraints, exact file ownership, other
writers' boundaries, expected artifact or answer, verification command, and
terminal state. Tell writers to preserve others' edits. Children inherit the
caller's checkout, so assign disjoint files or serialize overlapping writes.
Changing directories in a prompt does not change a thread's workspace binding.

Example task prompt, with placeholders resolved before dispatch:

```text
Implement <bounded change> in <owned files> only. Other agents own <other files>.
Preserve unrelated changes. Context: <facts and source locations>.
Do not commit, publish, install dependencies, or expand the scope.
Run <required check>. Return changed files, exact output, and unresolved issues.
Stop after <terminal state>.
```

Use `target` only for deliberate catalog-backed overrides. Omitted provider,
model, options, and modes inherit. Runtime permission modes and plan/default
interaction modes are separate settings. `delegate_task` accepts `role`:
`implementation`, `research`, `review`, `design`, `test`, or `general`. Use `runtimeMode: "approval-required"` when supported. Keep a person available
for permission requests and preserve the caller's permission ceiling. If an
approval cannot be answered through the available tools, report the blocker
instead of widening permissions.
Use a stable `clientRequestId` for the same logical operation wherever the live
schema supports it, including delegation. Prefer `mode: "async"` for long work.

Retain `taskId` for `task_status` and `task_cancel`. The returned `childThreadId`
is backing storage, and `childRunId` identifies the original delegated run.
Async completion notifies and wakes the parent, steering active turns where
supported or queuing otherwise. Continue independent work, then end the turn
when solely waiting. Do not busy-poll or spawn watcher agents.

Each review round needs a new `delegate_task` with the original brief, prior
findings, responses, and unresolved objections. Give each round a distinct
`clientRequestId`, stable across retries of that round, and retain its `taskId`.
Sending to `childThreadId` with `t3_thread_send` does not reopen a completed task.

Read `task_status` mid-turn when its result is needed. Interpret the whole
result rather than `status` alone:

- `workState: "working"`: work is in progress.
- `workState: "waiting_for_children"`: a completed turn still has nested work.
- `workState: "result_available"`: inspect the published result and status,
  including failures, before reporting its outcome.

`summary` and `resultContextTransferId` describe the stable published task
result. `hasPendingChildRuns` reports later queued/executing turns, while
`latestTerminalRunId`, `latestTerminalStatus`, `latestTerminalSummary`, and
`latestTerminalResultContextTransferId` describe later non-monitor results.
Do not replace the original result with an unrelated later turn or declare
later work finished while pending runs remain. Reading a terminal task result
acknowledges automatic parent delivery. Retain transfer IDs to correlate context status from
`t3_thread_transfers`, which takes a thread ID, not a transfer ID. A context
transfer is not evidence of a Git merge.

A child's blocking question wakes the parent and ends a blocking wait with
`hasPendingQuestion: true` on T3 nightly 20261015.3145 or later. This is not on
the current 0.0.46 fleet hosts. Use
`t3_pending_request_list`, `t3_pending_request_read` (with `requestId`), and
`t3_pending_request_respond` with `threadId=childThreadId`. Answer from available
input and authorization, or ask the user. These question tools cannot approve
a permission request. Keep the task handle for completion.

For `mode: "wait"`, `timeoutMs` is the parent's wait budget. `waitTimedOut: true`
does not cancel the child. Keep the task handle and inspect later status, whose
`waitTimedOut` is false and whose status/work state determines liveness.
`task_cancel` requests interruption and disposes automatic parent delivery.
Completed results remain readable. Cancellation acceptance is not proof that
execution stopped. See [supervision.md](supervision.md) for thread-level control.

### Native background subagents on newer builds

T3 nightly 20261017.3238 includes background-tool and restart-recovery fixes
([tool access](https://github.com/pingdotgg/t3code/pull/19751),
[restart recovery](https://github.com/pingdotgg/t3code/pull/19750)). A running
native background subagent owned by the caller's provider instance can keep
caller tools available after the parent turn ends. This does not apply after
Stop, archival, or provider credential revocation. `delegate_task`,
`create_threads`, and `request_secret` still require an active run.

When continue-after-restart is enabled, recovery can name interrupted native
subagents and wake their parent. Read the recovery note and resume or restart
only work still needed, checking for side effects before replay. This is not
proof that interrupted work completed. Upstream still reports gaps for some
desktop-update restarts and nested Codex children whose intermediate parent
already finished. A subagent resumed by its own background task can also lose
caller tools until a wake run starts. These changes were not exercised here.

## Requested separate sessions and workspace binding

Launch a top-level thread only when the user explicitly requests separate,
new, or top-level threads or sessions. Independent work or a PR stack alone
does not authorize a new conversation. Once a separate session is authorized,
choose its worktree for independent implementation or a PR stack. Use
`t3_thread_launch` for one thread and bind its workspace before the agent starts.
The task goes in `message`. Omitted project/model/modes inherit, but omitted
`workspaceStrategy` selects the project root, not the caller's worktree.
The new thread's runtime and interaction
modes are capped at the caller's. A caller outside a T3 thread must pass
`projectId` or top-level `scratch: true`. `scratch` cannot be combined with
`projectId` or `workspaceStrategy`.

- New worktree: `{type: "worktree", baseRef: "<verified-local-branch>",
  branch: "<new-branch>", startFromOrigin: false}` includes committed local work.
  Resolve the intended base first. Use `startFromOrigin: true` for an intended
  upstream base, within the task's authorization.
- Existing checkout: `{type: "existing_worktree", worktreePath:
  "<verified-absolute-checkout-path>", branch: "<existing-branch>"}`.
- Project root: choose `{type: "root"}` deliberately.
- Scratch folder: top-level `scratch: true` creates a fresh folder outside a
  repository and must fit the requested task.

Dirty edits are not copied into a new worktree. Bind the existing checkout if
those edits are required, or resolve an authorized transfer before launch.
Use `t3_worktree_status` for the caller's binding and `t3_worktree_list` for
branch-backed checkout inventory. Detached worktrees are omitted from that list.
Do not ask a launched agent to create its own worktree through shell commands.

`create_threads` is a requested batch sharing the caller's project, branch,
and checkout. Each entry may override catalog-backed selection/modes, but there
is no per-thread workspace override. Use its stable `clientRequestId` on retry.
Retain each returned `threadId` and `runId`. A launch may still be preparing.
`t3_thread_launch` has no retry key: inspect `t3_thread_list` after an error or
lost response before retrying, rather than creating duplicate work.

`t3_worktree_handoff` moves the current thread into a new worktree. Check its
binding first. It detaches the live provider session and ends the current turn,
so call it as the last action with `continuationPrompt` containing remaining
work. Conversation is preserved for the next turn. Without continuation the
thread stays idle. Review the setup script before accepting its default
execution, or use `runSetupScript: false` when appropriate. Deleting a thread
does not remove its worktree.

Fork with `t3_thread_fork` from `latest_stable`, a run, or a checkpoint when the
requested workflow needs a conversation branch. It inherits configuration.
`t3_thread_merge_back` and `t3_thread_transfers` concern context and lineage,
not Git merges or file integration. Inspect transfer status and independently
verify code integration before claiming changes were applied.

## PR association

Use `link_pull_request` immediately for every PR created, and link every PR
worked on, including every layer in a stack. Inspect
`list_thread_pull_requests` to verify all associations and stack order.
`t3_thread_update` can also link/unlink metadata for a scoped thread.
`unlink_pull_request` repairs an incorrect association. Linking an existing PR
is bookkeeping and grants no authority to open, publish, push, or merge it.

### PR watching

Use `watch_pull_request` instead of watcher agents. Unsettle the thread before
starting a new watch. T3 checks every two minutes and wakes the thread on a
failed check, required checks passing, another
person's comment or review, or a merge conflict. Only comments after the watch
call wake it. Handle existing comments first, then end your turn. A wake is
news, not a merge decision. The thread stays Working while watched.
Call `unwatch_pull_request` before handing back. A subagent
cannot watch. Watches also end on merge/close, settle/archive, eight consecutive
read failures, user Stop, or `task_cancel`.

## Outside agents (OAuth MCP clients)

Source: from v0.1.0 tool access declarations. The OAuth flow was not exercised.

Use the environment's `/mcp` URL from Settings, Connections, Copy MCP URL and
sign in with OAuth. Approval uses a pairing code (`t3 auth pairing create` on
the host) or a browser session with `access:write`. Read only is the default
MCP ceiling.
Supervised through Full access permits writes within the approved ceiling.
Sign-in lasts 30 days and can be revoked under Settings, Connections. Hosted
clients need a reachable T3 Connect address. A Tailscale address works only
inside the tailnet. Clients without OAuth support cannot connect.

On nightly 20261017.3238, new standard pairing grants also include
`access:read`, `access:write`, and `relay:write`
([upstream change](https://github.com/pingdotgg/t3code/pull/20432)). Existing
sessions retain their scopes until paired again. Explicit `--scope` selections
replace the default grant. Inspect the selected build and grant before pairing.
These environment scopes are separate from the outside client's MCP ceiling.

| Access | Available tools |
| --- | --- |
| Reads | `orchestrator_capabilities`, `t3_thread_list`, `t3_thread_read`, `t3_thread_search`, `t3_thread_wait`, `t3_queue_list`, `t3_queue_read`, `t3_pending_request_list`, `t3_pending_request_read`, `t3_thread_configuration`, `t3_thread_transfers`, `t3_project_list`, `t3_project_read`, `t3_worktree_list`, `t3_environment_read`, `list_scheduled_tasks`, `list_thread_pull_requests` |
| Writes within the client's ceiling | `t3_thread_launch`, `t3_thread_send`, `t3_thread_interrupt`, `t3_thread_update`, `t3_thread_fork`, `t3_thread_merge_back`, `t3_queue_edit`, `t3_queue_cancel`, `t3_queue_reorder`, `t3_queue_promote_to_steer`, `t3_pending_request_respond`, `link_pull_request`, `unlink_pull_request`, `watch_pull_request`, `unwatch_pull_request`, `schedule_task`, `update_scheduled_task`, `delete_scheduled_task`, attachment upload tools |
| Unavailable without a live T3 thread | `delegate_task`, `task_status`, `task_cancel`, `create_threads`, `request_secret`, `preview_*`, `device_*`, `t3_worktree_handoff`, `t3_worktree_status`, `html_*` |

Delegation also requires an active parent run owned by the credential's provider
session. Outside clients use `t3_thread_launch` with explicit `projectId` or
top-level `scratch: true`, then `t3_thread_send`, `t3_thread_wait`, and
`t3_thread_read`. New or steered threads
cannot exceed the client's approved runtime and interaction modes. A read-only
client receives `capability_denied` for writes.

Outside clients can create webhook schedules but cannot call `request_secret`.
For signing, have the operator enter the secret in the T3 UI [unverified].
Never put a secret value in `signature.secret` or chat. Use URL-token
authentication without a signature only when that meets the authorized task.
`webhookUrl` requires a
T3 Connect managed tunnel. Outside-client webhook binding is [unverified]:
without a calling thread, each run is expected to launch a fresh thread unless
bound elsewhere. Verify binding before relying on thread-local deduplication.
See [supervision.md](supervision.md) for webhook placeholders and delivery age.

## Scoped readiness and compatibility

These tools act on the attached environment. Cross-host work uses the local
`t3-fleet` client and [contract.md](contract.md). Native handles, app `taskId`,
thread/run pairs, qualified fleet request IDs, and Brigade delegation IDs are
separate identities. Adopt a parent's Brigade receipt with
`submit --delegation-id` rather than allocating a second decision/reservation.

Verify installed client flags with help. Scope authorized diagnostics:

```sh
t3-fleet doctor --host target-a --repository example-repo
t3-fleet capacity --host target-a
```

Respect live claims. An aggregate `blocked_offline` may concern another host.
Check transport, authenticated protocol compatibility, and exact project mapping
separately. Preserve the request handle after uncertain acceptance and recover
instead of replaying the prompt. A desktop version is not a server descriptor.
Protocol support requires the actual adapter contract, not a version allowlist.

Discovery, readiness, and prose coverage do not prove provider execution.
For an authorized bounded live canary, record source revision, exact workspace,
selected seat/model from the Hub or live catalog, execution handle, and fresh
terminal evidence. Use the correct lifecycle surface and inspect the result.
Independently passed remote canaries do not prove new app operations in this
skill. Use `approval-required` for fleet canaries and do not widen app modes.
