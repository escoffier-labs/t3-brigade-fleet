# Supervision and recurring work

Before using tool names below, read [tool compatibility](tool-compatibility.md).
T3 v0.1.3 consolidates several tools. Use the attached catalog and the matching
argument schema. Earlier names in this reference apply only to older catalogs.

## Read results with their scope

`t3_thread_list` takes `projectId` (default the caller's), `includeSubagents`,
`snoozed`, `statuses`, `titleContains`, and `settled`. It returns newest-first
cursor pagination. On builds that advertise `allProjects`, pass
`allProjects: true` to list across projects, including `snoozed: true` for snoozed
threads. The result's top-level `projectId` is null and each item carries its
own `projectId`. Without that flag, the project default still applies. This is
included in v0.1.4 ([upstream change](https://github.com/pingdotgg/t3code/pull/26442)).

`t3_thread_search` takes `projectId` and is bounded and
non-paginated over active titles/content. Inside a thread, the default is the
calling thread's project. Outside a T3 thread, omitting `projectId` searches
every project. Project-scoped results are limited to one project out of the
global top matches. Search is not exhaustive, so fewer matches
or no match does not prove absence. `t3_thread_read` reads any thread in the
environment. Use `view: "messages"` for messages/plans and `"activity"` for the
summarized timeline.

Follow `afterPosition: nextPosition` while `hasMore` is true. For truncated
items, use `itemId` and `textOffset: nextTextOffset` until that offset is null.
Offsets count UTF-16 code units. Inspect durable run status and the complete
result before reporting an outcome. An untruncated terminal assistant result
from a direct app-owned child acknowledges automatic delivery to this parent.

Retain `threadId` plus the intended `runId` for `t3_thread_wait`. Without a run
ID it selects the latest run at call time, and an idle thread returns at once.
A wait reports status only, delivers no result, and does not acknowledge a
child result. `timedOut: true` does not interrupt work. Read the result with
`t3_thread_read` or the delegated `task_status` route in
[orchestration.md](orchestration.md). Wait again only when useful, without
busy-polling or a watcher task.

Nightly 20261107.4314 defaults inactive threads to settled after seven days
([upstream change](https://github.com/pingdotgg/t3code/pull/32325)). When
looking for older work, include settled threads through the live schema
before concluding that it is missing. A settled thread is not proof that
its task completed successfully.

## Control a run or its queue

Use `t3_thread_send` with the delivery intent made explicit:

| Mode | Use |
| --- | --- |
| `auto` | Start idle work, steer a fully active turn, or queue if not yet steerable |
| `steer` | Update an in-flight turn |
| `queue` | Request a separate follow-up turn |
| `restart` | Interrupt and restart an active turn |

When a thread tool returns a ready `link`, use that returned link instead of
constructing one. Newer builds return web URLs for outside OAuth clients and
environment-qualified `t3-thread` links for app clients
([upstream change](https://github.com/pingdotgg/t3code/pull/26642)). Retain returned delivery, message, thread, and run IDs. Use a stable
`clientRequestId` where supported, including send and interrupt, for retries of
the same logical action. `t3_thread_interrupt` with an exact run ID avoids
interrupting a newer turn. Without it the newest interruptible run is chosen.
Terminal runs return without another side effect. Interruptions and steering
within existing authorization need no repeated approval. Check durable status
after requesting interruption.

On nightly 20261030.3917, an unconfirmed Stop can leave the run active and
mark the agent unresponsive. The UI offers Force stop in that state. Do not
report the agent stopped merely because interruption was requested, and do not
invent a force flag for `t3_thread_interrupt`. Inspect the selected build's
supported control surface and preserve the exact thread/run target. See the
[upstream Stop change](https://github.com/pingdotgg/t3code/pull/27252).

Use only arguments in the live `t3_thread_organize` schema. Newer builds
[reject excess arguments](https://github.com/pingdotgg/t3code/pull/26718).
Older builds could ignore an unsupported target such as `threadIds` and archive
the caller instead. Confirm the supported target field before any organization
write. A schema error is a refused action, not evidence the target changed.

Read `t3_queue_list` in delivery order and `t3_queue_read` for message detail.
List pages are live offsets and may shift. Read thread history for full context
or queued text that is truncated. `t3_queue_edit` replaces text and preserves
attachments, rejecting runs no longer queued. Recheck queue state before edit,
`t3_queue_cancel`, `t3_queue_reorder`, or `t3_queue_promote_to_steer` against a
specified active run. Use `beforeRunId: null` to reorder to the end. Promotion follows provider and
run-state rules, and cancellation applies to queued work rather than active
execution. Inspect the resulting queue/delivery instead of assuming acceptance
means the provider acted.

## Questions and permission boundaries

`t3_pending_request_list` and `t3_pending_request_read` expose user questions,
not permission approvals. Answer with `t3_pending_request_respond` only from
available input and authorization. It cannot approve a permission request.
Do not fabricate an approval API or use question tools to widen permissions.
Use an exposed, supported approval surface if present and already authorized,
or report the actual permission boundary. App runtime modes are separate from
operator authorization, fleet admission, and interaction mode.

## Explicitly requested schedules

`schedule_task` creates work that runs even with no active turn. T3 0.1.7
also supports a single run at a specified date and time. Inspect the live
schema for the schedule form and timezone before creating it. Use the form
the user requested. Confirm explicit authorization for recurring work. Snoozing a thread through
`t3_thread_organize` changes its organization and schedules no action.

Pass a structured `schedule` object. Examples, to adapt to the requested
cadence:

```json
{"type": "fixed_time", "timeOfDay": "09:00", "weekdays": [1, 2, 3, 4, 5]}
```

```json
{"type": "interval", "everyMs": 3600000}
```

Fixed time is local wall-clock time. Resolve an ambiguous timezone from the
environment or user before promising the cadence. Do not stringify the object
for ordinary calls. Use a stable `clientRequestId` on schedule creation.
Provider/model/runtime settings inherit. The default
`bindToCurrentThread: true` posts runs in this thread's project into this thread.
Elsewhere each run launches a fresh thread. Choose false only when the user
requests a fresh top-level thread per run. Report
`scheduledTaskId`, returned schedule/cadence, binding, enabled state, and
`nextRunAt` after success.

`list_scheduled_tasks` takes `projectId`, defaults to the calling thread's
project, and lists every project when there is no calling thread. Keep its
`scheduledTaskId` for `update_scheduled_task`, which preserves omitted fields. Pause with
`enabled: false`, update cadence/prompt/binding within authorization, and verify
the returned state or list. Permanent deletion through `delete_scheduled_task`
needs authorization for deletion.

`run_scheduled_task_now` uses `taskId` for the schedule ID. Treat it as an
operator action and verify the live capability gate before attempting it. Each call creates a manual run without a retry key.
Dispatch/bookkeeping completion and `lastRunStatus` are not proof that the
provider turn completed. Inspect the returned thread and durable runs/results.
After uncertain acceptance, inspect schedule and thread state before retrying.

From nightly 20261111.4515, updating a schedule preserves its saved workspace
strategy while its thread binding remains the same. Unbinding a bound schedule
selects a worktree. Check the saved binding and workspace before changing where
future runs execute. See the
[upstream change](https://github.com/pingdotgg/t3code/pull/32494).

### Webhook schedules and private signing secrets

`schedule_task` and `update_scheduled_task` accept a structured webhook schedule:

```json
{"type": "webhook"}
```

Webhook deliveries can be filtered, deduplicated, or coalesced before a run
starts. Inspect the selected build's live schedule schema before using these
options. The prompt sees the request
only through `{{body.path}}`, `{{headers.name}}`, `{{query.name}}`, `{{body}}`,
or `{{request}}`. For example, `{{body.release.tag_name}}` reads a nested body
field. `{{request}}` includes method, credential-redacted headers, and body.
The result includes `webhookUrl` only with a T3 Connect managed tunnel. If it
is absent, ask the user to enable T3 Connect remote access rather than share a
path. Webhook runs bind to the creating thread by default in that thread's
project (`bindToCurrentThread: true`), so the orchestrator can dedupe triggers
against work in flight. Choose false only for requested fresh threads per run.

On builds with [event filters](https://github.com/pingdotgg/t3code/pull/22194),
all `filters` must match. Supported operators are `equals`, `notEquals`, `in`,
`startsWith`, and `exists`, on request paths such as `headers.x-github-event`
and `body.action`. A filtered delivery starts no run. On update, omitting
`filters` preserves them. `filters: []` clears them.

[Replay deduplication and coalescing](https://github.com/pingdotgg/t3code/pull/22234)
use the webhook schedule's `dedupe` object. `replayKey` and `coalesceKey` are
request templates. Replay keys suppress repeated deliveries for 7 days.
Coalescing keeps the latest payload in a fixed window measured from the first
delivery. `coalesceWindowSeconds` accepts 10-3600, default 120. For example:

```json
{"type": "webhook", "dedupe": {"replayKey": "{{headers.delivery-id}}", "coalesceKey": "{{body.id}}", "coalesceWindowSeconds": 120}}
```

Read delivery outcomes, including `filtered`, `duplicate`, and `coalesced`,
when checking whether a request started work. Delivery acceptance alone does
not prove that a provider turn ran. These behaviors are source-verified for
nightly 20261021.3473. They were not exercised against a live webhook here.

`maxDeliveryAgeMinutes` accepts 1-1440 and skips requests held by the relay
longer than that while the environment was offline. Offline holding is opt-in
and also needs relay hold. A webhook arriving during downtime may run late or
be dropped.

For a signing secret, call `request_secret` with `label`, `reason`, and optional
`placeholder`, `timeoutMs`, and `clientRequestId`. It shows a private card and
returns a one-use `secretRef`, never the value. `timeoutMs` accepts
1000-3600000, default 10 minutes. Never ask for secrets in chat and never put a
secret value in `signature.secret`. Pass the returned reference to
`signature.secretRef`, for example this schedule shape after resolving the
reference:

```json
{"type": "webhook", "signature": {"header": "x-hub-signature-256", "encoding": "hex", "prefix": "sha256=", "secretRef": "<returned-secretRef>"}}
```

`request_secret` needs a live T3 thread. For outside clients, use the signing
route in [orchestration.md](orchestration.md).

### Secrets supplied by the user

On nightly 20261104.4179 and newer, use `request_secret` for a login, token,
PIN, or pairing code that only the user can supply. Request one secret per
private card. Use an existing authorized credential store when it already
supplies access. Keep secrets out of chat and tool arguments. If a secret was
already pasted, treat it as exposed and ask for a replacement through a private
card. Use the returned reference when the recipient accepts it. Choose
`delivery: "file"` for a command that consumes the file privately.
See [the upstream change](https://github.com/pingdotgg/t3code/pull/30257).

### CLI credentials on newer T3 builds

From nightly 20261025.3669, `request_secret` also supports `delivery: "file"`
for a credential needed by an authorized shell command. Check the live schema
first. File delivery returns a private file path instead of `secretRef`.
Read it only inside the consuming command, keep the value out of output and
logs, and do not assume command output is redacted. Files expire after 24 hours.
Reuse `clientRequestId` after an uncertain result to recover the same file.
Keep the default reference delivery for tools accepting `secretRef`.
See [the upstream change](https://github.com/pingdotgg/t3code/pull/24809).

### Commands requiring a sudo password

On nightly 20261109.4399 and newer, a sudo password that only the user can
supply belongs in a private `request_secret` card with `delivery: "file"`.
First try the authorized command with `sudo -n`. Request a password only if
sudo explicitly reports that authentication is required. Another nonzero exit
can mean the command already ran. Check its effects before retrying.

Consume the returned file with `sudo -S -p '' <command> < "$file"`. Standard
input now carries the password, so pass any command data as a file argument
rather than piping it into sudo. Keep the value out of chat, arguments, and
logs. Use existing authorized access when available.
See [the upstream change](https://github.com/pingdotgg/t3code/pull/27748).
