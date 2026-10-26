# Local lane client

`t3-lane` delivers external evidence to explicitly bound local T3 app threads.
It also prepares context packets, requests Codex compaction, and lists attention
items. Fleet-managed threads require the admitted `t3-fleet` continuation route.

The client uses the existing fleet configuration and target-local session issuer.
It requires exactly one enabled `transport = "local"` host whose
`expected_hostname` matches this machine's hostname without case sensitivity.
The `--host` value must select that host. Configure an explicit `agent_state_dir`.
URLs and executables come from that configuration. The client
does not launch threads, change models or modes,
answer requests, run builds, or use SSH.

## Commands

```sh
t3-lane --config fleet.toml --host local --state-dir private-state bind < binding.json
t3-lane --config fleet.toml --host local --state-dir private-state deliver < event.json
t3-lane --config fleet.toml --host local --state-dir private-state context < context.json
t3-lane --config fleet.toml --host local --state-dir private-state compact < context.json
t3-lane --config fleet.toml --host local --state-dir private-state attention
```

Each write command reads one JSON object from stdin, bounded to 65,536 bytes.
Unknown fields and trailing JSON values are errors. `attention` reads no stdin.
Results are one JSON object on stdout. The package also supports
`python -m t3_fleet.lane` with the same arguments.

## Bind

```json
{"thread_id":"thread-1","project_id":"project-1","workspace":"/srv/repos/example"}
```

All fields are required. The workspace must be an existing absolute directory
matching the live thread worktree, or the project's workspace root when the
thread has no worktree. The client captures the live environment ID and refuses
rebinding the same host/thread to another identity. A successful result contains
`status: "bound"`, `thread_id`, `environment_id`, `project_id`, and `workspace`.
There are at most 128 bindings and 10,000 durable operations per state database.
At capacity, an existing identical replay still returns its receipt, while a
new operation is refused. Unknown and partial rows are never pruned.

Before binding, packet creation, or message delivery, the client reads the
configured agent's `target.db` with SQLite read-only access. A matching receipt
refuses the operation, including a terminal fleet receipt. Missing `target.db`
means no registered receipts. Unreadable or malformed ownership state refuses
work. The client also refuses work while any nonterminal target claim has a
NULL `thread_id`, because attachment may still be in progress. It validates the
`state` and `thread_id` schema and stored types, and refuses unknown states.
The known target states are `preflight`, `dispatching`, `running`,
`needs_attention`, `unknown`, `succeeded`, `failed`, and `interrupted`.
Only the last three are terminal.
The client never initializes or migrates that database. The local host must
configure `agent_state_dir` explicitly. No platform default or environment
variable supplies a fallback. Ownership reads use bounded schema results and
existence queries without a whole-database integrity scan.

## Deliver

```json
{
  "event_id":"build-1",
  "thread_id":"thread-1",
  "run_id":"run-1",
  "source_revision":"0123456789abcdef0123456789abcdef01234567",
  "kind":"build_result",
  "summary":"Unit checks failed. Review the evidence reference.",
  "exit_code":1,
  "failed_checks":["unit"],
  "evidence_path":"evidence/result.json",
  "mode":"queue"
}
```

Required fields are `event_id`, `thread_id`, `run_id`, `source_revision`, `kind`,
and `summary`. `kind` is `build_result` or `prerequisite`. The revision is exactly
40 hexadecimal characters and is normalized to lowercase. `summary` permits up
to 4,000 characters. Optional `exit_code` is a signed 32-bit integer.
`failed_checks` defaults to an empty list and permits 32 strings of up to 1,000
characters each. `evidence_path` is a reference of up to 4,096 characters. The
client does not read that reference. IDs permit up to 512 characters without
control characters. All text fields reject empty values and NUL characters.
The total stdin size bound still applies.

`mode` defaults to `queue`. Queue dispatch uses the v2 `message.dispatch` command
with `dispatchMode.type = "queue_after_active"`. Explicit `steer` uses
`steer_active` with the exact `targetRunId`. There is no restart fallback.
The existing transport's negotiated v2 version contract applies.

Every mutation checks the bound environment, project, and resolved workspace.
Delivery also compares the latest run and workspace HEAD to the event, including
a fresh check after reserving its identity. Mismatches return `stale` and send
nothing. These are observations before dispatch, not an atomic lock on Git or
T3. The revision is caller-asserted evidence. These checks cannot establish a
historical run-to-revision association. A matching HEAD does not prove that
verification covered a clean worktree.
Delivered text labels all event fields as caller-provided external evidence,
not instructions or new authority.

Event identity is scoped by environment and thread. The client reserves the
payload digest and stable command/message IDs in SQLite before network I/O.
Identical retries return the stored receipt. Different content for the same
ID returns `idempotency_conflict`. Concurrent duplicates cannot send again.
Lost replies, malformed acknowledgements, and interrupted processes retain
`unknown`. There is no automatic retry or takeover of an unknown operation.
Inspect the lane before deciding what to do with an uncertain delivery. Changing
an event ID can send a duplicate and does not resolve the uncertainty.

An accepted result contains `status: "accepted"`, `environment_id`, `thread_id`,
`command_id`, `message_id`, and `sequence`. It confirms server acceptance only.
It does not confirm agent consumption, run completion, or build success. `stale`
and `unknown` retain the reserved IDs without an accepted sequence.

## Context and compaction

Both commands take the same required fields:

```json
{
  "thread_id":"thread-1",
  "objective":"Finish the current change",
  "decisions":["Keep the existing API"],
  "verification":["evidence/result.json"],
  "next_action":"Review the failing unit check",
  "output_path":"/private/lane/context.md"
}
```

`objective` and `next_action` permit 4,000 characters each. `decisions` and
`verification` each permit 32 strings of up to 1,000 characters. Verification
entries are caller-provided references. `output_path` permits 4,096 characters,
must be absolute, and must have an existing owner-only parent directory.
Symlink path components and existing output files are refused. The client
creates the packet exclusively with mode `0600` and flushes its contents.
It does not overwrite a packet.

The Markdown packet labels supplied facts as caller-provided and records live
environment/thread/project/run identity, resolved workspace, HEAD, and dirty
file names. It copies no conversation history, file contents, diffs, or logs.
At most 256 dirty names and 65,536 bytes of Git status output are accepted.
Git reads disable optional locks, configured fsmonitor programs and hooks, and
remove inherited `GIT_*` overrides before inspecting this bound workspace.
`context` returns `prepared`, the output path, identity, run ID, and HEAD.
It sends no lane message.

`compact` first prepares that packet, then queues a bounded handoff reference
and the exact `/compact` text with no attachments. Both use the narrow v2 queue
method. The installed build's root-run slash handler selects `compactThread`
for that exact message, and the Codex adapter calls `thread/compact/start`.
The trigger is restricted to Codex identified by the bound root provider-thread
record, and to these exact inspected server builds with protocol 2:

- `0.0.46-nightly.20261007.2787`
- `0.1.1-nightly.20261017.3238`

The latter artifact's SHA256 is
`1834effecd2d7255d89849ed97525156cb83b9568c45aa3a4e2ae501476502fe`.
Its embedded dispatch schema supports `queue_after_active` and `steer_active`
with `targetRunId`. Inspection confirmed the exact slash-command path and Codex
adapter call described above. Other drivers or builds return `unsupported` after
preparation when the live identity can be read. Unsupported read protocols cannot prepare a
verified packet. Future builds need adapter verification before admission.

Compaction identity is the output path scoped by environment/thread. Changing
facts at that path conflicts. Identical retries return the stored operation
without overwriting the packet or sending messages again. Each of the two
messages has persisted `command_id`, `message_id`, and delivery status. An
accepted message also has `sequence`. The first message may advance the latest
run, so the second checks binding identity without requiring the old run ID.
Immediately before each message, the client rechecks the server build, protocol,
and current root Codex provider record against the thread's model selection.
The model selection and active provider-thread ID must also match the initial
operation snapshot. Provider or model drift refuses that message. If the reference was
accepted, the result is `partial` with the second message `refused` and no
`/compact` dispatch. Both message identities and the first acknowledgement remain
in the durable receipt.

The operation result includes `packet_prepared`, `output_path` after successful
preparation, and a `messages` array in reference/compaction order:

| Status | Meaning |
| --- | --- |
| `accepted` | Both messages were accepted. Compaction completion is unverified. |
| `unsupported` | Packet prepared, trigger refused, no messages sent. |
| `partial` | Reference accepted, second message uncertain or refused. |
| `unknown` | Acceptance or an interrupted operation remains uncertain. |

Individual message states are `not_sent`, `unknown`, `accepted`, or `refused`.
The client stops after the first uncertain message. A crash during preparation
can leave `packet_prepared: false` even if a file exists. That value means
preparation was not confirmed in the receipt. No retry sends automatically.
A preparation error returns an error envelope and leaves the reserved operation
held. Keep any packet and inspect the recorded state before further action.

## Attention and private state

`attention` lists only explicitly bound lanes. `lanes` contains thread/run IDs,
run status, pending request IDs/kinds, uncertain delivery IDs/statuses, and
`needs_review`. Completed, failed, interrupted, cancelled, or rolled-back latest
runs require review. Listing does not acknowledge a result or answer a request.
No prompts, assistant text, tokens, or raw logs appear. Failed identity or
ownership reads appear in `unavailable` and are never reported as healthy lanes.

Each lane lists at most 32 pending requests and 8 uncertain deliveries.
`pending_truncated` and `unknown_truncated` report omitted entries. Output is
bounded to 65,536 bytes, with `omitted_lanes` and `omitted_unavailable` counts
when whole entries must be omitted. A truncated result is incomplete.

The caller-selected state directory is created with mode `0700` when absent.
An existing shared directory is refused. `lane.db` must be an owner-only regular
file with one hard link. The database contains bindings, digests, command IDs,
and receipts, including prepared output references. It stores no credentials,
event summaries, full conversation text, or raw logs. Keep this directory private
and persistent across retries. Deleting it removes duplicate suppression.

Exit 0 means `bound`, `prepared`, `accepted`, or a completed attention listing.
Exit 3 covers `stale`, `unsupported`, `partial`, and `unknown` operation results.
Error envelopes contain `status: "error"` and a stable `code`. Validation/config
errors use exit 2, identity conflicts use exit 4, and transport errors use the
existing fleet error codes. Private exception details are omitted.

Known local guard errors also include an optional fixed `reason`:

| Reason | Action |
| --- | --- |
| `local_host_required` | Select an enabled local host. |
| `agent_state_dir_required` | Configure the local host's `agent_state_dir`. |
| `binding_required` | Bind the intended thread before using it. |
| `binding_changed` | Inspect the live identity and workspace. |
| `ownership_unavailable` | Check access to the configured agent state. |
| `ownership_malformed` | Inspect the target ownership schema and records. |
| `fleet_ownership_refused` | Use the admitted fleet continuation route. |
| `packet_path_unavailable` | Select an unused path in an existing private directory. |

Reasons never contain supplied input, transport exception text, tokens, or URLs.
Other errors retain their stable code without a reason.
