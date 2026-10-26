# Local T3 lane client

The user approved all four capabilities on 2026-10-25: build-result delivery,
prerequisite updates, context preparation before compaction, and attention summaries.
Implement a local client in this existing Python package, with no dependencies,
daemon, timers, frontend changes, or runtime installation during development.

## Boundary and defaults

Add `t3-lane`, a separate operator CLI for local app threads. It never launches
threads, changes provider/model/permission settings, executes builds, or approves
requests. Fleet-managed threads must use the existing admitted fleet continuation
route and are refused for lane mutations. Resolve the configured local host using
the existing fleet config, not a caller-supplied URL or executable. No SSH.
Check the configured target.db read-only for matching thread receipts before any
mutation. Fail closed on unreadable/malformed ownership state. An absent target.db
means there are no registered target receipts. Do not create or alter that database.

Bind a thread explicitly to its environment ID, project ID and workspace path.
Check live identity and workspace on every mutation. The client has its own
owner-only SQLite state in a caller-selected state directory. It contains only
bindings, event digests, delivery receipts and prepared command IDs, never tokens,
raw logs or full conversation text. All text input comes through bounded JSON stdin.

Caller examples (CLI names and shape are fixed, JSON fields documented below):

```text
t3-lane --config fleet.toml --host local --state-dir private-state bind < binding.json
t3-lane --config fleet.toml --host local --state-dir private-state deliver < event.json
t3-lane --config fleet.toml --host local --state-dir private-state context < context.json
t3-lane --config fleet.toml --host local --state-dir private-state compact < context.json
t3-lane --config fleet.toml --host local --state-dir private-state attention
```

`bind`: thread_id, project_id, workspace. Capture environment ID live. No lane write.
`deliver`: event_id, thread_id, run_id, source_revision (40 hex), kind
(`build_result` or `prerequisite`), summary (max 4000 chars), optional exit_code,
failed_checks (max 32 names), evidence_path (reference only), mode (`queue` default,
`steer` explicit). The event belongs to the original run and revision. Compare
workspace HEAD and latest run before delivery. Mismatches return `stale` without
delivery. Do not treat a matching SHA as proof of clean-worktree verification.
Clearly label event text as external evidence, not instructions or new authority.

An event ID is scoped by environment/thread. Identical retries return the prior
receipt without another send. Different payload with same ID is a conflict.
Reserve before network I/O, persist stable command/message IDs, reject concurrent
duplicate attempts, and retain `unknown` after uncertain acceptance. No automatic
retry of an uncertain mutation. Print accepted sequence and IDs, not a claim that
the agent consumed the event or that a build passed. Queue uses the verified v2
`message.dispatch` with `dispatchMode.type=queue_after_active`; steer uses
`steer_active` and the exact targetRunId. Never restart or silently fall back.

`context`: thread_id, objective, decisions (list), verification (list of evidence
references), next_action, output_path. Write an exclusive owner-only Markdown
packet containing supplied facts, live thread/run IDs, workspace HEAD and dirty
file names. Do not copy raw history, diffs or logs. Do not overwrite an existing
packet. Label supplied fields as caller-provided, not inferred from the thread.

`compact`: prepares that packet first, then queues the exact `/compact` message
with no attachments on Codex only, through the existing v2 message dispatch seam.
The installed T3 binary contains `/compact` handling and Codex
`thread/compact/start`; verify the adapter path before implementing the trigger.
Reject other drivers and unsupported server versions, while retaining the packet.
Also queue a bounded handoff reference before `/compact`, so the prepared context
is represented in the conversation, not only on disk. Persist operation identity
for both messages and report partial/unknown delivery honestly. Do not compact the
parent or another active human lane in testing. Preparation is useful even when
compaction cannot be requested. Accepted compaction is not completed compaction.

`attention`: read-only, scoped to explicitly bound lanes. Summarize pending
questions/permissions (without answering), failed runs, unknown deliveries and
terminal results that need review. Never mark results as acknowledged merely by
listing them. Bound output; do not print full prompts, tokens, private log text.
Report unavailable lanes separately rather than presenting them as healthy.

## Verification and review

Use test-first standard-library unittest. Test actual SQLite persistence and
concurrency, malformed inputs, oversized input, stale run/SHA, wrong environment,
workspace drift, fleet ownership refusal, queue versus steer, uncertain acceptance,
exclusive packet writing, unsupported compaction and scoped attention. Reuse
T3Client/T3SessionIssuer, but expose narrow public methods for verified v2 commands
instead of letting callers send arbitrary commands. Keep existing dispatch intact.
Run all repo checks through Brigade. Review the complete diff and security boundary.
No deployment or recurring automation is included. A disposable app lane is
authorized for live delivery verification after unit checks pass.
