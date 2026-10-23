# Recovery

Preserve the exact request handle after uncertain acceptance. Recovery checks
the execution already named by that handle. It never replays its prompt.

## Unknown outcomes

Terminal states are `succeeded`, `failed`, and `interrupted`. `unknown` is
non-terminal. Lost transport, a process interruption, or unmatched T3 evidence
can leave the outcome unknown. Capacity and reservations remain counted until
the target can establish a safe outcome. A timeout alone proves no termination.

Require operator authorization before recovery:

```bash
t3-fleet recover --request-id <qualified-request-id> --include-output
```

Without a request ID, `recover --target target-a` succeeds only when this
controller journal has exactly one unknown request for that target. Zero or
multiple matches fail closed. The target is a lookup hint and cannot redirect
an explicit qualified request ID. Preserve the controller and target portions
of cross-controller handles unchanged.

In target-authoritative mode the target receipt owns the outcome. A second
controller can discover an existing request through its qualified handle.
Missing receipts remain subject to a grace period, and live or ambiguous work
keeps its capacity. Do not treat an immediate `not_found` as proof of no run.
Recovery releases capacity only after the applicable identity and containment
rules establish a safe result.

## Terminal evidence and containment

A terminal projection must match request, thread, and turn identity. Missing or
uncorroborated identity remains unresolved. Doctor and capacity can report a
bounded `resolution` with its reason and deadline. A starting or running session
retains capacity and clears a terminal-resolution clock. Queued work starts
launch grace after the previous run projects terminal.

At a blocking timeout the host remains blocked. Explicit operator `recover`
may request containment of the unresolved execution, or matching identity
may resolve it. Passive status, doctor, capacity, observation, and settlement
never interrupt, cancel, or stop target work. A reservation stays held while
acceptance or execution remains uncertain.

Read fresh status after recovery. Report `unknown` if it remains unresolved.
Do not submit replacement work to guess whether the earlier request ran.

## Queue recovery

The optional local outbox admits only explicit `--allow-queue` submissions on a
controller with `[outbox] enabled = true`. Known pre-launch capacity or offline
blocks can yield `pending_autotick` with a `job_id`. No execution has started.
Queueing cannot adopt a parent `--delegation-id`.

`t3-fleet tick` drains a bounded slice under a single-owner lease and renews
reservations this local node owns as a target. Overlapping ticks return `busy`.
Every attempt uses live policy, roster, privacy, and capacity checks. A retry
routes the caller's original intent again rather than replaying a failed
machine selection. The original submission's authorization covers only that
same deferred work. A tick is never authority to create new work.

Inspect `last_tick_at`, `queue_disposition`, and `queue_reason` in results.
`held-acceptance-unknown` means the target may already have accepted the work.
Such jobs are held, never retried, and do not free capacity speculatively.
Retention expiry logically removes the prompt and request from live queue
records. It is never evidence that a run stopped. Known acceptance and terminal disposition
also purge the queued payload. Prompts stay in the controller's owner-only
outbox file and are excluded from Hub requests and status, error, tick, and
receipt projections. `payload_purged` means logical removal, without guaranteed
physical erasure from SQLite/WAL, snapshots, storage journals, or backups.

## Attention and continuations

`needs_attention` carries the current `pending_request`. Its `respond_with`
selects `approve` or `answer`. Preserve the ID and return only the operator's
specific decision or input. Followup cannot answer a pending request.

Enrolled `followup`, `approve`, and `answer` revalidate policy and renew the
same live reservation before resuming. Expired, released, or missing
reservations fail closed with `continuation-admission-required`. They need
newly negotiated admission. No resurrection protocol is invented here.

A terminal delegated result can report `disposition: "pending-handoff"` and
an opaque `worktree_ref`. The target retained writable work for review. This
proves no source checkout was updated or branch merged. Preserve the worktree
and handoff record for the operator.

## Rollback

Do not downgrade a live concurrency database directly. Drain known active work,
back up state, and review the rollback helper described in
[deployment](deployment.md#upgrade-and-rollback). Unknown work and retained
worktrees require reconciliation before any destructive cleanup.
