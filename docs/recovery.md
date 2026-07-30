# Recovery

This document covers the `recover` command and how `t3-fleet` handles unknown outcomes. For the request lifecycle see `docs/architecture.md`.

## When an outcome is unknown

A request normally reaches one of these terminal states: `succeeded`, `failed`, or `interrupted`. A request that the controller cannot confirm reaches `unknown`.

`unknown` is non-terminal. It means the controller lost contact with the target mid-mutation and cannot prove the session ended. The slot stays reserved until `recover` reconciles the request.

Common causes:

- The transport dropped after the controller sent the request but before the agent acknowledged.
- The controller process was interrupted between sending the request and recording the result.
- The target host rebooted while a session was running.

## What recover does

`recover` reconciles a request against the target store. The target store is the source of truth in `target-authoritative` mode and a strong hint in `single-controller` mode.

The controller asks the agent for the recorded outcome of the request. The agent returns one of:

- A terminal receipt: `succeeded`, `failed`, or `interrupted`. The controller records the receipt and releases the slot.
- No receipt: the controller marks the request as not started and releases the slot.
- An indeterminate state: the controller leaves the slot reserved and returns `unknown`. Retry `recover` after the target is reachable.

`recover` does not start a new session. It only reconciles the existing one.

## When to use recover

Run `recover` when:

- `status` or `wait` returns `unknown_state` (exit 5).
- A controller process was killed mid-request.
- A target host was unreachable during a request and is now reachable.

Do not run `recover` for a request that already returned `succeeded`, `failed`, or `interrupted`. It is a no-op in those cases and adds load to the target.

## How to call recover

By request id:

```bash
t3-fleet recover --request-id <request-id>
```

By target hint, when you do not have the request id but know which host ran the work:

```bash
t3-fleet recover --target target-a
```

Include output to see the reconciled result:

```bash
t3-fleet recover --request-id <request-id> --include-output
```

`recover` requires at least a `request_id` or a `target`. The CLI rejects an empty `target`.

## Multi-controller recovery

In `target-authoritative` mode more than one controller may dispatch to the same fleet. Each controller writes its own request id. The target store records the controller id with each receipt.

When `controller-a` loses contact with a request, a second controller may run `recover` against the same target store. The target store returns the receipt that `controller-a` recorded, or the agent's record if the controller receipt is missing. This is why the target store is the source of truth in this mode.

In `single-controller` mode there is one controller. `recover` still reconciles against the target store, but the controller-side store is the primary record.

## Recovery is not a retry

`recover` does not resubmit a failed request. If a session ended in `failed`, fix the cause and submit a new request with a new `request_id`. If a session ended in `interrupted`, that was a deliberate stop. Submit a new request only if you want to restart the work.

## Limits

- `recover` cannot reconcile a request that the target store never recorded. In that case the controller releases the slot and marks the request as not started.
- `recover` cannot prove a negative. If the target is unreachable, it returns `unknown` and leaves the slot reserved. Bring the target up before retrying.
- `recover` does not bypass approvals. A session that paused for an approval stays paused. Use `approve` or `answer` to drive it forward.
