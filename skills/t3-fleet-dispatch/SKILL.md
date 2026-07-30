---
name: t3-fleet-dispatch
license: Apache-2.0
description: Use when dispatching, monitoring, sending a followup to, interrupting, approving, answering, or recovering T3 Code tasks through the local `t3-fleet` client.
---

# T3 Fleet dispatch

Use the public client, `t3-fleet`, from `controller-a` to operate only reviewed, allowlisted targets such as `target-a`. Treat the client result as the current record. Do not infer state from a model transcript or local process output.

## Operator gate

Get explicit operator approval before every `submit`, `followup`, `interrupt`, `approve`, `answer`, or `recover` call. State the request handle, intended action, and target when it is known. A prior approval for one action does not authorize another action.

For `submit` and `followup`, use `approval-required`. Keep that mode unless the operator explicitly authorizes a different public mode. The reviewer, not the agent, makes approval decisions.

Protect prompts. Never put prompt text on a command line. Use a prompt file reference or a JSON object on standard input. Keep prompt contents and private paths out of command examples and logs.

## Public commands

Document and use only this public command surface:

| Command | Use |
| --- | --- |
| `doctor` | Check reviewed targets for reachability and registration. |
| `capacity` | Read active-session and limit information. |
| `submit` | Queue a new session. Requires explicit operator approval. |
| `status` | Read the current state without waiting. |
| `wait` | Wait for a terminal state or timeout. |
| `followup` | Send a prompt to a running or paused session. Requires explicit operator approval. |
| `interrupt` | Request a graceful interruption. Requires explicit operator approval. |
| `approve` | Return the operator's decision for one pending approval. Requires explicit operator approval. |
| `answer` | Return the operator's answers for one pending input request. Requires explicit operator approval. |
| `recover` | Reconcile an unknown outcome. Requires explicit operator approval. |

Do not construct endpoints, connection commands, worktree paths, private state paths, or host addresses. Use only the reviewed names accepted by the public client. Do not invent request handles, approval identifiers, input identifiers, or target names.

## Dispatch and monitoring

Before dispatch, use `doctor` and `capacity` when their current information matters:

```bash
t3-fleet doctor --host target-a
t3-fleet capacity --host target-a
```

For a submit, preserve the exact returned request handle. Supply an immutable 40-character lowercase source revision. Do not replace it with a branch name, shorten it, or change it after submission.

```bash
t3-fleet submit \
  --host target-a \
  --repository example-repo \
  --source-revision 0123456789abcdef0123456789abcdef01234567 \
  --title "requested task" \
  --prompt-file "$PROMPT_FILE" \
  --runtime-mode approval-required
```

Use qualified request handles returned by the client. In a multi-controller fleet, preserve a returned controller-and-target-qualified handle exactly and use it for later operations. Never shorten, reinterpret, or manufacture a handle.

```bash
t3-fleet status --request-id <qualified-request-id>
t3-fleet wait --request-id <qualified-request-id> --timeout 600
```

Do not claim completion from `wait`, output, or a prior result alone. Read a fresh `status` result for the same qualified request handle and claim completion only when it reports `succeeded`.

## Live-session actions

After explicit operator approval, a followup uses the same qualified request handle and `approval-required` mode:

```bash
t3-fleet followup \
  --request-id <qualified-request-id> \
  --prompt-file "$FOLLOWUP_PROMPT_FILE" \
  --runtime-mode approval-required
```

After explicit operator approval, use `interrupt` only for the named live request:

```bash
t3-fleet interrupt --request-id <qualified-request-id>
```

After explicit operator approval, send only the decision the operator supplied:

```bash
t3-fleet approve \
  --request-id <qualified-request-id> \
  --approval-id <approval-id> \
  --decision accept
```

Public status output may not expose the approval id that headless `approve` needs. If it is absent, report that limitation to the operator. Do not guess an approval id or inspect private state to obtain one.

`answer` is JSON-stdin-only. After explicit operator approval, provide exactly one JSON object on standard input with `request_id`, `input_id`, and `answers`. Do not pass answer data as flags or command-line text.

```bash
t3-fleet answer < "$ANSWER_JSON"
```

## Recovery

Use `recover` only for an unknown outcome, after explicit operator approval. It reconciles the existing request and does not create a retry. In target-authoritative mode, the target record is authoritative. Preserve the result returned by recovery, then read fresh status before making a completion claim.

```bash
t3-fleet recover --request-id <qualified-request-id>
```

If recovery cannot confirm the outcome, leave it unknown and report that state. Do not submit replacement work as an implicit recovery step.

## Command contract

Read [the contract reference](references/contract.md) before selecting a command or shaping JSON input.
