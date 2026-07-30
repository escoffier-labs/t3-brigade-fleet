# Public command contract

This reference records the public `t3-fleet` contract used by the dispatch skill. Run the client from `controller-a` and select reviewed names such as `target-a`. It does not authorize actions by itself. Obtain explicit operator approval before every `submit`, `followup`, `interrupt`, `approve`, `answer`, and `recover` call.

## Commands

| Command | Public input and result contract |
| --- | --- |
| `doctor` | Optionally accepts a reviewed host and repository. Reports target reachability and registration. |
| `capacity` | Optionally accepts a reviewed host. Reports active sessions, host limits, and the global limit. |
| `submit` | Requires host, repository, source revision, title, and exactly one prompt source. Use `approval-required` and a fixed 40-character lowercase source revision. |
| `status` | Requires a request handle. Reads the current state without waiting. |
| `wait` | Requires a request handle. Waits for a terminal state or timeout. |
| `followup` | Requires a request handle and exactly one prompt source. Use `approval-required`. |
| `interrupt` | Requires a request handle. Requests a graceful interruption. |
| `approve` | Requires a request handle, approval id, and one operator decision. |
| `answer` | JSON-stdin-only. Requires one JSON object containing `request_id`, `input_id`, and `answers`. |
| `recover` | Requires a request handle or reviewed target hint. Reconciles an existing unknown outcome. |

## Request rules

- Use qualified request handles returned by the client. Preserve a controller-and-target-qualified handle exactly when provided. Never fabricate, shorten, or rewrite one.
- A source revision is an immutable 40-character lowercase commit identifier. Do not use a branch name or abbreviated revision, and do not mutate the revision of an existing request.
- Prompt text must never appear on a command line. Supply it by prompt-file reference or JSON standard input, and keep prompt text and private paths out of logs and examples.
- `answer` accepts no flags. Its full request belongs in one JSON object on standard input.
- The public status result may not include the approval id required by headless `approve`. If it does not, stop and report the limitation. Do not discover it from private state or guess it.
- Do not construct endpoints, connection commands, worktree paths, private state paths, or host addresses. Use the public client with reviewed names only.

## State and recovery rules

- Terminal states are `succeeded`, `failed`, and `interrupted`. `unknown` is non-terminal.
- Do not claim completion until a fresh `status` read for the same qualified request handle reports `succeeded`.
- `recover` is not a retry. It reconciles existing work after an unknown result.
- In target-authoritative mode, the target record is the source of truth for recovery. A controller must accept the reconciled target result rather than infer an outcome locally.
- A request that remains unknown stays unresolved. Do not replace it by submitting new work unless the operator separately approves that new submission.

## Generic examples

```bash
t3-fleet doctor --host target-a
t3-fleet capacity --host target-a
t3-fleet status --request-id <qualified-request-id>
t3-fleet wait --request-id <qualified-request-id> --timeout 600
```

For JSON-stdin-only answer input, place the reviewed JSON object in standard input without putting its contents on the command line:

```bash
t3-fleet answer < "$ANSWER_JSON"
```
