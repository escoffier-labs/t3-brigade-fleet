# Security model

This document covers the trust boundaries of `t3-fleet`, the approval model, and the runtime modes. For reporting a vulnerability see `SECURITY.md`.

## Trust boundaries

Three systems share responsibility. Each owns a distinct surface.

- **T3 Code** owns the session. It runs the model, manages the working tree, and produces the turn stream. `t3-fleet` does not interpret session contents.
- **t3-fleet** owns the allowlisted cross-machine transport, target authority, and recovery. It decides which hosts may receive work, which request may run on which target, and how an unknown outcome is reconciled.
- **Brigade** owns model admission and enrolled policy routing, reservations,
  and prepared context. These gates apply even outside a Brigade tool runner.
  Verification receipts record observations. `t3-fleet` does not grade itself.

The human reviewer owns approval decisions. `t3-fleet` enforces that a decision is required. It does not make the decision.

## Allowlisting

Every target must be listed in the fleet config. The config is a TOML file with schema `t3-fleet.config.v1`. The loader rejects:

- Unknown top-level keys, and any key that looks like a credential (`token`, `secret`, `password`, `credential`, `private_key`).
- IP literals in `ssh_alias`. Use a reviewed SSH alias from your SSH config.
- Config files that are not mode `0600` on Unix.
- Host entries with no `expected_hostname`.
- Repository entries whose `branch_prefix` does not start with `codex/`.

A host that is not in the config cannot receive work. There is no wildcard host and no dynamic discovery.

## Transport

Transport is `local` or `ssh`. There is no HTTP transport and no remote agent listener.

- `local` runs the agent as a subprocess on the controller host. It must not set `ssh_alias`.
- `ssh` runs the agent over SSH using a reviewed alias. The controller invokes the SSH command from config. It does not construct host strings from IP literals.

The controller never opens a direct connection to T3 Code on a remote host. It always goes through the agent. This keeps the trust boundary at the agent process.

Each request names the selected logical host. The agent rejects a different
host before acquiring request locks or mutating state. Every successful reply
attests the agent's host and uses `t3-fleet.agent-result.v1`. The controller
checks the schema, every supplied host/target identity, and the matching
request ID before accepting a reply, including `not_found`. Invalid replies
after mutation leave acceptance unknown and capacity reserved for recovery.
Upgrade controllers and targets together to preserve these protocol checks.

The target's `t3_url` must use HTTP or HTTPS with a literal loopback IPv4 or
IPv6 address, such as `127.0.0.1` or `[::1]`. Hostname aliases, including
`localhost`, remote addresses, scoped IPv6, user information, query strings,
fragments, non-root paths, and invalid ports are rejected before session
issuance or network I/O. The production HTTP client does not follow redirects.
Injected HTTP openers are trusted application code and own their redirect
behavior.

## Capacity as a safety control

Each host has an `active_limit`. The fleet has a `global_active_limit`. In `target-authoritative` mode the global limit must equal the sum of enabled host limits.

Capacity is a safety control first and a throughput knob second. Keep limits low. A host that hits its limit refuses new sessions instead of degrading the ones already running.

A host that fails repeatedly opens a circuit breaker. The controller stops sending new work to that host until the breaker resets. `capacity` reports `circuit_open` per host.

## Approval model

`submit` and `followup` default to `approval-required`. Mutating actions also
require operator authorization for their exact scope. Model admission and tool
runner approval remain independent gates. Existing authorization for an exact
action can be reused.

A session in `approval-required` retains T3 runtime permission prompts. The
operator can return `accept`, `acceptForSession`, `decline`, or `cancel` for a
current approval. Session-wide acceptance has broader scope than one action.
Non-edit questions use `answer`. A bounded `pending_request` carries the ID and
`respond_with` for the current response. Never guess an ID or answer.

## Runtime modes and admission

Public guidance recommends `approval-required`. The runtime also recognizes
`auto-accept-edits`, `auto`, and `full-access`. These modes reduce permission
prompts and need separate operator review. Public examples do not enable them.
Runtime mode never authorizes another host, model, publication, or task.

A compatible external Brigade CLI and admitted logical seat are required on
controllers and targets. Policy enrollment off does not disable model admission.
An enrolled controller sends a plan, while the target prepares the exact policy
context, acknowledges its hash, and admits immediately before launch. Caller
provider/model overrides and T3 project defaults are never authority. Missing,
malformed, expired, or denied authority fails closed.

Enrolled continuations must renew the same live reservation. Released, expired,
or missing authority cannot be resurrected by followup or an approval response.
Unknown transport outcomes retain capacity. Only explicit operator recovery may
request containment. Passive status and settlement never stop running work.

## Prompt input

Prompt text is accepted only through `prompt` in a JSON request or through `prompt_file`. The CLI rejects inline prompts in Brigade tool calls. This keeps prompts out of tool-call journals that may be shared.

The CLI caps prompt size at 120000 characters and standard input at 1 MiB. A prompt file must be a non-empty path.

## Idempotency

In single-controller mode, `submit` accepts a client-chosen `request_id`.
Target-authoritative submit generates a qualified controller-and-target handle. The controller records the request and rejects a conflicting submit with `idempotency_conflict`. After uncertain acceptance, preserve the returned handle and recover instead
of replaying the original prompt. A parent delegation is one execution and
exact adoption retries replay its durable receipt without new routing.

## Queue storage and operator controls

The local queue is disabled by default and accepts only explicit
`--allow-queue` submissions. It stores protected prompt/request payloads in an
owner-only controller outbox, outside Hub and public status/error projections.
On Windows restrict the parent directory ACL. Known acceptance, terminal
disposition, and retention expiry logically remove payloads from live queue
records. `payload_purged` reports that logical removal. Each outbox connection
sets and verifies SQLite `secure_delete=ON` as defense in depth. This does not
guarantee physical erasure from SQLite pages or WAL, snapshots, storage journals,
or backups. Retention expiry never proves execution stopped. Ambiguous
acceptance remains held, never retried, and never frees capacity speculatively.

The model-facing schema contains 11 dispatch actions including bounded `tick`.
Settlement and timer installation are operator-only CLI commands, excluded from
JSON actions. Catalog invocation requires a matching approved journal record,
protected prompt files, and `approval_mode = "always"`. A model cannot use
integration plumbing to bypass those gates.

## What t3-fleet does not do

- It does not audit T3 Code. Session behavior is owned by T3 Code and the reviewer.
- It does not execute arbitrary commands on targets. It only dispatches T3 Code sessions.
- It does not store credentials in config. The loader rejects credential-shaped keys.
- It does not auto-promote runtime modes. The reviewer chooses.
