# Security model

This document covers the trust boundaries of `t3-fleet`, the approval model, and the runtime modes. For reporting a vulnerability see `SECURITY.md`.

## Trust boundaries

Three systems share responsibility. Each owns a distinct surface.

- **T3 Code** owns the session. It runs the model, manages the working tree, and produces the turn stream. `t3-fleet` does not interpret session contents.
- **t3-fleet** owns the allowlisted cross-machine transport, target authority, and recovery. It decides which hosts may receive work, which request may run on which target, and how an unknown outcome is reconciled.
- **Brigade** owns dispatch and verification receipts when `t3-fleet` is invoked through Brigade. `t3-fleet` does not grade its own work.

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

## Capacity as a safety control

Each host has an `active_limit`. The fleet has a `global_active_limit`. In `target-authoritative` mode the global limit must equal the sum of enabled host limits.

Capacity is a safety control first and a throughput knob second. Keep limits low. A host that hits its limit refuses new sessions instead of degrading the ones already running.

A host that fails repeatedly opens a circuit breaker. The controller stops sending new work to that host until the breaker resets. `capacity` reports `circuit_open` per host.

## Approval model

All mutating commands default to `approval-required`. The controller never selects `full-access` for you.

A session in `approval-required` mode pauses for a human decision before any action that touches the working tree. The reviewer returns a decision with `approve`:

- `accept` approves this one action.
- `acceptForSession` approves this action and the rest of the session.
- `decline` rejects this action.
- `cancel` cancels the session.

For non-edit questions the session raises an input request. The reviewer returns answers with `answer`.

## Runtime modes

`t3-fleet` supports four runtime modes. Public guidance only recommends `approval-required`.

- `approval-required` is the default. Every action that touches the working tree pauses for a human decision.
- `auto-accept-edits` accepts edit approvals automatically. Useful for trusted, reviewed prompts. Still pauses for non-edit questions.
- `auto` runs without pausing for approvals. Treat as a high-trust, scoped mode.
- `full-access` removes the remaining guardrails. This is a high-risk mode that public guidance does not recommend. It exists for sealed reference environments, not for everyday work.

Do not enable `full-access` on targets that share state with other work. Do not set `full-access` as a default in shared config.

## Prompt input

Prompt text is accepted only through `prompt` in a JSON request or through `prompt_file`. The CLI rejects inline prompts in Brigade tool calls. This keeps prompts out of tool-call journals that may be shared.

The CLI caps prompt size at 120000 characters and standard input at 1 MiB. A prompt file must be a non-empty path.

## Idempotency

`submit` accepts a client-chosen `request_id`. The controller records the request and rejects a conflicting submit with `idempotency_conflict`. Use a stable `request_id` for retries after a network failure, not a fresh one.

## What t3-fleet does not do

- It does not audit T3 Code. Session behavior is owned by T3 Code and the reviewer.
- It does not execute arbitrary commands on targets. It only dispatches T3 Code sessions.
- It does not store credentials in config. The loader rejects credential-shaped keys.
- It does not auto-promote runtime modes. The reviewer chooses.
