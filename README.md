# t3-fleet

`t3-fleet` is a command-line dispatcher for running [T3 Code](https://github.com/pingdotgg/t3code) sessions across an allowlisted fleet of hosts. It routes prompts to a reviewed set of targets, tracks each request to a terminal state, and exposes a closed recovery surface for unknown outcomes.

This project is pre-1.0 and experimental. APIs, schemas, and config formats may change between minor versions before 1.0. Pin a version in production until 1.0 lands.

## Scope and boundaries

Three systems share responsibility. Knowing the line between them prevents the most common mistakes.

- **T3 Code** owns each coding session. It runs the model, manages the working tree, and produces the turn stream. `t3-fleet` never interprets session contents.
- **t3-fleet** owns the allowlisted cross-machine transport, target authority, and recovery. It decides which hosts may receive work, which request is allowed to run on which target, and how an unknown outcome is reconciled.
- **Brigade** owns dispatch and verification receipts. When `t3-fleet` is invoked through Brigade, Brigade records the dispatch and its captured verification result. `t3-fleet` does not grade its own work.

`t3-fleet` is not a job runner for arbitrary commands. It only dispatches T3 Code sessions to hosts you have reviewed and listed in config.

## Requirements

- Python 3.11 or newer.
- T3 Code installed and reachable on each target host.
- An SSH config (or local transport) for every host you list.

## Install

```bash
pipx install .
```

This provides two console entry points:

- `t3-fleet` is the controller CLI you run from your workstation.
- `t3-fleet-agent` is the per-target agent invoked by the controller over the configured transport.

## Quick start

The basic doctor and config validation path runs from a fresh clone without a live fleet. Use the single local-host example so doctor never probes an arbitrary SSH alias.

```bash
git clone https://github.com/escoffier-labs/t3-brigade-fleet
cd t3-brigade-fleet
pipx install .

# Point t3-fleet at the safe single local-host example.
export T3_FLEET_CONFIG="$PWD/examples/local.example.toml"
chmod 600 "$T3_FLEET_CONFIG"

# Validate config and probe the allowlisted hosts.
t3-fleet doctor
t3-fleet capacity
```

The shipped local example contains a placeholder agent command. Without a live agent, `doctor` and `capacity` fail closed with `host_unavailable` and exit code `3` after the config is parsed and the allowlisted host is selected. With a reachable agent, `doctor` reports per-host reachability and `capacity` reports active and limit per host plus the global limit. See `docs/quickstart.md` for the full walkthrough, `examples/local.example.toml` for the safe no-live-fleet shape, and `examples/fleet.example.toml` for a three-host target-authoritative example.

## Public commands

All mutating commands default to `approval-required`. The controller never selects `full-access` for you. See `docs/security-model.md` before changing runtime mode.

- `t3-fleet doctor` reports whether each allowlisted host is reachable and registered.
- `t3-fleet capacity` reports active sessions and limits per host plus the global limit.
- `t3-fleet submit` queues a new session on a chosen host and repository.
- `t3-fleet status` reads the current state of a request without waiting.
- `t3-fleet wait` blocks until a request reaches a terminal state or the timeout elapses.
- `t3-fleet followup` sends a follow-up prompt to a running or paused session.
- `t3-fleet interrupt` requests a graceful interrupt of a running session.
- `t3-fleet approve` returns a decision for a pending approval on a session.
- `t3-fleet answer` returns answers for a pending input request on a session.
- `t3-fleet recover` reconciles a request whose outcome is unknown.

Most commands accept flags or a single JSON object on standard input. `answer` is JSON-stdin-only: it has no flags and reads a single JSON object with `request_id`, `input_id`, and `answers`. The JSON contract is documented in `schemas/request.schema.json` and `schemas/result.schema.json`.

## Runtime modes

`t3-fleet` supports four runtime modes. Public guidance only recommends `approval-required`.

- `approval-required` is the default. Every action that touches the working tree pauses for a human decision.
- `auto-accept-edits` accepts edit approvals automatically. Useful for trusted, reviewed prompts. Still pauses for non-edit questions.
- `auto` runs without pausing for approvals. Treat as a high-trust, scoped mode.
- `full-access` removes the remaining guardrails. This is a high-risk mode that public guidance does not recommend. It exists for sealed reference environments, not for everyday work.

## Windows service scripts

The PowerShell scripts under `scripts/` are reference examples for running a headless T3 Code service on Windows. They are not installed by `pipx install .` and are not required for Linux or macOS targets. Treat them as starting points and review them before use. See `scripts/install-windows-t3-service.ps1` and `scripts/t3-fleet-agent.ps1`.

## Documentation

- `docs/architecture.md` covers the controller, agent, transport, and store layers.
- `docs/quickstart.md` walks through config, doctor, submit, and wait.
- `docs/security-model.md` covers allowlisting, approvals, and runtime modes.
- `docs/recovery.md` covers the `recover` command and unknown-outcome handling.
- `SECURITY.md` covers reporting and supported versions.
- `CONTRIBUTING.md` covers the contribution path.
- `CHANGELOG.md` covers released changes.

## License

Apache-2.0. See `LICENSE`.
