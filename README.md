<p align="center">
  <img src="docs/assets/t3-brigade-fleet-social-preview.jpg" alt="T3 Brigade Fleet banner" width="900">
</p>

<h1 align="center">T3 Brigade Fleet</h1>

<p align="center">
  <strong>Dispatch T3 Code sessions across an allowlisted fleet, with Brigade receipts for operators running agents on more than one machine.</strong>
</p>

<p align="center">
  <a href="https://brigade.tools">Brigade</a> &middot;
  <a href="docs/architecture.md">Architecture</a> &middot;
  <a href="#try-it-in-60-seconds">Quickstart</a> &middot;
  <a href="docs/security-model.md">Security</a>
</p>

<p align="center">
  <img src="https://shieldcn.dev/github/ci/escoffier-labs/t3-brigade-fleet.svg?workflow=ci.yml&branch=main&label=ci&size=xs" alt="CI status">
  <img src="https://shieldcn.dev/badge/python-3.11+-3776AB.svg?logo=python&logoColor=white&size=xs" alt="Python 3.11+">
  <img src="https://shieldcn.dev/badge/license-Apache--2.0-4e7247.svg?size=xs" alt="Apache-2.0 license">
</p>

One controller, reviewed targets, stable request IDs, and a closed recovery path when a connection drops during dispatch.

<p align="center">
  <img src="docs/assets/t3-brigade-fleet-dispatch.svg" alt="Recording: t3-fleet submits a T3 Code session to an allowlisted host and waits for a successful terminal result" width="900">
</p>

<p align="center"><em><code>t3-fleet submit</code> starts an allowlisted session and <code>wait</code> reports its terminal result.</em></p>

## What it does

`t3-fleet` sends a prompt to T3 Code on an allowlisted target, reserves that repository, and tracks the request until it succeeds, fails, pauses, or enters an unknown state. It gives operators with several machines one controller surface without giving that controller arbitrary remote-command access. T3 Code still owns the coding session, while Brigade can wrap each dispatch and verification in a receipt.

The project is experimental and pre-1.0. Pin the commit you deploy until the public interfaces settle.

## Install

Python 3.11 or newer is required.

```bash
pipx install git+https://github.com/escoffier-labs/t3-brigade-fleet.git
```

This installs two commands:

- `t3-fleet` is the controller you run from your workstation.
- `t3-fleet-agent` is the per-target agent invoked through the configured transport.

## Try it in 60 seconds

The shipped local example validates the controller without contacting an arbitrary SSH alias:

```bash
git clone https://github.com/escoffier-labs/t3-brigade-fleet.git
cd t3-brigade-fleet
pipx install .

export T3_FLEET_CONFIG="$PWD/examples/local.example.toml"
chmod 600 "$T3_FLEET_CONFIG"
t3-fleet doctor
```

The placeholder agent is deliberately unavailable, so the safe offline check fails closed after parsing the config and selecting the allowlisted target:

```json
{"error":{"code":"host_unavailable","message":"fleet host is unavailable"},"schema":"t3-fleet.error.v1"}
```

The command exits `3`. Replace the placeholder agent with a reviewed target before submitting work. The full path from config through `submit` and `wait` is in [the quickstart](docs/quickstart.md).

## Dispatch a session

Submit accepts flags for interactive use or one JSON object on standard input for Brigade and other callers. Prompt text stays out of the process arguments.

```bash
t3-fleet submit \
  --host target-a \
  --repository example-repo \
  --source-revision 0123456789abcdef0123456789abcdef01234567 \
  --title "Verify the public API" \
  --runtime-mode approval-required \
  < prompt.txt
```

The result includes a stable `request_id` and the T3 Code `thread_id`. Use that request ID for every later action:

```bash
t3-fleet wait --request-id <request-id> --timeout 600 --include-output
```

## Who owns what

| System | Responsibility |
|---|---|
| [T3 Code](https://github.com/pingdotgg/t3code) | Runs the model, owns the working tree, manages approvals, and emits the turn stream |
| `t3-fleet` | Enforces the target allowlist, transports requests, reserves repositories, and reconciles unknown outcomes |
| [Brigade](https://brigade.tools) | Dispatches work through reviewed lanes and records verification receipts |

The controller never connects directly to a remote T3 Code server. It talks to the target agent, and that agent talks to its local T3 Code instance.

## Public commands

- `t3-fleet doctor` checks whether each allowlisted target is reachable and registered.
- `t3-fleet capacity` reports active sessions and limits per target.
- `t3-fleet submit` starts a T3 Code session on a selected target and repository.
- `t3-fleet status` reads a request without waiting.
- `t3-fleet wait` polls until the request is terminal or needs attention.
- `t3-fleet followup` sends another prompt to an existing session.
- `t3-fleet approve` returns a decision for a pending approval.
- `t3-fleet answer` answers a pending structured input request.
- `t3-fleet interrupt` requests a graceful stop.
- `t3-fleet recover` reconciles a request whose dispatch outcome is unknown.
- `t3-fleet tick` drains previously authorized queued work and renews local target reservations.

Most commands accept flags or a single JSON object on standard input. `answer` is JSON-stdin-only. The request and result contracts live in [`schemas/`](schemas/).

## Operator and integration commands

`settle-sweep` and `install-settle-timer` are operator-only commands. JSON
`invoke` rejects them. Settlement is disabled by default and requires an
operator to configure it. Do not install a timer as part of model-driven work.
`invoke` reads request JSON for integrations, while `brigade-invoke` reads only
the authorized Brigade tool-call journal. Neither bypasses admission or approval.

## Approval modes

Every mutating command defaults to `approval-required`. The controller never selects `full-access` on your behalf.

- `approval-required` pauses before an action touches the working tree.
- `auto-accept-edits` accepts edit approvals but still pauses for other questions.
- `auto` runs without approval pauses and should stay limited to reviewed prompts.
- `full-access` removes the remaining T3 Code guardrails and is intended only for sealed reference environments.

Read the [security model](docs/security-model.md) before changing the default.

## Windows targets

The PowerShell files under [`scripts/`](scripts/) are reference launchers for a headless T3 Code service and `t3-fleet-agent` on Windows. They are not installed by `pipx` and should be reviewed before use.

## Why not manual SSH or a generic queue?

Manual SSH and one T3 Code window per machine are reasonable when one operator has one or two targets. They do not provide a shared request identity, repository leases, target-side receipts, or a recovery command for a connection that drops after the remote session starts.

A generic queue can distribute jobs across machines. It does not know T3 Code thread, approval, follow-up, and input semantics. `t3-fleet` exposes those operations and refuses arbitrary shell execution.

If one T3 Code server already covers every machine you use, start with T3 Code alone. Add this dispatcher when you need an allowlisted controller across separate T3 Code targets or want Brigade receipts around that work.

## What T3 Brigade Fleet is not

T3 Brigade Fleet is not a hosted control plane, a model provider, or a replacement for T3 Code or Brigade. It does not synchronize private memory stores, copy credentials between devices, inspect prompt contents, or accept arbitrary remote commands. Fleet configuration, authentication, and target approval remain operator-owned.

## Docs

- [Architecture](docs/architecture.md)
- [Quickstart](docs/quickstart.md)
- [Security model](docs/security-model.md)
- [Recovery](docs/recovery.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Changelog](CHANGELOG.md)

## License

Apache-2.0. See [LICENSE](LICENSE). An [Escoffier Labs](https://github.com/escoffier-labs) project for operators using [Brigade](https://brigade.tools) and T3 Code. The public command is `t3-fleet`.
