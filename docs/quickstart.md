# Quick start

This walkthrough runs the basic doctor and config validation path from a fresh clone. It does not require a live fleet. The submit and wait steps require a reachable T3 Code target.

## Roles used in this document

This document uses generic roles only. Replace them with your reviewed aliases.

- `controller-a` is the workstation where you run `t3-fleet`.
- `target-a` is an allowlisted target host that runs `t3-fleet-agent` and T3 Code.
- `target-b` is a second allowlisted target host, used in the multi-host examples.

## 1. Clone and install

```bash
git clone https://github.com/escoffier-labs/t3-brigade-fleet
cd t3-brigade-fleet
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Python 3.11 or newer is required. Verify the install:

```bash
t3-fleet --help
```

## 2. Create a config

`t3-fleet` reads a TOML config. The schema is `t3-fleet.config.v1`. The file must be mode `0600` on Unix.

Use the single local-host example so doctor can run without a live fleet and never probes an arbitrary SSH alias:

```bash
cp examples/local.example.toml .fleet.toml
chmod 600 .fleet.toml
export T3_FLEET_CONFIG="$PWD/.fleet.toml"
```

`examples/local.example.toml` defines one `local` host named `controller-a` and keeps `controller_host` set to that host. It is the safe doctor and config-validation path. To run real sessions, copy `examples/fleet.example.toml` instead and edit it to your reviewed `controller-a`, `target-a`, and `target-b` hosts. Keep `global_active_limit` at the sum of enabled host limits if you use `target-authoritative` mode.

## 3. Validate config and probe hosts

Run doctor and capacity. These do not start sessions.

```bash
t3-fleet doctor
t3-fleet capacity
```

The shipped local example contains a placeholder agent command. With no live agent, both commands fail closed with a `host_unavailable` JSON error and exit code `3`. That expected outcome confirms the config parsed and the allowlisted host was selected. It does not confirm target reachability.

With a reachable agent, `doctor` returns one entry per allowlisted host. A host is `eligible` when the agent is reachable and the project is registered. `capacity` returns `active`, `limit`, and `circuit_open` per host plus the global limit.

## 4. Submit a session

This step requires a reachable T3 Code target. The example uses `approval-required`, the default.

Write a prompt to a file so it does not enter your shell history:

```bash
cat > .prompt.txt <<'EOF'
Summarize the public commands in README.md.
EOF
```

Submit:

```bash
t3-fleet submit \
  --host target-a \
  --repository example-repo \
  --source-revision 0000000000000000000000000000000000000000 \
  --title "readme summary" \
  --prompt-file .prompt.txt \
  --runtime-mode approval-required
```

The controller prints a JSON result with a `request_id`. Save it.

## 5. Wait for the result

```bash
t3-fleet wait --request-id <request-id> --timeout 600 --include-output
```

`wait` blocks until the request reaches a terminal state or the timeout elapses. With `approval-required`, the session pauses for approvals. Use `approve` to return a decision:

```bash
t3-fleet approve \
  --request-id <request-id> \
  --approval-id <approval-id> \
  --decision accept
```

Run `status` to read pending approvals:

```bash
t3-fleet status --request-id <request-id>
```

Public status may omit the approval id that headless `approve` needs. If it is absent, stop and report the limitation. Do not guess an id or inspect private state to find one.

## 6. Interrupt or follow up

To stop a running session:

```bash
t3-fleet interrupt --request-id <request-id>
```

To send a follow-up prompt to a live session:

```bash
t3-fleet followup \
  --request-id <request-id> \
  --prompt-file .followup.txt \
  --runtime-mode approval-required
```

## 7. Recover an unknown outcome

If a request ends in `unknown`, run `recover` to reconcile against the target store:

```bash
t3-fleet recover --request-id <request-id>
```

See `docs/recovery.md` for when to use `recover` and what it does.

## Next steps

- `docs/security-model.md` for allowlisting, approvals, and runtime modes.
- `docs/architecture.md` for the layer model and schemas.
- `SECURITY.md` for reporting a vulnerability.
