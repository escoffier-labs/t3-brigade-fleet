# Contributing

Contributions are welcome. This project is pre-1.0, so the contract is still moving. Read this file and `docs/architecture.md` before opening a change.

## Scope

`t3-fleet` is a closed dispatcher for T3 Code sessions across an allowlisted fleet. Changes that broaden the trust surface need a design note before code.

In scope:

- Bug fixes for the controller, agent, transport, store, or CLI.
- Documentation improvements.
- Schema clarifications that do not break existing valid requests.
- New allowlist or approval hardening.

Out of scope without prior discussion:

- Arbitrary command execution on targets.
- Auto-promotion of `full-access` or any non-approval default.
- Inline prompt storage in Brigade tool calls.

## Before you start

1. Open an issue describing the change. Link the issue from your branch.
2. Confirm the change fits the boundaries in `README.md` and `docs/security-model.md`.
3. Use generic roles in examples: `controller-a`, `target-a`, `target-b`. Do not add real hosts, paths, ports, or identifiers.

## Development setup

```bash
git clone https://github.com/escoffier-labs/t3-brigade-fleet
cd t3-brigade-fleet
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Python 3.11 or newer is required. Runtime dependencies remain empty. Tests use
standard-library unittest. Live dispatch also needs the external Brigade
CLI and operator admission setup described in `docs/deployment.md`.

## Running tests

Run the suite before and after your change. Do not skip tests to land a feature.

```bash
python scripts/project-skill.py
PYTHONPATH=src:tests python -m unittest discover -s tests -v
python scripts/project-skill.py --check
```

The cross-repository wire check is explicitly skipped unless
`T3_FLEET_BRIGADE_SOURCE` names a compatible Brigade source checkout. The
checked-in generic wire fixture is still exercised on every run. Do not hide a
skip or claim it proves current interoperability with a live Hub.

CI retains Linux Python 3.11/3.12/3.13, wheel installation, and a Windows
PowerShell launcher contract. Run the PowerShell check on Windows when changing
launcher behavior. Generated harness files are local outputs of
`scripts/project-skill.py`. Edit the canonical skill and tool sources, then
regenerate and run the public safety checks. Do not commit operator settings.

## Style and writing rules

- Keep prose concise. Avoid filler, figurative language, and marketing tone.
- Do not use em-dashes, semicolons, or exclamation marks in prose.
- Use the active voice. Name the system that owns each action.
- Examples default to `approval-required`. Mention `full-access` only as a high-risk mode that public guidance does not recommend.

## Commit and pull request

- Use conventional commit messages (`feat:`, `fix:`, `docs:`, `test:`, `chore:`).
- Add a `Co-Authored-By` trailer only when a tool did substantial work on the change. Verify the trailer before adding it: a real, correct name and email for the tool that authored the substantial portion. Do not add unverified or decorative trailers. Do not mention AI tools in commit messages outside of a verified `Co-Authored-By` trailer.
- Keep pull requests small and reviewable. One concern per request.
- Do not commit secrets, real hostnames, credentials, or private infrastructure details.

## Reporting problems with this guide

Open an issue. Pull requests that improve clarity are welcome.
