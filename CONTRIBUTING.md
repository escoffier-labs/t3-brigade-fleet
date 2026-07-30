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
3. Use generic roles in examples: `controller-a`, `controller-b`, `target-a`. Do not add real hosts, paths, ports, or identifiers.

## Development setup

```bash
git clone https://github.com/escoffier-labs/t3-brigade-fleet
cd t3-brigade-fleet
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Python 3.11 or newer is required.

## Running tests

Run the suite before and after your change. Do not skip tests to land a feature.

```bash
python -m pytest -q
```

## Style and writing rules

- Keep prose concise. Avoid filler, figurative language, and marketing tone.
- Do not use em-dashes, semicolons, or exclamation marks in prose.
- Use the active voice. Name the system that owns each action.
- Examples default to `approval-required`. Mention `full-access` only as a high-risk mode that public guidance does not recommend.

## Commit and pull request

- Use conventional commit messages (`feat:`, `fix:`, `docs:`, `test:`, `chore:`).
- Do not add `Co-Authored-By` trailers or mention AI tools in commit messages.
- Keep pull requests small and reviewable. One concern per request.
- Do not commit secrets, real hostnames, credentials, or private infrastructure details.

## Reporting problems with this guide

Open an issue. Pull requests that improve clarity are welcome.
