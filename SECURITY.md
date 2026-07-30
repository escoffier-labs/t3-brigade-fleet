# Security policy

## Supported versions

`t3-fleet` is pre-1.0 and experimental. Only the latest released version receives security fixes. There are no long-term support branches before 1.0.

Pin a specific version in production and track the changelog for breaking changes between minor versions.

## Reporting a vulnerability

Report security issues privately. Do not open a public issue for a vulnerability.

1. Use GitHub's private vulnerability reporting on the repository Security tab: open `https://github.com/escoffier-labs/t3-brigade-fleet/security` and choose "Report a vulnerability". This submits a private advisory through GitHub's reporter form. It is not email and does not post a public issue.
2. Include a description, a minimal reproduction, and the affected version.
3. Wait for a response before publishing any details.

Maintainers acknowledge reports within five business days and aim to ship a fix or mitigation within 30 days for issues that affect a default configuration.

## Threat model summary

`t3-fleet` dispatches T3 Code sessions to an allowlisted fleet. The trust boundaries are documented in `docs/security-model.md`.

- The controller runs on your workstation and reads a config file that must be mode `0600`.
- Hosts are listed explicitly. Transport is local or SSH only. IP literals are rejected in `ssh_alias`.
- Mutating commands default to `approval-required`. The controller never selects `full-access` for you.
- Prompt text is accepted only through a prompt file or a single JSON object on standard input. The controller rejects inline prompts in Brigade tool calls.

## Configuration hardening

- Store the fleet config at mode `0600` on a filesystem that honors Unix permissions.
- Review every `ssh_alias` before adding it to config. Use reviewed SSH aliases, not raw hostnames or IP literals.
- Keep `active_limit` and `global_active_limit` low. Capacity is a safety control first and a throughput knob second.
- Do not enable `full-access` on targets that share state with other work.

## Not in scope

`t3-fleet` does not audit T3 Code itself. Session behavior, model output, and working-tree changes are owned by T3 Code and the human reviewer. Report T3 Code issues to the T3 Code project, not here.
