# Changelog

All notable changes to `t3-fleet` are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) with the pre-1.0 exception noted below.

This project is pre-1.0. Before 1.0, minor versions may include breaking changes. Pin a version in production and read this file before upgrading.

## [Unreleased]

### Added
- Public repository surface: `LICENSE`, `README.md`, `SECURITY.md`, `CONTRIBUTING.md`, `CHANGELOG.md`.
- Documentation set: `docs/architecture.md`, `docs/quickstart.md`, `docs/security-model.md`, `docs/recovery.md`.
- Public metadata in `pyproject.toml`: license, classifiers, project URLs.

## [0.1.0] - Unreleased

Initial public candidate. Not yet released.

### Added
- `t3-fleet` controller CLI with commands: `doctor`, `capacity`, `submit`, `status`, `wait`, `followup`, `interrupt`, `approve`, `answer`, `recover`.
- `t3-fleet-agent` per-target agent invoked over local or SSH transport.
- Allowlisted fleet config format `t3-fleet.config.v1` with owner-only file permissions.
- Request and result JSON schemas `t3-fleet.request.v1` and `t3-fleet.result.v1`.
- `approval-required` as the default runtime mode for all mutating commands.
- Reference Windows service scripts under `scripts/`.
