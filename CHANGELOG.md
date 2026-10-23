# Changelog

All notable changes to `t3-fleet` are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) with the pre-1.0 exception noted below.

This project is pre-1.0. Before 1.0, minor versions may include breaking changes. Pin a version in production and read this file before upgrading.

## [Unreleased]

## [0.2.0] - Unreleased

Public candidate. No registry release has been cut.

### Added

- Required Brigade model admission and optional enrolled policy routing,
  prepared-context acknowledgement, and parent delegation adoption.
- Protocol 2 support, bounded pending requests, lifecycle reporting, and target
  reconciliation that preserves identity and capacity under uncertain outcomes.
- Per-repository concurrency controls, a bounded opt-in local queue, and `tick`.
- Operator-only settlement and timer commands, excluded from JSON tool actions.
- Public skill references and reproducible projections using approval-required.
- Deployment prerequisites and installed-wheel checks while retaining Linux
  Python 3.11/3.12/3.13 and Windows launcher CI.

### Changed

- A pre-0.2 installation must configure Brigade and an admitted logical seat
  before dispatch. Policy enrollment off does not bypass model admission.
- Doctor and capacity report unreachable targets per host as `blocked_offline`.
  A completed diagnostic can exit zero while no target is eligible.
- Generic examples use protected prompts and keep settlement and queuing off.

## [0.1.0] - Unreleased

Initial public candidate. Not yet released.

### Added
- `t3-fleet` controller CLI with commands: `doctor`, `capacity`, `submit`, `status`, `wait`, `followup`, `interrupt`, `approve`, `answer`, `recover`.
- `t3-fleet-agent` per-target agent invoked over local or SSH transport.
- Allowlisted fleet config format `t3-fleet.config.v1` with owner-only file permissions.
- Request and result JSON schemas `t3-fleet.request.v1` and `t3-fleet.result.v1`.
- `approval-required` as the default runtime mode for all mutating commands.
- Reference Windows service scripts under `scripts/`.
