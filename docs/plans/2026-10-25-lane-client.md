# Local lane client implementation plan

- Goal: deliver the four approved local T3 client capabilities.
- Architecture: a separate `t3-lane` CLI reuses existing local config, session
  issuance, and bounded v2 transport. Its own durable state owns binding and retry
  identity. It never mutates fleet-managed threads.
- Tech: Python 3.11+, standard library, unittest.
- Spec: docs/specs/2026-10-25-lane-client.md.
- Global constraints: no dependencies, daemon, deployment, new thread launch,
  model changes, permission changes, fleet admission bypass, or raw log ingestion.
- Review focus: uncertain acceptance, concurrent duplicate IDs, workspace drift,
  filesystem privacy, partial compaction, and fleet-owned thread refusal.
- Execute tasks in order, recording checks and ruling decisions here.

## Files and ownership

- New src/t3_fleet/lane_store.py: durable bindings and event receipt state.
- New src/t3_fleet/lane.py: local client operations and CLI.
- Existing src/t3_fleet/t3_api.py: narrow queue/steer dispatch methods.
- Existing pyproject.toml: t3-lane entry point.
- New tests/test_lane.py: owner-boundary behavior and SQLite races.
- New docs/lane-client.md: operator use, JSON contracts and limitations.
- Existing README.md: link to local lane client guide.

## Task 1: Local client vertical slice

Follow src/t3_fleet/t3_api.py for bounded error/session behavior and
tests/test_t3_api.py for transport contract fixtures. Follow existing config loading.

Interfaces: LaneStore(path) owns bind/get/claim/finish state; LaneClient(config,
host_id,state_dir) owns bind(payload), deliver(payload), context(payload),
compact(payload), attention(). CLI main(argv=None) returns an exit code and prints
one bounded JSON result. Actual helper signatures may follow existing house style;
all public input/output contracts and failure behaviors are fixed by the spec.

- [ ] Write tests before each behavior and observe its expected missing-feature failure.
- [ ] Implement binding, identity checks and local-only configuration first.
- [ ] Implement durable dedupe and queue/steer delivery with stale-result refusal.
- [ ] Implement exclusive context packet, Codex compaction sequence and attention.
- [ ] Document all commands, JSON fields, statuses, partial outcomes and limits.
- [ ] Run focused tests through Brigade, then all required repo checks.
- [ ] Commit with conventional subject and verified co-author trailer.

## Task 2: Independent review and live proof

- [ ] Review whole branch against the spec, including security and retry behavior.
- [ ] Fix verified defects with regression tests and rerun relevant checks.
- [ ] Prove disposable-lane delivery, duplicate suppression and stale-run/SHA refusal.
- [ ] Write durable handoff and complete authorized in-house PR gates.

## Initial evidence

Base f6bd1e2. Code graph synced before edits. `brigade code affected` for
controller.py, agent.py, cli.py and t3_api.py attributes tests/test_agent.py,
test_cli.py, test_controller.py, test_integration.py, test_lifecycle.py,
test_outbox.py, test_protocol_v2.py, test_reconciliation_contract.py,
test_settle.py, test_skill_package.py, test_store.py, test_t3_api.py. Only
t3_api.py remains in the intended existing-code impact set. New module edges
must be synced and rechecked after implementation. No historical Brigade claim.
