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

Interfaces: LaneStore(path) owns bind/get/claim/finish state. LaneClient(config,
host_id,state_dir) owns bind(payload), deliver(payload), context(payload),
compact(payload), attention(). CLI main(argv=None) returns an exit code and prints
one bounded JSON result. Actual helper signatures may follow existing house style.
all public input/output contracts and failure behaviors are fixed by the spec.

- [x] Write tests before each behavior and observe its expected missing-feature failure.
- [x] Implement binding, identity checks and local-only configuration first.
- [x] Implement durable dedupe and queue/steer delivery with stale-result refusal.
- [x] Implement exclusive context packet, Codex compaction sequence and attention.
- [x] Document all commands, JSON fields, statuses, partial outcomes and limits.
- [x] Run focused tests through Brigade, then all required repo checks.
- [x] Commit with conventional subject and verified co-author trailer.

## Task 2: Independent review and live proof

- [x] Review whole branch against the spec, including security and retry behavior.
- [x] Fix verified defects with regression tests and rerun relevant checks.
- [x] Prove disposable-lane delivery, duplicate suppression and stale-run/SHA refusal.
- [ ] Write durable handoff and complete authorized in-house PR gates.

## Initial evidence

Base f6bd1e2. Code graph synced before edits. `brigade code affected` for
controller.py, agent.py, cli.py and t3_api.py attributes tests/test_agent.py,
test_cli.py, test_controller.py, test_integration.py, test_lifecycle.py,
test_outbox.py, test_protocol_v2.py, test_reconciliation_contract.py,
test_settle.py, test_skill_package.py, test_store.py, test_t3_api.py. Only
t3_api.py remains in the intended existing-code impact set. New module edges
must be synced and rechecked after implementation. No historical Brigade claim.

## Task 1 implementation rulings and evidence

- Local work began at 1a26d16 with a clean tree. The initial all-tests receipt
  `20261026-023423-work-verify-6e8551` ran 1,088 tests with four missing generated
  projection errors and one documented cross-repository interoperability skip.
  The prescribed projection generator is required before the final suite.
- Missing-feature RED: `20261026-023740-work-verify-107f28` failed because the lane
  module did not exist. Subsequent regression RED receipts cover reservation/run
  races, compaction reference run advancement, unsent versus unknown status,
  Unicode serialization, output bounds, and malformed live/ownership state.
- Ruling: reserve an unknown operation before I/O and never take over or resend
  that operation automatically. Compaction persists each message's uncertainty
  before its individual send and retains the first acknowledgement across crashes.
- Ruling: compaction trigger admission is pinned to the inspected build
  `0.0.46-nightly.20261007.2787` with protocol 2 and a matching root Codex provider
  thread. Other readable builds retain a packet and report unsupported. Inspection
  used bounded embedded-source substrings: message schema near byte 151940709,
  slash routing near 162342803, and Codex compact adapter near 160663923. No live
  lane messages were sent.
- Ruling: context output requires an existing private parent and exclusive creation.
  Attention output is bounded to 64 KiB and reports omitted entries explicitly.
- Refreshed graph impact for lane.py, lane_store.py, and t3_api.py includes
  tests/test_lane.py, test_protocol_v2.py, test_reconciliation_contract.py,
  test_settle.py, test_skill_package.py, test_t3_api.py, test_agent.py, test_cli.py,
  and incoming agent/controller edges. These static edges provide a lower bound on impact. Dynamic coverage is unproven.
- Brigade receipts in this worktree are audit-only. The installed external skill
  guided the work, but its ID is absent from the target registry. Projection
  generation registered the t3-fleet skill. No verifier manifests are tracked.
  These captures report unknown attribution and cannot score an artifact.
- Independent review, PR work, and disposable-lane live proof remain Task 2 and
  belong to the parent. No install, deployment, external write, or lane message
  is part of Task 1 execution.

- Focused GREEN: `20261026-024746-work-verify-04e98d`, 24 tests, OK.
- Required generation: `20261026-024941-work-verify-cd4c8d`, exit 0.
- Required suite: `20261026-025021-work-verify-2424aa`, 1,112 tests,
  `OK (skipped=1)`. The skip is the explicit T3_FLEET_BRIGADE_SOURCE wire check.
- Required projection check: `20261026-025245-work-verify-af67fe`, exit 0.
- An optional receipt-index import reported SQLITE_BUSY and retained its export.
  Receipts remain available locally. No artifact scoring is claimed.

## Task 2 review-fix evidence

- Review fixes A-D were reproduced before implementation in
  `20261026-031925-work-verify-bb28dc` and passed together in
  `20261026-032023-work-verify-ce2a65` (5 focused tests). The first invocation
  had a missing `PYTHONPATH` (`20261026-031916-work-verify-f4103c`). It did not
  import the test module and is not RED evidence.
- Compaction now persists `unknown` after packet preparation and before support
  checks or sends. The interruption regression verifies no network call,
  `unknown` replay with both messages `not_sent`, exit 3, and attention visibility.
- Lane Git reads disable optional locks, fsmonitor and hooks, and remove inherited
  `GIT_*` environment overrides. A configured fsmonitor marker remains absent
  while context preparation still reads the bound HEAD and status.
- Lane construction now requires one case-insensitive hostname match and rejects
  ambiguous matches or a mismatched `--host` before T3 construction or state
  creation. A nonmatching alias with a missing alternate state database does not
  affect the unique match.
- The lane database now has a fixed 10,000-operation limit. Duplicate replay is
  checked before capacity, and concurrent reservations cannot exceed the cap.
  Bindings remain capped at 128.
- Final verification passed. Generation receipt: `20261026-032042-work-verify-682316`.
  Full suite receipt: `20261026-032048-work-verify-c65366` with 1,122 tests
  and one skip. Projection check receipt: `20261026-032200-work-verify-0b876e`.
  Vale receipt: `20261026-032216-work-verify-9c2bbc`. Diff check receipt:
  `20261026-032224-work-verify-1c65ee`. An earlier Vale run reported one
  semicolon in operator prose (`20261026-032206-work-verify-f7a620`). It was
  corrected before the passing Vale run.
- Remaining finding: parent is separately adjudicating Daybreak's unverified
  provider identity race in `/compact`. This task does not resolve or redesign
  that behavior. No live lane messages were sent and no commit was made.

## Review and live-proof status

Astra confirmed the interrupted-compaction fix at `1e3f630`. Daybreak's second
pass reviewed `f6bd1e2..1e3f630` and confirmed the Git-read, local-host, receipt-cap,
and interrupted-operation fixes without a new concrete blocker. Security approval
was conditional on the compaction queue decision, now accepted as described below.

A disposable lane acknowledged build and prerequisite events. Identical replay
returned the original receipt. Stale run and revision events were refused. Private
context preparation and attention worked. The inspected server recorded completed
Codex compaction before the security fixes. A fresh delivery/context check on the
hardened source also passed. Final wheel construction passed without dependencies
or network access. These are local audit checks, not scoreable artifacts.

The operator explicitly accepted ordinary T3 queue semantics on 2026-10-26,
with the limitation documented. The server snapshots model selection for queued
runs and can restore it when they start. No expected-provider/model compare-and-set
field exists. This is an accepted platform limitation, not a resolved atomicity
guarantee. The client issues no model or permission configuration commands.
The queue-related publication and installation hold is lifted.
