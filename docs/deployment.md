# Deployment and upgrades

The package installs a controller and a target agent. It ships no fleet roster,
node credentials, service, timer, or provider fallback. Python 3.11 or newer is
required, with no third-party Python runtime dependencies.

## Before a real dispatch

1. Install the package in a reviewed Python environment on `controller-a` and
   each target. Use the same tested runtime revision across controllers and
   targets. Keep operator configuration outside the source checkout.
2. Install a compatible Brigade CLI separately on each node. Confirm
   `brigade fleet models --help` exposes the plan/admit contract. Have the
   operator enroll each node with its own scoped credential using Brigade's
   supported enrollment workflow. Never copy another node's identity.
3. Configure an enabled logical seat in the current Brigade roster for the
   `t3-fleet` consumer. Confirm its exact provider/model binding and native
   provider availability on the target. Use the operator's admitted seat, not
   the placeholder `example-seat`. T3 project defaults confer no authority.
4. Configure a reviewed fleet TOML file at mode `0600` on Unix. Set the local
   `controller_host`, target `expected_hostname`, reviewed executable paths,
   and repository paths. Use an owner-only state directory. On Windows,
   restrict its ACL before storing prompts or receipts there.
5. Start and authenticate the target's local T3 Code environment through its
   supported operator workflow. Register the exact configured checkout as a
   T3 project. Configure `t3_command`, `t3_url`, and any base/state directories
   only in the operator config. The controller invokes the agent over local
   process transport or a reviewed SSH alias. It never opens a remote T3 API.
6. Run scoped `t3-fleet doctor --host target-a --repository example-repo` and
   `t3-fleet capacity --host target-a`. Check per-host eligibility, compatible
   protocol, project registration, host `limit`, and `repository_limit`.
   These checks do not grant launch admission. Stop on a blocked target.

The supported T3 version gate accepts 0.0.30 through 0.0.40, 0.0.43, 0.0.45,
0.0.46, and 0.1.x, including prerelease/build suffixes. Protocol 2 applies to
0.0.46 and 0.1.x and requires descriptor protocol 2 and shell schema 2.
Unlisted versions require adapter verification before changing the gate.

The Windows launchers in `scripts/` are reference operator tools. Review their
paths, account permissions, and service configuration before use. Installation
is a separate operator action. Package installation starts no service.

## Choose the admission path

With policy enrollment off, supply an allowlisted host, repository, immutable
40-character lowercase revision, and logical `--seat`. Brigade must admit the
model on the controller and confirm it on the target before mutation. Missing
helpers, stale authority, malformed replies, and denials fail closed. An
installation without Brigade can use the offline config example and help, but
cannot dispatch a model.

With `[policy] enrollment = "required"`, enroll the controller and target and
configure each repository's `policy_identity`, such as
`github.com/example-org/example-repo`. The local alias remains `example-repo`.
The Hub owns routing, privacy rules, decisions, and reservations through the
Brigade CLI. The client has no direct Hub HTTP route. Prefer `--host auto` and
omit `--seat`. An explicit host or seat requires `--override-reason` and the
same admission. Never switch enrollment off to work around a denial.

Under enrollment, the controller plans a model. The target prepares and applies
the exact context, acknowledges its hash, then obtains launch admission. A
plan has `launch_authorized: false` and cannot authorize execution. Only the
target renews or releases its reservation. A terminal, expired, or released
reservation cannot resume through `followup`, `approve`, or `answer` without
new admission. The client does not invent a replacement reservation.

A parent Brigade run can supply `--delegation-id` for adoption of its existing
placement and reservation. Adoption requires enrollment on both nodes, uses the
receipt's exact repository and source revision, and never routes or reserves a
second time. It cannot be combined with `--allow-queue`.

## Optional queue and settlement

Queueing is disabled unless the operator enables `[outbox]`. A submission must
also opt in with `--allow-queue`. Tune the bounded settings in
`examples/fleet.example.toml`. Store its protected state locally. `t3-fleet tick`
drains already authorized work and renews this node's own target reservations.
An external operator-managed scheduler may call tick. The package installs no
daemon or schedule. See [recovery](recovery.md) for ambiguous acceptance.

Settlement is disabled unless `[settle] enabled = true`. `settle-sweep` and
`install-settle-timer` are operator-only CLI commands and are rejected by JSON
`invoke`. A sweep handles fleet threads terminal past the configured grace.
Passive settlement never contains live work. `unlink_pull_requests` is a
separate opt-in. An agent must not install a timer or enable settlement as a
routine dispatch step.

## Upgrade and rollback

Back up controller and target state before upgrading. SQLite lease schemas now
permit multiple active requests per repository. Keep `active_limit` low and set
`max_concurrent_per_repository = 1` if each repository needs a stricter cap.
The default `0` permits the host's capacity, with one isolated worktree per run.
`repository_lease_ttl_seconds` defaults to 21600.

Do not point an older runtime at upgraded concurrency state. If rollback is
necessary, drain active work first and review
`scripts/rollback-concurrency-schema.py --help`. The helper backs up state and
refuses conflicting live leases by default. A forced rollback can discard lease
records and needs a separate operator decision. Preserve unknown requests and
retained worktrees for recovery. Installing version 0.2.0 does not migrate a
live deployment or cut a package release.
