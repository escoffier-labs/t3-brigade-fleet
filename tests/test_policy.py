from __future__ import annotations

import json
import subprocess
import unittest
from datetime import UTC, datetime

from t3_fleet.errors import (
    HostUnavailable,
    PolicyRejected,
    PolicyRevisionConflict,
    ValidationError,
)
from t3_fleet.policy import (
    BrigadeFleetPolicy,
    decision_identity,
    delegation_stamp,
    load_stored_prepared,
    normalize_policy_envelope,
    normalize_prepared_session,
    normalize_policy_receipt,
    normalize_policy_stamp,
    normalize_route_decision,
    policy_receipt,
    policy_stamp,
)

CANONICAL_REPO = "github.com/example-org/example-repo"

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
DIGEST = "sha256:" + "b" * 64

ENVELOPE = {
    "schema": "brigade.fleet_policy.v1",
    "version": 7,
    "digest": DIGEST,
    "effective": {
        "roles": {"impl": "muse13", "review": "claude_standby"},
        "execution": {"machine": "second-worker"},
        "data": {"allow_training": False},
    },
    "sources": {
        "roles.impl": {
            "layer": "repo",
            "value": "muse13",
            "chain": [{"layer": "fleet", "action": "set", "value": "muse13"}],
        }
    },
    "ack_required": True,
}

SESSION = "t3f-" + "0" * 32

ACK = {
    "schema": "brigade.fleet_policy_session.v1",
    "session_id": SESSION,
    "consumer": "t3-fleet",
    "repo_identity": "example-repo",
    "version": 7,
    "digest": "b" * 64,
    "state": "current",
    "loaded_at": "2026-09-05T11:59:30Z",
    "applied": True,
    # The frozen final field of this contract. The key is always present. It is
    # null only for a legacy resolve-only session that loaded no prepared
    # context and therefore cannot launch, which is what this fixture is.
    "context_hash": None,
}

RESERVATION = {
    "schema": "brigade.fleet_reservation.v1",
    "reservation_id": "reservation-1",
    "decision_id": "decision-1",
    "state": "active",
    "expires_at": "2026-09-05T12:10:00Z",
}

DECISION = {
    "schema": "brigade.fleet_route.v1",
    "decision_id": "decision-1",
    "policy_version": 7,
    "policy_digest": DIGEST,
    "selected": {"machine": "second-worker", "seat": "muse13"},
    "reason": "workload match",
    "candidates": [],
    "reservation_id": "reservation-1",
    "expires_at": "2026-09-05T12:10:00Z",
}


class FakeRunner:
    """Mirrors the subprocess call shape used by the model-admission adapter."""

    def __init__(self, *results: object) -> None:
        self.results = list(results)
        self.calls: list[list[str]] = []
        self.kwargs: list[dict[str, object]] = []

    def __call__(self, argv, **kwargs):
        self.calls.append(list(argv))
        self.kwargs.append(dict(kwargs))
        result = self.results.pop(0) if self.results else (0, "{}")
        if isinstance(result, BaseException):
            raise result
        code, stdout = result
        return subprocess.CompletedProcess(argv, code, stdout=stdout, stderr="")


def adapter(*results: object) -> tuple[BrigadeFleetPolicy, FakeRunner]:
    runner = FakeRunner(*results)
    return BrigadeFleetPolicy(runner=runner, now=lambda: NOW), runner


def flag(argv: list[str], name: str) -> str | None:
    return argv[argv.index(name) + 1] if name in argv else None


CONTEXT_HASH = "sha256:" + "c" * 64

PREPARED = {
    **ENVELOPE,
    "repo_identity": CANONICAL_REPO,
    "selected": {
        "seat": "muse13",
        "provider": "openai",
        "model": "gpt-5.6-terra",
        "launch_model": "gpt-5.6-terra-2026-08-30",
        "reasoning": "high",
        "instance_id": "codex",
    },
    "instructions": "Do not widen the seat, the machine, or the model.",
    "context_hash": CONTEXT_HASH,
}


class PreparedSessionTests(unittest.TestCase):
    """`session prepare` is the decision-scoped call that names a real selection.

    A plain resolve stays exactly as strict as it was: the prepared reply is
    parsed by its own reader, which reuses the envelope reader for the six keys
    a resolve returns and validates the three extra facts itself.
    """

    def prepare(self, *results: object) -> tuple[dict, list[str]]:
        policy, runner = adapter(*results)
        prepared = policy.prepare(
            consumer="t3-fleet",
            repository="/srv/repos/example",
            session_id=SESSION,
            origin="controller-host",
            provider="openai",
            model="gpt-5.6-terra-2026-08-30",
            instance_id="codex",
            reasoning="high",
            decision_id="decision-1",
        )
        return prepared, runner.calls[0]

    def test_prepare_names_the_decision_the_checkout_and_the_selection(self) -> None:
        _, argv = self.prepare((0, json.dumps(PREPARED)))
        self.assertEqual(
            argv[:5], ["brigade", "fleet", "policy", "session", "prepare"]
        )
        self.assertEqual(flag(argv, "--consumer"), "t3-fleet")
        # The actual configured checkout path, so Brigade's own resolver
        # normalizes it and answers about the repository that will really run.
        self.assertEqual(flag(argv, "--repo"), "/srv/repos/example")
        self.assertEqual(flag(argv, "--session-id"), SESSION)
        # The original validated origin, not this caller's own host.
        self.assertEqual(flag(argv, "--origin"), "controller-host")
        self.assertEqual(flag(argv, "--provider"), "openai")
        self.assertEqual(flag(argv, "--model"), "gpt-5.6-terra-2026-08-30")
        self.assertEqual(flag(argv, "--instance-id"), "codex")
        self.assertEqual(flag(argv, "--reasoning"), "high")
        self.assertEqual(flag(argv, "--decision-id"), "decision-1")
        self.assertEqual(argv[-1], "--json")

    def test_reasoning_is_optional(self) -> None:
        policy, runner = adapter((0, json.dumps(PREPARED)))
        policy.prepare(
            consumer="t3-fleet",
            repository="/srv/repos/example",
            session_id=SESSION,
            origin="controller-host",
            provider="openai",
            model="gpt-5.6-terra-2026-08-30",
            instance_id="codex",
            decision_id="decision-1",
        )
        self.assertNotIn("--reasoning", runner.calls[0])

    def test_the_reply_carries_the_envelope_and_the_three_extra_facts(self) -> None:
        prepared, _ = self.prepare((0, json.dumps(PREPARED)))
        self.assertEqual(prepared["version"], 7)
        self.assertEqual(prepared["digest"], DIGEST)
        self.assertEqual(prepared["repo_identity"], CANONICAL_REPO)
        # Canonical roster identity and native launch identity stay separate.
        self.assertEqual(prepared["selected"]["model"], "gpt-5.6-terra")
        self.assertEqual(
            prepared["selected"]["launch_model"], "gpt-5.6-terra-2026-08-30"
        )
        self.assertEqual(prepared["context_hash"], CONTEXT_HASH)
        self.assertEqual(prepared["instructions"], PREPARED["instructions"])
        # The canonical rules and their provenance survive intact.
        self.assertEqual(prepared["sources"]["roles.impl"]["value"], "muse13")

    def test_an_incomplete_prepared_reply_is_refused(self) -> None:
        for missing in ("repo_identity", "selected", "instructions"):
            with self.subTest(missing=missing):
                payload = {key: item for key, item in PREPARED.items() if key != missing}
                with self.assertRaisesRegex(HostUnavailable, "prepared session"):
                    self.prepare((0, json.dumps(payload)))

    def test_an_incomplete_selection_is_refused(self) -> None:
        for missing in ("seat", "provider", "model", "reasoning", "instance_id"):
            with self.subTest(missing=missing):
                selection = {
                    key: item
                    for key, item in PREPARED["selected"].items()
                    if key != missing
                }
                payload = {**PREPARED, "selected": selection}
                with self.assertRaisesRegex(HostUnavailable, "prepared session"):
                    self.prepare((0, json.dumps(payload)))

    def test_unknown_fields_are_refused_everywhere(self) -> None:
        for payload in (
            {**PREPARED, "launch_authorized": True},
            {**PREPARED, "selected": {**PREPARED["selected"], "token": "placeholder"}},
        ):
            with self.subTest(payload=sorted(payload)):
                with self.assertRaisesRegex(HostUnavailable, "prepared session"):
                    self.prepare((0, json.dumps(payload)))

    def test_instructions_are_bounded(self) -> None:
        for instructions in ("", "x" * 8193, 7):
            with self.subTest(instructions=repr(instructions)[:20]):
                payload = {**PREPARED, "instructions": instructions}
                with self.assertRaisesRegex(HostUnavailable, "prepared session"):
                    self.prepare((0, json.dumps(payload)))

    def test_a_prepared_repo_identity_may_not_carry_a_credential(self) -> None:
        payload = {
            **PREPARED,
            "repo_identity": "operator:placeholder@github.com/example-org/example-repo",  # <!-- content-guard: allow pii/email -->
        }
        with self.assertRaisesRegex(HostUnavailable, "prepared session"):
            self.prepare((0, json.dumps(payload)))

    def test_a_selection_may_not_carry_shell_syntax(self) -> None:
        for model in ("gpt-5.6 terra", "gpt-5.6;rm", "$MODEL"):
            with self.subTest(model=model):
                payload = {**PREPARED, "selected": {**PREPARED["selected"], "model": model}}
                with self.assertRaisesRegex(HostUnavailable, "prepared session"):
                    self.prepare((0, json.dumps(payload)))

    def test_a_selection_argument_may_not_carry_shell_syntax(self) -> None:
        policy, runner = adapter((0, json.dumps(PREPARED)))
        with self.assertRaises(ValidationError):
            policy.prepare(
                consumer="t3-fleet",
                repository="/srv/repos/example",
                session_id=SESSION,
                origin="controller-host",
                provider="openai",
                model="gpt-5.6-terra; rm -rf /",
                instance_id="codex",
                decision_id="decision-1",
            )
        self.assertEqual(runner.calls, [])

    def test_plain_resolve_stays_strict_about_the_extra_keys(self) -> None:
        # Adding `prepare` must not widen `resolve`: a resolve that came back
        # with a prepared session's extra keys is still refused.
        policy, _ = adapter((0, json.dumps(PREPARED)))
        with self.assertRaisesRegex(HostUnavailable, "policy envelope"):
            policy.resolve(
                consumer="t3-fleet",
                repository=CANONICAL_REPO,
                session_id=SESSION,
                origin="controller-host",
            )

    def test_a_prepared_session_is_revalidated_at_the_point_of_use(self) -> None:
        prepared, _ = self.prepare((0, json.dumps(PREPARED)))
        self.assertEqual(load_stored_prepared(prepared), prepared)
        for mutation in (
            {"launch_authorized": True},
            {"repo_identity": "https://github.com/example-org/example-repo"},
            {"instructions": ""},
        ):
            with self.subTest(mutation=sorted(mutation)):
                with self.assertRaises(ValidationError):
                    load_stored_prepared({**prepared, **mutation})

    def test_a_bare_envelope_is_not_a_prepared_session(self) -> None:
        with self.assertRaises(ValidationError):
            normalize_prepared_session(ENVELOPE)


class NormalizationTests(unittest.TestCase):
    def test_policy_envelope_normalizes_known_fields(self) -> None:
        envelope = normalize_policy_envelope(ENVELOPE)
        self.assertEqual(envelope["version"], 7)
        self.assertEqual(envelope["digest"], DIGEST)
        self.assertEqual(envelope["roles"], {"impl": "muse13", "review": "claude_standby"})
        self.assertEqual(envelope["execution"], {"machine": "second-worker"})
        self.assertEqual(envelope["data"], {"allow_training": False})
        self.assertTrue(envelope["ack_required"])
        self.assertEqual(
            envelope["sources"]["roles.impl"]["chain"][0]["action"], "set"
        )
        self.assertEqual(envelope["sources"]["roles.impl"]["value"], "muse13")

    def test_policy_envelope_rejects_unknown_and_invalid_fields(self) -> None:
        for mutation in (
            {"schema": "brigade.fleet_policy.v2"},
            {"version": 0},
            {"version": True},
            {"digest": "b" * 64},
            {"ack_required": False},
            {"extra": "field"},
            {"effective": {"roles": {}, "execution": {}}},
            {
                "effective": {
                    "roles": {"impl": "muse13"},
                    "execution": {},
                    "data": {"allow_training": "no"},
                }
            },
        ):
            with self.subTest(mutation=sorted(mutation)):
                with self.assertRaises(ValidationError):
                    normalize_policy_envelope({**ENVELOPE, **mutation})

    def test_route_decision_normalizes_and_flattens_selection(self) -> None:
        decision = normalize_route_decision(DECISION)
        self.assertEqual(decision["decision_id"], "decision-1")
        self.assertEqual(decision["policy_version"], 7)
        self.assertEqual(decision["policy_digest"], DIGEST)
        self.assertEqual(decision["machine"], "second-worker")
        self.assertEqual(decision["seat"], "muse13")
        self.assertEqual(decision["reservation_id"], "reservation-1")
        self.assertEqual(decision["expires_at"], "2026-09-05T12:10:00Z")

    def test_route_decision_rejects_unknown_and_invalid_fields(self) -> None:
        for mutation in (
            {"schema": "brigade.fleet_route.v2"},
            {"decision_id": ""},
            {"policy_version": -1},
            {"policy_digest": "nope"},
            {"selected": {"machine": "second-worker"}},
            {"selected": {"machine": "second worker", "seat": "muse13"}},
            {"reservation_id": ""},
            {"expires_at": "2026-09-05 12:10:00"},
            {"expires_at": "2026-09-05T12:10:00"},
            {"candidates": {}},
            {"extra": "field"},
        ):
            with self.subTest(mutation=sorted(mutation)):
                with self.assertRaises(ValidationError):
                    normalize_route_decision({**DECISION, **mutation})

    def test_decision_identity_excludes_the_ephemeral_expiry(self) -> None:
        first = normalize_route_decision(DECISION)
        later = normalize_route_decision(
            {**DECISION, "expires_at": "2026-09-05T12:11:00Z", "reason": "re-reserved"}
        )
        self.assertNotEqual(first["expires_at"], later["expires_at"])
        self.assertEqual(decision_identity(first), decision_identity(later))
        self.assertNotIn("expires_at", decision_identity(first))

    def test_policy_stamp_carries_safe_metadata_only(self) -> None:
        stamp = policy_stamp(
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
            workload="linux-development",
            repo_identity=CANONICAL_REPO,
            envelope=normalize_policy_envelope(ENVELOPE),
            decision=normalize_route_decision(DECISION),
        )
        self.assertEqual(
            set(stamp),
            {
                "schema",
                "session_id",
                "origin",
                "workload",
                "repo_identity",
                "version",
                "digest",
                "decision_id",
                "reservation_id",
                "machine",
                "seat",
                "expires_at",
            },
        )
        self.assertEqual(stamp["repo_identity"], CANONICAL_REPO)
        self.assertEqual(normalize_policy_stamp(stamp), stamp)
        receipt = policy_receipt(stamp, acknowledged=True)
        self.assertTrue(receipt["acknowledged"])
        self.assertNotIn("origin", receipt)
        # The receipt carries the canonical identity so a resumed request can be
        # revalidated against the same repository it was admitted for.
        self.assertEqual(receipt["repo_identity"], CANONICAL_REPO)

    def test_policy_stamp_requires_a_canonical_repo_identity(self) -> None:
        stamp = policy_stamp(
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
            workload="linux-development",
            repo_identity=CANONICAL_REPO,
            envelope=normalize_policy_envelope(ENVELOPE),
            decision=normalize_route_decision(DECISION),
        )
        missing = {key: value for key, value in stamp.items() if key != "repo_identity"}
        with self.assertRaises(ValidationError):
            normalize_policy_stamp(missing)
        for bad in ("", "repo; rm -rf /", "x" * 300, 7):
            with self.subTest(repo_identity=bad):
                with self.assertRaises(ValidationError):
                    normalize_policy_stamp({**stamp, "repo_identity": bad})

    def test_a_legacy_receipt_without_a_canonical_identity_still_reads(self) -> None:
        # Receipts written before canonical identity landed stay readable; the
        # continuation path is what refuses to resume them under enrollment.
        stamp = policy_stamp(
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
            workload="linux-development",
            repo_identity=CANONICAL_REPO,
            envelope=normalize_policy_envelope(ENVELOPE),
            decision=normalize_route_decision(DECISION),
        )
        receipt = policy_receipt(stamp, acknowledged=True)
        legacy = {key: value for key, value in receipt.items() if key != "repo_identity"}
        self.assertIsNone(normalize_policy_receipt(legacy)["repo_identity"])

    def test_policy_stamp_rejects_unknown_or_malformed_fields(self) -> None:
        stamp = policy_stamp(
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
            workload="linux-development",
            repo_identity=CANONICAL_REPO,
            envelope=normalize_policy_envelope(ENVELOPE),
            decision=normalize_route_decision(DECISION),
        )
        for mutation in (
            {"schema": "t3-fleet.fleet-policy.v2"},
            {"session_id": ""},
            {"version": 0},
            {"digest": "b" * 64},
            {"machine": "second worker"},
            {"expires_at": "later"},
            {"prompt": "leaked"},
        ):
            with self.subTest(mutation=sorted(mutation)):
                with self.assertRaises(ValidationError):
                    normalize_policy_stamp({**stamp, **mutation})


class AdapterTests(unittest.TestCase):
    def test_resolve_builds_the_documented_argv(self) -> None:
        policy, runner = adapter((0, json.dumps(ENVELOPE)))
        envelope = policy.resolve(
            consumer="t3-fleet",
            repository="example-repo",
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
        )
        self.assertEqual(envelope["version"], 7)
        self.assertEqual(
            runner.calls[0],
            [
                "brigade",
                "fleet",
                "policy",
                "resolve",
                "--consumer",
                "t3-fleet",
                "--repo",
                "example-repo",
                "--session-id",
                "t3f-" + "0" * 32,
                "--origin",
                "linux-worker",
                "--json",
            ],
        )
        self.assertIsNone(runner.kwargs[0]["input"])

    def test_plain_resolve_carries_no_decision_id(self) -> None:
        # The controller's own resolve is unchanged: it has no decision yet.
        policy, runner = adapter((0, json.dumps(ENVELOPE)))
        policy.resolve(
            consumer="t3-fleet",
            repository="example-repo",
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
        )
        self.assertNotIn("--decision-id", runner.calls[0])

    def test_resolve_forwards_a_decision_id_when_one_is_given(self) -> None:
        policy, runner = adapter((0, json.dumps(ENVELOPE)))
        policy.resolve(
            consumer="t3-fleet",
            repository="example-repo",
            session_id="t3f-" + "0" * 32,
            origin="controller-host",
            decision_id="decision-1",
        )
        self.assertEqual(flag(runner.calls[0], "--decision-id"), "decision-1")

    def test_resolve_rejects_a_malformed_decision_id(self) -> None:
        policy, runner = adapter((0, json.dumps(ENVELOPE)))
        with self.assertRaises(ValidationError):
            policy.resolve(
                consumer="t3-fleet",
                repository="example-repo",
                session_id="t3f-" + "0" * 32,
                origin="controller-host",
                decision_id="decision one",
            )
        self.assertEqual(runner.calls, [])

    def test_resolve_accepts_a_canonical_repository_identity(self) -> None:
        # The Hub is addressed by canonical identity, which carries separators a
        # local alias never does.
        policy, runner = adapter((0, json.dumps(ENVELOPE)))
        policy.resolve(
            consumer="t3-fleet",
            repository="github.com/example-org/example-repo",
            session_id="t3f-" + "0" * 32,
            origin="controller-host",
        )
        self.assertEqual(
            flag(runner.calls[0], "--repo"), "github.com/example-org/example-repo"
        )

    def test_resolve_accepts_an_actual_checkout_path(self) -> None:
        # A target proves which checkout it holds by naming the path; Brigade
        # normalizes it to the canonical identity on its side.
        policy, runner = adapter((0, json.dumps(ENVELOPE)))
        policy.resolve(
            consumer="t3-fleet",
            repository="/srv/repos/example",
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
            decision_id="decision-1",
        )
        self.assertEqual(flag(runner.calls[0], "--repo"), "/srv/repos/example")

    def test_a_credential_shaped_repository_argument_is_rejected(self) -> None:
        policy, runner = adapter((0, json.dumps(ENVELOPE)))
        for repository in (
            "operator:hunter2@github.com/example-org/example-repo",  # <!-- content-guard: allow pii/email -->
            "https://github.com/example-org/example-repo",
        ):
            with self.subTest(repository=repository):
                with self.assertRaises(ValidationError):
                    policy.resolve(
                        consumer="t3-fleet",
                        repository=repository,
                        session_id="t3f-" + "0" * 32,
                        origin="controller-host",
                    )
        self.assertEqual(runner.calls, [])

    def test_a_repository_merely_named_like_a_credential_is_accepted(self) -> None:
        # The guard is about credential syntax, not credential-sounding words.
        # A real repository called `tokenjuice` must still dispatch.
        policy, runner = adapter((0, json.dumps(ENVELOPE)))
        policy.resolve(
            consumer="t3-fleet",
            repository="github.com/example-org/tokenjuice",
            session_id="t3f-" + "0" * 32,
            origin="controller-host",
        )
        self.assertEqual(
            flag(runner.calls[0], "--repo"), "github.com/example-org/tokenjuice"
        )

    def test_resolve_rejects_a_shell_unsafe_repository_argument(self) -> None:
        policy, runner = adapter((0, json.dumps(ENVELOPE)))
        for repository in ("repo; rm -rf /", "repo\nnext", "x" * 300, ""):
            with self.subTest(repository=repository):
                with self.assertRaises(ValidationError):
                    policy.resolve(
                        consumer="t3-fleet",
                        repository=repository,
                        session_id="t3f-" + "0" * 32,
                        origin="linux-worker",
                    )
        self.assertEqual(runner.calls, [])

    def test_route_forwards_workload_machine_and_override_reason(self) -> None:
        policy, runner = adapter((0, json.dumps(DECISION)))
        policy.route(
            consumer="t3-fleet",
            repository="example-repo",
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
            workload="linux-development",
            machine="second-worker",
            override_reason="operator pinned the worker",
        )
        argv = runner.calls[0]
        self.assertEqual(argv[:4], ["brigade", "fleet", "policy", "route"])
        self.assertEqual(flag(argv, "--workload"), "linux-development")
        self.assertEqual(flag(argv, "--machine"), "second-worker")
        self.assertEqual(flag(argv, "--override-reason"), "operator pinned the worker")
        self.assertEqual(flag(argv, "--origin"), "linux-worker")
        self.assertNotIn("--decision-id", argv)
        self.assertEqual(argv[-1], "--json")

    def test_route_forwards_a_negotiated_seat_override(self) -> None:
        policy, runner = adapter((0, json.dumps(DECISION)))
        policy.route(
            consumer="t3-fleet",
            repository="example-repo",
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
            workload="linux-development",
            seat="muse13",
            override_reason="operator pinned the review seat",
        )
        argv = runner.calls[0]
        self.assertEqual(flag(argv, "--seat"), "muse13")
        self.assertEqual(
            flag(argv, "--override-reason"), "operator pinned the review seat"
        )
        self.assertNotIn("--machine", argv)

    def test_route_rejects_an_invalid_seat(self) -> None:
        policy, runner = adapter((0, json.dumps(DECISION)))
        with self.assertRaises(ValidationError):
            policy.route(
                consumer="t3-fleet",
                repository="example-repo",
                session_id="t3f-" + "0" * 32,
                origin="linux-worker",
                workload="linux-development",
                seat="muse 13; rm -rf /",
                override_reason="operator pinned the review seat",
            )
        self.assertEqual(runner.calls, [])

    def test_route_omits_optional_flags_for_auto_selection(self) -> None:
        policy, runner = adapter((0, json.dumps(DECISION)))
        policy.route(
            consumer="t3-fleet",
            repository="example-repo",
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
            workload="linux-development",
        )
        self.assertNotIn("--machine", runner.calls[0])
        self.assertNotIn("--seat", runner.calls[0])
        self.assertNotIn("--override-reason", runner.calls[0])

    def test_target_validation_replays_the_route_with_decision_id(self) -> None:
        policy, runner = adapter((0, json.dumps(DECISION)))
        policy.route(
            consumer="t3-fleet",
            repository="example-repo",
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
            workload="linux-development",
            machine="second-worker",
            decision_id="decision-1",
        )
        argv = runner.calls[0]
        self.assertEqual(flag(argv, "--decision-id"), "decision-1")
        self.assertEqual(flag(argv, "--machine"), "second-worker")

    def test_acknowledge_preserves_the_canonical_policy_digest(self) -> None:
        policy, runner = adapter((0, json.dumps(ACK)))
        record = policy.acknowledge(
            session_id="t3f-" + "0" * 32,
            consumer="t3-fleet",
            repository="example-repo",
            version=7,
            digest=DIGEST,
        )
        argv = runner.calls[0]
        self.assertEqual(
            argv[:5], ["brigade", "fleet", "policy", "session", "acknowledge"]
        )
        self.assertEqual(flag(argv, "--session-id"), "t3f-" + "0" * 32)
        self.assertEqual(flag(argv, "--consumer"), "t3-fleet")
        self.assertEqual(flag(argv, "--repo"), "example-repo")
        self.assertEqual(flag(argv, "--version"), "7")
        self.assertEqual(flag(argv, "--digest"), DIGEST)
        self.assertEqual(argv[-1], "--json")
        self.assertEqual(record["state"], "current")
        self.assertTrue(record["applied"])
        self.assertEqual(record["repo_identity"], "example-repo")
        self.assertEqual(record["version"], 7)
        self.assertEqual(record["digest"], DIGEST)
        # A resolve-only session sends no digest, so the mandatory key comes
        # back null rather than absent, and no flag is spelled on the way out.
        self.assertIsNone(record["context_hash"])
        self.assertNotIn("--context-hash", argv)

    def test_acknowledge_requires_the_full_session_contract(self) -> None:
        """A bare positive flag is not a Fleet Hub session acknowledgement."""
        for payload in (
            {"acknowledged": True},
            {"acknowledged": True, "session_id": SESSION, "version": 7},
            {key: value for key, value in ACK.items() if key != "applied"},
            {key: value for key, value in ACK.items() if key != "state"},
            {key: value for key, value in ACK.items() if key != "loaded_at"},
            {key: value for key, value in ACK.items() if key != "repo_identity"},
            {key: value for key, value in ACK.items() if key != "digest"},
            # The frozen final field is mandatory in the reply. An adapter that
            # simply omits it is incomplete rather than legacy-compatible.
            {key: value for key, value in ACK.items() if key != "context_hash"},
        ):
            with self.subTest(payload=sorted(payload)):
                policy, _ = adapter((0, json.dumps(payload)))
                with self.assertRaises((PolicyRejected, HostUnavailable)):
                    policy.acknowledge(
                        session_id=SESSION,
                        consumer="t3-fleet",
                        repository="example-repo",
                        version=7,
                        digest=DIGEST,
                    )

    def test_acknowledge_rejects_a_mismatched_or_negative_response(self) -> None:
        for mutation in (
            {"applied": False},
            {"applied": "true"},
            {"state": "stale"},
            {"state": "pending"},
            {"version": 8},
            {"digest": "c" * 64},
            {"session_id": "t3f-" + "1" * 32},
            {"consumer": "brigade"},
            {"repo_identity": "other-repo"},
            {"schema": "brigade.fleet_policy_session.v2"},
            {"schema": "brigade.fleet_policy_ack.v1"},
            {"loaded_at": "2026-09-05T11:00:00Z"},
            {"loaded_at": "2026-09-05T12:05:00Z"},
            {"loaded_at": "2026-09-05 11:59:30"},
            {"acknowledged": True},
        ):
            with self.subTest(mutation=sorted(mutation)):
                policy, _ = adapter((0, json.dumps({**ACK, **mutation})))
                with self.assertRaises((PolicyRejected, HostUnavailable)):
                    policy.acknowledge(
                        session_id=SESSION,
                        consumer="t3-fleet",
                        repository="example-repo",
                        version=7,
                        digest=DIGEST,
                    )

    def test_acknowledging_a_changed_policy_is_a_revision_conflict(self) -> None:
        policy, _ = adapter(
            (
                1,
                json.dumps(
                    {"error": {"code": "policy_version_conflict", "message": "stale"}}
                ),
            )
        )
        with self.assertRaises(PolicyRevisionConflict):
            policy.acknowledge(
                session_id=SESSION,
                consumer="t3-fleet",
                repository="example-repo",
                version=7,
                digest=DIGEST,
            )

    def test_missing_helper_fails_closed(self) -> None:
        policy, _ = adapter(FileNotFoundError("brigade"))
        with self.assertRaises(HostUnavailable):
            policy.resolve(
                consumer="t3-fleet",
                repository="example-repo",
                session_id="t3f-" + "0" * 32,
                origin="linux-worker",
            )

    def test_timeout_fails_closed(self) -> None:
        policy, _ = adapter(subprocess.TimeoutExpired(["brigade"], 15))
        with self.assertRaises(HostUnavailable):
            policy.resolve(
                consumer="t3-fleet",
                repository="example-repo",
                session_id="t3f-" + "0" * 32,
                origin="linux-worker",
            )

    def test_invalid_json_fails_closed(self) -> None:
        for stdout in ("not json", "[]", "", json.dumps({"schema": "wrong"})):
            with self.subTest(stdout=stdout):
                policy, _ = adapter((0, stdout))
                with self.assertRaises(HostUnavailable):
                    policy.resolve(
                        consumer="t3-fleet",
                        repository="example-repo",
                        session_id="t3f-" + "0" * 32,
                        origin="linux-worker",
                    )

    def test_oversize_response_fails_closed(self) -> None:
        payload = json.dumps({**ENVELOPE, "pad": "x" * (512 * 1024)})
        policy, _ = adapter((0, payload))
        with self.assertRaises(HostUnavailable):
            policy.resolve(
                consumer="t3-fleet",
                repository="example-repo",
                session_id="t3f-" + "0" * 32,
                origin="linux-worker",
            )

    def test_denial_exit_code_is_policy_rejected(self) -> None:
        policy, _ = adapter(
            (3, json.dumps({"error": {"code": "policy_denied", "message": "denied"}}))
        )
        with self.assertRaises(PolicyRejected):
            policy.route(
                consumer="t3-fleet",
                repository="example-repo",
                session_id="t3f-" + "0" * 32,
                origin="linux-worker",
                workload="linux-development",
            )

    def test_denial_error_envelope_wins_over_a_generic_exit_code(self) -> None:
        policy, _ = adapter(
            (1, json.dumps({"error": {"code": "policy_denied", "message": "denied"}}))
        )
        with self.assertRaises(PolicyRejected):
            policy.route(
                consumer="t3-fleet",
                repository="example-repo",
                session_id="t3f-" + "0" * 32,
                origin="linux-worker",
                workload="linux-development",
            )

    def test_version_conflict_is_a_revision_conflict(self) -> None:
        policy, _ = adapter(
            (
                1,
                json.dumps(
                    {"error": {"code": "policy_version_conflict", "message": "stale"}}
                ),
            )
        )
        with self.assertRaises(PolicyRevisionConflict):
            policy.route(
                consumer="t3-fleet",
                repository="example-repo",
                session_id="t3f-" + "0" * 32,
                origin="linux-worker",
                workload="linux-development",
            )

    def test_hub_hyphenated_errors_keep_their_failure_class(self) -> None:
        for code, expected in (("revision-conflict", PolicyRevisionConflict),
                               ("auth-failed", PolicyRejected)):
            with self.subTest(code=code):
                policy, _ = adapter((1, json.dumps({"error": {"code": code}})))
                with self.assertRaises(expected):
                    policy.route(consumer="t3-fleet", repository="example-repo",
                                 session_id=SESSION, origin="linux-worker",
                                 workload="linux-development")

    def test_unmapped_nonzero_exit_fails_closed(self) -> None:
        policy, _ = adapter((9, ""))
        with self.assertRaises(HostUnavailable):
            policy.route(
                consumer="t3-fleet",
                repository="example-repo",
                session_id="t3f-" + "0" * 32,
                origin="linux-worker",
                workload="linux-development",
            )

    def test_expired_decision_is_rejected(self) -> None:
        policy, _ = adapter(
            (0, json.dumps({**DECISION, "expires_at": "2026-09-05T11:59:00Z"}))
        )
        with self.assertRaises(PolicyRejected):
            policy.route(
                consumer="t3-fleet",
                repository="example-repo",
                session_id="t3f-" + "0" * 32,
                origin="linux-worker",
                workload="linux-development",
            )

    def test_decision_lifetime_is_bounded(self) -> None:
        policy, _ = adapter(
            (0, json.dumps({**DECISION, "expires_at": "2026-09-07T12:00:00Z"}))
        )
        with self.assertRaises(PolicyRejected):
            policy.route(
                consumer="t3-fleet",
                repository="example-repo",
                session_id="t3f-" + "0" * 32,
                origin="linux-worker",
                workload="linux-development",
            )

    def test_override_reason_is_bounded_and_shell_safe(self) -> None:
        for reason in ("", "x" * 300, "pin; rm -rf /", "token=abcd1234"):
            with self.subTest(reason=reason[:16]):
                policy, runner = adapter((0, json.dumps(DECISION)))
                with self.assertRaises(ValidationError):
                    policy.route(
                        consumer="t3-fleet",
                        repository="example-repo",
                        session_id="t3f-" + "0" * 32,
                        origin="linux-worker",
                        workload="linux-development",
                        machine="second-worker",
                        override_reason=reason,
                    )
                self.assertEqual(runner.calls, [])

    def test_identifiers_are_validated_before_the_helper_runs(self) -> None:
        policy, runner = adapter((0, json.dumps(ENVELOPE)))
        with self.assertRaises(ValidationError):
            policy.resolve(
                consumer="not-t3-fleet",
                repository="example-repo",
                session_id="t3f-" + "0" * 32,
                origin="linux-worker",
            )
        with self.assertRaises(ValidationError):
            policy.resolve(
                consumer="t3-fleet",
                repository="example repo",
                session_id="t3f-" + "0" * 32,
                origin="linux-worker",
            )
        self.assertEqual(runner.calls, [])

class ReservationLifecycleTests(unittest.TestCase):
    """The lifecycle adapter is a fencing client, not a fencing authority.

    Ownership and generation are decided at the Hub. These tests pin what the
    client refuses to believe about the reply it gets back.
    """

    def test_renew_builds_the_documented_argv(self) -> None:
        policy, runner = adapter((0, json.dumps(RESERVATION)))
        record = policy.renew_reservation(
            reservation_id="reservation-1",
            decision_id="decision-1",
            session_id=SESSION,
        )
        self.assertEqual(
            runner.calls[0],
            [
                "brigade",
                "fleet",
                "policy",
                "reservation",
                "renew",
                "--reservation-id",
                "reservation-1",
                "--decision-id",
                "decision-1",
                "--session-id",
                SESSION,
                "--json",
            ],
        )
        self.assertEqual(record["state"], "active")
        self.assertEqual(record["expires_at"], "2026-09-05T12:10:00Z")
        self.assertIsNone(runner.kwargs[0]["input"])

    def test_release_builds_the_documented_argv(self) -> None:
        policy, runner = adapter(
            (0, json.dumps({**RESERVATION, "state": "released"}))
        )
        record = policy.release_reservation(
            reservation_id="reservation-1",
            decision_id="decision-1",
            session_id=SESSION,
        )
        self.assertEqual(runner.calls[0][:5], [
            "brigade",
            "fleet",
            "policy",
            "reservation",
            "release",
        ])
        self.assertEqual(record["state"], "released")

    def test_renew_cannot_resurrect_a_released_or_expired_reservation(self) -> None:
        for payload in (
            {"state": "released"},
            {"expires_at": "2026-09-05T11:59:00Z"},
            {"expires_at": "2026-09-07T12:00:00Z"},
        ):
            with self.subTest(payload=sorted(payload)):
                policy, _ = adapter((0, json.dumps({**RESERVATION, **payload})))
                with self.assertRaises(PolicyRejected):
                    policy.renew_reservation(
                        reservation_id="reservation-1",
                        decision_id="decision-1",
                        session_id=SESSION,
                    )

    def test_release_that_still_reports_active_is_rejected(self) -> None:
        policy, _ = adapter((0, json.dumps(RESERVATION)))
        with self.assertRaises(PolicyRejected):
            policy.release_reservation(
                reservation_id="reservation-1",
                decision_id="decision-1",
                session_id=SESSION,
            )

    def test_reply_for_another_reservation_or_decision_is_rejected(self) -> None:
        for payload in (
            {"reservation_id": "reservation-2"},
            {"decision_id": "decision-2"},
        ):
            with self.subTest(payload=sorted(payload)):
                policy, _ = adapter((0, json.dumps({**RESERVATION, **payload})))
                with self.assertRaises(PolicyRejected):
                    policy.renew_reservation(
                        reservation_id="reservation-1",
                        decision_id="decision-1",
                        session_id=SESSION,
                    )

    def test_malformed_reply_fails_closed(self) -> None:
        for payload in (
            {"schema": "brigade.fleet_reservation.v2"},
            {"state": "unknown"},
            {"extra": "field"},
            {"expires_at": "2026-09-05 12:10:00"},
        ):
            with self.subTest(payload=sorted(payload)):
                policy, _ = adapter((0, json.dumps({**RESERVATION, **payload})))
                with self.assertRaises((HostUnavailable, ValidationError)):
                    policy.renew_reservation(
                        reservation_id="reservation-1",
                        decision_id="decision-1",
                        session_id=SESSION,
                    )

    def test_missing_field_fails_closed(self) -> None:
        for key in ("reservation_id", "decision_id", "state", "expires_at"):
            with self.subTest(key=key):
                payload = {k: v for k, v in RESERVATION.items() if k != key}
                policy, _ = adapter((0, json.dumps(payload)))
                with self.assertRaises((HostUnavailable, ValidationError)):
                    policy.renew_reservation(
                        reservation_id="reservation-1",
                        decision_id="decision-1",
                        session_id=SESSION,
                    )

    def test_transport_ambiguity_never_reads_as_released(self) -> None:
        for failure in (
            FileNotFoundError("brigade"),
            subprocess.TimeoutExpired(["brigade"], 15),
        ):
            with self.subTest(failure=type(failure).__name__):
                policy, _ = adapter(failure)
                with self.assertRaises(HostUnavailable):
                    policy.release_reservation(
                        reservation_id="reservation-1",
                        decision_id="decision-1",
                        session_id=SESSION,
                    )

    def test_identifiers_are_validated_before_the_helper_runs(self) -> None:
        policy, runner = adapter((0, json.dumps(RESERVATION)))
        with self.assertRaises(ValidationError):
            policy.renew_reservation(
                reservation_id="reservation 1",
                decision_id="decision-1",
                session_id=SESSION,
            )
        with self.assertRaises(ValidationError):
            policy.release_reservation(
                reservation_id="reservation-1",
                decision_id="decision-1",
                session_id="session id",
            )
        self.assertEqual(runner.calls, [])


class HelperArgumentTests(unittest.TestCase):
    def test_helper_arguments_never_carry_prompt_text(self) -> None:
        policy, runner = adapter(
            (0, json.dumps(ENVELOPE)),
            (0, json.dumps(DECISION)),
            (0, json.dumps(ACK)),
        )
        policy.resolve(
            consumer="t3-fleet",
            repository="example-repo",
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
        )
        policy.route(
            consumer="t3-fleet",
            repository="example-repo",
            session_id="t3f-" + "0" * 32,
            origin="linux-worker",
            workload="linux-development",
        )
        policy.acknowledge(
            session_id="t3f-" + "0" * 32,
            consumer="t3-fleet",
            repository="example-repo",
            version=7,
            digest=DIGEST,
        )
        for argv, kwargs in zip(runner.calls, runner.kwargs):
            self.assertIsNone(kwargs["input"])
            for item in argv:
                self.assertNotIn("private prompt", item)
                self.assertLessEqual(len(item), 256)


DELEGATION_RECEIPT = {
    "schema": "brigade.fleet_policy_delegation.v1",
    "delegation_id": "del-8f2c1a",
    "launch_authorized": False,
    "consumer": "brigade-run",
    "adapter": "t3-fleet",
    "parent_request_id": "run-7a1c9e",
    "decision_id": "decision-1",
    "reservation_id": "reservation-1",
    "session_id": SESSION,
    "repo_identity": "git.example.invalid/team/repository",
    "seat": "muse13",
    "workload": "windows-development",
    "policy_version": 7,
    "policy_digest": DIGEST,
    "origin_node": "origin-node",
    "target_machine": "second-worker",
    "target_node": "second-worker-node",
    "source_revision": "abcdef0123456789abcdef0123456789abcdef01",
    "created_at": "2026-09-05T11:55:00Z",
    "expires_at": "2026-09-05T12:10:00Z",
}


class DelegationReceiptTests(unittest.TestCase):
    """The receipt is the whole authority for an adopted request."""

    def show(self, *results: object) -> tuple[dict, list[str]]:
        policy, runner = adapter(*results)
        receipt = policy.delegation(delegation_id="del-8f2c1a")
        return receipt, runner.calls[0]

    def test_show_is_a_read_only_brigade_cli_call(self) -> None:
        receipt, argv = self.show((0, json.dumps(DELEGATION_RECEIPT)))
        self.assertEqual(
            argv[:5], ["brigade", "fleet", "policy", "delegation", "show"]
        )
        self.assertEqual(flag(argv, "--delegation-id"), "del-8f2c1a")
        self.assertIn("--json", argv)
        # Nothing about routing, reserving, or the prompt crosses this call.
        for forbidden in ("--repo", "--workload", "--machine", "--seat", "--prompt"):
            self.assertNotIn(forbidden, argv)
        self.assertEqual(receipt["consumer"], "brigade-run")
        self.assertEqual(receipt["adapter"], "t3-fleet")
        self.assertIs(receipt["launch_authorized"], False)
        self.assertEqual(receipt["parent_request_id"], "run-7a1c9e")
        self.assertEqual(
            receipt["source_revision"], "abcdef0123456789abcdef0123456789abcdef01"
        )

    def test_a_receipt_for_another_delegation_is_refused(self) -> None:
        payload = {**DELEGATION_RECEIPT, "delegation_id": "del-other"}
        with self.assertRaises(PolicyRejected):
            self.show((0, json.dumps(payload)))

    def test_every_field_is_mandatory(self) -> None:
        for missing in sorted(DELEGATION_RECEIPT):
            with self.subTest(missing=missing):
                payload = {
                    key: item
                    for key, item in DELEGATION_RECEIPT.items()
                    if key != missing
                }
                with self.assertRaises(HostUnavailable):
                    self.show((0, json.dumps(payload)))

    def test_an_extra_identity_field_is_refused(self) -> None:
        payload = {**DELEGATION_RECEIPT, "worktree": "/srv/state/worktrees/x"}
        with self.assertRaises(HostUnavailable):
            self.show((0, json.dumps(payload)))

    def test_a_receipt_that_claims_authorization_is_refused(self) -> None:
        for mutation in (
            {"launch_authorized": True},
            {"launch_authorized": None},
            {"consumer": "t3-fleet"},
            {"adapter": "brigade-run"},
            {"schema": "brigade.fleet_policy_delegation.v2"},
        ):
            with self.subTest(mutation=sorted(mutation)):
                with self.assertRaises(HostUnavailable):
                    self.show((0, json.dumps({**DELEGATION_RECEIPT, **mutation})))

    def test_the_source_revision_is_a_full_lowercase_commit(self) -> None:
        for revision in (
            "ABCDEF0123456789abcdef0123456789abcdef01",
            "abcdef01",
            "abcdef0123456789abcdef0123456789abcdef012",
            "refs/heads/main",
        ):
            with self.subTest(revision=revision):
                with self.assertRaises(HostUnavailable):
                    self.show(
                        (
                            0,
                            json.dumps(
                                {**DELEGATION_RECEIPT, "source_revision": revision}
                            ),
                        )
                    )

    def test_an_expired_or_implausible_window_is_refused(self) -> None:
        for expires_at in ("2026-09-05T11:59:00Z", "2026-09-08T12:00:00Z"):
            with self.subTest(expires_at=expires_at):
                with self.assertRaises(PolicyRejected):
                    self.show(
                        (0, json.dumps({**DELEGATION_RECEIPT, "expires_at": expires_at}))
                    )

    def test_a_receipt_created_in_the_future_is_refused(self) -> None:
        with self.assertRaises(PolicyRejected):
            self.show(
                (
                    0,
                    json.dumps(
                        {**DELEGATION_RECEIPT, "created_at": "2026-09-05T12:05:00Z"}
                    ),
                )
            )

    def test_a_refusing_helper_denies_rather_than_adopting(self) -> None:
        with self.assertRaises(PolicyRejected):
            self.show((3, json.dumps({"error": {"code": "policy_denied"}})))

    def test_the_stamp_is_built_entirely_from_the_receipt(self) -> None:
        receipt, _ = self.show((0, json.dumps(DELEGATION_RECEIPT)))
        stamp = delegation_stamp(receipt)
        self.assertEqual(normalize_policy_stamp(stamp), stamp)
        self.assertEqual(stamp["origin"], "origin-node")
        self.assertEqual(stamp["machine"], "second-worker")
        self.assertEqual(stamp["decision_id"], "decision-1")
        self.assertEqual(stamp["reservation_id"], "reservation-1")
        self.assertEqual(stamp["workload"], "windows-development")
        self.assertEqual(stamp["delegation_id"], "del-8f2c1a")
        # The parent's own request id is the parent's; it never becomes local
        # execution identity or crosses to the target on the stamp.
        self.assertNotIn("parent_request_id", stamp)


class DelegatedAdapterArgumentTests(unittest.TestCase):
    """A delegated call runs under the parent consumer and names the delegation."""

    def test_prepare_is_delegation_scoped_under_the_parent_consumer(self) -> None:
        policy, runner = adapter((0, json.dumps(PREPARED)))
        policy.prepare(
            consumer="brigade-run",
            repository="/srv/repos/example",
            session_id=SESSION,
            origin="origin-node",
            provider="openai",
            model="gpt-5.6-terra-2026-08-30",
            instance_id="codex",
            decision_id="decision-1",
            delegation_id="del-8f2c1a",
        )
        argv = runner.calls[0]
        self.assertEqual(flag(argv, "--consumer"), "brigade-run")
        self.assertEqual(flag(argv, "--delegation-id"), "del-8f2c1a")
        self.assertEqual(flag(argv, "--decision-id"), "decision-1")
        self.assertEqual(argv[-1], "--json")

    def test_an_ordinary_prepare_names_no_delegation(self) -> None:
        policy, runner = adapter((0, json.dumps(PREPARED)))
        policy.prepare(
            consumer="t3-fleet",
            repository="/srv/repos/example",
            session_id=SESSION,
            origin="controller-host",
            provider="openai",
            model="gpt-5.6-terra-2026-08-30",
            instance_id="codex",
            decision_id="decision-1",
        )
        self.assertNotIn("--delegation-id", runner.calls[0])

    def test_only_the_two_contracted_consumers_are_accepted(self) -> None:
        for consumer, delegation_id in (
            ("brigade-run", None),
            ("t3-fleet", "del-8f2c1a"),
            ("some-other-tool", None),
        ):
            with self.subTest(consumer=consumer, delegation_id=delegation_id):
                policy, _ = adapter((0, json.dumps(PREPARED)))
                with self.assertRaises(ValidationError):
                    policy.prepare(
                        consumer=consumer,
                        repository="/srv/repos/example",
                        session_id=SESSION,
                        origin="controller-host",
                        provider="openai",
                        model="gpt-5.6-terra-2026-08-30",
                        instance_id="codex",
                        decision_id="decision-1",
                        delegation_id=delegation_id,
                    )

    def test_the_acknowledgement_forwards_the_prepared_context_hash(self) -> None:
        policy, runner = adapter((0, json.dumps({**ACK, "context_hash": CONTEXT_HASH})))
        policy.acknowledge(
            session_id=SESSION,
            consumer="t3-fleet",
            repository="example-repo",
            version=7,
            digest=DIGEST,
            context_hash=CONTEXT_HASH,
        )
        argv = runner.calls[0]
        self.assertEqual(flag(argv, "--context-hash"), CONTEXT_HASH)
        self.assertEqual(argv[-1], "--json")

    def test_an_enrolled_acknowledgement_must_echo_the_exact_prepared_hash(self) -> None:
        policy, _ = adapter(
            (0, json.dumps({**ACK, "context_hash": CONTEXT_HASH}))
        )
        record = policy.acknowledge(
            session_id=SESSION,
            consumer="t3-fleet",
            repository="example-repo",
            version=7,
            digest=DIGEST,
            context_hash=CONTEXT_HASH,
        )
        self.assertEqual(record["context_hash"], CONTEXT_HASH)

    def test_an_enrolled_acknowledgement_refuses_a_missing_null_or_other_hash(
        self,
    ) -> None:
        """Null is not a weaker success for a session that applied a context."""
        for acknowledged in (
            None,
            "sha256:" + "d" * 64,
            CONTEXT_HASH.split(":", 1)[1],
        ):
            with self.subTest(acknowledged=acknowledged):
                policy, _ = adapter(
                    (0, json.dumps({**ACK, "context_hash": acknowledged}))
                )
                with self.assertRaises(PolicyRejected):
                    policy.acknowledge(
                        session_id=SESSION,
                        consumer="t3-fleet",
                        repository="example-repo",
                        version=7,
                        digest=DIGEST,
                        context_hash=CONTEXT_HASH,
                    )
        payload = {key: value for key, value in ACK.items() if key != "context_hash"}
        policy, _ = adapter((0, json.dumps(payload)))
        with self.assertRaises(HostUnavailable):
            policy.acknowledge(
                session_id=SESSION,
                consumer="t3-fleet",
                repository="example-repo",
                version=7,
                digest=DIGEST,
                context_hash=CONTEXT_HASH,
            )

    def test_a_resolve_only_session_refuses_a_context_it_never_applied(self) -> None:
        policy, _ = adapter((0, json.dumps({**ACK, "context_hash": CONTEXT_HASH})))
        with self.assertRaises(PolicyRejected):
            policy.acknowledge(
                session_id=SESSION,
                consumer="t3-fleet",
                repository="example-repo",
                version=7,
                digest=DIGEST,
            )

    def test_an_acknowledgement_with_an_unknown_extra_key_is_refused(self) -> None:
        policy, _ = adapter(
            (0, json.dumps({**ACK, "context_hash": CONTEXT_HASH, "applied_at": "now"}))
        )
        with self.assertRaises(HostUnavailable):
            policy.acknowledge(
                session_id=SESSION,
                consumer="t3-fleet",
                repository="example-repo",
                version=7,
                digest=DIGEST,
                context_hash=CONTEXT_HASH,
            )

    def test_a_derived_or_malformed_context_hash_never_reaches_the_hub(self) -> None:
        for context_hash in ("2026-09-05T12:00:00Z", "b" * 64, "sha1:" + "b" * 40):
            with self.subTest(context_hash=context_hash):
                policy, _ = adapter((0, json.dumps(ACK)))
                with self.assertRaises(ValidationError):
                    policy.acknowledge(
                        session_id=SESSION,
                        consumer="t3-fleet",
                        repository="example-repo",
                        version=7,
                        digest=DIGEST,
                        context_hash=context_hash,
                    )


if __name__ == "__main__":
    unittest.main()
