from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from t3_fleet.config import HostConfig
from t3_fleet.errors import (
    HostUnavailable,
    PolicyRejected,
    PolicyRevisionConflict,
    TargetBusy,
    ValidationError,
)
from t3_fleet.transport import (
    BrigadeModelAdmission,
    LocalTransport,
    SshTransport,
    admission_identity,
    admission_matches_plan,
    normalize_admission,
    normalize_model_plan,
    plan_identity,
    require_fresh_admission,
    require_fresh_plan,
)


def host() -> HostConfig:
    return HostConfig(
        host_id="linux-worker",
        enabled=True,
        platform="linux",
        transport="ssh",
        ssh_alias="linux-worker",
        agent_command=("/opt/t3-fleet-agent",),
        active_limit=2,
        expected_hostname="linux-worker",
        t3_url="http://127.0.0.1:3773",
        t3_command=("t3",),
        t3_base_dir=None,
        agent_state_dir=None,
    )


def windows_host() -> HostConfig:
    return HostConfig(
        host_id="windows-worker",
        enabled=True,
        platform="windows",
        transport="ssh",
        ssh_alias="windows-worker",
        agent_command=(
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-File",
            r"C:\Users\Example Operator\bin\t3-fleet-agent.ps1",  # content-guard: allow pii/windows-home-path
        ),
        active_limit=2,
        expected_hostname="windows-worker",
        t3_url="http://127.0.0.1:3773",
        t3_command=("t3",),
        t3_base_dir=None,
        agent_state_dir=None,
    )


def local_host() -> HostConfig:
    return HostConfig(
        host_id="local-worker",
        enabled=True,
        platform="linux",
        transport="local",
        ssh_alias=None,
        agent_command=(sys.executable,),
        active_limit=1,
        expected_hostname="local-worker",
        t3_url="http://127.0.0.1:3773",
        t3_command=("t3",),
        t3_base_dir=None,
        agent_state_dir=None,
    )


class FakeRunner:
    def __init__(self, *, stdout: str = '{"state":"running"}', returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode
        self.argv = None
        self.stdin = None

    def __call__(self, argv, **kwargs):
        self.argv = argv
        self.stdin = kwargs.get("input")
        return subprocess.CompletedProcess(argv, self.returncode, stdout=self.stdout, stderr="")


def _admission_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": "brigade.model_admission.v1",
        "state": "authoritative",
        "source": "hub",
        "roster_revision": 42,
        "roster_digest": "sha256:" + "a" * 64,
        "seat": "coder",
        "provider": "openai",
        "model": "gpt-5.6-terra",
        "reasoning": "high",
        "binding": {"instance_id": "codex", "service_tier": "priority"},
        "expires_at": "2026-08-30T14:10:00Z",
    }
    payload.update(overrides)
    return payload


def _safe_admission(**overrides: object) -> dict[str, object]:
    admission = {
        "roster_revision": 42,
        "roster_digest": "sha256:" + "a" * 64,
        "seat": "coder",
        "provider": "openai",
        "model": "gpt-5.6-terra",
        "reasoning": "high",
        "provider_instance_id": "codex",
        "service_tier": "priority",
        "source": "hub",
        "expires_at": "2026-08-30T14:10:00Z",
    }
    admission.update(overrides)
    return admission


def _plan_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": "brigade.fleet_model_plan.v1",
        "launch_authorized": False,
        "consumer": "t3-fleet",
        "roster_revision": 42,
        "roster_digest": "sha256:" + "a" * 64,
        "seat": "coder",
        "provider": "openai",
        "model": "gpt-5.6-terra",
        "reasoning": "high",
        "binding": {
            "instance_id": "codex",
            "service_tier": "priority",
            "model": "gpt-5.6-terra-2026-08-30",
        },
        "source": "hub",
        "expires_at": "2026-08-30T14:10:00Z",
    }
    payload.update(overrides)
    return payload


PLAN_NOW = datetime(2026, 8, 30, 14, 0, tzinfo=UTC)


def flag(argv: list[str] | None, name: str) -> str | None:
    if argv is None or name not in argv:
        return None
    return argv[argv.index(name) + 1]


class ModelPlanTests(unittest.TestCase):
    """Planning is a distinct, read-only act with its own strict parser."""

    def adapter(self, runner: FakeRunner) -> BrigadeModelAdmission:
        return BrigadeModelAdmission(runner=runner, now=lambda: PLAN_NOW)

    def test_plan_invokes_the_read_only_planning_command(self) -> None:
        runner = FakeRunner(stdout=json.dumps(_plan_payload()))
        self.adapter(runner).plan({"consumer": "t3-fleet", "seat": "coder"})
        self.assertEqual(
            runner.argv,
            [
                "brigade",
                "fleet",
                "models",
                "plan",
                "--consumer",
                "t3-fleet",
                "--seat",
                "coder",
                "--json",
            ],
        )

    def test_an_explicit_binding_model_is_the_native_launch_model(self) -> None:
        runner = FakeRunner(stdout=json.dumps(_plan_payload()))
        plan = self.adapter(runner).plan({"consumer": "t3-fleet", "seat": "coder"})
        # The canonical model identity is untouched; the native launch model is
        # the explicit binding, carried verbatim.
        self.assertEqual(plan["model"], "gpt-5.6-terra")
        self.assertEqual(plan["launch_model"], "gpt-5.6-terra-2026-08-30")
        self.assertTrue(plan["launch_model_explicit"])
        self.assertEqual(plan["state"], "planned")

    def test_without_a_binding_model_the_canonical_model_is_used_verbatim(self) -> None:
        payload = _plan_payload()
        payload["binding"] = {"instance_id": "codex"}
        runner = FakeRunner(stdout=json.dumps(payload))
        plan = self.adapter(runner).plan({"consumer": "t3-fleet", "seat": "coder"})
        self.assertEqual(plan["launch_model"], "gpt-5.6-terra")
        self.assertFalse(plan["launch_model_explicit"])
        self.assertNotIn("service_tier", plan)

    def test_a_plan_that_claims_launch_authorization_is_refused(self) -> None:
        runner = FakeRunner(stdout=json.dumps(_plan_payload(launch_authorized=True)))
        with self.assertRaisesRegex(HostUnavailable, "invalid plan"):
            self.adapter(runner).plan({"consumer": "t3-fleet", "seat": "coder"})

    def test_planning_is_hub_only_and_never_last_known_good(self) -> None:
        runner = FakeRunner(stdout=json.dumps(_plan_payload(source="lkg")))
        with self.assertRaisesRegex(HostUnavailable, "invalid plan"):
            self.adapter(runner).plan({"consumer": "t3-fleet", "seat": "coder"})

    def test_an_unknown_plan_field_is_refused(self) -> None:
        runner = FakeRunner(
            stdout=json.dumps(_plan_payload(request_id="req-12345678"))
        )
        with self.assertRaisesRegex(HostUnavailable, "invalid plan"):
            self.adapter(runner).plan({"consumer": "t3-fleet", "seat": "coder"})

    def test_a_plan_for_another_consumer_is_refused(self) -> None:
        runner = FakeRunner(stdout=json.dumps(_plan_payload(consumer="other-fleet")))
        with self.assertRaisesRegex(HostUnavailable, "invalid plan"):
            self.adapter(runner).plan({"consumer": "t3-fleet", "seat": "coder"})

    def test_an_admission_envelope_is_not_a_plan(self) -> None:
        # The two parsers are distinct on purpose: neither reads the other's
        # document, so a planned seat can never be mistaken for an admitted one.
        runner = FakeRunner(stdout=json.dumps(_admission_payload()))
        with self.assertRaisesRegex(HostUnavailable, "invalid plan"):
            self.adapter(runner).plan({"consumer": "t3-fleet", "seat": "coder"})
        with self.assertRaises(ValidationError):
            normalize_model_plan(_safe_admission())

    def test_a_plan_is_not_an_admission(self) -> None:
        runner = FakeRunner(stdout=json.dumps(_plan_payload()))
        plan = self.adapter(runner).plan({"consumer": "t3-fleet", "seat": "coder"})
        with self.assertRaises(ValidationError):
            normalize_admission(plan)

    def test_a_plan_window_beyond_fifteen_minutes_is_refused(self) -> None:
        runner = FakeRunner(
            stdout=json.dumps(_plan_payload(expires_at="2026-08-30T14:30:00Z"))
        )
        with self.assertRaisesRegex(PolicyRejected, "freshness"):
            self.adapter(runner).plan({"consumer": "t3-fleet", "seat": "coder"})

    def test_an_expired_plan_is_refused(self) -> None:
        runner = FakeRunner(
            stdout=json.dumps(_plan_payload(expires_at="2026-08-30T13:59:00Z"))
        )
        with self.assertRaisesRegex(PolicyRejected, "freshness"):
            self.adapter(runner).plan({"consumer": "t3-fleet", "seat": "coder"})

    def test_a_denied_plan_is_a_policy_rejection(self) -> None:
        runner = FakeRunner(stdout="{}", returncode=3)
        with self.assertRaises(PolicyRejected):
            self.adapter(runner).plan({"consumer": "t3-fleet", "seat": "coder"})

    def test_plan_identity_ignores_the_freshness_window(self) -> None:
        first = normalize_model_plan(
            {**_normalized_plan(), "expires_at": "2026-08-30T14:10:00Z"}
        )
        second = normalize_model_plan(
            {**_normalized_plan(), "expires_at": "2026-08-30T14:12:00Z"}
        )
        self.assertEqual(plan_identity(first), plan_identity(second))
        self.assertNotEqual(first, second)

    def test_a_plan_may_not_silently_rewrite_the_native_model(self) -> None:
        # `launch_model_explicit` is false, so the native model has to be the
        # canonical one. A record that says otherwise is refused rather than
        # having one value inferred from the other.
        with self.assertRaises(ValidationError):
            normalize_model_plan(
                {
                    **_normalized_plan(),
                    "launch_model": "gpt-5.6-terra-2026-08-30",
                    "launch_model_explicit": False,
                }
            )

    def test_an_admission_matches_only_the_plan_it_was_taken_for(self) -> None:
        plan = normalize_model_plan(_normalized_plan())
        admission = normalize_admission(_safe_admission())
        self.assertTrue(admission_matches_plan(admission, plan))
        for mutation in (
            {"seat": "muse13"},
            {"model": "gpt-5.6-sol"},
            {"provider": "anthropic"},
            {"reasoning": "low"},
            {"provider_instance_id": "claude"},
            {"roster_revision": 43},
            {"service_tier": "standard"},
        ):
            with self.subTest(mutation=sorted(mutation)):
                moved = normalize_admission(_safe_admission(**mutation))
                self.assertFalse(admission_matches_plan(moved, plan))

    def test_require_fresh_plan_needs_a_timezone_aware_clock(self) -> None:
        with self.assertRaisesRegex(ValidationError, "timezone-aware"):
            require_fresh_plan(
                _normalized_plan(), now=datetime(2026, 8, 30, 14, 0)
            )


def _normalized_plan(**overrides: object) -> dict[str, object]:
    plan: dict[str, object] = {
        "state": "planned",
        "roster_revision": 42,
        "roster_digest": "sha256:" + "a" * 64,
        "seat": "coder",
        "provider": "openai",
        "model": "gpt-5.6-terra",
        "reasoning": "high",
        "provider_instance_id": "codex",
        "service_tier": "priority",
        "launch_model": "gpt-5.6-terra-2026-08-30",
        "launch_model_explicit": True,
        "source": "hub",
        "expires_at": "2026-08-30T14:10:00Z",
    }
    plan.update(overrides)
    return plan


class LaunchAdmissionTests(unittest.TestCase):
    """A real launch admission carries the policy proof that paid for it."""

    def adapter(self, runner: FakeRunner) -> BrigadeModelAdmission:
        return BrigadeModelAdmission(
            runner=runner, now=lambda: datetime(2026, 8, 30, 14, 0, tzinfo=UTC)
        )

    def request(self, **overrides: object) -> dict[str, object]:
        payload: dict[str, object] = {
            "consumer": "t3-fleet",
            "phase": "launch",
            "request_id": "req-12345678",
            "seat": "coder",
            "expect_revision": 42,
            "expect_digest": "sha256:" + "a" * 64,
            "policy_session_id": "t3f-" + "0" * 32,
            "policy_version": 7,
            "policy_digest": "sha256:" + "b" * 64,
            "decision_id": "decision-1",
            "policy_repo_identity": "github.example/example-org/example-repo",
            "policy_context_hash": "sha256:" + "c" * 64,
        }
        payload.update(overrides)
        return payload

    def test_launch_admission_sends_the_full_policy_proof(self) -> None:
        runner = FakeRunner(stdout=json.dumps(_admission_payload()))
        self.adapter(runner).admit(self.request())
        self.assertEqual(
            runner.argv,
            [
                "brigade",
                "fleet",
                "models",
                "admit",
                "--consumer",
                "t3-fleet",
                "--seat",
                "coder",
                "--request-id",
                "req-12345678",
                "--phase",
                "launch",
                "--expect-revision",
                "42",
                "--expect-digest",
                "sha256:" + "a" * 64,
                "--policy-session-id",
                "t3f-" + "0" * 32,
                "--policy-version",
                "7",
                "--policy-digest",
                "sha256:" + "b" * 64,
                "--decision-id",
                "decision-1",
                "--repo",
                "github.example/example-org/example-repo",
                "--policy-context-hash",
                "sha256:" + "c" * 64,
                "--json",
            ],
        )

    def test_a_partial_policy_proof_is_a_malformed_request(self) -> None:
        for missing in (
            "policy_session_id",
            "policy_version",
            "policy_digest",
            "decision_id",
            "policy_repo_identity",
            "policy_context_hash",
        ):
            with self.subTest(missing=missing):
                runner = FakeRunner(stdout=json.dumps(_admission_payload()))
                request = self.request()
                request.pop(missing)
                with self.assertRaisesRegex(ValidationError, "complete"):
                    self.adapter(runner).admit(request)
                self.assertIsNone(runner.argv)

    def test_a_launch_admission_still_pins_the_planned_roster(self) -> None:
        runner = FakeRunner(stdout=json.dumps(_admission_payload()))
        request = self.request()
        request.pop("expect_revision")
        with self.assertRaisesRegex(ValidationError, "expect_revision"):
            self.adapter(runner).admit(request)
        self.assertIsNone(runner.argv)

    def test_only_a_launch_may_carry_a_policy_proof(self) -> None:
        for phase in ("controller", "target"):
            with self.subTest(phase=phase):
                runner = FakeRunner(stdout=json.dumps(_admission_payload()))
                request = self.request(phase=phase)
                if phase == "controller":
                    request.pop("expect_revision")
                    request.pop("expect_digest")
                with self.assertRaisesRegex(ValidationError, "only a launch"):
                    self.adapter(runner).admit(request)
                self.assertIsNone(runner.argv)

    def test_a_malformed_proof_never_reaches_the_helper(self) -> None:
        for mutation in (
            {"policy_session_id": "Not An Id"},
            {"policy_version": 0},
            {"policy_digest": "b" * 64},
            {"decision_id": "decision one"},
            # A checkout path is not a canonical identity, a credential must
            # never ride out on it, and the context hash is a sha256 digest
            # rather than a bare hex string or a timestamp.
            {"policy_repo_identity": "/srv/repos/example"},
            {"policy_repo_identity": "https://example.test/org/repo"},
            {"policy_repo_identity": "user:secret@example.test/org/repo"},
            {"policy_context_hash": "c" * 64},
            {"policy_context_hash": "2026-08-30T14:00:00Z"},
            {"policy_context_hash": "sha1:" + "c" * 40},
        ):
            with self.subTest(mutation=sorted(mutation)):
                runner = FakeRunner(stdout=json.dumps(_admission_payload()))
                with self.assertRaises(ValidationError):
                    self.adapter(runner).admit(self.request(**mutation))
                self.assertIsNone(runner.argv)

    def test_the_launch_reply_may_carry_a_native_binding_model(self) -> None:
        runner = FakeRunner(
            stdout=json.dumps(
                _admission_payload(
                    binding={
                        "instance_id": "codex",
                        "model": "gpt-5.6-terra-2026-08-30",
                    }
                )
            )
        )
        admission = self.adapter(runner).admit(self.request())
        # The canonical identity is untouched; the native model is exposed
        # separately as the launch model.
        self.assertEqual(admission["model"], "gpt-5.6-terra")
        self.assertEqual(admission["launch_model"], "gpt-5.6-terra-2026-08-30")

    def test_a_launch_reply_without_a_binding_model_has_no_launch_model(self) -> None:
        runner = FakeRunner(stdout=json.dumps(_admission_payload()))
        admission = self.adapter(runner).admit(self.request())
        self.assertNotIn("launch_model", admission)

    def test_a_launch_reply_with_an_unknown_binding_key_is_refused(self) -> None:
        runner = FakeRunner(
            stdout=json.dumps(
                _admission_payload(
                    binding={
                        "instance_id": "codex",
                        "model": "gpt-5.6-terra-2026-08-30",
                        "extra": "x",
                    }
                )
            )
        )
        with self.assertRaisesRegex(HostUnavailable, "invalid admission"):
            self.adapter(runner).admit(self.request())

    def test_a_launch_reply_with_an_invalid_binding_model_is_refused(self) -> None:
        for model in ("", 7):
            with self.subTest(model=model):
                runner = FakeRunner(
                    stdout=json.dumps(
                        _admission_payload(
                            binding={"instance_id": "codex", "model": model}
                        )
                    )
                )
                with self.assertRaisesRegex(HostUnavailable, "invalid admission"):
                    self.adapter(runner).admit(self.request())


class TransportTests(unittest.TestCase):
    def test_an_admission_launch_model_is_part_of_its_identity(self) -> None:
        native = normalize_admission(
            _safe_admission(launch_model="gpt-5.6-terra-2026-08-30")
        )
        self.assertEqual(native["launch_model"], "gpt-5.6-terra-2026-08-30")
        self.assertNotEqual(
            admission_identity(native), admission_identity(_safe_admission())
        )
        for bad in ("", 7):
            with self.subTest(launch_model=bad):
                with self.assertRaises(ValidationError):
                    normalize_admission(_safe_admission(launch_model=bad))

    def test_an_admission_launch_model_must_match_the_planned_one(self) -> None:
        plan = normalize_model_plan(_normalized_plan())
        same = normalize_admission(
            _safe_admission(launch_model="gpt-5.6-terra-2026-08-30")
        )
        self.assertTrue(admission_matches_plan(same, plan))
        moved = normalize_admission(
            _safe_admission(launch_model="gpt-5.6-terra-2026-01-01")
        )
        self.assertFalse(admission_matches_plan(moved, plan))

    def test_admission_identity_ignores_expires_at(self) -> None:
        first = normalize_admission(_safe_admission(expires_at="2026-08-30T14:10:00Z"))
        second = normalize_admission(_safe_admission(expires_at="2026-08-30T14:12:00Z"))
        self.assertEqual(admission_identity(first), admission_identity(second))
        self.assertNotIn("expires_at", admission_identity(first))

    def test_fresh_admission_accepts_bounded_target_clock_skew(self) -> None:
        require_fresh_admission(
            _safe_admission(expires_at="2026-08-30T14:15:00Z"),
            now=datetime(2026, 8, 30, 13, 59, 30, tzinfo=UTC),
        )

    def test_fresh_admission_rejects_excessive_future_window(self) -> None:
        now = datetime(2026, 8, 30, 13, 59, 30, tzinfo=UTC)
        with self.assertRaisesRegex(ValidationError, "freshness"):
            require_fresh_admission(
                _safe_admission(expires_at="2026-08-30T14:15:01Z"),
                now=now,
            )

    def test_fresh_admission_rejects_expired_receipt_despite_clock_skew(self) -> None:
        now = datetime(2026, 8, 30, 14, 0, tzinfo=UTC)
        with self.assertRaisesRegex(ValidationError, "expires_at"):
            require_fresh_admission(
                _safe_admission(expires_at="2026-08-30T13:59:00Z"),
                now=now,
            )
        with self.assertRaisesRegex(ValidationError, "expires_at"):
            require_fresh_admission(
                _safe_admission(expires_at="2026-08-30T14:10:00"),
                now=now,
            )

    def test_model_admission_uses_exact_cli_contract_without_stdin(self) -> None:
        runner = FakeRunner(
            stdout=json.dumps(
                {
                    "schema": "brigade.model_admission.v1",
                    "state": "authoritative",
                    "source": "hub",
                    "roster_revision": 42,
                    "roster_digest": "sha256:" + "a" * 64,
                    "seat": "coder",
                    "provider": "openai",
                    "model": "gpt-5.6-terra",
                    "reasoning": "high",
                    "binding": {
                        "instance_id": "codex",
                        "service_tier": "priority",
                    },
                    "expires_at": "2099-08-30T14:00:00Z",
                }
            )
        )

        admission = BrigadeModelAdmission(
            runner=runner,
            now=lambda: datetime(2099, 8, 30, 13, 50, tzinfo=UTC),
        ).admit(
            {
                "consumer": "t3-fleet",
                "phase": "target",
                "request_id": "req-12345678",
                "seat": "coder",
                "expect_revision": 42,
                "expect_digest": "sha256:" + "a" * 64,
            }
        )

        self.assertEqual(
            runner.argv,
            [
                "brigade",
                "fleet",
                "models",
                "admit",
                "--consumer",
                "t3-fleet",
                "--seat",
                "coder",
                "--request-id",
                "req-12345678",
                "--phase",
                "target",
                "--expect-revision",
                "42",
                "--expect-digest",
                "sha256:" + "a" * 64,
                "--json",
            ],
        )
        self.assertIsNone(runner.stdin)
        self.assertEqual(admission["roster_revision"], 42)
        self.assertEqual(admission["provider_instance_id"], "codex")
        self.assertEqual(admission["service_tier"], "priority")

    def test_model_admission_maps_policy_denial_and_revision_conflict(self) -> None:
        request = {
            "consumer": "t3-fleet",
            "phase": "controller",
            "request_id": "req-12345678",
            "seat": "coder",
        }
        denied = BrigadeModelAdmission(
            runner=FakeRunner(
                returncode=3,
                stdout=json.dumps({"error": "retired model denied"}),
            )
        )
        with self.assertRaises(PolicyRejected):
            denied.admit(request)

        conflict = BrigadeModelAdmission(
            runner=FakeRunner(
                returncode=4,
                stdout=json.dumps({"error": "roster changed"}),
            )
        )
        with self.assertRaises(PolicyRevisionConflict):
            conflict.admit(request)

        unavailable = BrigadeModelAdmission(runner=FakeRunner(returncode=1))
        with self.assertRaises(HostUnavailable):
            unavailable.admit(request)

        malformed = BrigadeModelAdmission(runner=FakeRunner(returncode=2))
        with self.assertRaises(ValidationError):
            malformed.admit(request)

    def test_model_admission_rejects_malformed_authoritative_envelope(self) -> None:
        runner = FakeRunner(
            stdout=json.dumps(
                {
                    "schema": "brigade.model_admission.v1",
                    "state": "authoritative",
                    "source": "hub",
                    "roster_revision": "42",
                    "roster_digest": "a" * 64,
                    "seat": "coder",
                    "provider": "openai",
                    "model": "gpt-5.6-terra",
                    "reasoning": "high",
                    "binding": {"instance_id": "codex"},
                    "expires_at": "2099-08-30T14:00:00Z",
                }
            )
        )
        with self.assertRaisesRegex(HostUnavailable, "invalid admission"):
            BrigadeModelAdmission(runner=runner).admit(
                {
                    "consumer": "t3-fleet",
                    "phase": "controller",
                    "request_id": "req-12345678",
                    "seat": "coder",
                }
            )

    def test_model_admission_requires_expiring_authority(self) -> None:
        payload = {
            "schema": "brigade.model_admission.v1",
            "state": "authoritative",
            "source": "hub",
            "roster_revision": 42,
            "roster_digest": "sha256:" + "a" * 64,
            "seat": "coder",
            "provider": "openai",
            "model": "gpt-5.6-terra",
            "reasoning": "high",
            "binding": {"instance_id": "codex"},
        }
        with self.assertRaisesRegex(HostUnavailable, "invalid admission"):
            BrigadeModelAdmission(
                runner=FakeRunner(stdout=json.dumps(payload))
            ).admit(
                {
                    "consumer": "t3-fleet",
                    "phase": "controller",
                    "request_id": "req-12345678",
                    "seat": "coder",
                }
            )

    def test_model_admission_rejects_expired_authority(self) -> None:
        payload = {
            "schema": "brigade.model_admission.v1",
            "state": "authoritative",
            "source": "hub",
            "roster_revision": 42,
            "roster_digest": "sha256:" + "a" * 64,
            "seat": "coder",
            "provider": "openai",
            "model": "gpt-5.6-terra",
            "reasoning": "high",
            "binding": {"instance_id": "codex"},
            "expires_at": "2020-01-01T00:00:00Z",
        }
        with self.assertRaisesRegex(PolicyRejected, "expired"):
            BrigadeModelAdmission(
                runner=FakeRunner(stdout=json.dumps(payload))
            ).admit(
                {
                    "consumer": "t3-fleet",
                    "phase": "controller",
                    "request_id": "req-12345678",
                    "seat": "coder",
                }
            )

    def test_model_admission_real_process_uses_descriptor_safe_path(self) -> None:
        payload = {
            "schema": "brigade.model_admission.v1",
            "state": "authoritative",
            "source": "hub",
            "roster_revision": 42,
            "roster_digest": "sha256:" + "a" * 64,
            "seat": "coder",
            "provider": "openai",
            "model": "gpt-5.6-terra",
            "reasoning": "high",
            "binding": {"instance_id": "codex", "service_tier": "priority"},
            "expires_at": "2099-08-30T14:00:00Z",
        }
        with tempfile.TemporaryDirectory() as directory:
            executable = Path(directory) / "brigade"
            executable.write_text(
                f"#!{sys.executable}\n"
                "import json, sys\n"
                "assert sys.stdin.read() == ''\n"
                f"print(json.dumps({payload!r}, sort_keys=True))\n",
                encoding="utf-8",
            )
            executable.chmod(0o755)
            with patch.dict(
                os.environ,
                {"PATH": directory + os.pathsep + os.environ.get("PATH", "")},
            ):
                admission = BrigadeModelAdmission(
                    now=lambda: datetime(2099, 8, 30, 13, 50, tzinfo=UTC),
                ).admit(
                    {
                        "consumer": "t3-fleet",
                        "phase": "controller",
                        "request_id": "req-12345678",
                        "seat": "coder",
                    }
                )

        self.assertEqual(admission["provider_instance_id"], "codex")

    def test_ssh_uses_fixed_command_and_json_stdin(self) -> None:
        runner = FakeRunner()
        transport = SshTransport(runner=runner)
        request = {
            "action": "status",
            "thread_id": "thread-1",
            "prompt": "caller text; touch /tmp/not-an-argv",
        }
        result = transport.call(host(), request)
        self.assertEqual(
            runner.argv,
            ["ssh", "-o", "BatchMode=yes", "--", "linux-worker", "/opt/t3-fleet-agent"],
        )
        self.assertNotIn("caller text", " ".join(runner.argv))
        self.assertTrue(runner.stdin.endswith("\n"))
        self.assertEqual(runner.stdin.count("\n"), 1)
        self.assertEqual(json.loads(runner.stdin)["action"], "status")
        self.assertEqual(result["state"], "running")

    def test_ssh_quotes_each_posix_remote_command_argument(self) -> None:
        configured = replace(
            host(),
            agent_command=(
                "/opt/Fleet Agent/t3-fleet-agent",
                "--state-dir",
                "/var/lib/t3 fleet",
            ),
        )
        runner = FakeRunner()
        SshTransport(runner=runner).call(configured, {"action": "doctor"})
        self.assertEqual(
            runner.argv,
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "--",
                "linux-worker",
                "'/opt/Fleet Agent/t3-fleet-agent' --state-dir '/var/lib/t3 fleet'",
            ],
        )

    def test_ssh_quotes_windows_remote_command_for_create_process(self) -> None:
        runner = FakeRunner()
        SshTransport(runner=runner).call(windows_host(), {"action": "doctor"})
        self.assertEqual(
            runner.argv,
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "--",
                "windows-worker",
                (
                    "powershell.exe -NoProfile -NonInteractive -File "
                    '"C:\\Users\\Example Operator\\bin\\t3-fleet-agent.ps1"'  # content-guard: allow pii/windows-home-path
                ),
            ],
        )

    def test_ssh_uses_configured_client_executable_as_one_argument(self) -> None:
        runner = FakeRunner()
        SshTransport(
            ssh_command=(r"C:\Program Files\Git\usr\bin\ssh.exe",),
            runner=runner,
        ).call(host(), {"action": "doctor"})
        self.assertEqual(
            runner.argv,
            [
                r"C:\Program Files\Git\usr\bin\ssh.exe",
                "-o",
                "BatchMode=yes",
                "--",
                "linux-worker",
                "/opt/t3-fleet-agent",
            ],
        )

    def test_default_runner_spools_output_before_bounded_read(self) -> None:
        observed = {}

        def emit_oversized_response(argv, **kwargs):
            observed.update(kwargs)
            kwargs["stdout"].write(b"x" * (1024 * 1024 + 1))
            return subprocess.CompletedProcess(argv, 0)

        with patch("t3_fleet.transport.subprocess.run", side_effect=emit_oversized_response):
            with self.assertRaisesRegex(HostUnavailable, "response exceeds"):
                LocalTransport().call(local_host(), {"action": "doctor"})

        self.assertNotEqual(observed["stdin"], subprocess.PIPE)
        self.assertNotEqual(observed["stdout"], subprocess.PIPE)
        self.assertEqual(observed["stderr"], subprocess.DEVNULL)
        self.assertNotIn("input", observed)
        self.assertNotIn("capture_output", observed)

    def test_nonzero_exit_is_unavailable_without_stderr_leak(self) -> None:
        runner = FakeRunner(returncode=255)
        transport = SshTransport(runner=runner)
        with self.assertRaisesRegex(HostUnavailable, "target adapter failed"):
            transport.call(host(), {"action": "doctor"})

    def test_nonzero_exit_preserves_structured_agent_detail(self) -> None:
        runner = FakeRunner(
            returncode=3,
            stdout=json.dumps(
                {
                    "schema": "t3-fleet.agent-error.v1",
                    "error": {
                        "code": "host_unavailable",
                        "message": "target is unavailable",
                        "detail": "target capacity is exhausted at 1 active requests",
                    },
                }
            ),
        )
        transport = SshTransport(runner=runner)

        with self.assertRaises(HostUnavailable) as raised:
            transport.call(host(), {"action": "doctor"})

        self.assertEqual(
            raised.exception.detail,
            "target capacity is exhausted at 1 active requests",
        )

    def test_a_typed_policy_reason_survives_the_target_hop(self) -> None:
        runner = FakeRunner(
            returncode=3,
            stdout=json.dumps(
                {
                    "schema": "t3-fleet.agent-error.v1",
                    "error": {
                        "code": "policy_rejected",
                        "message": "target model admission was denied",
                        "reason": "continuation-admission-required",
                    },
                }
            ),
        )
        transport = SshTransport(runner=runner)

        with self.assertRaises(PolicyRejected) as raised:
            transport.call(host(), {"action": "followup"})

        self.assertEqual(raised.exception.reason, "continuation-admission-required")

    def test_a_busy_target_decline_crosses_as_target_busy(self) -> None:
        runner = FakeRunner(
            returncode=3,
            stdout=json.dumps(
                {
                    "schema": "t3-fleet.agent-error.v1",
                    "error": {
                        "code": "target_busy",
                        "message": "target is busy and started nothing; retry later",
                        "detail": "repository git operations stayed busy on this target",
                        "reason": "repository-busy",
                    },
                }
            ),
        )
        transport = SshTransport(runner=runner)

        with self.assertRaises(TargetBusy) as raised:
            transport.call(host(), {"action": "claim"})

        self.assertEqual(raised.exception.reason, "repository-busy")
        self.assertIn("stayed busy", raised.exception.detail)

    def test_a_busy_decline_with_an_unknown_reason_keeps_no_reason(self) -> None:
        runner = FakeRunner(
            returncode=3,
            stdout=json.dumps(
                {
                    "schema": "t3-fleet.agent-error.v1",
                    "error": {"code": "target_busy", "message": "busy", "reason": "/srv/x"},
                }
            ),
        )
        with self.assertRaises(TargetBusy) as raised:
            SshTransport(runner=runner).call(host(), {"action": "claim"})
        self.assertIsNone(raised.exception.reason)

    def test_an_unknown_policy_reason_from_a_target_is_dropped(self) -> None:
        # A target that is newer, buggy, or hostile cannot widen what crosses
        # this boundary: only allowlisted slugs survive.
        runner = FakeRunner(
            returncode=3,
            stdout=json.dumps(
                {
                    "schema": "t3-fleet.agent-error.v1",
                    "error": {
                        "code": "policy_rejected",
                        "message": "target model admission was denied",
                        "reason": "/srv/repos/private-checkout",
                    },
                }
            ),
        )
        transport = SshTransport(runner=runner)

        with self.assertRaises(PolicyRejected) as raised:
            transport.call(host(), {"action": "followup"})

        self.assertIsNone(raised.exception.reason)

    def test_invalid_json_is_rejected(self) -> None:
        transport = SshTransport(runner=FakeRunner(stdout="not-json"))
        with self.assertRaisesRegex(HostUnavailable, "invalid JSON"):
            transport.call(host(), {"action": "doctor"})


class DelegatedModelCallTests(unittest.TestCase):
    """A delegated plan or launch names the parent consumer and the delegation."""

    def adapter(self, runner: FakeRunner) -> BrigadeModelAdmission:
        return BrigadeModelAdmission(runner=runner, now=lambda: PLAN_NOW)

    def launch(self, **overrides: object) -> dict[str, object]:
        payload: dict[str, object] = {
            "consumer": "brigade-run",
            "phase": "launch",
            "request_id": "req-12345678",
            "seat": "coder",
            "expect_revision": 42,
            "expect_digest": "sha256:" + "a" * 64,
            "policy_session_id": "t3f-" + "0" * 32,
            "policy_version": 7,
            "policy_digest": "sha256:" + "b" * 64,
            "decision_id": "decision-1",
            "policy_repo_identity": "github.example/example-org/example-repo",
            "policy_context_hash": "sha256:" + "c" * 64,
            "delegation_id": "del-8f2c1a",
        }
        payload.update(overrides)
        return payload

    def test_a_delegated_plan_names_the_delegation_before_json(self) -> None:
        runner = FakeRunner(stdout=json.dumps(_plan_payload()))
        self.adapter(runner).plan(
            {
                "consumer": "brigade-run",
                "seat": "coder",
                "delegation_id": "del-8f2c1a",
            }
        )
        self.assertEqual(
            runner.argv,
            [
                "brigade",
                "fleet",
                "models",
                "plan",
                "--consumer",
                "brigade-run",
                "--seat",
                "coder",
                "--delegation-id",
                "del-8f2c1a",
                "--json",
            ],
        )

    def test_a_delegated_launch_names_the_delegation_after_the_proof(self) -> None:
        runner = FakeRunner(stdout=json.dumps(_admission_payload()))
        self.adapter(runner).admit(self.launch())
        self.assertEqual(flag(runner.argv, "--consumer"), "brigade-run")
        self.assertEqual(flag(runner.argv, "--delegation-id"), "del-8f2c1a")
        self.assertEqual(runner.argv[-1], "--json")
        self.assertLess(
            runner.argv.index("--policy-session-id"),
            runner.argv.index("--delegation-id"),
        )
        # The frozen final proof fields sit inside the proof block, before the
        # delegation that scopes it, not appended after it.
        self.assertLess(
            runner.argv.index("--policy-context-hash"),
            runner.argv.index("--delegation-id"),
        )
        self.assertEqual(
            flag(runner.argv, "--repo"), "github.example/example-org/example-repo"
        )

    def test_the_consumer_and_the_delegation_never_travel_alone(self) -> None:
        for overrides in (
            {"consumer": "t3-fleet"},
            {"delegation_id": None},
            {"consumer": "some-other-tool"},
            {"delegation_id": "del 8f2c1a"},
        ):
            with self.subTest(overrides=sorted(overrides)):
                runner = FakeRunner(stdout=json.dumps(_admission_payload()))
                with self.assertRaises(ValidationError):
                    self.adapter(runner).admit(self.launch(**overrides))
                self.assertIsNone(runner.argv)

    def test_only_a_launch_admission_may_carry_a_delegation(self) -> None:
        for phase in ("controller", "target"):
            with self.subTest(phase=phase):
                runner = FakeRunner(stdout=json.dumps(_admission_payload()))
                with self.assertRaises(ValidationError):
                    self.adapter(runner).admit(self.launch(phase=phase))

    def test_a_delegated_plan_still_refuses_unsupported_fields(self) -> None:
        runner = FakeRunner(stdout=json.dumps(_plan_payload()))
        with self.assertRaises(ValidationError):
            self.adapter(runner).plan(
                {
                    "consumer": "brigade-run",
                    "seat": "coder",
                    "delegation_id": "del-8f2c1a",
                    "adapter": "t3-fleet",
                }
            )


class ContinuationFailureMappingTests(unittest.TestCase):
    """A refusal that changed nothing must not read as a lost mutation."""

    REQUEST = {
        "consumer": "t3-fleet",
        "phase": "target",
        "request_id": "req-12345678",
        "seat": "coder",
        "expect_revision": 42,
        "expect_digest": "sha256:" + "a" * 64,
    }

    def test_hub_admission_expired_is_a_policy_rejection_with_its_reason(self) -> None:
        # Observed from `brigade fleet models admit` when the Hub replays a
        # stored admission whose fifteen-minute window has passed: exit 1 and
        # a bare string error, which used to collapse into "adapter failed".
        runner = FakeRunner(
            returncode=1,
            stdout=json.dumps({"error": "admission-expired", "reason": "admission-expired"}),
        )
        with self.assertRaises(PolicyRejected) as raised:
            BrigadeModelAdmission(runner=runner).admit(dict(self.REQUEST))
        self.assertEqual(raised.exception.reason, "model-admission-expired")
        self.assertIn("expired", raised.exception.message)

    def test_other_adapter_failures_name_the_safe_hub_reason(self) -> None:
        runner = FakeRunner(returncode=1, stdout=json.dumps({"error": "hub-unavailable"}))
        with self.assertRaises(HostUnavailable) as raised:
            BrigadeModelAdmission(runner=runner).admit(dict(self.REQUEST))
        self.assertIn("hub-unavailable", raised.exception.message)
        unsafe = FakeRunner(returncode=1, stdout=json.dumps({"error": "/srv/private path"}))
        with self.assertRaises(HostUnavailable) as raised:
            BrigadeModelAdmission(runner=unsafe).admit(dict(self.REQUEST))
        self.assertNotIn("/srv", raised.exception.message)

    def _agent_error(self, code: str, **extra: object) -> FakeRunner:
        error = {"code": code, "message": "m", **extra}
        return FakeRunner(
            returncode=3 if code != "invalid_request" else 2,
            stdout=json.dumps({"schema": "t3-fleet.agent-error.v1", "error": error}),
        )

    def test_not_started_invalid_request_becomes_a_validation_error(self) -> None:
        runner = self._agent_error(
            "invalid_request",
            detail="user-input request is not currently pending",
            not_started=True,
        )
        with self.assertRaises(ValidationError) as raised:
            SshTransport(runner=runner).call(host(), {"action": "answer"})
        self.assertIn("not currently pending", raised.exception.message)

    def test_not_started_target_failure_is_marked_not_started(self) -> None:
        runner = self._agent_error(
            "host_unavailable", detail="model admission adapter failed: hub-unavailable", not_started=True
        )
        with self.assertRaises(HostUnavailable) as raised:
            SshTransport(runner=runner).call(host(), {"action": "approve"})
        self.assertTrue(raised.exception.not_started)
        self.assertIn("hub-unavailable", raised.exception.detail)

    def test_without_the_marker_a_failure_stays_ambiguous(self) -> None:
        for marker in ({}, {"not_started": "true"}, {"not_started": 1}):
            with self.subTest(marker=marker):
                runner = self._agent_error("invalid_request", **marker)
                with self.assertRaises(HostUnavailable) as raised:
                    SshTransport(runner=runner).call(host(), {"action": "answer"})
                self.assertFalse(raised.exception.not_started)


if __name__ == "__main__":
    unittest.main()
