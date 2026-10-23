"""Issue 8: identity evidence, bounded ambiguity, and public diagnostics."""
from __future__ import annotations

import json
import sqlite3
import unittest
from unittest.mock import Mock, patch

import test_agent as fixtures
from test_agent import StaleShellProjectionT3, MalformedDetailProjectionT3
from t3_fleet.errors import HostUnavailable, ValidationError


class ReconciliationContractTests(unittest.TestCase):
    setUp = fixtures.AgentTests.setUp
    _seed_admitted_request = fixtures.AgentTests._seed_admitted_request

    def seed(self):
        self._seed_admitted_request()
        store = self.agent._target_store()
        store.reconcile('req-12345678', state='running', thread_id='thread-1', turn_ref='turn-1')
        return store

    def projection(self, shell, detail, *, malformed=None):
        kwargs = dict(shell_latest_turn=shell, detail_latest_turn=detail,
                      session={'status': 'stopped'})
        self.agent.t3 = (MalformedDetailProjectionT3(malformed, **kwargs)
                         if malformed is not None else StaleShellProjectionT3(**kwargs))

    def reconcile(self):
        return self.agent.handle({'action': 'reconcile', 'request_id': 'req-12345678'})

    def leases(self, store):
        with sqlite3.connect(store.path) as connection:
            return connection.execute('SELECT COUNT(*) FROM repository_leases').fetchone()[0]

    def test_passive_actions_never_contain_at_or_after_deadline(self):
        from datetime import datetime, timedelta
        store = self.seed()
        self.projection({'state': 'interrupted'}, {'state': 'running'})
        first = self.reconcile()
        deadline = datetime.fromisoformat(first['resolution']['deadline'].replace('Z', '+00:00'))
        for seconds in (0, 60):
            self.agent._now = lambda: (deadline + timedelta(seconds=seconds)).isoformat()
            store._now = self.agent._now
            for action in ('observe', 'status', 'reconcile', 'doctor', 'capacity', 'settle'):
                with self.subTest(seconds=seconds, action=action):
                    self.agent.t3.dispatched.clear()
                    request = {'action': action, 'request_id': 'req-12345678', 'thread_id': 'thread-1'}
                    self.agent.handle(request)
                    self.assertEqual(self.agent.t3.dispatched, [])
        self.assertTrue(store.lookup('req-12345678').lifecycle['resolution']['blocking'])
        self.assertEqual(store.lookup('req-12345678').state, 'failed')
        self.assertEqual(self.leases(store), 0)

    def test_recover_containment_requires_explicit_boolean(self):
        store = self.seed()
        self.projection({'state': 'interrupted'}, {'state': 'running'})
        first = self.reconcile()
        self.agent._now = lambda: first['resolution']['deadline']
        store._now = self.agent._now
        for action in ('reconcile', 'recover'):
            request = {'action': action, 'request_id': 'req-12345678', 'thread_id': 'thread-1'}
            with self.subTest(action=action):
                self.agent.t3.dispatched.clear()
                self.agent.handle(request)
                self.assertEqual(self.agent.t3.dispatched, [])
                self.agent.handle(dict(request, contain=True))
                self.assertIn('thread.session.stop', [c['type'] for c in self.agent.t3.dispatched])
                for invalid in (1, 'true', None, [], {}):
                    with self.subTest(contain=invalid), self.assertRaisesRegex(ValidationError, 'contain must be a boolean'):
                        self.agent.handle(dict(request, contain=invalid))

    def test_live_session_projection_matrix_has_no_resolution_clock(self):
        store = self.seed()
        for session in ('starting', 'running'):
            for latest in ('missing', {}, {'turnId': 'turn-1'}, {'state': 'running'},
                           {'turnId': 'turn-1', 'state': 'completed'}):
                with self.subTest(session=session, latest=latest):
                    self.projection(latest, {'turnId': 'turn-1', 'state': 'completed'})
                    self.agent.t3.thread['session'] = {'status': session}
                    shell = self.agent.t3.shell_snapshot()
                    if latest == 'missing':
                        del shell['threads'][0]['latestTurn']
                    with patch.object(self.agent.t3, 'shell_snapshot', return_value=shell):
                        result = self.reconcile()
                    self.assertEqual(result['state'], 'running')
                    if isinstance(latest, dict) and latest.get('state') == 'completed':
                        self.assertEqual(self.agent.handle({'action': 'capacity'})['active'], 1)
                    self.assertNotIn('resolution', result)
                    self.assertNotIn('resolution', store.lookup('req-12345678').lifecycle or {})
                    self.assertEqual(store.active_count(), 1)
                    self.assertEqual(self.leases(store), 1)
                    self.assertEqual(self.agent.t3.dispatched, [])

    def test_terminal_detail_transport_failure_degrades_to_unknown(self):
        store = self.seed()
        for latest in ({'turnId': 'turn-1', 'state': 'completed'}, None):
            self.projection(latest, {'turnId': 'turn-1', 'state': 'completed'})
            with patch.object(self.agent.t3, 'thread_snapshot', side_effect=RuntimeError('transport lost')):
                for bounded in (False, True):
                    with self.subTest(bounded=bounded, latest=latest):
                        result = self.agent._status('thread-1', bounded=bounded, include_output=True)
                        self.assertEqual(result['state'], 'unknown')
                        self.assertNotIn('_terminal_projection', result)
                        self.assertNotIn('_explicit_no_turn', result)
                self.assertEqual(self.reconcile()['state'], 'unknown')
                self.agent.handle({'action': 'observe'})
        self.assertNotIn('resolution', store.lookup('req-12345678').lifecycle or {})
        self.assertEqual(store.active_count(), 1)
        self.assertEqual(self.leases(store), 1)

    def test_live_session_clears_old_resolution_with_previous_identity(self):
        store = self.seed()
        store.begin_turn('req-12345678', previous_turn_ref='turn-1')
        store.confirm_turn('req-12345678')
        store.reconcile('req-12345678', state='unknown', thread_id='thread-1', resolution={
            'reason': 'terminal-unconfirmed', 'deadline': '2026-07-28T01:05:00Z', 'blocking': True})
        self.projection({'turnId': 'turn-1', 'state': 'completed'}, {'state': 'completed'})
        for now in ('2026-07-28T01:05:00Z', '2026-07-28T01:06:00Z'):
            for session in ('starting', 'running'):
                with self.subTest(now=now, session=session):
                    self.agent._now = lambda: now
                    store._now = self.agent._now
                    self.agent.t3.thread['session'] = {'status': session}
                    result = self.reconcile()
                    self.assertEqual(result['state'], 'running')
                    self.assertNotIn('resolution', result)
                    self.assertEqual(store.active_count(), 1)
                    self.assertEqual(self.leases(store), 1)
                    self.assertEqual(self.agent.t3.dispatched, [])

    def test_waiting_followup_does_not_start_resolution_clock(self):
        store = self.seed()
        store.begin_turn('req-12345678', previous_turn_ref='turn-1')
        store.confirm_turn('req-12345678')
        self.projection({'turnId': 'turn-1', 'state': 'running'}, {'state': 'running'})
        for session in ('starting', 'running'):
            self.agent.t3.thread['session'] = {'status': session}
            self.agent._now = lambda: '2026-07-28T02:00:00Z'
            store._now = self.agent._now
            result = self.reconcile()
            self.assertEqual(result['state'], 'running')
            self.assertNotIn('resolution', result)
            lifecycle = store.lookup('req-12345678').lifecycle
            self.assertTrue(lifecycle['waiting_for_previous_turn'])
            self.assertIsNone(lifecycle['launch_deadline'])
            self.assertEqual(store.active_count(), 1)
            self.assertEqual(self.leases(store), 1)

    def test_uncorroborated_previous_terminal_arms_grace_before_resolution(self):
        store = self.seed()
        store.begin_turn('req-12345678', previous_turn_ref='turn-1')
        store.confirm_turn('req-12345678')
        self.projection({'state': 'interrupted'}, {'state': 'interrupted'})
        self.agent._now = lambda: '2026-07-28T02:00:00Z'
        store._now = self.agent._now
        first = self.reconcile()
        self.assertNotIn('resolution', first)
        lifecycle = store.lookup('req-12345678').lifecycle
        self.assertFalse(lifecycle['waiting_for_previous_turn'])
        self.assertEqual(lifecycle['launch_deadline'], '2026-07-28T02:00:30Z')
        self.agent._now = lambda: '2026-07-28T02:00:30Z'
        store._now = self.agent._now
        started = self.reconcile()
        self.assertEqual(started['state'], 'unknown')
        self.assertEqual(started['resolution']['deadline'], '2026-07-28T02:05:30Z')
        self.agent._now = lambda: started['resolution']['deadline']
        store._now = self.agent._now
        expired = self.reconcile()
        self.assertTrue(expired['resolution']['blocking'])
        self.assertEqual(self.leases(store), 0)
        for action in ('doctor', 'capacity'):
            health = self.agent.handle({'action': action})
            self.assertEqual(health['state'], 'blocked_offline')
            self.assertEqual(health['resolution'], expired['resolution'])
        self.assertEqual(self.agent.t3.dispatched, [])
        self.agent.handle({'action': 'reconcile', 'request_id': 'req-12345678', 'contain': True})
        self.assertIn('thread.session.stop', [c['type'] for c in self.agent.t3.dispatched])

    def test_settle_skips_waiting_or_reserved_followup(self):
        store = self.seed()
        store.reconcile('req-12345678', state='succeeded', thread_id='thread-1')
        self.projection({'turnId': 'turn-1', 'state': 'completed'},
                        {'turnId': 'turn-1', 'state': 'completed'})
        store.reserve_followup('req-12345678', active_limit=2, reservation_ttl=60)
        request = {'action': 'settle', 'request_id': 'req-12345678', 'thread_id': 'thread-1'}
        reserved = self.agent.handle(request)
        with self.subTest(phase='reserved'):
            self.assertEqual(reserved.get('reason'), 'not_terminal')
        store.begin_turn('req-12345678', previous_turn_ref='turn-1')
        store.confirm_turn('req-12345678')
        waiting = self.agent.handle(request)
        with self.subTest(phase='unobserved'):
            self.assertEqual(waiting.get('reason'), 'not_terminal')
        store.begin_turn('req-12345678', previous_turn_ref='turn-1')
        store.confirm_turn('req-12345678')
        self.assertTrue(store.lookup('req-12345678').lifecycle['waiting_for_previous_turn'])
        queued = self.agent.handle(request)
        self.assertEqual(queued.get('reason'), 'not_terminal')
        self.assertEqual(self.agent.t3.dispatched, [])

    def test_settle_allows_terminal_rejected_followup_without_new_identity(self):
        store = self.seed()
        store.reconcile('req-12345678', state='succeeded', thread_id='thread-1')
        store.begin_turn('req-12345678', previous_turn_ref=None)
        receipt = store.reconcile('req-12345678', state='unknown', thread_id='thread-1', rejected_turn=True)
        self.assertEqual(receipt.state, 'succeeded')
        self.assertTrue(receipt.lifecycle['turn_ref'].startswith('epoch:'))
        self.projection({'turnId': 'turn-1', 'state': 'completed'},
                        {'turnId': 'turn-1', 'state': 'completed'})
        result = self.agent.handle({'action': 'settle', 'request_id': 'req-12345678', 'thread_id': 'thread-1'})
        self.assertEqual(result.get('outcome'), 'settled')
        self.assertEqual([c['type'] for c in self.agent.t3.dispatched], ['thread.settle'])

    def test_settle_skips_receipt_with_blocking_resolution(self):
        store = self.seed()
        store.reconcile('req-12345678', state='unknown', thread_id='thread-1', resolution={
            'reason': 'terminal-unconfirmed', 'deadline': '2026-07-28T01:05:00Z', 'blocking': True})
        # Even a newly matching projection must be reconciled before settling.
        self.projection({'turnId': 'turn-1', 'state': 'completed'},
                        {'turnId': 'turn-1', 'state': 'completed'})
        result = self.agent.handle({'action': 'settle', 'request_id': 'req-12345678', 'thread_id': 'thread-1'})
        self.assertEqual(result['reason'], 'not_terminal')
        self.assertEqual(self.agent.t3.dispatched, [])

    def test_matching_completed_detail_overrides_interrupted_shell(self):
        store = self.seed()
        self.projection({'turnId': 'turn-1', 'state': 'interrupted'},
                        {'turnId': 'turn-1', 'state': 'completed'})
        self.assertEqual(self.reconcile()['state'], 'succeeded')
        self.assertEqual(store.active_count(), 0)
        self.assertEqual(self.leases(store), 0)

    def test_matching_interrupted_detail_releases_capacity_and_lease(self):
        store = self.seed()
        self.projection({'turnId': 'turn-1', 'state': 'interrupted'},
                        {'turnId': 'turn-1', 'state': 'interrupted'})
        self.assertEqual(self.reconcile()['state'], 'interrupted')
        self.assertEqual(store.active_count(), 0)
        self.assertEqual(self.leases(store), 0)

    def test_unresolved_projection_matrix_retains_capacity_and_lease(self):
        cases = {
            'mismatched': ({'turnId': 'turn-1', 'state': 'interrupted'},
                           {'turnId': 'other', 'state': 'completed'}, None),
            'missing-shell-id': ({'state': 'interrupted'},
                                 {'turnId': 'turn-1', 'state': 'completed'}, None),
            'missing-detail-id': ({'turnId': 'turn-1', 'state': 'interrupted'},
                                  {'state': 'completed'}, None),
            'malformed': ({'turnId': 'turn-1', 'state': 'interrupted'}, None,
                          {'thread': {'id': 'thread-1'}}),
            'wrong-thread': ({'turnId': 'turn-1', 'state': 'interrupted'}, None,
                             {'thread': {'id': 'other', 'latestTurn': {'turnId': 'turn-1', 'state': 'completed'}}}),
            'stale': ({'turnId': 'old-turn', 'state': 'completed'},
                      {'turnId': 'old-turn', 'state': 'completed'}, None),
            'running-detail': ({'turnId': 'turn-1', 'state': 'interrupted'},
                               {'turnId': 'turn-1', 'state': 'running'}, None),
        }
        store = self.seed()
        for label, (shell, detail, malformed) in cases.items():
            with self.subTest(label=label):
                self.projection(shell, detail, malformed=malformed)
                result = self.reconcile()
                self.assertEqual(result['state'], 'unknown')
                self.assertEqual(result['resolution']['reason'], 'terminal-unconfirmed')
                self.assertEqual(store.active_count(), 1)
                self.assertEqual(self.leases(store), 1)

    def test_timeout_releases_lease_but_quarantines_capacity_until_identity_resolves(self):
        store = self.seed()
        self.projection({'turnId': 'turn-1', 'state': 'interrupted'},
                        {'turnId': 'other', 'state': 'completed'})
        first = self.reconcile()
        self.assertEqual(first['state'], 'unknown')
        deadline = first['resolution']['deadline']
        self.agent._now = lambda: deadline
        store._now = self.agent._now
        expired = self.reconcile()
        self.assertEqual(expired['state'], 'failed')
        self.assertEqual(expired['resolution']['reason'], 'terminal-unconfirmed')
        self.assertEqual(self.leases(store), 0)
        for action in ('doctor', 'capacity'):
            health = self.agent.handle({'action': action})
            self.assertEqual(health['state'], 'blocked_offline')
            self.assertEqual(health['resolution'], expired['resolution'])
        from t3_fleet.errors import TargetBusy
        for action in ('submit', 'claim', 'followup'):
            with self.subTest(action=action), self.assertRaises(TargetBusy):
                self.agent.handle(dict(self.submit_request, action=action,
                                       request_id='linux-worker-controller-a-87654321-1234-1234-1234-123456789abc', thread_id='thread-1', digest='b' * 64))
        # A terminal timeout is a failed resolution decision, not execution proof.
        # Matching evidence can subsequently clear the quarantine safely.
        self.projection({'turnId': 'turn-1', 'state': 'interrupted'},
                        {'turnId': 'turn-1', 'state': 'completed'})
        self.assertEqual(self.reconcile()['state'], 'succeeded')
        self.assertFalse(store.unresolved())

    def test_observer_uses_same_resolution_deadline(self):
        store = self.seed()
        self.projection({'state': 'interrupted'}, {'state': 'interrupted'})
        first = self.reconcile()
        self.agent._now = lambda: first['resolution']['deadline']
        store._now = self.agent._now
        self.agent.handle({'action': 'observe'})
        receipt = store.lookup('req-12345678')
        self.assertEqual(receipt.state, 'failed')
        self.assertEqual(self.leases(store), 0)

    def test_missing_latest_turn_is_not_no_turn(self):
        self.seed()
        self.agent.t3 = MalformedDetailProjectionT3(
            {'thread': {'id': 'thread-1'}}, shell_latest_turn=None,
            detail_latest_turn=None, session={'status': 'ready'})
        self.assertFalse(self.agent._thread_has_no_turn('thread-1'))

    def test_null_turn_with_live_session_is_not_no_turn(self):
        for status in ("starting", "running"):
            self.agent.t3 = MalformedDetailProjectionT3(
                {"thread": {"id": "thread-1", "latestTurn": None, "session": {"status": status}}},
                shell_latest_turn=None, detail_latest_turn=None, session={"status": "ready"})
            self.assertFalse(self.agent._thread_has_no_turn("thread-1"))

    def test_new_live_turn_clears_terminal_ambiguity_before_deadline(self):
        store = self.seed()
        store.reconcile("req-12345678", state="succeeded", thread_id="thread-1")
        store.begin_turn("req-12345678", previous_turn_ref="turn-1")
        store.confirm_turn("req-12345678")
        self.projection({"turnId": "turn-1", "state": "completed"},
                        {"turnId": "turn-1", "state": "completed"})
        self.agent._now = lambda: "2026-07-28T01:01:00Z"
        store._now = self.agent._now
        first = self.reconcile()
        self.assertEqual(first["state"], "unknown")
        self.agent.t3.thread["session"] = {"status": "running"}
        self.agent.t3.shell_latest_turn = {"turnId": "new-turn", "state": "running"}
        self.agent.t3.detail_latest_turn = {"turnId": "new-turn", "state": "running"}
        live = self.reconcile()
        self.assertEqual(live["state"], "running")
        self.assertNotIn("resolution", live)
        self.agent._now = lambda: "2026-07-28T02:00:00Z"
        store._now = self.agent._now
        self.assertEqual(self.reconcile()["state"], "running")
        self.assertEqual(store.active_count(), 1)
        self.assertEqual(self.leases(store), 1)

    def test_never_started_followup_timeout_stops_owned_session_and_unblocks(self):
        store = self.seed()
        store.reconcile("req-12345678", state="succeeded", thread_id="thread-1")
        store.begin_turn("req-12345678", previous_turn_ref="turn-1")
        self.projection({"turnId": "turn-1", "state": "completed"},
                        {"turnId": "turn-1", "state": "completed"})
        self.agent.t3.thread["session"] = {"status": "ready"}
        self.agent._now = lambda: "2026-07-28T01:01:00Z"
        store._now = self.agent._now
        first = self.reconcile()
        deadline = first["resolution"]["deadline"]
        self.agent._now = lambda: deadline
        store._now = self.agent._now
        original = self.agent.t3.dispatch
        def stop(command):
            result = original(command)
            if command["type"] == "thread.session.stop":
                self.agent.t3.thread["session"] = {"status": "stopped"}
            return result
        snapshot = self.agent.t3.thread_snapshot
        def projected(thread_id):
            result = snapshot(thread_id)
            if self.agent.t3.thread["session"]["status"] == "stopped":
                result["snapshotSequence"] = 9
            return result
        with (patch.object(self.agent.t3, "dispatch", side_effect=stop),
              patch.object(self.agent.t3, "thread_snapshot", side_effect=projected)):
            expired = self.agent.handle({"action": "reconcile", "request_id": "req-12345678", "contain": True})
        self.assertEqual(expired["state"], "failed")
        self.assertFalse(expired["resolution"]["blocking"])
        self.assertIn("thread.session.stop", [c["type"] for c in self.agent.t3.dispatched])
        self.assertFalse(store.unresolved())
        self.assertEqual(store.active_count(), 0)
        self.assertEqual(self.leases(store), 0)
        self.assertEqual(self.agent.handle({"action": "capacity"})["state"], "eligible")

    def test_stale_stopped_session_cannot_clear_live_or_unchanged_detail(self):
        store = self.seed()
        for detail_state in ("running", "completed"):
            self.projection({"turnId": "turn-1", "state": "interrupted"},
                            {"turnId": "other", "state": detail_state})
            first = self.reconcile()
            self.agent._now = lambda: first["resolution"]["deadline"]
            store._now = self.agent._now
            expired = self.reconcile()
            self.assertTrue(expired["resolution"]["blocking"])
            self.assertEqual(self.leases(store), 0)
            self.assertEqual(self.agent.handle({"action": "capacity"})["state"], "blocked_offline")

    def test_legacy_recover_cannot_skip_terminal_ambiguity_deadline(self):
        original = self.fake_t3.dispatch
        def lost_start(command):
            response = original(command)
            if command["type"] == "thread.turn.start":
                raise RuntimeError("lost acknowledgement")
            return response
        with patch.object(self.fake_t3, "dispatch", side_effect=lost_start), self.assertRaises(RuntimeError):
            self.agent.handle(self.submit_request)
        self.projection({"turnId": "turn-1", "state": "interrupted"},
                        {"turnId": "turn-1", "state": "running"})
        self.agent._now = lambda: "2026-07-28T01:02:00Z"
        result = self.agent.handle({"action": "recover", "request_id": "req-12345678"})
        self.assertEqual(result["state"], "unknown")
        self.assertEqual(result["resolution"]["reason"], "terminal-unconfirmed")
        store = self.agent._target_store()
        self.assertEqual(store.active_count(), 1)
        self.assertEqual(self.leases(store), 1)

    def test_legacy_recover_missing_shell_turn_cannot_override_live_detail(self):
        original = self.fake_t3.dispatch
        def lost_start(command):
            response = original(command)
            if command["type"] == "thread.turn.start":
                raise RuntimeError("lost acknowledgement")
            return response
        with patch.object(self.fake_t3, "dispatch", side_effect=lost_start), self.assertRaises(RuntimeError):
            self.agent.handle(self.submit_request)
        self.projection(None, {"turnId": "turn-1", "state": "running"})
        self.agent.t3.thread["session"] = {"status": "ready"}
        self.agent._now = lambda: "2026-07-28T01:01:00Z"
        result = self.agent.handle({"action": "recover", "request_id": "req-12345678"})
        self.assertEqual(result["state"], "unknown")
        store = self.agent._target_store()
        self.assertEqual(store.active_count(), 1)
        self.assertEqual(self.leases(store), 1)

    def test_new_epoch_does_not_inherit_a_contained_timeout_deadline(self):
        store = self.seed()
        store.reconcile("req-12345678", state="unknown", thread_id="thread-1", resolution={
            "reason": "terminal-unconfirmed", "deadline": "2026-07-28T00:00:00Z", "blocking": False})
        store.reserve_followup("req-12345678", active_limit=2, reservation_ttl=60)
        receipt = store.begin_turn("req-12345678", previous_turn_ref="turn-1")
        self.assertNotIn("resolution", receipt.lifecycle)

    def test_reserve_followup_cannot_acquire_quarantined_target_lease(self):
        from t3_fleet.errors import TargetBusy
        store = self.seed()
        self.projection({"turnId": "turn-1", "state": "interrupted"},
                        {"turnId": "other", "state": "completed"})
        first = self.reconcile()
        self.agent._now = lambda: first["resolution"]["deadline"]
        store._now = self.agent._now
        self.reconcile()
        with self.assertRaises(TargetBusy):
            store.reserve_followup("req-12345678", active_limit=2, reservation_ttl=60)
        self.assertEqual(self.leases(store), 0)

    def test_empty_run_history_with_missing_or_unrelated_identity_is_not_no_turn(self):
        for detail in (
            {"id": "other", "runs": [], "latestRunId": None, "activeRunId": None},
            {"id": "thread-1", "runs": []},
            {"id": "thread-1", "runs": [], "latestRunId": "run-1", "activeRunId": None},
            {"id": "thread-1", "runs": [], "latestRunId": None, "activeRunId": "run-1"},
        ):
            with self.subTest(detail=detail):
                self.agent.t3 = MalformedDetailProjectionT3(
                    {"thread": detail}, shell_latest_turn=None,
                    detail_latest_turn=None, session={"status": "ready"})
                self.assertFalse(self.agent._thread_has_no_turn("thread-1"))

    def test_preclaim_error_is_typed_before_claim(self):
        with patch.object(self.agent, '_prepare_submit_envelope_checked',
                          side_effect=ValidationError('project default is unavailable')):
            with self.assertRaises(ValidationError) as caught:
                self.agent.handle(dict(self.submit_request, action='claim',
                                       request_id='linux-worker-controller-a-12345678-1234-1234-1234-123456789abc',
                                       digest='a' * 64))
        self.assertEqual(caught.exception.target_error['code'], 'invalid_request')
        self.assertEqual(caught.exception.target_error['detail'], 'project default is unavailable')
        self.assertEqual(self.agent._target_store().active_count(), 0)

class ControllerDiagnosticTests(unittest.TestCase):
    def setUp(self):
        import test_controller as controller_fixtures
        self.fixture = controller_fixtures.ControllerTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.store = self.fixture.store
        self.controller = self.fixture.controller
        self.transport = self.fixture.transport

    def test_controller_only_recover_requests_containment_in_both_modes(self):
        from dataclasses import replace
        from t3_fleet.controller import FleetController
        for mode in ('single-controller', 'target-authoritative'):
            with self.subTest(mode=mode):
                controller = FleetController(replace(self.controller.config, coordination_mode=mode),
                                             self.store, self.transport, admission=self.fixture.admission)
                rid = 'req-12345678' if mode == 'single-controller' else 'linux-worker-controller-a-87654321-1234-1234-1234-123456789abc'
                controller.submit(dict(self.fixture.request, request_id=rid), prompt='private prompt')
                self.transport.calls.clear()
                controller.status(rid)
                status = self.transport.calls[-1]
                self.assertEqual(status['action'], 'reconcile' if mode == 'target-authoritative' else 'status')
                self.assertFalse(status.get('contain', False))
                controller.recover(rid)
                recover = self.transport.calls[-1]
                self.assertEqual(recover['action'], 'reconcile' if mode == 'target-authoritative' else 'recover')
                self.assertIs(recover.get('contain'), True)

    def test_target_preflight_diagnostic_survives_status_recovery_and_restart(self):
        # The transport can only attach this after decoding a typed envelope.
        error = HostUnavailable('target adapter failed')
        error.target_error = {'code': 'invalid_request', 'detail': 'project default is unavailable'}
        self.transport.fail_on = 'submit'
        self.transport.fail_exc = error
        with self.assertRaises(HostUnavailable) as caught:
            self.controller.submit(self.fixture.request, prompt='private prompt')
        self.assertEqual(caught.exception.request_id, "req-12345678")
        first = self.controller.status('req-12345678')
        self.assertEqual(first['target_error'], error.target_error)
        self.transport.fail_on = None
        self.transport.status_state = 'failed'
        self.assertEqual(self.controller.recover('req-12345678')['target_error'], error.target_error)
        from t3_fleet.store import FleetStore
        reopened = FleetStore(self.store.path)
        self.assertEqual(reopened.get('req-12345678').target_error, error.target_error)

    def test_doctor_capacity_block_unknown_with_shared_resolution(self):
        from t3_fleet.cli import _Application
        self.store.create_request('req-12345678', 'a' * 64, 'linux-worker', 'example-repo')
        for state in ('preflight', 'dispatching', 'unknown'):
            self.store.transition('req-12345678', state)
        resolution = {'reason': 'terminal-unconfirmed', 'deadline': '2026-07-28T01:05:00Z'}
        self.store.record_diagnostics('req-12345678', resolution=resolution)
        transport = Mock()
        transport.call.return_value = {'state': 'eligible', 'active': 0}
        app = _Application(self.controller.config, self.store, transport, self.controller)
        for action in ('doctor', 'capacity'):
            result = getattr(app, action)({'host': 'linux-worker'})
            self.assertEqual(result['state'], 'blocked_offline')
            self.assertEqual(result['hosts'][0]['resolution'], resolution)
        self.assertTrue(all(call.args[1]["action"] == "lookup" for call in transport.call.call_args_list))

    def test_successful_followup_clears_old_resolution_diagnostic(self):
        self.controller.submit(self.fixture.request, prompt="private prompt")
        self.store.transition("req-12345678", "failed")
        self.store.record_diagnostics("req-12345678", resolution={
            "reason": "terminal-unconfirmed", "deadline": "2026-07-28T00:00:00Z", "blocking": False})
        result = self.controller.followup("req-12345678", prompt="continue")
        self.assertEqual(result["state"], "running")
        self.assertNotIn("resolution", result)

    def test_health_refreshes_a_target_resolution_cleared_by_observer(self):
        from t3_fleet.cli import _Application
        self.controller.submit(self.fixture.request, prompt="private prompt")
        self.store.record_diagnostics("req-12345678", resolution={
            "reason": "terminal-unconfirmed", "deadline": "2026-07-28T01:05:00Z"})
        self.store.transition("req-12345678", "unknown")
        # A complete target receipt is required before the local blocker clears.
        record = self.store.get("req-12345678")
        receipt = {"schema": "t3-fleet.agent-result.v1", "request_id": record.request_id,
                   "request_digest": "legacy:" + "a" * 64, "target_id": record.host,
                   "repository": record.repository, "source_revision": fixtures.FULL_SHA,
                   "state": "succeeded", "thread_id": "thread-1", "prompt_bytes": record.prompt_bytes,
                   "prompt_sha256": "a" * 64,
                   "created_at": "2026-07-28T01:00:00Z", "updated_at": "2026-07-28T02:00:00Z"}
        transport = Mock()
        transport.call.side_effect = lambda host, req: receipt if req["action"] == "lookup" else {"state": "eligible", "active": 0}
        self.controller.transport = transport
        app = _Application(self.controller.config, self.store, transport, self.controller)
        self.assertEqual(app.doctor({"host": "linux-worker"})["state"], "eligible")
        self.assertIsNone(self.store.get("req-12345678").resolution)

    def test_doctor_capacity_explain_threadless_unknown_before_recovery(self):
        from t3_fleet.cli import _Application
        self.store.create_request('req-12345678', 'a' * 64, 'linux-worker', 'example-repo')
        for state in ('preflight', 'dispatching', 'unknown'):
            self.store.transition('req-12345678', state)
        transport = Mock()
        transport.call.return_value = {'state': 'eligible', 'active': 0}
        app = _Application(self.controller.config, self.store, transport, self.controller)
        for action in ('doctor', 'capacity'):
            host = getattr(app, action)({'host': 'linux-worker'})['hosts'][0]
            self.assertEqual(host['state'], 'blocked_offline')
            self.assertEqual(host['resolution']['reason'], 'submit-unresolved')
        self.assertTrue(all(call.args[1]["action"] == "lookup" for call in transport.call.call_args_list))

class AdapterDiagnosticTests(unittest.TestCase):
    def test_typed_preflight_error_crosses_adapter_without_raw_stderr(self):
        from test_transport import FakeRunner, host
        from t3_fleet.agent import _agent_error
        from t3_fleet.transport import SshTransport
        error = ValidationError('project default is unavailable')
        error.target_error = {'code': error.code, 'detail': error.message}
        envelope = _agent_error(error, {'action': 'claim'})
        transport = SshTransport(runner=FakeRunner(returncode=2, stdout=json.dumps(envelope)))
        with self.assertRaises(HostUnavailable) as caught:
            transport.call(host(), {'action': 'claim'})
        self.assertEqual(caught.exception.target_error, error.target_error)

    def test_preflight_diagnostics_redact_paths_secrets_and_bound_size(self):
        from t3_fleet.errors import bounded_target_error, bounded_resolution
        for detail in ('private token abc', '/srv/private/repo', 'C:\\private\\repo', 'x' * 513):
            with self.subTest(detail=detail[:20]):
                result = bounded_target_error({'code': 'invalid_request', 'detail': detail})
                self.assertEqual(result['detail'], 'target preflight rejected; details redacted')
        for value in ({'code': []}, {'code': 'untrusted'}, [], None):
            self.assertIsNone(bounded_target_error(value))
        self.assertIsNone(bounded_resolution({'reason': []}))


class PolicyReservationResolutionTests(fixtures._AgentPolicyFixture, unittest.TestCase):
    leases = ReconciliationContractTests.leases

    def test_blocking_timeout_retains_hub_reservation_and_can_renew_live_turn(self):
        rid = fixtures.PEER_REQUEST_ID
        self.agent.handle(dict(self.submit_request, action="claim", request_id=rid, digest="d" * 64))
        store = self.agent._target_store()
        store.reconcile(rid, state="running", thread_id="thread-1", turn_ref="turn-1")
        target = StaleShellProjectionT3(
            shell_latest_turn={"turnId": "turn-1", "state": "interrupted"},
            detail_latest_turn={"turnId": "other", "state": "completed"}, session={"status": "stopped"})
        self.agent.t3 = target
        self.agent._now = lambda: "2026-07-28T01:01:00Z"
        store._now = self.agent._now
        first = self.agent.handle({"action": "reconcile", "request_id": rid})
        self.agent._now = lambda: first["resolution"]["deadline"]
        store._now = self.agent._now
        expired = self.agent.handle({"action": "reconcile", "request_id": rid})
        self.assertTrue(expired["resolution"]["blocking"])
        self.assertEqual(self.policy.release_calls, [])
        self.assertEqual(store.lookup(rid).reservation_state, "active")
        self.assertEqual(self.leases(store), 0)
        target.thread["session"] = {"status": "running"}
        target.shell_latest_turn = {"turnId": "turn-1", "state": "running"}
        target.detail_latest_turn = dict(target.shell_latest_turn)
        live = self.agent.handle({"action": "reconcile", "request_id": rid})
        self.assertEqual(live["state"], "running")
        self.assertEqual(store.lookup(rid).reservation_state, "active")
        self.assertTrue(self.policy.renew_calls)
        self.assertEqual(self.policy.release_calls, [])
        self.assertEqual(self.leases(store), 1)


class Protocol2ContainmentTests(unittest.TestCase):
    def test_timeout_retains_capacity_for_background_nodes_and_subagents(self):
        from tests import test_protocol_v2 as v2
        fixture = v2.ProtocolV2Tests("runTest")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.fixture._seed_admitted_request()
        agent, server = fixture.agent, fixture.server
        store = agent._target_store()
        store.reconcile("req-12345678", state="running", thread_id="thread-1", turn_ref="run-1")
        for key, value in (
            ("providerThreads", [{"pendingBackgroundTasks": [{"taskId": "bg-1"}]}]),
            ("nodes", [{"status": "running"}]),
            ("subagents", [{"status": "running"}]),
        ):
            for variant in ("null-latest", "mismatched-run"):
                with self.subTest(key=key, variant=variant):
                    shell = dict(v2.thread("completed"), latestRunId=None if variant == "null-latest" else "run-0")
                    server.shell["threads"] = [shell]
                    server.projection["thread"] = dict(shell)
                    server.projection["runs"] = [] if variant == "null-latest" else [{"id": "run-0", "status": "interrupted"}]
                    for field in ("providerThreads", "nodes", "subagents"):
                        server.projection[field] = value if field == key else []
                    first = agent.handle({"action": "reconcile", "request_id": "req-12345678"})
                    deadline = first["resolution"]["deadline"]
                    agent._now = lambda: deadline
                    store._now = agent._now
                    expired = agent.handle({"action": "reconcile", "request_id": "req-12345678", "contain": True})
                    self.assertTrue(expired["resolution"]["blocking"])
                    self.assertEqual(ReconciliationContractTests.leases(self, store), 0)
                    self.assertEqual(agent.handle({"action": "capacity"})["state"], "blocked_offline")


if __name__ == "__main__":
    unittest.main()
