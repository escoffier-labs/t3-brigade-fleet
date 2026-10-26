from __future__ import annotations

import copy
import io
import json
import os
import sqlite3
import stat
import subprocess
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from t3_fleet.config import FleetConfig, HostConfig
from t3_fleet.errors import IdempotencyConflict, T3HttpError, ValidationError
from t3_fleet.lane import LaneClient, main
from t3_fleet.lane_store import LaneStore
from t3_fleet.t3_api import T3Client, T3SessionIssuer
from tests.test_t3_api import FakeRunner
from tests.test_protocol_v2 import V2Server

VERSION = '0.0.46-nightly.20261007.2787'


class App:
    def __init__(self, workspace):
        self.desc = {'environmentId': 'env-1', 'serverVersion': VERSION,
                     'orchestrationProtocolVersion': 2}
        self.thread = {'id': 'thread-1', 'projectId': 'project-1', 'worktreePath': None,
                       'modelSelection': {'instanceId': 'custom-codex', 'model': 'example'},
                       'activeProviderThreadId': 'provider-1',
                       'runs': [{'id': 'run-1', 'ordinal': 1, 'status': 'running',
                                 'providerThreadId': 'provider-1'}],
                       'runtimeRequests': [], 'messages': [],
                       'providerThreads': [{'id': 'provider-1', 'driver': 'codex',
                                            'providerInstanceId': 'custom-codex',
                                            'appThreadId': 'thread-1', 'ownerNodeId': None}]}
        self.shell = {'projects': [{'id': 'project-1', 'workspaceRoot': str(workspace)}],
                      'threads': [{'id': 'thread-1', 'projectId': 'project-1',
                                   'latestRunId': 'run-1', 'activeRunId': 'run-1'}]}
        self.calls = []
        self.fail_at = None
        self.block = None

    def descriptor(self):
        return copy.deepcopy(self.desc)

    def shell_snapshot(self):
        return copy.deepcopy(self.shell)

    def thread_snapshot(self, thread_id):
        if thread_id != self.thread['id']:
            raise T3HttpError('unavailable')
        return {'thread': copy.deepcopy(self.thread)}

    def _send(self, thread_id, text, command_id, message_id, target_run_id=None):
        self.calls.append((thread_id, text, command_id, message_id, target_run_id))
        if self.block:
            self.block[0].set()
            self.block[1].wait(5)
        if self.fail_at == len(self.calls):
            raise T3HttpError('private transport detail')
        return {'sequence': len(self.calls)}

    queue_message = _send
    steer_message = _send


class LaneTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root / 'workspace'
        self.workspace.mkdir()
        for args in [('init', '-q'), ('-c', 'user.name=Example', '-c', 'user.email=example@example.invalid',
                                     'commit', '--allow-empty', '-qm', 'init')]:
            subprocess.run(['git', '-C', str(self.workspace), *args], check=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.sha = subprocess.check_output(['git', '-C', str(self.workspace), 'rev-parse', 'HEAD']).decode().strip()
        host = HostConfig('local', True, 'linux', 'local', None, ('agent',), 1, 'example',
                          'http://127.0.0.1:3773', ('t3',), None, str(self.root / 'agent'))  # content-guard: allow loopback-ipv4
        self.config = FleetConfig('local', 1, None, {'local': host}, {})
        self.app = App(self.workspace)
        self.state = self.root / 'state'
        self.client = self.make_client()
        self.binding = {'thread_id': 'thread-1', 'project_id': 'project-1', 'workspace': str(self.workspace)}
        self.event = {'event_id': 'build-1', 'thread_id': 'thread-1', 'run_id': 'run-1',
                      'source_revision': self.sha, 'kind': 'build_result', 'summary': 'Check failed',
                      'exit_code': 1, 'failed_checks': ['unit'], 'evidence_path': 'evidence/result.json'}
        self.packet = {'thread_id': 'thread-1', 'objective': 'Finish task', 'decisions': ['Keep scope'],
                       'verification': ['evidence/result.json'], 'next_action': 'Review result',
                       'output_path': str(self.root / 'packet.md')}

    def make_client(self):
        with patch('t3_fleet.lane.T3Client', return_value=self.app):
            return LaneClient(self.config, 'local', self.state)

    def bind(self):
        return self.client.bind(self.binding)

    def test_binding_persists_and_refuses_rebinding_and_remote_host(self):
        self.assertEqual(self.bind()['status'], 'bound')
        self.assertEqual(self.make_client().bind(self.binding)['environment_id'], 'env-1')
        self.assertEqual(stat.S_IMODE(self.state.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE((self.state / 'lane.db').stat().st_mode), 0o600)
        self.app.desc['environmentId'] = 'env-2'
        with self.assertRaises(IdempotencyConflict):
            self.bind()
        remote = replace(self.config.hosts['local'], transport='ssh')
        with self.assertRaises(ValidationError):
            LaneClient(replace(self.config, hosts={'local': remote}), 'local', self.root / 'remote')
        self.assertEqual(self.app.calls, [])

    def test_delivery_is_durable_and_conflicting_payload_never_sends(self):
        self.bind()
        receipt = self.client.deliver(self.event)
        self.assertEqual(receipt['status'], 'accepted')
        self.assertEqual(receipt['sequence'], 1)
        self.assertEqual(self.make_client().deliver(self.event), receipt)
        with self.assertRaises(IdempotencyConflict):
            self.client.deliver({**self.event, 'summary': 'Different'})
        self.assertEqual(len(self.app.calls), 1)
        self.assertIn('External evidence', self.app.calls[0][1])
        self.assertIn('not instructions', self.app.calls[0][1])
        self.assertIsNone(self.app.calls[0][-1])
        self.assertNotIn(b'Check failed', (self.state / 'lane.db').read_bytes())

    def test_stale_run_or_revision_never_sends_and_steer_targets_exact_run(self):
        self.bind()
        for field, value in [('run_id', 'older'), ('source_revision', '0' * 40)]:
            event = {**self.event, 'event_id': field, field: value}
            self.assertEqual(self.client.deliver(event)['status'], 'stale')
        self.assertEqual(self.app.calls, [])
        self.client.deliver({**self.event, 'mode': 'steer'})
        self.assertEqual(self.app.calls[0][-1], 'run-1')

    def test_identity_workspace_and_fleet_guards_are_rechecked(self):
        self.bind()
        for change in ('environment', 'workspace', 'project', 'ownership', 'malformed'):
            with self.subTest(change=change):
                self.app = App(self.workspace)
                self.client = self.make_client()
                db = self.root / 'agent' / 'target.db'
                if db.exists():
                    db.unlink()
                if change == 'environment':
                    self.app.desc['environmentId'] = 'other'
                elif change == 'workspace':
                    self.app.thread['worktreePath'] = str(self.root)
                elif change == 'project':
                    self.app.thread['projectId'] = 'other'
                else:
                    db.parent.mkdir(exist_ok=True)
                    if change == 'ownership':
                        with sqlite3.connect(db) as conn:
                            conn.execute('CREATE TABLE target_requests(thread_id TEXT)')
                            conn.execute("INSERT INTO target_requests VALUES ('thread-1')")
                    else:
                        db.write_text('broken')
                with self.assertRaises(ValidationError):
                    self.client.deliver(self.event)
                with self.assertRaises(ValidationError):
                    self.client.context(self.packet)
                self.assertEqual(self.app.calls, [])

    def test_uncertain_acceptance_survives_restart_without_retry(self):
        self.bind()
        self.app.fail_at = 1
        receipt = self.client.deliver(self.event)
        self.assertEqual(receipt['status'], 'unknown')
        self.assertEqual(self.make_client().deliver(self.event), receipt)
        self.assertEqual(len(self.app.calls), 1)
        self.assertNotIn('private transport', json.dumps(receipt))

    def test_concurrent_clients_reserve_once_before_io(self):
        self.bind()
        entered, release = threading.Event(), threading.Event()
        self.app.block = entered, release
        result = []
        other = self.make_client()
        worker = threading.Thread(target=lambda: result.append(self.client.deliver(self.event)))
        worker.start()
        try:
            self.assertTrue(entered.wait(3))
            self.assertEqual(other.deliver(self.event)['status'], 'unknown')
            with self.assertRaises(IdempotencyConflict):
                other.deliver({**self.event, 'summary': 'conflict'})
            self.assertEqual(len(self.app.calls), 1)
        finally:
            release.set()
            worker.join(5)
        self.assertEqual(result[0]['status'], 'accepted')

    def test_context_is_private_exclusive_and_contains_only_selected_facts(self):
        self.bind()
        (self.workspace / 'changed.txt').write_text('private file contents')
        self.app.thread['messages'] = [{'text': 'private history'}]
        receipt = self.client.context(self.packet)
        path = Path(receipt['output_path'])
        text = path.read_text()
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        for expected in ['Caller-provided', 'Finish task', self.sha, 'changed.txt', 'run-1', 'env-1']:
            self.assertIn(expected, text)
        for absent in ['private history', 'private file contents']:
            self.assertNotIn(absent, text)
        with self.assertRaises(ValidationError):
            self.client.context(self.packet)
        self.assertEqual(path.read_text(), text)
        self.assertEqual(self.app.calls, [])

    def test_context_refuses_symlink_and_public_parent(self):
        self.bind()
        target = self.root / 'target'
        target.write_text('preserve')
        path = Path(self.packet['output_path'])
        path.symlink_to(target)
        with self.assertRaises(ValidationError):
            self.client.context(self.packet)
        self.assertEqual(target.read_text(), 'preserve')
        path.unlink()
        public = self.root / 'public'
        public.mkdir(mode=0o755)
        with self.assertRaises(ValidationError):
            self.client.context({**self.packet, 'output_path': str(public / 'packet.md')})

    def test_compact_queues_reference_then_exact_slash_with_durable_ids(self):
        self.bind()
        receipt = self.client.compact(self.packet)
        self.assertEqual(receipt['status'], 'accepted')
        self.assertTrue(receipt['packet_prepared'])
        self.assertEqual(len(receipt['messages']), 2)
        self.assertIn(self.packet['output_path'], self.app.calls[0][1])
        self.assertEqual(self.app.calls[1][1], '/compact')
        self.assertIsNone(self.app.calls[1][-1])
        self.assertEqual(self.make_client().compact(self.packet), receipt)
        self.assertEqual(len(self.app.calls), 2)

    def test_compact_allows_its_queued_reference_to_advance_latest_run(self):
        self.bind()
        original = self.app.queue_message
        def advancing(*args):
            result = original(*args)
            if len(self.app.calls) == 1:
                self.app.thread['runs'].append({'id': 'reference-run', 'ordinal': 2, 'status': 'queued'})
                self.app.shell['threads'][0]['latestRunId'] = 'reference-run'
            return result
        self.app.queue_message = advancing
        result = self.client.compact(self.packet)
        self.assertEqual(result['status'], 'accepted')
        self.assertEqual(self.app.calls[-1][1], '/compact')

    def test_delivery_rechecks_run_and_head_after_reservation(self):
        self.bind()
        original = self.client.store.claim
        def changed(*args, **kwargs):
            result = original(*args, **kwargs)
            self.app.thread['runs'].append({'id': 'new-run', 'ordinal': 2, 'status': 'running'})
            self.app.shell['threads'][0]['latestRunId'] = 'new-run'
            return result
        with patch.object(self.client.store, 'claim', side_effect=changed):
            result = self.client.deliver(self.event)
        self.assertEqual(result['status'], 'stale')
        self.assertEqual(self.app.calls, [])

    def test_crash_before_second_ack_keeps_first_acceptance_and_second_unknown(self):
        self.bind()
        original = self.app.queue_message
        def interrupted(*args):
            if len(self.app.calls) == 1:
                raise KeyboardInterrupt()
            return original(*args)
        self.app.queue_message = interrupted
        with self.assertRaises(KeyboardInterrupt):
            self.client.compact(self.packet)
        result = self.make_client().compact(self.packet)
        self.assertEqual(result['status'], 'partial')
        self.assertEqual([m['status'] for m in result['messages']], ['accepted', 'unknown'])
        self.assertEqual(len(self.app.calls), 1)

    def test_unsupported_compaction_retains_packet_without_sending(self):
        self.bind()
        for driver, version in [('claude', VERSION), ('codex', '0.0.45')]:
            self.app.thread['providerThreads'][0]['driver'] = driver
            self.app.desc['serverVersion'] = version
            payload = {**self.packet, 'output_path': str(self.root / f'{driver}.md')}
            result = self.client.compact(payload)
            self.assertEqual(result['status'], 'unsupported')
            self.assertTrue(Path(payload['output_path']).exists())
        self.assertEqual(self.app.calls, [])

    def test_compaction_partial_and_unknown_stop_and_do_not_retry(self):
        self.bind()
        self.app.fail_at = 2
        receipt = self.client.compact(self.packet)
        self.assertEqual(receipt['status'], 'partial')
        self.assertEqual([m['status'] for m in receipt['messages']], ['accepted', 'unknown'])
        self.assertEqual(self.make_client().compact(self.packet), receipt)
        self.assertEqual(len(self.app.calls), 2)
        self.app.fail_at = 3
        payload = {**self.packet, 'output_path': str(self.root / 'second.md')}
        second = self.client.compact(payload)
        self.assertEqual(second['status'], 'unknown')
        self.assertEqual([m['status'] for m in second['messages']], ['unknown', 'not_sent'])
        self.assertEqual(len(self.app.calls), 3)

    def test_attention_is_bound_read_only_bounded_and_keeps_unavailable_separate(self):
        self.bind()
        self.app.fail_at = 1
        self.client.deliver(self.event)
        self.app.thread['runs'][0]['status'] = 'failed'
        self.app.thread['runtimeRequests'] = [{'id': 'request-1', 'kind': 'permission',
                                              'status': 'pending', 'prompt': 'private prompt'}]
        self.app.thread['messages'] = [{'text': 'private history'}]
        before = (self.state / 'lane.db').read_bytes()
        report = self.client.attention()
        self.assertEqual(len(report['lanes']), 1)
        lane = report['lanes'][0]
        self.assertTrue(lane['needs_review'])
        self.assertEqual(lane['pending_requests'], [{'id': 'request-1', 'kind': 'permission'}])
        self.assertEqual(len(lane['unknown_deliveries']), 1)
        self.assertNotIn('private', json.dumps(report))
        self.assertEqual((self.state / 'lane.db').read_bytes(), before)
        self.assertEqual(len(self.app.calls), 1)
        self.app.desc['environmentId'] = 'other'
        report = self.client.attention()
        self.assertEqual(report['lanes'], [])
        self.assertEqual(len(report['unavailable']), 1)

    def test_invalid_inputs_never_send(self):
        self.bind()
        for bad in [None, [], {}, {**self.event, 'summary': 'x' * 4001},
                    {**self.event, 'exit_code': True}, {**self.event, 'mode': 'restart'},
                    {**self.event, 'failed_checks': ['x'] * 33},
                    {**self.event, 'source_revision': 'bad'}, {**self.event, 'extra': 'value'}]:
            with self.subTest(bad=type(bad)):
                with self.assertRaises(ValidationError):
                    self.client.deliver(bad)
        self.assertEqual(self.app.calls, [])

    def test_maximum_unicode_summary_delivers_through_real_transport_seam(self):
        server = V2Server()
        server.descriptor.update(environmentId='env-1', serverVersion=VERSION)
        server.shell['projects'][0]['workspaceRoot'] = str(self.workspace)
        transport = T3Client(self.config.hosts['local'].t3_url,
                             T3SessionIssuer(('t3',), runner=FakeRunner()), opener=server)
        with patch('t3_fleet.lane.T3Client', return_value=transport):
            client = LaneClient(self.config, 'local', self.state)
        client.bind(self.binding)
        with patch('t3_fleet.t3_api.websocket_rpc', side_effect=server.rpc):
            result = client.deliver({**self.event, 'kind': 'prerequisite', 'summary': '\U0001f600' * 4000})
        self.assertEqual(result['status'], 'accepted')
        self.assertEqual(len(server.commands), 1)
        self.assertIn('\U0001f600' * 4000, server.commands[0]['text'])

    def test_attention_output_stays_within_64_kibibytes_without_acknowledging(self):
        self.bind()
        self.app.fail_at = 1
        # A long evidence path must not become an unbounded attention summary.
        original = self.client.store.unknown
        unknown = [{'operation_id': 'x' * 512, 'output_path': 'p' * 4096,
                    'status': 'unknown'} for _ in range(32)]
        with patch.object(self.client.store, 'unknown', return_value=unknown):
            result = self.client.attention()
        self.assertLessEqual(len(json.dumps(result).encode()), 65536)
        self.assertTrue(result['lanes'][0]['unknown_truncated'])
        self.assertEqual(original('env-1', 'thread-1'), [])

    def test_state_refuses_public_directory_and_symlink_database(self):
        public = self.root / 'public-state'
        public.mkdir(mode=0o755)
        with self.assertRaises(ValidationError):
            LaneStore(public / 'lane.db')
        private = self.root / 'other-state'
        private.mkdir(mode=0o700)
        (private / 'lane.db').symlink_to(self.state / 'lane.db')
        with self.assertRaises(ValidationError):
            LaneStore(private / 'lane.db')

    def test_cli_malformed_live_descriptor_and_nested_json_return_one_safe_error(self):
        for descriptor, raw in [([], json.dumps(self.binding)),
                                (self.app.desc, '[' * 2000 + '0' + ']' * 2000)]:
            with self.subTest(nested=raw.startswith('[')):
                self.app.desc = descriptor
                with patch('sys.stdin', io.StringIO(raw)), patch('sys.stdout', new_callable=io.StringIO) as out, \
                     patch('t3_fleet.lane.load_config', return_value=self.config), \
                     patch('t3_fleet.lane.LaneClient', return_value=self.client):
                    code = main(['--config', 'fleet.toml', '--host', 'local',
                                 '--state-dir', str(self.state), 'bind'])
                self.assertEqual(code, 2)
                self.assertEqual(json.loads(out.getvalue())['status'], 'error')
        self.assertEqual(self.app.calls, [])

    def test_context_rechecks_fleet_ownership_immediately_before_file_creation(self):
        self.bind()
        original = self.client._head
        def claimed(live):
            result = original(live)
            db = self.root / 'agent' / 'target.db'
            db.parent.mkdir()
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE target_requests(thread_id TEXT)')
                conn.execute("INSERT INTO target_requests VALUES ('thread-1')")
            return result
        with patch.object(self.client, '_head', side_effect=claimed):
            with self.assertRaises(ValidationError):
                self.client.context(self.packet)
        self.assertFalse(Path(self.packet['output_path']).exists())

    def test_cli_rejects_oversized_stdin_before_constructing_client(self):
        with patch('sys.stdin', io.StringIO('x' * 70000)), patch('sys.stdout', new_callable=io.StringIO) as out, \
             patch('t3_fleet.lane.LaneClient') as client:
            code = main(['--config', 'missing', '--host', 'local', '--state-dir', str(self.state), 'deliver'])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(out.getvalue())['status'], 'error')
        client.assert_not_called()


class LaneWireTests(unittest.TestCase):
    def test_queue_and_steer_use_verified_v2_wire_and_revoke_sessions(self):
        server = V2Server()
        runner = FakeRunner()
        client = T3Client('http://127.0.0.1:3773', T3SessionIssuer(('t3',), runner=runner), opener=server)  # content-guard: allow loopback-ipv4
        with patch('t3_fleet.t3_api.websocket_rpc', side_effect=server.rpc):
            client.queue_message('thread-1', 'external', 'command-1', 'message-1')
            client.steer_message('thread-1', 'external', 'command-2', 'message-2', 'run-1')
        self.assertEqual([c['dispatchMode'] for c in server.commands],
                         [{'type': 'queue_after_active'}, {'type': 'steer_active', 'targetRunId': 'run-1'}])
        self.assertEqual(server.commands[0], {'type': 'message.dispatch', 'createdBy': 'agent',
                         'creationSource': 'server', 'commandId': 'command-1', 'threadId': 'thread-1',
                         'messageId': 'message-1', 'text': 'external', 'attachments': [],
                         'dispatchMode': {'type': 'queue_after_active'}})
        self.assertEqual(sum('revoke' in call for call in runner.calls), 2)

    def test_legacy_and_malformed_dispatch_acknowledgement_fail_closed(self):
        server = V2Server()
        server.descriptor = {'serverVersion': '0.0.45'}
        client = T3Client('http://127.0.0.1:3773', T3SessionIssuer(('t3',), runner=FakeRunner()), opener=server)  # content-guard: allow loopback-ipv4
        with patch('t3_fleet.t3_api.websocket_rpc') as rpc:
            with self.assertRaises(T3HttpError):
                client.queue_message('t', '/compact', 'c', 'm')
            rpc.assert_not_called()
        client._protocol = 2
        with patch('t3_fleet.t3_api.websocket_rpc', return_value={'sequence': True}):
            with self.assertRaises(T3HttpError):
                client.queue_message('t', '/compact', 'c', 'm')
