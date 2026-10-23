from __future__ import annotations

import json
import re
import unittest
from pathlib import Path
from typing import Any

from t3_fleet.errors import POLICY_REASONS
from t3_fleet.models import JOB_STATES


ROOT = Path(__file__).resolve().parents[1]
REQUEST_SCHEMA = ROOT / "schemas" / "request.schema.json"
RESULT_SCHEMA = ROOT / "schemas" / "result.schema.json"
TOOL_CALL_INPUT_SCHEMA = ROOT / "schemas" / "tool-call.input.schema.json"

REQUEST_ACTIONS = {
    "doctor",
    "capacity",
    "submit",
    "status",
    "wait",
    "followup",
    "interrupt",
    "approve",
    "answer",
    "recover",
    "tick",
}
REQUEST_ID_PATTERN = r"^[a-z0-9][a-z0-9-]{7,63}$"
PEER_REQUEST_ID_PATTERN = (
    r"^[a-z0-9][a-z0-9-]{0,63}-[a-z0-9][a-z0-9-]{0,63}-"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
SOURCE_REVISION_PATTERN = r"^[0-9a-f]{40}$"

REQUEST_FORBIDDEN_FIELDS = {
    "argv",
    "command",
    "raw_command",
    "endpoint",
    "env",
    "environment",
    "token",
    "password",
    "secret",
    "credential",
    "api_key",
    "base_url",
    "t3_url",
    "ssh_alias",
    "model_selection",
    "model_instance",
    "model",
}
RESULT_FORBIDDEN_FIELDS = (REQUEST_FORBIDDEN_FIELDS - {
    "model_selection",
    "model_instance",
    "model",
}) | {
    "prompt",
    "prompt_file",
    "repository_path",
    "workspace_root",
    "worktree_path",
    "path",
}


def load_schema(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise AssertionError(f"{path.name} must contain one JSON object")
    return value


def object_schemas(value: Any):
    if isinstance(value, dict):
        if value.get("type") == "object":
            yield value
        for child in value.values():
            yield from object_schemas(child)
    elif isinstance(value, list):
        for child in value:
            yield from object_schemas(child)


def find_target_rejection_rule(schema: dict[str, Any]) -> dict[str, Any]:
    for rule in schema.get("allOf", []):
        action_if = rule.get("if", {}).get("properties", {}).get("action", {})
        if action_if.get("not", {}).get("const") == "recover":
            if rule.get("then", {}).get("properties", {}).get("target") is False:
                return rule
    raise AssertionError(
        "missing allOf rule that sets target:false for non-recover actions"
    )


def accepting_branches(schema: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    """Which closed `oneOf` branches could accept this exact result envelope.

    A stdlib stand-in for the part of JSON Schema that decides *which* branch a
    result lands in: the union bound at the top level, each branch's closed
    property set and required keys, and the discriminating `const`/`enum`
    scalars. It is not a validator. It exists because a result envelope that
    matches no branch at all is invalid however well formed it looks, and the
    structural assertions elsewhere in this file cannot see that.
    """
    if not set(payload) <= set(schema["properties"]):
        return []
    accepted: list[str] = []
    for branch in schema["oneOf"]:
        properties = branch["properties"]
        if not set(payload) <= set(properties):
            continue
        if not set(branch.get("required", [])) <= set(payload):
            continue
        if any(
            "const" in properties[key] and payload[key] != properties[key]["const"]
            for key in payload
        ):
            continue
        if any(
            "enum" in properties[key] and payload[key] not in properties[key]["enum"]
            for key in payload
        ):
            continue
        accepted.append(branch["title"])
    return accepted


def property_names(value: Any):
    if isinstance(value, dict):
        properties = value.get("properties")
        if isinstance(properties, dict):
            yield from properties
        for child in value.values():
            yield from property_names(child)
    elif isinstance(value, list):
        for child in value:
            yield from property_names(child)


class SchemaTests(unittest.TestCase):
    def test_submit_schema_accepts_automatic_and_explicit_seats(self):
        try:
            from jsonschema import Draft202012Validator
        except ImportError:
            self.skipTest("optional JSON Schema tooling is not installed")

        validator = Draft202012Validator(load_schema(REQUEST_SCHEMA))
        automatic = {
            "action": "submit",
            "host": "auto",
            "repository": "example-repo",
            "source_revision": "a" * 40,
            "title": "Repair",
            "prompt": "work",
        }
        self.assertEqual(list(validator.iter_errors(automatic)), [])
        explicit = dict(automatic, host="linux-worker", seat="coder")
        self.assertEqual(list(validator.iter_errors(explicit)), [])
        for seat in (None, "", 42):
            with self.subTest(seat=seat):
                self.assertTrue(list(validator.iter_errors(dict(automatic, seat=seat))))

    def test_schema_documents_parse_and_are_closed(self) -> None:
        for path in (REQUEST_SCHEMA, RESULT_SCHEMA):
            with self.subTest(path=path.name):
                schema = load_schema(path)
                self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
                self.assertEqual(schema["type"], "object")
                self.assertFalse(schema["additionalProperties"])
                nested_objects = list(object_schemas(schema))
                self.assertGreaterEqual(len(nested_objects), 1)
                self.assertTrue(
                    all(item.get("additionalProperties") is False for item in nested_objects),
                    "every object-shaped payload must reject unknown fields",
                )

    def test_request_actions_and_identifiers_are_fixed(self) -> None:
        schema = load_schema(REQUEST_SCHEMA)
        properties = schema["properties"]
        self.assertEqual(set(properties["action"]["enum"]), REQUEST_ACTIONS)
        request_id_defs = schema["$defs"]["request_id"]
        self.assertEqual(
            request_id_defs["anyOf"],
            [
                {"$ref": "#/$defs/legacy_request_id"},
                {"$ref": "#/$defs/peer_request_id"},
            ],
        )
        self.assertEqual(
            schema["$defs"]["legacy_request_id"]["pattern"],
            REQUEST_ID_PATTERN,
        )
        self.assertEqual(
            schema["$defs"]["peer_request_id"]["pattern"],
            PEER_REQUEST_ID_PATTERN,
        )
        self.assertEqual(
            schema["$defs"]["peer_request_id"]["maxLength"],
            166,
        )
        self.assertEqual(
            properties["source_revision"]["pattern"],
            SOURCE_REVISION_PATTERN,
        )

        legacy_request_id = re.compile(REQUEST_ID_PATTERN)
        peer_request_id = re.compile(PEER_REQUEST_ID_PATTERN)
        self.assertIsNotNone(legacy_request_id.fullmatch("agent-20260727-example"))
        self.assertIsNone(legacy_request_id.fullmatch("short"))
        self.assertIsNone(legacy_request_id.fullmatch("Agent-20260727-example"))
        self.assertIsNotNone(
            peer_request_id.fullmatch(
                "linux-worker-controller-a-12345678-1234-1234-1234-123456789abc"
            )
        )
        self.assertIsNone(
            peer_request_id.fullmatch(
                "linux-worker-controller-a-12345678-1234-1234-1234-123456789ab"
            )
        )
        short_peer_id = "a-b-12345678-1234-1234-1234-123456789abc"
        self.assertIsNotNone(peer_request_id.fullmatch(short_peer_id))
        self.assertIsNotNone(legacy_request_id.fullmatch(short_peer_id))

        revision = re.compile(properties["source_revision"]["pattern"])
        self.assertIsNotNone(
            revision.fullmatch("0123456789abcdef0123456789abcdef01234567")
        )
        self.assertIsNone(
            revision.fullmatch("0123456789ABCDEF0123456789ABCDEF01234567")
        )
        self.assertIsNone(revision.fullmatch("0123456789abcdef"))

    def test_request_prompt_has_no_command_endpoint_or_environment_escape_hatch(self) -> None:
        schema = load_schema(REQUEST_SCHEMA)
        names = set(property_names(schema))
        self.assertTrue({"prompt", "prompt_file"}.issubset(names))
        self.assertEqual(schema["properties"]["prompt"]["maxLength"], 120000)
        self.assertTrue(
            names.isdisjoint(REQUEST_FORBIDDEN_FIELDS),
            f"unsafe request fields: {sorted(names & REQUEST_FORBIDDEN_FIELDS)}",
        )

        self.assertNotIn("model_selection", schema["$defs"])
        self.assertNotIn("model_selection", schema["properties"])

    def test_answer_request_has_a_closed_user_input_dictionary(self) -> None:
        schema = load_schema(REQUEST_SCHEMA)
        properties = schema["properties"]
        answers = properties["answers"]

        self.assertEqual(properties["input_id"]["type"], "string")
        self.assertEqual(answers["type"], "object")
        self.assertFalse(answers["additionalProperties"])
        self.assertEqual(answers["minProperties"], 1)
        self.assertEqual(answers["maxProperties"], 100)
        self.assertEqual(
            answers["patternProperties"],
            {r"^.{1,256}$": {}},
        )

        answer_rule = next(
            rule
            for rule in schema["allOf"]
            if rule["if"]["properties"]["action"].get("const") == "answer"
        )
        self.assertEqual(
            set(answer_rule["then"]["required"]),
            {"request_id", "input_id", "answers"},
        )

    def test_reconciliation_diagnostics_are_closed_and_bounded(self) -> None:
        schema = json.loads((Path(__file__).resolve().parents[1] / "schemas" / "result.schema.json").read_text())
        for key in ("resolution", "target_error"):
            self.assertFalse(schema["$defs"][key]["additionalProperties"])
            self.assertIn(key, schema["properties"])
            self.assertIn(key, schema["oneOf"][0]["properties"])
        self.assertEqual(schema["$defs"]["target_error"]["properties"]["detail"]["maxLength"], 512)
        for branch in schema["oneOf"][1:3]:
            self.assertIn("resolution", branch["properties"]["hosts"]["items"]["properties"])

    def test_result_contract_has_six_closed_output_branches(self) -> None:
        schema = load_schema(RESULT_SCHEMA)
        self.assertEqual(
            set(schema["properties"]["schema"]["enum"]),
            {"t3-fleet.result.v1", "t3-fleet.error.v1"},
        )
        branches = schema["oneOf"]
        self.assertEqual(len(branches), 6)
        self.assertTrue(
            all(
                branch.get("type") == "object"
                and branch.get("additionalProperties") is False
                for branch in branches
            )
        )

        per_request, doctor, capacity, error, tick, queued = branches
        self.assertEqual(tick["title"], "Tick result")
        self.assertEqual(queued["title"], "Queued submit result")
        self.assertNotIn("action", per_request["properties"])
        self.assertEqual(
            set(per_request["properties"]["state"]["enum"]),
            set(JOB_STATES),
        )
        self.assertEqual(
            schema["$defs"]["request_id"]["anyOf"],
            [
                {"$ref": "#/$defs/legacy_request_id"},
                {"$ref": "#/$defs/peer_request_id"},
            ],
        )
        self.assertEqual(
            per_request["properties"]["request_id"],
            {"$ref": "#/$defs/request_id"},
        )
        self.assertEqual(per_request["properties"]["output"]["maxLength"], 16384)
        self.assertEqual(
            set(per_request["required"]),
            {
                "schema",
                "request_id",
                "host",
                "state",
                "thread_id",
                "needs_attention",
            },
        )

        self.assertEqual(doctor["properties"]["action"]["const"], "doctor")
        self.assertEqual(
            set(doctor["properties"]["state"]["enum"]),
            {"eligible", "blocked_offline"},
        )
        doctor_host = doctor["properties"]["hosts"]["items"]
        self.assertEqual(
            set(doctor_host["properties"]["state"]["enum"]),
            {
                "eligible",
                "disabled",
                "blocked_offline",
                "incompatible",
                "at_capacity",
            },
        )
        self.assertEqual(
            doctor_host["properties"]["project_registered"]["type"],
            "boolean",
        )
        self.assertEqual(doctor_host["properties"]["active"]["minimum"], 0)
        self.assertEqual(doctor_host["properties"]["limit"]["minimum"], 1)
        self.assertIn("approved_model_admission", doctor_host["properties"])
        self.assertIn("observed_project_default", doctor_host["properties"])
        self.assertIn("project_default_drift", doctor_host["properties"])
        self.assertFalse(doctor_host["additionalProperties"])
        self.assertIn(
            "expires_at",
            schema["$defs"]["admission"]["required"],
        )

        self.assertEqual(capacity["properties"]["action"]["const"], "capacity")
        self.assertEqual(capacity["properties"]["state"]["const"], "eligible")
        self.assertIn("global_limit", capacity["required"])
        self.assertEqual(
            capacity["properties"]["global_limit"]["minimum"],
            1,
        )
        capacity_host = capacity["properties"]["hosts"]["items"]
        self.assertEqual(
            set(capacity_host["required"]),
            {"host", "state", "active", "limit", "circuit_open"},
        )
        self.assertEqual(
            set(capacity_host["properties"]["state"]["enum"]),
            {"eligible", "disabled", "blocked_offline"},
        )
        self.assertFalse(capacity_host["additionalProperties"])

        self.assertEqual(error["properties"]["schema"]["const"], "t3-fleet.error.v1")
        self.assertEqual(set(error["required"]), {"schema", "error"})
        self.assertEqual(
            error["properties"]["error"]["$ref"],
            "#/$defs/error_detail",
        )
        error_detail = schema["$defs"]["error_detail"]
        self.assertFalse(error_detail["additionalProperties"])
        self.assertEqual(set(error_detail["required"]), {"code", "message"})
        self.assertEqual(
            set(error_detail["properties"]["code"]["enum"]),
            {
                "invalid_config",
                "invalid_request",
                "host_unavailable",
                "idempotency_conflict",
                "policy_rejected",
                "policy_revision_conflict",
                "unknown_state",
                "t3_auth_failed",
                "t3_http_failed",
                "internal_error",
            },
        )
        self.assertEqual(error_detail["properties"]["message"]["maxLength"], 512)
        self.assertEqual(error_detail["properties"]["detail"]["maxLength"], 512)
        self.assertNotIn("detail", error_detail["required"])

    def test_a_model_plan_is_a_distinct_closed_definition(self) -> None:
        schema = load_schema(RESULT_SCHEMA)
        plan = schema["$defs"]["model_plan"]
        self.assertFalse(plan["additionalProperties"])
        # A plan is planned, is Hub-only, and carries the native launch model
        # separately from the canonical model identity.
        self.assertEqual(plan["properties"]["state"]["const"], "planned")
        self.assertEqual(plan["properties"]["source"]["const"], "hub")
        self.assertLessEqual(
            {"launch_model", "launch_model_explicit"}, set(plan["properties"])
        )
        self.assertIn("launch_model", plan["required"])
        self.assertIn("launch_model_explicit", plan["required"])
        # And it is not the admission definition wearing another name.
        admission = schema["$defs"]["admission"]
        # An admission may carry the native launch model, but never requires it.
        self.assertIn("launch_model", admission["properties"])
        self.assertNotIn("launch_model", admission["required"])
        self.assertNotIn("state", admission["properties"])
        self.assertEqual(set(admission["properties"]["source"]["enum"]), {"hub", "lkg"})
        per_request = schema["oneOf"][0]
        self.assertEqual(
            per_request["properties"]["model_plan"], {"$ref": "#/$defs/model_plan"}
        )
        self.assertNotIn("model_plan", per_request["required"])

    def test_a_caller_cannot_name_a_model_plan_or_admission(self) -> None:
        # The Fleet Hub decides the model. Neither surface a caller can reach
        # has a field for one, and both are closed.
        for path in (REQUEST_SCHEMA, TOOL_CALL_INPUT_SCHEMA):
            with self.subTest(schema=path.name):
                schema = load_schema(path)
                self.assertFalse(schema["additionalProperties"])
                names = set(property_names(schema))
                self.assertNotIn("model_plan", names)
                self.assertNotIn("model_admission", names)
                self.assertNotIn("model_selection", names)

    def test_result_contract_disallows_private_fields(self) -> None:
        schema = load_schema(RESULT_SCHEMA)
        names = set(property_names(schema))
        self.assertTrue(
            names.isdisjoint(RESULT_FORBIDDEN_FIELDS),
            f"private result fields: {sorted(names & RESULT_FORBIDDEN_FIELDS)}",
        )

    def test_recover_allows_optional_target_lookup_hint(self) -> None:
        schema = load_schema(REQUEST_SCHEMA)
        properties = schema["properties"]
        self.assertIn("target", properties)
        self.assertEqual(
            properties["target"]["pattern"],
            properties["host"]["pattern"],
        )
        self.assertNotIn(
            "target",
            set(properties["action"]["enum"]),
        )
        for internal_action in (
            "claim",
            "lookup",
            "reconcile",
            "reserve_followup",
            "release",
        ):
            self.assertNotIn(internal_action, properties["action"]["enum"])

        status_wait_rule = next(
            rule
            for rule in schema["allOf"]
            if rule["if"]["properties"]["action"].get("enum")
            == ["status", "wait", "interrupt"]
        )
        self.assertEqual(
            set(status_wait_rule["then"]["required"]),
            {"request_id"},
        )

    def test_recover_requires_request_id_or_target(self) -> None:
        schema = load_schema(REQUEST_SCHEMA)
        recover_rule = next(
            rule
            for rule in schema["allOf"]
            if rule["if"]["properties"]["action"].get("const") == "recover"
        )
        self.assertEqual(
            recover_rule["then"]["anyOf"],
            [
                {"required": ["request_id"]},
                {"required": ["target"]},
            ],
        )

    def test_error_result_allows_optional_redacted_request_id(self) -> None:
        schema = load_schema(RESULT_SCHEMA)
        error_branch = next(
            branch
            for branch in schema["oneOf"]
            if branch.get("title") == "Error result"
        )
        self.assertFalse(error_branch["additionalProperties"])
        self.assertIn("request_id", error_branch["properties"])
        self.assertNotIn("request_id", error_branch.get("required", []))
        self.assertEqual(
            error_branch["properties"]["request_id"],
            {"$ref": "#/$defs/request_id"},
        )

    def test_submit_schema_allows_omitted_request_id(self) -> None:
        schema = load_schema(REQUEST_SCHEMA)
        submit_rule = next(
            rule
            for rule in schema["allOf"]
            if rule["if"]["properties"]["action"].get("const") == "submit"
        )
        self.assertNotIn("request_id", submit_rule["then"]["required"])
        self.assertNotIn("seat", submit_rule["then"]["required"])
        self.assertTrue(
            {"model_selection", "model_instance", "model"}.isdisjoint(
                set(schema["properties"])
            )
        )

    def test_target_is_rejected_outside_recover(self) -> None:
        for path in (REQUEST_SCHEMA, TOOL_CALL_INPUT_SCHEMA):
            with self.subTest(path=path.name):
                schema = load_schema(path)
                rule = find_target_rejection_rule(schema)
                self.assertEqual(
                    rule["if"]["properties"]["action"],
                    {"not": {"const": "recover"}},
                )
                self.assertEqual(rule["if"]["required"], ["action"])
                self.assertEqual(rule["then"]["properties"]["target"], False)
                actions = set(schema["properties"]["action"]["enum"])
                self.assertIn("recover", actions)
                self.assertIn("target", schema["properties"])
                self.assertTrue(actions - {"recover"})


class WorkloadAndDelegationSchemaTests(unittest.TestCase):
    """The two new public fields, and the branches they open and close."""

    def submit_rules(self, schema: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            rule
            for rule in schema["allOf"]
            if rule["if"]["properties"].get("action", {}).get("const") == "submit"
        ]

    def test_workload_is_a_bounded_public_identifier(self) -> None:
        for path in (REQUEST_SCHEMA, TOOL_CALL_INPUT_SCHEMA):
            with self.subTest(schema=path.name):
                workload = load_schema(path)["properties"]["workload"]
                self.assertEqual(workload["type"], "string")
                self.assertEqual(
                    workload["pattern"], "^[a-z0-9][a-z0-9-]{0,63}$"
                )

    def test_delegation_id_is_the_only_field_that_opens_adoption(self) -> None:
        for path in (REQUEST_SCHEMA, TOOL_CALL_INPUT_SCHEMA):
            with self.subTest(schema=path.name):
                schema = load_schema(path)
                delegation = schema["properties"]["delegation_id"]
                self.assertEqual(delegation["type"], "string")
                self.assertEqual(delegation["maxLength"], 128)
                # There is no public consumer or adapter override beside it.
                names = set(property_names(schema))
                self.assertNotIn("consumer", names)
                self.assertNotIn("adapter", names)
                self.assertNotIn("model_admission", names)
                self.assertNotIn("model_plan", names)

    def test_an_ordinary_submit_keeps_its_required_envelope(self) -> None:
        schema = load_schema(REQUEST_SCHEMA)
        ordinary = next(
            rule
            for rule in self.submit_rules(schema)
            if "delegation_id" not in rule["if"].get("required", [])
        )
        self.assertEqual(
            ordinary["if"]["not"], {"required": ["delegation_id"]}
        )
        self.assertEqual(
            set(ordinary["then"]["required"]),
            {"host", "repository", "source_revision", "title"},
        )

    def test_an_adopted_submit_derives_its_envelope_from_the_receipt(self) -> None:
        schema = load_schema(REQUEST_SCHEMA)
        adopted = next(
            rule
            for rule in self.submit_rules(schema)
            if "delegation_id" in rule["if"].get("required", [])
        )
        self.assertEqual(set(adopted["then"]["required"]), {"title"})
        # The execution identity is derived from the delegation, and there is
        # no placement left to justify.
        self.assertIs(adopted["then"]["properties"]["request_id"], False)
        self.assertIs(adopted["then"]["properties"]["override_reason"], False)

    def test_only_submit_may_name_a_delegation_or_a_workload(self) -> None:
        for path in (REQUEST_SCHEMA, TOOL_CALL_INPUT_SCHEMA):
            with self.subTest(schema=path.name):
                rule = next(
                    rule
                    for rule in load_schema(path)["allOf"]
                    if rule["if"]["properties"].get("action", {}).get("not")
                    == {"const": "submit"}
                )
                self.assertIs(rule["then"]["properties"]["delegation_id"], False)
                self.assertIs(rule["then"]["properties"]["workload"], False)

    def test_the_handoff_field_set_is_frozen(self) -> None:
        schema = load_schema(RESULT_SCHEMA)
        handoff = schema["$defs"]["handoff"]
        self.assertFalse(handoff["additionalProperties"])
        frozen = {
            "target_machine",
            "repository",
            "branch",
            "source_revision",
            "request_id",
            "worktree_ref",
        }
        self.assertEqual(set(handoff["required"]), frozen)
        self.assertEqual(set(handoff["properties"]), frozen)
        self.assertEqual(
            handoff["properties"]["source_revision"]["pattern"], "^[0-9a-f]{40}$"
        )

    def test_the_disposition_is_a_single_value_paired_with_a_handoff(self) -> None:
        schema = load_schema(RESULT_SCHEMA)
        self.assertEqual(
            schema["properties"]["disposition"]["const"], "pending-handoff"
        )
        per_request = schema["oneOf"][0]
        self.assertEqual(
            per_request["properties"]["disposition"]["const"], "pending-handoff"
        )
        self.assertEqual(
            per_request["properties"]["handoff"], {"$ref": "#/$defs/handoff"}
        )
        # Neither half is meaningful alone.
        self.assertEqual(
            per_request["dependentRequired"],
            {"disposition": ["handoff"], "handoff": ["disposition"]},
        )
        # The real execution state is still required and unchanged.
        self.assertIn("state", per_request["required"])
        self.assertEqual(
            set(per_request["properties"]["state"]["enum"]), set(JOB_STATES)
        )

    def test_the_delegation_denial_vocabulary_is_closed_and_shared(self) -> None:
        schema = load_schema(RESULT_SCHEMA)
        reasons = set(
            schema["$defs"]["error_detail"]["properties"]["reason"]["enum"]
        )
        self.assertEqual(reasons, set(POLICY_REASONS))
        self.assertLessEqual(
            {
                "delegation-not-enrolled",
                "delegation-invalid",
                "delegation-conflict",
                "delegation-repository-unmatched",
            },
            reasons,
        )


class OutboxResultBranchTests(unittest.TestCase):
    """The local-queue envelopes this client emits must be representable.

    `oneOf` branches are closed, so declaring a key at the top level does not
    make a result valid: some branch has to accept the whole envelope. Both
    shapes here are built from the code that produces them rather than
    transcribed, so a key added or dropped there fails this instead of shipping
    a result no consumer can validate.
    """

    def job(self) -> "OutboxJob":
        from t3_fleet.outbox import OutboxJob

        return OutboxJob(
            job_id="job-" + "a" * 32,
            digest="b" * 64,
            state="queued",
            disposition="queued-blocked",
            execution_state="never-dispatched",
            attempts=0,
            request_id=None,
            parent_request_id=None,
            host=None,
            repository="example-repo",
            payload_purged=False,
            lease_generation=0,
            created_at=0.0,
            updated_at=0.0,
            expires_at=1.0,
            next_attempt_at=0.0,
        )

    def test_a_queued_submit_lands_on_exactly_one_branch(self) -> None:
        from t3_fleet.controller import FleetController

        schema = load_schema(RESULT_SCHEMA)
        for reason in ("host-offline", None):
            with self.subTest(reason=reason):
                payload = FleetController._queued_result(self.job(), reason)
                self.assertEqual(
                    accepting_branches(schema, payload),
                    ["Queued submit result"],
                )

    def test_a_tick_result_lands_on_exactly_one_branch(self) -> None:
        import tempfile

        from t3_fleet.outbox import OutboxStore

        schema = load_schema(RESULT_SCHEMA)
        with tempfile.TemporaryDirectory() as directory:
            status = OutboxStore(Path(directory) / "outbox").tick_status()
        for state in ("eligible", "busy"):
            with self.subTest(state=state):
                payload = {
                    "schema": "t3-fleet.result.v1",
                    "action": "tick",
                    "state": state,
                    "examined": 0,
                    "dispatched": [],
                    "blocked": [],
                    "held": [],
                    "expired": [],
                    "reservations": [],
                    "truncated": False,
                    **status,
                }
                self.assertEqual(
                    accepting_branches(schema, payload), ["Tick result"]
                )

    def test_the_tick_counts_cover_every_outbox_state(self) -> None:
        from t3_fleet.outbox import _STATES

        counts = load_schema(RESULT_SCHEMA)["properties"]["counts"]
        self.assertFalse(counts["additionalProperties"])
        self.assertEqual(set(counts["properties"]), set(_STATES))

    def test_a_drained_job_is_reported_as_a_closed_outbox_record(self) -> None:
        """Every bucket a tick fills carries the redacted job and nothing else."""
        record = load_schema(RESULT_SCHEMA)["$defs"]["outbox_record"]
        self.assertFalse(record["additionalProperties"])
        self.assertEqual(
            set(record["properties"]),
            set(self.job().redacted()) | {"queue_reason"},
        )

    def test_a_queued_result_never_claims_an_execution_or_attention(self) -> None:
        """`pending_autotick` is not a dispatch state and needs no operator.

        Both are pinned as `const` rather than left to an enum, so a queued
        result can never be read as a request a target accepted, nor as one
        waiting on an answer.
        """
        queued = next(
            branch
            for branch in load_schema(RESULT_SCHEMA)["oneOf"]
            if branch["title"] == "Queued submit result"
        )
        self.assertEqual(queued["properties"]["state"]["const"], "pending_autotick")
        self.assertNotIn("pending_autotick", JOB_STATES)
        self.assertIs(queued["properties"]["needs_attention"]["const"], False)
        self.assertNotIn("thread_id", queued["properties"])
        self.assertNotIn("disposition", queued["properties"])
        self.assertNotIn("handoff", queued["properties"])

    def test_a_tick_result_carries_no_per_request_execution_claim(self) -> None:
        """A drain reports its own queue, never a target's view of a run."""
        tick = next(
            branch
            for branch in load_schema(RESULT_SCHEMA)["oneOf"]
            if branch["title"] == "Tick result"
        )
        for absent in ("thread_id", "output", "disposition", "handoff", "request_id"):
            self.assertNotIn(absent, tick["properties"])
        self.assertEqual(set(tick["properties"]["state"]["enum"]), {"eligible", "busy"})


if __name__ == "__main__":
    unittest.main()
