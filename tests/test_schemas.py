from __future__ import annotations

import json
import re
import unittest
from pathlib import Path
from typing import Any

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
}
RESULT_FORBIDDEN_FIELDS = REQUEST_FORBIDDEN_FIELDS | {
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

        model_selection = schema["$defs"]["model_selection"]
        self.assertFalse(model_selection["additionalProperties"])
        self.assertFalse(
            model_selection["properties"]["options"]["additionalProperties"]
        )

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

    def test_result_contract_has_four_closed_output_branches(self) -> None:
        schema = load_schema(RESULT_SCHEMA)
        self.assertEqual(
            set(schema["properties"]["schema"]["enum"]),
            {"t3-fleet.result.v1", "t3-fleet.error.v1"},
        )
        branches = schema["oneOf"]
        self.assertEqual(len(branches), 4)
        self.assertTrue(
            all(
                branch.get("type") == "object"
                and branch.get("additionalProperties") is False
                for branch in branches
            )
        )

        per_request, doctor, capacity, error = branches
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
        self.assertFalse(doctor_host["additionalProperties"])

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
                "unknown_state",
                "t3_auth_failed",
                "t3_http_failed",
                "internal_error",
            },
        )
        self.assertEqual(error_detail["properties"]["message"]["maxLength"], 512)

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


if __name__ == "__main__":
    unittest.main()
