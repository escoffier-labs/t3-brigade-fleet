#!/usr/bin/env python3
"""Generate the checked-in Brigade policy wire fixture from the real resolver.

The fixture under `tests/fixtures/` is not hand-written. It is produced by
importing Brigade's own `fleet_policy.resolve_policy` and the Hub's own
`fleet_hub_policy_api._envelope` projection from an explicitly passed Brigade
source tree, so the `sources` leaf shape T3 parses is the shape Brigade
actually emits rather than one this repo imagined.

The Brigade source path is always passed explicitly. Nothing here consults
`PATH`, so a `brigade` that happens to be installed cannot silently stand in
for the source tree the fixture claims to come from.

Usage:
    python3 scripts/generate-policy-wire-fixture.py --brigade-source /path/to/brigade
    python3 scripts/generate-policy-wire-fixture.py --brigade-source /path/to/brigade --check
    python3 scripts/generate-policy-wire-fixture.py --brigade-source /path/to/brigade --check-contract

`--check` is a byte comparison. `--check-contract` fails only when the wire
contract moved, which is what the cross-repo interoperability test asserts.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "brigade-policy-resolve.json"

# The exact root files whose behaviour this fixture pins. Their digests are
# recorded in the fixture so a later reader can tell whether the root contract
# moved underneath us.
PROVENANCE_FILES = (
    "src/brigade/fleet_policy.py",
    "src/brigade/fleet_hub_policy_api.py",
)

CONSUMER = "t3-fleet"
PUBLIC_REPO = "github.com/example-org/example-public-repo"
PRIVATE_REPO = "github.com/example-org/example-private-repo"

# A generic policy document. Every identifier is an `example-*` placeholder so
# the fixture documents the wire contract without carrying a real roster.
DOCUMENT: dict[str, Any] = {
    "schema": "brigade.fleet_policy.v1",
    "defaults": {
        "roles": {"impl": "example-impl-seat", "review": "example-review-seat"},
        "execution": {"concurrency": 2},
        "data": {"retention": "example-retention-window"},
    },
    "machines": {
        "example-machine": {"enabled": True},
        "example-other-machine": {"enabled": True},
    },
    "seats": {
        "example-impl-seat": {"provider": "example-provider", "model": "example-model"},
        "example-review-seat": {"provider": "example-provider", "model": "example-model"},
        "example-override-seat": {"provider": "example-provider", "model": "example-model"},
    },
    "consumers": {
        CONSUMER: {"default_patches": {"roles": {"review": "example-review-seat"}}},
    },
    "repositories": {
        PUBLIC_REPO: {
            "privacy": "public",
            "patches": {"execution": {"machine": "example-machine"}},
        },
        PRIVATE_REPO: {
            "privacy": "private",
            "patches": {"execution": {"machine": "example-machine"}},
        },
    },
}

# The Hub assigns the revision number from its own ledger, so the generator
# pins a stable placeholder rather than pretending to have queried a Hub.
REVISION = 7

CASES: tuple[dict[str, Any], ...] = (
    {
        "case": "inheritance",
        "description": (
            "No session overrides. Every leaf wins at the layer that set it and "
            "an unset leaf carries layer null with value null."
        ),
        "repository": PUBLIC_REPO,
        "overrides": None,
        "override_reason": None,
    },
    {
        "case": "explicit_override",
        "description": (
            "A session override outranks the repository and consumer layers, and "
            "an explicit null at the session layer inherits instead of clearing."
        ),
        "repository": PUBLIC_REPO,
        "overrides": {
            "roles": {"impl": "example-override-seat", "review": None},
            "execution": {"machine": "example-other-machine"},
        },
        "override_reason": "example operator override reason",
    },
    {
        "case": "privacy_constraint",
        "description": (
            "A private repository denies a session override that would enable "
            "training. The winning value is the constrained one while the chain "
            "still shows the requested override, so provenance survives denial."
        ),
        "repository": PRIVATE_REPO,
        "overrides": {"data": {"allow_training": True}},
        "override_reason": "example operator override reason",
    },
)


def _load_brigade(source: Path) -> tuple[Any, Any]:
    src = source / "src"
    if not (src / "brigade" / "fleet_policy.py").is_file():
        raise SystemExit(f"not a Brigade source tree: {source}")
    sys.path.insert(0, str(src))
    return (
        importlib.import_module("brigade.fleet_policy"),
        importlib.import_module("brigade.fleet_hub_policy_api"),
    )


def _git_describe(source: Path) -> dict[str, str]:
    def run(*args: str) -> str:
        try:
            completed = subprocess.run(
                ["git", "-C", str(source), *args],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return "unavailable"
        return completed.stdout.strip() if completed.returncode == 0 else "unavailable"

    return {"commit": run("rev-parse", "HEAD"), "branch": run("rev-parse", "--abbrev-ref", "HEAD")}


def _provenance(source: Path) -> dict[str, Any]:
    files = {}
    for relative in PROVENANCE_FILES:
        digest = hashlib.sha256((source / relative).read_bytes()).hexdigest()
        files[relative] = f"sha256:{digest}"
    return {
        "generator": "scripts/generate-policy-wire-fixture.py",
        "note": (
            "Generated by importing Brigade's own resolver and Hub envelope "
            "projection from the source tree below. The policy document and every "
            "identifier are generic placeholders; the revision number is a "
            "placeholder because a Hub assigns it. Regenerate with "
            "--brigade-source <path> after a root contract change."
        ),
        "brigade_source_files": files,
        "brigade_source_git": _git_describe(source),
    }


def _build(source: Path) -> dict[str, Any]:
    fleet_policy, hub_api = _load_brigade(source)
    document = fleet_policy.parse_document(DOCUMENT)
    current = {
        "revision": REVISION,
        "digest": fleet_policy.document_digest(document),
        "document": document,
    }
    cases = []
    for case in CASES:
        resolved = fleet_policy.resolve_policy(
            document,
            CONSUMER,
            case["repository"],
            case["overrides"],
            case["override_reason"],
        )
        cases.append(
            {
                "case": case["case"],
                "description": case["description"],
                "consumer": CONSUMER,
                "repository": case["repository"],
                "envelope": hub_api._envelope(current, resolved),
            }
        )
    return {
        "schema": "t3-fleet.brigade-policy-wire-fixture.v1",
        "provenance": _provenance(source),
        "public_envelope_keys": list(hub_api.RESOLVE_PUBLIC_KEYS),
        "cases": cases,
    }


def _contract(fixture: dict[str, Any]) -> dict[str, Any]:
    """The part of the fixture T3's parser is actually a client of."""
    return {
        "public_envelope_keys": fixture["public_envelope_keys"],
        "cases": {
            case["case"]: {
                "effective": case["envelope"]["effective"],
                "sources": case["envelope"]["sources"],
            }
            for case in fixture["cases"]
        },
    }


def _check_contract(current: dict[str, Any], generated: dict[str, Any]) -> int:
    """Fail on wire-contract drift; report provenance drift without failing.

    The document digest and the root file digests move whenever anything in the
    Brigade policy module changes, including changes T3 never sees. Failing on
    those would train a reader to regenerate on reflex and stop reading the
    diff, so only the contract itself is a failure.
    """
    if _contract(current) != _contract(generated):
        print(
            "wire contract drift: the Brigade policy sources or effective shape "
            f"changed. Regenerate {FIXTURE} and re-check the T3 parser against it.",
            file=sys.stderr,
        )
        return 1
    if current.get("provenance") != generated.get("provenance"):
        print(
            "note: provenance moved (root files or document digest) but the wire "
            "contract is unchanged; regenerate when convenient."
        )
    print(f"wire contract current: {FIXTURE}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--brigade-source",
        required=True,
        type=Path,
        help="Path to the Brigade source tree to generate from. Never resolved from PATH.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if the checked-in fixture differs from a freshly generated one at all.",
    )
    parser.add_argument(
        "--check-contract",
        action="store_true",
        help=(
            "Fail only if the wire contract moved: the public envelope keys, or "
            "any case's effective policy or sources. Provenance drift is reported "
            "as a note, because unrelated root edits move the document digest "
            "without changing anything T3 parses."
        ),
    )
    args = parser.parse_args(argv)

    source = args.brigade_source.expanduser().resolve()
    generated = _build(source)
    rendered = json.dumps(generated, indent=2, sort_keys=True) + "\n"

    if args.check or args.check_contract:
        if not FIXTURE.is_file():
            print(f"missing fixture: {FIXTURE}", file=sys.stderr)
            return 1
        current = FIXTURE.read_text(encoding="utf-8")
        if args.check:
            if current != rendered:
                print(f"stale fixture: {FIXTURE}", file=sys.stderr)
                return 1
            print(f"fixture current: {FIXTURE}")
            return 0
        return _check_contract(json.loads(current), generated)

    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(rendered, encoding="utf-8")
    print(f"wrote {FIXTURE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
