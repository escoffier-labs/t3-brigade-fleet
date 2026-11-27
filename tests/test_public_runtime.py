"""Offline contracts for the explicit-pin public-runtime artifact builder."""
from __future__ import annotations

import base64
import copy
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "build-public-runtime.py"
REPOSITORY = "https://github.com/escoffier-labs/t3-brigade-fleet.git"
MODULES = ("__init__", "__main__", "agent", "cli", "config", "controller",
           "errors", "models", "outbox", "policy", "reporting", "store", "t3_api",
           "t3_origin", "t3_rpc", "target_store", "timer", "transport")
ENTRYPOINTS = {"t3-fleet": "t3_fleet.cli:main", "t3-fleet-agent": "t3_fleet.agent:main"}
DIST = "t3_fleet-0.2.0.dist-info/"
WHEEL = "t3_fleet-0.2.0-py3-none-any.whl"


class PublicRuntimeTests(unittest.TestCase):
    def test_explicit_pin_is_required_and_no_pin_is_checked_in(self):
        self.assertFalse((ROOT / "runtime/public-runtime.json").exists())
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--output-dir", str(self.root / "out")],
            capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--pin", result.stderr)

    def test_current_and_established_source_surfaces_come_from_git_objects(self):
        # The established pin surface: the 18 reviewed modules and two scripts.
        established = self.api["source_modules"](self.source_files)
        self.assertNotIn("lane.py", established)
        self.assertEqual(
            self.api["package_contract"](self.source_files)["console_scripts"],
            ENTRYPOINTS)
        # The current public package surface: an added reviewed module and
        # console script. Both are authenticated by Git object names and the
        # pinned pyproject, never by a wheel's own member list.
        current_files = dict(self.source_files)
        current_files["src/t3_fleet/lane.py"] = b"# current public lane module\n"
        current_files["pyproject.toml"] = current_files["pyproject.toml"].replace(
            b't3-fleet-agent = "t3_fleet.agent:main"\n',
            b't3-fleet-agent = "t3_fleet.agent:main"\nt3-lane = "t3_fleet.lane:main"\n')
        current_pin = copy.deepcopy(self.pin)
        current_pin["package"]["console_scripts"] = {
            **ENTRYPOINTS, "t3-lane": "t3_fleet.lane:main"}
        modules = self.api["validate_source_contract"](current_pin, current_files)
        self.assertIn("lane.py", modules)
        self.assertIn("t3_fleet/lane.py", self.api["module_names"](current_files))
        # A pin whose package contract disagrees with the authenticated source
        # is refused even when its shape is valid.
        disagreeing = copy.deepcopy(current_pin)
        disagreeing["package"]["version"] = "9.9.9"
        with self.assertRaises(ValueError):
            self.api["validate_source_contract"](disagreeing, current_files)
        # A wheel cannot widen the allowlist beyond the authenticated Git set.
        members = self.wheel_members()
        members["t3_fleet/unreviewed.py"] = b"# not in the pinned tree\n"
        self.write_wheel(members)
        with self.assertRaises(ValueError):
            self.api["inspect_wheel"](self.wheel, self.pin, self.source_files)



    def setUp(self):
        self.assertTrue(SCRIPT.is_file(), "public runtime builder contract is missing")
        self.api = runpy.run_path(str(SCRIPT))
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.pin = {
            "schema": 1, "repository": REPOSITORY,
            "revision": "a" * 40, "tree": "b" * 40,
            "package": {"name": "t3-fleet", "version": "0.2.0",
                        "requires_python": ">=3.11", "dependencies": [],
                        "console_scripts": ENTRYPOINTS},
        }
        self.modules = {f"t3_fleet/{name}.py": b"# public fixture\n" for name in MODULES}
        self.source_files = {
            **{"src/" + name: data for name, data in self.modules.items()},
            "README.md": b"# Public fixture\n\nPublic runtime.\n",
            "LICENSE": b"\nPublic fixture license\n\n  Preserve indentation.\n",
            "pyproject.toml": (
                '[build-system]\nrequires = ["setuptools>=68"]\nbuild-backend = "setuptools.build_meta"\n'
                '[project]\nname = "t3-fleet"\nversion = "0.2.0"\nrequires-python = ">=3.11"\ndependencies = []\n'
                'description = "Public fixture"\nreadme = "README.md"\nlicense = {file = "LICENSE"}\n'
                'authors = [{name = "Public Authors"}]\nkeywords = ["fleet", "cli"]\n'
                'classifiers = ["Programming Language :: Python :: 3"]\n'
                '[project.urls]\nRepository = "https://example.invalid/public"\n'
                '[project.scripts]\nt3-fleet = "t3_fleet.cli:main"\nt3-fleet-agent = "t3_fleet.agent:main"\n'
                '[tool.setuptools.packages.find]\nwhere = ["src"]\n'
            ).encode(),
        }
        self.source = self.root / "source"
        self.source.mkdir()
        self.pin_file = self.root / "pin.json"
        self.wheel = self.root / WHEEL

    def write_pin(self):
        self.pin_file.write_text(json.dumps(self.pin), encoding="utf-8")

    def wheel_members(self):
        return {
            **self.modules,
            DIST + "METADATA": (
                b"Metadata-Version: 2.1\nName: t3-fleet\nVersion: 0.2.0\nSummary: Public fixture\n"
                b"Author: Public Authors\nLicense: \n        Public fixture license\n        \n          Preserve indentation.\n        \n"
                b"Project-URL: Repository, https://example.invalid/public\nKeywords: fleet,cli\n"
                b"Classifier: Programming Language :: Python :: 3\nRequires-Python: >=3.11\n"
                b"Description-Content-Type: text/markdown\nLicense-File: LICENSE\n\n"
                + self.source_files["README.md"]
            ),
            DIST + "LICENSE": self.source_files["LICENSE"],
            DIST + "WHEEL": b"Wheel-Version: 1.0\nGenerator: bdist_wheel (0.42.0)\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n",
            DIST + "entry_points.txt": b"[console_scripts]\nt3-fleet = t3_fleet.cli:main\nt3-fleet-agent = t3_fleet.agent:main\n",
            DIST + "top_level.txt": b"t3_fleet\n",
        }

    def write_wheel(self, members, *, record_newline="\n"):
        record = io.StringIO(newline="")
        writer = csv.writer(record, lineterminator=record_newline)
        for name, data in members.items():
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
            writer.writerow([name, "sha256=" + digest, str(len(data))])
        writer.writerow([DIST + "RECORD", "", ""])
        with zipfile.ZipFile(self.wheel, "w") as archive:
            for name, data in members.items():
                archive.writestr(name, data)
            archive.writestr(DIST + "RECORD", record.getvalue())

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.source), *args], check=True,
                              capture_output=True, text=True).stdout.strip()

    def git_fixture(self):
        self.git("init", "-q")
        self.git("config", "user.name", "Fixture")
        self.git("config", "user.email", "fixture@example.invalid")
        self.git("remote", "add", "origin", REPOSITORY)
        for name, data in self.source_files.items():
            path = self.source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        self.git("add", ".")
        self.git("commit", "-qm", "fixture")
        self.pin["revision"] = self.git("rev-parse", "HEAD")
        self.pin["tree"] = self.git("rev-parse", "HEAD^{tree}")
        self.write_pin()

    def test_pin_rejects_short_identity_wrong_repo_and_package_contract(self):
        for field, value in (("revision", "a" * 7), ("tree", "b" * 39),
                             ("repository", "https://example.invalid/other.git")):
            with self.subTest(field=field):
                bad = copy.deepcopy(self.pin)
                bad[field] = value
                self.pin_file.write_text(json.dumps(bad), encoding="utf-8")
                with self.assertRaises(ValueError):
                    self.api["load_pin"](self.pin_file)
        for key, value in (("dependencies", ["other"]), ("requires_python", ">=3.10"),
                           ("console_scripts", {}), ("name", "other")):
            with self.subTest(key=key):
                bad = copy.deepcopy(self.pin)
                bad["package"][key] = value
                # The pin's package contract is compared with authenticated
                # source Git objects, never trusted from the pin or a wheel.
                with self.assertRaises(ValueError):
                    self.api["validate_source_contract"](bad, self.source_files)

    def test_git_archive_uses_pin_and_excludes_dirty_and_untracked_files(self):
        self.git_fixture()
        (self.source / "src/t3_fleet/cli.py").write_text("# dirty\n")
        (self.source / "private-config.toml").write_text("not public\n")
        destination = self.root / "export"
        source_files = self.api["export_source"](self.pin, self.source, destination)
        self.assertEqual(source_files, self.source_files)
        self.assertEqual((destination / "src/t3_fleet/cli.py").read_bytes(), b"# public fixture\n")
        self.assertFalse((destination / "private-config.toml").exists())

    def test_local_promisor_source_refuses_fetch_without_mutating_objects(self):
        self.git_fixture()
        remote = self.root / "remote.git"
        shutil.copytree(self.source / ".git", remote)
        marker = self.root / "remote-executed"
        upload_pack = self.root / "upload-pack"
        upload_pack.write_text(
            "#!" + sys.executable + "\nimport os, pathlib, sys\n"
            + "pathlib.Path(" + repr(str(marker)) + ").write_text('executed')\n"
            + "os.execvp('git-upload-pack', ['git-upload-pack', *sys.argv[1:]])\n")
        upload_pack.chmod(0o700)
        self.git("config", "remote.origin.promisor", "true")
        self.git("config", "remote.origin.partialclonefilter", "blob:none")
        self.git("config", "remote.origin.uploadpack", str(upload_pack))
        self.git("config", "url." + remote.as_uri() + ".insteadOf", REPOSITORY)
        self.git("config", "protocol.allow", "always")
        self.git("config", "protocol.file.allow", "always")
        oid = self.git("rev-parse", "HEAD:src/t3_fleet/cli.py")
        objects = self.source / ".git/objects"
        (objects / oid[:2] / oid[2:]).unlink()
        # The same incomplete repository can fetch from the offline fixture remote.
        control = self.root / "control.git"
        shutil.copytree(self.source / ".git", control)
        environment = dict(os.environ, GIT_ALLOW_PROTOCOL="file", GIT_CONFIG_NOSYSTEM="1",
                           GIT_CONFIG_GLOBAL=os.devnull)
        result = subprocess.run(["git", "--git-dir", str(control), "archive", self.pin["revision"]],
                                env=environment, capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0)
        self.assertTrue(marker.exists(), "negative control must execute the remote")
        self.assertTrue(list((control / "objects/pack").glob("*.promisor")))
        marker.unlink()
        before = {str(p.relative_to(objects)): (p.read_bytes(), p.stat().st_mtime_ns)
                  for p in objects.rglob("*") if p.is_file()}
        output = self.root / "artifacts"
        result = subprocess.run([sys.executable, str(SCRIPT), "--pin", str(self.pin_file),
                                 "--source", str(self.source), "--output-dir", str(output),
                                 "--verify-only"], capture_output=True, text=True, timeout=30)
        with self.subTest(contract="no remote execution"):
            self.assertFalse(marker.exists())
        with self.subTest(contract="unchanged source objects"):
            self.assertEqual(before, {str(p.relative_to(objects)): (p.read_bytes(), p.stat().st_mtime_ns)
                                      for p in objects.rglob("*") if p.is_file()})
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stderr, "public-runtime: public source/build command failed\n")
        self.assertFalse(output.exists())

    def test_source_rejects_wrong_origin_revision_tree_and_export_substitution(self):
        self.git_fixture()
        for key, value in (("revision", "c" * 40), ("tree", "c" * 40)):
            with self.subTest(key=key):
                bad = dict(self.pin, **{key: value})
                with self.assertRaises(ValueError):
                    self.api["export_source"](bad, self.source, self.root / key)
        self.git("remote", "set-url", "origin", "https://example.invalid/other.git")
        with self.assertRaises(ValueError):
            self.api["export_source"](self.pin, self.source, self.root / "wrong-origin")
        self.git("remote", "set-url", "origin", REPOSITORY)
        (self.source / "src/t3_fleet/cli.py").write_bytes(b"# $Format:%H$\n")
        (self.source / ".gitattributes").write_text("src/t3_fleet/cli.py export-subst\n")
        self.git("add", ".")
        self.git("commit", "-qm", "export substitution")
        self.pin["revision"] = self.git("rev-parse", "HEAD")
        self.pin["tree"] = self.git("rev-parse", "HEAD^{tree}")
        with self.assertRaisesRegex(ValueError, "source"):
            self.api["export_source"](self.pin, self.source, self.root / "substituted")

    def test_archive_rejects_traversal_and_links_before_writing(self):
        for name, kind in (("../escape", tarfile.REGTYPE), ("/absolute", tarfile.REGTYPE),
                           ("C:/escape", tarfile.REGTYPE), ("safe", tarfile.SYMTYPE)):
            with self.subTest(name=name, kind=kind):
                stream = io.BytesIO()
                with tarfile.open(fileobj=stream, mode="w") as archive:
                    item = tarfile.TarInfo(name)
                    item.type = kind
                    item.linkname = "../escape" if kind == tarfile.SYMTYPE else ""
                    archive.addfile(item)
                with self.assertRaises(ValueError):
                    self.api["extract_archive"](stream.getvalue(), self.root / "extract")
                self.assertFalse((self.root / "extract").exists())

    def test_valid_wheel_manifest_binds_folded_source_metadata_and_hashes(self):
        self.write_wheel(self.wheel_members())
        manifest = self.api["inspect_wheel"](self.wheel, self.pin, self.source_files)
        self.assertEqual(manifest["revision"], self.pin["revision"])
        self.assertEqual(manifest["tree"], self.pin["tree"])
        self.assertEqual(manifest["repository"], REPOSITORY)
        self.assertEqual(manifest["wheel"]["sha256"], hashlib.sha256(self.wheel.read_bytes()).hexdigest())
        self.assertEqual(manifest["modules"]["t3_fleet/cli.py"], hashlib.sha256(b"# public fixture\n").hexdigest())
        self.assertEqual(manifest["package"], self.pin["package"])
        self.write_wheel(self.wheel_members(), record_newline="\r\n")
        self.assertEqual(self.api["inspect_wheel"](self.wheel, self.pin, self.source_files)
                         ["modules"], manifest["modules"])
        reordered = self.wheel_members()
        reordered[DIST + "METADATA"] = reordered[DIST + "METADATA"].replace(
            b"Metadata-Version: 2.1\nName: t3-fleet\n",
            b"Name: t3-fleet\nMetadata-Version: 2.1\n",
            1,
        )
        reordered[DIST + "WHEEL"] = reordered[DIST + "WHEEL"].replace(
            b"Wheel-Version: 1.0\nGenerator: bdist_wheel (0.42.0)\n",
            b"Generator: bdist_wheel (0.42.0)\nWheel-Version: 1.0\n",
            1,
        )
        self.write_wheel(reordered)
        self.assertEqual(self.api["inspect_wheel"](self.wheel, self.pin, self.source_files)
                         ["modules"], manifest["modules"])

    def test_installed_setuptools_license_layout_is_accepted(self):
        for names in (("LICENSE",), ("licenses/LICENSE",), ("LICENSE", "licenses/LICENSE")):
            with self.subTest(names=names):
                members = self.wheel_members()
                del members[DIST + "LICENSE"]
                for name in names:
                    members[DIST + name] = self.source_files["LICENSE"]
                self.write_wheel(members)
                self.assertEqual(self.api["inspect_wheel"](self.wheel, self.pin, self.source_files)
                                 ["package"]["version"], "0.2.0")

    def test_wheel_accepts_only_source_derived_license_header_variants(self):
        members = self.wheel_members()
        key = DIST + "METADATA"
        original = b"License: \n        Public fixture license"
        trimmed = b"License: Public fixture license"
        self.assertIn(original, members[key])
        members[key] = members[key].replace(original, trimmed, 1)
        self.write_wheel(members)
        self.api["inspect_wheel"](self.wheel, self.pin, self.source_files)

        members[key] = members[key].replace(trimmed, b"License: Different license", 1)
        self.write_wheel(members)
        with self.assertRaisesRegex(ValueError, "public wheel metadata mismatch"):
            self.api["inspect_wheel"](self.wheel, self.pin, self.source_files)

    def test_wheel_rejects_missing_modified_private_and_unsafe_members(self):
        cases = [dict(self.wheel_members(), **{"t3_fleet/cli.py": b"# altered\n"}),
                 {k: v for k, v in self.wheel_members().items() if k != "t3_fleet/agent.py"}]
        for name in ("../escape", "C:/escape", "t3_fleet/../escape", "private-config.toml",
                     "t3_fleet/state.db", "other_package/__init__.py"):
            cases.append(dict(self.wheel_members(), **{name: b"unexpected"}))
        for index, members in enumerate(cases):
            with self.subTest(case=index):
                self.write_wheel(members)
                with self.assertRaises(ValueError):
                    self.api["inspect_wheel"](self.wheel, self.pin, self.source_files)

    def test_wheel_rejects_metadata_dependencies_entrypoints_and_record_tampering(self):
        cases = [("METADATA", b"Name: other\nVersion: 0.2.0\nRequires-Python: >=3.11\n"),
                 ("METADATA", b"Name: t3-fleet\nVersion: 0.1.0\nRequires-Python: >=3.11\n"),
                 ("METADATA", b"Name: t3-fleet\nVersion: 0.2.0\nRequires-Python: >=3.10\n"),
                 ("METADATA", self.wheel_members()[DIST + "METADATA"].replace(b"\n\n", b"\nRequires-Dist: other\n\n")),
                 ("entry_points.txt", b"[console_scripts]\nt3-fleet = other:main\n"),
                 ("WHEEL", b"Wheel-Version: 1.0\nRoot-Is-Purelib: false\nTag: py3-none-any\n")]
        for member, data in cases:
            with self.subTest(member=member, data=data):
                self.write_wheel(dict(self.wheel_members(), **{DIST + member: data}))
                with self.assertRaises(ValueError):
                    self.api["inspect_wheel"](self.wheel, self.pin, self.source_files)
        self.write_wheel(self.wheel_members())
        with zipfile.ZipFile(self.wheel) as archive:
            contents = {name: archive.read(name) for name in archive.namelist()}
        contents[DIST + "RECORD"] = b""
        with zipfile.ZipFile(self.wheel, "w") as archive:
            for name, data in contents.items():
                archive.writestr(name, data)
        with self.assertRaises(ValueError):
            self.api["inspect_wheel"](self.wheel, self.pin, self.source_files)

    def test_cli_rejects_metadata_substitution_with_regenerated_record_and_manifest(self):
        self.git_fixture()
        members = self.wheel_members()
        self.write_wheel(members)
        output = self.root / "artifacts"
        manifest = self.api["publish_artifacts"](output, self.wheel, self.pin, self.source_files)
        metadata = members[DIST + "METADATA"]
        header, body = metadata.split(b"\n\n", 1)
        cases = [("METADATA", header + b"\n\nNONPUBLIC_SENTINEL\n" + body),
                 ("METADATA", header + b"\n\n" + body + b" "),
                 ("METADATA", metadata.replace(b"Metadata-Version: 2.1", b"Metadata-Version: 9.9")),
                 ("METADATA", metadata.replace(b"Name: t3-fleet", b"Name:\tt3-fleet", 1)),
                 ("METADATA", metadata.replace(b"License: \n        Public", b"License: \n       Public", 1)),
                 ("METADATA", metadata.replace(b"\n", b"\r\n")),
                 ("METADATA", b"Broken header\n" + metadata),
                 ("METADATA", b"\tOrphan folding\n" + metadata),
                 ("METADATA", header + b"\n# comment\n\n" + body),
                 ("LICENSE", None)]
        for field in (b"Summary", b"Author", b"Project-URL", b"Keywords", b"Classifier",
                      b"Description-Content-Type", b"License-File", b"License"):
            cases.append(("METADATA", metadata.replace(field + b": ", field + b": NONPUBLIC ", 1)))
            cases.append(("METADATA", header + b"\n" + field + b": duplicate\n\n" + body))
        for line in (b"X-Private: NONPUBLIC", b"Requires-Dist: other", b"Dynamic: Summary",
                     b"Author-email: private@example.invalid", b"License-Expression: MIT"):
            cases.append(("METADATA", header + b"\n" + line + b"\n\n" + body))
        for data in (b"NONPUBLIC license\n", self.source_files["LICENSE"] + b" ",
                     self.source_files["LICENSE"].replace(b"  Preserve", b" Preserve")):
            cases.append(("LICENSE", data))
            cases.append(("licenses/LICENSE", data))
        wheel = members[DIST + "WHEEL"]
        for data in (wheel + b"NONPUBLIC\n", wheel.replace(b"\n\n", b"\nX-Private: NONPUBLIC\n\n"),
                     wheel.replace(b"bdist_wheel (0.42.0)", b"NONPUBLIC"),
                     wheel.replace(b"bdist_wheel (0.42.0)", b"bdist_wheel (999.0)"),
                     wheel.replace(b"\n\n", b"\nTag: py3-none-any\n\n")):
            cases.append(("WHEEL", data))
        for name in ("entry_points.txt", "top_level.txt"):
            cases.append((name, members[DIST + name] + b"# NONPUBLIC\n"))
        cases.append(("RECORD", b""))
        for index, (name, data) in enumerate(cases):
            with self.subTest(case=index, member=name):
                changed = dict(members)
                if name == "RECORD":
                    # Quoted CSV fields carry the same values but are outside the canonical format.
                    self.write_wheel(members)
                    with zipfile.ZipFile(self.wheel) as archive:
                        data = archive.read(DIST + "RECORD").replace(b"t3_fleet/cli.py,", b'"t3_fleet/cli.py",')
                if data is None:
                    del changed[DIST + name]
                else:
                    changed[DIST + name] = data
                if name == "RECORD":
                    with zipfile.ZipFile(self.wheel, "w") as archive:
                        for member, content in changed.items():
                            archive.writestr(member, content)
                else:
                    self.write_wheel(changed)
                (output / WHEEL).write_bytes(self.wheel.read_bytes())
                updated = copy.deepcopy(manifest)
                updated["wheel"]["sha256"] = hashlib.sha256(self.wheel.read_bytes()).hexdigest()
                (output / "public-runtime-manifest.json").write_text(json.dumps(updated))
                original = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in output.iterdir()}
                result = subprocess.run([sys.executable, str(SCRIPT), "--pin", str(self.pin_file),
                                         "--source", str(self.source), "--output-dir", str(output),
                                         "--verify-only"], capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")
                self.assertIn(result.stderr, {
                    "public-runtime: public wheel metadata mismatch\n",
                    "public-runtime: public wheel license content mismatch\n",
                    "public-runtime: public wheel console entrypoints mismatch\n",
                    "public-runtime: public wheel top-level package mismatch\n",
                    "public-runtime: public wheel RECORD format mismatch\n",
                })
                self.assertEqual(original, {p.name: (p.read_bytes(), p.stat().st_mtime_ns)
                                            for p in output.iterdir()})

    def test_output_repeats_verify_without_clobber_and_reject_tampering(self):
        self.write_wheel(self.wheel_members())
        output = self.root / "artifacts"
        self.api["publish_artifacts"](output, self.wheel, self.pin, self.source_files)
        original = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in output.iterdir()}
        self.api["verify_artifacts"](output, self.pin, self.source_files)
        self.api["publish_artifacts"](output, self.wheel, self.pin, self.source_files)
        self.assertEqual(original, {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in output.iterdir()})
        manifest = output / "public-runtime-manifest.json"
        document = json.loads(manifest.read_text())
        document["revision"] = "c" * 40
        manifest.write_text(json.dumps(document))
        with self.assertRaises(ValueError):
            self.api["verify_artifacts"](output, self.pin, self.source_files)
        manifest.write_bytes(original[manifest.name][0])
        (output / WHEEL).write_bytes(b"tampered")
        with self.assertRaises(ValueError):
            self.api["verify_artifacts"](output, self.pin, self.source_files)
        unrelated = self.root / "unrelated"
        unrelated.mkdir()
        (unrelated / "keep").write_bytes(b"keep")
        with self.assertRaises(ValueError):
            self.api["publish_artifacts"](unrelated, self.wheel, self.pin, self.source_files)
        self.assertEqual((unrelated / "keep").read_bytes(), b"keep")

    def test_cli_safe_errors_and_output_cannot_write_inside_checkout(self):
        self.git_fixture()
        other_checkout = self.root / "other-checkout"
        other_checkout.mkdir()
        (other_checkout / ".git").mkdir()
        for output in (ROOT / "dist", self.source / "dist", other_checkout / "dist"):
            result = subprocess.run([sys.executable, str(SCRIPT), "--pin", str(self.pin_file),
                                     "--source", str(self.source), "--output-dir", str(output)],
                                    capture_output=True, text=True, timeout=30)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("outside", result.stderr)
            self.assertFalse(output.exists())
        self.git("remote", "set-url", "origin", "https://secret-user:secret-value@example.invalid/repo")
        result = subprocess.run([sys.executable, str(SCRIPT), "--pin", str(self.pin_file),
                                 "--source", str(self.source), "--output-dir", str(self.root / "dist")],
                                capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("secret-value", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_cli_rejects_corrupt_wheels_without_tracebacks(self):
        self.git_fixture()
        self.write_wheel(self.wheel_members())
        output = self.root / "artifacts"
        self.api["publish_artifacts"](output, self.wheel, self.pin, self.source_files)
        with zipfile.ZipFile(self.wheel) as archive:
            contents = {name: archive.read(name) for name in archive.namelist()}
        manifest = output / "public-runtime-manifest.json"
        original_manifest = (manifest.read_bytes(), manifest.stat().st_mtime_ns)
        for case, compression in (("encrypted", zipfile.ZIP_STORED),
                                  ("unsupported", zipfile.ZIP_STORED),
                                  ("oversized_record", zipfile.ZIP_STORED),
                                  ("deflate", zipfile.ZIP_DEFLATED),
                                  ("bzip2", zipfile.ZIP_BZIP2), ("lzma", zipfile.ZIP_LZMA)):
            with self.subTest(case=case):
                with zipfile.ZipFile(output / WHEEL, "w", compression=compression) as archive:
                    for name, data in contents.items():
                        if case == "oversized_record" and name == DIST + "RECORD":
                            data = b"private-artifact-value" * (csv.field_size_limit() // 22 + 1) + b",,\n"
                        archive.writestr(name, data)
                if case in {"deflate", "bzip2", "lzma"}:
                    # These standard ZIP compressions remain valid when undamaged.
                    result = self.api["inspect_wheel"](output / WHEEL, self.pin, self.source_files)
                    self.assertEqual(result["revision"], self.pin["revision"])
                modified = bytearray((output / WHEEL).read_bytes())
                central = modified.index(b"PK\x01\x02")
                if case in {"encrypted", "unsupported"}:
                    local_offset, central_offset, value = (6, 8, 1) if case == "encrypted" else (8, 10, 99)
                    struct.pack_into("<H", modified, local_offset, value)
                    struct.pack_into("<H", modified, central + central_offset, value)
                elif case in {"deflate", "bzip2", "lzma"}:
                    name_size, extra_size = struct.unpack_from("<HH", modified, 26)
                    offset = 30 + name_size + extra_size
                    # Keep the LZMA ZIP header, damage the filter properties.
                    offset += 4 if case == "lzma" else 0
                    modified[offset:offset + 4] = b"\xff" * 4
                (output / WHEEL).write_bytes(modified)
                original_wheel = ((output / WHEEL).read_bytes(), (output / WHEEL).stat().st_mtime_ns)
                result = subprocess.run([sys.executable, str(SCRIPT), "--pin", str(self.pin_file),
                                         "--source", str(self.source), "--output-dir", str(output),
                                         "--verify-only"], capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")
                self.assertIn(result.stderr, {
                    "public-runtime: invalid public runtime source or artifact\n",
                    "public-runtime: invalid public wheel compressed data\n",
                    "public-runtime: invalid public wheel RECORD CSV\n",
                })
                self.assertEqual(original_wheel, ((output / WHEEL).read_bytes(),
                                                  (output / WHEEL).stat().st_mtime_ns))
                self.assertEqual(original_manifest, (manifest.read_bytes(), manifest.stat().st_mtime_ns))


if __name__ == "__main__":
    unittest.main()
