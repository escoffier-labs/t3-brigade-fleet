"""Local operator contracts. All installation destinations are disposable fixtures."""
from __future__ import annotations

import contextlib
import base64
import copy
import csv
import gc
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import py_compile
import shutil
import sqlite3
import struct
import subprocess
import sys
import tempfile
import uuid
import unittest
import warnings
from unittest.mock import patch
import venv
import zipfile

from tests import test_public_runtime as artifact_contract

DIST = artifact_contract.DIST
WHEEL = artifact_contract.WHEEL


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/install-public-runtime.py"


def load_adapter():
    spec = importlib.util.spec_from_file_location("local_install_adapter", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def modeled_anchor(python: Path, probe: dict | None = None) -> dict:
    """Model an already admitted anchor for tests unrelated to anchor admission."""
    python = Path(python)
    return {"python": str(python), "resolved": str(python.resolve()), "inventory": {},
            "probe": probe or {}, "prefix": str((probe or {}).get("prefix", sys.prefix)),
            "implementation": "cpython", "marshal": __import__("marshal").version,
            "cache_tag": sys.implementation.cache_tag}


@unittest.skipUnless(os.name == "posix", "direct invocation contract uses POSIX executable fixtures")
class InterpreterAnchorInvocationTests(unittest.TestCase):
    def setUp(self):
        previous = os.umask(0o022)
        self.addCleanup(os.umask, previous)
        self.temporary = tempfile.TemporaryDirectory(prefix="r12-anchor-", dir="/tmp")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o755)

    def arguments(self, python: Path) -> list[str]:
        values = {
            "prefix": self.root / "prefix", "purelib": self.root / "purelib",
            "scripts": self.root / "scripts", "source-root": self.root / "source",
            "fleet-launcher": self.root / "t3-fleet",
            "agent-launcher": self.root / "t3-fleet-agent",
            "artifact-dir": self.root / "artifacts", "public-source": self.root / "public",
            "target-host": "target-a",
            "config": self.root / "config.toml", "hash-record": self.root / "identity.json",
            "backup-parent": self.root / "backups",
            "pin": self.root / "pin.json",
        }
        result = ["plan", "--layout", "source-wrapper", "--python", str(python)]
        for name, value in values.items():
            result.extend(["--" + name, str(value)])
        return result

    def invoke(self, flags: list[str], python: Path) -> subprocess.CompletedProcess:
        return subprocess.run([sys.executable, *flags, str(SCRIPT), *self.arguments(python)],
                              cwd=self.root, env={"PATH": os.environ["PATH"]},
                              capture_output=True, text=True)

    def anchor_diagnostic(self, python: Path, arguments: list[str]) -> str:
        program = r'''import importlib.util,json,os,stat,sys,sysconfig
from pathlib import Path

def describe(value):
    path = Path(value)
    try:
        info = path.lstat()
    except OSError as error:
        return {"path": str(path), "error": type(error).__name__}
    return {"path": str(path), "resolved": str(path.resolve(strict=False)),
            "mode": oct(stat.S_IMODE(info.st_mode)), "uid": getattr(info, "st_uid", None),
            "kind": "symlink" if path.is_symlink() else "directory" if path.is_dir() else "file"}

spec = importlib.util.spec_from_file_location("anchor_diagnostic_adapter", sys.argv[1])
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)
toolchain_refusal = anchor_subject = None
original_toolchain_parent, original_anchor_object = adapter._toolchain_parent, adapter._anchor_object
def diagnostic_toolchain_parent(path):
    global toolchain_refusal
    try: return original_toolchain_parent(path)
    except ValueError as error:
        ancestors = []
        for ancestor in (Path(path), *Path(path).parents):
            try: info = ancestor.stat()
            except OSError as metadata_error:
                ancestors.append({"path": str(ancestor), "reason": type(metadata_error).__name__})
                continue
            reason = ("owner refused" if info.st_uid not in {0, os.geteuid()} else
                      "mutation permissions refused" if info.st_mode & 0o022 and not
                      (info.st_uid == 0 and info.st_mode & stat.S_ISVTX) else None)
            if reason: ancestors.append({**describe(ancestor), "reason": reason})
        toolchain_refusal = {"subject": str(path), "message": str(error),
                             "ancestors": ancestors[:8]}
        raise
def diagnostic_anchor_object(path, *values, **options):
    global anchor_subject
    anchor_subject = describe(path)
    return original_anchor_object(path, *values, **options)
adapter._toolchain_parent, adapter._anchor_object = diagnostic_toolchain_parent, diagnostic_anchor_object
args = adapter.parser().parse_args(sys.argv[2:])
stdlib, platstdlib = (Path(sysconfig.get_path(name)).resolve() for name in ("stdlib", "platstdlib"))
destshared = sysconfig.get_config_var("DESTSHARED")
dynload = Path(destshared).resolve() if destshared else platstdlib / "lib-dynload"
zip_root = Path(sys.base_prefix) / f"lib/python{sys.version_info[0]}{sys.version_info[1]}.zip"
startup_roots = list(dict.fromkeys([stdlib, platstdlib, dynload, zip_root]))
result = {"layout": args.layout, "prefix": describe(args.prefix),
          "python": describe(args.python), "base_prefix": describe(sys.base_prefix),
          "stdlib": describe(stdlib), "purelib": describe(sysconfig.get_path("purelib")),
          "sys_path": [describe(path) for path in sys.path[:16]],
          "destshared": describe(destshared) if destshared else None,
          "startup_roots": [describe(path) for path in startup_roots[:8]]}
try:
    adapter.establish_interpreter_anchor(args)
except BaseException as error:
    result.update(exception=type(error).__name__, message=str(error),
                  toolchain_refusal=toolchain_refusal, anchor_subject=anchor_subject)
else:
    result["status"] = "eligible"
print(json.dumps(result, sort_keys=True))'''
        completed = subprocess.run(
            [str(python), "-I", "-S", "-B", "-c", program, str(SCRIPT), *arguments],
            cwd=self.root, env={"PATH": os.environ["PATH"]}, capture_output=True, text=True)
        return completed.stdout.strip() or json.dumps(
            {"diagnostic_returncode": completed.returncode,
             "diagnostic_stderr": completed.stderr.strip()})

    def test_different_selected_interpreter_refuses_before_process_or_write(self):
        selected = self.root / "selected-python"
        marker = self.root / "selected-executed"
        selected.write_text(f"#!{sys.executable}\nfrom pathlib import Path\nPath({str(marker)!r}).touch()\n")
        selected.chmod(0o755)
        before = {path.relative_to(self.root): path.read_bytes() for path in self.root.rglob("*")
                  if path.is_file()}
        completed = self.invoke(["-I", "-S", "-B"], selected)
        self.assertNotEqual(completed.returncode, 0)
        self.assertEqual(json.loads(completed.stdout)["code"], "interpreter-anchor-required")
        self.assertFalse(marker.exists())
        self.assertEqual({path.relative_to(self.root): path.read_bytes() for path in self.root.rglob("*")
                          if path.is_file()}, before)

    def test_each_missing_isolation_flag_refuses_before_write(self):
        cases = (("isolated", ["-S", "-B"]),
                 ("no-site", ["-I", "-B"]),
                 ("no-bytecode", ["-I", "-S"]))
        for name, flags in cases:
            with self.subTest(flag=name):
                before = list(self.root.iterdir())
                completed = self.invoke(flags, Path(sys.executable))
                self.assertNotEqual(completed.returncode, 0)
                self.assertEqual(json.loads(completed.stdout)["code"], "interpreter-anchor-required")
                self.assertEqual(list(self.root.iterdir()), before)

    def test_direct_isolated_os_python_reaches_artifact_preflight(self):
        python = Path(sys._base_executable).resolve()
        arguments = self.arguments(python)
        arguments[arguments.index("--prefix") + 1] = sys.base_prefix
        completed = subprocess.run([str(python), "-I", "-S", "-B", str(SCRIPT), *arguments],
                                   cwd=self.root, env={"PATH": os.environ["PATH"]},
                                   capture_output=True, text=True)
        self.assertNotEqual(completed.returncode, 0)
        code = json.loads(completed.stdout)["code"]
        if code != "artifact-invalid":
            self.fail(f"unexpected code {code!r}; anchor diagnostic: "
                      f"{self.anchor_diagnostic(python, arguments)}")

    def test_direct_conventional_venv_symlink_reaches_artifact_preflight(self):
        environment = self.root / "venv"
        venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
        python = environment / "bin/python"
        arguments = self.arguments(python)
        replacements = {"--layout": "venv", "--prefix": str(environment),
                        "--purelib": str(environment / "lib" / f"python{sys.version_info[0]}.{sys.version_info[1]}" / "site-packages"),
                        "--scripts": str(environment / "bin")}
        source_index = arguments.index("--source-root")
        del arguments[source_index:source_index + 2]
        for flag, value in replacements.items():
            arguments[arguments.index(flag) + 1] = value
        completed = subprocess.run([str(python), "-I", "-S", "-B", str(SCRIPT), *arguments],
                                   cwd=self.root, env={"PATH": os.environ["PATH"]},
                                   capture_output=True, text=True)
        self.assertNotEqual(completed.returncode, 0)
        code = json.loads(completed.stdout)["code"]
        if code != "artifact-invalid":
            self.fail(f"unexpected code {code!r}; anchor diagnostic: "
                      f"{self.anchor_diagnostic(python, arguments)}")

    def test_startup_inventory_drift_refuses_before_child_launch(self):
        adapter = load_adapter()
        target = self.root / "python-target"
        target.write_bytes(b"trusted executable")
        target.chmod(0o755)
        selected = self.root / "python-selected"
        selected.symlink_to(target.name)
        anchor = modeled_anchor(selected)
        anchor["inventory"] = adapter._anchor_inventory([selected, target])
        replacement = self.root / "python-replacement"
        replacement.write_bytes(b"replacement executable")
        replacement.chmod(0o755)
        os.replace(replacement, target)
        with patch.object(adapter.subprocess, "run") as launch:
            with self.assertRaises(ValueError):
                adapter.run_anchored_python(anchor, ["-c", "pass"])
        launch.assert_not_called()
        controls = ("executable", "pyvenv.cfg", "stdlib", "stdlib.zip", "lib-dynload")
        for name in controls:
            with self.subTest(object=name):
                control = self.root / (name.replace("/", "-") + "-control")
                if name in {"stdlib", "lib-dynload"}:
                    control.mkdir()
                    changed = control / "startup.py"
                else:
                    changed = control
                changed.write_bytes(b"trusted startup bytes")
                changed.chmod(0o644)
                anchor = modeled_anchor(Path(sys.executable))
                anchor["inventory"] = adapter._anchor_inventory([control])
                changed.write_bytes(b"changed startup bytes")
                with patch.object(adapter.subprocess, "run") as launch:
                    with self.assertRaises(ValueError):
                        adapter.run_anchored_python(anchor, ["-c", "pass"])
                launch.assert_not_called()

    def test_disabled_base_packages_are_excluded_while_stdlib_remains_guarded(self):
        adapter = load_adapter()
        base = self.root / "base"
        stdlib = base / "lib/python3.12"
        dynload = stdlib / "lib-dynload"
        disabled = stdlib / "site-packages"
        active = self.root / "venv/lib/python3.12/site-packages"
        for directory in (dynload, disabled, active):
            directory.mkdir(parents=True)
        startup = stdlib / "startup.py"
        startup.write_bytes(b"trusted startup bytes")
        (dynload / "trusted.so").write_bytes(b"trusted extension bytes")
        for index in range(8):
            (disabled / f"third_party_{index}.py").write_bytes(b"disabled package bytes")
        schemes = {
            "posix_prefix": {
                "stdlib": str(stdlib), "platstdlib": str(stdlib),
                "purelib": str(disabled), "platlib": str(disabled),
            },
            "posix_user": {
                "stdlib": str(self.root / "user/lib/python3.12"),
                "platstdlib": str(self.root / "user/lib/python3.12"),
                "purelib": str(self.root / "user/lib/python3.12/site-packages"),
                "platlib": str(self.root / "user/lib/python3.12/site-packages"),
            },
        }
        with patch.object(adapter.sysconfig, "get_scheme_names", return_value=tuple(schemes)), \
             patch.object(adapter.sysconfig, "get_paths", side_effect=lambda *, scheme, vars: schemes[scheme]):
            exclusions = adapter._disabled_base_site_roots(
                base, [stdlib, dynload], [active])
        self.assertEqual(exclusions, [disabled])
        with patch.object(adapter, "ANCHOR_OBJECT_LIMIT", 6):
            inventory = adapter._anchor_inventory(
                [stdlib, dynload], excluded_roots=[active, *exclusions])
        self.assertIn(str(stdlib), inventory)
        self.assertIn(str(startup), inventory)
        self.assertIn(str(dynload / "trusted.so"), inventory)
        self.assertFalse(any(Path(path).is_relative_to(disabled) for path in inventory))

        anchor = modeled_anchor(Path(sys.executable))
        anchor.update(inventory_roots=[str(stdlib), str(dynload)],
                      inventory_excluded=[str(active), *(str(path) for path in exclusions)],
                      inventory=inventory)
        startup.write_bytes(b"changed startup bytes")
        with patch.object(adapter.subprocess, "run") as launch:
            with self.assertRaisesRegex(ValueError, "anchor changed"):
                adapter.run_anchored_python(anchor, ["-c", "pass"])
        launch.assert_not_called()

    def test_anchor_inventory_enforces_bounds_before_bulk_work(self):
        adapter = load_adapter()
        oversized = self.root / "oversized"
        oversized.write_bytes(b"four")
        with patch.object(adapter, "ANCHOR_BYTE_LIMIT", 3), \
             patch.object(Path, "read_bytes", side_effect=AssertionError("opened oversized file")):
            with self.assertRaisesRegex(ValueError, "byte inventory"):
                adapter._anchor_inventory([oversized])

        crowded = self.root / "crowded"
        crowded.mkdir()
        for index in range(8):
            (crowded / str(index)).write_bytes(b"x")
        consumed = []
        original_iterdir = Path.iterdir

        def observed_iterdir(path):
            for child in original_iterdir(path):
                consumed.append(child)
                yield child

        with patch.object(adapter, "ANCHOR_OBJECT_LIMIT", 3), \
             patch.object(Path, "iterdir", observed_iterdir):
            with self.assertRaisesRegex(ValueError, "inventory exceeded"):
                adapter._anchor_inventory([crowded])
        self.assertLessEqual(len(consumed), 3)

        config = self.root / "pyvenv.cfg"
        config.write_bytes(b"x" * 65537)
        with patch.object(Path, "read_bytes", side_effect=AssertionError("unbounded config read")):
            with self.assertRaisesRegex(ValueError, "configuration refused"):
                adapter._read_pyvenv(config)


class CacheDerivativeTests(unittest.TestCase):
    def setUp(self):
        if os.name == "posix":
            previous = os.umask(0o022)
            self.addCleanup(os.umask, previous)

    def checked_files(self, adapter, args, artifact):
        if os.name == "nt":
            # This compile-only fixture cannot admit the host TEMP ancestor's
            # ACL. Native execution-integrity coverage belongs to its own suite.
            from unittest.mock import patch
            with patch.object(adapter, "execution_integrity"):
                return adapter.public_package_files(args, artifact)
        return adapter.public_package_files(args, artifact)

    def test_trusted_source_caches_across_interning_optimization_and_headers(self):
        # Explicit actual-public-source runs exercise the pinned artifact. The
        # default bounded graph avoids the legacy source cache graph's 11-string cap.
        source = os.environ.get("T3_FLEET_CACHE_SOURCE")
        groups = (("starting", "running"), ("running", "needs_attention"),
                  ("alpha", "beta", "gamma"), ("delta", "epsilon", "zeta"))
        content = Path(source).read_bytes() if source else "\n".join(
            f"def f{i}_{j}(x): return x in {{{', '.join(repr(s) for s in group)}}}"
            for i, group in enumerate(groups) for j in range(3 if i == 1 else 2)).encode()
        adapter = load_adapter()
        with tempfile.TemporaryDirectory(dir="/tmp" if os.name == "posix" else None) as temporary:
            purelib = Path(temporary)
            package = purelib / "t3_fleet"
            package.mkdir()
            installed = package / "agent.py"
            installed.write_bytes(content)
            artifact = {"manifest": {"modules": {"t3_fleet/agent.py":
                        hashlib.sha256(installed.read_bytes()).hexdigest()}}}
            args = adapter.argparse.Namespace(purelib=purelib, python=Path(sys.executable))
            args._anchor = modeled_anchor(args.python)
            generate = ("import sys,py_compile; "
                        "keep=[sys.intern(s) for s in sys.argv[4:]]; "
                        "py_compile.compile(sys.argv[1],doraise=True,optimize=int(sys.argv[2]),"
                        "invalidation_mode=py_compile.PycInvalidationMode[sys.argv[3]])")
            for context in ((), ("starting",), ("needs_attention",), ("running",)):
                for optimization in range(3):
                    for mode in ("TIMESTAMP", "CHECKED_HASH", "UNCHECKED_HASH"):
                        with self.subTest(context=context, optimization=optimization, mode=mode):
                            subprocess.run([sys.executable, "-I", "-S", "-B", "-c", generate,
                                            str(installed), str(optimization), mode, *context], check=True)
                            before = {p: p.read_bytes() for p in package.rglob("*") if p.is_file()}
                            self.assertEqual(self.checked_files(adapter, args, artifact), {"agent.py": installed})
                            self.assertEqual({p: p.read_bytes() for p in package.rglob("*") if p.is_file()}, before)

    def test_cache_alias_candidate_cap_fails_closed(self):
        adapter = load_adapter()
        with tempfile.TemporaryDirectory(dir="/tmp" if os.name == "posix" else None) as temporary:
            purelib = Path(temporary)
            package = purelib / "t3_fleet"
            package.mkdir()
            source = package / "agent.py"
            # Six repeated pairs derive twelve strings: 4096 contexts exceed
            # the budget even though their old alias-partition product was 64.
            source.write_text("\n".join(
                f"def f{i}_{j}(x): return x in {{'alpha{i}', 'beta{i}'}}"
                for i in range(6) for j in range(2)))
            artifact = {"manifest": {"modules": {"t3_fleet/agent.py":
                        hashlib.sha256(source.read_bytes()).hexdigest()}}}
            subprocess.run([sys.executable, "-I", "-S", "-B", "-c",
                            "import py_compile,sys; py_compile.compile(sys.argv[1],doraise=True)",
                            str(source)], check=True)
            args = adapter.argparse.Namespace(purelib=purelib, python=Path(sys.executable))
            args._anchor = modeled_anchor(args.python)
            before = {p: p.read_bytes() for p in package.rglob("*") if p.is_file()}
            with self.assertRaises(ValueError):
                self.checked_files(adapter, args, artifact)
            self.assertEqual({p: p.read_bytes() for p in package.rglob("*") if p.is_file()}, before)


class CompilerBoundaryTests(unittest.TestCase):
    def setUp(self):
        if os.name == "posix":
            previous = os.umask(0o022)
            self.addCleanup(os.umask, previous)

    def test_compiler_budget_and_child_failures_refuse_without_writes(self):
        # Exercise the parent-owned anchored compiler orchestration at its
        # process and shared-clock boundaries without a production test hook.
        adapter = load_adapter()
        with tempfile.TemporaryDirectory(dir="/tmp" if os.name == "posix" else None) as temporary:
            purelib = Path(temporary)
            package = purelib / "t3_fleet"
            package.mkdir()
            source = package / "agent.py"
            source.write_text("value = 1\n")
            subprocess.run([sys.executable, "-I", "-S", "-B", "-c",
                            "import py_compile,sys; py_compile.compile(sys.argv[1],doraise=True)",
                            str(source)], check=True)
            artifact = {"manifest": {"modules": {"t3_fleet/agent.py":
                        hashlib.sha256(source.read_bytes()).hexdigest()}}}
            args = adapter.argparse.Namespace(purelib=purelib, python=Path(sys.executable))
            args._anchor = modeled_anchor(args.python)
            before = {p: p.read_bytes() for p in package.rglob("*") if p.is_file()}
            cases = (("child failure", ValueError("unavailable")),
                     ("invalid discovery", b"{}"),
                     ("cap", json.dumps([str(i) for i in range(11)]).encode()))
            for name, failure in cases:
                with self.subTest(name=name):
                    deadlines = []
                    def child(anchor, deadline, program, *arguments):
                        deadlines.append(deadline)
                        if isinstance(failure, BaseException):
                            raise failure
                        return failure
                    with patch.object(adapter, "_compiler_child", side_effect=child), \
                         patch.object(adapter, "execution_integrity"):
                        with self.assertRaises(ValueError):
                            adapter.public_package_files(args, artifact)
                    self.assertLessEqual(len(set(deadlines)), 1)
                    self.assertEqual({p: p.read_bytes() for p in package.rglob("*") if p.is_file()}, before)
            args._anchor["marshal"] = 6
            with patch.object(adapter, "_compiler_child") as child, \
                 patch.object(adapter, "execution_integrity"):
                with self.assertRaises(ValueError):
                    adapter.public_package_files(args, artifact)
            child.assert_not_called()
            args._anchor["marshal"] = __import__("marshal").version

            clock = [0.0]
            args._anchor.update(resolved=str(Path(args._anchor["python"]).resolve()), inventory={})

            def inventory(*unused, **kwargs):
                clock[0] += 4.0
                return {}

            completed = subprocess.CompletedProcess([], 0, stdout=b"[]", stderr=b"")
            with patch.object(adapter.time, "monotonic", side_effect=lambda: clock[0]), \
                 patch.object(adapter, "_anchor_inventory", side_effect=inventory), \
                 patch.object(adapter.subprocess, "run", return_value=completed) as launch:
                self.assertEqual(adapter._compiler_child(args._anchor, 6, "program"), b"[]")
            self.assertEqual(launch.call_args.kwargs["timeout"], 2)
            with patch.object(adapter.time, "monotonic", return_value=151), \
                 patch.object(adapter, "run_anchored_python") as launch:
                with self.assertRaises(ValueError):
                    adapter._compiler_child(args._anchor, 150, "program")
            launch.assert_not_called()

    def test_package_deadline_includes_initial_anchor_inventory(self):
        adapter = load_adapter()
        with tempfile.TemporaryDirectory(dir="/tmp" if os.name == "posix" else None) as temporary:
            package = Path(temporary) / "t3_fleet"
            cache = package / "__pycache__"
            cache.mkdir(parents=True)
            source = package / "agent.py"
            source.write_text("value = 1\n")
            cache_file = cache / ("agent." + sys.implementation.cache_tag + ".pyc")
            cache_file.write_bytes(importlib.util.MAGIC_NUMBER + struct.pack(
                "<III", 0, int(source.stat().st_mtime) & 0xffffffff,
                len(source.read_bytes()) & 0xffffffff) + b"payload")
            artifact = {"manifest": {"modules": {"t3_fleet/agent.py":
                        hashlib.sha256(source.read_bytes()).hexdigest()}}}
            args = adapter.argparse.Namespace(purelib=Path(temporary), python=Path(sys.executable),
                                              _code="runtime-drift")
            args._anchor = modeled_anchor(args.python)
            clock = [0.0]

            def inventory(*unused, **kwargs):
                clock[0] = 601.0
                return {}

            platform = type("Platform", (), {"name": "nt"})()
            with patch.object(adapter, "os", platform), \
                 patch.object(adapter.time, "monotonic", side_effect=lambda: clock[0]), \
                 patch.object(adapter, "_anchor_inventory", side_effect=inventory), \
                 patch.object(adapter, "execution_integrity"), \
                 patch.object(adapter.subprocess, "run",
                              side_effect=AssertionError("child launched after budget")) as launch:
                with self.assertRaisesRegex(ValueError, "budget exhausted"):
                    adapter.public_package_files(args, artifact)
            launch.assert_not_called()

    def test_package_deadline_selects_platform_budget(self):
        adapter = load_adapter()
        with tempfile.TemporaryDirectory(dir="/tmp" if os.name == "posix" else None) as temporary:
            package = Path(temporary) / "t3_fleet"
            package.mkdir()
            source = package / "agent.py"
            source.write_text("value = 1\n")
            artifact = {"manifest": {"modules": {"t3_fleet/agent.py":
                        hashlib.sha256(source.read_bytes()).hexdigest()}}}
            args = adapter.argparse.Namespace(purelib=Path(temporary), python=Path(sys.executable),
                                              _code="runtime-drift")
            args._anchor = modeled_anchor(args.python)

            for platform_name, expected in (("posix", 150), ("nt", 600)):
                with self.subTest(platform=platform_name):
                    deadlines = []

                    def guard(*unused, deadline=None):
                        deadlines.append(deadline)
                        return args._anchor

                    platform = type("Platform", (), {"name": platform_name})()
                    with patch.object(adapter, "os", platform), \
                         patch.object(adapter.time, "monotonic", return_value=1000), \
                         patch.object(adapter, "guard_interpreter_anchor", side_effect=guard), \
                         patch.object(adapter, "execution_integrity"):
                        adapter.public_package_files(args, artifact)
                    self.assertEqual(deadlines, [1000 + expected])


@unittest.skipUnless(os.name == "nt", "requires native Windows security APIs")
class WindowsPrivateCreationTests(unittest.TestCase):
    def setUp(self):
        self.adapter = load_adapter()
        self.root = Path(tempfile.gettempdir()) / ("t3fleet-private-test-" + uuid.uuid4().hex)
        with contextlib.closing(self.adapter.WindowsSecurity()) as security:
            try:
                with security.parents(self.root):
                    pass
            except ValueError:
                self.skipTest("native TEMP ancestor failed the production trust gate")
        # Creation failures under a trusted parent are test failures, not skips.
        self.adapter.mkdir_private(self.root)
        self.addCleanup(shutil.rmtree, self.root)

    def explicit_runtime_acl(self, path, text=None):
        # Existing native fixture setter, confined to the test-owned TEMP object.
        with contextlib.closing(self.adapter.WindowsSecurity()) as security:
            c = self.adapter.ctypes
            function = security.a.SetNamedSecurityInfoW
            function.argtypes = [security.w.LPWSTR, c.c_int, security.w.DWORD,
                                 security.p, security.p, security.p, security.p]
            function.restype = security.w.DWORD
            if text is None:
                descriptor = security.private_descriptor(directory=path.is_dir())
            else:
                descriptor = security.p()
                security.ok(security.a.ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    text, 1, c.byref(descriptor), None))
            try:
                present, defaulted, dacl = security.w.BOOL(), security.w.BOOL(), security.p()
                security.ok(security.a.GetSecurityDescriptorDacl(
                    descriptor, c.byref(present), c.byref(dacl), c.byref(defaulted)))
                self.assertEqual(function(str(path), 1, 0x80000005, security.me, None, dacl, None), 0)
            finally:
                security.k.LocalFree(descriptor)

    def test_runtime_baseline_refuses_unprotected_objects_before_plan_or_apply_writes(self):
        adapter = self.adapter
        for unsafe in ("package", "nested", "module", "metadata", "fleet", "agent", "identity"):
            with self.subTest(object=unsafe):
                base = self.root / unsafe
                adapter.mkdir_private(base)
                package, nested, metadata = base / "package", base / "package/nested", base / "metadata"
                paths = {"package": package, "nested": nested, "module": nested / "one.py",
                         "metadata": metadata, "fleet": base / "fleet", "agent": base / "agent",
                         "identity": base / "identity"}
                for name, path in paths.items():
                    directory = name in {"package", "nested", "metadata"}
                    if name == unsafe:
                        path.mkdir() if directory else path.write_bytes(b"original")
                    elif directory:
                        adapter.mkdir_private(path)
                    else:
                        adapter.write_private(path, b"original")
                    if name != unsafe:
                        self.explicit_runtime_acl(path)
                inputs = [{"destination": str(path), "runtime": True,
                           "kind": "tree" if path.is_dir() else "file", "exists": True}
                          for path in (package, metadata, paths["fleet"], paths["agent"], paths["identity"])]
                # Readonly toolchain/private input ACLs are outside runtime rollback.
                private = base / "private-input"; private.write_bytes(b"retained")
                inputs.append({"destination": str(private), "runtime": False})
                args = adapter.argparse.Namespace(purelib=base, layout="windows-user-site")
                def observed():
                    return {str(path.relative_to(base)): (path.stat().st_mode, path.stat().st_mtime_ns,
                            path.read_bytes() if path.is_file() else None)
                            for path in base.rglob("*")}
                before = observed()
                with patch.object(adapter, "validate_artifact", return_value={}), \
                     patch.object(adapter, "inspect_layout", return_value={}), \
                     patch.object(adapter, "inventory_backup", return_value=inputs), \
                     patch.object(adapter, "check_boundaries"):
                    for operation in (adapter.plan, adapter.apply):
                        with self.assertRaisesRegex(ValueError, "Windows runtime baseline"):
                            operation(args)
                        self.assertEqual(observed(), before)
                        self.assertFalse((base / adapter.LOCK).exists())
                        self.assertFalse((base / adapter.RECOVERY).exists())

    def test_runtime_baseline_is_rechecked_after_acquiring_apply_lock(self):
        adapter = self.adapter
        base = self.root / "lock-recheck"; adapter.mkdir_private(base)
        path = base / "module.py"; adapter.write_private(path, b"baseline")
        self.explicit_runtime_acl(path)
        inputs = [{"destination": str(path), "runtime": True, "kind": "file", "exists": True}]
        args = adapter.argparse.Namespace(purelib=base, layout="windows-user-site")
        real_lock = adapter.local_lock
        entered = []
        @contextlib.contextmanager
        def change_at_lock(parent):
            with real_lock(parent):
                entered.append((base / adapter.LOCK).exists())
                path.unlink(); path.write_bytes(b"baseline")  # An external writer's inherited ACL.
                yield
        with patch.object(adapter, "validate_artifact", return_value={}), \
             patch.object(adapter, "inspect_layout", return_value={}), \
             patch.object(adapter, "inventory_backup", return_value=inputs), \
             patch.object(adapter, "check_boundaries"):
            adapter.plan(args)  # The protected original is eligible before the lock.
            with patch.object(adapter, "local_lock", change_at_lock):
                with self.assertRaisesRegex(ValueError, "Windows runtime baseline"):
                    adapter.apply(args)
        self.assertEqual(path.read_bytes(), b"baseline")
        self.assertEqual(entered, [True])
        self.assertFalse((base / adapter.LOCK).exists())  # Failed scope removes its new lock.
        self.assertFalse((base / adapter.RECOVERY).exists())
        self.assertEqual(args._code, "windows-baseline-ineligible")

    def test_saved_runtime_descriptors_require_native_protected_known_acl(self):
        adapter = self.adapter
        path = self.root / "baseline"
        adapter.write_private(path, b"baseline")
        before = adapter.fingerprint(path)
        entry = {"runtime": True, "kind": "file", "exists": True,
                 "destination": str(path), "before": before}
        # Native creation gives P without completed auto-inheritance. The current
        # exact restoration setter cannot round-trip that control state.
        with self.assertRaisesRegex(ValueError, "Windows runtime baseline"):
            adapter.windows_runtime_baseline([entry], saved=True)
        self.explicit_runtime_acl(path)
        before = adapter.fingerprint(path)
        entry["before"] = before
        adapter.windows_runtime_baseline([entry], saved=True)
        for descriptor in ("opaque-P-descriptor", "D:P", "O:SYG:SYD:NO_ACCESS_CONTROL",
                           "O:SYG:SYD:(A;;FA;;;SY)", "O:SYG:SYD:P(A;;FA;;;SY)",
                           "O:SYG:SYD:PAI(OA;;FA;00000000-0000-0000-0000-000000000001;;SY)"):
            with self.subTest(descriptor_class="unsupported"):
                entry["before"] = {**before, "acl": descriptor}
                with self.assertRaisesRegex(ValueError, "Windows runtime baseline"):
                    adapter.windows_runtime_baseline([entry], saved=True)
        # An optional absent destination has no original descriptor to restore.
        adapter.windows_runtime_baseline([{"runtime": True, "exists": False, "kind": "file",
                                          "before": {"kind": "absent"}}], saved=True)

    def test_execution_integrity_rejects_protected_writable_acl_separately_from_restore(self):
        adapter = self.adapter
        path = self.root / "writable-executable"
        adapter.write_private(path, b"same exact bytes")
        self.explicit_runtime_acl(path, "D:PAI(A;;FA;;;WD)(A;;FA;;;SY)")
        with contextlib.closing(adapter.WindowsSecurity()) as security:
            security.restoration_descriptor(adapter.windows_descriptor(path))
        before = adapter.fingerprint(path)
        with self.assertRaisesRegex(ValueError, "mutation grant"):
            adapter.execution_integrity(path)
        with self.assertRaisesRegex(ValueError, "mutation grant"):
            adapter.execution_integrity(path, saved=before)
        self.assertEqual(adapter.fingerprint(path), before)
        readonly_admin = {"kind": "file", "mode": 0o444,
                          "acl": "O:BAG:BAD:PAI(A;;FA;;;BA)(A;;FR;;;WD)"}
        adapter.execution_integrity(path, saved=readonly_admin)
        with self.assertRaises(ValueError):
            adapter.execution_integrity(path, saved=readonly_admin, identity=True)

    def test_private_creation_publication_and_existing_lock_exclusion(self):
        path = self.root / "record"
        self.adapter.atomic_json(path, {"synthetic": True})
        self.adapter.private_file(path)
        self.assertEqual(json.loads(path.read_text()), {"synthetic": True})
        self.adapter.atomic_json(path, {"synthetic": False})
        self.adapter.private_file(path)
        self.assertEqual(json.loads(path.read_text()), {"synthetic": False})
        with self.adapter.local_lock(self.root):
            with self.assertRaises((ValueError, OSError)):
                with self.adapter.local_lock(self.root):
                    self.fail("a second lock was admitted")
        lock = self.root / self.adapter.LOCK
        lock.write_bytes(b"retained lock content")
        with self.adapter.local_lock(self.root):
            pass
        self.assertEqual(lock.read_bytes(), b"retained lock content")
        self.adapter.private_file(lock)

    def test_setup_and_prewrite_failure_remove_only_exclusively_created_objects(self):
        path = self.root / "record"
        with patch.object(self.adapter.WindowsSecurity, "private_descriptor", side_effect=ValueError("injected")):
            with self.assertRaises(ValueError):
                self.adapter.write_private(path, b"private bytes")
        self.assertFalse(path.exists())
        original = self.adapter.WindowsSecurity.validate
        sizes = []
        def fail_after_gate(security, handle, **options):
            original(security, handle, **options)
            if options["private"]:
                c = self.adapter.ctypes
                size = c.c_longlong()
                function = security.k.GetFileSizeEx
                function.argtypes = [security.w.HANDLE, c.POINTER(c.c_longlong)]
                function.restype = security.w.BOOL
                security.ok(function(handle, c.byref(size)))
                sizes.append(size.value)
                raise ValueError("injected after gate")
        with patch.object(self.adapter.WindowsSecurity, "validate", fail_after_gate):
            with self.assertRaises(ValueError):
                self.adapter.write_private(path, b"private bytes")
        self.assertEqual(sizes, [0])
        self.assertFalse(path.exists())
        self.adapter.write_private(path, b"preserve")
        with self.assertRaises(FileExistsError):
            self.adapter.write_private(path, b"replacement")
        self.assertEqual(path.read_bytes(), b"preserve")

    def test_short_writes_complete_and_zero_write_fails_without_residual(self):
        path = self.root / "short"
        original = self.adapter.WindowsSecurity.write_chunk
        def short(security, handle, data):
            return original(security, handle, data[:3])
        with patch.object(self.adapter.WindowsSecurity, "write_chunk", short):
            self.adapter.write_private(path, b"0123456789")
        self.assertEqual(path.read_bytes(), b"0123456789")
        path = self.root / "zero"
        with patch.object(self.adapter.WindowsSecurity, "write_chunk", return_value=0):
            with self.assertRaises(ValueError):
                self.adapter.write_private(path, b"private bytes")
        self.assertFalse(path.exists())

    def test_source_stage_matcher_checks_native_acl_and_exact_files(self):
        stage = self.root / "source-stage"
        self.adapter.mkdir_private(stage)
        module = stage / "module.py"
        self.adapter.write_private(module, b"# staged module\n")
        modules = {"t3_fleet/module.py": hashlib.sha256(module.read_bytes()).hexdigest()}
        expected = self.adapter.private_stage_identity(modules)

        self.assertTrue(
            self.adapter.matches_private_stage(
                stage, self.adapter.fingerprint(stage), expected
            )
        )
        self.adapter.write_private(stage / "unexpected.py", b"# foreign module\n")
        self.assertFalse(
            self.adapter.matches_private_stage(
                stage, self.adapter.fingerprint(stage), expected
            )
        )


class InstallPublicRuntimeTests(unittest.TestCase):
    def setUp(self):
        if os.name == "posix":
            previous = os.umask(0o022)
            self.addCleanup(os.umask, previous)
        self.assertTrue(SCRIPT.is_file(), "local operator adapter is missing")
        self.adapter = load_adapter()
        # Reuse the artifact owner's public fixture, rather than inventing another contract.
        self.artifacts = artifact_contract.PublicRuntimeTests()
        with patch("tempfile.tempdir", "/tmp" if os.name == "posix" else None):
            self.artifacts.setUp()
        self.addCleanup(self.artifacts.doCleanups)
        self.root = self.artifacts.root
        self.artifacts.git_fixture()
        self.artifact_dir = self.root / "artifacts"
        self.artifacts.write_wheel(self.artifacts.wheel_members())
        self.artifacts.api["publish_artifacts"](
            self.artifact_dir, self.artifacts.wheel, self.artifacts.pin,
            self.artifacts.source_files)
        self.prefix = self.root / "selected-python"
        self.prefix.mkdir()
        self.python = self.prefix / "python"
        self.python.write_text("fixture interpreter, never executed\n")
        self.purelib = self.root / "source-reviewed"
        self.purelib.mkdir()
        self.purelib.chmod(0o755)
        package = self.purelib / "t3_fleet"
        package.mkdir()
        (package / "__init__.py").write_text("raise RuntimeError('installed code executed')\n")
        (package / "stale.py").write_text("old module\n")
        (package / "__pycache__").mkdir()
        (package / "__pycache__/stale.pyc").write_bytes(b"old bytecode")
        self.scripts = self.root / "bin"
        self.scripts.mkdir()
        self.scripts.chmod(0o755)
        self.launchers = [self.scripts / name for name in ("t3-fleet", "t3-fleet-agent")]
        for path, module in zip(self.launchers, ("cli", "agent")):
            path.write_text(f'#!/bin/sh\nexport PYTHONPATH="{self.purelib}"\n'
                            f'exec "{self.python}" -m t3_fleet.{module} "$@"\n')
        self.config = self.root / "private.toml"
        self.policy = self.root / "policy.json"
        self.policy.write_text('{"private": "not-for-output"}\n')
        self.task = self.root / "task-sentinel"
        self.task.write_bytes(b"unchanged task")
        self.hash_record = self.root / "hashes.json"
        self.backups = self.root / "backups"
        self.backups.mkdir(mode=0o700)
        self.state = self.root / "custom-state" / "fleet.db"
        self.target = self.root / "another-target" / "target.db"
        self.outbox = self.state.parent / "outbox/outbox.db"
        self.connections = []
        self.addCleanup(lambda: [connection.close() for connection in self.connections])
        for path in (self.state, self.target, self.outbox):
            path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(path)
            self.connections.append(connection)
            connection.execute("pragma journal_mode=wal")
            connection.execute("create table retained (payload text)")
            connection.execute("insert into retained values ('private queued prompt and intent')")
            connection.commit()
        self.config.write_text(
            f'[hosts.target-a]\nagent_state_dir = {json.dumps(str(self.target.parent))}\n'
            't3_url = "http://127.0.0.1:3774"\n'
        )
        self.argv = ["--layout", "source-wrapper", "--python", str(self.python),
                     "--prefix", str(self.prefix), "--purelib", str(self.purelib),
                     "--scripts", str(self.scripts), "--source-root", str(self.purelib),
                     "--fleet-launcher", str(self.launchers[0]),
                     "--agent-launcher", str(self.launchers[1]),
                     "--artifact-dir", str(self.artifact_dir),
                     "--pin", str(self.artifacts.pin_file),
                     "--public-source", str(self.artifacts.source),
                     "--target-host", "target-a",
                     "--config", str(self.config), "--private-file", str(self.policy),
                     "--private-file", str(self.task),
                     "--state-db", str(self.state), "--state-db", str(self.target),
                     "--hash-record", str(self.hash_record), "--backup-parent", str(self.backups)]
        self.probe = {"executable": str(self.python), "prefix": str(self.prefix),
                      "base_prefix": str(self.prefix), "purelib": str(self.purelib),
                      "scripts": str(self.scripts), "version": [3, 14, 7],
                      "user_site_enabled": True, "platform": "posix",
                      "import_origin": str(self.purelib / "t3_fleet/__init__.py")}
        self.anchor = modeled_anchor(self.python, self.probe)
        self.anchor_patch = patch.object(self.adapter, "establish_interpreter_anchor",
                                         return_value=self.anchor)
        self.anchor_patch.start()
        self.addCleanup(self.anchor_patch.stop)
        self.adapter.load_builder()
        self.probe_patch = patch.object(self.adapter, "probe_interpreter", return_value=self.probe)
        self.probe_patch.start()
        self.addCleanup(self.probe_patch.stop)

    def set_pin(self, path):
        if "--pin" in self.argv:
            self.argv[self.argv.index("--pin") + 1] = str(path)
        else:
            self.argv.extend(["--pin", str(path)])

    def options(self, action="plan", extra=()):
        result = self.adapter.parser().parse_args([action, *self.argv, *extra])
        result._anchor = self.anchor
        return result

    def invoke(self, arguments):
        output = io.StringIO()
        diagnostics = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(diagnostics):
            code = self.adapter.main(arguments)
        output = output.getvalue()
        self.diagnostics = diagnostics.getvalue()
        for captured in (output, self.diagnostics):
            self.assertNotIn("not-for-output", captured)
            self.assertNotIn("private queued prompt", captured)
            self.assertNotIn(str(self.root), captured)
        return code, output

    def call(self, action="plan", extra=()):
        return self.invoke([action, *self.argv, *extra])

    def test_call_separates_diagnostics_from_structured_stdout(self):
        diagnostic = "bounded resource diagnostic"
        real_plan = self.adapter.plan

        def plan_with_diagnostic(args):
            print(diagnostic, file=sys.stderr)
            return real_plan(args)

        with patch.object(self.adapter, "plan", side_effect=plan_with_diagnostic):
            code, output = self.call()
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "planned")
        self.assertNotIn(diagnostic, output)
        self.assertIn(diagnostic, self.diagnostics)

        with patch.object(
            self.adapter,
            "plan_report",
            return_value={"detail": "not-for-output"},
        ):
            with self.assertRaises(AssertionError):
                self.call()

        def plan_with_private_diagnostic(args):
            warnings.warn("private queued prompt", ResourceWarning)
            return real_plan(args)

        with warnings.catch_warnings():
            warnings.simplefilter("always", ResourceWarning)
            with patch.object(
                self.adapter,
                "plan",
                side_effect=plan_with_private_diagnostic,
            ):
                with self.assertRaises(AssertionError):
                    self.call()

    def three_host_config(
        self,
        *,
        selected_state: Path | None = None,
        selected_origin: str = "http://127.0.0.1:3774",
        third_origin: str = "https://unused.example.invalid",
        third_state: Path | None = None,
    ) -> str:
        selected_state = selected_state or self.target.parent
        third_state = third_state or self.root / "unused-target"
        return f'''\
state_dir = {json.dumps(str(self.state.parent))}

[hosts.controller-a]
transport = "local"
agent_state_dir = {json.dumps(str(self.root / "foreign-controller"))}
t3_url = "http://127.0.0.1:3773"

[hosts.target-a]
transport = "ssh"
agent_state_dir = {json.dumps(str(selected_state))}
t3_url = {json.dumps(selected_origin)}

[hosts.target-b]
transport = "ssh"
agent_state_dir = {json.dumps(str(third_state))}
t3_url = {json.dumps(third_origin)}
'''

    def layout_diagnostic(self) -> str:
        try:
            self.adapter.inspect_layout(self.options())
        except BaseException as error:
            return json.dumps({"layout": self.argv[self.argv.index("--layout") + 1],
                               "exception": type(error).__name__, "message": str(error)},
                              sort_keys=True)
        return json.dumps({"layout": self.argv[self.argv.index("--layout") + 1],
                           "status": "eligible"}, sort_keys=True)

    def tree(self):
        return {str(path.relative_to(self.root)): (path.stat().st_mode, path.stat().st_mtime_ns,
                hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None)
                for path in self.root.rglob("*")}

    def transaction(self):
        return next(self.backups.glob("transaction-*"))

    def prepare_successor(self, *, same_runtime=False):
        attribute = "same_runtime_successor" if same_runtime else "successor"
        if hasattr(self, attribute):
            return getattr(self, attribute)
        source_a = self.artifacts.source
        artifact_a = self.artifact_dir
        pin_a = copy.deepcopy(self.artifacts.pin)
        source_files_a = dict(self.artifacts.source_files)
        modules_a = dict(self.artifacts.modules)
        pin_file_a = self.artifacts.pin_file
        wheel_a = self.artifacts.wheel

        source_b = self.root / "source-b"
        shutil.copytree(source_a, source_b)
        cli_b = b'''\
import argparse

def main():
    parser = argparse.ArgumentParser(prog="t3-fleet")
    parser.parse_args()

if __name__ == "__main__":
    main()

# public fixture successor
'''
        changed_path = "README.md" if same_runtime else "src/t3_fleet/cli.py"
        if same_runtime:
            readme_b = source_files_a["README.md"] + b"\nMetadata-only successor.\n"
            (source_b / changed_path).write_bytes(readme_b)
        else:
            (source_b / changed_path).write_bytes(cli_b)
        subprocess.run(
            ["git", "-C", str(source_b), "add", changed_path],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "-C", str(source_b), "commit", "-qm", "successor fixture"],
            check=True,
            capture_output=True,
        )
        revision_b = subprocess.run(
            ["git", "-C", str(source_b), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        tree_b = subprocess.run(
            ["git", "-C", str(source_b), "rev-parse", "HEAD^{tree}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        pin_b = copy.deepcopy(pin_a)
        pin_b.update(revision=revision_b, tree=tree_b)
        modules_b = dict(modules_a)
        source_files_b = dict(source_files_a)
        if same_runtime:
            source_files_b["README.md"] = readme_b
        else:
            modules_b["t3_fleet/cli.py"] = cli_b
            source_files_b["src/t3_fleet/cli.py"] = cli_b
        pin_file_b = self.root / "pin-b.json"
        pin_file_b.write_text(json.dumps(pin_b), encoding="utf-8")
        build_b = self.root / "build-b"
        build_b.mkdir()
        wheel_b = build_b / WHEEL
        artifact_b = self.root / "artifacts-b"

        try:
            self.artifacts.source = source_b
            self.artifacts.pin = pin_b
            self.artifacts.modules = modules_b
            self.artifacts.source_files = source_files_b
            self.artifacts.pin_file = pin_file_b
            self.artifacts.wheel = wheel_b
            self.artifacts.write_wheel(self.artifacts.wheel_members())
            self.artifacts.api["publish_artifacts"](
                artifact_b,
                wheel_b,
                pin_b,
                source_files_b,
            )
        finally:
            self.artifacts.source = source_a
            self.artifacts.pin = pin_a
            self.artifacts.modules = modules_a
            self.artifacts.source_files = source_files_a
            self.artifacts.pin_file = pin_file_a
            self.artifacts.wheel = wheel_a

        successor = {
            "artifact": artifact_b,
            "pin": pin_b,
            "pin_file": pin_file_b,
            "source": source_b,
            "source_files": source_files_b,
            "predecessor_artifact": artifact_a,
            "predecessor_pin": pin_a,
            "predecessor_pin_file": pin_file_a,
            "predecessor_source": source_a,
        }
        setattr(self, attribute, successor)
        return successor

    def prepare_from(self, base, label, marker):
        """Build an independent successor artifact from an existing one."""
        source = self.root / ("source-" + label)
        shutil.copytree(base["source"], source)
        changed = base["source_files"]["src/t3_fleet/cli.py"] + marker
        (source / "src/t3_fleet/cli.py").write_bytes(changed)
        subprocess.run(["git", "-C", str(source), "add", "src/t3_fleet/cli.py"],
                       check=True, capture_output=True)
        subprocess.run(["git", "-C", str(source), "commit", "-qm", label + " fixture"],
                       check=True, capture_output=True)
        revision = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"],
                                  check=True, capture_output=True, text=True).stdout.strip()
        tree = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD^{tree}"],
                              check=True, capture_output=True, text=True).stdout.strip()
        pin = copy.deepcopy(base["pin"])
        pin.update(revision=revision, tree=tree)
        modules = dict(base["source_files"])
        source_files = dict(base["source_files"])
        modules.pop("src/pyproject.toml", None)
        source_files["src/t3_fleet/cli.py"] = changed
        module_map = {name.removeprefix("src/"): data for name, data in
                      base["source_files"].items() if name.startswith("src/t3_fleet/")}
        module_map["t3_fleet/cli.py"] = changed
        pin_file = self.root / ("pin-" + label + ".json")
        pin_file.write_text(json.dumps(pin), encoding="utf-8")
        build = self.root / ("build-" + label)
        build.mkdir()
        wheel = build / WHEEL
        artifact = self.root / ("artifacts-" + label)
        artifacts = self.artifacts
        saved = (artifacts.source, artifacts.pin, artifacts.modules,
                 artifacts.source_files, artifacts.pin_file, artifacts.wheel)
        try:
            artifacts.source = source
            artifacts.pin = pin
            artifacts.modules = module_map
            artifacts.source_files = source_files
            artifacts.pin_file = pin_file
            artifacts.wheel = wheel
            artifacts.write_wheel(artifacts.wheel_members())
            artifacts.api["publish_artifacts"](artifact, wheel, pin, source_files)
        finally:
            (artifacts.source, artifacts.pin, artifacts.modules,
             artifacts.source_files, artifacts.pin_file, artifacts.wheel) = saved
        return {"artifact": artifact, "pin": pin, "pin_file": pin_file,
                "source": source, "source_files": source_files,
                "predecessor": base}

    def select_successor(self, *, same_runtime=False):
        successor = self.prepare_successor(same_runtime=same_runtime)
        self.argv[self.argv.index("--artifact-dir") + 1] = str(successor["artifact"])
        self.argv[self.argv.index("--public-source") + 1] = str(successor["source"])
        self.set_pin(successor["pin_file"])
        return successor

    def install_predecessor_select_successor(self, *, same_runtime=False):
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        predecessor_backup = self.transaction()
        record_path = self.purelib / self.adapter.RECOVERY
        predecessor_raw = record_path.read_bytes()
        predecessor_tree = self.tree()
        successor = self.select_successor(same_runtime=same_runtime)
        upgrade_backup = self.backups / ("transaction-" + uuid.uuid4().hex)
        return predecessor_backup, predecessor_raw, predecessor_tree, successor, upgrade_backup

    @contextlib.contextmanager
    def fresh_upgrade_case(self):
        case = type(self)("test_plan_is_write_free_and_does_not_execute_installed_code")
        case.setUp()
        try:
            yield case
        finally:
            case.doCleanups()

    def rewrite_upgrade_inventory(self, upgrade_backup, mutate):
        inventory_path = upgrade_backup / "inventory.json"
        record_path = self.purelib / self.adapter.RECOVERY
        inventory = json.loads(inventory_path.read_bytes())
        record = json.loads(record_path.read_bytes())
        mutate(inventory)
        inventory_raw = (
            json.dumps(inventory, sort_keys=True, indent=2) + "\n"
        ).encode()
        inventory_path.write_bytes(inventory_raw)
        record["inventory_sha256"] = hashlib.sha256(inventory_raw).hexdigest()
        record_raw = (
            json.dumps(record, sort_keys=True, indent=2) + "\n"
        ).encode()
        record_path.write_bytes(record_raw)
        return inventory, record_raw

    def selected_identity_change(self, *, selected_state, selected_origin):
        alternate_state = self.root / "alternate-target"
        alternate_db = alternate_state / "target.db"
        if not alternate_db.exists():
            alternate_db.parent.mkdir(parents=True)
            with contextlib.closing(sqlite3.connect(alternate_db)) as connection:
                connection.execute("create table retained (payload text)")
                connection.execute("insert into retained values ('alternate private state')")
                connection.commit()
            self.argv.extend(["--state-db", str(alternate_db)])
        original = self.three_host_config()
        self.config.write_text(original)
        if selected_state == "alternate":
            selected_state = alternate_state
        changed = self.three_host_config(
            selected_state=selected_state,
            selected_origin=selected_origin,
        )
        return original, changed

    def assert_binding_refusal(self, output, *, code, phase, writes):
        self.assertEqual(
            json.loads(output),
            {
                "code": code,
                "phase": phase,
                "recovery": "preserve-record-and-backup",
                "status": "refused",
                "writes": writes,
            },
        )

    def install_with_alternate_target(self):
        alternate_state = self.root / "alternate-target"
        alternate_db = alternate_state / "target.db"
        alternate_db.parent.mkdir(parents=True)
        with contextlib.closing(sqlite3.connect(alternate_db)) as connection:
            connection.execute("create table retained (payload text)")
            connection.execute("insert into retained values ('alternate private state')")
            connection.commit()
        self.argv.extend(["--state-db", str(alternate_db)])
        self.config.write_text(self.three_host_config())
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        backup = self.transaction()
        expected = {
            "agent_state_dir": str(self.target.parent),
            "t3_url": "http://127.0.0.1:3774",
        }
        record = json.loads((self.purelib / self.adapter.RECOVERY).read_text())
        inventory = json.loads((backup / "inventory.json").read_text())
        self.assertEqual(record["binding"]["selected_target"], expected)
        self.assertEqual(inventory["binding"]["selected_target"], expected)
        return backup, alternate_state

    def assert_selected_identity_drift_refused(self, *, selected_state, selected_origin):
        backup, alternate_state = self.install_with_alternate_target()
        if selected_state == "alternate":
            selected_state = alternate_state
        self.config.write_text(
            self.three_host_config(
                selected_state=selected_state,
                selected_origin=selected_origin,
            )
        )
        observed = []
        for action, extra in (
            ("plan", []),
            ("apply", ["--apply"]),
            ("rollback", ["--backup", str(backup), "--apply"]),
        ):
            before = self.tree()
            code, output = self.call(action, extra)
            observed.append((action, code, json.loads(output), self.tree() == before))
        expected = {
            "code": "backup-invalid",
            "phase": "preflight",
            "recovery": "preserve-record-and-backup",
            "status": "refused",
            "writes": "none",
        }
        for action, code, result, write_free in observed:
            with self.subTest(action=action):
                self.assertEqual(code, 1, result)
                self.assertEqual(result, expected)
                self.assertTrue(write_free)

    def assert_rollback_binding_edge_refused(self, *, selected_state, selected_origin):
        backup, alternate_state = self.install_with_alternate_target()
        if selected_state == "alternate":
            selected_state = alternate_state
        original_config = self.config.read_text()
        changed_config = self.three_host_config(
            selected_state=selected_state,
            selected_origin=selected_origin,
        )
        expected_refusal = {
            "phase": "locking",
            "recovery": "preserve-record-and-backup",
            "status": "refused",
            "writes": "possible",
        }
        record_path = self.purelib / self.adapter.RECOVERY

        def refuse_after_locked_drift(expected_phase):
            real_check = self.adapter.check_rollback_drift
            checks = []
            snapshot = []

            def change_selected_identity(*args):
                real_check(*args)
                checks.append(True)
                if len(checks) == 2:
                    self.config.write_text(changed_config)
                    snapshot.append(self.tree())

            with patch.object(
                self.adapter,
                "check_rollback_drift",
                side_effect=change_selected_identity,
            ), patch.object(
                self.adapter,
                "cleanup_owned",
                wraps=self.adapter.cleanup_owned,
            ) as cleanup:
                code, output = self.call(
                    "rollback", ["--backup", str(backup), "--apply"]
                )
            self.assertEqual(len(checks), 2)
            self.assertEqual(code, 1, output)
            self.assertEqual(
                json.loads(output),
                {
                    **expected_refusal,
                    "code": (
                        "recovery-recheck-failed"
                        if expected_phase == "rolled-back"
                        else "rollback-failed"
                    ),
                },
            )
            self.assertEqual(self.tree(), snapshot[0])
            self.assertEqual(json.loads(record_path.read_text())["phase"], expected_phase)
            cleanup.assert_not_called()

        refuse_after_locked_drift("verified")
        self.config.write_text(original_config)
        code, output = self.call(
            "rollback", ["--backup", str(backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "rolled-back")
        refuse_after_locked_drift("rolled-back")

    def windows_fixture(self, *, extra_script=""):
        self.argv[self.argv.index("--layout") + 1] = "windows-user-site"
        index = self.argv.index("--source-root")
        del self.argv[index:index + 2]
        self.probe["platform"] = "nt"
        self.probe.update(pointer_bits=64, sysconfig_platform="win-amd64")
        pip_root = self.root / "global-python-site"
        (pip_root / "pip").mkdir(parents=True)
        (pip_root / "pip/__init__.py").write_text("# existing global pip fixture\n")
        (pip_root / "pip/_vendor").mkdir()
        (pip_root / "pip/_vendor/__init__.py").write_text("DEBUNDLED = False\n")
        (pip_root / "pip/_internal").mkdir()
        (pip_root / "pip/_internal/__init__.py").write_text("# fixture\n")
        (pip_root / "pip/__main__.py").write_text(
            "import json, sys, fractions\n"
            "print(json.dumps({'origin': __file__, 'argv': sys.argv[1:]}))\n")
        distlib = pip_root / "pip/_vendor/distlib"
        distlib.mkdir()
        (distlib / "scripts.py").write_text(
            "SCRIPT_TEMPLATE = r'''# -*- coding: utf-8 -*-\n"
            "import re\n"
            "import sys\n"
            "from %(module)s import %(import_name)s\n"
            "if __name__ == '__main__':\n"
            "    sys.argv[0] = re.sub(r'(-script\\.pyw|\\.exe)?$', '', sys.argv[0])\n"
            "    sys.exit(%(func)s())\n"
            "'''\n"
            "class ScriptMaker:\n"
            "    script_template = SCRIPT_TEMPLATE\n"
            "    def _get_script_text(self, entry):\n"
            "        return self.script_template % dict(module=entry.prefix, import_name=entry.suffix.split('.')[0], func=entry.suffix)\n"
            "    def _get_launcher(self, kind):\n"
            "        if struct.calcsize('P') == 8:\n"
            "            bits = '64'\n"
            "        else:\n"
            "            bits = '32'\n"
            "        platform_suffix = '-arm' if get_platform() == 'win-arm64' else ''\n"
            "        name = '%s%s%s.exe' % (kind, bits, platform_suffix)\n"
            "        return WRAPPERS[name]\n")
        for resource in ("t32.exe", "t64.exe", "t64-arm.exe"):
            (distlib / resource).write_bytes(b"MZ-fixture-only\0" if resource == "t64.exe"
                                           else b"MZ-other-architecture\0")
        metadata = pip_root / "pip-25.0.dist-info"
        metadata.mkdir()
        (metadata / "METADATA").write_text("Name: pip\nVersion: 25.0\n")
        self.probe["pip_root"] = str(pip_root)
        self.probe["pip_origin"] = str(pip_root / "pip/__init__.py")
        self.probe["site_roots"] = [str(self.purelib), str(pip_root)]
        self.probe["pip_candidates"] = [{"root": str(pip_root), "origin": self.probe["pip_origin"]}]
        for index, module in enumerate(("cli", "agent")):
            launcher = self.launchers[index].with_suffix(".exe")
            self.argv[self.argv.index(("--fleet-launcher", "--agent-launcher")[index]) + 1] = str(launcher)
            self.launchers[index] = launcher
            data = io.BytesIO()
            data.write(b"MZ-fixture-only\0#!" + str(self.python).encode() + b"\n")
            with zipfile.ZipFile(data, "a") as archive:
                archive.writestr("__main__.py", self.console_body(module) + extra_script)
            launcher.write_bytes(data.getvalue())
        dist = self.purelib / DIST.rstrip("/")
        dist.mkdir()
        for name, data in self.artifacts.wheel_members().items():
            if name.startswith(DIST):
                path = self.purelib / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
        record = ["t3_fleet/__init__.py,,", DIST + "METADATA,,", DIST + "RECORD,,"]
        record += [os.path.relpath(path, self.purelib).replace(os.sep, "/") + ",," for path in self.launchers]
        (dist / "RECORD").write_text("\n".join(record) + "\n")
        return dist

    @staticmethod
    def console_body(module):
        return (f"import re\nimport sys\nfrom t3_fleet.{module} import main\n"
                "if __name__ == '__main__':\n"
                "    sys.argv[0] = re.sub(r'(-script\\.pyw|\\.exe)?$', '', sys.argv[0])\n"
                "    sys.exit(main())\n")

    @staticmethod
    def guarded_import_template() -> str:
        return ("# -*- coding: utf-8 -*-\n"
                "import re\n"
                "import sys\n"
                "if __name__ == '__main__':\n"
                "    from %(module)s import %(import_name)s\n"
                "    sys.argv[0] = re.sub(r'(-script\\.pyw|\\.exe)?$', '', sys.argv[0])\n"
                "    sys.exit(%(func)s())\n")

    def test_windows_native_stub_must_match_trusted_vendor_resource(self):
        self.windows_fixture()
        with patch.object(self.adapter, "windows_acl", return_value=None):
            self.assertEqual(self.call()[0], 0)
            path = self.launchers[0]
            original = path.read_bytes()
            for data in (original.replace(b"MZ-fixture-only", b"MZ-foreign-code"),
                         original.replace(str(self.python).encode(), b"/wrong/python")):
                with self.subTest(contract="native-prefix-or-shebang"):
                    path.write_bytes(data)
                    before = self.tree()
                    self.assertNotEqual(self.call()[0], 0)
                    self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
                    self.assertEqual(self.tree(), before)
            path.write_bytes(original)

    def test_source_wrapper_accepts_unrelated_distro_executable_pth(self):
        pth = self.purelib / "distutils-precedence.pth"
        original = ("import os; var = 'SETUPTOOLS_USE_DISTUTILS'; enabled = "
                    "os.environ.get(var, 'local') == 'local'; enabled and "
                    "__import__('_distutils_hack').add_shim();\n")
        pth.write_text(original)
        self.assertEqual(self.call()[0], 0)
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        # The unrelated .pth is neither selected nor executed. Source wrappers
        # start with -S -B, so site processing never runs.
        self.assertEqual(pth.read_text(), original)

    @unittest.skipUnless(os.name == "posix", "POSIX mutation permission contract")
    def test_writable_runtime_and_empty_pth_refuse_before_any_writes(self):
        empty = self.purelib / "empty.pth"
        empty.write_text("# startup-free\n\n")
        empty.chmod(0o644)
        outer = self.root / "outer-wrapper"
        outer.write_bytes(b"fixture outer launcher")
        outer.chmod(0o755)
        self.argv += ["--wrapper", str(outer)]
        self.hash_record.write_bytes(b"previous identity")
        self.hash_record.chmod(0o600)
        package = self.purelib / "t3_fleet"
        for path in (package, package / "__init__.py", package / "__pycache__",
                     package / "__pycache__/stale.pyc", *self.launchers, outer,
                     self.hash_record):
            with self.subTest(object=path.name):
                mode = path.stat().st_mode & 0o777
                path.chmod(0o777 if path.is_dir() else 0o666)
                before = self.tree()
                self.assertNotEqual(self.call()[0], 0)
                self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
                self.assertEqual(self.tree(), before)
                path.chmod(mode)
        empty.chmod(0o666)
        # A world-writable unrelated .pth is irrelevant when startup disables
        # site processing, so the source-wrapper plan remains eligible.
        self.assertEqual(self.call()[0], 0)

    @unittest.skipUnless(os.name == "posix", "POSIX metadata mutation permissions")
    def test_writable_owned_metadata_refuses_before_writes(self):
        dist = self.windows_fixture()
        with patch.object(self.adapter, "windows_acl", return_value=None):
            self.assertEqual(self.call()[0], 0)
            for path in (dist, dist / "METADATA"):
                mode = path.stat().st_mode & 0o777
                path.chmod(0o777 if path.is_dir() else 0o666)
                before = self.tree()
                self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
                self.assertEqual(self.tree(), before)
                path.chmod(mode)

    def test_startup_code_added_after_snapshot_is_ignored_by_source_wrapper(self):
        pth = self.purelib / "eligible.pth"
        pth.write_text("# startup-free\n")
        stage = self.adapter.stage_artifact
        original = pth.read_bytes()
        def add_startup_code(*args):
            result = stage(*args)
            pth.write_text("import t3_fleet.agent\n")
            return result
        with patch.object(self.adapter, "stage_artifact", side_effect=add_startup_code):
            code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        self.assertTrue(self.hash_record.exists())
        self.assertNotEqual(pth.read_bytes(), original)  # fixture mutation is inert
        self.assertEqual(
            self.launchers[0].read_text(),
            self.adapter.source_wrapper_text(self.python, self.purelib, "cli"),
        )

    def test_windows_contract_uses_selected_interpreter_and_survives_toolchain_drift(self):
        self.windows_fixture()
        distlib = Path(self.probe["pip_root"]) / "pip/_vendor/distlib"
        with patch.object(self.adapter, "windows_acl", return_value=None):
            original = {p: p.read_bytes() for p in self.launchers}
            for bits, platform, name in ((32, "win32", "t32.exe"), (64, "win-arm64", "t64-arm.exe")):
                self.probe.update(pointer_bits=bits, sysconfig_platform=platform)
                for path in self.launchers:
                    path.write_bytes(original[path].replace(b"MZ-fixture-only\0", (distlib / name).read_bytes()))
                prepared = self.adapter.plan(self.options())
                self.assertEqual(prepared["layout"]["launcher_contract"]["resource"], name)
            self.probe.update(pointer_bits=64, sysconfig_platform="win-amd64")
            for path in self.launchers:
                path.write_bytes(original[path])
            real_lock = self.adapter.local_lock
            @contextlib.contextmanager
            def drift_at_lock(parent):
                with real_lock(parent):
                    (distlib / "t64.exe").write_bytes(b"MZ-new-trusted-toolchain\0")
                    for path in self.launchers:
                        path.write_bytes(original[path].replace(b"MZ-fixture-only\0", b"MZ-new-trusted-toolchain\0"))
                    yield
            with patch.object(self.adapter, "local_lock", drift_at_lock):
                with self.assertRaisesRegex(ValueError, "contract changed"):
                    self.adapter.apply(self.options("apply", ["--apply"]))
            self.assertFalse(list(self.backups.iterdir()))
            self.assertFalse((self.purelib / self.adapter.RECOVERY).exists())
            # A stable but unsupported selector cannot authorize the vendor resource.
            selector = distlib / "scripts.py"
            selector.write_text(selector.read_text().replace("'win-arm64'", "'different-platform'"))
            before = self.tree()
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
            self.assertEqual(self.tree(), before)

    def test_windows_contract_accepts_guarded_entrypoint_import_template(self):
        self.windows_fixture()
        template = self.guarded_import_template()
        pip_root = Path(self.probe["pip_root"])
        self.set_distlib_template(pip_root, template)
        for path, module in zip(self.launchers, ("cli", "agent")):
            data = io.BytesIO()
            data.write(b"MZ-fixture-only\0#!" + str(self.python).encode() + b"\n")
            rendered = template % {"module": f"t3_fleet.{module}",
                                   "import_name": "main", "func": "main"}
            with zipfile.ZipFile(data, "a") as archive:
                archive.writestr("__main__.py", rendered)
            path.write_bytes(data.getvalue())
        with patch.object(self.adapter, "windows_acl", return_value=None):
            prepared = self.adapter.plan(self.options())
        self.assertEqual(prepared["layout"]["launcher_contract"]["template"],
                         "guarded-regex")

    def test_windows_recovery_refuses_interpreter_override_before_probe(self):
        self.windows_fixture()
        with patch.object(self.adapter, "windows_acl", return_value=None):
            with patch.object(self.adapter, "install", side_effect=ValueError("stopped before runtime mutation")):
                self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
            override = self.python.with_suffix("._pth")
            override.write_text("uninspected-root\nimport site\n")
            before = self.tree()
            with patch.object(self.adapter, "probe_interpreter", side_effect=AssertionError("probe must not start")):
                self.assertNotEqual(self.call("rollback", ["--backup", str(self.transaction()), "--apply"])[0], 0)
            self.assertEqual(self.tree(), before)
            override.unlink()
            self.assertEqual(self.call("rollback", ["--backup", str(self.transaction()), "--apply"])[0], 0)

    def test_launcher_selector_drift_after_snapshot_preserves_runtime_and_contract(self):
        self.windows_fixture()
        selector = Path(self.probe["pip_root"]) / "pip/_vendor/distlib/scripts.py"
        original = selector.read_bytes()
        stage = self.adapter.stage_artifact
        package = self.adapter.fingerprint(self.purelib / "t3_fleet")
        def change_selector(*args):
            result = stage(*args)
            selector.write_bytes(original + b"# toolchain changed after preflight\n")
            return result
        with patch.object(self.adapter, "windows_acl", return_value=None):
            with patch.object(self.adapter, "stage_artifact", side_effect=change_selector):
                self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
            self.assertEqual(self.adapter.fingerprint(self.purelib / "t3_fleet"), package)
            self.assertFalse(self.hash_record.exists())
            backup = self.transaction()
            inventory = json.loads((backup / "inventory.json").read_text())
            self.assertEqual(inventory["launcher_contract"]["scripts_sha256"], hashlib.sha256(original).hexdigest())
            record = json.loads((self.purelib / self.adapter.RECOVERY).read_text())
            self.assertEqual(record["phase"], "backup-complete")
            before = self.tree()
            self.assertNotEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
            self.assertEqual(self.tree(), before)
            selector.write_bytes(original)
            self.assertEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)

    @unittest.skipUnless(os.name == "posix", "POSIX execution integrity lifecycle")
    def test_execution_integrity_rechecked_at_lock_and_before_identity_publication(self):
        path = self.purelib / "t3_fleet/__init__.py"
        original_mode = path.stat().st_mode & 0o777
        real_lock = self.adapter.local_lock
        @contextlib.contextmanager
        def mutate_at_lock(parent):
            with real_lock(parent):
                path.chmod(0o666)
                yield
        with patch.object(self.adapter, "local_lock", mutate_at_lock):
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertFalse(list(self.backups.iterdir()))
        self.assertFalse((self.purelib / self.adapter.RECOVERY).exists())
        path.chmod(original_mode)
        install = self.adapter.install
        def unsafe_target(*args):
            install(*args)
            (self.purelib / "t3_fleet/agent.py").chmod(0o666)
        with patch.object(self.adapter, "install", side_effect=unsafe_target):
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertFalse(self.hash_record.exists())
        backup = self.transaction()
        before = self.tree()
        self.assertNotEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
        self.assertEqual(self.tree(), before)

    def test_interpreter_anchor_drift_before_identity_publication_preserves_recovery(self):
        control = self.root / "interpreter-anchor-control"
        control.write_bytes(b"admitted interpreter bytes")
        admitted_mtime = control.stat().st_mtime_ns
        self.anchor["inventory"] = self.adapter._anchor_inventory([control])
        install = self.adapter.install

        def drift_after_install(*args):
            install(*args)
            control.write_bytes(b"changed interpreter bytes")

        with patch.object(self.adapter, "install", side_effect=drift_after_install):
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertFalse(self.hash_record.exists())
        backup = self.transaction()
        record = json.loads((self.purelib / self.adapter.RECOVERY).read_text())
        self.assertEqual(record["phase"], "mutation-started")
        before = self.tree()
        self.assertNotEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
        self.assertEqual(self.tree(), before)
        control.write_bytes(b"admitted interpreter bytes")
        os.utime(control, ns=(control.stat().st_atime_ns, admitted_mtime))
        self.assertEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)

    @unittest.skipUnless(os.name == "posix", "POSIX saved baseline trust")
    def test_writable_verified_runtime_and_old_saved_modes_preserve_all_evidence(self):
        self.use_real_source_interpreter()
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        backup = self.transaction()
        self.connections[0].execute("insert into retained values ('newer row')")
        self.connections[0].commit()
        package = self.purelib / "t3_fleet"
        subprocess.run([sys.executable, "-I", "-B", "-c",
                        "import py_compile,sys; py_compile.compile(sys.argv[1],doraise=True)",
                        str(package / "agent.py")], check=True)
        for path in (package, package / "agent.py", package / "__pycache__",
                     next(package.glob("__pycache__/*.pyc")), self.hash_record):
            mode = path.stat().st_mode & 0o777
            path.chmod(0o777 if path.is_dir() else 0o666)
            before = self.tree()
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
            self.assertNotEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
            self.assertEqual(self.tree(), before)
            path.chmod(mode)
        inventory_path = backup / "inventory.json"
        recovery_path = self.purelib / self.adapter.RECOVERY
        inventory = json.loads(inventory_path.read_text())
        record = json.loads(recovery_path.read_text())
        saved_inventory, saved_record = inventory_path.read_bytes(), recovery_path.read_bytes()
        for kind in ("unsafe-mode", "missing-policy"):
            if kind == "unsafe-mode":
                inventory["inputs"][0]["before"]["files"]["stale.py"]["mode"] = 0o666
            else:
                inventory = json.loads(saved_inventory)
                inventory.pop("execution_policy", None)
            inventory_path.write_text(json.dumps(inventory))
            record["inventory_sha256"] = self.adapter.sha(inventory_path)
            recovery_path.write_text(json.dumps(record))
            before = self.tree()
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
            self.assertNotEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
            self.assertEqual(self.tree(), before)
        inventory_path.write_bytes(saved_inventory)
        recovery_path.write_bytes(saved_record)
        self.assertEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
        self.assertEqual(self.connections[0].execute("select count(*) from retained").fetchone()[0], 2)

    def test_windows_complete_crash_target_uses_protected_vendor_contract(self):
        self.windows_fixture()
        run, install = self.adapter.run_anchored_python, self.adapter.install
        def model_pip(anchor, arguments, **kwargs):
            if anchor["python"] != str(self.python):
                return run(anchor, arguments, **kwargs)
            self.assertIn("install", arguments)
            wheel = Path(arguments[-1])
            with zipfile.ZipFile(wheel) as archive:
                for name in archive.namelist():
                    path = self.purelib / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(archive.read(name))
            dist = self.purelib / DIST.rstrip("/")
            for name, data in {"INSTALLER": b"pip\n", "REQUESTED": b"",
                    "direct_url.json": json.dumps({"url": wheel.as_uri(), "archive_info": {
                        "hashes": {"sha256": self.adapter.sha(wheel)}}}).encode()}.items():
                (dist / name).write_bytes(data)
            paths = [*(self.purelib / "t3_fleet").rglob("*.py"), *dist.iterdir(), *self.launchers]
            rows = []
            for path in paths:
                relative = os.path.relpath(path, self.purelib).replace(os.sep, "/")
                digest = base64.urlsafe_b64encode(hashlib.sha256(path.read_bytes()).digest()).rstrip(b"=").decode()
                rows.append([relative, "" if path.name == "RECORD" else "sha256=" + digest,
                             "" if path.name == "RECORD" else str(path.stat().st_size)])
            output = io.StringIO()
            csv.writer(output, lineterminator="\n").writerows(rows)
            (dist / "RECORD").write_text(output.getvalue())
            return b""
        def crash(*args):
            install(*args)
            raise KeyboardInterrupt()
        with patch.object(self.adapter, "windows_acl", return_value=None):
            with patch.object(self.adapter, "run_anchored_python", side_effect=model_pip), \
                 patch.object(self.adapter, "install", side_effect=crash):
                with self.assertRaises(KeyboardInterrupt):
                    self.call("apply", ["--apply"])
            backup = self.transaction()
            inventory = json.loads((backup / "inventory.json").read_text())
            contract = inventory["launcher_contract"]
            self.assertEqual(base64.b64decode(contract["prefix"]),
                             b"MZ-fixture-only\0#!" + str(self.python).encode() + b"\n")
            stub = Path(self.probe["pip_root"]) / "pip/_vendor/distlib/t64.exe"
            original = stub.read_bytes()
            # Matching current vendor and launchers cannot replace the protected target authority.
            stub.write_bytes(b"MZ-replaced-trusted-toolchain\0")
            saved_launchers = {path: path.read_bytes() for path in self.launchers}
            for path in self.launchers:
                path.write_bytes(saved_launchers[path].replace(original, stub.read_bytes()))
            before = self.tree()
            self.assertNotEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
            self.assertEqual(self.tree(), before)
            stub.write_bytes(original)
            for path, data in saved_launchers.items():
                path.write_bytes(data)
            # Supported complete target still rolls back using the original protected authority.
            self.assertEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
            self.assertTrue((self.purelib / "t3_fleet/stale.py").exists())

    @unittest.skipUnless(os.name == "posix", "POSIX executable object ownership")
    def test_foreign_owned_runtime_or_comment_pth_refuses_without_writes(self):
        pth = self.purelib / "comment.pth"
        pth.write_text("# no startup code\n")
        stat = self.adapter.Path.stat
        for foreign in (self.purelib / "t3_fleet/__init__.py", pth):
            def foreign_owner(path, *args, **kwargs):
                info = stat(path, *args, **kwargs)
                if path == foreign:
                    fields = list(info)
                    fields[4] = os.geteuid() + 1
                    return os.stat_result(fields)
                return info
            before = self.tree()
            with patch.object(self.adapter.Path, "stat", foreign_owner):
                code = self.call("apply", ["--apply"])[0]
            if foreign == pth:
                # A comment-only .pth is inert under -S -B and is not part of
                # the runtime trust boundary for source wrappers.
                self.assertEqual(code, 0)
            else:
                self.assertNotEqual(code, 0)
                self.assertEqual(self.tree(), before)

    def test_private_staging_with_acl_requires_native_privacy_or_exact_restore(self):
        path = self.root / "staged"
        path.write_bytes(b"synthetic")
        actual = {"mode": 0o666, "acl": "synthetic-private-descriptor"}
        expected = {"mode": 0o444, "acl": "original-descriptor"}
        with patch.object(self.adapter, "windows_acl") as gate:
            self.assertTrue(self.adapter.private_or_restored(path, actual, expected, directory=False))
            gate.assert_called_once_with(path)
        with patch.object(self.adapter, "windows_acl", side_effect=ValueError("not private")):
            self.assertFalse(self.adapter.private_or_restored(path, actual, expected, directory=False))
            self.assertTrue(self.adapter.private_or_restored(path, expected, expected, directory=False))
        # A file-byte match never makes an arbitrary private descriptor eligible.
        with patch.object(self.adapter, "windows_acl", side_effect=ValueError("not private")):
            self.assertFalse(self.adapter.private_or_restored(path, actual, expected, directory=True))

    def test_plan_is_write_free_and_does_not_execute_installed_code(self):
        before = self.tree()
        code, output = self.call()
        self.assertEqual(code, 0, output)
        self.assertEqual(self.tree(), before)
        self.assertIn('"status": "planned"', output)
        self.assertEqual(self.adapter.parser().parse_args(self.argv).action, "plan")

    def test_upgrade_plan_validates_both_pins_without_writes_or_installed_execution(self):
        predecessor_backup, predecessor_raw, _, successor, upgrade_backup = (
            self.install_predecessor_select_successor()
        )
        record_path = self.purelib / self.adapter.RECOVERY
        predecessor_inventory = predecessor_backup / "inventory.json"
        before = self.tree()
        record_digest = hashlib.sha256(predecessor_raw).hexdigest()
        inventory_digest = hashlib.sha256(predecessor_inventory.read_bytes()).hexdigest()

        code, output = self.call("upgrade", ["--backup", str(upgrade_backup)])

        self.assertEqual(code, 0, output)
        result = json.loads(output)
        self.assertEqual(result["status"], "planned")
        self.assertEqual(result["operation"], "upgrade")
        self.assertEqual(result["revision"], successor["pin"]["revision"])
        self.assertEqual(self.tree(), before)
        self.assertEqual(record_path.read_bytes(), predecessor_raw)
        self.assertEqual(hashlib.sha256(record_path.read_bytes()).hexdigest(), record_digest)
        self.assertEqual(hashlib.sha256(predecessor_inventory.read_bytes()).hexdigest(), inventory_digest)
        self.assertFalse(upgrade_backup.exists())

    def test_upgrade_stable_binding_and_record_refusals_are_write_free(self):
        predecessor_backup, predecessor_raw, _, _, upgrade_backup = (
            self.install_predecessor_select_successor()
        )
        record_path = self.purelib / self.adapter.RECOVERY
        inventory_path = predecessor_backup / "inventory.json"
        inventory_raw = inventory_path.read_bytes()
        original_record = json.loads(predecessor_raw)

        def refused(arguments=(), *, recovery="preserve-record-and-backup"):
            before = self.tree()
            code, output = self.call(
                "upgrade", ["--backup", str(upgrade_backup), *arguments]
            )
            self.assertEqual(code, 1, output)
            result = json.loads(output)
            self.assertEqual(result["phase"], "preflight")
            self.assertEqual(result["writes"], "none")
            self.assertEqual(result["recovery"], recovery)
            self.assertEqual(self.tree(), before)
            self.assertFalse(upgrade_backup.exists())

        binding_cases = {}
        for key, value in original_record["binding"].items():
            if key in {"artifact_dir", "public_source"}:
                continue
            changed = copy.deepcopy(original_record)
            if key == "selected_target":
                changed["binding"][key]["t3_url"] = "http://127.0.0.1:3775"
            elif isinstance(value, list):
                changed["binding"][key] = [*value, str(self.root / ("extra-" + key))]
            else:
                changed["binding"][key] = value + "-changed"
            binding_cases[key] = changed
        selected_state = copy.deepcopy(original_record)
        selected_state["binding"]["selected_target"]["agent_state_dir"] = str(self.root / "other-state")
        binding_cases["selected_target.agent_state_dir"] = selected_state
        for label, record in binding_cases.items():
            with self.subTest(binding=label):
                record_path.write_text(json.dumps(record))
                refused()
                record_path.write_bytes(predecessor_raw)

        malformed = []
        unknown = copy.deepcopy(original_record)
        unknown["binding"]["unknown"] = "value"
        malformed.append(("unknown-binding", unknown))
        missing = copy.deepcopy(original_record)
        del missing["binding"]["wrapper"]
        malformed.append(("missing-binding", missing))
        path_type = copy.deepcopy(original_record)
        path_type["binding"]["artifact_dir"] = [str(self.artifact_dir)]
        malformed.append(("path-type", path_type))
        traversal = copy.deepcopy(original_record)
        traversal["binding"]["artifact_dir"] = str(self.root / "source" / ".." / "artifacts")
        malformed.append(("traversal", traversal))
        wrong_hash = copy.deepcopy(original_record)
        wrong_hash["after"][str(self.hash_record)]["sha256"] = "0" * 64
        malformed.append(("identity-hash", wrong_hash))
        for label, record in malformed:
            with self.subTest(record=label):
                record_path.write_text(json.dumps(record))
                refused()
                record_path.write_bytes(predecessor_raw)

        with self.subTest(record="inventory-descriptor"):
            inventory = json.loads(inventory_raw)
            inventory["inputs"][0]["category"] = "other-runtime"
            inventory_path.write_text(json.dumps(inventory))
            changed = copy.deepcopy(original_record)
            changed["inventory_sha256"] = hashlib.sha256(inventory_path.read_bytes()).hexdigest()
            record_path.write_text(json.dumps(changed))
            refused()
            inventory_path.write_bytes(inventory_raw)
            record_path.write_bytes(predecessor_raw)

        with self.subTest(record="missing-original-artifacts"):
            old_artifact = Path(original_record["binding"]["artifact_dir"])
            displaced = old_artifact.with_name("artifacts-a-missing")
            os.replace(old_artifact, displaced)
            try:
                refused()
            finally:
                os.replace(displaced, old_artifact)

        with self.subTest(record="same-manifest-changed-path"):
            copied_artifact = self.root / "copied-artifact-a"
            shutil.copytree(Path(original_record["binding"]["artifact_dir"]), copied_artifact)
            changed = copy.deepcopy(original_record)
            changed["binding"]["artifact_dir"] = str(copied_artifact)
            record_path.write_text(json.dumps(changed))
            refused()
            record_path.write_bytes(predecessor_raw)

        with self.subTest(record="backup-parent-escape"):
            refused(["--backup", str(self.root / ("transaction-" + uuid.uuid4().hex))])

        with self.subTest(record="linked-new-transaction"):
            linked = self.backups / ("transaction-" + uuid.uuid4().hex)
            link_target = self.root / "link-target"
            link_target.mkdir()
            linked.symlink_to(link_target, target_is_directory=True)
            refused(["--backup", str(linked)])

        with self.subTest(record="unsupported-layout"):
            before = self.tree()
            index = self.argv.index("--layout") + 1
            self.argv[index] = "venv"
            try:
                code, output = self.call("upgrade", ["--backup", str(upgrade_backup)])
            finally:
                self.argv[index] = "source-wrapper"
            self.assertEqual(code, 1, output)
            self.assertEqual(json.loads(output)["writes"], "none")
            self.assertEqual(self.tree(), before)

        with self.subTest(record="schema-2-next-edge"):
            schema2 = {
                "schema": 2,
                "operation": "upgrade",
                "phase": "verified",
                "backup": str(upgrade_backup),
                "binding": self.adapter.binding(self.options("upgrade", ["--backup", str(upgrade_backup)])),
                "inventory_sha256": "0" * 64,
                "owned": {},
                "predecessor": {},
                "successor": {},
                "after": {},
            }
            record_path.write_text(json.dumps(schema2))
            refused()

        record_path.write_bytes(predecessor_raw)
        inventory_path.write_bytes(inventory_raw)

    def test_upgrade_same_identity_is_write_free(self):
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        backup = self.backups / ("transaction-" + uuid.uuid4().hex)
        before = self.tree()
        code, output = self.call("upgrade", ["--backup", str(backup)])
        self.assertEqual(code, 0, output)
        result = json.loads(output)
        self.assertEqual(result["status"], "planned")
        self.assertEqual(result["operation"], "unchanged")
        self.assertEqual(self.tree(), before)
        self.assertFalse(backup.exists())

    def test_upgrade_same_identity_still_requires_predecessor_runtime_authority(self):
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        backup = self.backups / ("transaction-" + uuid.uuid4().hex)
        package = self.purelib / "t3_fleet"
        cli = package / "cli.py"
        cli.chmod(cli.stat().st_mode ^ 0o100)
        before = self.tree()

        code, output = self.call("upgrade", ["--backup", str(backup)])

        self.assertEqual(code, 1, output)
        self.assertEqual(json.loads(output)["code"], "runtime-drift")
        self.assertEqual(self.tree(), before)
        self.assertFalse(backup.exists())

        copied_artifact = self.root / "same-identity-artifacts"
        copied_source = self.root / "same-identity-source"
        shutil.copytree(self.artifact_dir, copied_artifact)
        shutil.copytree(self.artifacts.source, copied_source)
        self.argv[self.argv.index("--artifact-dir") + 1] = str(copied_artifact)
        self.argv[self.argv.index("--public-source") + 1] = str(copied_source)
        before = self.tree()
        code, output = self.call("upgrade", ["--backup", str(backup)])
        self.assertEqual(code, 1, output)
        self.assertEqual(self.tree(), before)
        self.assertFalse(backup.exists())

    def test_source_wrapper_install_revalidation_routes_through_platform_verifier(self):
        args = self.options()
        prepared = self.adapter.plan(args)
        transaction = self.backups / ("transaction-" + uuid.uuid4().hex)
        modules = prepared["artifact"]["manifest"]["modules"]
        expected = {
            "kind": "private-tree",
            "files": {
                name.removeprefix("t3_fleet/"): digest
                for name, digest in modules.items()
            },
        }
        checks = []

        def verify(path, actual, selected):
            checks.append((Path(path), actual, selected))
            return (
                selected == expected
                and actual.get("kind") == "tree"
                and not actual["directories"]
                and {
                    name: value["sha256"]
                    for name, value in actual["files"].items()
                }
                == expected["files"]
            )

        authority = []
        with patch.object(
            self.adapter, "private_stage_identity", return_value=expected
        ), patch.object(
            self.adapter, "matches_private_stage", side_effect=verify
        ):
            self.adapter.install(
                args,
                prepared,
                prepared["artifact"]["wheel"],
                transaction,
                self.adapter.binding(args),
                authority=lambda: authority.append(True),
            )

        self.assertEqual(len(checks), 3)
        # Two authority edges around the package swap plus one before each
        # explicit legacy wrapper migration.
        self.assertEqual(authority, [True, True, True, True])
        self.assertEqual(
            {
                "t3_fleet/" + name: hashlib.sha256(path.read_bytes()).hexdigest()
                for name, path in self.adapter.tree_files(
                    self.purelib / "t3_fleet"
                ).items()
            },
            modules,
        )

    def test_snapshot_copy_stability_ignores_atime_only_changes(self):
        source = self.root / "atime-source"
        source.mkdir()
        selected = source / "module.py"
        selected.write_bytes(b"# stable source\n")
        transaction = self.backups / ("transaction-" + uuid.uuid4().hex)
        self.adapter.mkdir_private(transaction)
        entry = {
            "label": "input-0000",
            "destination": str(source),
            "kind": "tree",
            "category": "runtime-package",
            "required": True,
            "runtime": True,
            "exists": True,
        }
        original_read = Path.read_bytes
        original_stat = Path.stat
        selected_reads = []
        read_completed = []

        class AtimeOnlyStat:
            def __init__(self, value):
                for name in (
                    "st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns",
                    "st_ctime_ns", "st_atime_ns",
                ):
                    setattr(self, name, getattr(value, name))
                self.st_atime_ns += 3_600_000_000_000

        def read(path, *args, **kwargs):
            data = original_read(path, *args, **kwargs)
            if path == selected:
                selected_reads.append(True)
                if len(selected_reads) == 2:
                    read_completed.append(True)
            return data

        def stat(path, *args, **kwargs):
            value = original_stat(path, *args, **kwargs)
            if path == selected and read_completed:
                read_completed.clear()
                return AtimeOnlyStat(value)
            return value

        with patch.object(Path, "read_bytes", read), patch.object(Path, "stat", stat):
            inventory = self.adapter.snapshot(transaction, [entry], self.options())

        self.assertEqual(
            (transaction / "input-0000/data/module.py").read_bytes(),
            b"# stable source\n",
        )
        self.assertEqual(inventory["inputs"][0]["snapshot"]["kind"], "tree")

    def test_snapshot_copy_stability_refuses_meaningful_metadata_changes(self):
        def exercise(label, mutate):
            source = self.root / ("changed-source-" + label)
            source.mkdir()
            selected = source / "module.py"
            selected.write_bytes(b"# source\n")
            transaction = self.backups / ("transaction-" + uuid.uuid4().hex)
            self.adapter.mkdir_private(transaction)
            entry = {
                "label": "input-0000",
                "destination": str(source),
                "kind": "tree",
                "category": "runtime-package",
                "required": True,
                "runtime": True,
                "exists": True,
            }
            original_read = Path.read_bytes
            selected_reads = []
            changed = []

            def read(path, *args, **kwargs):
                data = original_read(path, *args, **kwargs)
                if path == selected:
                    selected_reads.append(True)
                    if len(selected_reads) == 2 and not changed:
                        mutate(selected, data)
                        changed.append(True)
                return data

            with patch.object(Path, "read_bytes", read):
                with self.assertRaisesRegex(
                    ValueError, "snapshot source changed while being copied"
                ):
                    self.adapter.snapshot(transaction, [entry], self.options())
            self.assertEqual(changed, [True])

        def change_content(path, data):
            path.write_bytes(data + b"# changed\n")

        def change_identity(path, data):
            replacement = path.with_name("replacement.py")
            replacement.write_bytes(data)
            os.replace(replacement, path)

        def change_mtime(path, unused):
            info = path.stat()
            os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000_000))

        def change_ctime(path, unused):
            before = path.stat()
            mode = before.st_mode & 0o777
            os.chmod(path, mode ^ 0o100)
            os.chmod(path, mode)
            self.assertNotEqual(path.stat().st_ctime_ns, before.st_ctime_ns)

        for label, mutate in (
            ("content", change_content),
            ("identity", change_identity),
            ("mtime", change_mtime),
            ("ctime", change_ctime),
        ):
            with self.subTest(change=label):
                exercise(label, mutate)

    def test_legacy_apply_new_pin_still_refuses(self):
        _, predecessor_raw, _, _, _ = self.install_predecessor_select_successor()
        record_path = self.purelib / self.adapter.RECOVERY
        before = self.tree()
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 1, output)
        self.assertEqual(json.loads(output)["code"], "backup-invalid")
        self.assertEqual(self.tree(), before)
        self.assertEqual(record_path.read_bytes(), predecessor_raw)

    def test_upgrade_creates_new_edge_preserving_original_provenance(self):
        outer = self.root / "outer-wrapper"
        outer.write_bytes(b"outer wrapper retained across both public pins\n")
        outer.chmod(0o755)
        self.argv.extend(["--wrapper", str(outer)])
        predecessor_backup, predecessor_raw, _, successor, upgrade_backup = (
            self.install_predecessor_select_successor()
        )
        original_transaction = self.adapter.fingerprint(predecessor_backup)
        original_manifest = json.loads(self.hash_record.read_text())
        launchers = {path: path.read_bytes() for path in [*self.launchers, outer]}
        config_bytes = self.config.read_bytes()
        policy_bytes = self.policy.read_bytes()
        for index, connection in enumerate(self.connections):
            connection.execute("insert into retained values (?)", (f"newer-row-{index}",))
            connection.commit()

        code, output = self.call(
            "upgrade", ["--backup", str(upgrade_backup), "--apply"]
        )

        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "upgraded")
        record_path = self.purelib / self.adapter.RECOVERY
        record = json.loads(record_path.read_text())
        inventory = json.loads((upgrade_backup / "inventory.json").read_text())
        self.assertEqual(record["schema"], 2)
        self.assertEqual(record["operation"], "upgrade")
        self.assertEqual(record["phase"], "verified")
        self.assertEqual(record["backup"], str(upgrade_backup))
        self.assertEqual(record["predecessor"]["record_sha256"],
                         hashlib.sha256(predecessor_raw).hexdigest())
        self.assertEqual((upgrade_backup / "predecessor-record.json").read_bytes(), predecessor_raw)
        self.assertEqual(record["successor"]["manifest"], successor["pin"] | {
            "wheel": record["successor"]["manifest"]["wheel"],
            "modules": record["successor"]["manifest"]["modules"],
        })
        self.assertEqual(json.loads(self.hash_record.read_text())["revision"], successor["pin"]["revision"])
        package_entry = next(entry for entry in inventory["inputs"]
                             if entry["category"] == "runtime-package")
        snapshot_package = upgrade_backup / package_entry["label"] / "data"
        self.assertEqual(
            {"t3_fleet/" + name: hashlib.sha256(path.read_bytes()).hexdigest()
             for name, path in self.adapter.tree_files(snapshot_package).items()},
            original_manifest["modules"],
        )
        for entry in inventory["inputs"]:
            if entry["kind"] != "sqlite" or not entry["exists"]:
                continue
            snapshot_db = upgrade_backup / entry["label"] / "data"
            with contextlib.closing(sqlite3.connect(snapshot_db)) as connection:
                rows = {row[0] for row in connection.execute("select payload from retained")}
            self.assertTrue(any(row.startswith("newer-row-") for row in rows))
        self.assertEqual(self.adapter.fingerprint(predecessor_backup), original_transaction)
        self.assertEqual({path: path.read_bytes() for path in launchers}, launchers)
        self.assertEqual(self.config.read_bytes(), config_bytes)
        self.assertEqual(self.policy.read_bytes(), policy_bytes)
        self.assertFalse(any(path.name.endswith(".dist-info") for path in self.purelib.iterdir()))
        before_repeat = self.tree()
        code, output = self.call("plan")
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "planned")
        self.assertEqual(self.tree(), before_repeat)
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")
        self.assertEqual(self.tree(), before_repeat)
        functional_wrapper = self.root / "functional-t3-fleet"
        functional_wrapper.write_text(
            self.adapter.source_wrapper_text(Path(sys.executable), self.purelib, "cli")
        )
        functional_wrapper.chmod(0o755)
        completed = subprocess.run(
            [functional_wrapper, "--help"], cwd=self.root,
            env={"PATH": os.environ["PATH"], "PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertTrue(completed.stdout.strip())
        self.assertIn("usage:", completed.stdout)

    def test_upgrade_equal_runtime_pins_remain_operable_and_recoverable(self):
        predecessor_backup, predecessor_raw, _, successor, upgrade_backup = (
            self.install_predecessor_select_successor(same_runtime=True)
        )
        predecessor_manifest = json.loads(self.hash_record.read_text())

        code, output = self.call(
            "upgrade", ["--backup", str(upgrade_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "upgraded")
        record_path = self.purelib / self.adapter.RECOVERY
        record = json.loads(record_path.read_text())
        self.assertNotEqual(
            record["predecessor"]["pin"]["revision"],
            record["successor"]["pin"]["revision"],
        )
        self.assertNotEqual(
            record["predecessor"]["pin"]["tree"],
            record["successor"]["pin"]["tree"],
        )
        self.assertEqual(
            record["predecessor"]["manifest"]["modules"],
            record["successor"]["manifest"]["modules"],
        )
        self.assertEqual(
            json.loads(self.hash_record.read_text())["revision"],
            successor["pin"]["revision"],
        )

        stable = self.tree()
        code, output = self.call("plan")
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "planned")
        self.assertEqual(self.tree(), stable)
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")
        self.assertEqual(self.tree(), stable)

        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "rolled-back")
        self.assertEqual(record_path.read_bytes(), predecessor_raw)
        self.assertEqual(json.loads(self.hash_record.read_text()), predecessor_manifest)

        restored = self.tree()
        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")
        self.assertEqual(record_path.read_bytes(), predecessor_raw)
        self.assertEqual(self.tree(), restored)

        self.argv[self.argv.index("--artifact-dir") + 1] = str(
            successor["predecessor_artifact"]
        )
        self.argv[self.argv.index("--public-source") + 1] = str(
            successor["predecessor_source"]
        )
        self.set_pin(successor["predecessor_pin_file"])
        code, output = self.call(
            "rollback", ["--backup", str(predecessor_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "rolled-back")
        self.assertFalse(self.hash_record.exists())

    def test_upgrade_preserves_empty_admitted_package_directory(self):
        predecessor_backup, predecessor_raw, _, _, upgrade_backup = (
            self.install_predecessor_select_successor()
        )
        package = self.purelib / "t3_fleet"
        cache = package / "__pycache__"
        cache.mkdir()
        predecessor_package = self.adapter.fingerprint(package)
        self.assertEqual(set(predecessor_package["directories"]), {"__pycache__"})
        predecessor_identity = self.hash_record.read_bytes()
        predecessor_transaction = self.adapter.fingerprint(predecessor_backup)
        predecessor_mtimes = {
            name: (package / name.removeprefix("t3_fleet/")).stat().st_mtime_ns
            for name in self.artifacts.modules
        }

        code, output = self.call(
            "upgrade", ["--backup", str(upgrade_backup), "--apply"]
        )

        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "upgraded")
        inventory = json.loads((upgrade_backup / "inventory.json").read_bytes())
        package_entry = next(
            entry
            for entry in inventory["inputs"]
            if entry["category"] == "runtime-package"
        )
        saved_package = upgrade_backup / package_entry["label"] / "data"
        saved_fingerprint = self.adapter.fingerprint(saved_package)
        self.assertEqual(set(saved_fingerprint["directories"]), {"__pycache__"})
        if os.name == "posix":
            self.assertEqual(saved_fingerprint["mode"], 0o700)
            self.assertEqual(
                saved_fingerprint["directories"]["__pycache__"]["mode"], 0o700
            )
            self.assertTrue(
                all(
                    value["mode"] == 0o600
                    for value in saved_fingerprint["files"].values()
                )
            )

        code, output = self.call("plan")
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "planned")
        code, output = self.call(
            "upgrade", ["--backup", str(upgrade_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")

        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "rolled-back")
        self.assertEqual(
            (self.purelib / self.adapter.RECOVERY).read_bytes(), predecessor_raw
        )
        self.assertEqual(self.hash_record.read_bytes(), predecessor_identity)
        self.assertEqual(self.adapter.fingerprint(package), predecessor_package)
        self.assertEqual(
            {
                name: (package / name.removeprefix("t3_fleet/")).stat().st_mtime_ns
                for name in self.artifacts.modules
            },
            predecessor_mtimes,
        )
        self.assertEqual(
            self.adapter.fingerprint(predecessor_backup), predecessor_transaction
        )

    def test_upgrade_intent_caches_preserve_equal_runtime_rollback(self):
        for same_runtime in (True, False):
            with self.subTest(same_runtime=same_runtime), self.fresh_upgrade_case() as case:
                case.use_real_source_interpreter()
                _, predecessor_raw, _, _, upgrade_backup = (
                    case.install_predecessor_select_successor(
                        same_runtime=same_runtime
                    )
                )
                record_path = case.purelib / case.adapter.RECOVERY
                original_publish = case.adapter.atomic_record_bytes
                interrupted = []

                def publish(path, raw, **kwargs):
                    result = original_publish(path, raw, **kwargs)
                    if Path(path) == record_path and not interrupted:
                        record = json.loads(raw)
                        if record.get("phase") == "upgrade-intent":
                            interrupted.append(record["phase"])
                            raise KeyboardInterrupt("after upgrade intent")
                    return result

                with patch.object(
                    case.adapter, "atomic_record_bytes", side_effect=publish
                ):
                    with self.assertRaises(KeyboardInterrupt):
                        case.call(
                            "upgrade", ["--backup", str(upgrade_backup), "--apply"]
                        )
                self.assertEqual(interrupted, ["upgrade-intent"])
                record = json.loads(record_path.read_bytes())
                self.assertEqual(record["phase"], "upgrade-intent")
                self.assertEqual(
                    record["predecessor"]["manifest"]["modules"]
                    == record["successor"]["manifest"]["modules"],
                    same_runtime,
                )

                py_compile.compile(
                    str(case.purelib / "t3_fleet/cli.py"),
                    doraise=True,
                    invalidation_mode=py_compile.PycInvalidationMode.TIMESTAMP,
                )
                self.assertTrue(
                    list((case.purelib / "t3_fleet/__pycache__").glob("*.pyc"))
                )

                code, output = case.call(
                    "rollback", ["--backup", str(upgrade_backup), "--apply"]
                )
                self.assertEqual(code, 0, output)
                self.assertEqual(json.loads(output)["status"], "rolled-back")
                self.assertEqual(record_path.read_bytes(), predecessor_raw)

    def test_upgrade_snapshot_must_match_captured_authenticated_predecessor(self):
        for target in ("saved-copy", "active-before-capture"):
            with self.subTest(target=target), self.fresh_upgrade_case() as case:
                _, predecessor_raw, _, _, upgrade_backup = (
                    case.install_predecessor_select_successor()
                )
                record_path = case.purelib / case.adapter.RECOVERY
                package_before = case.adapter.fingerprint(case.purelib / "t3_fleet")
                identity_before = case.hash_record.read_bytes()
                reached = []

                if target == "saved-copy":
                    original = case.adapter.write_private
                    snapshot_cli = upgrade_backup / "input-0000" / "data" / "cli.py"

                    def write(path, data):
                        if Path(path) == snapshot_cli and not reached:
                            data += b"\n# snapshot-only drift\n"
                            reached.append(target)
                        return original(path, data)

                    context = patch.object(case.adapter, "write_private", side_effect=write)
                else:
                    original = case.adapter.snapshot

                    def snapshot(*args, **kwargs):
                        if not reached:
                            (case.purelib / "t3_fleet/cli.py").write_bytes(
                                b"unexplained current predecessor bytes\n"
                            )
                            reached.append(target)
                        return original(*args, **kwargs)

                    context = patch.object(case.adapter, "snapshot", side_effect=snapshot)

                with context:
                    code, output = case.call(
                        "upgrade", ["--backup", str(upgrade_backup), "--apply"]
                    )

                self.assertEqual(reached, [target])
                self.assertEqual(code, 1, output)
                self.assertEqual(record_path.read_bytes(), predecessor_raw)
                self.assertEqual(case.hash_record.read_bytes(), identity_before)
                if target == "saved-copy":
                    self.assertEqual(
                        case.adapter.fingerprint(case.purelib / "t3_fleet"),
                        package_before,
                    )
                self.assertTrue(upgrade_backup.exists())

    def test_upgrade_source_mtime_schema_refuses_before_recovery_writes(self):
        malformed = (
            ("container", [], None),
            ("missing-key", "missing", None),
            ("extra-key", "extra", None),
            ("negative", -1, None),
            ("false-equals-zero", False, 0),
            ("true-equals-one", True, 1),
            ("float-equals-zero", 0.0, 0),
            ("string", "0", None),
            ("null", None, None),
        )
        for label, value, saved_mtime in malformed:
            with self.subTest(schema=label), self.fresh_upgrade_case() as case:
                _, _, _, _, upgrade_backup = (
                    case.install_predecessor_select_successor()
                )
                code, output = case.call(
                    "upgrade", ["--backup", str(upgrade_backup), "--apply"]
                )
                self.assertEqual(code, 0, output)
                inventory = json.loads((upgrade_backup / "inventory.json").read_bytes())
                source = sorted(inventory["source_mtimes_ns"])[0]
                package_entry = next(
                    entry
                    for entry in inventory["inputs"]
                    if entry["category"] == "runtime-package"
                )
                saved_source = upgrade_backup / package_entry["label"] / "data" / source
                if saved_mtime is not None:
                    os.utime(saved_source, ns=(saved_mtime, saved_mtime))

                def mutate(document):
                    values = document["source_mtimes_ns"]
                    if label == "container":
                        document["source_mtimes_ns"] = value
                    elif label == "missing-key":
                        values.pop(source)
                    elif label == "extra-key":
                        values["foreign.py"] = 0
                    else:
                        values[source] = value

                _, record_raw = case.rewrite_upgrade_inventory(
                    upgrade_backup, mutate
                )
                stable = case.tree()
                for action, extra in (
                    ("plan", []),
                    ("rollback", ["--backup", str(upgrade_backup), "--apply"]),
                ):
                    with self.subTest(schema=label, action=action):
                        code, output = case.call(action, extra)
                        self.assertEqual(code, 1, output)
                        result = json.loads(output)
                        self.assertEqual(result["phase"], "preflight")
                        self.assertEqual(result["writes"], "none")
                        self.assertEqual(case.tree(), stable)
                        self.assertEqual(
                            (case.purelib / case.adapter.RECOVERY).read_bytes(),
                            record_raw,
                        )

        for value in (0, 1):
            with self.subTest(valid_integer=value), self.fresh_upgrade_case() as case:
                _, _, _, _, upgrade_backup = (
                    case.install_predecessor_select_successor()
                )
                code, output = case.call(
                    "upgrade", ["--backup", str(upgrade_backup), "--apply"]
                )
                self.assertEqual(code, 0, output)
                inventory = json.loads((upgrade_backup / "inventory.json").read_bytes())
                source = sorted(inventory["source_mtimes_ns"])[0]
                package_entry = next(
                    entry
                    for entry in inventory["inputs"]
                    if entry["category"] == "runtime-package"
                )
                saved_source = upgrade_backup / package_entry["label"] / "data" / source
                os.utime(saved_source, ns=(value, value))
                case.rewrite_upgrade_inventory(
                    upgrade_backup,
                    lambda document: document["source_mtimes_ns"].__setitem__(
                        source, value
                    ),
                )
                stable = case.tree()
                code, output = case.call("plan")
                self.assertEqual(code, 0, output)
                self.assertEqual(json.loads(output)["status"], "planned")
                self.assertEqual(case.tree(), stable)

    def test_upgrade_publication_edges_recheck_after_private_temp_sync(self):
        upgrade_edges = ("upgrade-intent", "mutation-started", "identity", "verified")
        rollback_edges = (
            "rollback-started", "ready-receipt", "predecessor-ready", "predecessor-record",
        )
        for edge in (*upgrade_edges, *rollback_edges):
            with self.subTest(edge=edge), self.fresh_upgrade_case() as case:
                original, changed = case.selected_identity_change(
                    selected_state="alternate", selected_origin="http://127.0.0.1:3775"
                )
                _, predecessor_raw, _, successor, upgrade_backup = (
                    case.install_predecessor_select_successor()
                )
                record_path = case.purelib / case.adapter.RECOVERY
                predecessor_manifest = json.loads(case.hash_record.read_text())
                if edge in rollback_edges:
                    code, output = case.call(
                        "upgrade", ["--backup", str(upgrade_backup), "--apply"]
                    )
                    self.assertEqual(code, 0, output)

                original_write = case.adapter.write_private
                reached = []

                def write(path, data):
                    result = original_write(path, data)
                    path = Path(path)
                    try:
                        document = json.loads(data)
                    except (TypeError, ValueError, UnicodeError):
                        document = {}
                    record_temp = path.parent == case.purelib and (
                        path.name.startswith(".runtime-record-")
                        or path == record_path
                    )
                    identity_temp = path.parent == case.hash_record.parent and (
                        path.name.startswith(case.hash_record.name + ".new-")
                        or path.name.startswith(".runtime-record-")
                    )
                    receipt_temp = path.parent == upgrade_backup and (
                        path == upgrade_backup / "rollback-ready.json"
                        or path.name.startswith(".runtime-record-")
                    )
                    selected = (
                        edge in {"upgrade-intent", "mutation-started", "verified"}
                        and record_temp and document.get("phase") == edge
                    ) or (
                        edge == "identity" and identity_temp
                        and document.get("revision") == successor["pin"]["revision"]
                    ) or (
                        edge == "rollback-started" and record_temp
                        and document.get("phase") == "rollback-started"
                    ) or (
                        edge == "ready-receipt" and receipt_temp
                        and "predecessor_record_sha256" in document
                    ) or (
                        edge == "predecessor-ready" and record_temp
                        and document.get("phase") == "predecessor-ready"
                    ) or (
                        edge == "predecessor-record" and record_temp
                        and document.get("schema") == 1
                        and document.get("phase") == "verified"
                    )
                    if selected and not reached:
                        case.config.write_text(changed)
                        reached.append(edge)
                    return result

                with patch.object(case.adapter, "write_private", side_effect=write):
                    if edge in upgrade_edges:
                        code, output = case.call(
                            "upgrade", ["--backup", str(upgrade_backup), "--apply"]
                        )
                    else:
                        code, output = case.call(
                            "rollback", ["--backup", str(upgrade_backup), "--apply"]
                        )

                self.assertEqual(reached, [edge], output)
                self.assertEqual(code, 1, output)
                active = case.adapter.read_record(case.purelib)
                if edge == "upgrade-intent":
                    self.assertEqual(record_path.read_bytes(), predecessor_raw)
                elif edge == "mutation-started":
                    self.assertEqual(active["phase"], "upgrade-intent")
                elif edge == "identity":
                    self.assertEqual(json.loads(case.hash_record.read_text()), predecessor_manifest)
                elif edge == "verified":
                    self.assertEqual(active["phase"], "mutation-started")
                elif edge == "rollback-started":
                    self.assertEqual(active["phase"], "verified")
                elif edge == "ready-receipt":
                    self.assertFalse((upgrade_backup / "rollback-ready.json").exists())
                    self.assertEqual(active["phase"], "rollback-started")
                elif edge == "predecessor-ready":
                    self.assertEqual(active["phase"], "rollback-started")
                else:
                    self.assertEqual(active["phase"], "predecessor-ready")
                    self.assertNotEqual(record_path.read_bytes(), predecessor_raw)
                case.config.write_text(original)

    def test_upgrade_and_restore_stage_consumption_revalidates_exact_content(self):
        for edge in ("successor-stage", "inter-rename-authority", "restore-stage"):
            with self.subTest(edge=edge), self.fresh_upgrade_case() as case:
                original, changed = case.selected_identity_change(
                    selected_state="alternate", selected_origin="http://127.0.0.1:3775"
                )
                _, _, _, _, upgrade_backup = case.install_predecessor_select_successor()
                token = upgrade_backup.name.removeprefix("transaction-")
                package = case.purelib / "t3_fleet"
                source_stage = case.purelib / (".t3-fleet-stage-" + token)
                reached = []

                if edge == "inter-rename-authority":
                    original_sync = case.adapter.fsync_directory

                    def sync(path):
                        result = original_sync(path)
                        old = case.purelib / (".t3-fleet-old-" + token)
                        if (Path(path) == package.parent and old.exists()
                                and not package.exists() and source_stage.exists()
                                and not reached):
                            case.config.write_text(changed)
                            reached.append(edge)
                        return result

                    context = patch.object(case.adapter, "fsync_directory", side_effect=sync)
                else:
                    if edge == "restore-stage":
                        code, output = case.call(
                            "upgrade", ["--backup", str(upgrade_backup), "--apply"]
                        )
                        self.assertEqual(code, 0, output)
                    original_authority = case.adapter.require_upgrade_authority

                    def authority(args, expected_record_sha256, expected_binding):
                        result = original_authority(
                            args, expected_record_sha256, expected_binding
                        )
                        active = case.adapter.read_record(case.purelib)
                        if edge == "successor-stage":
                            selected = (
                                active.get("phase") == "mutation-started"
                                and source_stage.exists() and package.exists()
                            )
                            stage = source_stage
                        else:
                            stages = list(case.purelib.glob(".t3-fleet-restore-*"))
                            selected = (
                                active.get("phase") == "rollback-started"
                                and stages and not package.exists()
                            )
                            stage = stages[0] if stages else None
                        if selected and not reached:
                            (stage / "cli.py").write_bytes(b"unknown staged runtime bytes\n")
                            reached.append(edge)
                        return result

                    context = patch.object(
                        case.adapter, "require_upgrade_authority", side_effect=authority
                    )

                with context:
                    action = "rollback" if edge == "restore-stage" else "upgrade"
                    code, output = case.call(
                        action, ["--backup", str(upgrade_backup), "--apply"]
                    )
                self.assertEqual(reached, [edge], output)
                self.assertEqual(code, 1, output)
                if package.exists():
                    self.assertNotEqual(
                        (package / "cli.py").read_bytes(), b"unknown staged runtime bytes\n"
                    )
                if edge == "inter-rename-authority":
                    self.assertFalse(package.exists())
                    self.assertTrue(source_stage.exists())
                    case.config.write_text(original)

    def test_upgrade_cleanup_reclassifies_current_target_at_delete_edge(self):
        _, _, _, _, upgrade_backup = self.install_predecessor_select_successor()
        original_remove = self.adapter.shutil.rmtree
        with patch.object(
            self.adapter.shutil, "rmtree", side_effect=OSError("retain verified owned path")
        ):
            code, output = self.call(
                "upgrade", ["--backup", str(upgrade_backup), "--apply"]
            )
        self.assertEqual(code, 1, output)
        active = self.adapter.read_record(self.purelib)
        old = next(Path(path) for path in active["owned"]
                   if Path(path).name.startswith(".t3-fleet-old-"))
        original_authority = self.adapter.require_upgrade_authority
        inserted = []

        def authority(args, expected_record_sha256, expected_binding):
            result = original_authority(args, expected_record_sha256, expected_binding)
            if not inserted:
                (old / "foreign-content").write_bytes(b"must remain for review\n")
                inserted.append(True)
            return result

        with patch.object(
            self.adapter, "require_upgrade_authority", side_effect=authority
        ), patch.object(self.adapter.shutil, "rmtree", wraps=original_remove):
            code, output = self.call(
                "upgrade", ["--backup", str(upgrade_backup), "--apply"]
            )
        self.assertEqual(inserted, [True])
        self.assertEqual(code, 1, output)
        self.assertEqual((old / "foreign-content").read_bytes(), b"must remain for review\n")

    def test_upgrade_preintent_failure_retains_old_authority_and_requires_fresh_path(self):
        _, predecessor_raw, _, _, _ = self.install_predecessor_select_successor()
        record_path = self.purelib / self.adapter.RECOVERY
        package_before = self.adapter.fingerprint(self.purelib / "t3_fleet")
        identity_before = self.hash_record.read_bytes()
        failures = []

        def attempt(label, injector):
            selected = self.backups / ("transaction-" + uuid.uuid4().hex)
            before = self.tree()
            with injector(selected):
                code, output = self.call(
                    "upgrade", ["--backup", str(selected), "--apply"]
                )
            self.assertEqual(code, 1, (label, output))
            self.assertEqual(record_path.read_bytes(), predecessor_raw)
            self.assertEqual(self.adapter.fingerprint(self.purelib / "t3_fleet"), package_before)
            self.assertEqual(self.hash_record.read_bytes(), identity_before)
            self.assertTrue(selected.exists(), label)
            retained = self.adapter.fingerprint(selected)
            failures.append((selected, retained))
            reuse_before = self.tree()
            reuse_code, reuse_output = self.call(
                "upgrade", ["--backup", str(selected), "--apply"]
            )
            self.assertEqual(reuse_code, 1, reuse_output)
            self.assertEqual(self.tree(), reuse_before)
            self.assertEqual(self.adapter.fingerprint(selected), retained)
            self.assertNotEqual(self.tree(), before)

        @contextlib.contextmanager
        def fail_after_mkdir(selected):
            real = self.adapter.mkdir_private
            def injected(path):
                result = real(path)
                if Path(path) == selected:
                    raise ValueError("injected transaction mkdir failure")
                return result
            with patch.object(self.adapter, "mkdir_private", side_effect=injected):
                yield

        @contextlib.contextmanager
        def fail_during_predecessor_copy(selected):
            real = self.adapter.write_private
            target = selected / "predecessor-record.json"
            def injected(path, data):
                if Path(path) == target:
                    real(path, data[: max(1, len(data) // 2)])
                    raise ValueError("injected predecessor copy failure")
                return real(path, data)
            with patch.object(self.adapter, "write_private", side_effect=injected):
                yield

        @contextlib.contextmanager
        def fail_during_snapshot(selected):
            real = self.adapter.snapshot
            def injected(*args, **kwargs):
                folder = selected / "input-0000"
                self.adapter.mkdir_private(folder)
                raise ValueError("injected snapshot failure")
            with patch.object(self.adapter, "snapshot", side_effect=injected):
                yield

        @contextlib.contextmanager
        def fail_after_artifact_stage(selected):
            real = self.adapter.stage_artifact
            def injected(*args, **kwargs):
                real(*args, **kwargs)
                raise ValueError("injected artifact stage failure")
            with patch.object(self.adapter, "stage_artifact", side_effect=injected):
                yield

        @contextlib.contextmanager
        def fail_backup_parent_sync(selected):
            real = self.adapter.fsync_directory
            def injected(path):
                if Path(path) == self.backups:
                    raise OSError("injected backup parent sync failure")
                return real(path)
            with patch.object(self.adapter, "fsync_directory", side_effect=injected):
                yield

        for label, injector in (
            ("empty-orphan", fail_after_mkdir),
            ("partial-predecessor", fail_during_predecessor_copy),
            ("partial-snapshot", fail_during_snapshot),
            ("complete-stage-before-intent", fail_after_artifact_stage),
            ("backup-parent-sync", fail_backup_parent_sync),
        ):
            with self.subTest(edge=label):
                attempt(label, injector)

        fresh = self.backups / ("transaction-" + uuid.uuid4().hex)
        code, output = self.call("upgrade", ["--backup", str(fresh), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "upgraded")
        for path, retained in failures:
            self.assertEqual(self.adapter.fingerprint(path), retained)

    def test_upgrade_mutation_edge_identity_checks(self):
        edges = ("after-lock", "after-snapshot-proof", "after-source-stage",
                 "before-identity", "before-verified")
        for edge in edges:
            with self.subTest(edge=edge), self.fresh_upgrade_case() as case:
                original, changed = case.selected_identity_change(
                    selected_state="alternate",
                    selected_origin="http://127.0.0.1:3775",
                )
                _, predecessor_raw, _, _, upgrade_backup = (
                    case.install_predecessor_select_successor()
                )
                record_path = case.purelib / case.adapter.RECOVERY
                injected = []
                stack = contextlib.ExitStack()
                case.addCleanup(stack.close)

                if edge == "after-lock":
                    real = case.adapter.plan_upgrade
                    calls = []
                    def hook(args):
                        result = real(args)
                        calls.append(True)
                        if len(calls) == 2:
                            case.config.write_text(changed)
                            injected.append(case.tree())
                        return result
                    stack.enter_context(patch.object(case.adapter, "plan_upgrade", side_effect=hook))
                elif edge == "after-snapshot-proof":
                    real = case.adapter.validate_upgrade_backup
                    def hook(*args, **kwargs):
                        result = real(*args, **kwargs)
                        if not injected:
                            case.config.write_text(changed)
                            injected.append(case.tree())
                        return result
                    stack.enter_context(patch.object(case.adapter, "validate_upgrade_backup", side_effect=hook))
                elif edge == "after-source-stage":
                    real = case.adapter.require_upgrade_authority
                    def hook(args, expected_record_sha256, expected_binding):
                        record = case.adapter.read_record(case.purelib)
                        if record and record.get("schema") == 2 and record["phase"] == "mutation-started":
                            token = upgrade_backup.name.removeprefix("transaction-")
                            stage = case.purelib / (".t3-fleet-stage-" + token)
                            if stage.exists() and not injected:
                                case.config.write_text(changed)
                                injected.append(case.tree())
                        return real(args, expected_record_sha256, expected_binding)
                    stack.enter_context(patch.object(case.adapter, "require_upgrade_authority", side_effect=hook))
                elif edge == "before-identity":
                    real = case.adapter.verify_installed
                    def hook(*args, **kwargs):
                        result = real(*args, **kwargs)
                        if not injected:
                            case.config.write_text(changed)
                            injected.append(case.tree())
                        return result
                    stack.enter_context(patch.object(case.adapter, "verify_installed", side_effect=hook))
                else:
                    real = case.adapter.runtime_fingerprints
                    def hook(inputs):
                        result = real(inputs)
                        record = case.adapter.read_record(case.purelib)
                        if (record and record.get("schema") == 2
                                and record["phase"] == "mutation-started"
                                and case.hash_record.exists()
                                and json.loads(case.hash_record.read_text())["revision"]
                                    == case.successor["pin"]["revision"]
                                and not injected):
                            case.config.write_text(changed)
                            injected.append(case.tree())
                        return result
                    stack.enter_context(patch.object(case.adapter, "runtime_fingerprints", side_effect=hook))

                cleanup = stack.enter_context(patch.object(
                    case.adapter,
                    "cleanup_upgrade_owned",
                    wraps=case.adapter.cleanup_upgrade_owned,
                ))
                code, output = case.call(
                    "upgrade", ["--backup", str(upgrade_backup), "--apply"]
                )
                self.assertEqual(len(injected), 1, output)
                self.assertEqual(code, 1, output)
                self.assertEqual(case.tree(), injected[0])
                cleanup.assert_not_called()
                record = case.adapter.read_record(case.purelib)
                if edge in {"after-lock", "after-snapshot-proof"}:
                    self.assertEqual(record_path.read_bytes(), predecessor_raw)
                    self.assertEqual(record["schema"], 1)
                    self.assertFalse(case.hash_record.read_text() == json.dumps(case.successor["pin"]))
                else:
                    self.assertEqual(record["schema"], 2)
                    self.assertEqual(record["phase"], "mutation-started")
                case.config.write_text(original)

    def test_upgrade_crash_authority_matrix(self):
        edges = (
            "intent-before-replace", "intent-after-replace", "mutation-after-replace",
            "old-package-after-rename", "new-package-after-rename",
            "identity-after-replace", "verified-after-replace", "cleanup-after-remove",
        )
        for edge in edges:
            with self.subTest(edge=edge), self.fresh_upgrade_case() as case:
                _, predecessor_raw, _, successor, upgrade_backup = (
                    case.install_predecessor_select_successor()
                )
                record_path = case.purelib / case.adapter.RECOVERY
                package = case.purelib / "t3_fleet"
                predecessor_manifest = json.loads(case.hash_record.read_text())
                injected = []

                def phase(raw):
                    try:
                        return json.loads(raw)["phase"]
                    except (KeyError, TypeError, ValueError):
                        return None

                with contextlib.ExitStack() as stack:
                    if edge.startswith("intent") or edge in {
                            "mutation-after-replace", "verified-after-replace"}:
                        real = case.adapter.atomic_record_bytes
                        def publish(path, raw, **kwargs):
                            selected_phase = phase(raw)
                            target = {
                                "intent-before-replace": "upgrade-intent",
                                "intent-after-replace": "upgrade-intent",
                                "mutation-after-replace": "mutation-started",
                                "verified-after-replace": "verified",
                            }[edge]
                            if selected_phase == target and not injected:
                                if edge == "intent-before-replace":
                                    injected.append("before")
                                    raise KeyboardInterrupt("injected before record replacement")
                                real(path, raw, **kwargs)
                                injected.append("after")
                                raise KeyboardInterrupt("injected after record replacement")
                            return real(path, raw, **kwargs)
                        stack.enter_context(patch.object(
                            case.adapter, "atomic_record_bytes", side_effect=publish))
                    elif edge in {"old-package-after-rename", "new-package-after-rename"}:
                        real = case.adapter.os.replace
                        def replace(source, destination):
                            source, destination = Path(source), Path(destination)
                            result = real(source, destination)
                            old_edge = (source == package and destination.name.startswith(".t3-fleet-old-"))
                            new_edge = (source.name.startswith(".t3-fleet-stage-")
                                        and destination == package)
                            if ((edge == "old-package-after-rename" and old_edge)
                                    or (edge == "new-package-after-rename" and new_edge)):
                                injected.append("after")
                                raise KeyboardInterrupt("injected after package rename")
                            return result
                        stack.enter_context(patch.object(case.adapter.os, "replace", side_effect=replace))
                    elif edge == "identity-after-replace":
                        real = case.adapter.atomic_record_bytes
                        def publish_identity(path, raw, **kwargs):
                            result = real(path, raw, **kwargs)
                            if Path(path) == case.hash_record and not injected:
                                injected.append("after")
                                raise KeyboardInterrupt("injected after identity replacement")
                            return result
                        stack.enter_context(patch.object(
                            case.adapter, "atomic_record_bytes", side_effect=publish_identity))
                    else:
                        real = case.adapter.shutil.rmtree
                        def remove(path):
                            result = real(path)
                            if Path(path).name.startswith(".t3-fleet-old-") and not injected:
                                injected.append("after")
                                raise OSError("injected after owned cleanup")
                            return result
                        stack.enter_context(patch.object(
                            case.adapter.shutil, "rmtree", side_effect=remove))

                    if edge == "cleanup-after-remove":
                        code, output = case.call(
                            "upgrade", ["--backup", str(upgrade_backup), "--apply"])
                        self.assertEqual(code, 1, output)
                    else:
                        with self.assertRaises(KeyboardInterrupt):
                            case.call("upgrade", ["--backup", str(upgrade_backup), "--apply"])
                self.assertEqual(injected, ["before" if edge == "intent-before-replace" else "after"])

                record = case.adapter.read_record(case.purelib)
                identity = json.loads(case.hash_record.read_text())
                if edge == "intent-before-replace":
                    self.assertEqual(record_path.read_bytes(), predecessor_raw)
                    self.assertEqual(record["schema"], 1)
                    self.assertTrue(package.exists())
                    self.assertEqual(identity, predecessor_manifest)
                    continue

                expected_phase = "verified" if edge in {
                    "verified-after-replace", "cleanup-after-remove"} else (
                    "upgrade-intent" if edge == "intent-after-replace" else "mutation-started")
                self.assertEqual((record["schema"], record["phase"]), (2, expected_phase))
                if edge == "old-package-after-rename":
                    self.assertFalse(package.exists())
                else:
                    self.assertTrue(package.exists())
                expected_identity = (successor["pin"]["revision"] if edge in {
                    "identity-after-replace", "verified-after-replace", "cleanup-after-remove"
                } else predecessor_manifest["revision"])
                self.assertEqual(identity["revision"], expected_identity)

                if expected_phase != "verified":
                    stable = case.tree()
                    for action, extra in (
                        ("plan", []), ("apply", ["--apply"]),
                        ("upgrade", ["--backup", str(upgrade_backup), "--apply"]),
                    ):
                        code, output = case.call(action, extra)
                        self.assertEqual(code, 1, (action, output))
                        self.assertEqual(case.tree(), stable)
                else:
                    code, output = case.call(
                        "upgrade", ["--backup", str(upgrade_backup), "--apply"])
                    self.assertEqual(code, 0, output)
                    self.assertEqual(
                        json.loads(output)["status"],
                        "unchanged" if edge == "cleanup-after-remove" else "finalized",
                    )
                    code, output = case.call(
                        "upgrade", ["--backup", str(upgrade_backup), "--apply"])
                    self.assertEqual(code, 0, output)
                    self.assertEqual(json.loads(output)["status"], "unchanged")

    def test_upgrade_rollback_previous_then_private(self):
        outer = self.root / "outer-wrapper"
        outer.write_bytes(b"outer wrapper retained through both rollback levels\n")
        outer.chmod(0o755)
        self.argv.extend(["--wrapper", str(outer)])
        initial_package = self.adapter.fingerprint(self.purelib / "t3_fleet")
        launcher_bytes = {path: path.read_bytes() for path in [*self.launchers, outer]}
        predecessor_backup, predecessor_raw, _, successor, upgrade_backup = (
            self.install_predecessor_select_successor()
        )
        migrated_launchers = {path: path.read_bytes() for path in [*self.launchers, outer]}
        predecessor_transaction = self.adapter.fingerprint(predecessor_backup)
        code, output = self.call(
            "upgrade", ["--backup", str(upgrade_backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "upgraded")

        self.policy.write_bytes(b'{"private": "maintained-after-upgrade"}\n')
        for index, connection in enumerate(self.connections):
            connection.execute("insert into retained values (?)", (f"post-upgrade-{index}",))
            connection.commit()
        maintained_policy = self.policy.read_bytes()

        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "rolled-back")
        self.assertEqual((self.purelib / self.adapter.RECOVERY).read_bytes(), predecessor_raw)
        self.assertEqual(json.loads(self.hash_record.read_text())["revision"],
                         successor["predecessor_pin"]["revision"])
        self.assertEqual(self.adapter.fingerprint(predecessor_backup), predecessor_transaction)
        self.assertEqual(self.policy.read_bytes(), maintained_policy)
        self.assertEqual({path: path.read_bytes() for path in migrated_launchers}, migrated_launchers)
        for index, connection in enumerate(self.connections):
            rows = {row[0] for row in connection.execute("select payload from retained")}
            self.assertIn(f"post-upgrade-{index}", rows)

        before_repeat = self.tree()
        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")
        self.assertEqual(self.tree(), before_repeat)

        self.argv[self.argv.index("--artifact-dir") + 1] = str(
            successor["predecessor_artifact"])
        self.argv[self.argv.index("--public-source") + 1] = str(
            successor["predecessor_source"])
        self.set_pin(successor["predecessor_pin_file"])
        code, output = self.call(
            "rollback", ["--backup", str(predecessor_backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual({path: path.read_bytes() for path in launcher_bytes}, launcher_bytes)
        self.assertEqual(json.loads(output)["status"], "rolled-back")
        self.assertEqual(self.adapter.fingerprint(self.purelib / "t3_fleet"), initial_package)
        self.assertFalse(self.hash_record.exists())
        self.assertEqual(self.policy.read_bytes(), maintained_policy)

    def seed_verified_legacy_record(self):
        launchers = {path: path.read_bytes() for path in self.launchers}
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        record_path = self.purelib / self.adapter.RECOVERY
        record = json.loads(record_path.read_bytes())
        inventory_path = self.transaction() / "inventory.json"
        inventory = json.loads(inventory_path.read_bytes())
        for path, data in launchers.items():
            path.write_bytes(data)
            record["after"].pop(str(path))
        for entry in inventory["inputs"]:
            if entry["category"] == "entrypoint":
                entry["runtime"] = False
        inventory_path.write_text(json.dumps(inventory, sort_keys=True, indent=2) + "\n")
        record["inventory_sha256"] = hashlib.sha256(inventory_path.read_bytes()).hexdigest()
        record_path.write_bytes(self.adapter.record_bytes(record))
        self.assertEqual(len(record["after"]), 2)
        return launchers, record_path.read_bytes()

    def test_legacy_launcher_snapshot_supports_plan_and_rollback_after_migration(self):
        launchers, predecessor_record = self.seed_verified_legacy_record()
        self.select_successor()
        backup = self.backups / ("transaction-" + uuid.uuid4().hex)
        code, output = self.call("upgrade", ["--backup", str(backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertNotEqual({path: path.read_bytes() for path in launchers}, launchers)
        code, output = self.call()
        self.assertEqual(code, 0, output)
        code, output = self.call("rollback", ["--backup", str(backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual({path: path.read_bytes() for path in launchers}, launchers)
        self.assertEqual((self.purelib / self.adapter.RECOVERY).read_bytes(), predecessor_record)

    def test_interrupted_legacy_launcher_migration_restores_both_wrappers(self):
        launchers, predecessor_record = self.seed_verified_legacy_record()
        self.select_successor()
        backup = self.backups / ("transaction-" + uuid.uuid4().hex)
        migrate = self.adapter.migrate_source_wrapper
        migrated = []

        def interrupt_second(path, *args):
            if migrated:
                raise KeyboardInterrupt
            result = migrate(path, *args)
            migrated.append(path)
            return result

        with patch.object(self.adapter, "migrate_source_wrapper", side_effect=interrupt_second):
            with self.assertRaises(KeyboardInterrupt):
                self.call("upgrade", ["--backup", str(backup), "--apply"])
        self.assertEqual(len(migrated), 1)
        code, output = self.call("rollback", ["--backup", str(backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual({path: path.read_bytes() for path in launchers}, launchers)
        self.assertEqual((self.purelib / self.adapter.RECOVERY).read_bytes(), predecessor_record)

    def test_schema2_predecessor_upgrades_to_new_pin_and_rolls_back_two_edges(self):
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        a_package = self.adapter.fingerprint(self.purelib / "t3_fleet")
        b = self.select_successor()
        b_backup = self.backups / ("transaction-" + uuid.uuid4().hex)
        code, output = self.call("upgrade", ["--backup", str(b_backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "upgraded")
        b_raw = (self.purelib / self.adapter.RECOVERY).read_bytes()
        b_record = json.loads(b_raw)
        self.assertEqual(b_record["schema"], 2)
        b_package = self.adapter.fingerprint(self.purelib / "t3_fleet")
        self.assertEqual(json.loads(self.hash_record.read_text()), b_record["successor"]["manifest"])

        c = self.prepare_from(b, "c", b"\n# third reviewed pin\n")
        self.argv[self.argv.index("--artifact-dir") + 1] = str(c["artifact"])
        self.argv[self.argv.index("--public-source") + 1] = str(c["source"])
        self.set_pin(c["pin_file"])
        c_backup = self.backups / ("transaction-" + uuid.uuid4().hex)
        code, output = self.call("upgrade", ["--backup", str(c_backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "upgraded")
        c_record = json.loads((self.purelib / self.adapter.RECOVERY).read_bytes())
        self.assertEqual(c_record["schema"], 2)
        # The new edge records the exact verified schema-2 predecessor and its
        # bounded rollback edge back to the schema-1 authority.
        self.assertEqual(c_record["predecessor"]["record_sha256"],
                         hashlib.sha256(b_raw).hexdigest())
        self.assertEqual(c_record["predecessor"]["backup"], str(b_backup))
        self.assertEqual(c_record["successor"]["manifest"]["revision"], c["pin"]["revision"])

        code, output = self.call("rollback", ["--backup", str(c_backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "rolled-back")
        self.assertEqual((self.purelib / self.adapter.RECOVERY).read_bytes(), b_raw)
        self.assertEqual(self.adapter.fingerprint(self.purelib / "t3_fleet"), b_package)
        self.assertEqual(json.loads(self.hash_record.read_text()), b_record["successor"]["manifest"])

        # The predecessor schema-2 transaction remains intact for its own
        # second rollback edge back to the schema-1 pin.
        self.argv[self.argv.index("--artifact-dir") + 1] = str(b["artifact"])
        self.argv[self.argv.index("--public-source") + 1] = str(b["source"])
        self.set_pin(b["pin_file"])
        code, output = self.call(
            "rollback", ["--backup", b_record["backup"], "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "rolled-back")
        self.assertEqual(self.adapter.fingerprint(self.purelib / "t3_fleet"), a_package)
        self.assertEqual(json.loads(self.hash_record.read_text())["revision"],
                         b["predecessor_pin"]["revision"])

    def test_schema2_successive_pins_roll_back_to_each_verified_predecessor(self):
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        previous = self.select_successor()
        pins = []
        for index in range(3):
            if index:
                previous = self.prepare_from(
                    previous, f"pin-{index}", f"\n# reviewed pin {index}\n".encode(),
                )
                self.argv[self.argv.index("--artifact-dir") + 1] = str(previous["artifact"])
                self.argv[self.argv.index("--public-source") + 1] = str(previous["source"])
                self.set_pin(previous["pin_file"])
            previous["backup"] = self.backups / ("transaction-" + uuid.uuid4().hex)
            code, output = self.call("upgrade", ["--backup", str(previous["backup"]), "--apply"])
            self.assertEqual(code, 0, output)
            self.assertEqual(json.loads(output)["status"], "upgraded")
            previous["record_bytes"] = (self.purelib / self.adapter.RECOVERY).read_bytes()
            previous["runtime"] = self.adapter.fingerprint(self.purelib / "t3_fleet")
            pins.append(previous)
        for successor, predecessor in zip(reversed(pins[1:]), reversed(pins[:-1])):
            code, output = self.call("rollback", ["--backup", str(successor["backup"]), "--apply"])
            self.assertEqual(code, 0, output)
            self.assertEqual(json.loads(output)["status"], "rolled-back")
            code, output = self.call("rollback", ["--backup", str(successor["backup"]), "--apply"])
            self.assertEqual(code, 0, output)
            self.assertEqual(json.loads(output)["status"], "unchanged")
            self.assertEqual((self.purelib / self.adapter.RECOVERY).read_bytes(),
                             predecessor["record_bytes"])
            self.assertEqual(self.adapter.fingerprint(self.purelib / "t3_fleet"),
                             predecessor["runtime"])
            self.argv[self.argv.index("--artifact-dir") + 1] = str(predecessor["artifact"])
            self.argv[self.argv.index("--public-source") + 1] = str(predecessor["source"])
            self.set_pin(predecessor["pin_file"])

    def test_three_script_pin_refuses_unsupported_layout_before_opening_artifacts(self):
        self.adapter.load_builder()
        pin = json.loads(Path(self.argv[self.argv.index("--pin") + 1]).read_text())
        pin["package"]["console_scripts"]["t3-lane"] = "t3_fleet.lane:main"
        for layout in ("venv", "windows-user-site"):
            with self.subTest(layout=layout):
                args = self.options()
                args.layout = layout
                with patch.object(self.adapter, "git_objects") as git_objects:
                    with self.assertRaisesRegex(ValueError, "managed launcher set"):
                        self.adapter.validate_artifact(args, pin=pin)
                    git_objects.assert_not_called()

    def test_schema2_rollback_retries_after_interruption(self):
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        b = self.select_successor()
        b_backup = self.backups / ("transaction-" + uuid.uuid4().hex)
        self.assertEqual(self.call("upgrade", ["--backup", str(b_backup), "--apply"])[0], 0)
        b_raw = (self.purelib / self.adapter.RECOVERY).read_bytes()
        b_package = self.adapter.fingerprint(self.purelib / "t3_fleet")
        c = self.prepare_from(b, "c-retry", b"\n# retry pin\n")
        self.argv[self.argv.index("--artifact-dir") + 1] = str(c["artifact"])
        self.argv[self.argv.index("--public-source") + 1] = str(c["source"])
        self.set_pin(c["pin_file"])
        c_backup = self.backups / ("transaction-" + uuid.uuid4().hex)
        self.assertEqual(self.call("upgrade", ["--backup", str(c_backup), "--apply"])[0], 0)
        original = self.adapter.restore_entry
        calls = []
        def interrupted(*args, **kwargs):
            calls.append(True)
            if len(calls) == 1:
                raise KeyboardInterrupt()
            return original(*args, **kwargs)
        with patch.object(self.adapter, "restore_entry", side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):
                self.call("rollback", ["--backup", str(c_backup), "--apply"])
        record = json.loads((self.purelib / self.adapter.RECOVERY).read_bytes())
        self.assertEqual(record["phase"], "rollback-started")
        code, output = self.call("rollback", ["--backup", str(c_backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "rolled-back")
        self.assertEqual((self.purelib / self.adapter.RECOVERY).read_bytes(), b_raw)
        self.assertEqual(self.adapter.fingerprint(self.purelib / "t3_fleet"), b_package)

    @unittest.skipUnless(os.name == "posix", "real wrapper startup is POSIX-only")
    def test_source_wrapper_startup_disables_site_and_imports_only_approved_root(self):
        python = Path(sys._base_executable).resolve()
        source = self.root / "real-source"
        package = source / "t3_fleet"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("")
        (package / "cli.py").write_text(
            "import t3_fleet, sys\nprint(t3_fleet.__file__)\n")
        wrapper = self.root / "real-t3-fleet"
        wrapper.write_text(
            "#!/bin/sh\nexport PYTHONPATH=" + str(source) + "\n"
            f'exec "{python}" -m t3_fleet.cli "$@"\n')
        wrapper.chmod(0o755)
        userbase = self.root / "userbase"
        site_packages = userbase / ("lib/python%d.%d/site-packages" % sys.version_info[:2])
        site_packages.mkdir(parents=True)
        marker = self.root / "pth-executed"
        (site_packages / "unrelated.pth").write_text(
            "import pathlib; pathlib.Path(%r).write_text('executed')\n" % str(marker))
        environment = {"PATH": os.environ["PATH"], "PYTHONUSERBASE": str(userbase),
                       "PYTHONPATH": str(source)}
        # Control: ordinary startup processes user site .pth files.
        control = subprocess.run([str(python), "-m", "t3_fleet.cli"],
                                 env=environment, capture_output=True, text=True)
        self.assertEqual(control.returncode, 0, control.stderr)
        self.assertTrue(marker.exists(), "control startup did not process the test .pth")
        marker.unlink()
        before = wrapper.read_bytes()
        self.assertTrue(self.adapter.migrate_source_wrapper(
            wrapper, python, source, "cli"))
        self.assertEqual(wrapper.read_bytes().count(b" -I -S -B "), 1)
        unrelated = self.root / "unrelated-cwd"
        shadow = unrelated / "t3_fleet"
        shadow.mkdir(parents=True)
        (shadow / "__init__.py").write_text("")
        (shadow / "cli.py").write_text("print('unapproved source')\n")
        result = subprocess.run(
            [str(wrapper)], cwd=unrelated,
            env={**environment, "PYTHONHOME": str(self.root / "unset-runtime")},
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(package / "__init__.py"))
        self.assertFalse(marker.exists(), "source wrapper executed an unrelated .pth")
        # Idempotent, explicit migration leaves already-safe bytes alone.
        self.assertFalse(self.adapter.migrate_source_wrapper(
            wrapper, python, source, "cli"))
        self.assertNotEqual(before, wrapper.read_bytes())

    def test_mixed_legacy_and_current_source_wrappers_migrate_once_and_restore(self):
        current = [self.launchers[0]]
        legacy = [self.launchers[1]]
        original = {path: path.read_bytes() for path in self.launchers}
        for path in current:
            path.write_text(
                self.adapter.source_wrapper_text(self.python, self.purelib, "cli")
            )
        mixed = {path: path.read_bytes() for path in self.launchers}
        self.assertNotEqual(original, mixed)
        code, output = self.call()
        self.assertEqual(code, 0, output)
        self.assertIn("migrate-legacy-source-wrappers", json.loads(output)["changes"])
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        for path in self.launchers:
            self.assertEqual(path.read_text().count(" -I -S -B "), 1)
        # Same pin is idempotent after migration.
        before = self.tree()
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")
        self.assertEqual(self.tree(), before)
        code, output = self.call("rollback", ["--backup", str(self.transaction()), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual({path: path.read_bytes() for path in self.launchers}, mixed)

    def test_unknown_source_wrapper_grammar_still_refuses(self):
        path = self.launchers[0]
        path.write_text(path.read_text().replace(" -m ", " -I -m "))
        before = self.tree()
        self.assertNotEqual(self.call()[0], 0)
        self.assertEqual(self.tree(), before)

    def test_upgrade_timestamp_cache_survives_restore_retry_and_private_rollback(self):
        self.use_real_source_interpreter()
        predecessor_backup, predecessor_raw, _, successor, upgrade_backup = (
            self.install_predecessor_select_successor()
        )
        package = self.purelib / "t3_fleet"
        source = package / "cli.py"
        cache = Path(importlib.util.cache_from_source(str(source)))
        py_compile.compile(
            str(source), cfile=str(cache), doraise=True,
            invalidation_mode=py_compile.PycInvalidationMode.TIMESTAMP,
        )
        data = cache.read_bytes()
        self.assertEqual(struct.unpack("<I", data[4:8])[0], 0)
        expected_mtime_ns = source.stat().st_mtime_ns

        code, output = self.call(
            "upgrade", ["--backup", str(upgrade_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        interrupted = []
        original_restore = self.adapter.restore_entry

        def restore(*args, **kwargs):
            result = original_restore(*args, **kwargs)
            if args[2]["category"] == "runtime-package" and not interrupted:
                interrupted.append(True)
                raise KeyboardInterrupt("after timestamped package restore")
            return result

        with patch.object(self.adapter, "restore_entry", side_effect=restore):
            with self.assertRaises(KeyboardInterrupt):
                self.call("rollback", ["--backup", str(upgrade_backup), "--apply"])
        self.assertEqual(interrupted, [True])
        self.assertEqual(source.stat().st_mtime_ns, expected_mtime_ns)

        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        self.assertEqual((self.purelib / self.adapter.RECOVERY).read_bytes(), predecessor_raw)
        self.assertEqual(source.stat().st_mtime_ns, expected_mtime_ns)
        completed = subprocess.run(
            [sys.executable, "-B", "-c", "import t3_fleet.cli"], cwd=self.purelib,
            env={"PATH": os.environ["PATH"]}, capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

        self.argv[self.argv.index("--artifact-dir") + 1] = str(
            successor["predecessor_artifact"]
        )
        self.argv[self.argv.index("--public-source") + 1] = str(
            successor["predecessor_source"]
        )
        self.set_pin(successor["predecessor_pin_file"])
        code, output = self.call(
            "rollback", ["--backup", str(predecessor_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)

    def test_upgrade_rollback_admits_only_exact_partial_successor_stage(self):
        for written in (0, 3):
            with self.subTest(files_written=written), self.fresh_upgrade_case() as case:
                _, predecessor_raw, _, _, upgrade_backup = (
                    case.install_predecessor_select_successor()
                )
                token = upgrade_backup.name.removeprefix("transaction-")
                stage = case.purelib / (".t3-fleet-stage-" + token)
                original_write = case.adapter.write_private
                copied = []

                def write(path, data):
                    if Path(path).parent == stage:
                        if len(copied) == written:
                            raise KeyboardInterrupt("during successor staging")
                        copied.append(Path(path))
                    return original_write(path, data)

                with patch.object(case.adapter, "write_private", side_effect=write):
                    with self.assertRaises(KeyboardInterrupt):
                        case.call(
                            "upgrade", ["--backup", str(upgrade_backup), "--apply"]
                        )
                self.assertEqual(len(list(stage.iterdir())), written)
                code, output = case.call(
                    "rollback", ["--backup", str(upgrade_backup), "--apply"]
                )
                self.assertEqual(code, 0, output)
                self.assertEqual(
                    (case.purelib / case.adapter.RECOVERY).read_bytes(), predecessor_raw
                )

        with self.fresh_upgrade_case() as case:
            _, _, _, _, upgrade_backup = case.install_predecessor_select_successor()
            token = upgrade_backup.name.removeprefix("transaction-")
            stage = case.purelib / (".t3-fleet-stage-" + token)
            original_write = case.adapter.write_private
            copied = []

            def write(path, data):
                if Path(path).parent == stage:
                    if copied:
                        raise KeyboardInterrupt("during successor staging")
                    copied.append(Path(path))
                return original_write(path, data)

            with patch.object(case.adapter, "write_private", side_effect=write):
                with self.assertRaises(KeyboardInterrupt):
                    case.call("upgrade", ["--backup", str(upgrade_backup), "--apply"])
            copied[0].write_bytes(copied[0].read_bytes()[:-1])
            retained = case.tree()
            code, output = case.call(
                "rollback", ["--backup", str(upgrade_backup), "--apply"]
            )
            self.assertEqual(code, 1, output)
            self.assertEqual(case.tree(), retained)

    def test_upgrade_cleanup_retries_exact_mid_rmtree_subsets(self):
        for phase in ("verified", "rollback-started"):
            with self.subTest(phase=phase), self.fresh_upgrade_case() as case:
                _, predecessor_raw, _, _, upgrade_backup = (
                    case.install_predecessor_select_successor()
                )
                token = upgrade_backup.name.removeprefix("transaction-")
                old = case.purelib / (".t3-fleet-old-" + token)
                original_remove = case.adapter.shutil.rmtree
                interrupted = []

                def retain_old(*args, **kwargs):
                    if Path(args[0]) == old:
                        raise OSError("retain old tree")
                    return original_remove(*args, **kwargs)

                if phase == "rollback-started":
                    with patch.object(case.adapter.shutil, "rmtree", side_effect=retain_old):
                        code, output = case.call(
                            "upgrade", ["--backup", str(upgrade_backup), "--apply"]
                        )
                    self.assertEqual(code, 1, output)

                def partial(path, *args, **kwargs):
                    path = Path(path)
                    if path == old and not interrupted:
                        victim = sorted(child for child in path.rglob("*") if child.is_file())[0]
                        victim.unlink()
                        interrupted.append(victim)
                        raise KeyboardInterrupt("during owned tree removal")
                    return original_remove(path, *args, **kwargs)

                with patch.object(case.adapter.shutil, "rmtree", side_effect=partial):
                    with self.assertRaises(KeyboardInterrupt):
                        action = "upgrade" if phase == "verified" else "rollback"
                        case.call(action, ["--backup", str(upgrade_backup), "--apply"])
                self.assertEqual(len(interrupted), 1)
                action = "upgrade" if phase == "verified" else "rollback"
                code, output = case.call(
                    action, ["--backup", str(upgrade_backup), "--apply"]
                )
                self.assertEqual(code, 0, output)
                code, output = case.call(
                    action, ["--backup", str(upgrade_backup), "--apply"]
                )
                self.assertEqual(code, 0, output)
                if phase == "rollback-started":
                    self.assertEqual(
                        (case.purelib / case.adapter.RECOVERY).read_bytes(), predecessor_raw
                    )

    def test_upgrade_completed_retry_accepts_only_recognized_predecessor_caches(self):
        self.use_real_source_interpreter()
        _, predecessor_raw, _, _, upgrade_backup = self.install_predecessor_select_successor()
        code, output = self.call(
            "upgrade", ["--backup", str(upgrade_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        completed = subprocess.run(
            [sys.executable, "-c", "import t3_fleet.cli"], cwd=self.purelib,
            env={"PATH": os.environ["PATH"]}, capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")
        self.assertEqual((self.purelib / self.adapter.RECOVERY).read_bytes(), predecessor_raw)

        unknown = self.purelib / "t3_fleet/__pycache__/foreign.pyc"
        unknown.write_bytes(b"unknown cache")
        before = self.tree()
        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"]
        )
        self.assertEqual(code, 1, output)
        self.assertEqual(self.tree(), before)

    def test_upgrade_rollback_restores_identity_from_authenticated_predecessor_state(self):
        _, predecessor_raw, _, successor, upgrade_backup = (
            self.install_predecessor_select_successor()
        )
        code, output = self.call(
            "upgrade", ["--backup", str(upgrade_backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        original_restore = self.adapter.restore_entry
        interrupted = []

        def restore(*args, **kwargs):
            if not interrupted:
                interrupted.append(True)
                raise KeyboardInterrupt("before first restore")
            return original_restore(*args, **kwargs)

        with patch.object(self.adapter, "restore_entry", side_effect=restore):
            with self.assertRaises(KeyboardInterrupt):
                self.call(
                    "rollback", ["--backup", str(upgrade_backup), "--apply"]
                )
        self.assertEqual(interrupted, [True])
        self.assertEqual(
            self.adapter.read_record(self.purelib)["phase"], "rollback-started"
        )
        self.hash_record.unlink()

        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"]
        )

        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "rolled-back")
        self.assertEqual(
            (self.purelib / self.adapter.RECOVERY).read_bytes(), predecessor_raw
        )
        self.assertEqual(
            json.loads(self.hash_record.read_text())["revision"],
            successor["predecessor_pin"]["revision"],
        )

    def test_upgrade_rollback_crash_matrix(self):
        edges = (
            "restore-subset", "after-displaced-rename", "after-package-restore",
            "after-hash-restore", "cleanup-after-remove", "ready-receipt-after-write",
            "predecessor-ready-after-replace", "predecessor-record-before-replace",
            "predecessor-record-after-replace",
        )
        for edge in edges:
            with self.subTest(edge=edge), self.fresh_upgrade_case() as case:
                _, predecessor_raw, _, _, upgrade_backup = (
                    case.install_predecessor_select_successor()
                )
                code, output = case.call(
                    "upgrade", ["--backup", str(upgrade_backup), "--apply"])
                self.assertEqual(code, 0, output)
                package = case.purelib / "t3_fleet"
                maintained = b'{"private": "rollback-crash-maintained"}\n'
                case.policy.write_bytes(maintained)
                injected = []

                with contextlib.ExitStack() as stack:
                    if edge == "restore-subset":
                        real = case.adapter.write_private
                        def write(path, data):
                            result = real(path, data)
                            selected = ".t3-fleet-restore-" in Path(path).name
                            if selected and not injected:
                                injected.append("after")
                                raise KeyboardInterrupt("injected after protected write")
                            return result
                        stack.enter_context(patch.object(
                            case.adapter, "write_private", side_effect=write))
                    elif edge == "after-displaced-rename":
                        real = case.adapter.os.replace
                        def replace(source, destination):
                            result = real(source, destination)
                            if (Path(source) == package
                                    and Path(destination).name.startswith(".t3-fleet-displaced-")
                                    and not injected):
                                injected.append("after")
                                raise KeyboardInterrupt("injected after displaced rename")
                            return result
                        stack.enter_context(patch.object(
                            case.adapter.os, "replace", side_effect=replace))
                    elif edge in {"after-package-restore", "after-hash-restore"}:
                        real = case.adapter.restore_entry
                        def restore(*args, **kwargs):
                            result = real(*args, **kwargs)
                            entry = args[2]
                            selected = (edge == "after-package-restore"
                                        and entry["category"] == "runtime-package")
                            selected = selected or (edge == "after-hash-restore"
                                                       and entry["category"] == "hash-record")
                            if selected and not injected:
                                injected.append("after")
                                raise KeyboardInterrupt("injected after restore entry")
                            return result
                        stack.enter_context(patch.object(
                            case.adapter, "restore_entry", side_effect=restore))
                    elif edge == "cleanup-after-remove":
                        real = case.adapter.shutil.rmtree
                        def remove(path):
                            result = real(path)
                            if not injected:
                                injected.append("after")
                                raise KeyboardInterrupt("injected during rollback cleanup")
                            return result
                        stack.enter_context(patch.object(
                            case.adapter.shutil, "rmtree", side_effect=remove))
                    else:
                        real = case.adapter.atomic_record_bytes
                        def publish(path, raw, **kwargs):
                            document = json.loads(raw)
                            selected = (edge == "predecessor-ready-after-replace"
                                        and document.get("schema") == 2
                                        and document.get("phase") == "predecessor-ready")
                            predecessor = document.get("schema") == 1
                            selected = selected or (
                                edge in {"predecessor-record-before-replace",
                                         "predecessor-record-after-replace"} and predecessor)
                            selected = selected or (
                                edge == "ready-receipt-after-write"
                                and Path(path) == upgrade_backup / "rollback-ready.json")
                            if selected and not injected:
                                if edge == "predecessor-record-before-replace":
                                    injected.append("before")
                                    raise KeyboardInterrupt("injected before predecessor replacement")
                                real(path, raw, **kwargs)
                                injected.append("after")
                                raise KeyboardInterrupt("injected after recovery record replacement")
                            return real(path, raw, **kwargs)
                        stack.enter_context(patch.object(
                            case.adapter, "atomic_record_bytes", side_effect=publish))

                    with self.assertRaises(KeyboardInterrupt):
                        case.call("rollback", ["--backup", str(upgrade_backup), "--apply"])
                self.assertEqual(injected, [
                    "before" if edge == "predecessor-record-before-replace" else "after"])
                self.assertEqual(case.policy.read_bytes(), maintained)
                code, output = case.call(
                    "rollback", ["--backup", str(upgrade_backup), "--apply"])
                self.assertEqual(code, 0, output)
                self.assertEqual(json.loads(output)["status"],
                                 "unchanged" if edge == "predecessor-record-after-replace"
                                 else "rolled-back")
                self.assertEqual((case.purelib / case.adapter.RECOVERY).read_bytes(),
                                 predecessor_raw)
                self.assertEqual(case.policy.read_bytes(), maintained)

        for kind in ("truncated-stage", "truncated-receipt"):
            with self.subTest(edge=kind), self.fresh_upgrade_case() as case:
                _, _, _, _, upgrade_backup = case.install_predecessor_select_successor()
                code, output = case.call(
                    "upgrade", ["--backup", str(upgrade_backup), "--apply"])
                self.assertEqual(code, 0, output)
                if kind == "truncated-stage":
                    real = case.adapter.write_private
                    def corrupt(path, data):
                        if ".t3-fleet-restore-" in Path(path).name:
                            real(path, b"{")
                            raise KeyboardInterrupt("injected truncated protected object")
                        return real(path, data)
                    patched = patch.object(
                        case.adapter, "write_private", side_effect=corrupt)
                else:
                    real = case.adapter.atomic_record_bytes
                    def corrupt(path, raw, **kwargs):
                        if Path(path) == upgrade_backup / "rollback-ready.json":
                            real(path, b"{", **kwargs)
                            raise KeyboardInterrupt("injected truncated protected object")
                        return real(path, raw, **kwargs)
                    patched = patch.object(
                        case.adapter, "atomic_record_bytes", side_effect=corrupt)
                with patched:
                    with self.assertRaises(KeyboardInterrupt):
                        case.call("rollback", ["--backup", str(upgrade_backup), "--apply"])
                retained = case.tree()
                code, output = case.call(
                    "rollback", ["--backup", str(upgrade_backup), "--apply"])
                self.assertEqual(code, 1, output)
                self.assertEqual(case.tree(), retained)

    def test_upgrade_record_restore_rechecks_authority_and_binding(self):
        for target in ("selected-identity", "active-record", "inventory",
                       "predecessor-copy", "runtime"):
            with self.subTest(target=target), self.fresh_upgrade_case() as case:
                original, changed = case.selected_identity_change(
                    selected_state="alternate", selected_origin="http://127.0.0.1:3775")
                _, predecessor_raw, _, _, upgrade_backup = (
                    case.install_predecessor_select_successor()
                )
                code, output = case.call(
                    "upgrade", ["--backup", str(upgrade_backup), "--apply"])
                self.assertEqual(code, 0, output)
                real = case.adapter.validate_upgrade_ready
                calls = []
                cleanup = []
                real_cleanup = case.adapter.cleanup_upgrade_owned
                def count_cleanup(*args, **kwargs):
                    cleanup.append(True)
                    return real_cleanup(*args, **kwargs)
                def validate(*args, **kwargs):
                    result = real(*args, **kwargs)
                    calls.append(True)
                    if len(calls) == 2:
                        if target == "selected-identity":
                            case.config.write_text(changed)
                        elif target == "active-record":
                            (case.purelib / case.adapter.RECOVERY).write_text("{}")
                        elif target == "inventory":
                            (upgrade_backup / "inventory.json").write_bytes(b"{}")
                        elif target == "predecessor-copy":
                            (upgrade_backup / "predecessor-record.json").write_bytes(b"{}")
                        else:
                            (case.purelib / "t3_fleet/cli.py").write_bytes(b"changed after readiness")
                    return result
                with patch.object(case.adapter, "cleanup_upgrade_owned", side_effect=count_cleanup), \
                        patch.object(case.adapter, "validate_upgrade_ready", side_effect=validate):
                    code, output = case.call(
                        "rollback", ["--backup", str(upgrade_backup), "--apply"])
                self.assertEqual(code, 1, output)
                self.assertEqual(len(calls), 2)
                self.assertEqual(len(cleanup), 1)
                self.assertNotEqual((case.purelib / case.adapter.RECOVERY).read_bytes(),
                                    predecessor_raw)
                self.assertTrue((upgrade_backup / "rollback-ready.json").exists())
                if target == "selected-identity":
                    case.config.write_text(original)

    def test_upgrade_completed_retry_cannot_select_ancestor(self):
        _, predecessor_raw, _, _, upgrade_backup = (
            self.install_predecessor_select_successor()
        )
        code, output = self.call(
            "upgrade", ["--backup", str(upgrade_backup), "--apply"])
        self.assertEqual(code, 0, output)
        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual((self.purelib / self.adapter.RECOVERY).read_bytes(), predecessor_raw)

        copied = self.backups / ("transaction-" + uuid.uuid4().hex)
        shutil.copytree(upgrade_backup, copied)
        before = self.tree()
        code, output = self.call("rollback", ["--backup", str(copied), "--apply"])
        self.assertEqual(code, 1, output)
        self.assertEqual(self.tree(), before)

        receipt = upgrade_backup / "rollback-ready.json"
        receipt_raw = receipt.read_bytes()
        document = json.loads(receipt_raw)
        document["predecessor_record_sha256"] = "0" * 64
        receipt.write_text(json.dumps(document, sort_keys=True, indent=2) + "\n")
        changed = self.tree()
        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"])
        self.assertEqual(code, 1, output)
        self.assertEqual(self.tree(), changed)
        receipt.write_bytes(receipt_raw)

        record_path = self.purelib / self.adapter.RECOVERY
        record_path.write_text(json.dumps({"schema": 1, "phase": "verified"}))
        changed = self.tree()
        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"])
        self.assertEqual(code, 1, output)
        self.assertEqual(self.tree(), changed)
        record_path.write_bytes(predecessor_raw)

        code, output = self.call(
            "rollback", ["--backup", str(upgrade_backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")
        self.assertEqual(record_path.read_bytes(), predecessor_raw)

    def test_plan_reports_checks_changes_inventory_and_external_gates_without_paths(self):
        before = self.tree()
        code, output = self.call()
        self.assertEqual(code, 0, output)
        result = json.loads(output)
        self.assertTrue({"artifact", "layout", "backup-inventory", "boundaries"} <= result["checks"].keys())
        self.assertTrue(all(value == "passed" for value in result["checks"].values()))
        self.assertTrue({"replace-runtime-package", "migrate-legacy-source-wrappers",
                         "publish-verified-hash-record"} <= set(result["changes"]))
        inputs = result["backup_inputs"]
        self.assertTrue({"runtime-package", "entrypoint", "hash-record", "config", "private-file", "state-db"}
                        <= {entry["category"] for entry in inputs})
        self.assertTrue(any(entry["presence"] == "absent" and not entry["required"]
                            and entry["category"] == "state-db" for entry in inputs))
        self.assertTrue(all(entry["label"].startswith("input-") for entry in inputs))
        gates = {gate["gate"]: gate["status"] for gate in result["external_gates"]}
        self.assertEqual(gates["quiescence"], "operator-required")
        self.assertEqual(gates["availability-window"], "operator-required")
        self.assertEqual(gates["scheduled-task-context"], "operator-required")
        self.assertEqual(self.tree(), before)

    def test_plan_scopes_three_host_inventory_and_origin_to_explicit_target(self):
        for selected_origin in ("http://127.0.0.1:3774", "http://[::1]:3774"):
            with self.subTest(selected_origin=selected_origin):
                self.config.write_text(self.three_host_config(selected_origin=selected_origin))
                before = self.tree()
                code, output = self.call(extra=["--target-host", "target-a"])
                self.assertEqual(code, 0, output)
                prepared = self.adapter.plan(self.options(extra=["--target-host", "target-a"]))
                sqlite_entries = {
                    entry["destination"]: entry
                    for entry in prepared["inputs"]
                    if entry["kind"] == "sqlite"
                }
                selected_outbox = self.target.parent / "outbox/outbox.db"
                self.assertEqual(
                    set(sqlite_entries),
                    {str(self.state), str(self.target), str(self.outbox), str(selected_outbox)},
                )
                self.assertFalse(sqlite_entries[str(self.outbox)]["required"])
                self.assertFalse(sqlite_entries[str(selected_outbox)]["required"])
                self.assertFalse(selected_outbox.exists())
                self.assertEqual(self.tree(), before)

    def test_target_identity_inventory_and_origin_refusals_are_write_free(self):
        invalid_origins = (
            ("non-loopback", "https://selected.example.invalid"),
            ("leading-space", " http://127.0.0.1:3774"),
            ("leading-tab", "\thttp://127.0.0.1:3774"),
            ("trailing-newline", "http://127.0.0.1:3774\n"),
            ("embedded-control", "\x1fhttp://127.0.0.1:3774"),
            ("empty-query", "http://127.0.0.1:3774?"),
            ("empty-fragment", "http://127.0.0.1:3774#"),
            ("empty-userinfo", "http://@127.0.0.1:3774"),
            ("ipv6-zone", "http://[::1%lo]:3774"),
            ("encoded-ipv6-zone", "http://[::1%25lo]:3774"),
        )
        cases = (
            (
                "unknown",
                self.three_host_config(),
                ["--target-host", "missing-target"],
            ),
            (
                "selected-db-absent",
                self.three_host_config(selected_state=self.root / "missing-target-state"),
                ["--target-host", "target-a"],
            ),
            *((
                f"selected-origin-{label}",
                self.three_host_config(selected_origin=origin),
                ["--target-host", "target-a"],
            ) for label, origin in invalid_origins),
            (
                "selected-host-malformed",
                '[hosts]\ntarget-a = "not-a-table"\n',
                ["--target-host", "target-a"],
            ),
            (
                "duplicate-toml-key",
                '[hosts.target-a]\nagent_state_dir = "/tmp/a"\n'
                '[hosts.target-a]\nagent_state_dir = "/tmp/b"\n',
                ["--target-host", "target-a"],
            ),
        )
        for label, text, extra in cases:
            with self.subTest(label=label):
                self.config.write_text(text)
                before = self.tree()
                code, output = self.call(extra=extra)
                self.assertNotEqual(code, 0, output)
                self.assertEqual(json.loads(output)["code"], "inventory-invalid")
                self.assertEqual(self.tree(), before)

    def test_target_host_cli_argument_is_required_and_validated(self):
        argv = list(self.argv)
        if "--target-host" in argv:
            index = argv.index("--target-host")
            del argv[index:index + 2]
        for arguments in (argv, [*self.argv, "--target-host", "not/valid"]):
            with self.subTest(arguments="missing" if arguments is argv else "malformed"):
                before = self.tree()
                code, output = self.invoke(["plan", *arguments])
                self.assertNotEqual(code, 0, output)
                self.assertEqual(json.loads(output)["code"], "arguments-invalid")
                self.assertEqual(self.tree(), before)

    def test_multiple_agent_configs_refuse_an_inconsistent_selected_target(self):
        self.config.write_text(self.three_host_config())
        second = self.root / "second-private.toml"
        second.write_text(
            self.three_host_config(selected_origin="http://127.0.0.1:3775")
        )
        before = self.tree()
        code, output = self.call(
            extra=["--target-host", "target-a", "--config", str(second)]
        )
        self.assertNotEqual(code, 0, output)
        self.assertEqual(json.loads(output)["code"], "inventory-invalid")
        self.assertEqual(self.tree(), before)

    def test_saved_binding_refuses_target_change_for_plan_apply_and_rollback(self):
        self.config.write_text(
            self.three_host_config(
                third_origin="http://127.0.0.1:3775",
                third_state=self.target.parent,
            )
        )
        code, output = self.call("apply", ["--target-host", "target-a", "--apply"])
        self.assertEqual(code, 0, output)
        backup = self.transaction()
        record = json.loads((self.purelib / self.adapter.RECOVERY).read_text())
        inventory = json.loads((backup / "inventory.json").read_text())
        self.assertEqual(record["binding"]["target_host"], "target-a")
        self.assertEqual(inventory["binding"]["target_host"], "target-a")
        for action, extra in (
            ("plan", ["--target-host", "target-b"]),
            ("apply", ["--target-host", "target-b", "--apply"]),
            (
                "rollback",
                [
                    "--target-host",
                    "target-b",
                    "--backup",
                    str(backup),
                    "--apply",
                ],
            ),
        ):
            with self.subTest(action=action):
                before = self.tree()
                code, output = self.call(action, extra)
                self.assertNotEqual(code, 0, output)
                self.assertEqual(self.tree(), before)

    def test_saved_binding_refuses_selected_state_change_for_all_actions(self):
        self.assert_selected_identity_drift_refused(
            selected_state="alternate",
            selected_origin="http://127.0.0.1:3774",
        )

    def test_saved_binding_refuses_selected_origin_change_for_all_actions(self):
        self.assert_selected_identity_drift_refused(
            selected_state=self.target.parent,
            selected_origin="http://127.0.0.1:3775",
        )

    def test_saved_binding_refuses_combined_selected_identity_change_for_all_actions(self):
        self.assert_selected_identity_drift_refused(
            selected_state="alternate",
            selected_origin="http://127.0.0.1:3775",
        )

    def test_rollback_binding_edge_refuses_selected_state_change(self):
        self.assert_rollback_binding_edge_refused(
            selected_state="alternate",
            selected_origin="http://127.0.0.1:3774",
        )

    def test_rollback_binding_edge_refuses_selected_origin_change(self):
        self.assert_rollback_binding_edge_refused(
            selected_state=self.target.parent,
            selected_origin="http://127.0.0.1:3775",
        )

    def test_rollback_binding_edge_refuses_combined_selected_identity_change(self):
        self.assert_rollback_binding_edge_refused(
            selected_state="alternate",
            selected_origin="http://127.0.0.1:3775",
        )

    def test_apply_binding_rechecked_after_baseline_fingerprints(self):
        _, changed = self.selected_identity_change(
            selected_state="alternate",
            selected_origin="http://127.0.0.1:3774",
        )
        record_path = self.purelib / self.adapter.RECOVERY
        package = self.purelib / "t3_fleet"
        real_fingerprints = self.adapter.runtime_fingerprints
        injected = []

        def change_after_baseline(inputs):
            actual = real_fingerprints(inputs)
            if (
                not injected
                and record_path.exists()
                and json.loads(record_path.read_text())["phase"] == "backup-complete"
            ):
                self.config.write_text(changed)
                injected.append((self.tree(), self.adapter.fingerprint(package)))
            return actual

        with patch.object(
            self.adapter,
            "runtime_fingerprints",
            side_effect=change_after_baseline,
        ), patch.object(
            self.adapter,
            "cleanup_owned",
            wraps=self.adapter.cleanup_owned,
        ) as cleanup:
            code, output = self.call("apply", ["--apply"])
        self.assertEqual(len(injected), 1)
        self.assertEqual(code, 1, output)
        self.assert_binding_refusal(
            output,
            code="baseline-changed",
            phase="backup-complete",
            writes="started",
        )
        self.assertEqual(self.tree(), injected[0][0])
        self.assertEqual(self.adapter.fingerprint(package), injected[0][1])
        self.assertEqual(json.loads(record_path.read_text())["phase"], "backup-complete")
        self.assertFalse(self.hash_record.exists())
        cleanup.assert_not_called()

    def test_install_binding_rechecked_after_complete_source_stage(self):
        _, changed = self.selected_identity_change(
            selected_state=self.target.parent,
            selected_origin="http://127.0.0.1:3775",
        )
        record_path = self.purelib / self.adapter.RECOVERY
        package = self.purelib / "t3_fleet"
        real_sync = self.adapter.fsync_directory
        install_active = []
        injected = []
        real_install = self.adapter.install

        def observe_install(*args):
            install_active.append(True)
            try:
                return real_install(*args)
            finally:
                install_active.pop()

        def change_after_stage_sync(path):
            real_sync(path)
            if not install_active or injected or Path(path) != package.parent:
                return
            record = json.loads(record_path.read_text())
            token = Path(record["backup"]).name.removeprefix("transaction-")
            stage = package.parent / (".t3-fleet-stage-" + token)
            if stage.exists():
                self.config.write_text(changed)
                injected.append({
                    "runtime": self.adapter.fingerprint(package),
                    "owned": {
                        owned: self.adapter.fingerprint(Path(owned))
                        for owned in record["owned"]
                    },
                })

        with patch.object(
            self.adapter,
            "install",
            side_effect=observe_install,
        ), patch.object(
            self.adapter,
            "fsync_directory",
            side_effect=change_after_stage_sync,
        ), patch.object(
            self.adapter,
            "cleanup_owned",
            wraps=self.adapter.cleanup_owned,
        ) as cleanup:
            code, output = self.call("apply", ["--apply"])
        self.assertEqual(len(injected), 1)
        self.assertEqual(code, 1, output)
        self.assert_binding_refusal(
            output,
            code="install-failed",
            phase="mutation-started",
            writes="started",
        )
        record = json.loads(record_path.read_text())
        self.assertEqual(record["phase"], "mutation-started")
        self.assertIn("after", record)
        self.assertEqual(self.adapter.fingerprint(package), injected[0]["runtime"])
        self.assertEqual(
            {
                owned: self.adapter.fingerprint(Path(owned))
                for owned in record["owned"]
            },
            injected[0]["owned"],
        )
        self.assertFalse(self.hash_record.exists())
        cleanup.assert_not_called()

    def test_identity_binding_rechecked_after_interpreter_anchor(self):
        _, changed = self.selected_identity_change(
            selected_state="alternate",
            selected_origin="http://127.0.0.1:3775",
        )
        record_path = self.purelib / self.adapter.RECOVERY
        real_guard = self.adapter.guard_interpreter_anchor
        injected = []

        def change_after_anchor(*args, **kwargs):
            result = real_guard(*args, **kwargs)
            label = args[1] if len(args) > 1 else kwargs.get("code")
            if not injected and label == "identity-publication-failed":
                self.config.write_text(changed)
                record = json.loads(record_path.read_text())
                injected.append({
                    "runtime": self.adapter.runtime_fingerprints(
                        json.loads((Path(record["backup"]) / "inventory.json").read_text())["inputs"]
                    ),
                    "owned": {
                        owned: self.adapter.fingerprint(Path(owned))
                        for owned in record["owned"]
                    },
                })
            return result

        with patch.object(
            self.adapter,
            "guard_interpreter_anchor",
            side_effect=change_after_anchor,
        ), patch.object(
            self.adapter,
            "cleanup_owned",
            wraps=self.adapter.cleanup_owned,
        ) as cleanup:
            code, output = self.call("apply", ["--apply"])
        self.assertEqual(len(injected), 1)
        self.assertEqual(code, 1, output)
        self.assert_binding_refusal(
            output,
            code="identity-publication-failed",
            phase="mutation-started",
            writes="started",
        )
        record = json.loads(record_path.read_text())
        inventory = json.loads((Path(record["backup"]) / "inventory.json").read_text())
        self.assertEqual(record["phase"], "mutation-started")
        self.assertIn("after", record)
        self.assertEqual(
            self.adapter.runtime_fingerprints(inventory["inputs"]),
            injected[0]["runtime"],
        )
        self.assertEqual(
            {
                owned: self.adapter.fingerprint(Path(owned))
                for owned in record["owned"]
            },
            injected[0]["owned"],
        )
        self.assertFalse(self.hash_record.exists())
        cleanup.assert_not_called()

    def test_verified_binding_rechecked_after_installed_fingerprints(self):
        _, changed = self.selected_identity_change(
            selected_state="alternate",
            selected_origin="http://127.0.0.1:3774",
        )
        record_path = self.purelib / self.adapter.RECOVERY
        real_fingerprints = self.adapter.runtime_fingerprints
        injected = []

        def change_after_installed(inputs):
            actual = real_fingerprints(inputs)
            if (
                not injected
                and record_path.exists()
                and json.loads(record_path.read_text())["phase"] == "mutation-started"
                and self.hash_record.exists()
            ):
                self.config.write_text(changed)
                record = json.loads(record_path.read_text())
                injected.append({
                    "runtime": actual,
                    "identity": self.hash_record.read_bytes(),
                    "owned": {
                        owned: self.adapter.fingerprint(Path(owned))
                        for owned in record["owned"]
                    },
                })
            return actual

        with patch.object(
            self.adapter,
            "runtime_fingerprints",
            side_effect=change_after_installed,
        ), patch.object(
            self.adapter,
            "cleanup_owned",
            wraps=self.adapter.cleanup_owned,
        ) as cleanup:
            code, output = self.call("apply", ["--apply"])
        self.assertEqual(len(injected), 1)
        self.assertEqual(code, 1, output)
        self.assert_binding_refusal(
            output,
            code="identity-publication-failed",
            phase="mutation-started",
            writes="started",
        )
        record = json.loads(record_path.read_text())
        inventory = json.loads((Path(record["backup"]) / "inventory.json").read_text())
        self.assertEqual(record["phase"], "mutation-started")
        self.assertIn("after", record)
        self.assertEqual(
            self.adapter.runtime_fingerprints(inventory["inputs"]),
            injected[0]["runtime"],
        )
        self.assertEqual(self.hash_record.read_bytes(), injected[0]["identity"])
        self.assertEqual(
            {
                owned: self.adapter.fingerprint(Path(owned))
                for owned in record["owned"]
            },
            injected[0]["owned"],
        )
        cleanup.assert_not_called()

    def test_restore_binding_rechecked_after_complete_stage_fingerprint(self):
        _, changed = self.selected_identity_change(
            selected_state="alternate",
            selected_origin="http://127.0.0.1:3775",
        )
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        backup = self.transaction()
        record_path = self.purelib / self.adapter.RECOVERY
        inventory = json.loads((backup / "inventory.json").read_text())
        entry = next(
            item
            for item in inventory["inputs"]
            if item["runtime"] and Path(item["destination"]) != self.hash_record
        )
        stage = self.adapter.restore_paths(backup, entry)[0]
        real_fingerprint = self.adapter.fingerprint
        injected = []

        def change_after_complete_stage(path):
            actual = real_fingerprint(path)
            if not injected and Path(path) == stage and actual == entry["before"]:
                self.config.write_text(changed)
                injected.append(self.tree())
            return actual

        with patch.object(
            self.adapter,
            "fingerprint",
            side_effect=change_after_complete_stage,
        ), patch.object(
            self.adapter,
            "cleanup_owned",
            wraps=self.adapter.cleanup_owned,
        ) as cleanup:
            code, output = self.call(
                "rollback", ["--backup", str(backup), "--apply"]
            )
        self.assertEqual(len(injected), 1)
        self.assertEqual(code, 1, output)
        self.assert_binding_refusal(
            output,
            code="rollback-failed",
            phase="mutation-started",
            writes="started",
        )
        self.assertEqual(self.tree(), injected[0])
        self.assertEqual(json.loads(record_path.read_text())["phase"], "mutation-started")
        cleanup.assert_not_called()

    def test_restore_binding_rechecked_before_absent_hash_identity_removal(self):
        _, changed = self.selected_identity_change(
            selected_state=self.target.parent,
            selected_origin="http://127.0.0.1:3775",
        )
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        backup = self.transaction()
        record_path = self.purelib / self.adapter.RECOVERY
        real_restore = self.adapter.restore_entry
        real_fingerprint = self.adapter.fingerprint
        active = []
        injected = []

        def observe_restore(*args):
            entry = next(
                value
                for value in args
                if isinstance(value, dict) and "destination" in value
            )
            active.append(Path(entry["destination"]) == self.hash_record)
            try:
                return real_restore(*args)
            finally:
                active.pop()

        def change_after_absent_check(path):
            actual = real_fingerprint(path)
            if active and active[-1] and not injected and Path(path) == self.hash_record:
                self.config.write_text(changed)
                injected.append(self.tree())
            return actual

        with patch.object(
            self.adapter,
            "restore_entry",
            side_effect=observe_restore,
        ), patch.object(
            self.adapter,
            "fingerprint",
            side_effect=change_after_absent_check,
        ), patch.object(
            self.adapter,
            "cleanup_owned",
            wraps=self.adapter.cleanup_owned,
        ) as cleanup:
            code, output = self.call(
                "rollback", ["--backup", str(backup), "--apply"]
            )
        self.assertEqual(len(injected), 1)
        self.assertEqual(code, 1, output)
        self.assert_binding_refusal(
            output,
            code="rollback-failed",
            phase="mutation-started",
            writes="started",
        )
        self.assertEqual(self.tree(), injected[0])
        self.assertTrue(self.hash_record.exists())
        self.assertEqual(json.loads(record_path.read_text())["phase"], "mutation-started")
        cleanup.assert_not_called()

    def test_cleanup_binding_rechecked_after_scan_for_all_four_callers(self):
        original, changed = self.selected_identity_change(
            selected_state="alternate",
            selected_origin="http://127.0.0.1:3775",
        )
        record_path = self.purelib / self.adapter.RECOVERY
        real_cleanup = self.adapter.cleanup_owned
        real_fingerprint = self.adapter.fingerprint

        def refuse_at_last_cleanup_fingerprint(action, arguments, *, expected_code,
                                               expected_result_phase, expected_record_phase):
            active = []
            injected = []

            def observe_cleanup(args, record, inventory):
                owned = [Path(path) for path in record.get("owned", {})]
                if "rollback" in record:
                    for entry in inventory["inputs"]:
                        if entry["runtime"]:
                            owned.extend(self.adapter.restore_paths(Path(record["backup"]), entry))
                active.append(owned[-1])
                try:
                    return real_cleanup(args, record, inventory)
                finally:
                    active.pop()

            def change_after_last_scan(path):
                actual = real_fingerprint(path)
                if active and not injected and Path(path) == active[-1]:
                    self.config.write_text(changed)
                    injected.append(self.tree())
                return actual

            with patch.object(
                self.adapter,
                "cleanup_owned",
                side_effect=observe_cleanup,
            ) as cleanup, patch.object(
                self.adapter,
                "fingerprint",
                side_effect=change_after_last_scan,
            ):
                code, output = self.call(action, arguments)
            self.assertEqual(len(injected), 1)
            self.assertEqual(cleanup.call_count, 1)
            self.assertEqual(code, 1, output)
            self.assert_binding_refusal(
                output,
                code=expected_code,
                phase=expected_result_phase,
                writes="possible" if expected_result_phase == "locking" else "started",
            )
            self.assertEqual(self.tree(), injected[0])
            self.assertEqual(
                json.loads(record_path.read_text())["phase"],
                expected_record_phase,
            )
            self.config.write_text(original)

        refuse_at_last_cleanup_fingerprint(
            "apply",
            ["--apply"],
            expected_code="owned-cleanup-failed",
            expected_result_phase="mutation-started",
            expected_record_phase="verified",
        )
        refuse_at_last_cleanup_fingerprint(
            "apply",
            ["--apply"],
            expected_code="backup-invalid",
            expected_result_phase="locking",
            expected_record_phase="verified",
        )
        backup = self.transaction()
        refuse_at_last_cleanup_fingerprint(
            "rollback",
            ["--backup", str(backup), "--apply"],
            expected_code="owned-cleanup-failed",
            expected_result_phase="mutation-started",
            expected_record_phase="rolled-back",
        )
        refuse_at_last_cleanup_fingerprint(
            "rollback",
            ["--backup", str(backup), "--apply"],
            expected_code="recovery-recheck-failed",
            expected_result_phase="locking",
            expected_record_phase="rolled-back",
        )

    def test_rollback_binding_edge_refuses_before_success_and_final_cleanup(self):
        backup, alternate_state = self.install_with_alternate_target()
        original_config = self.config.read_text()
        changed_config = self.three_host_config(
            selected_state=alternate_state,
            selected_origin="http://127.0.0.1:3775",
        )
        record_path = self.purelib / self.adapter.RECOVERY
        inventory = json.loads((backup / "inventory.json").read_text())
        expected_runtime = {
            entry["destination"]: entry["before"]
            for entry in inventory["inputs"]
            if entry["runtime"]
        }

        real_fingerprints = self.adapter.runtime_fingerprints
        before_success = []

        def change_after_restoration(inputs):
            actual = real_fingerprints(inputs)
            if (
                not before_success
                and json.loads(record_path.read_text())["phase"] == "mutation-started"
                and actual == expected_runtime
            ):
                self.config.write_text(changed_config)
                before_success.append(self.tree())
            return actual

        with patch.object(
            self.adapter,
            "runtime_fingerprints",
            side_effect=change_after_restoration,
        ), patch.object(
            self.adapter,
            "cleanup_owned",
            wraps=self.adapter.cleanup_owned,
        ) as cleanup:
            code, output = self.call(
                "rollback", ["--backup", str(backup), "--apply"]
            )
        self.assertEqual(len(before_success), 1)
        self.assertEqual(code, 1, output)
        self.assertEqual(
            json.loads(output),
            {
                "code": "rollback-failed",
                "phase": "mutation-started",
                "recovery": "preserve-record-and-backup",
                "status": "refused",
                "writes": "started",
            },
        )
        self.assertEqual(self.tree(), before_success[0])
        self.assertEqual(json.loads(record_path.read_text())["phase"], "mutation-started")
        cleanup.assert_not_called()

        self.config.write_text(original_config)
        real_atomic_json = self.adapter.atomic_json
        before_cleanup = []

        def change_after_success(path, value):
            real_atomic_json(path, value)
            if path == record_path and value.get("phase") == "rolled-back":
                self.config.write_text(changed_config)
                before_cleanup.append(self.tree())

        with patch.object(
            self.adapter,
            "atomic_json",
            side_effect=change_after_success,
        ), patch.object(
            self.adapter,
            "cleanup_owned",
            wraps=self.adapter.cleanup_owned,
        ) as cleanup:
            code, output = self.call(
                "rollback", ["--backup", str(backup), "--apply"]
            )
        self.assertEqual(len(before_cleanup), 1)
        self.assertEqual(code, 1, output)
        self.assertEqual(
            json.loads(output),
            {
                "code": "owned-cleanup-failed",
                "phase": "mutation-started",
                "recovery": "preserve-record-and-backup",
                "status": "refused",
                "writes": "started",
            },
        )
        self.assertEqual(self.tree(), before_cleanup[0])
        self.assertEqual(json.loads(record_path.read_text())["phase"], "rolled-back")
        cleanup.assert_not_called()

    def test_saved_identity_allows_unrelated_config_maintenance(self):
        backup, _ = self.install_with_alternate_target()
        original = self.three_host_config()
        maintained = self.three_host_config(
            third_origin="https://changed-unused.example.invalid",
            third_state=self.root / "changed-unused-target",
        )
        maintained = maintained.replace(
            "state_dir = ",
            "global_active_limit = 7\nstate_dir = ",
            1,
        ).replace(
            '[hosts.target-a]\ntransport = "ssh"\n',
            '[hosts.target-a]\ntransport = "ssh"\nenabled = false\nactive_limit = 7\n',
            1,
        )
        self.config.write_text(maintained)
        before = self.tree()
        code, output = self.call("plan")
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "planned")
        self.assertEqual(self.tree(), before)
        self.config.write_text(original)
        before = self.tree()
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")
        self.assertEqual(self.tree(), before)
        code, output = self.call(
            "rollback", ["--backup", str(backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "rolled-back")
        self.assertEqual(self.config.read_text(), original)
        before = self.tree()
        code, output = self.call(
            "rollback", ["--backup", str(backup), "--apply"]
        )
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")
        self.assertEqual(self.tree(), before)

    def test_refusal_reports_preflight_and_preserved_mutation_record_separately(self):
        code, invalid = self.invoke(["--layout", "private-secret-not-a-layout"])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(invalid)["code"], "arguments-invalid")
        self.assertNotIn("private-secret", invalid)
        self.assertNotIn("private-secret", self.diagnostics)
        before = self.tree()
        code, output = self.call("apply", ["--apply", "--state-db", str(self.root / "missing.db")])
        self.assertNotEqual(code, 0)
        result = json.loads(output)
        self.assertEqual(result["code"], "inventory-invalid")
        self.assertEqual(result["phase"], "preflight")
        self.assertEqual(result["writes"], "none")
        self.assertEqual(result["recovery"], "none-created")
        self.assertEqual(self.tree(), before)
        with patch.object(self.adapter, "install", side_effect=ValueError("private path /secret/prompt")):
            code, output = self.call("apply", ["--apply"])
        self.assertNotEqual(code, 0)
        result = json.loads(output)
        self.assertEqual(result["code"], "install-failed")
        self.assertEqual(result["phase"], "mutation-started")
        self.assertEqual(result["recovery"], "preserve-record-and-backup")
        self.assertNotIn("/secret", output)
        before = self.tree()
        code, output = self.call("apply", ["--apply"])
        self.assertNotEqual(code, 0)
        self.assertEqual(json.loads(output)["code"], "recovery-required")
        self.assertEqual(json.loads(output)["recovery"], "preserve-record-and-backup")
        self.assertEqual(self.tree(), before)

    def test_home_prefix_wrappers_apply_and_rollback_preserve_exact_bytes(self):
        source = self.root / ".local/share/t3-fleet/source-reviewed"
        source.parent.mkdir(parents=True)
        for parent in (source.parent, source.parent.parent, source.parent.parent.parent):
            parent.chmod(0o700)
        self.purelib.rename(source)
        self.purelib = source
        for flag in ("--purelib", "--source-root"):
            self.argv[self.argv.index(flag) + 1] = str(source)
        self.probe["purelib"] = str(source)
        self.probe["import_origin"] = str(source / "t3_fleet/__init__.py")
        for path, module in zip(self.launchers, ("cli", "agent")):
            path.write_text('#!/bin/sh\nexport PYTHONPATH="$HOME/.local/share/t3-fleet/source-reviewed"\n'
                            f'exec {self.python} -m t3_fleet.{module} "$@"\n')
        sentinel = {path: path.read_bytes() for path in self.launchers}
        with patch.object(self.adapter.Path, "home", return_value=self.root):
            before = self.tree()
            self.assertEqual(self.call()[0], 0)
            self.assertEqual(self.tree(), before)
            self.assertEqual(self.call("apply", ["--apply"])[0], 0)
            self.assertEqual(self.call("rollback", ["--backup", str(self.transaction()), "--apply"])[0], 0)
        for path, data in sentinel.items():
            self.assertEqual(path.read_bytes(), data)

    def test_home_wrapper_expansion_is_exact_and_never_executes_shell_text(self):
        forms = ["\"$HOME/wrong\"", "'$HOME/source-reviewed'", '"${HOME}/source-reviewed"',
                 '"$OTHER/source-reviewed"', '"$(echo secret)/source-reviewed"',
                 '"$HOME/../source-reviewed"', '"$HOME/source-*"',
                 '"$HOME/source-reviewed"; echo extra', '"$HOME/source-reviewed`id`"']
        with patch.object(self.adapter.Path, "home", return_value=self.root):
            for path in self.launchers:
                original = path.read_text()
                for form in forms:
                    with self.subTest(launcher=path.name, form=form):
                        path.write_text(original.replace('"' + str(self.purelib) + '"', form))
                        before = self.tree()
                        self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
                        self.assertEqual(self.tree(), before)
                path.write_text(original)

    def test_global_only_pip_bootstrap_is_isolated_and_loads_the_inspected_origin(self):
        self.windows_fixture()
        pip_root = Path(self.probe["pip_root"])
        # A global sibling must never become an import search path.
        (pip_root / "fractions.py").write_text("raise RuntimeError('global sibling executed')\n")
        commands = []
        original_run = self.adapter.run_anchored_python
        def inspect_bootstrap(anchor, arguments, **kwargs):
            if anchor["python"] == str(self.python):
                completed = subprocess.run([sys.executable, "-I", "-S", "-B", *arguments], capture_output=True,
                                           env=self.adapter.command_environment(), cwd=self.backups)
                self.assertEqual(completed.returncode, 0, completed.stderr.decode())
                result = json.loads(completed.stdout)
                self.assertEqual(result["origin"], str(pip_root / "pip/__main__.py"))
                self.assertIn("--user", result["argv"])
                self.assertIn("--no-index", result["argv"])
                commands.append(arguments)
                raise ValueError("fixture stops before any pip installation")
            return original_run(anchor, arguments, **kwargs)
        with patch.object(self.adapter, "windows_acl", return_value=None):
            before = self.tree()
            self.assertEqual(self.call()[0], 0)
            self.assertEqual(self.tree(), before)
            with patch.object(self.adapter, "run_anchored_python", side_effect=inspect_bootstrap):
                self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
            self.assertEqual(len(commands), 1)
            self.assertEqual(self.call("rollback", ["--backup", str(self.transaction()), "--apply"])[0], 0)

    def test_pip_missing_ambiguous_or_unsafe_refuses_before_lock_or_backup(self):
        self.windows_fixture()
        candidates = self.probe["pip_candidates"]
        with patch.object(self.adapter, "windows_acl", return_value=None):
            for value in ([], candidates * 2,
                          [{"root": str(self.root), "origin": self.probe["pip_origin"]}]):
                self.probe["pip_candidates"] = value
                before = self.tree()
                code, output = self.call("apply", ["--apply"])
                self.assertNotEqual(code, 0, output)
                self.assertEqual(json.loads(output)["code"], "pip-ineligible")
                self.assertEqual(json.loads(output)["phase"], "preflight")
                self.assertEqual(self.tree(), before)
            self.probe["pip_candidates"] = candidates
            vendor = Path(self.probe["pip_root"]) / "pip/_vendor/__init__.py"
            vendor.write_text("DEBUNDLED = True\n")
            before = self.tree()
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
            self.assertEqual(self.tree(), before)
            vendor.write_text("DEBUNDLED = False\n")
            entrypoint = Path(self.probe["pip_root"]) / "pip/__main__.py"
            entrypoint.unlink()
            entrypoint.symlink_to(self.policy)
            before = self.tree()
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
            self.assertEqual(self.tree(), before)

    def test_source_apply_repeat_and_runtime_rollback_preserve_private_state(self):
        sentinel = {path: path.read_bytes() for path in (*self.launchers, self.config, self.task)}
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        package = self.purelib / "t3_fleet"
        self.assertEqual({p.name for p in package.iterdir()},
                         {Path(name).name for name in self.artifacts.modules})
        self.assertEqual((package / "agent.py").read_bytes(), b"# public fixture\n")
        backup = self.transaction()
        inventory = json.loads((backup / "inventory.json").read_text())
        db_entries = [entry for entry in inventory["inputs"] if entry["kind"] == "sqlite"]
        self.assertEqual({entry["destination"] for entry in db_entries if entry["exists"]},
                         {str(self.state), str(self.target), str(self.outbox)})
        self.assertTrue(any(not entry["exists"] and entry["destination"].endswith("outbox.db")
                            for entry in db_entries))
        for entry in db_entries:
            if entry["exists"]:
                with contextlib.closing(sqlite3.connect(backup / entry["label"] / "data")) as db:
                    self.assertEqual(db.execute("select payload from retained").fetchall(),
                                     [("private queued prompt and intent",)])
        if os.name == "posix":
            self.assertEqual(backup.stat().st_mode & 0o777, 0o700)
            self.assertTrue(all(path.stat().st_mode & 0o777 == 0o600
                                for path in backup.rglob("*") if path.is_file()))
        before = self.tree()
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        self.assertIn('"status": "unchanged"', output)
        self.assertEqual(self.tree(), before)
        self.connections[0].execute("insert into retained values ('new receipt')")
        self.connections[0].commit()
        code, output = self.call("rollback", ["--backup", str(backup), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertFalse(self.hash_record.exists())
        self.assertTrue((package / "stale.py").is_file())
        self.assertFalse((package / "agent.py").exists())
        self.assertEqual(self.connections[0].execute("select count(*) from retained").fetchone()[0], 2)
        for path, data in sentinel.items():
            self.assertEqual(path.read_bytes(), data)

    def test_refusals_happen_before_adapter_writes(self):
        cases = [
            ["--purelib", str(self.artifact_dir)],
            ["--backup-parent", str(self.purelib)],
            ["--source-root", str(self.root)],
            ["--state-db", str(self.root / "missing.db")],
            ["--private-file", str(self.root / "missing-policy")],
        ]
        for extra in cases:
            with self.subTest(extra=extra):
                before = self.tree()
                self.assertNotEqual(self.call("apply", ["--apply", *extra])[0], 0)
                self.assertEqual(self.tree(), before)
        for action in ("apply", "rollback"):
            before = self.tree()
            self.assertNotEqual(self.call(action)[0], 0)
            self.assertEqual(self.tree(), before)

    def test_wrong_wrapper_interpreter_scheme_and_editable_are_rejected(self):
        original = self.launchers[0].read_text()
        for text in (original.replace(str(self.python), "/wrong/python"),
                     original + "echo unexpected\n", original.replace(str(self.purelib), "/wrong/source")):
            self.launchers[0].write_text(text)
            self.assertNotEqual(self.call()[0], 0)
        self.launchers[0].write_text(original)
        self.probe["executable"] = "/wrong/python"
        self.assertNotEqual(self.call()[0], 0)
        self.probe["executable"] = str(self.python)
        (self.purelib / "t3-fleet.egg-link").write_text("editable")
        self.assertNotEqual(self.call()[0], 0)

    def test_wheel_and_self_consistent_manifest_cannot_replace_trusted_git(self):
        wheel = self.artifact_dir / WHEEL
        members = self.artifacts.wheel_members()
        members["t3_fleet/agent.py"] = b"substituted runtime\n"
        self.artifacts.write_wheel(members)
        wheel.write_bytes(self.artifacts.wheel.read_bytes())
        manifest = json.loads((self.artifact_dir / "public-runtime-manifest.json").read_text())
        manifest["wheel"]["sha256"] = hashlib.sha256(wheel.read_bytes()).hexdigest()
        manifest["modules"]["t3_fleet/agent.py"] = hashlib.sha256(members["t3_fleet/agent.py"]).hexdigest()
        (self.artifact_dir / "public-runtime-manifest.json").write_text(json.dumps(manifest))
        before = self.tree()
        self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual(self.tree(), before)

    def test_symlink_reparse_and_unprotected_backup_are_rejected(self):
        linked = self.root / "linked"
        linked.symlink_to(self.backups, target_is_directory=True)
        self.assertNotEqual(self.call(extra=["--backup-parent", str(linked)])[0], 0)
        (self.purelib / "t3_fleet/link.py").symlink_to(self.config)
        self.assertNotEqual(self.call()[0], 0)
        (self.purelib / "t3_fleet/link.py").unlink()
        with patch.object(self.adapter, "is_reparse", return_value=True):
            self.assertNotEqual(self.call()[0], 0)
        self.backups.chmod(0o755)
        self.assertNotEqual(self.call()[0], 0)

    def test_interrupted_rename_refuses_retry_and_allows_explicit_rollback(self):
        replace = self.adapter.os.replace
        def stop_after_old(source, destination):
            if Path(destination) == self.purelib / "t3_fleet":
                raise OSError("fixture rename interruption")
            return replace(source, destination)
        with patch.object(self.adapter.os, "replace", side_effect=stop_after_old):
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        backup = self.transaction()
        inventory_bytes = (backup / "inventory.json").read_bytes()
        self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual((backup / "inventory.json").read_bytes(), inventory_bytes)
        self.assertEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
        self.assertTrue((self.purelib / "t3_fleet/stale.py").exists())

    def test_snapshot_failure_and_identity_failure_retain_recovery(self):
        with patch.object(self.adapter, "snapshot", side_effect=OSError("snapshot interrupted")):
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertTrue((self.purelib / "t3_fleet/stale.py").exists())
        self.assertFalse(self.hash_record.exists())
        self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)

    def test_identity_publication_failure_requires_runtime_rollback(self):
        atomic = self.adapter.atomic_json
        def refuse_identity(path, value):
            if path == self.hash_record:
                raise OSError("identity publication interrupted")
            return atomic(path, value)
        with patch.object(self.adapter, "atomic_json", side_effect=refuse_identity):
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertFalse(self.hash_record.exists())
        self.assertFalse((self.purelib / "t3_fleet/stale.py").exists())
        self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual(self.call("rollback", ["--backup", str(self.transaction()), "--apply"])[0], 0)

    def test_abrupt_exit_between_renames_has_recoverable_known_footprint(self):
        replace = self.adapter.os.replace
        def killed(source, destination):
            if Path(destination) == self.purelib / "t3_fleet":
                record = json.loads((self.purelib / self.adapter.RECOVERY).read_text())
                self.assertEqual(record["phase"], "mutation-started")
                raise KeyboardInterrupt()
            return replace(source, destination)
        with patch.object(self.adapter.os, "replace", side_effect=killed):
            with self.assertRaises(KeyboardInterrupt):
                self.call("apply", ["--apply"])
        self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual(self.call("rollback", ["--backup", str(self.transaction()), "--apply"])[0], 0)

    def test_windows_scheme_acl_and_record_boundaries_before_writes(self):
        dist = self.windows_fixture()
        before = self.tree()
        self.assertNotEqual(self.call()[0], 0)  # Linux cannot prove a Windows ACL.
        self.assertEqual(self.tree(), before)
        with patch.object(self.adapter, "windows_acl", return_value=None):
            self.assertEqual(self.call()[0], 0)
            for key, value in (("version", [3, 13, 0]), ("user_site_enabled", False),
                               ("purelib", str(self.root)), ("scripts", str(self.root))):
                original = self.probe[key]
                self.probe[key] = value
                self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
                self.probe[key] = original
                self.assertEqual(self.tree(), before)
            record = dist / "RECORD"
            record.write_text(record.read_text() + "../../unowned-file,,\n")
            before = self.tree()
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
            self.assertEqual(self.tree(), before)

    def test_console_script_with_extra_execution_is_not_an_owned_entrypoint(self):
        self.windows_fixture(extra_script="raise RuntimeError('extra execution')\n")
        before = self.tree()
        with patch.object(self.adapter, "windows_acl", return_value=None):
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual(self.tree(), before)

    def test_invalid_sqlite_snapshot_never_mutates_package_and_retry_refuses(self):
        broken = self.root / "broken.db"
        broken.write_bytes(b"not a sqlite database")
        self.argv += ["--state-db", str(broken)]
        before = (self.purelib / "t3_fleet/__init__.py").read_bytes()
        self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual((self.purelib / "t3_fleet/__init__.py").read_bytes(), before)
        self.assertFalse(self.hash_record.exists())
        self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)

    def test_sqlite_snapshots_are_self_contained_after_connections_close(self):
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        backup = self.transaction()
        inventory = json.loads((backup / "inventory.json").read_text())
        for entry in inventory["inputs"]:
            if entry["kind"] == "sqlite" and entry["exists"]:
                path = backup / entry["label"] / "data"
                self.assertEqual(path.read_bytes()[18:20], b"\x01\x01", "archived database still requires WAL mode")
                with contextlib.closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as database:
                    self.assertEqual(database.execute("pragma integrity_check").fetchone(), ("ok",))
        before = self.tree()
        gc.collect()
        self.assertEqual(self.tree(), before)

    def test_windows_pip_failure_uses_explicit_user_scheme_and_can_rollback(self):
        self.windows_fixture()
        commands = []
        run = self.adapter.run_anchored_python
        def pip_failure(anchor, arguments, **kwargs):
            if str(self.python) == anchor["python"]:
                commands.append(arguments)
                raise ValueError("disposable pip failure")
            return run(anchor, arguments, **kwargs)
        with patch.object(self.adapter, "windows_acl", return_value=None):
            with patch.object(self.adapter, "run_anchored_python", side_effect=pip_failure):
                self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
            self.assertEqual(len(commands), 1)
            self.assertIn("--user", commands[0])
            self.assertIn("--no-index", commands[0])
            self.assertIn("--no-deps", commands[0])
            self.assertIn(self.probe["pip_origin"], commands[0])
            self.assertFalse(self.hash_record.exists())
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
            self.assertEqual(self.call("rollback", ["--backup", str(self.transaction()), "--apply"])[0], 0)
        self.assertTrue((self.purelib / "t3_fleet/stale.py").exists())

    def test_tampered_backup_and_excess_distribution_are_rejected(self):
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        backup = self.transaction()
        (backup / "input-0000/data/stale.py").write_text("backup tamper\n")
        before = self.tree()
        self.assertNotEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
        self.assertEqual(self.tree(), before)

    def test_payload_and_prior_identity_are_backed_up_but_never_restored_as_state(self):
        self.hash_record.write_bytes(b"prior identity\n")
        payload = self.root / "private-payload"
        payload.write_bytes(b"queued extra private payload")
        self.argv += ["--payload-file", str(payload)]
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        backup = self.transaction()
        inventory = json.loads((backup / "inventory.json").read_text())
        entry = next(item for item in inventory["inputs"] if item["destination"] == str(payload))
        self.assertEqual((backup / entry["label"] / "data").read_bytes(), payload.read_bytes())
        payload.write_bytes(b"new work after installation")
        self.assertEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
        self.assertEqual(payload.read_bytes(), b"new work after installation")
        self.assertEqual(self.hash_record.read_bytes(), b"prior identity\n")

    def test_traversal_duplicate_and_metadata_tamper_rejected_before_writes(self):
        wheel = self.artifact_dir / WHEEL
        baseline = wheel.read_bytes()
        for name in ("../escape.py", "t3_fleet/agent.py", DIST + "METADATA"):
            wheel.write_bytes(baseline)
            with contextlib.suppress(UserWarning):
                with zipfile.ZipFile(wheel, "a") as archive:
                    archive.writestr(name, b"untrusted")
            before = self.tree()
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
            self.assertEqual(self.tree(), before)

    def test_unsupported_wheel_compression_has_a_bounded_refusal(self):
        wheel = self.artifact_dir / WHEEL
        data = bytearray(wheel.read_bytes())
        # ZIP local and central compression-method fields, without extraction.
        for signature, offset in ((b"PK\x03\x04", 8), (b"PK\x01\x02", 10)):
            start = 0
            while (start := data.find(signature, start)) >= 0:
                struct.pack_into("<H", data, start + offset, 99)
                start += 4
        wheel.write_bytes(data)
        before = self.tree()
        self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual(self.tree(), before)

    def test_missing_configured_controller_state_inventory_is_rejected(self):
        text = self.three_host_config().replace(
            json.dumps(str(self.state.parent)),
            json.dumps(str(self.root / "unresolved-state")),
            1,
        )
        self.config.write_text(text)
        before = self.tree()
        self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual(self.tree(), before)

    def test_module_origin_and_executable_pth_cannot_redirect_runtime(self):
        self.probe["import_origin"] = str(self.root / "unowned/t3_fleet/__init__.py")
        self.assertNotEqual(self.call()[0], 0)
        self.probe["import_origin"] = str(self.purelib / "t3_fleet/__init__.py")
        (self.purelib / "redirect.pth").write_text("import t3_fleet.agent\n")
        # Source wrappers disable site processing, so the .pth is inert. The
        # exact package origin check above remains mandatory.
        self.assertEqual(self.call()[0], 0)

    def test_interpreter_pth_override_refuses_before_probe_and_writes(self):
        (self.python.parent / "python314._pth").write_text("uninspected-root\nimport site\n")
        before = self.tree()
        with patch.object(self.adapter, "probe_interpreter", side_effect=AssertionError("probe must not start")):
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual(self.tree(), before)

    def test_console_directory_cannot_shadow_the_selected_package(self):
        self.windows_fixture()
        (self.scripts / "t3_fleet.py").write_text("raise RuntimeError('shadow runtime')\n")
        before = self.tree()
        with patch.object(self.adapter, "windows_acl", return_value=None):
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual(self.tree(), before)

    def test_same_runtime_lock_is_released_on_process_exit(self):
        code = ("import importlib.util,sys,os; from pathlib import Path; "
                "s=importlib.util.spec_from_file_location('adapter',sys.argv[1]); "
                "m=importlib.util.module_from_spec(s); s.loader.exec_module(m); "
                "lock=m.local_lock(Path(sys.argv[2])); lock.__enter__(); os._exit(0)")
        child = subprocess.run([sys.executable, "-B", "-c", code, str(SCRIPT), str(self.purelib)],
                               capture_output=True)
        self.assertEqual(child.returncode, 0, child.stderr.decode())
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)

    def test_rollback_refuses_unrelated_runtime_drift(self):
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        (self.purelib / "t3_fleet/agent.py").write_text("operator newer change\n")
        before = self.tree()
        self.assertNotEqual(self.call("rollback", ["--backup", str(self.transaction()), "--apply"])[0], 0)
        self.assertEqual(self.tree(), before)

    def test_os_lock_refuses_concurrent_apply(self):
        options = self.options()
        with self.adapter.local_lock(self.purelib):
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)

    def use_real_source_interpreter(self):
        for flag, value in (("--python", sys.executable), ("--prefix", sys.prefix)):
            self.argv[self.argv.index(flag) + 1] = value
        self.probe.update(executable=sys.executable, prefix=sys.prefix,
                          base_prefix=sys.base_prefix, version=list(sys.version_info[:3]))
        self.anchor.update(python=sys.executable, resolved=str(Path(sys.executable).resolve()),
                           prefix=sys.prefix)
        for path, module in zip(self.launchers, ("cli", "agent")):
            path.write_text(f'#!/bin/sh\nexport PYTHONPATH="{self.purelib}"\n'
                            f'exec "{sys.executable}" -m t3_fleet.{module} "$@"\n')

    def test_real_import_caches_keep_original_baseline_repeat_and_rollback(self):
        self.use_real_source_interpreter()
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        backup = self.transaction()
        record = (self.purelib / self.adapter.RECOVERY).read_bytes()
        inventory = (backup / "inventory.json").read_bytes()
        for optimization in ([], ["-O"], ["-OO"]):
            completed = subprocess.run([sys.executable, *optimization, "-c", "import t3_fleet"],
                                       cwd=self.purelib, env={"PATH": os.environ["PATH"]}, capture_output=True)
            self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(len(list((self.purelib / "t3_fleet/__pycache__").glob("*.pyc"))), 3)
        before = self.tree()
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")
        self.assertEqual(self.tree(), before)
        self.assertEqual((self.purelib / self.adapter.RECOVERY).read_bytes(), record)
        self.assertEqual((backup / "inventory.json").read_bytes(), inventory)
        self.assertEqual(len(list(self.backups.iterdir())), 1)
        self.connections[0].execute("insert into retained values ('new receipt')")
        self.connections[0].commit()
        self.assertEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
        self.assertEqual(self.connections[0].execute("select count(*) from retained").fetchone()[0], 2)

    def test_verified_drift_refuses_repeat_without_replacing_recovery(self):
        self.use_real_source_interpreter()
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        package = self.purelib / "t3_fleet"
        cache = package / "__pycache__"
        cache.mkdir()
        cases = [(package / "agent.py", b"modified source\n"),
                 (cache / "unknown.pyc", b"unknown bytecode"),
                 (cache / f"__init__.{sys.implementation.cache_tag}.pyc", b"forged bytecode"),
                 (package / "unknown.py", b"# unknown module\n")]
        for path, data in cases:
            with self.subTest(name=path.name):
                cache.mkdir(exist_ok=True)
                original = path.read_bytes() if path.exists() else None
                path.write_bytes(data)
                before = self.tree()
                self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
                self.assertNotEqual(self.call("rollback", ["--backup", str(self.transaction()), "--apply"])[0], 0)
                self.assertEqual(self.tree(), before)
                if original is None:
                    path.unlink()
                else:
                    path.write_bytes(original)

    def test_valid_cache_with_forged_header_or_code_is_refused(self):
        self.use_real_source_interpreter()
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        completed = subprocess.run([sys.executable, "-c", "import t3_fleet"], cwd=self.purelib,
                                   env={"PATH": os.environ["PATH"]}, capture_output=True)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        cache = next((self.purelib / "t3_fleet/__pycache__").glob("*.pyc"))
        original = cache.read_bytes()
        # Build hostile candidates from fresh trusted compilation, never by
        # decoding a cache. Each keeps the original valid header unless named.
        generate = r'''import sys,json,base64,marshal
from pathlib import Path
source=Path(sys.argv[1])
code=compile(source.read_bytes(),str(source),"exec",dont_inherit=True)
variants={"instructions":code.replace(co_code=b"\x09\x00"+code.co_code[2:]),
          "constants":code.replace(co_consts=("foreign constant",)+code.co_consts[1:]),
          "filename":code.replace(co_filename=str(source)+".foreign"),
          "code_flags":code.replace(co_flags=code.co_flags ^ 0x1000000)}
print(json.dumps({name:base64.b64encode(marshal.dumps(value)).decode()
                  for name,value in variants.items()}))
'''
        changed = json.loads(subprocess.check_output([sys.executable, "-I", "-S", "-B", "-c",
                                                     generate, str(cache.parent.parent / "__init__.py")]))
        cases = {name: original[:16] + base64.b64decode(payload) for name, payload in changed.items()}
        cases.update(trailing=original+b"foreign", truncated=original[:-1],
                     magic=b"BAD!"+original[4:], header=original[:8]+b"bad!bad!"+original[16:],
                     header_flags=original[:4]+struct.pack("<I", 2)+original[8:])
        backup = self.transaction()
        record = (self.purelib / self.adapter.RECOVERY).read_bytes()
        inventory = (backup / "inventory.json").read_bytes()
        identity = self.hash_record.read_bytes()
        for label, forged in cases.items():
            with self.subTest(case=label):
                cache.write_bytes(forged)
                before = self.tree()
                self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
                self.assertNotEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
                self.assertEqual(self.tree(), before)
                self.assertEqual((self.purelib / self.adapter.RECOVERY).read_bytes(), record)
                self.assertEqual((backup / "inventory.json").read_bytes(), inventory)
                self.assertEqual(self.hash_record.read_bytes(), identity)
                self.assertEqual(len(list(self.backups.iterdir())), 1)
                cache.write_bytes(original)
        for tag in ("foreign", sys.implementation.cache_tag + ".opt-3"):
            with self.subTest(tag=tag):
                unknown = cache.with_name("__init__." + tag + ".pyc")
                unknown.write_bytes(original)
                before = self.tree()
                self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
                self.assertNotEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
                self.assertEqual(self.tree(), before)
                unknown.unlink()

    def test_backup_and_staged_artifact_directories_are_synced_before_publication(self):
        sync = self.adapter.fsync_directory
        atomic = self.adapter.atomic_json
        synced = []
        def observe_sync(path):
            sync(path)
            synced.append(path)
        def observe_record(path, value):
            if path == self.purelib / self.adapter.RECOVERY and value["phase"] == "backup-complete":
                backup = Path(value["backup"])
                self.assertIn(backup / "artifacts", synced)
                self.assertIn(backup, synced)
                self.assertIn(self.backups, synced)
                self.assertLess(synced.index(backup / "artifacts"), len(synced) - 1)
                self.assertEqual(synced[-1], self.backups)
            return atomic(path, value)
        with patch.object(self.adapter, "fsync_directory", side_effect=observe_sync), \
                patch.object(self.adapter, "atomic_json", side_effect=observe_record):
            self.assertEqual(self.call("apply", ["--apply"])[0], 0)

    def test_backup_parent_sync_failure_never_starts_runtime_mutation(self):
        sync = self.adapter.fsync_directory
        def fail_parent(path):
            if path == self.backups:
                raise OSError("backup parent sync failed")
            return sync(path)
        package_before = self.adapter.fingerprint(self.purelib / "t3_fleet")
        with patch.object(self.adapter, "fsync_directory", side_effect=fail_parent):
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual(self.adapter.fingerprint(self.purelib / "t3_fleet"), package_before)
        self.assertFalse(self.hash_record.exists())
        self.assertEqual(json.loads((self.purelib / self.adapter.RECOVERY).read_text())["phase"], "preparing")
        self.assertTrue((self.transaction() / "inventory.json").exists())

    def test_rollback_retries_after_first_staged_file_and_between_tree_swaps(self):
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        backup = self.transaction()
        saved = self.adapter.fingerprint(backup)
        write = self.adapter.write_private
        def interrupted_copy(path, data):
            write(path, data)
            if path.name == "__init__.py" and not path.is_relative_to(backup):
                raise KeyboardInterrupt()
        with patch.object(self.adapter, "write_private", side_effect=interrupted_copy):
            with self.assertRaises(KeyboardInterrupt):
                self.call("rollback", ["--backup", str(backup), "--apply"])
        # Readers still see a complete public tree during interrupted staging.
        self.assertTrue((self.purelib / "t3_fleet/agent.py").exists())
        replace = self.adapter.os.replace
        def interrupted_swap(source, destination):
            replace(source, destination)
            if Path(source) == self.purelib / "t3_fleet":
                raise KeyboardInterrupt()
        with patch.object(self.adapter.os, "replace", side_effect=interrupted_swap):
            with self.assertRaises(KeyboardInterrupt):
                self.call("rollback", ["--backup", str(backup), "--apply"])
        self.assertEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
        self.assertTrue((self.purelib / "t3_fleet/stale.py").exists())
        self.assertEqual(self.adapter.fingerprint(backup), saved)
        self.assertEqual(json.loads(self.call("rollback", ["--backup", str(backup), "--apply"])[1])["status"], "unchanged")

    def test_cleanup_removes_only_owned_trees_and_retains_backup(self):
        unrelated = self.purelib / (".t3-fleet-old-" + "f" * 32)
        unrelated.mkdir()
        (unrelated / "sentinel").write_bytes(b"another transaction")
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual(list(self.purelib.glob(".t3-fleet-old-*")), [unrelated])
        self.assertEqual(self.call("rollback", ["--backup", str(self.transaction()), "--apply"])[0], 0)
        self.assertEqual(list(self.purelib.glob(".t3-fleet-*-*")), [unrelated])
        self.assertTrue((self.transaction() / "inventory.json").exists())

    def test_unknown_partial_restore_stage_is_preserved_for_review(self):
        self.assertEqual(self.call("apply", ["--apply"])[0], 0)
        backup = self.transaction()
        saved = self.adapter.fingerprint(backup)
        write = self.adapter.write_private
        def interrupted_copy(path, data):
            write(path, data)
            if path.name == "__init__.py" and not path.is_relative_to(backup):
                raise KeyboardInterrupt()
        with patch.object(self.adapter, "write_private", side_effect=interrupted_copy):
            with self.assertRaises(KeyboardInterrupt):
                self.call("rollback", ["--backup", str(backup), "--apply"])
        stage = next(self.purelib.glob(".t3-fleet-restore-*"))
        (stage / "unowned").write_bytes(b"keep unknown staging content")
        active = self.adapter.fingerprint(self.purelib / "t3_fleet")
        self.assertNotEqual(self.call("rollback", ["--backup", str(backup), "--apply"])[0], 0)
        self.assertEqual((stage / "unowned").read_bytes(), b"keep unknown staging content")
        self.assertEqual(self.adapter.fingerprint(self.purelib / "t3_fleet"), active)
        self.assertEqual(self.adapter.fingerprint(backup), saved)

    def test_cleanup_preserves_changed_owned_tree(self):
        install = self.adapter.install
        def changed_old(*args):
            install(*args)
            old = next(self.purelib.glob(".t3-fleet-old-*"))
            (old / "unowned").write_bytes(b"preserve changed content")
        with patch.object(self.adapter, "install", side_effect=changed_old):
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        old = next(self.purelib.glob(".t3-fleet-old-*"))
        self.assertEqual((old / "unowned").read_bytes(), b"preserve changed content")
        self.assertTrue((self.transaction() / "inventory.json").exists())

    @unittest.skipUnless(os.name == "posix", "real pip integration uses a disposable POSIX venv")
    def real_venv_fixture(self, *, legacy_version=False):
        self.probe_patch.stop()
        environment = self.root / "disposable-venv"
        # ensurepip honors umask. Create the operator-owned fixture with the
        # same no-group-write contract the adapter requires of existing pip.
        previous_umask = os.umask(0o022)
        try:
            venv.EnvBuilder(with_pip=True, symlinks=True).create(environment)
        finally:
            os.umask(previous_umask)
        for directory in [environment, *(path for path in environment.rglob("*") if path.is_dir() and not path.is_symlink())]:
            directory.chmod(directory.stat().st_mode & ~0o022)
        python = environment / "bin/python"
        purelib = environment / "lib" / f"python{sys.version_info[0]}.{sys.version_info[1]}" / "site-packages"
        scripts = environment / "bin"
        probe = {"executable": str(python), "prefix": str(environment),
                 "base_prefix": sys.base_prefix, "purelib": str(purelib),
                 "scripts": str(scripts), "version": list(sys.version_info[:3]),
                 "user_site_enabled": False, "platform": os.name,
                 "import_origin": str(purelib / "t3_fleet/__init__.py"),
                 "site_roots": [str(purelib)], "pip_candidates": [
                     {"root": str(purelib), "origin": str(purelib / "pip/__init__.py")}],
                 "extension_suffixes": __import__("importlib.machinery").machinery.EXTENSION_SUFFIXES,
                 "pointer_bits": struct.calcsize("P") * 8,
                 "sysconfig_platform": __import__("sysconfig").get_platform()}
        pth = purelib / "distutils-precedence.pth"
        if pth.exists():
            self.assertTrue(pth.is_relative_to(self.root))
            expected = (b"import os; var = 'SETUPTOOLS_USE_DISTUTILS'; enabled = "
                        b"os.environ.get(var, 'local') == 'local'; enabled and "
                        b"__import__('_distutils_hack').add_shim();")
            self.assertIn(pth.read_bytes(), (expected + b"\n", expected + b" \n"))
            pth.unlink()  # Only this test-owned known fixture, never a live environment.
        # Seed old installed distribution using the validated public fixture wheel, offline.
        completed = subprocess.run([str(python), "-I", "-m", "pip", "--isolated", "install",
                                    "--no-index", "--no-deps", "--no-compile", str(self.artifact_dir / WHEEL)],
                                   capture_output=True, env=self.adapter.command_environment())
        self.assertEqual(completed.returncode, 0, completed.stderr.decode())
        if legacy_version:
            # Model an older wheel's metadata and removal footprint so pip
            # must replace two distinct dist-info paths during this fixture.
            old_dist = "t3_fleet-0.1.0.dist-info/"
            (purelib / DIST.rstrip("/")).rename(purelib / old_dist.rstrip("/"))
            metadata = purelib / old_dist / "METADATA"
            metadata.write_bytes(metadata.read_bytes().replace(b"Version: 0.2.0\n", b"Version: 0.1.0\n"))
            record = purelib / old_dist / "RECORD"
            rows = list(csv.reader(io.StringIO(record.read_text().replace(DIST, old_dist))))
            for row in rows:
                path = purelib / row[0]
                if path != record:
                    row[1] = "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(path.read_bytes()).digest()).rstrip(b"=").decode()
                    row[2] = str(path.stat().st_size)
            output = io.StringIO()
            csv.writer(output, lineterminator="\n").writerows(rows)
            record.write_text(output.getvalue())
        (purelib / "t3_fleet/stale.py").write_text("legacy extra\n")
        replacements = {"--layout": "venv", "--python": str(python), "--prefix": str(environment),
                        "--purelib": str(purelib), "--scripts": str(scripts),
                        "--fleet-launcher": str(scripts / "t3-fleet"),
                        "--agent-launcher": str(scripts / "t3-fleet-agent")}
        for key, value in replacements.items():
            self.argv[self.argv.index(key) + 1] = value
        self.probe.clear()
        self.probe.update(probe)
        self.anchor.update(modeled_anchor(python, self.probe))
        index = self.argv.index("--source-root")
        del self.argv[index:index + 2]
        return purelib, scripts, python

    @staticmethod
    def set_distlib_template(purelib: Path, template: str) -> Path:
        scripts = purelib / "pip/_vendor/distlib/scripts.py"
        source = scripts.read_text(encoding="utf-8")
        marker = "SCRIPT_TEMPLATE = r'''"
        start = source.index(marker) + len(marker)
        end = source.index("'''", start)
        scripts.write_text(source[:start] + template + source[end:], encoding="utf-8")
        return scripts

    @staticmethod
    def removesuffix_template() -> str:
        return ("# -*- coding: utf-8 -*-\n"
                "import sys\n"
                "from %(module)s import %(import_name)s\n"
                "if __name__ == '__main__':\n"
                "    sys.argv[0] = sys.argv[0].removesuffix('.exe')\n"
                "    sys.exit(%(func)s())\n")

    @unittest.skipUnless(os.name == "posix", "real pip launcher generation uses a disposable POSIX venv")
    def test_real_pip_new_generator_accepts_old_launchers_apply_repeat_and_rollback(self):
        purelib, scripts, python = self.real_venv_fixture()
        launchers = [scripts / "t3-fleet", scripts / "t3-fleet-agent"]
        original_launchers = {path: path.read_bytes() for path in launchers}
        self.set_distlib_template(purelib, self.removesuffix_template())
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        for path in launchers:
            self.assertIn(b"sys.argv[0].removesuffix('.exe')", path.read_bytes())
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")
        self.assertEqual(len(list(self.backups.iterdir())), 1)
        code, output = self.call("rollback", ["--backup", str(self.transaction()), "--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual({path: path.read_bytes() for path in launchers}, original_launchers)

    @unittest.skipUnless(os.name == "posix", "real pip launcher generation uses a disposable POSIX venv")
    def test_real_pip_unknown_generator_refuses_before_mutation(self):
        purelib, scripts, python = self.real_venv_fixture()
        unknown = self.removesuffix_template().replace(
            "sys.argv[0].removesuffix('.exe')", "sys.argv[0].removeprefix('unexpected')")
        self.set_distlib_template(purelib, unknown)
        before = self.tree()
        code, output = self.call("apply", ["--apply"])
        self.assertNotEqual(code, 0, output)
        self.assertEqual(json.loads(output)["code"], "layout-invalid")
        self.assertEqual(self.tree(), before)
        self.assertFalse(list(self.backups.iterdir()))
        self.assertFalse((purelib / self.adapter.RECOVERY).exists())

    @unittest.skipUnless(os.name == "posix", "real pip launcher generation uses a disposable POSIX venv")
    def test_real_pip_unknown_placeholder_refuses_before_mutation(self):
        purelib, scripts, python = self.real_venv_fixture()
        template = "# %(missing)s\n" + self.removesuffix_template()
        self.set_distlib_template(purelib, template)
        before = self.tree()
        code, output = self.call("apply", ["--apply"])
        self.assertNotEqual(code, 0, output)
        self.assertEqual(json.loads(output)["code"], "layout-invalid")
        self.assertEqual(self.tree(), before)
        self.assertFalse(list(self.backups.iterdir()))
        self.assertFalse((purelib / self.adapter.RECOVERY).exists())

    @unittest.skipUnless(os.name == "posix", "real pip integration uses a disposable POSIX venv")
    def test_real_offline_venv_install_and_runtime_only_rollback(self):
        purelib, scripts, python = self.real_venv_fixture()
        previous_umask = os.umask(0)
        try:
            code, output = self.call("apply", ["--apply"])
        finally:
            os.umask(previous_umask)
        if code != 0:
            self.fail(f"{output}\nlayout diagnostic: {self.layout_diagnostic()}")
        self.assertTrue(all(not path.stat().st_mode & 0o022
                            for path in [purelib / "t3_fleet", *(purelib / "t3_fleet").rglob("*"),
                                         purelib / DIST.rstrip("/"), *(purelib / DIST.rstrip("/")).rglob("*"),
                                         scripts / "t3-fleet", scripts / "t3-fleet-agent"]))
        self.assertFalse((purelib / "t3_fleet/stale.py").exists())
        self.assertFalse(list((purelib / "t3_fleet").rglob("*.pyc")))
        imported = subprocess.run([str(python), "-c", "import t3_fleet"], cwd=self.root,
                                  env={"PATH": os.environ["PATH"]}, capture_output=True)
        self.assertEqual(imported.returncode, 0, imported.stderr)
        code, output = self.call("apply", ["--apply"])
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["status"], "unchanged")
        self.assertEqual(len(list(self.backups.iterdir())), 1)
        self.assertEqual(self.call("rollback", ["--backup", str(self.transaction()), "--apply"])[0], 0)
        self.assertEqual((purelib / "t3_fleet/stale.py").read_text(), "legacy extra\n")

    @unittest.skipUnless(os.name == "posix", "real console precedence uses a disposable POSIX venv")
    def test_real_console_shadow_forms_and_startup_overrides_refuse_without_writes(self):
        purelib, scripts, python = self.real_venv_fixture()
        import importlib.machinery
        marker = self.root / "shadow-executed"
        unrelated = scripts / "unrelated.py"
        unrelated.write_text("# keep unrelated file\n")
        for suffix in (".py", ".pyc", *importlib.machinery.EXTENSION_SUFFIXES):
            path = scripts / ("t3_fleet" + suffix)
            path.write_text(f"from pathlib import Path\nPath({str(marker)!r}).touch()\n")
            if suffix == ".py":
                # Control: the real console script actually resolves this
                # inert fixture before purelib. Adapter inspection must not.
                launched = subprocess.run([str(python), "-B", str(scripts / "t3-fleet")],
                                          env={"PATH": os.environ["PATH"]}, cwd=self.root,
                                          capture_output=True)
                self.assertNotEqual(launched.returncode, 0)
                self.assertTrue(marker.exists())
                marker.unlink()
            before = self.tree()
            code, output = self.call("apply", ["--apply"])
            self.assertNotEqual(code, 0, (suffix, output))
            self.assertEqual(self.tree(), before)
            self.assertFalse(marker.exists())
            path.unlink()
        package = scripts / "t3_fleet"
        package.mkdir()
        (package / "__init__.py").write_bytes(b"raise RuntimeError('shadow package')\n")
        before = self.tree()
        self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
        self.assertEqual(self.tree(), before)
        shutil.rmtree(package)
        for name in ("sitecustomize.py", "sitecustomize.pyc", "usercustomize.py"):
            path = purelib / name
            path.write_bytes(b"raise RuntimeError('startup must not execute')\n")
            before = self.tree()
            self.assertNotEqual(self.call("apply", ["--apply"])[0], 0, name)
            self.assertEqual(self.tree(), before)
            path.unlink()
        self.assertEqual(unrelated.read_text(), "# keep unrelated file\n")

    @unittest.skipUnless(os.name == "posix", "real pip crash recovery uses a disposable POSIX venv")
    def test_real_pip_complete_target_is_recoverable_at_each_finalization_crash(self):
        purelib, scripts, python = self.real_venv_fixture(legacy_version=True)
        for edge in ("pip", "verification", "identity", "final-record"):
            with self.subTest(edge=edge):
                install, verify, atomic = self.adapter.install, self.adapter.verify_installed, self.adapter.atomic_json
                def after_install(*args):
                    install(*args)
                    raise KeyboardInterrupt()
                def after_verify(*args):
                    verify(*args)
                    raise KeyboardInterrupt()
                def at_record(path, value):
                    if edge == "final-record" and path == purelib / self.adapter.RECOVERY and value["phase"] == "verified":
                        raise KeyboardInterrupt()
                    atomic(path, value)
                    if edge == "identity" and path == self.hash_record:
                        raise KeyboardInterrupt()
                with contextlib.ExitStack() as stack:
                    if edge == "pip":
                        stack.enter_context(patch.object(self.adapter, "install", side_effect=after_install))
                    if edge == "verification":
                        stack.enter_context(patch.object(self.adapter, "verify_installed", side_effect=after_verify))
                    stack.enter_context(patch.object(self.adapter, "atomic_json", side_effect=at_record))
                    with self.assertRaises(KeyboardInterrupt):
                        code, output = self.call("apply", ["--apply"])
                        self.fail(f"{output}\nlayout diagnostic: {self.layout_diagnostic()}")
                record = json.loads((purelib / self.adapter.RECOVERY).read_text())
                self.assertEqual(record["phase"], "mutation-started")
                self.assertNotIn("after", record)
                self.assertFalse((purelib / "t3_fleet-0.1.0.dist-info").exists())
                self.assertTrue((purelib / DIST.rstrip("/")).exists())
                backup = Path(record["backup"])
                before = self.adapter.fingerprint(backup)
                self.assertNotEqual(self.call("apply", ["--apply"])[0], 0)
                code, output = self.call("rollback", ["--backup", str(backup), "--apply"])
                self.assertEqual(code, 0, output)
                self.assertEqual((purelib / "t3_fleet/stale.py").read_text(), "legacy extra\n")
                self.assertTrue((purelib / "t3_fleet-0.1.0.dist-info").exists())
                self.assertFalse((purelib / DIST.rstrip("/")).exists())
                self.assertEqual(self.adapter.fingerprint(backup), before)

    @unittest.skipUnless(os.name == "posix", "real pip recovery uses a disposable POSIX venv")
    def test_abrupt_pip_target_with_self_consistent_unknown_metadata_is_preserved(self):
        purelib, scripts, python = self.real_venv_fixture()
        install = self.adapter.install
        def after_install(*args):
            install(*args)
            raise KeyboardInterrupt()
        with patch.object(self.adapter, "install", side_effect=after_install):
            with self.assertRaises(KeyboardInterrupt):
                code, output = self.call("apply", ["--apply"])
                self.fail(f"{output}\nlayout diagnostic: {self.layout_diagnostic()}")
        record = json.loads((purelib / self.adapter.RECOVERY).read_text())
        dist = purelib / DIST.rstrip("/")
        original_record = (dist / "RECORD").read_text()
        for name, data in (("INSTALLER", b"unrecognized installer\n"),
                           ("direct_url.json", b"[]"),
                           ("direct_url.json", b'{"archive_info": []}')):
            with self.subTest(name=name, data=data):
                path = dist / name
                original = path.read_bytes()
                path.write_bytes(data)
                rows = list(csv.reader(io.StringIO(original_record)))
                for row in rows:
                    if row[0] == DIST + name:
                        row[1] = "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
                        row[2] = str(len(data))
                output = io.StringIO()
                csv.writer(output, lineterminator="\n").writerows(rows)
                (dist / "RECORD").write_text(output.getvalue())
                before = self.tree()
                try:
                    self.assertNotEqual(self.call("rollback", ["--backup", record["backup"], "--apply"])[0], 0)
                    self.assertEqual(self.tree(), before)
                finally:
                    path.write_bytes(original)
                    (dist / "RECORD").write_text(original_record)


if __name__ == "__main__":
    unittest.main()
