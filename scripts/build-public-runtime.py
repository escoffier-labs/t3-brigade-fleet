#!/usr/bin/env python3
"""Build or verify a pinned public wheel without installing it.

The approved pin is supplied explicitly with ``--pin``. Authenticated source
Git objects, not wheel metadata, define the package surface and module
allowlist, so the same builder verifies both the current public package and
older reviewed pins that remain part of the public history.
"""
from __future__ import annotations

import argparse
import base64
from collections import Counter
import csv
from email.parser import Parser
import hashlib
import io
import json
import lzma
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
import zlib


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "https://github.com/escoffier-labs/t3-brigade-fleet.git"
MANIFEST = "public-runtime-manifest.json"
MAX_BYTES = 64 * 1024 * 1024
PACKAGE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+!-]*")
GIT_SHA = re.compile(r"[0-9a-f]{40}")


def normalized_name(value: str) -> str:
    return re.sub(r"[-_.]+", "_", value)


def wheel_name(pin: dict) -> str:
    return f"{normalized_name(pin['package']['name'])}-{pin['package']['version']}-py3-none-any.whl"


def dist_dir(pin: dict) -> str:
    return f"{normalized_name(pin['package']['name'])}-{pin['package']['version']}.dist-info/"


def validate_pin(pin: dict) -> dict:
    if (not isinstance(pin, dict)
            or set(pin) != {"schema", "repository", "revision", "tree", "package"}
            or type(pin.get("schema")) is not int or pin["schema"] != 1
            or pin.get("repository") != REPOSITORY):
        raise ValueError("invalid public pin repository or schema")
    for key in ("revision", "tree"):
        if not isinstance(pin[key], str) or not GIT_SHA.fullmatch(pin[key]):
            raise ValueError(f"public pin {key} must be a full lowercase Git SHA")
    package = pin.get("package")
    if (not isinstance(package, dict)
            or set(package) != {"name", "version", "requires_python",
                                "dependencies", "console_scripts"}
            or not isinstance(package["name"], str) or not PACKAGE_NAME.fullmatch(package["name"])
            or not isinstance(package["version"], str) or not VERSION.fullmatch(package["version"])
            or not isinstance(package["requires_python"], str)
            or not isinstance(package["dependencies"], list)
            or any(not isinstance(value, str) for value in package["dependencies"])
            or not isinstance(package["console_scripts"], dict)
            or not package["console_scripts"]
            or any(not isinstance(key, str) or not key or not isinstance(value, str) or not value
                   for key, value in package["console_scripts"].items())):
        raise ValueError("invalid public pin package contract")
    return pin


def load_pin(path: Path) -> dict:
    try:
        pin = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        raise ValueError("invalid public pin") from None
    return validate_pin(pin)


def package_contract(source_files: dict[str, bytes]) -> dict:
    try:
        project = tomllib.loads(source_files["pyproject.toml"].decode("utf-8"))["project"]
    except (KeyError, UnicodeError, tomllib.TOMLDecodeError):
        raise ValueError("public source package contract is unreadable") from None
    if (not isinstance(project, dict)
            or set(project) != {"name", "version", "description", "readme", "requires-python",
                                "license", "authors", "keywords", "classifiers", "dependencies",
                                "scripts", "urls"}
            or not isinstance(project.get("scripts"), dict)):
        raise ValueError("unsupported public source metadata contract")
    return {
        "name": project.get("name"),
        "version": project.get("version"),
        "requires_python": project.get("requires-python"),
        "dependencies": project.get("dependencies"),
        "console_scripts": project.get("scripts"),
    }


def source_modules(source_files: dict[str, bytes]) -> dict[str, bytes]:
    """Return the authenticated Git-derived module allowlist."""
    prefix = "src/t3_fleet/"
    modules = {}
    for name, content in source_files.items():
        if not name.startswith(prefix):
            continue
        relative = name[len(prefix):]
        if "/" in relative or not relative.endswith(".py") or not safe_name(relative):
            raise ValueError("unsupported public source module path")
        if not isinstance(content, bytes):
            raise ValueError("public source module content is invalid")
        modules[relative] = content
    if "__init__.py" not in modules:
        raise ValueError("public source module allowlist is incomplete")
    return modules


def module_names(source_files: dict[str, bytes]) -> set[str]:
    return {"t3_fleet/" + name for name in source_modules(source_files)}


def validate_source_contract(pin: dict, source_files: dict[str, bytes]) -> dict:
    validate_pin(pin)
    if package_contract(source_files) != pin["package"]:
        raise ValueError("public source package contract mismatch")
    return source_modules(source_files)


def command(argv: list[str], home: Path, *, cwd: Path | None = None,
            git_allow_protocol: str = "") -> bytes:
    # Do not inherit Git overrides, pip config, credentials, or arbitrary Python paths.
    environment = {key: os.environ[key] for key in ("PATH", "SYSTEMROOT", "WINDIR", "PATHEXT") if key in os.environ}
    environment.update({
        "HOME": str(home), "USERPROFILE": str(home), "XDG_CONFIG_HOME": str(home),
        "TMPDIR": str(home), "TEMP": str(home), "TMP": str(home),
        "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_TERMINAL_PROMPT": "0", "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_ALLOW_PROTOCOL": git_allow_protocol,
        "PIP_CONFIG_FILE": os.devnull, "PIP_NO_INDEX": "1",
        "PIP_DISABLE_PIP_VERSION_CHECK": "1", "SOURCE_DATE_EPOCH": "1704067200",
    })
    try:
        result = subprocess.run(argv, cwd=cwd, env=environment, capture_output=True,
                                timeout=180, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise ValueError("public source/build tool unavailable or timed out") from None
    if result.returncode:
        raise ValueError("public source/build command failed")
    return result.stdout


def git(source: Path, home: Path, *args: str, allow_https: bool = False) -> bytes:
    return command(["git", "--no-optional-locks", "-c", "core.hooksPath=" + os.devnull,
                    "-c", "core.fsmonitor=false", "-c", "protocol.allow=never",
                    "-c", "protocol.https.allow=always", "-C", str(source), *args], home,
                   git_allow_protocol="https" if allow_https else "")


def safe_name(name: str) -> bool:
    return (bool(name) and "\\" not in name and ":" not in name and "\x00" not in name
            and not name.startswith("/") and all(part not in ("", ".", "..") for part in name.split("/")))


def extract_archive(data: bytes, destination: Path) -> dict[str, bytes]:
    if len(data) > MAX_BYTES:
        raise ValueError("public source archive too large")
    files = {}
    names = set()
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        members = archive.getmembers()
        if len(members) > 5000 or sum(member.size for member in members) > MAX_BYTES:
            raise ValueError("public source archive too large")
        for member in members:
            name = member.name.rstrip("/") if member.isdir() else member.name
            if not safe_name(name) or name in names or not (member.isfile() or member.isdir()):
                raise ValueError("unsafe public source archive member")
            names.add(name)
            if member.isfile():
                files[name] = archive.extractfile(member).read()
    # Validate all names and types before creating anything. Never use extractall.
    destination.mkdir()
    for name, content in files.items():
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as handle:
            handle.write(content)
    return files


def export_source(pin: dict, source: Path | None, destination: Path) -> dict[str, bytes]:
    with tempfile.TemporaryDirectory(prefix="public-runtime-git-") as temporary:
        home = Path(temporary)
        if source is None:
            source = home / "repository"
            source.mkdir()
            git(source, home, "init", "--bare", "-q")
            git(source, home, "remote", "add", "origin", pin["repository"])
            # Only this deliberate fetch into our temporary repository may use transport.
            git(source, home, "fetch", "--no-tags", "--depth=1", "origin", pin["revision"], allow_https=True)
            git(source, home, "update-ref", "HEAD", pin["revision"])
        origin = git(source, home, "config", "--local", "--get", "remote.origin.url").decode().strip()
        if origin != pin["repository"]:
            raise ValueError("public source repository mismatch")
        revision = git(source, home, "rev-parse", "--verify", "HEAD^{commit}").decode().strip()
        tree = git(source, home, "rev-parse", "--verify", pin["revision"] + "^{tree}").decode().strip()
        if revision != pin["revision"] or tree != pin["tree"]:
            raise ValueError("public source revision/tree mismatch")
        entries = git(source, home, "ls-tree", "-rz", "--full-tree", pin["revision"])
        blobs = {}
        for entry in entries.split(b"\0"):
            if not entry:
                continue
            header, name_bytes = entry.split(b"\t", 1)
            mode, kind, oid = header.decode("ascii").split()
            name = name_bytes.decode("utf-8")
            if mode not in {"100644", "100755"} or kind != "blob" or not safe_name(name):
                raise ValueError("unsafe public source tree member")
            blobs[name] = oid
        files = extract_archive(git(source, home, "archive", "--format=tar", pin["revision"]), destination)
        if files.keys() != blobs.keys():
            raise ValueError("public source archive differs from tracked tree")
        for name, content in files.items():
            blob = b"blob " + str(len(content)).encode("ascii") + b"\0" + content
            if hashlib.sha1(blob).hexdigest() != blobs[name]:
                raise ValueError("public source content differs from pinned tree")
        validate_source_contract(pin, files)
        return files


def check_metadata(
    data: bytes, headers: list[tuple[str, str]], body: bytes, *, license_text: str | None = None,
) -> None:
    # Support the exact LF/folding syntax emitted by the documented toolchain.
    # Parsing alone discards comments, malformed lines and some whitespace.
    head, separator, payload = data.partition(b"\n\n")
    parsed = Parser().parsestr((head + b"\n\n").decode("utf-8"))
    items: list[bytes] = []
    for line in head.split(b"\n"):
        if line.startswith((b" ", b"\t")):
            if not items:
                raise ValueError("public wheel metadata mismatch")
            items[-1] += b"\n" + line
        else:
            items.append(line)
    expected = Counter(f"{key}: {value}".encode("utf-8") for key, value in headers)
    actual = Counter(items)
    matches = actual == expected
    if not matches and license_text is not None:
        # Some setuptools/Python combinations strip the leading header value
        # whitespace. Both forms must come from the exact pinned license bytes.
        original = "License: " + license_text.replace("\n", "\n        ")
        trimmed = "License: " + license_text.lstrip().replace("\n", "\n        ")
        expected[original.encode("utf-8")] -= 1
        expected[trimmed.encode("utf-8")] += 1
        matches = actual == expected
    if (not separator or parsed.defects or parsed.is_multipart()
            or not matches or payload != body):
        raise ValueError("public wheel metadata mismatch")


def inspect_wheel(wheel: Path, pin: dict, source_files: dict[str, bytes]) -> dict:
    validate_source_contract(pin, source_files)
    module_map = {"t3_fleet/" + name: data for name, data in source_modules(source_files).items()}
    required_modules = set(module_map)
    dist = dist_dir(pin)
    expected_wheel = wheel_name(pin)
    if wheel.name != expected_wheel or wheel.is_symlink() or not wheel.is_file() or wheel.stat().st_size > MAX_BYTES:
        raise ValueError("invalid public wheel artifact")
    with zipfile.ZipFile(wheel) as archive:
        entries = archive.infolist()
        names = [entry.filename for entry in entries]
        if (len(names) != len(set(names)) or len(names) > 5000
                or sum(entry.file_size for entry in entries) > MAX_BYTES):
            raise ValueError("invalid public wheel inventory")
        for entry in entries:
            mode = entry.external_attr >> 16
            if not safe_name(entry.filename) or entry.is_dir() or stat.S_IFMT(mode) not in (0, stat.S_IFREG):
                raise ValueError("unsafe public wheel member")
        required_info = {dist + name for name in ("METADATA", "WHEEL", "entry_points.txt", "RECORD", "top_level.txt")}
        allowed = required_modules | required_info | {dist + "LICENSE", dist + "licenses/LICENSE"}
        if not (required_modules | required_info) <= set(names) or not set(names) <= allowed:
            raise ValueError("public wheel required member set mismatch")
        try:
            contents = {name: archive.read(name) for name in names}
        except (zlib.error, lzma.LZMAError, OSError, EOFError):
            raise ValueError("invalid public wheel compressed data") from None
    if any(contents[name] != data for name, data in module_map.items()):
        raise ValueError("public wheel source content mismatch")
    project = tomllib.loads(source_files["pyproject.toml"].decode("utf-8"))["project"]
    if (project["readme"] != "README.md" or project["license"] != {"file": "LICENSE"}
            or any(set(author) != {"name"} for author in project["authors"])
            or package_contract(source_files) != pin["package"]):
        raise ValueError("unsupported public source metadata contract")
    license_bytes = source_files["LICENSE"]
    headers = [
        ("Metadata-Version", "2.1"), ("Name", project["name"]), ("Version", project["version"]),
        ("Summary", project["description"]),
        ("Author", ", ".join(author["name"] for author in project["authors"])),
        ("License", license_bytes.decode("utf-8").replace("\n", "\n        ")),
        *[("Project-URL", f"{key}, {value}") for key, value in project["urls"].items()],
        ("Keywords", ",".join(project["keywords"])),
        *[("Classifier", value) for value in project["classifiers"]],
        ("Requires-Python", project["requires-python"]),
        ("Description-Content-Type", "text/markdown"), ("License-File", "LICENSE"),
    ]
    readme = source_files["README.md"]
    check_metadata(contents[dist + "METADATA"], headers, readme, license_text=license_bytes.decode("utf-8"))
    license_names = {dist + "LICENSE", dist + "licenses/LICENSE"} & contents.keys()
    if not license_names or any(contents[name] != license_bytes for name in license_names):
        raise ValueError("public wheel license content mismatch")
    check_metadata(contents[dist + "WHEEL"], [
        ("Wheel-Version", "1.0"), ("Generator", "bdist_wheel (0.42.0)"),
        ("Root-Is-Purelib", "true"), ("Tag", "py3-none-any"),
    ], b"")
    entrypoints = "[console_scripts]\n" + "".join(
        f"{key} = {value}\n" for key, value in sorted(pin["package"]["console_scripts"].items()))
    if contents[dist + "entry_points.txt"] != entrypoints.encode("utf-8"):
        raise ValueError("public wheel console entrypoints mismatch")
    if contents[dist + "top_level.txt"] != (normalized_name(pin["package"]["name"]) + "\n").encode("utf-8"):
        raise ValueError("public wheel top-level package mismatch")
    try:
        rows = list(csv.reader(io.StringIO(contents[dist + "RECORD"].decode("utf-8"))))
    except csv.Error:
        raise ValueError("invalid public wheel RECORD CSV") from None
    if any(len(row) != 3 for row in rows) or len(rows) != len(contents) or {row[0] for row in rows} != contents.keys():
        raise ValueError("public wheel RECORD inventory mismatch")
    canonical_record = io.StringIO(newline="")
    csv.writer(canonical_record, lineterminator="\n").writerows(rows)
    record_bytes = canonical_record.getvalue().encode("utf-8")
    if contents[dist + "RECORD"] not in (record_bytes, record_bytes.replace(b"\n", b"\r\n")):
        raise ValueError("public wheel RECORD format mismatch")
    for name, digest, size in rows:
        if name == dist + "RECORD":
            if digest or size:
                raise ValueError("public wheel RECORD self hash mismatch")
        else:
            expected = base64.urlsafe_b64encode(hashlib.sha256(contents[name]).digest()).rstrip(b"=").decode("ascii")
            if digest != "sha256=" + expected or size != str(len(contents[name])):
                raise ValueError("public wheel RECORD integrity mismatch")
    return {
        "schema": 1, "repository": pin["repository"], "revision": pin["revision"], "tree": pin["tree"],
        "package": pin["package"],
        "wheel": {"filename": wheel.name, "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest()},
        "modules": {name: hashlib.sha256(data).hexdigest() for name, data in sorted(module_map.items())},
    }


def verify_artifacts(output: Path, pin: dict, source_files: dict[str, bytes]) -> dict:
    expected_names = {MANIFEST, wheel_name(pin)}
    if output.is_symlink() or not output.is_dir() or {path.name for path in output.iterdir()} != expected_names:
        raise ValueError("output directory is not a complete public runtime artifact set")
    manifest_path = output / MANIFEST
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError("invalid public artifact manifest")
    try:
        expected = inspect_wheel(output / wheel_name(pin), pin, source_files)
    except zipfile.BadZipFile:
        raise ValueError("invalid public wheel ZIP") from None
    if json.loads(manifest_path.read_text(encoding="utf-8")) != expected:
        raise ValueError("public artifact manifest integrity mismatch")
    return expected


def publish_artifacts(output: Path, wheel: Path, pin: dict, source_files: dict[str, bytes]) -> dict:
    if output.exists() or output.is_symlink():
        return verify_artifacts(output, pin, source_files)
    manifest = inspect_wheel(wheel, pin, source_files)
    # Exclusive directory creation and exclusive files never replace an existing artifact.
    output.mkdir()
    with (output / wheel_name(pin)).open("xb") as handle:
        handle.write(wheel.read_bytes())
    with (output / MANIFEST).open("x", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return verify_artifacts(output, pin, source_files)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pin", type=Path, required=True,
                        help="explicit approved pin outside the checkout; no default pin is shipped")
    parser.add_argument("--source", type=Path, help="local public Git checkout at the exact pin (no network)")
    parser.add_argument("--output-dir", type=Path, required=True, help="explicit artifact directory outside checkouts")
    parser.add_argument("--verify-only", action="store_true", help="verify existing artifacts against pinned Git objects without building")
    args = parser.parse_args(argv)
    try:
        pin = load_pin(args.pin)
        output = args.output_dir.absolute()
        for checkout in (ROOT, args.source):
            if checkout is not None and output.resolve().is_relative_to(checkout.resolve()):
                raise ValueError("output directory must be outside source checkouts")
        if any((ancestor / ".git").exists() for ancestor in (output.resolve(), *output.resolve().parents)):
            raise ValueError("output directory must be outside Git checkouts")
        if output.is_symlink():
            raise ValueError("output directory cannot be a symlink")
        if not output.parent.is_dir():
            raise ValueError("output parent directory must already exist")
        with tempfile.TemporaryDirectory(prefix="public-runtime-build-") as temporary:
            temporary_path = Path(temporary)
            source = temporary_path / "source"
            source_files = export_source(pin, args.source, source)
            if args.verify_only or output.exists():
                manifest = verify_artifacts(output, pin, source_files)
            else:
                wheels = temporary_path / "wheels"
                wheels.mkdir()
                command([sys.executable, "-m", "pip", "wheel", "--no-deps", "--no-build-isolation",
                         "--no-index", "--disable-pip-version-check", "--wheel-dir", str(wheels), str(source)],
                        temporary_path, cwd=temporary_path)
                if {path.name for path in wheels.iterdir()} != {wheel_name(pin)}:
                    raise ValueError("build did not produce exactly the pinned public wheel")
                manifest = publish_artifacts(output, wheels / wheel_name(pin), pin, source_files)
        print(json.dumps({"status": "verified", "manifest": str(output / MANIFEST),
                          "revision": manifest["revision"], "wheel_sha256": manifest["wheel"]["sha256"]}, sort_keys=True))
        return 0
    except (ValueError, OSError, KeyError, TypeError, UnicodeError, RuntimeError, tarfile.TarError,
            zipfile.BadZipFile):
        # Specific validation messages are static and safe. Tool stderr and paths stay private.
        error = sys.exception()
        message = str(error) if type(error) is ValueError else "invalid public runtime source or artifact"
        print("public-runtime: " + message, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
