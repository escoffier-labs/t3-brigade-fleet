#!/usr/bin/env python3
"""Inspect or recoverably install the reviewed public runtime on this machine only."""
from __future__ import annotations

import argparse
import ast
import base64
from contextlib import closing, contextmanager, ExitStack
import csv
import ctypes
from email.parser import BytesParser
import hashlib
import importlib.util
import io
import ipaddress
from importlib.machinery import EXTENSION_SUFFIXES
from importlib.machinery import PathFinder
import json
import marshal
import os
from pathlib import Path
import platform
import re
import runpy
import shlex
import shutil
import sqlite3
import stat
import struct
import subprocess
import sys
import sysconfig
import time
import tomllib
from urllib.parse import urlsplit
import uuid
import zipfile


ROOT = Path(__file__).resolve().parents[1]
# The approved pin is an explicit operator input. No pin is shipped in the
# public history, and no path is inferred from the checkout.
# Trusted repository code is loaded only after the current interpreter anchor
# has been admitted. Nothing imported by the public artifact participates here.
BUILDER = None
LOCK = ".t3-fleet-install.lock"
RECOVERY = ".t3-fleet-install.json"
MAX_PREDECESSOR_EDGES = 16
ANCHOR_OBJECT_LIMIT = 10000
ANCHOR_BYTE_LIMIT = 512 * 1024 * 1024
ANCHOR_TIME_LIMIT = 50
PACKAGE_DEADLINE_POSIX = 150
PACKAGE_DEADLINE_WINDOWS = 600
T3_AUTHORITY = re.compile(r"(?:\[[0-9a-fA-F:]+\]|[0-9.]+)(?::[0-9]+)?")
SHA256 = re.compile(r"[0-9a-f]{64}")
GIT_SHA = re.compile(r"[0-9a-f]{40}")
STABLE_BINDING_FIELDS = {
    "layout", "python", "prefix", "purelib", "scripts", "source_root",
    "fleet_launcher", "agent_launcher", "target_host", "hash_record",
    "backup_parent", "config", "private_file", "payload_file", "wrapper",
    "state_db", "optional_state_db", "selected_target",
}
BINDING_PATH_FIELDS = {
    "python", "prefix", "purelib", "scripts", "source_root", "fleet_launcher",
    "agent_launcher", "hash_record", "backup_parent", "artifact_dir", "public_source",
}
BINDING_LIST_FIELDS = {
    "config", "private_file", "payload_file", "wrapper", "state_db", "optional_state_db",
}
BINDING_FIELDS = STABLE_BINDING_FIELDS | {"artifact_dir", "public_source"}
SCHEMA2_PHASE_FIELDS = {
    "upgrade-intent": set(),
    "mutation-started": set(),
    "verified": {"after"},
    "rollback-started": {"rollback"},
    "predecessor-ready": {"rollback", "rollback_ready_sha256"},
}


def load_builder() -> dict:
    global BUILDER
    if BUILDER is None:
        BUILDER = runpy.run_path(str(ROOT / "scripts/build-public-runtime.py"))
    return BUILDER


def command_environment() -> dict[str, str]:
    environment = {key: os.environ[key] for key in
                   ("PATH", "SYSTEMROOT", "WINDIR", "PATHEXT", "APPDATA", "LOCALAPPDATA")
                   if key in os.environ}
    environment.update({
        "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_TERMINAL_PROMPT": "0", "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_ALLOW_PROTOCOL": "", "GIT_OPTIONAL_LOCKS": "0", "GIT_NO_LAZY_FETCH": "1",
        "PIP_CONFIG_FILE": os.devnull, "PIP_NO_INDEX": "1",
        "PIP_DISABLE_PIP_VERSION_CHECK": "1", "PYTHONDONTWRITEBYTECODE": "1",
    })
    return environment


def run(argv: list[str], *, cwd: Path | None = None) -> bytes:
    try:
        result = subprocess.run(argv, env=command_environment(), cwd=cwd,
                                capture_output=True, timeout=180, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise ValueError("local inspection or installation tool unavailable") from None
    if result.returncode:
        # Never forward subprocess stderr: it can contain private paths or config.
        raise ValueError("local inspection or installation command failed")
    return result.stdout


def is_reparse(info: os.stat_result) -> bool:
    return bool(getattr(info, "st_file_attributes", 0) & 0x400)


def checked_path(value: Path, *, exists: bool = False, interpreter: bool = False) -> Path:
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("all operator paths must be absolute and unambiguous")
    for part in reversed((path, *path.parents)):
        try:
            info = part.lstat()
        except FileNotFoundError:
            continue
        if is_reparse(info) or (stat.S_ISLNK(info.st_mode) and not (interpreter and part == path)):
            raise ValueError("symlink or reparse destination refused")
        if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)
                or (interpreter and part == path and stat.S_ISLNK(info.st_mode))):
            raise ValueError("special filesystem object refused")
    if exists and not path.exists():
        raise ValueError("required operator input is missing")
    return path


def tree_files(path: Path) -> dict[str, Path]:
    checked_path(path)
    if not path.exists():
        return {}
    if not path.is_dir():
        raise ValueError("expected a package or metadata directory")
    files = {}
    for child in path.rglob("*"):
        checked_path(child, exists=True)
        if child.is_file():
            files[child.relative_to(path).as_posix()] = child
    return files


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class WindowsSecurity:
    """Native object creation and handle-bound ACL checks, without identity output."""

    def __init__(self):
        if os.name != "nt":
            raise ValueError("native Windows security is unavailable")
        from ctypes import wintypes as w
        c = ctypes
        self.w = w
        self.k = c.WinDLL("kernel32", use_last_error=True)
        self.a = c.WinDLL("advapi32", use_last_error=True)
        p, d, b, h = c.c_void_p, w.DWORD, w.BOOL, w.HANDLE
        self.p = p
        class Attributes(c.Structure):
            _fields_ = [("length", d), ("descriptor", p), ("inherit", b)]
        class AclInfo(c.Structure):
            _fields_ = [("count", d), ("used", d), ("free", d)]
        class FileInfo(c.Structure):
            _fields_ = [("attributes", d), ("created", w.FILETIME), ("accessed", w.FILETIME),
                        ("written", w.FILETIME), ("volume", d), ("size_high", d),
                        ("size_low", d), ("links", d), ("index_high", d), ("index_low", d)]
        self.Attributes, self.AclInfo, self.FileInfo = Attributes, AclInfo, FileInfo
        signatures = [
            (self.k, "GetCurrentProcess", [], h),
            (self.k, "CloseHandle", [h], b), (self.k, "LocalFree", [p], p),
            (self.k, "CreateFileW", [w.LPCWSTR, d, d, c.POINTER(Attributes), d, d, h], h),
            (self.k, "CreateDirectoryW", [w.LPCWSTR, c.POINTER(Attributes)], b),
            (self.k, "GetFileInformationByHandle", [h, c.POINTER(FileInfo)], b),
            (self.k, "WriteFile", [h, p, d, c.POINTER(d), p], b),
            (self.k, "FlushFileBuffers", [h], b),
            (self.a, "OpenProcessToken", [h, d, c.POINTER(h)], b),
            (self.a, "GetTokenInformation", [h, c.c_int, p, d, c.POINTER(d)], b),
            (self.a, "ConvertSidToStringSidW", [p, c.POINTER(w.LPWSTR)], b),
            (self.a, "ConvertStringSidToSidW", [w.LPCWSTR, c.POINTER(p)], b),
            (self.a, "EqualSid", [p, p], b),
            (self.a, "IsValidSid", [p], b),
            (self.a, "GetLengthSid", [p], d),
            (self.a, "ConvertStringSecurityDescriptorToSecurityDescriptorW",
             [w.LPCWSTR, d, c.POINTER(p), c.POINTER(d)], b),
            (self.a, "GetSecurityInfo", [h, c.c_int, d, c.POINTER(p), c.POINTER(p),
                                         c.POINTER(p), c.POINTER(p), c.POINTER(p)], d),
            (self.a, "GetSecurityDescriptorControl", [p, c.POINTER(w.WORD), c.POINTER(d)], b),
            (self.a, "GetSecurityDescriptorOwner", [p, c.POINTER(p), c.POINTER(b)], b),
            (self.a, "GetSecurityDescriptorGroup", [p, c.POINTER(p), c.POINTER(b)], b),
            (self.a, "GetSecurityDescriptorDacl", [p, c.POINTER(b), c.POINTER(p), c.POINTER(b)], b),
            (self.a, "GetSecurityDescriptorLength", [p], d),
            (self.a, "IsValidSecurityDescriptor", [p], b),
            (self.a, "IsValidAcl", [p], b),
            (self.a, "GetAclInformation", [p, p, d, c.c_int], b),
            (self.a, "GetAce", [p, d, c.POINTER(p)], b),
        ]
        for library, name, args, result in signatures:
            function = getattr(library, name)
            function.argtypes, function.restype = args, result
        self.allocated = []
        token = h()
        self.ok(self.a.OpenProcessToken(self.k.GetCurrentProcess(), 8, c.byref(token)))
        try:
            size = d()
            self.a.GetTokenInformation(token, 1, None, 0, c.byref(size))
            if not 0 < size.value < 65536:
                raise ValueError("Windows token information unavailable")
            self.token_buffer = c.create_string_buffer(size.value)
            self.ok(self.a.GetTokenInformation(token, 1, self.token_buffer, size, c.byref(size)))
            self.me = c.cast(self.token_buffer, c.POINTER(p))[0]
        finally:
            self.k.CloseHandle(token)
        try:
            self.system = self.sid("S-1-5-18")
            self.admins = self.sid("S-1-5-32-544")
            # Windows OS ancestors can be owned by the servicing authority.
            # This SID is never admitted for pip objects or private records.
            self.servicing = self.sid("S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464")
        except BaseException:
            self.close()
            raise

    def ok(self, value):
        if not value:
            raise ValueError("Windows security operation failed")

    def sid(self, text):
        value = self.p()
        self.ok(self.a.ConvertStringSidToSidW(text, ctypes.byref(value)))
        self.allocated.append(value)
        return value

    def close(self):
        for value in self.allocated:
            self.k.LocalFree(value)
        self.allocated.clear()

    def private_descriptor(self, *, directory=False):
        text = self.w.LPWSTR()
        self.ok(self.a.ConvertSidToStringSidW(self.me, ctypes.byref(text)))
        try:
            flags = "OICI" if directory else ""
            sddl = "O:" + text.value + "D:P(A;" + flags + ";FA;;;" + text.value + ")(A;" + flags + ";FA;;;SY)"
            descriptor = self.p()
            self.ok(self.a.ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl, 1, ctypes.byref(descriptor), None))
            return descriptor
        finally:
            self.k.LocalFree(ctypes.cast(text, self.p))

    def open(self, path, *, create=False, descriptor=None, exclusive=False, pin=False, share_write=True):
        attributes = self.Attributes(ctypes.sizeof(self.Attributes), descriptor, False)
        handle = self.k.CreateFileW(str(path), 0xC0030000 if create or exclusive else (0x20001 if pin else 0x20000),
                                   0 if create or exclusive else (3 if share_write else 1),
                                   ctypes.byref(attributes) if descriptor else None,
                                   1 if create else 3, 0x02200000, None)
        if handle == self.p(-1).value:
            error = ctypes.get_last_error()
            if create and error in (80, 183):
                raise FileExistsError("Windows destination already exists")
            raise ValueError("Windows object could not be opened safely")
        return handle

    def validate(self, handle, *, private, toolchain=False, ancestor=False, descriptor_text=None):
        owner, dacl, descriptor = self.p(), self.p(), self.p()
        if descriptor_text is None:
            info = self.FileInfo()
            self.ok(self.k.GetFileInformationByHandle(handle, ctypes.byref(info)))
            if info.attributes & 0x400:
                raise ValueError("Windows reparse object refused")
            if self.a.GetSecurityInfo(handle, 1, 5, ctypes.byref(owner), None,
                                      ctypes.byref(dacl), None, ctypes.byref(descriptor)):
                raise ValueError("Windows ACL unavailable")
        else:
            if (not isinstance(descriptor_text, str) or not descriptor_text
                    or "\0" in descriptor_text or len(descriptor_text) > 1024 * 1024):
                raise ValueError("invalid saved execution descriptor")
            self.ok(self.a.ConvertStringSecurityDescriptorToSecurityDescriptorW(
                descriptor_text, 1, ctypes.byref(descriptor), None))
        try:
            if descriptor_text is not None:
                defaulted, present = self.w.BOOL(), self.w.BOOL()
                self.ok(self.a.GetSecurityDescriptorOwner(descriptor, ctypes.byref(owner), ctypes.byref(defaulted)))
                self.ok(self.a.GetSecurityDescriptorDacl(
                    descriptor, ctypes.byref(present), ctypes.byref(dacl), ctypes.byref(defaulted)))
                if not present:
                    raise ValueError("saved execution DACL absent")
            control, revision = self.w.WORD(), self.w.DWORD()
            self.ok(self.a.GetSecurityDescriptorControl(descriptor, ctypes.byref(control), ctypes.byref(revision)))
            trusted = (self.me, self.system, self.admins) if toolchain or ancestor else (self.me, self.system)
            if ancestor:
                trusted += (self.servicing,)
            if not owner or not any(self.a.EqualSid(owner, sid) for sid in
                                    (trusted if toolchain or ancestor else (self.me,))):
                raise ValueError("Windows owner is outside the admitted policy")
            if not dacl or (private and not control.value & 0x1000):
                raise ValueError("Windows private DACL is absent or unprotected")
            acl = self.AclInfo()
            self.ok(self.a.GetAclInformation(dacl, ctypes.byref(acl), ctypes.sizeof(acl), 2))
            grants = []
            for index in range(acl.count):
                ace = self.p()
                self.ok(self.a.GetAce(dacl, index, ctypes.byref(ace)))
                kind = ctypes.c_ubyte.from_address(ace.value).value
                flags = ctypes.c_ubyte.from_address(ace.value + 1).value
                size = self.w.WORD.from_address(ace.value + 2).value
                if kind not in (0, 1) or size < 16 or flags & ~0x1f:
                    raise ValueError("unsupported Windows ACE")
                mask = self.w.DWORD.from_address(ace.value + 4).value
                sid = self.p(ace.value + 8)
                if not self.a.IsValidSid(sid) or self.a.GetLengthSid(sid) > size - 8:
                    raise ValueError("invalid Windows ACE identity")
                if private:
                    if kind != 0 or flags & ~3 or mask != 0x1f01ff:
                        raise ValueError("Windows private grant is not exact")
                    if self.a.EqualSid(sid, self.me):
                        grants.append("owner")
                    elif self.a.EqualSid(sid, self.system):
                        grants.append("system")
                    else:
                        raise ValueError("Windows private foreign grant refused")
                elif kind == 0 and not flags & 8:
                    # An ancestor can permit adding unrelated children. It may
                    # not permit replacing existing children or its own ACL.
                    mutation = 0x50000000 | 0xD0156
                    if ancestor:
                        mutation = 0x50000000 | 0xD0040
                    if mask & mutation and not any(self.a.EqualSid(sid, value) for value in trusted):
                        raise ValueError("Windows untrusted mutation grant refused")
            if private and sorted(grants) != ["owner", "system"]:
                raise ValueError("Windows private grants are incomplete")
        finally:
            self.k.LocalFree(descriptor)

    def object_identity(self, handle):
        """Return handle-bound file identity and raw security fingerprint."""
        info = self.FileInfo()
        self.ok(self.k.GetFileInformationByHandle(handle, ctypes.byref(info)))
        descriptor = self.p()
        if self.a.GetSecurityInfo(handle, 1, 7, None, None, None, None, ctypes.byref(descriptor)):
            raise ValueError("Windows security identity unavailable")
        try:
            length = self.a.GetSecurityDescriptorLength(descriptor)
            if not 0 < length <= 1024 * 1024:
                raise ValueError("Windows security identity exceeded its bound")
            acl = hashlib.sha256(ctypes.string_at(descriptor, length)).hexdigest()
        finally:
            self.k.LocalFree(descriptor)
        return {"volume": info.volume, "index": [info.index_high, info.index_low],
                "links": info.links, "attributes": info.attributes, "acl_sha256": acl}

    def restoration_descriptor(self, text=None, *, handle=None):
        """Validate native structures without changing or normalizing the raw ACL."""
        descriptor = self.p()
        try:
            if handle is not None:
                info = self.FileInfo()
                self.ok(self.k.GetFileInformationByHandle(handle, ctypes.byref(info)))
                if info.attributes & 0x400:
                    raise ValueError("reparse")
                if self.a.GetSecurityInfo(handle, 1, 7, None, None, None, None, ctypes.byref(descriptor)):
                    raise ValueError("unreadable")
            else:
                if not isinstance(text, str) or not text or "\0" in text or len(text) > 1024 * 1024:
                    raise ValueError("invalid descriptor")
                self.ok(self.a.ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    text, 1, ctypes.byref(descriptor), None))
            self.ok(self.a.IsValidSecurityDescriptor(descriptor))
            control, revision = self.w.WORD(), self.w.DWORD()
            self.ok(self.a.GetSecurityDescriptorControl(descriptor, ctypes.byref(control), ctypes.byref(revision)))
            # The existing exact restore setter completes auto-inheritance even
            # on protected DACLs. P-only originals cannot round-trip unchanged.
            if control.value & 0x1500 != 0x1400:
                raise ValueError("unsupported protection or auto-inheritance state")
            for getter in (self.a.GetSecurityDescriptorOwner, self.a.GetSecurityDescriptorGroup):
                sid, defaulted = self.p(), self.w.BOOL()
                self.ok(getter(descriptor, ctypes.byref(sid), ctypes.byref(defaulted)))
                if not sid or not self.a.IsValidSid(sid):
                    raise ValueError("missing identity")
            present, defaulted, dacl = self.w.BOOL(), self.w.BOOL(), self.p()
            self.ok(self.a.GetSecurityDescriptorDacl(
                descriptor, ctypes.byref(present), ctypes.byref(dacl), ctypes.byref(defaulted)))
            if not present or not dacl or not self.a.IsValidAcl(dacl):
                raise ValueError("absent DACL")
            acl = self.AclInfo()
            self.ok(self.a.GetAclInformation(dacl, ctypes.byref(acl), ctypes.sizeof(acl), 2))
            for index in range(acl.count):
                ace = self.p()
                self.ok(self.a.GetAce(dacl, index, ctypes.byref(ace)))
                kind = ctypes.c_ubyte.from_address(ace.value).value
                flags = ctypes.c_ubyte.from_address(ace.value + 1).value
                size = self.w.WORD.from_address(ace.value + 2).value
                if kind not in (0, 1) or flags & ~0x1f or size < 16:
                    raise ValueError("unsupported ACE")
                sid = self.p(ace.value + 8)
                if not self.a.IsValidSid(sid) or self.a.GetLengthSid(sid) != size - 8:
                    raise ValueError("invalid ACE")
        except (ValueError, TypeError, OSError):
            raise ValueError("Windows runtime baseline requires a supported protected DACL") from None
        finally:
            if descriptor:
                self.k.LocalFree(descriptor)

    @contextmanager
    def parents(self, path):
        handles = []
        try:
            for parent in reversed(path.parents):
                nearest = parent == path.parent
                # Rename publication needs a writable containing directory.
                # More distant ancestors are pinned against write and delete.
                handle = self.open(parent, pin=True, share_write=nearest)
                handles.append(handle)
                self.validate(handle, private=False, toolchain=nearest, ancestor=not nearest)
            yield
        finally:
            for handle in reversed(handles):
                self.k.CloseHandle(handle)

    @contextmanager
    def private_object(self, path, *, directory=False, reopen=False):
        created, handle, descriptor = False, None, None
        try:
            with self.parents(path):
                descriptor = self.private_descriptor(directory=directory)
                attributes = self.Attributes(ctypes.sizeof(self.Attributes), descriptor, False)
                try:
                    if directory:
                        if not self.k.CreateDirectoryW(str(path), ctypes.byref(attributes)):
                            if ctypes.get_last_error() in (80, 183):
                                raise FileExistsError("Windows destination already exists")
                            raise ValueError("Windows private directory creation failed")
                        created = True
                        handle = self.open(path)
                    else:
                        try:
                            handle = self.open(path, create=True, descriptor=descriptor)
                            created = True
                        except FileExistsError:
                            if not reopen:
                                raise
                            handle = self.open(path, exclusive=True)
                    self.validate(handle, private=True)
                    info = self.FileInfo()
                    self.ok(self.k.GetFileInformationByHandle(handle, ctypes.byref(info)))
                    if bool(info.attributes & 0x10) != directory:
                        raise ValueError("Windows private object has the wrong type")
                    yield handle
                    self.k.CloseHandle(handle)
                    handle = None
                except BaseException:
                    if handle is not None:
                        self.k.CloseHandle(handle)
                        handle = None
                    if created:
                        if directory:
                            path.rmdir()
                        else:
                            path.unlink()
                    raise
        finally:
            if handle is not None:
                self.k.CloseHandle(handle)
            if descriptor:
                self.k.LocalFree(descriptor)

    def write_chunk(self, handle, data):
        buffer = ctypes.create_string_buffer(data)
        written = self.w.DWORD()
        self.ok(self.k.WriteFile(handle, buffer, len(data), ctypes.byref(written), None))
        return written.value


def windows_acl(path: Path, *, private: bool = True) -> None:
    with closing(WindowsSecurity()) as security:
        with security.parents(path):
            handle = security.open(path)
            try:
                security.validate(handle, private=private)
            finally:
                security.k.CloseHandle(handle)


def safe_parent(path: Path, *, private: bool, windows: bool = False) -> None:
    checked_path(path, exists=True)
    if not path.is_dir():
        raise ValueError("protected parent must already be a directory")
    if windows or os.name == "nt":
        windows_acl(path, private=private)
    else:
        info = path.stat()
        if info.st_uid != os.geteuid() or info.st_mode & (0o077 if private else 0o022):
            raise ValueError("parent permissions or ownership are unsafe")
        for ancestor in path.parents:
            info = ancestor.stat()
            # Root-owned sticky temporary directories are acceptable ancestors.
            if info.st_mode & 0o022 and not (info.st_uid == 0 and info.st_mode & stat.S_ISVTX):
                raise ValueError("writable ancestor refused")


def execution_integrity(path: Path, *, saved: dict | None = None, identity: bool = False) -> None:
    """Executable bytes require trusted ownership and no untrusted mutation."""
    if saved is not None:
        if saved.get("kind") == "absent":
            return
        values = [saved]
        if saved.get("kind") == "tree":
            values += list(saved["directories"].values()) + list(saved["files"].values())
        if os.name == "nt":
            with closing(WindowsSecurity()) as security:
                for value in values:
                    security.validate(None, private=identity, toolchain=not identity,
                                      descriptor_text=value.get("acl", ""))
        elif any(not isinstance(value.get("mode"), int) or value["mode"] & 0o022 for value in values):
            raise ValueError("saved executable permissions are unsafe")
        return
    checked_path(path)
    if not path.exists():
        return
    if os.name == "nt":
        if identity:
            windows_acl(path)  # Identity remains current-user plus SYSTEM only.
        else:
            with closing(WindowsSecurity()) as security:
                windows_toolchain_tree(security, path)
        return
    safe_parent(path.parent, private=False)
    for current in ([path, *path.rglob("*")] if path.is_dir() else [path]):
        checked_path(current, exists=True)
        info = current.stat()
        if info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise ValueError("executable ownership or permissions are unsafe")


def execution_inputs(inputs: list[dict], *, saved: bool = False) -> None:
    for entry in inputs:
        if entry["runtime"] or entry.get("category") in {"entrypoint", "outer-wrapper"}:
            execution_integrity(Path(entry["destination"]),
                                saved=entry["before"] if saved else None,
                                identity=entry.get("category") == "hash-record")


def windows_descriptor(path: Path, restore: str | None = None) -> str:
    """Keep owner/group/DACL data inside the protected backup, including explicit ACEs."""
    if os.name != "nt":
        raise ValueError("native Windows security descriptors are unavailable")
    encoded_path = base64.b64encode(str(path).encode("utf-8")).decode("ascii")
    script = ("$ErrorActionPreference='Stop'; "
              "$p=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('" + encoded_path + "')); ")
    if restore is not None:
        encoded_acl = base64.b64encode(restore.encode("utf-8")).decode("ascii")
        script += ("$s=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('" + encoded_acl + "')); "
                   "$a=Get-Acl -LiteralPath $p; $a.SetSecurityDescriptorSddlForm($s); "
                   "Set-Acl -LiteralPath $p -AclObject $a; ")
    script += ("(Get-Acl -LiteralPath $p).GetSecurityDescriptorSddlForm("
               "[Security.AccessControl.AccessControlSections]'Access,Owner,Group')")
    executable = Path(os.environ["SYSTEMROOT"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    descriptor = run([str(executable), "-NoProfile", "-NonInteractive", "-EncodedCommand",
                      base64.b64encode(script.encode("utf-16le")).decode("ascii")]).decode().strip()
    if not descriptor or (restore is not None and descriptor != restore):
        raise ValueError("Windows security descriptor verification failed")
    return descriptor


def permissions(path: Path) -> dict:
    value = {"mode": stat.S_IMODE(path.stat().st_mode)}
    if os.name == "nt":
        value["acl"] = windows_descriptor(path)
    return value


def restore_permissions(path: Path, value: dict) -> None:
    path.chmod(value["mode"])
    if os.name == "nt":
        windows_descriptor(path, value["acl"])


def git_objects(source: Path, pin: dict) -> dict[str, bytes]:
    checked_path(source, exists=True)
    def git(*arguments):
        return run(["git", "--no-optional-locks", "-c", "core.hooksPath=" + os.devnull,
                    "-c", "core.fsmonitor=false", "-c", "protocol.allow=never",
                    "-C", str(source), *arguments])
    if git("config", "--local", "--get", "remote.origin.url").decode().strip() != pin["repository"]:
        raise ValueError("public checkout repository mismatch")
    if (git("rev-parse", "--verify", "HEAD^{commit}").decode().strip() != pin["revision"]
            or git("rev-parse", "--verify", "HEAD^{tree}").decode().strip() != pin["tree"]):
        raise ValueError("public checkout revision/tree mismatch")
    entries = git("ls-tree", "-rz", "--full-tree", pin["revision"])
    required = {"pyproject.toml", "README.md", "LICENSE"}
    blobs = {}
    modules = set()
    for entry in entries.split(b"\0"):
        if not entry:
            continue
        header, name = entry.split(b"\t", 1)
        mode, kind, oid = header.decode("ascii").split()
        name = name.decode("utf-8")
        if name.startswith("src/t3_fleet/"):
            relative = name.removeprefix("src/t3_fleet/")
            if "/" in relative or not relative.endswith(".py"):
                raise ValueError("pinned source module inventory mismatch")
            modules.add(name)
            required.add(name)
        if name in required:
            if mode not in {"100644", "100755"} or kind != "blob":
                raise ValueError("unsafe pinned source member")
            content = git("cat-file", "blob", oid)
            if len(content) > BUILDER["MAX_BYTES"]:
                raise ValueError("pinned source member too large")
            if hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest() != oid:
                raise ValueError("pinned Git blob integrity mismatch")
            blobs[name] = content
    if blobs.keys() != required or modules != {"src/t3_fleet/" + name for name in BUILDER["source_modules"](blobs)}:
        raise ValueError("pinned source module inventory mismatch")
    # The package contract and module allowlist come from authenticated Git
    # objects at the explicit pin, never from a wheel's self-description.
    BUILDER["validate_source_contract"](pin, blobs)
    return blobs


def validate_artifact(args, *, pin: dict | None = None) -> dict:
    pin = BUILDER["load_pin"](args.pin) if pin is None else validate_pin(pin)
    if args.layout != "source-wrapper" and set(pin["package"]["console_scripts"]) != {
        "t3-fleet", "t3-fleet-agent",
    }:
        raise ValueError("console scripts exceed the managed launcher set")
    source = checked_path(args.public_source, exists=True)
    artifact = checked_path(args.artifact_dir, exists=True)
    for item in artifact.rglob("*"):
        checked_path(item, exists=True)
    source_files = git_objects(source, pin)
    manifest = BUILDER["verify_artifacts"](artifact, pin, source_files)
    wheel = artifact / BUILDER["wheel_name"](pin)
    with zipfile.ZipFile(wheel) as archive:
        contents = {name: archive.read(name) for name in archive.namelist()}
    return {"pin": pin, "manifest": manifest, "contents": contents,
            "source_files": source_files, "wheel": wheel}


def validate_pin(pin: dict) -> dict:
    try:
        return BUILDER["validate_pin"](pin)
    except (TypeError, ValueError):
        raise ValueError("invalid public pin structure") from None


def pin_from_manifest(manifest: dict) -> dict:
    if (not isinstance(manifest, dict)
            or set(manifest) != {"schema", "repository", "revision", "tree", "package", "wheel", "modules"}
            or type(manifest.get("schema")) is not int or manifest["schema"] != 1
            or not isinstance(manifest.get("wheel"), dict)
            or set(manifest["wheel"]) != {"filename", "sha256"}
            or not isinstance(manifest.get("modules"), dict)
            or any(not isinstance(value, str) or not SHA256.fullmatch(value)
                   for value in manifest["modules"].values())):
        raise ValueError("invalid public identity manifest")
    pin = validate_pin({key: manifest[key] for key in
                        ("schema", "repository", "revision", "tree", "package")})
    if (manifest["wheel"].get("filename") != BUILDER["wheel_name"](pin)
            or not isinstance(manifest["wheel"].get("sha256"), str)
            or not SHA256.fullmatch(manifest["wheel"]["sha256"])):
        raise ValueError("invalid public identity manifest")
    names = set()
    for entry in manifest["modules"]:
        if (not isinstance(entry, str) or not entry.startswith("t3_fleet/")):
            raise ValueError("invalid public identity manifest")
        relative = entry.removeprefix("t3_fleet/")
        if "/" in relative or not relative.endswith(".py") or not relative:
            raise ValueError("invalid public identity manifest")
        names.add(relative)
    if "__init__.py" not in names:
        raise ValueError("invalid public identity manifest")
    return pin


def check_interpreter_overrides(python: Path) -> None:
    checked_path(python, exists=True, interpreter=True)
    # Windows path-configuration files can override isolated startup itself.
    # Refuse them before even the stdlib probe, including beside the resolved
    # executable when the selected venv Python is a symlink.
    for executable in {python, python.resolve()}:
        overrides = [executable.with_suffix("._pth"), *executable.parent.glob("python*._pth")]
        if any(checked_path(path).exists() for path in overrides):
            raise ValueError("interpreter path-configuration override refused")


def _toolchain_parent(path: Path) -> None:
    if os.name == "nt":
        return
    for ancestor in (path, *path.parents):
        info = ancestor.stat()
        if info.st_uid not in {0, os.geteuid()}:
            raise ValueError("interpreter anchor owner refused")
        if info.st_mode & 0o022 and not (info.st_uid == 0 and info.st_mode & stat.S_ISVTX):
            raise ValueError("interpreter anchor mutation permissions refused")


def _anchor_object(path: Path, security=None, *, deadline: float, remaining_bytes: int) -> dict:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return {"kind": "absent"}
    if os.name != "nt":
        _toolchain_parent(path.parent)
        if info.st_uid not in {0, os.geteuid()}:
            raise ValueError("interpreter anchor owner refused")
    value = {"device": info.st_dev, "inode": info.st_ino, "mode": stat.S_IMODE(info.st_mode),
             "uid": getattr(info, "st_uid", None), "size": info.st_size,
             "mtime_ns": info.st_mtime_ns}
    if stat.S_ISLNK(info.st_mode):
        if os.name == "nt":
            raise ValueError("interpreter anchor reparse object refused")
        value.update(kind="symlink", target=os.readlink(path))
    elif stat.S_ISDIR(info.st_mode):
        value["kind"] = "directory"
        if os.name != "nt" and info.st_mode & 0o022:
            raise ValueError("interpreter anchor directory is mutable")
    elif stat.S_ISREG(info.st_mode):
        if os.name != "nt" and info.st_mode & 0o022:
            raise ValueError("interpreter anchor file is mutable")
        if info.st_size > remaining_bytes:
            raise ValueError("interpreter anchor byte inventory exceeded its bound")
        digest = hashlib.sha256()
        read = 0
        with path.open("rb") as stream:
            while True:
                if time.monotonic() >= deadline:
                    raise ValueError("interpreter anchor inventory exceeded its bound")
                chunk = stream.read(min(1024 * 1024, remaining_bytes - read + 1))
                if not chunk:
                    break
                read += len(chunk)
                if read > remaining_bytes:
                    raise ValueError("interpreter anchor byte inventory exceeded its bound")
                digest.update(chunk)
        if read != info.st_size:
            raise ValueError("interpreter anchor file changed during inventory")
        value.update(kind="file", sha256=digest.hexdigest())
    else:
        raise ValueError("interpreter anchor special object refused")
    if os.name == "nt":
        handle = security.open(path)
        try:
            security.validate(handle, private=False, toolchain=True)
            value.update(security.object_identity(handle))
        finally:
            security.k.CloseHandle(handle)
    return value


def _anchor_inventory(paths: list[Path], *, shallow_paths=(), excluded_roots=(),
                      deadline: float | None = None) -> dict[str, dict]:
    started = time.monotonic()
    deadline = min(started + ANCHOR_TIME_LIMIT, deadline if deadline is not None else float("inf"))
    inventory: dict[str, dict] = {}
    total = 0
    security = WindowsSecurity() if os.name == "nt" else None
    roots = list(dict.fromkeys(Path(path) for path in paths))
    shallow = {Path(path) for path in shallow_paths}
    full_roots = set(roots) - shallow
    excluded = tuple(dict.fromkeys(Path(path) for path in excluded_roots))

    def excluded_path(path: Path) -> bool:
        return any(path == root or path.is_relative_to(root) for root in excluded)

    try:
        with ExitStack() as ancestors:
            if security is not None:
                for path in roots:
                    # A shallow installation root is itself the protected
                    # containing directory. Pin it as the nearest toolchain
                    # object so an OS-owned volume root remains an ancestor.
                    parent_subject = path / ".anchor-inventory-boundary" if path in shallow else path
                    ancestors.enter_context(security.parents(parent_subject))
            pending = [(path, 1 if path in shallow else None)
                       for path in roots if not excluded_path(path)]
            while pending:
                path, depth = pending.pop()
                name = str(path)
                if name in inventory:
                    continue
                if time.monotonic() >= deadline or len(inventory) >= ANCHOR_OBJECT_LIMIT:
                    raise ValueError("interpreter anchor inventory exceeded its bound")
                value = _anchor_object(path, security, deadline=deadline,
                                       remaining_bytes=ANCHOR_BYTE_LIMIT - total)
                inventory[name] = value
                if value["kind"] == "file":
                    total += value["size"]
                    if total > ANCHOR_BYTE_LIMIT:
                        raise ValueError("interpreter anchor byte inventory exceeded its bound")
                elif value["kind"] == "directory" and depth != 0:
                    children = []
                    for child in path.iterdir():
                        if time.monotonic() >= deadline:
                            raise ValueError("interpreter anchor inventory exceeded its bound")
                        if excluded_path(child):
                            continue
                        if len(inventory) + len(pending) + len(children) >= ANCHOR_OBJECT_LIMIT:
                            raise ValueError("interpreter anchor inventory exceeded its bound")
                        if child not in full_roots:
                            children.append(child)
                    children.sort(key=lambda child: child.name, reverse=True)
                    pending.extend((child, 0 if depth == 1 else None) for child in children)
                elif value["kind"] == "symlink":
                    target = path.resolve(strict=True)
                    if not excluded_path(target):
                        pending.append((target, 0 if depth == 1 else None))
    finally:
        if security is not None:
            security.close()
    return inventory


def _read_pyvenv(path: Path) -> tuple[dict[str, str], bytes]:
    if path.stat().st_size > 65536:
        raise ValueError("interpreter configuration refused")
    with path.open("rb") as stream:
        data = stream.read(65537)
    if len(data) > 65536 or b"\0" in data:
        raise ValueError("interpreter configuration refused")
    settings = {}
    for line in data.decode("utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if line.count("=") != 1:
            raise ValueError("interpreter configuration refused")
        key, value = (part.strip() for part in line.split("=", 1))
        key = key.lower()
        if not key or key in settings:
            raise ValueError("interpreter configuration refused")
        settings[key] = value
    return settings, data


def _disabled_base_site_roots(base_root: Path, startup_roots: list[Path],
                              active_site_roots: list[Path]) -> list[Path]:
    """Derive exact base third-party roots disabled by isolated no-site startup."""
    base_root = base_root.resolve()
    startup = {Path(path).resolve() for path in startup_roots}
    active = {Path(path).resolve() for path in active_site_roots}
    disabled = []
    variables = {"base": str(base_root),
                 "platbase": str(Path(getattr(sys, "base_exec_prefix", base_root)).resolve())}
    for scheme in sysconfig.get_scheme_names():
        scheme_paths = sysconfig.get_paths(scheme=scheme, vars=variables)
        scheme_stdlib = {Path(scheme_paths[name]).resolve()
                         for name in ("stdlib", "platstdlib")}
        if not scheme_stdlib or not scheme_stdlib <= startup:
            continue
        for name in ("purelib", "platlib"):
            candidate = Path(scheme_paths[name]).resolve()
            if (candidate in active or candidate in startup
                    or not candidate.is_relative_to(base_root)
                    or any(root.is_relative_to(candidate) for root in startup)):
                continue
            disabled.append(candidate)
    return sorted(dict.fromkeys(disabled), key=str)


def establish_interpreter_anchor(args) -> dict:
    args._code = "interpreter-anchor-required"
    if (sys.implementation.name != "cpython" or sys.version_info[:2] < (3, 11)
            or not sys.flags.isolated or not sys.flags.no_site
            or not sys.flags.dont_write_bytecode):
        raise ValueError("direct isolated CPython invocation required")
    selected = checked_path(args.python, exists=True, interpreter=True)
    current = Path(sys.executable)
    if selected != current:
        raise ValueError("selected interpreter is not the running interpreter")
    check_interpreter_overrides(selected)
    args._code = "interpreter-anchor-ineligible"
    prefix = checked_path(args.prefix, exists=True)
    config = prefix / "pyvenv.cfg"
    config_data = None
    if args.layout == "venv":
        settings, config_data = _read_pyvenv(checked_path(config, exists=True))
        bindir = prefix / ("Scripts" if os.name == "nt" else "bin")
        if selected.parent != bindir or settings.get("include-system-site-packages", "").lower() != "false":
            raise ValueError("unsupported virtual environment configuration")
        base = Path(getattr(sys, "_base_executable", selected.resolve())).resolve()
        if selected.resolve() != base:
            raise ValueError("virtual environment interpreter target mismatch")
        if "executable" in settings and Path(settings["executable"]).resolve() != base:
            raise ValueError("virtual environment configuration target mismatch")
        if "home" in settings and Path(settings["home"]).resolve() != base.parent:
            raise ValueError("virtual environment home mismatch")
        scheme = "venv" if "venv" in sysconfig.get_scheme_names() else os.name + "_venv"
        paths = sysconfig.get_paths(scheme=scheme, vars={"base": str(prefix), "platbase": str(prefix)})
    else:
        if config.exists() or prefix != Path(sys.prefix):
            raise ValueError("running interpreter prefix mismatch")
        paths = sysconfig.get_paths(scheme="nt_user") if args.layout == "windows-user-site" else sysconfig.get_paths()
    root = args.source_root or Path(paths["purelib"])
    spec = PathFinder.find_spec("t3_fleet", [str(root)])
    site_roots = [paths["purelib"]]
    if args.layout == "windows-user-site":
        site_roots.extend([sysconfig.get_path("purelib"), sysconfig.get_path("platlib")])
    site_roots = list(dict.fromkeys(site_roots))
    pip_candidates = []
    if args.layout != "source-wrapper":
        for candidate in site_roots:
            pip_spec = PathFinder.find_spec("pip", [candidate])
            if pip_spec is not None:
                pip_candidates.append({"root": candidate, "origin": pip_spec.origin})
    stdlib = Path(sysconfig.get_path("stdlib")).resolve()
    platstdlib = Path(sysconfig.get_path("platstdlib")).resolve()
    dynload_value = sysconfig.get_config_var("DESTSHARED")
    if os.name == "nt":
        dynload = (Path(sys.base_prefix) / "DLLs").resolve()
    else:
        dynload = Path(dynload_value).resolve() if dynload_value else platstdlib / "lib-dynload"
    zip_root = Path(sys.base_prefix) / "lib" / f"python{sys.version_info[0]}{sys.version_info[1]}.zip"
    if os.name == "nt":
        zip_root = Path(sys.base_prefix) / f"python{sys.version_info[0]}{sys.version_info[1]}.zip"
    base_root = Path(sys.base_prefix).resolve()
    startup_roots = list(dict.fromkeys([stdlib, platstdlib, dynload, zip_root,
                                        *([base_root] if os.name == "nt" else [])]))
    seen = set()
    for value in sys.path:
        if not value:
            raise ValueError("current-directory interpreter root refused")
        path = Path(value)
        if not path.is_absolute() or path in seen or "site-packages" in path.parts or "dist-packages" in path.parts:
            raise ValueError("unsupported interpreter startup root")
        seen.add(path)
        resolved = path.resolve(strict=False)
        if not any(resolved == allowed or resolved.is_relative_to(allowed)
                   for allowed in startup_roots):
            raise ValueError("interpreter startup root outside supported layout")
    inventory_paths = [selected, selected.resolve(), *startup_roots]
    if config_data is not None:
        inventory_paths.append(config)
    active_site_roots = [Path(value) for value in site_roots]
    disabled_base_roots = _disabled_base_site_roots(
        base_root, startup_roots, active_site_roots)
    excluded_roots = list(dict.fromkeys([*active_site_roots, *disabled_base_roots]))
    shallow_paths = [base_root] if os.name == "nt" else []
    inventory = _anchor_inventory(inventory_paths, shallow_paths=shallow_paths,
                                  excluded_roots=excluded_roots)
    probe = {"executable": str(selected), "prefix": str(prefix), "base_prefix": sys.base_prefix,
             "purelib": paths["purelib"], "scripts": paths["scripts"],
             "version": list(sys.version_info[:3]), "user_site_enabled": args.layout != "venv",
             "platform": os.name, "import_origin": spec.origin if spec else None,
             "site_roots": site_roots, "pip_candidates": pip_candidates,
             "extension_suffixes": EXTENSION_SUFFIXES, "pointer_bits": struct.calcsize("P") * 8,
             "sysconfig_platform": sysconfig.get_platform()}
    return {"python": str(selected), "resolved": str(selected.resolve()), "probe": probe,
            "inventory": inventory, "startup_roots": [str(path) for path in startup_roots],
            "flags": [sys.flags.isolated, sys.flags.no_site, sys.flags.dont_write_bytecode],
            "implementation": sys.implementation.name, "version": list(sys.version_info[:3]),
            "cache_tag": sys.implementation.cache_tag, "marshal": marshal.version,
            "platform": sys.platform, "prefix": str(prefix), "base_prefix": sys.base_prefix,
            "inventory_roots": [str(path) for path in inventory_paths],
            "inventory_shallow": [str(path) for path in shallow_paths],
            "inventory_excluded": [str(path) for path in excluded_roots],
            "config_sha256": hashlib.sha256(config_data).hexdigest() if config_data is not None else None}


def revalidate_interpreter_anchor(anchor: dict, *, deadline: float | None = None) -> None:
    roots = [Path(path) for path in anchor.get("inventory_roots", anchor["inventory"])]
    try:
        current = _anchor_inventory(
            roots,
            shallow_paths=[Path(path) for path in anchor.get("inventory_shallow", ())],
            excluded_roots=[Path(path) for path in anchor.get("inventory_excluded", ())],
            deadline=deadline,
        )
    except ValueError:
        if deadline is not None and time.monotonic() >= deadline:
            raise ValueError("trusted compiler budget exhausted") from None
        raise
    if deadline is not None and time.monotonic() >= deadline:
        raise ValueError("trusted compiler budget exhausted")
    if (current != anchor["inventory"] or str(Path(anchor["python"]).resolve()) != anchor["resolved"]):
        raise ValueError("interpreter anchor changed")


def require_anchor(args) -> dict:
    anchor = getattr(args, "_anchor", None)
    if not isinstance(anchor, dict) or anchor.get("python") != str(args.python):
        raise ValueError("interpreter anchor required")
    return anchor


def guard_interpreter_anchor(args, next_code: str | None = None, *,
                             deadline: float | None = None) -> dict:
    args._code = "interpreter-anchor-drift"
    anchor = require_anchor(args)
    revalidate_interpreter_anchor(anchor, deadline=deadline)
    if next_code is not None:
        args._code = next_code
    return anchor


def probe_interpreter(python: Path, layout: str, prefix: Path, source_root: Path | None = None,
                      *, anchor: dict | None = None) -> dict:
    if anchor is None or anchor.get("python") != str(python) or anchor.get("prefix") != str(prefix):
        raise ValueError("interpreter anchor required")
    revalidate_interpreter_anchor(anchor)
    return dict(anchor["probe"])


def run_anchored_python(anchor: dict, arguments: list[str], *, cwd: Path | None = None,
                        timeout: float = 180, deadline: float | None = None) -> bytes:
    revalidate_interpreter_anchor(anchor, deadline=deadline)
    if deadline is not None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError("trusted compiler budget exhausted")
        timeout = min(timeout, remaining)
    try:
        result = subprocess.run([anchor["python"], "-I", "-S", "-B", *arguments],
                                env=command_environment(), cwd=cwd, capture_output=True,
                                timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise ValueError("anchored interpreter command unavailable") from None
    if result.returncode:
        raise ValueError("anchored interpreter command failed")
    return result.stdout


def import_candidates(root: Path, name: str, probe: dict) -> list[Path]:
    # Include sourceless and native modules, even if malformed. Never import
    # a candidate merely to determine whether it can precede our package.
    return [root / (name + suffix) for suffix in
            ("", ".py", ".pyc", *probe.get("extension_suffixes", EXTENSION_SUFFIXES), ".pyd")]


def check_import_paths(purelib: Path, probe: dict, *, source_wrapper: bool = False) -> None:
    if probe.get("import_origin") != str(purelib / "t3_fleet/__init__.py"):
        raise ValueError("selected interpreter import origin mismatch")
    for root in {purelib, *(Path(value) for value in probe.get("site_roots", []))}:
        checked_path(root)
        if not root.exists():
            continue
        if any(path.exists() for path in import_candidates(root, "t3_fleet", probe)
               if path != purelib / "t3_fleet"):
            raise ValueError("multiple interpreter import origins")
        if source_wrapper:
            # Source wrappers start the interpreter with -I -S -B, so no site
            # processing occurs and an unrelated distro .pth cannot execute.
            # The package origin check above still binds the exact source root.
            continue
        if any(path.exists() for name in ("sitecustomize", "usercustomize")
               for path in import_candidates(root, name, probe)):
            raise ValueError("custom interpreter site execution is ambiguous")
        for pth in root.glob("*.pth"):
            checked_path(pth, exists=True)
            execution_integrity(pth)
            for line in pth.read_text(encoding="utf-8").splitlines():
                if not line.strip() or line.startswith("#"):
                    continue
                raise ValueError("custom interpreter path or executable pth refused")


def inspect_pip(probe: dict, purelib: Path, *, windows: bool) -> Path:
    if windows and os.name == "nt":
        with closing(WindowsSecurity()) as security:
            return _inspect_pip(probe, purelib, windows=windows, security=security)
    return _inspect_pip(probe, purelib, windows=windows)


def windows_toolchain_tree(security: WindowsSecurity, path: Path) -> None:
    """One bounded native batch, never a subprocess per pip file."""
    deadline = time.monotonic() + 50
    pending = [path]
    count = 0
    with security.parents(path):
        while pending:
            if time.monotonic() > deadline or count >= 10000:
                raise ValueError("selected pip ACL inspection exceeded its bound")
            current = checked_path(pending.pop(), exists=True)
            handle = security.open(current)
            try:
                security.validate(handle, private=False, toolchain=True)
                if current.is_dir():
                    with os.scandir(current) as entries:
                        for entry in entries:
                            if len(pending) + count >= 10000:
                                raise ValueError("selected pip inventory exceeded its bound")
                            pending.append(Path(entry.path))
            finally:
                security.k.CloseHandle(handle)
            count += 1


def _inspect_pip(probe: dict, purelib: Path, *, windows: bool,
                 security: WindowsSecurity | None = None) -> Path:
    """Admit one existing, self-vendored pip without importing it or site code."""
    candidates = probe.get("pip_candidates", [])
    if len(candidates) != 1:
        raise ValueError("one unambiguous existing pip origin is required")
    candidate = candidates[0]
    root = checked_path(Path(candidate["root"]), exists=True)
    if root not in {purelib, *(Path(value) for value in probe["site_roots"])}:
        raise ValueError("pip is outside selected interpreter schemes")
    origin = checked_path(Path(candidate["origin"]), exists=True)
    if origin != root / "pip/__init__.py":
        raise ValueError("pip package origin is ambiguous")
    if security:
        windows_toolchain_tree(security, root / "pip")
        # The scheme root itself influences imports, not only pip's files.
        with security.parents(root):
            handle = security.open(root)
            try:
                security.validate(handle, private=False, toolchain=True)
            finally:
                security.k.CloseHandle(handle)
    else:
        safe_parent(root, private=False, windows=windows)
    files = tree_files(root / "pip")
    if not {"__init__.py", "__main__.py", "_internal/__init__.py", "_vendor/__init__.py"} <= files.keys():
        raise ValueError("existing pip package is incomplete")
    for path in (root / "pip", *(root / "pip").rglob("*")):
        if security:
            continue
        if path.is_dir():
            safe_parent(path, private=False, windows=windows)
        elif windows:
            windows_acl(path, private=False)
        elif path.stat().st_uid != os.geteuid() or path.stat().st_mode & 0o022:
            raise ValueError("pip file ownership or permissions are unsafe")
    # Distribution-patched pip can depend on ambient third-party roots. Those
    # cannot be admitted to an isolated bootstrap without another trust boundary.
    vendor = ast.parse(files["_vendor/__init__.py"].read_text(encoding="utf-8"))
    assignments = [node.value for node in ast.walk(vendor)
                   if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "DEBUNDLED"
                           for target in node.targets)]
    if len(assignments) != 1 or not isinstance(assignments[0], ast.Constant) or assignments[0].value is not False:
        raise ValueError("pip must use its bundled dependencies")
    metadata = []
    for directory in root.glob("*.dist-info"):
        checked_path(directory, exists=True)
        file = checked_path(directory / "METADATA", exists=True)
        # Only pip metadata is an executable toolchain input. Other installed
        # metadata is parsed as data to detect an ambiguous pip distribution.
        if security and re.fullmatch(r"pip-[A-Za-z0-9_.+]+\.dist-info", directory.name, re.I):
            windows_toolchain_tree(security, directory)
        document = BytesParser().parsebytes(file.read_bytes())
        if document.get("Name", "").lower() == "pip":
            if not re.fullmatch(r"pip-[A-Za-z0-9_.+]+\.dist-info", directory.name, re.I):
                raise ValueError("pip metadata path is ambiguous")
            metadata.append(document)
    if len(metadata) != 1 or metadata[0].get_all("Requires-Dist"):
        raise ValueError("pip distribution or external dependencies are ambiguous")
    return origin


def source_wrapper_text(python: Path, source_root: Path, module: str) -> str:
    bootstrap = (
        "import runpy, sys; "
        "source_root, module, *args = sys.argv[1:]; "
        "sys.path.insert(0, source_root); "
        "sys.argv = [module, *args]; "
        'runpy.run_module(module, run_name="__main__", alter_sys=True)'
    )
    command = [str(python), "-I", "-S", "-B", "-c", bootstrap, str(source_root), f"t3_fleet.{module}"]
    return "#!/bin/sh\nexec " + shlex.join(command) + ' "$@"\n'


def source_wrapper(path: Path, python: Path, source_root: Path, module: str) -> str:
    """Validate a literal source wrapper and report whether it needs migration.

    The current form starts the selected interpreter in isolation. The
    interpreter ignores site hooks, startup environment, and the current
    directory, then loads the module from the approved root. The legacy form is
    accepted only for an explicit, recoverable migration during apply/upgrade.
    """
    text = path.read_text(encoding="utf-8")
    if text == source_wrapper_text(python, source_root, module):
        return "current"
    lines = text.splitlines()
    if not lines or lines[0] not in {"#!/bin/sh", "#!/bin/bash"}:
        raise ValueError("unrecognized source wrapper")
    lines = lines[1:]
    if lines and lines[0] in {"set -eu", "set -e", "set -euo pipefail"}:
        lines = lines[1:]
    if len(lines) != 2:
        raise ValueError("ambiguous source wrapper")
    # Expand only the observed double-quoted HOME prefix, statically against
    # this operator's home. Never evaluate wrapper text or other variables.
    home_form = re.fullmatch(r'export PYTHONPATH="\$HOME(/[A-Za-z0-9_./-]+)"', lines[0])
    if home_form:
        resolved = checked_path(Path.home() / home_form[1].lstrip("/"))
        if resolved != source_root or "//" in home_form[1]:
            raise ValueError("source wrapper HOME root mismatch")
        lines[0] = "export " + shlex.quote("PYTHONPATH=" + str(resolved))
    if (shlex.split(lines[0]) != ["export", "PYTHONPATH=" + str(source_root)]
            or any(character in lines[0] for character in ("$", "`", ";", "&", "|", "<", ">"))):
        raise ValueError("source wrapper interpreter or PYTHONPATH mismatch")
    tokens = shlex.split(lines[1])
    if tokens[:2] != ["exec", str(python)] or tokens[-3:] != ["-m", "t3_fleet." + module, "$@"]:
        raise ValueError("source wrapper interpreter or PYTHONPATH mismatch")
    if tokens[2:-3]:
        raise ValueError("source wrapper startup flags are unsupported")
    if not lines[1].endswith('"$@"'):
        raise ValueError("nonliteral source wrapper interpreter")
    # shlex is a tokenizer, not a shell validator. Admit only literal quoted
    # paths and the exact argument-forwarding suffix, with no expansions.
    literal = lines[1][len("exec "):-len(' -m t3_fleet.' + module + ' "$@"')]
    if any(character in literal for character in ("$", "`", ";", "&", "|", "<", ">", "\n")):
        raise ValueError("nonliteral source wrapper interpreter")
    return "legacy"


def migrate_source_wrapper(path: Path, python: Path, source_root: Path, module: str) -> bool:
    """Rewrite a validated legacy wrapper once. Caller owns backup/rollback."""
    if source_wrapper(path, python, source_root, module) == "current":
        return False
    data = source_wrapper_text(python, source_root, module).encode("utf-8")
    mode = stat.S_IMODE(path.stat().st_mode)
    temporary = path.parent / (".t3-fleet-wrapper-" + uuid.uuid4().hex)
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        fsync_directory(path.parent)
    except BaseException:
        if temporary.exists():
            temporary.unlink()
        raise
    if source_wrapper(path, python, source_root, module) != "current":
        raise ValueError("migrated source wrapper did not validate")
    return True


def _console_launcher_format(script: bytes, module: str) -> str | None:
    forms = {
        "regex": (f"import re\nimport sys\nfrom t3_fleet.{module} import main\n"
                  "if __name__ == '__main__':\n"
                  "    sys.argv[0] = re.sub(r'(-script\\.pyw|\\.exe)?$', '', sys.argv[0])\n"
                  "    sys.exit(main())\n"),
        "guarded-regex": (f"import re\nimport sys\nif __name__ == '__main__':\n"
                          f"    from t3_fleet.{module} import main\n"
                          "    sys.argv[0] = re.sub(r'(-script\\.pyw|\\.exe)?$', '', sys.argv[0])\n"
                          "    sys.exit(main())\n"),
        "removesuffix": (f"import sys\nfrom t3_fleet.{module} import main\n"
                         "if __name__ == '__main__':\n"
                         "    sys.argv[0] = sys.argv[0].removesuffix('.exe')\n"
                         "    sys.exit(main())\n"),
    }
    try:
        actual = ast.dump(ast.parse(script.decode("utf-8")))
    except (SyntaxError, UnicodeError):
        return None
    return next((name for name, expected in forms.items()
                 if actual == ast.dump(ast.parse(expected))), None)


def pip_launcher_contract(pip_origin: Path) -> dict:
    scripts = checked_path(pip_origin.parent / "_vendor/distlib/scripts.py", exists=True)
    try:
        document = ast.parse(scripts.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeError):
        raise ValueError("unsupported pip console launcher generator") from None
    stores = [node for node in ast.walk(document)
              if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store)
              and node.id == "SCRIPT_TEMPLATE"]
    assignments = [node for node in document.body
                   if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "SCRIPT_TEMPLATE"
                           for target in node.targets)]
    makers = [node for node in document.body
              if isinstance(node, ast.ClassDef) and node.name == "ScriptMaker"]
    expected_method = ast.parse(
        "def _get_script_text(self, entry):\n"
        "    return self.script_template % dict(module=entry.prefix, "
        "import_name=entry.suffix.split('.')[0], func=entry.suffix)\n").body[0]
    if (len(stores) != 1 or len(assignments) != 1 or len(makers) != 1
            or not isinstance(assignments[0].value, ast.Constant)
            or type(assignments[0].value.value) is not str):
        raise ValueError("unsupported pip console launcher generator")
    template_bindings = [node for node in makers[0].body
                         if isinstance(node, ast.Assign)
                         and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                         and node.targets[0].id == "script_template"
                         and isinstance(node.value, ast.Name) and node.value.id == "SCRIPT_TEMPLATE"]
    methods = [node for node in makers[0].body
               if isinstance(node, ast.FunctionDef) and node.name == "_get_script_text"]
    if (len(template_bindings) != 1 or len(methods) != 1
            or ast.dump(methods[0]) != ast.dump(expected_method)):
        raise ValueError("unsupported pip console launcher generator")
    template = assignments[0].value.value
    replacements = {"%(module)s": "t3_fleet.cli", "%(import_name)s": "main", "%(func)s": "main"}
    if any(template.count(key) != 1 for key in replacements):
        raise ValueError("unsupported pip console launcher generator")
    try:
        rendered = template % dict(module="t3_fleet.cli", import_name="main", func="main")
    except (KeyError, TypeError, ValueError):
        raise ValueError("unsupported pip console launcher generator") from None
    format_name = _console_launcher_format(rendered.encode("utf-8"), "cli")
    if format_name is None:
        raise ValueError("unsupported pip console launcher generator")
    return {"pip_origin": str(pip_origin), "scripts_sha256": sha(scripts),
            "template": format_name}


def windows_launcher_contract(probe: dict, pip_origin: Path, python: Path) -> dict:
    bits, platform = probe.get("pointer_bits"), probe.get("sysconfig_platform")
    resource = {(32, "win32"): "t32.exe", (64, "win-amd64"): "t64.exe",
                (64, "win-arm64"): "t64-arm.exe"}.get((bits, platform))
    if resource is None:
        raise ValueError("unsupported selected interpreter launcher architecture")
    contract = pip_launcher_contract(pip_origin)
    distlib = pip_origin.parent / "_vendor/distlib"
    scripts = checked_path(distlib / "scripts.py", exists=True)
    # Verify the supported resource-selection expressions as data. Both known
    # distlib spellings select from the selected interpreter, never this process.
    selectors = [node for node in ast.walk(ast.parse(scripts.read_text(encoding="utf-8")))
                 if isinstance(node, ast.FunctionDef) and node.name == "_get_launcher"]
    selections = [
        "if struct.calcsize('P') == 8:\n    bits = '64'\nelse:\n    bits = '32'\n",
        "bits = '64' if struct.calcsize('P') == 8 else '32'\n",
    ]
    suffix = ("platform_suffix = '-arm' if get_platform() == 'win-arm64' else ''\n"
              "name = '%s%s%s.exe' % (kind, bits, platform_suffix)\n")
    if len(selectors) != 1 or not any(
            [ast.dump(node) for node in selectors[0].body[:3]] ==
            [ast.dump(node) for node in ast.parse(selection + suffix).body]
            for selection in selections):
        raise ValueError("unsupported pip distlib launcher selection")
    stub = checked_path(distlib / resource, exists=True)
    execution_integrity(stub)
    data = stub.read_bytes()
    if not data:
        raise ValueError("empty pip native launcher resource")
    return {**contract, "resource": resource, "bits": bits, "platform": platform,
            "prefix": base64.b64encode(data + ("#!" + str(python) + "\n").encode("utf-8")).decode("ascii")}


def console_launcher(path: Path, python: Path, module: str, *, windows: bool, contract: dict | None = None) -> None:
    checked_path(path, exists=True)
    data = path.read_bytes()
    if windows:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            if archive.namelist() != ["__main__.py"]:
                raise ValueError("unrecognized Windows console launcher")
            script = archive.read("__main__.py")
            start = archive.getinfo("__main__.py").header_offset
        prefix = data[:start]
        if contract is None or prefix != base64.b64decode(contract["prefix"], validate=True):
            raise ValueError("Windows native launcher differs from trusted pip contract")
    else:
        line, separator, script = data.partition(b"\n")
        if line != ("#!" + str(python)).encode("utf-8") or not separator:
            raise ValueError("console interpreter mismatch or unsupported shell trampoline")
    if _console_launcher_format(script, module) is None:
        raise ValueError("console entrypoint mismatch")


def distribution(purelib: Path, package: Path, launchers: list[Path],
                 expected_scripts: dict[str, str]) -> Path:
    matches = []
    for child in purelib.iterdir():
        normalized = child.name.lower().replace("-", "_")
        if "t3_fleet" in normalized and (normalized.endswith(".egg_link") or normalized.endswith(".egg_info")):
            raise ValueError("editable or legacy distribution refused")
        if child.name.endswith(".dist-info"):
            checked_path(child, exists=True)
            metadata = child / "METADATA"
            if not metadata.is_file():
                raise ValueError("unreadable distribution metadata")
            parsed = BytesParser().parsebytes(metadata.read_bytes())
            name = re.sub(r"[-_.]+", "-", parsed.get("Name", "")).lower()
            if name == "t3-fleet":
                if len(parsed.get_all("Name", [])) != 1 or len(parsed.get_all("Version", [])) != 1:
                    raise ValueError("ambiguous installed metadata")
                matches.append(child)
    if len(matches) != 1:
        raise ValueError("multiple or missing installed runtime distributions")
    dist = matches[0]
    if not re.fullmatch(r"t3_fleet-[A-Za-z0-9_.+]+\.dist-info", dist.name):
        raise ValueError("unexpected installed distribution path")
    metadata = BytesParser().parsebytes((dist / "METADATA").read_bytes())
    if dist.name != "t3_fleet-" + metadata["Version"] + ".dist-info":
        raise ValueError("installed distribution version/path mismatch")
    files = tree_files(dist)
    if "direct_url.json" in files:
        direct = json.loads(files["direct_url.json"].read_text())
        if not isinstance(direct, dict) or not isinstance(direct.get("dir_info", {}), dict):
            raise ValueError("malformed installed provenance")
        if direct.get("dir_info", {}).get("editable"):
            raise ValueError("editable distribution refused")
    entries = (dist / "entry_points.txt").read_text()
    expected = "[console_scripts]\n" + "".join(
        f"{key} = {value}\n" for key, value in sorted(expected_scripts.items()))
    if entries != expected:
        raise ValueError("installed distribution entrypoints mismatch")
    rows = list(csv.reader(io.StringIO((dist / "RECORD").read_text())))
    destinations = set()
    for row in rows:
        if len(row) != 3 or not row[0] or "\\" in row[0] or ":" in row[0]:
            raise ValueError("unsafe installed RECORD")
        candidate = Path(os.path.abspath(purelib / row[0]))
        checked_path(candidate)
        if not (candidate.is_relative_to(package) or candidate.is_relative_to(dist) or candidate in launchers):
            raise ValueError("RECORD removal path outside runtime footprint")
        if candidate in destinations:
            raise ValueError("duplicate installed RECORD destination")
        destinations.add(candidate)
    required = {package / "__init__.py", dist / "METADATA", dist / "RECORD", *launchers}
    if not required <= destinations:
        raise ValueError("installed RECORD does not own the runtime launchers")
    return dist


def inspect_layout(args) -> dict:
    anchor = guard_interpreter_anchor(args, "layout-invalid")
    python = checked_path(args.python, exists=True, interpreter=True)
    check_interpreter_overrides(python)
    prefix = checked_path(args.prefix, exists=True)
    purelib = checked_path(args.purelib, exists=True)
    scripts = checked_path(args.scripts, exists=True)
    for parent in (purelib, scripts):
        safe_parent(parent, private=False, windows=args.layout == "windows-user-site")
    if (scripts / "t3_fleet").exists() or (scripts / "t3_fleet.py").exists():
        raise ValueError("console directory can shadow the selected runtime")
    probe = probe_interpreter(python, args.layout, prefix, args.source_root, anchor=anchor)
    if any(path.exists() for path in import_candidates(scripts, "t3_fleet", probe)):
        raise ValueError("console directory can shadow the selected runtime")
    if (Path(probe["executable"]) != python or Path(probe["prefix"]) != prefix
            or probe["version"][:2] < [3, 11]):
        raise ValueError("selected interpreter identity mismatch")
    windows = args.layout == "windows-user-site"
    if windows and (probe["platform"] != "nt" or probe["version"][:2] != [3, 14]
                    or not probe["user_site_enabled"] or probe["prefix"] != probe["base_prefix"]):
        raise ValueError("Windows Python 3.14 user-site eligibility mismatch")
    launchers = [checked_path(args.fleet_launcher, exists=True),
                 checked_path(args.agent_launcher, exists=True)]
    if len(set(launchers)) != 2:
        raise ValueError("both distinct launchers must be supplied")
    package = purelib / "t3_fleet"
    check_import_paths(purelib, probe, source_wrapper=args.layout == "source-wrapper")
    files = tree_files(package)
    if "__init__.py" not in files:
        raise ValueError("installed runtime package is missing")
    pip_origin = None
    if args.layout != "source-wrapper":
        args._code = "pip-ineligible"
        pip_origin = inspect_pip(probe, purelib, windows=windows)
        args._code = "layout-invalid"
    launcher_contract = (windows_launcher_contract(probe, pip_origin, python) if windows
                         else pip_launcher_contract(pip_origin)) if pip_origin else None
    wrapper_migration = []
    if args.layout == "source-wrapper":
        if args.source_root is None or checked_path(args.source_root, exists=True) != purelib:
            raise ValueError("explicit source root must match package parent")
        if any("t3_fleet" in child.name.lower().replace("-", "_") and child != package
               and child.name not in {LOCK, RECOVERY}
               and not re.fullmatch(r"\.t3-fleet-(old|stage)-[0-9a-f]{32}", child.name)
               for child in purelib.iterdir()):
            raise ValueError("ambiguous source distribution")
        for launcher, module in zip(launchers, ("cli", "agent")):
            if source_wrapper(launcher, python, purelib, module) == "legacy":
                wrapper_migration.append(launcher)
        dist = None
    else:
        if args.source_root is not None or Path(probe["purelib"]) != purelib or Path(probe["scripts"]) != scripts:
            raise ValueError("selected interpreter installation scheme mismatch")
        expected = [scripts / (name + (".exe" if windows else "")) for name in ("t3-fleet", "t3-fleet-agent")]
        if launchers != expected:
            raise ValueError("console script locations mismatch")
        pin = BUILDER["load_pin"](args.pin)
        for launcher, module in zip(launchers, ("cli", "agent")):
            console_launcher(launcher, python, module, windows=windows, contract=launcher_contract)
        dist = distribution(purelib, package, launchers, pin["package"]["console_scripts"])
    wrappers = [checked_path(path, exists=True) for path in args.wrapper]
    if len(set(wrappers)) != len(wrappers) or set(wrappers) & set(launchers):
        raise ValueError("ambiguous outer wrapper inventory")
    return {"python": python, "purelib": purelib, "scripts": scripts, "package": package,
            "launchers": launchers, "wrappers": wrappers, "dist": dist, "probe": probe,
            "pip_origin": pip_origin, "launcher_contract": launcher_contract,
            "wrapper_migration": wrapper_migration}


def state_databases(args) -> dict[Path, bool]:
    dbs = {checked_path(path): True for path in args.state_db}
    for path in args.optional_state_db:
        dbs.setdefault(checked_path(path), False)
    if not dbs:
        raise ValueError("explicit complete resolved state inventory is required")
    for path in list(dbs):
        if path.name != "outbox.db":
            dbs.setdefault(path.parent / "outbox/outbox.db", False)
    return dbs


def selected_target_identity(args, dbs: dict[Path, bool] | None = None) -> dict:
    dbs = state_databases(args) if dbs is None else dbs
    # Check target-local references without importing the runtime. Transport is
    # controller-relative, so only the explicit target identity can scope the
    # agent database and listener used by this installation.
    selected_identity = None
    for config in args.config:
        document = tomllib.loads(config.read_text(encoding="utf-8"))
        configured_state = document.get("state_dir")
        if configured_state:
            if checked_path(Path(configured_state)) / "fleet.db" not in dbs:
                raise ValueError("configured controller state missing from explicit inventory")
        hosts = document.get("hosts")
        if not isinstance(hosts, dict) or args.target_host not in hosts:
            raise ValueError("explicit target host is not present in configured hosts")
        host = hosts[args.target_host]
        if not isinstance(host, dict):
            raise ValueError("explicit target host must be a table")
        agent_state_dir = host.get("agent_state_dir")
        if not isinstance(agent_state_dir, str) or not agent_state_dir:
            raise ValueError("explicit target host requires agent state")
        agent_state_path = checked_path(Path(agent_state_dir))
        t3_url = host.get("t3_url", "http://127.0.0.1:3773")
        if not isinstance(t3_url, str):
            raise ValueError("explicit target host requires a string origin")
        identity = {"agent_state_dir": str(agent_state_path), "t3_url": t3_url}
        if selected_identity is not None and identity != selected_identity:
            raise ValueError("explicit target host is inconsistent across configurations")
        selected_identity = identity
        if agent_state_path / "target.db" not in dbs:
            raise ValueError("configured target state missing from explicit inventory")
        if (not t3_url or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in t3_url)
                or "?" in t3_url or "#" in t3_url):
            raise ValueError("target origin must be a literal loopback HTTP listener")
        origin = urlsplit(t3_url)
        try:
            loopback = ipaddress.ip_address(origin.hostname or "").is_loopback
        except ValueError:
            loopback = False
        if (origin.scheme != "http" or not T3_AUTHORITY.fullmatch(origin.netloc)
                or not loopback or origin.username is not None or origin.password is not None
                or origin.path not in ("", "/") or origin.query or origin.fragment or not origin.port):
            raise ValueError("target origin must be a literal loopback HTTP listener")
    if selected_identity is None:
        raise ValueError("explicit target host requires an effective configuration")
    return selected_identity


def inventory_backup(args, layout: dict) -> list[dict]:
    entries = []
    def add(path, kind, required, runtime=False, category="state-db"):
        path = checked_path(path, exists=required)
        if any(Path(entry["destination"]) == path for entry in entries):
            raise ValueError("duplicate backup input")
        if path.exists():
            if kind == "tree":
                tree_files(path)
            elif not path.is_file():
                raise ValueError("backup input must be a regular file")
        entries.append({"label": f"input-{len(entries):04d}", "destination": str(path),
                        "kind": kind, "exists": path.exists(), "runtime": runtime,
                        "category": category, "required": required})
    add(layout["package"], "tree", True, True, "runtime-package")
    if layout["dist"]:
        add(layout["dist"], "tree", True, True, "distribution-metadata")
        target_dist = args.purelib / BUILDER["dist_dir"](BUILDER["load_pin"](args.pin)).rstrip("/")
        if target_dist != layout["dist"]:
            if target_dist.exists():
                raise ValueError("target distribution path already occupied")
            add(target_dist, "tree", False, True, "distribution-metadata")
    for launcher in layout["launchers"]:
        # Source wrappers are runtime inputs whenever a legacy grammar may be
        # migrated, so their exact bytes participate in backup and rollback.
        add(launcher, "file", True, True, "entrypoint")
    for wrapper in layout["wrappers"]:
        add(wrapper, "file", True, category="outer-wrapper")
    add(args.hash_record, "file", False, True, "hash-record")
    for category, paths in (("config", args.config), ("private-file", args.private_file),
                            ("payload-file", args.payload_file)):
        for path in paths:
            add(path, "file", True, category=category)
    dbs = state_databases(args)
    for path, required in dbs.items():
        add(path, "sqlite", required)
        for suffix in ("-wal", "-shm", "-journal"):
            checked_path(Path(str(path) + suffix))
    selected_target_identity(args, dbs)
    return entries


def disjoint(left: Path, right: Path) -> bool:
    return not (left.is_relative_to(right) or right.is_relative_to(left))


def check_boundaries(args, inputs: list[dict]) -> None:
    backup = checked_path(args.backup_parent, exists=True)
    safe_parent(backup, private=True, windows=args.layout == "windows-user-site")
    anchors = [args.artifact_dir, args.public_source, args.purelib, args.scripts, backup]
    for index, path in enumerate(anchors):
        for other in anchors[index + 1:]:
            if not disjoint(path, other):
                raise ValueError("artifact, checkout, backup and installation paths overlap")
    for path in (backup, args.artifact_dir):
        if any((parent / ".git").exists() for parent in (path, *path.parents)):
            raise ValueError("backup and artifacts must be outside Git checkouts")
    destinations = [Path(entry["destination"]) for entry in inputs]
    for index, path in enumerate(destinations):
        if path in (args.purelib / LOCK, args.purelib / RECOVERY):
            raise ValueError("operator input overlaps recovery control files")
        for other in destinations[index + 1:]:
            if not disjoint(path, other):
                raise ValueError("backup inputs overlap")
        for anchor in (backup, args.artifact_dir, args.public_source):
            if not disjoint(path, anchor):
                raise ValueError("private input overlaps artifact, checkout or backup")
        if path == args.hash_record or any(path == value for value in
                                          [*args.config, *args.private_file, *args.payload_file,
                                           *args.state_db, *args.optional_state_db]):
            if not disjoint(path, args.purelib) or not disjoint(path, args.scripts):
                raise ValueError("private input overlaps runtime installation")
    safe_parent(args.hash_record.parent, private=False, windows=args.layout == "windows-user-site")
    total = sum(path.stat().st_size for destination in destinations
                for path in ([destination] if destination.is_file() else destination.rglob("*"))
                if path.is_file())
    # WAL and staging can exceed the main database size. Check both filesystems.
    total += sum(Path(str(path) + "-wal").stat().st_size for path in destinations
                 if Path(str(path) + "-wal").is_file())
    needed = total * 2 + (args.artifact_dir / BUILDER["wheel_name"](
        BUILDER["load_pin"](args.pin))).stat().st_size + 16 * 1024 * 1024
    for parent in (backup, args.purelib):
        if shutil.disk_usage(parent).free < needed:
            raise ValueError("insufficient backup or staging space")


def validate_schema2_record(record: dict) -> dict:
    base = {"schema", "operation", "phase", "backup", "binding", "inventory_sha256",
            "owned", "predecessor", "successor"}
    if (not isinstance(record, dict) or type(record.get("schema")) is not int
            or record["schema"] != 2 or record.get("operation") != "upgrade"
            or record.get("phase") not in SCHEMA2_PHASE_FIELDS):
        raise ValueError("invalid recovery record")
    optional = {"after"} if record["phase"] == "mutation-started" and "after" in record else set()
    if set(record) != base | SCHEMA2_PHASE_FIELDS[record["phase"]] | optional:
        raise ValueError("invalid recovery record")
    if (not isinstance(record["backup"], str)
            or not isinstance(record["binding"], dict)
            or not isinstance(record["owned"], dict)
            or not isinstance(record["inventory_sha256"], str)
            or not SHA256.fullmatch(record["inventory_sha256"])):
        raise ValueError("invalid recovery record")
    for key in ("predecessor", "successor"):
        if not isinstance(record[key], dict):
            raise ValueError("invalid recovery record")
    if (set(record["predecessor"]) != {"record_sha256", "backup", "inventory_sha256", "pin", "manifest"}
            or set(record["successor"]) != {"pin", "manifest"}
            or any(not isinstance(record["predecessor"].get(key), str)
                   or not SHA256.fullmatch(record["predecessor"][key])
                   for key in ("record_sha256", "inventory_sha256"))
            or not isinstance(record["predecessor"].get("backup"), str)):
        raise ValueError("invalid recovery record")
    validate_pin(record["predecessor"]["pin"])
    validate_pin(record["successor"]["pin"])
    if (pin_from_manifest(record["predecessor"]["manifest"]) != record["predecessor"]["pin"]
            or pin_from_manifest(record["successor"]["manifest"]) != record["successor"]["pin"]):
        raise ValueError("invalid recovery record")
    if "rollback_ready_sha256" in record and (
            not isinstance(record["rollback_ready_sha256"], str)
            or not SHA256.fullmatch(record["rollback_ready_sha256"])):
        raise ValueError("invalid recovery record")
    return record


def read_record_bytes(purelib: Path) -> tuple[bytes, dict] | tuple[None, None]:
    path = checked_path(purelib / RECOVERY)
    if not path.exists():
        return None, None
    private_file(path)
    raw = path.read_bytes()
    record = json.loads(raw)
    if record.get("schema") == 2:
        validate_schema2_record(record)
    elif record.get("schema") != 1 or record.get("phase") not in {
            "preparing", "backup-complete", "mutation-started", "verified", "rolled-back"}:
        raise ValueError("invalid recovery record")
    return raw, record


def read_record(purelib: Path) -> dict | None:
    return read_record_bytes(purelib)[1]


def predecessor_args(args, record: dict) -> argparse.Namespace:
    saved = record.get("binding")
    if not isinstance(saved, dict) or set(saved) != BINDING_FIELDS:
        raise ValueError("predecessor binding structure mismatch")
    for key in BINDING_PATH_FIELDS:
        value = saved.get(key)
        if not isinstance(value, str) or not value:
            raise ValueError("predecessor binding path is invalid")
        checked_path(Path(value), exists=key in {"artifact_dir", "public_source"},
                     interpreter=key == "python")
    for key in BINDING_LIST_FIELDS:
        values = saved.get(key)
        if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
            raise ValueError("predecessor binding inventory is invalid")
        for value in values:
            checked_path(Path(value))
    selected = saved.get("selected_target")
    if (not isinstance(selected, dict) or set(selected) != {"agent_state_dir", "t3_url"}
            or any(not isinstance(selected[key], str) or not selected[key]
                   for key in selected)):
        raise ValueError("predecessor selected target is invalid")
    if not isinstance(saved.get("layout"), str) or not isinstance(saved.get("target_host"), str):
        raise ValueError("predecessor binding scalar is invalid")
    current = binding(args)
    if any(saved[key] != current[key] for key in STABLE_BINDING_FIELDS):
        raise ValueError("predecessor stable binding changed")
    old = argparse.Namespace(**vars(args))
    old.artifact_dir = Path(saved["artifact_dir"])
    old.public_source = Path(saved["public_source"])
    backup = record.get("backup")
    if not isinstance(backup, str):
        raise ValueError("predecessor transaction is invalid")
    old.backup = checked_path(Path(backup), exists=True)
    return old


def expected_inventory_descriptors(args) -> list[dict]:
    entries = []
    def add(path, kind, required, runtime=False, category="state-db"):
        entries.append({"label": f"input-{len(entries):04d}", "destination": str(checked_path(path)),
                        "kind": kind, "runtime": runtime, "category": category,
                        "required": required})
    add(args.purelib / "t3_fleet", "tree", True, True, "runtime-package")
    for launcher in (args.fleet_launcher, args.agent_launcher):
        add(launcher, "file", True, False, "entrypoint")
    for wrapper in args.wrapper:
        add(wrapper, "file", True, False, "outer-wrapper")
    add(args.hash_record, "file", False, True, "hash-record")
    for category, paths in (("config", args.config), ("private-file", args.private_file),
                            ("payload-file", args.payload_file)):
        for path in paths:
            add(path, "file", True, False, category)
    for path, required in state_databases(args).items():
        add(path, "sqlite", required)
    return entries


def validate_inventory_descriptors(args, inputs: list[dict]) -> None:
    expected = expected_inventory_descriptors(args)
    if not isinstance(inputs, list) or len(inputs) != len(expected):
        raise ValueError("backup inventory descriptor mismatch")
    for actual, descriptor in zip(inputs, expected):
        comparison = dict(descriptor)
        # Older schema-1 records predate source-wrapper entrypoint rollback.
        # Their integrity-protected runtime flag stays authoritative for that
        # record, so the descriptor comparison accepts either boolean there.
        if descriptor["category"] == "entrypoint":
            comparison.pop("runtime")
        if not isinstance(actual, dict) or any(actual.get(key) != value for key, value in comparison.items()):
            raise ValueError("backup inventory descriptor mismatch")
        if descriptor["category"] == "entrypoint" and type(actual.get("runtime")) is not bool:
            raise ValueError("backup inventory descriptor mismatch")
        if type(actual.get("exists")) is not bool or not isinstance(actual.get("before"), dict):
            raise ValueError("backup inventory descriptor mismatch")
        fields = set(descriptor) | {"exists", "before"}
        if actual["exists"]:
            fields.add("snapshot")
            if not isinstance(actual.get("snapshot"), dict):
                raise ValueError("backup inventory descriptor mismatch")
        if set(actual) != fields:
            raise ValueError("backup inventory descriptor mismatch")


def saved_launcher_fingerprints(inventory: dict) -> dict:
    return {entry["destination"]: entry["before"] for entry in inventory["inputs"]
            if entry["category"] in {"entrypoint", "outer-wrapper"}}


def validate_predecessor_launchers(inventory: dict, record: dict, saved: dict | None) -> None:
    for destination, before in saved_launcher_fingerprints(inventory).items():
        expected = record["after"].get(destination, before)
        actual = fingerprint(Path(destination)) if saved is None else saved.get(destination)
        if actual != expected:
            raise ValueError("predecessor launcher bytes changed")


def validate_preserved_outer_wrappers(inventory: dict) -> None:
    for entry in inventory["inputs"]:
        if (entry["category"] == "outer-wrapper"
                and fingerprint(Path(entry["destination"])) != entry["before"]):
            raise ValueError("outer wrapper bytes changed")


def validate_schema2_predecessor(
    args, raw: bytes, record: dict, *, identity_bytes: bytes, remaining_edges: int,
    saved_launchers: dict | None,
) -> dict:
    """Authenticate a verified upgrade record and its preserved rollback chain."""
    if remaining_edges <= 0:
        raise ValueError("schema-2 predecessor chain is too deep")
    validate_schema2_record(record)
    if record["phase"] != "verified":
        raise ValueError("schema-2 predecessor is not a verified authority")
    old = predecessor_args(args, record)
    artifact = validate_artifact(old, pin=record["successor"]["pin"])
    if artifact["manifest"] != record["successor"]["manifest"]:
        raise ValueError("schema-2 predecessor artifact provenance mismatch")
    try:
        installed_identity = json.loads(identity_bytes)
    except (TypeError, ValueError, UnicodeError):
        raise ValueError("schema-2 predecessor identity bytes mismatch") from None
    if installed_identity != record["successor"]["manifest"]:
        raise ValueError("schema-2 predecessor identity bytes mismatch")
    # Validate B's protected transaction, artifacts and recorded runtime set
    # without requiring the live runtime to still be B. During C rollback the
    # live runtime is C until the staged predecessor is published.
    backup, inventory = validate_upgrade_backup(old, record)
    if any(fingerprint(Path(path))["kind"] != "absent" for path in record["owned"]):
        raise ValueError("predecessor staging cleanup is incomplete")
    validate_predecessor_launchers(inventory, record, saved_launchers)
    identity = next(entry for entry in inventory["inputs"] if entry["category"] == "hash-record")
    expected_identity = record["after"].get(identity["destination"])
    if (not isinstance(expected_identity, dict) or expected_identity.get("kind") != "file"
            or expected_identity.get("sha256") != hashlib.sha256(identity_bytes).hexdigest()):
        raise ValueError("schema-2 predecessor identity bytes mismatch")
    runtime_destinations = {entry["destination"] for entry in inventory["inputs"]
                            if entry["runtime"]}
    if set(record["after"]) != runtime_destinations:
        raise ValueError("schema-2 predecessor runtime fingerprint set mismatch")
    predecessor_raw = (backup / "predecessor-record.json").read_bytes()
    predecessor_record = json.loads(predecessor_raw)
    predecessor_identity = (backup / identity["label"] / "data").read_bytes()
    predecessor = validate_predecessor(
        old, predecessor_raw, predecessor_record, identity_bytes=predecessor_identity,
        remaining_edges=remaining_edges - 1,
        saved_launchers=saved_launcher_fingerprints(inventory),
    )
    expected_predecessor, expected_successor = upgrade_identity_blocks(predecessor, artifact)
    if (record["predecessor"] != expected_predecessor
            or record["successor"] != expected_successor):
        raise ValueError("schema-2 predecessor identity chain changed")
    return {"args": old, "backup": backup, "inventory": inventory,
            "artifact": artifact, "record": record, "manifest": artifact["manifest"],
            "pin": record["successor"]["pin"], "raw_sha256": hashlib.sha256(raw).hexdigest()}


def validate_predecessor(
    args, raw: bytes, record: dict, *, identity_bytes: bytes,
    remaining_edges: int = MAX_PREDECESSOR_EDGES, saved_launchers: dict | None = None,
) -> dict:
    if not isinstance(raw, bytes) or json.loads(raw) != record:
        raise ValueError("predecessor record bytes mismatch")
    if isinstance(record, dict) and record.get("schema") == 2:
        return validate_schema2_predecessor(
            args, raw, record, identity_bytes=identity_bytes, remaining_edges=remaining_edges,
            saved_launchers=saved_launchers,
        )
    if (not isinstance(record, dict)
            or set(record) != {"schema", "phase", "backup", "binding", "inventory_sha256",
                               "owned", "after"}
            or type(record.get("schema")) is not int or record["schema"] != 1
            or record.get("phase") != "verified" or not isinstance(record.get("owned"), dict)
            or not isinstance(record.get("after"), dict)):
        raise ValueError("predecessor record is not a verified schema-1 authority")
    old = predecessor_args(args, record)
    backup, inventory = validate_backup(old, record)
    if (not isinstance(inventory, dict)
            or set(inventory) != {"schema", "binding", "inputs", "execution_policy", "launcher_contract"}
            or type(inventory.get("schema")) is not int or inventory["schema"] != 1
            or inventory.get("launcher_contract") is not None):
        raise ValueError("predecessor inventory structure mismatch")
    validate_inventory_descriptors(old, inventory["inputs"])
    runtime_destinations = {entry["destination"] for entry in inventory["inputs"] if entry["runtime"]}
    if set(record["after"]) != runtime_destinations:
        raise ValueError("predecessor runtime fingerprint set mismatch")
    identity = next(entry for entry in inventory["inputs"] if entry["category"] == "hash-record")
    expected_identity = record["after"].get(identity["destination"])
    if (not isinstance(expected_identity, dict) or expected_identity.get("kind") != "file"
            or expected_identity.get("sha256") != hashlib.sha256(identity_bytes).hexdigest()):
        raise ValueError("predecessor identity bytes mismatch")
    manifest = json.loads(identity_bytes)
    pin = pin_from_manifest(manifest)
    artifact = validate_artifact(old, pin=pin)
    staged_manifest = BUILDER["verify_artifacts"](
        backup / "artifacts", pin, artifact["source_files"])
    if artifact["manifest"] != manifest or staged_manifest != manifest:
        raise ValueError("predecessor artifact provenance mismatch")
    token = backup.name.removeprefix("transaction-")
    expected_owned = {
        str(args.purelib / (".t3-fleet-old-" + token)),
        str(args.purelib / (".t3-fleet-stage-" + token)),
    }
    if set(record["owned"]) != expected_owned:
        raise ValueError("predecessor owned path set mismatch")
    if any(fingerprint(Path(path))["kind"] != "absent" for path in record["owned"]):
        raise ValueError("predecessor staging cleanup is incomplete")
    validate_predecessor_launchers(inventory, record, saved_launchers)
    return {"args": old, "backup": backup, "inventory": inventory, "artifact": artifact,
            "record": record,
            "manifest": manifest, "pin": pin,
            "raw_sha256": hashlib.sha256(raw).hexdigest()}


def validate_new_transaction(args, predecessor_backup: Path) -> Path:
    if args.backup is None:
        raise ValueError("upgrade requires an explicit new transaction")
    transaction = checked_path(args.backup)
    if (transaction.parent != args.backup_parent
            or not re.fullmatch(r"transaction-[0-9a-f]{32}", transaction.name)
            or transaction == predecessor_backup or transaction.exists()):
        raise ValueError("new upgrade transaction path is unavailable")
    return transaction


def plan_upgrade(args) -> dict:
    guard_interpreter_anchor(args, "artifact-invalid")
    if os.name != "posix" or args.layout != "source-wrapper":
        raise ValueError("upgrade supports only POSIX source-wrapper installations")
    successor = validate_artifact(args)
    args._code = "recovery-invalid"
    if checked_path(args.purelib / RECOVERY).exists():
        args._recovery = "preserve-record-and-backup"
    raw, record = read_record_bytes(checked_path(args.purelib, exists=True))
    if record:
        args._recovery = "preserve-record-and-backup"
    if record and record.get("schema") == 2:
        if record["phase"] != "verified":
            args._code = "recovery-required"
            raise ValueError("unfinished upgrade requires explicit rollback")
        if successor["manifest"] == record["successor"]["manifest"]:
            if inspect_layout(args)["wrapper_migration"]:
                args._code = "legacy-wrapper-new-pin-required"
                raise ValueError("migrating legacy wrappers requires a new reviewed pin")
            state = validate_upgrade_state(args, raw, record, successor=successor)
            transaction = checked_path(args.backup, exists=True)
            if transaction != state["backup"]:
                raise ValueError("only the active upgrade transaction can be finalized")
            outstanding = any(fingerprint(Path(path))["kind"] != "absent"
                              for path in record["owned"])
            return {**state, "transaction": transaction, "record_raw": raw,
                    "operation": "finalize" if outstanding else "unchanged"}
        if args.backup is None:
            raise ValueError("a new pin requires an explicit new transaction")
        predecessor = validate_predecessor(
            args, raw, record, identity_bytes=args.hash_record.read_bytes())
    elif record and record.get("schema") == 1 and record.get("phase") == "verified":
        predecessor = validate_predecessor(
            args, raw, record, identity_bytes=args.hash_record.read_bytes())
    else:
        raise ValueError("upgrade requires one verified schema-1 or schema-2 predecessor")
    args._code = "layout-invalid"
    layout = inspect_layout(args)
    args._code = "inventory-invalid"
    inputs = inventory_backup(args, layout)
    execution_inputs(inputs)
    args._code = "boundaries-invalid"
    check_boundaries(args, inputs)
    transaction = validate_new_transaction(args, predecessor["backup"])
    old_args = predecessor["args"]
    args._code = "runtime-drift"
    check_rollback_drift(old_args, record, predecessor["inventory"], predecessor["artifact"])
    if predecessor["manifest"] == successor["manifest"] and not layout.get("wrapper_migration"):
        if (old_args.artifact_dir != args.artifact_dir
                or old_args.public_source != args.public_source):
            raise ValueError("same identity cannot be rebound to different versioned paths")
        current_inputs = [dict(entry, before=fingerprint(Path(entry["destination"]))) for entry in inputs]
        verify_installed(args, {"artifact": successor, "inputs": current_inputs,
                                "launcher_contract": layout.get("launcher_contract")})
        return {"artifact": successor, "layout": layout, "inputs": inputs,
                "predecessor": predecessor, "transaction": transaction, "record": record,
                "record_raw": raw, "operation": "unchanged"}
    for left, right in (
            (old_args.artifact_dir, args.artifact_dir),
            (old_args.artifact_dir, args.public_source),
            (old_args.public_source, args.artifact_dir),
            (old_args.public_source, args.public_source),
            (transaction, predecessor["backup"])):
        if not disjoint(left, right):
            raise ValueError("predecessor and successor paths overlap")
    return {"artifact": successor, "layout": layout, "inputs": inputs,
            "predecessor": predecessor, "transaction": transaction, "record": record,
            "record_raw": raw, "operation": "upgrade"}


def upgrade_identity_blocks(predecessor: dict, successor: dict) -> tuple[dict, dict]:
    return (
        {"record_sha256": predecessor["raw_sha256"],
         "backup": str(predecessor["backup"]),
         "inventory_sha256": predecessor["record"]["inventory_sha256"],
         "pin": predecessor["pin"], "manifest": predecessor["manifest"]},
        {"pin": successor["pin"], "manifest": successor["manifest"]},
    )


def require_upgrade_authority(args, expected_record_sha256: str,
                              expected_binding: dict) -> None:
    guard_interpreter_anchor(args, getattr(args, "_code", "recovery-recheck-failed"))
    require_binding(args, expected_binding)
    raw, record = read_record_bytes(args.purelib)
    if (raw is None or hashlib.sha256(raw).hexdigest() != expected_record_sha256
            or not isinstance(record, dict)):
        raise ValueError("active recovery authority changed")


def validate_upgrade_backup(args, record: dict) -> tuple[Path, dict]:
    validate_schema2_record(record)
    backup = checked_path(args.backup or Path(record["backup"]), exists=True)
    safe_parent(backup, private=True)
    current_binding = binding(args)
    if (backup.parent != args.backup_parent
            or not re.fullmatch(r"transaction-[0-9a-f]{32}", backup.name)
            or record["backup"] != str(backup) or record["binding"] != current_binding):
        raise ValueError("upgrade backup does not match the active authority")
    inventory_path = backup / "inventory.json"
    private_file(inventory_path)
    if sha(inventory_path) != record["inventory_sha256"]:
        raise ValueError("upgrade inventory is incomplete or changed")
    inventory = json.loads(inventory_path.read_bytes())
    if (not isinstance(inventory, dict)
            or set(inventory) != {"schema", "operation", "binding", "inputs", "execution_policy",
                                  "launcher_contract", "predecessor", "successor",
                                  "source_mtimes_ns"}
            or type(inventory.get("schema")) is not int or inventory["schema"] != 2
            or inventory.get("operation") != "upgrade"
            or inventory.get("binding") != current_binding
            or inventory.get("execution_policy") != 1
            or inventory.get("launcher_contract") is not None
            or inventory.get("predecessor") != record["predecessor"]
            or inventory.get("successor") != record["successor"]):
        raise ValueError("upgrade inventory structure mismatch")
    source_names = manifest_source_names(record["predecessor"]["manifest"])
    source_mtimes_ns = inventory["source_mtimes_ns"]
    if (not isinstance(source_mtimes_ns, dict)
            or set(source_mtimes_ns) != source_names
            or any(type(value) is not int or value < 0
                   for value in source_mtimes_ns.values())):
        raise ValueError("upgrade snapshot source timestamp set changed")
    validate_inventory_descriptors(args, inventory["inputs"])
    execution_inputs(inventory["inputs"], saved=True)
    predecessor_copy = backup / "predecessor-record.json"
    private_file(predecessor_copy)
    if sha(predecessor_copy) != record["predecessor"]["record_sha256"]:
        raise ValueError("predecessor record copy changed")
    predecessor_record = json.loads(predecessor_copy.read_bytes())
    predecessor_after = predecessor_record.get("after")
    if not isinstance(predecessor_after, dict):
        raise ValueError("predecessor record copy changed")
    for entry in inventory["inputs"]:
        if entry["exists"]:
            saved = checked_path(backup / entry["label"] / "data", exists=True)
            if fingerprint(saved) != entry["snapshot"]:
                raise ValueError("upgrade snapshot content changed")
            for file in ([saved] if saved.is_file() else tree_files(saved).values()):
                private_file(file)
            if (entry["kind"] != "sqlite"
                    and entry["snapshot"] != protected_snapshot_fingerprint(entry["before"])):
                raise ValueError("upgrade snapshot differs from captured baseline")
            if entry["category"] == "runtime-package":
                public_package_files(
                    args, {"manifest": record["predecessor"]["manifest"]}, package=saved,
                    logical_package=args.purelib / "t3_fleet")
                expected_after = predecessor_after.get(entry["destination"])
                if (not isinstance(expected_after, dict)
                        or normalized_public_fingerprint(entry["before"])
                            != normalized_public_fingerprint(expected_after)):
                    raise ValueError("captured predecessor differs from its authority")
                if (package_source_mtimes_ns(saved, record["predecessor"]["manifest"])
                        != source_mtimes_ns):
                    raise ValueError("upgrade snapshot source timestamps changed")
    token = backup.name.removeprefix("transaction-")
    package = args.purelib / "t3_fleet"
    expected_owned = {
        str(package.parent / (".t3-fleet-old-" + token)):
            next(entry["before"] for entry in inventory["inputs"]
                 if entry["category"] == "runtime-package"),
        str(package.parent / (".t3-fleet-stage-" + token)):
            private_stage_identity(record["successor"]["manifest"]["modules"]),
    }
    if record["owned"] != expected_owned:
        raise ValueError("upgrade owned path set mismatch")
    successor = validate_artifact(args, pin=record["successor"]["pin"])
    if (successor["manifest"] != record["successor"]["manifest"]
            or BUILDER["verify_artifacts"](
                backup / "artifacts", successor["pin"], successor["source_files"])
                != successor["manifest"]):
        raise ValueError("successor artifact provenance mismatch")
    return backup, inventory


def validate_upgrade_state(args, raw: bytes, record: dict, *, successor: dict | None = None) -> dict:
    if raw is None or json.loads(raw) != record:
        raise ValueError("active upgrade record bytes mismatch")
    validate_schema2_record(record)
    successor = validate_artifact(args) if successor is None else successor
    if successor["manifest"] != record["successor"]["manifest"]:
        raise ValueError("active upgrade successor differs from the reviewed pin")
    backup, inventory = validate_upgrade_backup(args, record)
    check_boundaries(args, inventory["inputs"])
    validate_preserved_outer_wrappers(inventory)
    identity_entry = next(entry for entry in inventory["inputs"]
                          if entry["category"] == "hash-record")
    protected_identity = (backup / identity_entry["label"] / "data").read_bytes()
    predecessor_raw = (backup / "predecessor-record.json").read_bytes()
    predecessor_record = json.loads(predecessor_raw)
    predecessor = validate_predecessor(
        args, predecessor_raw, predecessor_record, identity_bytes=protected_identity,
        saved_launchers=saved_launcher_fingerprints(inventory),
    )
    if (record["predecessor"] != upgrade_identity_blocks(predecessor, successor)[0]
            or record["successor"] != upgrade_identity_blocks(predecessor, successor)[1]):
        raise ValueError("upgrade identity chain changed")
    runtime = classify_upgrade_runtime(
        args, record, inventory,
        {"predecessor": predecessor["artifact"],
         "predecessor_args": predecessor["args"], "successor": successor})
    if record["phase"] == "verified":
        if runtime["package"] != "successor" or runtime["identity"] != "successor":
            raise ValueError("verified upgrade runtime changed")
        expected = record["after"]
        if set(expected) != {entry["destination"] for entry in inventory["inputs"]
                             if entry["runtime"]}:
            raise ValueError("verified upgrade fingerprint set changed")
        for entry in inventory["inputs"]:
            if not entry["runtime"]:
                continue
            actual = runtime["runtime"][entry["destination"]]
            if actual == expected[entry["destination"]]:
                continue
            if (entry["category"] == "runtime-package" and actual["kind"] == "tree"
                    and public_fingerprint(args, successor, actual)
                    == expected[entry["destination"]]):
                continue
            raise ValueError("verified upgrade runtime fingerprint changed")
    return {"artifact": successor, "layout": {"package": args.purelib / "t3_fleet",
             "launcher_contract": inventory.get("launcher_contract")},
            "inputs": inventory["inputs"], "predecessor": predecessor,
            "backup": backup, "inventory": inventory, "record": record,
            "runtime": runtime}


def matches_successor_stage_subset(path: Path, actual: dict, manifest: dict) -> bool:
    if actual.get("kind") != "tree" or actual.get("directories"):
        return False
    expected = {name.removeprefix("t3_fleet/"): digest
                for name, digest in manifest["modules"].items()}
    if not actual["files"].keys() <= expected.keys():
        return False
    for name, value in actual["files"].items():
        if value.get("sha256") != expected[name] or value.get("mode") != 0o600:
            return False
    return actual.get("mode") == 0o700


def matches_tree_subset(actual: dict, expected: dict) -> bool:
    if actual.get("kind") != "tree" or expected.get("kind") != "tree":
        return False
    if ({key: value for key, value in actual.items() if key not in {"files", "directories"}}
            != {key: value for key, value in expected.items()
                if key not in {"files", "directories"}}):
        return False
    if (not actual["files"].keys() <= expected["files"].keys()
            or not actual["directories"].keys() <= expected["directories"].keys()):
        return False
    return (all(value == expected["files"][name]
                for name, value in actual["files"].items())
            and all(value == expected["directories"][name]
                    for name, value in actual["directories"].items()))


def matches_upgrade_owned(record: dict, path: Path, actual: dict, expected: dict) -> bool:
    if actual.get("kind") == "absent" or actual == expected:
        return True
    if matches_private_stage(path, actual, expected):
        return True
    if (record["phase"] in {"mutation-started", "rollback-started"}
            and path.name.startswith(".t3-fleet-stage-")
            and matches_successor_stage_subset(
                path, actual, record["successor"]["manifest"])):
        return True
    return (record["phase"] in {"verified", "rollback-started"}
            and matches_tree_subset(actual, expected))


def classify_upgrade_runtime(args, record: dict, inventory: dict,
                             artifacts: dict) -> dict:
    package_entry = next(entry for entry in inventory["inputs"]
                         if entry["category"] == "runtime-package")
    identity_entry = next(entry for entry in inventory["inputs"]
                          if entry["category"] == "hash-record")
    package = Path(package_entry["destination"])
    actual_package = fingerprint(package)
    if actual_package == package_entry["before"]:
        package_state = "predecessor"
    elif actual_package["kind"] == "absent":
        package_state = "absent"
    else:
        package_state = None
        for label, artifact, artifact_args in (
                ("successor", artifacts["successor"], args),
                ("predecessor", artifacts["predecessor"], artifacts["predecessor_args"])):
            try:
                public_package_files(artifact_args, artifact)
            except ValueError:
                continue
            package_state = label
            break
        if package_state is None:
            raise ValueError("unknown upgrade runtime package bytes")
    identity_path = Path(identity_entry["destination"])
    actual_identity = fingerprint(identity_path)
    identity_state = "absent"
    if actual_identity["kind"] != "absent":
        if actual_identity["kind"] != "file":
            raise ValueError("unknown upgrade identity object")
        try:
            identity_manifest = json.loads(identity_path.read_bytes())
        except (ValueError, UnicodeError):
            raise ValueError("unknown upgrade identity bytes") from None
        if identity_manifest == record["predecessor"]["manifest"]:
            identity_state = "predecessor"
        elif identity_manifest == record["successor"]["manifest"]:
            identity_state = "successor"
        else:
            raise ValueError("unknown upgrade identity bytes")
    if (package_state in {"predecessor", "successor"} and identity_state != "absent"
            and record["predecessor"]["manifest"]["modules"]
                == record["successor"]["manifest"]["modules"]):
        package_state = identity_state
    allowed = {
        "upgrade-intent": {("predecessor", "predecessor")},
        "mutation-started": {("predecessor", "predecessor"), ("absent", "predecessor"),
                             ("successor", "predecessor"), ("successor", "successor")},
        "verified": {("successor", "successor")},
        "rollback-started": {(left, right) for left in ("predecessor", "successor", "absent")
                             for right in ("predecessor", "successor", "absent")},
        "predecessor-ready": {("predecessor", "predecessor")},
    }
    if (package_state, identity_state) not in allowed[record["phase"]]:
        raise ValueError("upgrade runtime state does not match its authority phase")
    for path_text, expected in record["owned"].items():
        path = Path(path_text)
        actual = fingerprint(path)
        if matches_upgrade_owned(record, path, actual, expected):
            continue
        raise ValueError("unknown upgrade-owned content")
    return {"package": package_state, "identity": identity_state,
            "runtime": runtime_fingerprints(inventory["inputs"])}


def cleanup_upgrade_owned(args, record: dict, inventory: dict,
                          expected_record_sha256: str, expected_binding: dict) -> None:
    owned = {Path(path): expected for path, expected in record["owned"].items()}
    if "rollback" in record:
        backup = Path(record["backup"])
        for entry in inventory["inputs"]:
            if entry["runtime"]:
                stage, retired = restore_paths(backup, entry)
                progress = record["rollback"][entry["destination"]]
                if progress["stage"] != str(stage) or progress["retired"] != str(retired):
                    raise ValueError("upgrade rollback cleanup path mismatch")
                owned[stage] = entry["before"]
                owned[retired] = progress["start"]
    for path, expected in owned.items():
        actual = fingerprint(path)
        if matches_upgrade_owned(record, path, actual, expected):
            continue
        raise ValueError("transaction-owned cleanup content changed")
    for path, expected in owned.items():
        require_upgrade_authority(args, expected_record_sha256, expected_binding)
        actual = fingerprint(path)
        if not matches_upgrade_owned(record, path, actual, expected):
            raise ValueError("transaction-owned cleanup content changed at deletion edge")
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
        fsync_directory(path.parent)


def plan(args) -> dict:
    guard_interpreter_anchor(args, "artifact-invalid")
    artifact = validate_artifact(args)
    args._code = "recovery-invalid"
    raw, record = read_record_bytes(checked_path(args.purelib, exists=True))
    if record:
        args._recovery = "preserve-record-and-backup"
    if record and record.get("schema") == 2:
        if record["phase"] != "verified":
            args._code = "recovery-required"
            raise ValueError("unfinished upgrade requires explicit rollback")
        if artifact["manifest"] != record["successor"]["manifest"]:
            raise ValueError("a new pin requires the explicit upgrade action")
        if inspect_layout(args)["wrapper_migration"]:
            args._code = "legacy-wrapper-new-pin-required"
            raise ValueError("migrating legacy wrappers requires a new reviewed pin")
        return validate_upgrade_state(args, raw, record, successor=artifact)
    if record and record["phase"] not in {"verified", "rolled-back"}:
        args._code = "recovery-required"
        raise ValueError("unfinished transaction requires explicit recovery")
    args._code = "layout-invalid"
    layout = inspect_layout(args)
    args._code = "inventory-invalid"
    inputs = inventory_backup(args, layout)
    execution_inputs(inputs)
    args._code = "boundaries-invalid"
    check_boundaries(args, inputs)
    if record:
        args._code = "backup-invalid"
        _, inventory = validate_backup(args, record)
        if layout.get("launcher_contract") != inventory.get("launcher_contract"):
            raise ValueError("saved launcher toolchain contract changed")
        args._code = "runtime-drift"
        check_rollback_drift(args, record, inventory, artifact)
    # A verified transaction's original inventory remains the rollback baseline.
    # New pip objects may legitimately have inherited ACLs. Never rebaseline them.
    if not record or record["phase"] != "verified":
        if os.name == "nt":
            args._code = "windows-baseline-ineligible"
        windows_runtime_baseline(inputs)
    return {"artifact": artifact, "layout": layout, "inputs": inputs}


def plan_report(args, prepared: dict) -> dict:
    """Expose only bounded labels and actions, never operator-supplied text."""
    changes = ["replace-runtime-package", "publish-verified-hash-record"]
    if args.layout == "source-wrapper":
        changes.append("migrate-legacy-source-wrappers"
                       if prepared.get("layout", {}).get("wrapper_migration")
                       else "preserve-entrypoints")
    else:
        changes.extend(["replace-distribution-metadata", "regenerate-owned-entrypoints"])
    return {
        "checks": {name: "passed" for name in
                   ("artifact", "layout", "backup-inventory", "boundaries")},
        "changes": changes,
        "backup_inputs": [{"label": entry["label"], "category": entry["category"],
                           "kind": entry["kind"], "required": entry["required"],
                           "presence": "existing" if entry["exists"] else "absent",
                           "runtime_rollback": entry["runtime"]} for entry in prepared["inputs"]],
        "external_gates": [{"gate": name, "status": "operator-required"} for name in
                           ("hub-health-and-claims", "dispatch-hold-and-queue-reconciliation",
                            "availability-window", "quiescence", "loopback-listener-and-descriptor",
                            "scheduled-task-context", "post-install-health-and-canary")],
    }


def upgrade(args) -> str:
    prepared = plan_upgrade(args)
    if prepared["operation"] == "unchanged":
        return "unchanged"
    initial_binding = binding(args)
    initial_predecessor_sha = prepared["predecessor"]["raw_sha256"]
    args._phase, args._writes, args._code = "locking", "possible", "lock-unavailable"
    with local_lock(args.purelib):
        prepared = plan_upgrade(args)
        if prepared["operation"] == "unchanged":
            return "unchanged"
        if prepared["operation"] == "finalize":
            expected_record_sha = hashlib.sha256(prepared["record_raw"]).hexdigest()
            args._phase, args._writes, args._code = (
                "verified", "possible", "owned-cleanup-failed")
            cleanup_upgrade_owned(
                args, prepared["record"], prepared["inventory"],
                expected_record_sha, initial_binding)
            return "finalized"
        if (binding(args) != initial_binding
                or prepared["predecessor"]["raw_sha256"] != initial_predecessor_sha):
            raise ValueError("upgrade admission changed under lock")
        predecessor = prepared["predecessor"]
        transaction = prepared["transaction"]
        predecessor_block, successor_block = upgrade_identity_blocks(
            predecessor, prepared["artifact"])
        args._phase, args._writes, args._code = "snapshotting", "started", "snapshot-failed"
        mkdir_private(transaction)
        write_private(transaction / "predecessor-record.json", prepared["record_raw"])
        inventory = snapshot(
            transaction,
            prepared["inputs"],
            args,
            launcher_contract=prepared["layout"].get("launcher_contract"),
            preserve_mtime=True,
        )
        package_entry = next(entry for entry in inventory["inputs"]
                             if entry["category"] == "runtime-package")
        saved_package = transaction / package_entry["label"] / "data"
        inventory = {
            "schema": 2,
            "operation": "upgrade",
            "binding": inventory["binding"],
            "inputs": inventory["inputs"],
            "execution_policy": inventory["execution_policy"],
            "launcher_contract": inventory["launcher_contract"],
            "predecessor": predecessor_block,
            "successor": successor_block,
            "source_mtimes_ns": package_source_mtimes_ns(
                saved_package, predecessor_block["manifest"]),
        }
        atomic_json(transaction / "inventory.json", inventory)
        wheel = stage_artifact(transaction, prepared)
        prepared["staged_wheel"] = wheel
        prepared["inputs"] = inventory["inputs"]
        token = transaction.name.removeprefix("transaction-")
        package = prepared["layout"]["package"]
        record = {
            "schema": 2,
            "operation": "upgrade",
            "phase": "upgrade-intent",
            "backup": str(transaction),
            "binding": initial_binding,
            "inventory_sha256": sha(transaction / "inventory.json"),
            "owned": {
                str(package.parent / (".t3-fleet-old-" + token)):
                    next(entry["before"] for entry in inventory["inputs"]
                         if entry["category"] == "runtime-package"),
                str(package.parent / (".t3-fleet-stage-" + token)):
                    private_stage_identity(prepared["artifact"]["manifest"]["modules"]),
            },
            "predecessor": predecessor_block,
            "successor": successor_block,
        }
        if any(checked_path(Path(path)).exists() for path in record["owned"]):
            raise ValueError("transaction-owned staging path is occupied")
        identity_entry = next(entry for entry in inventory["inputs"]
                              if entry["category"] == "hash-record")
        protected_identity = (transaction / identity_entry["label"] / "data").read_bytes()
        validate_predecessor(
            args,
            (transaction / "predecessor-record.json").read_bytes(),
            prepared["record"],
            identity_bytes=protected_identity,
        )
        validate_upgrade_backup(args, record)
        fsync_directory(transaction)
        fsync_directory(args.backup_parent)
        args._code = "baseline-changed"
        require_upgrade_authority(args, initial_predecessor_sha, initial_binding)
        record_path = args.purelib / RECOVERY
        active_raw = record_bytes(record)
        atomic_record_bytes(
            record_path, active_raw,
            before_replace=lambda: require_upgrade_authority(
                args, initial_predecessor_sha, initial_binding),
        )
        expected_record_sha = hashlib.sha256(active_raw).hexdigest()
        args._recovery = "preserve-record-and-backup"

        args._phase, args._code = "upgrade-intent", "baseline-changed"
        require_upgrade_authority(args, expected_record_sha, initial_binding)
        record["phase"] = "mutation-started"
        active_raw = record_bytes(record)
        atomic_record_bytes(
            record_path, active_raw,
            before_replace=lambda: require_upgrade_authority(
                args, expected_record_sha, initial_binding),
        )
        expected_record_sha = hashlib.sha256(active_raw).hexdigest()

        args._phase, args._code = "mutation-started", "install-failed"
        install(
            args,
            prepared,
            wheel,
            transaction,
            initial_binding,
            authority=lambda: require_upgrade_authority(
                args, expected_record_sha, initial_binding),
        )
        args._code = "verification-failed"
        verify_installed(args, prepared)
        args._code = "identity-publication-failed"
        require_upgrade_authority(args, expected_record_sha, initial_binding)
        atomic_record_bytes(
            args.hash_record, record_bytes(prepared["artifact"]["manifest"]),
            before_replace=lambda: require_upgrade_authority(
                args, expected_record_sha, initial_binding),
        )
        execution_inputs(inventory["inputs"])
        after = runtime_fingerprints(inventory["inputs"])
        require_upgrade_authority(args, expected_record_sha, initial_binding)
        record["phase"] = "verified"
        record["after"] = after
        active_raw = record_bytes(record)
        atomic_record_bytes(
            record_path, active_raw,
            before_replace=lambda: require_upgrade_authority(
                args, expected_record_sha, initial_binding),
        )
        expected_record_sha = hashlib.sha256(active_raw).hexdigest()
        args._code = "owned-cleanup-failed"
        cleanup_upgrade_owned(
            args, record, inventory, expected_record_sha, initial_binding)
    return "upgraded"


def private_file(path: Path) -> None:
    checked_path(path, exists=True)
    if not path.is_file():
        raise ValueError("protected record is not a file")
    if os.name == "nt":
        windows_acl(path)
    elif path.stat().st_uid != os.geteuid() or path.stat().st_mode & 0o077:
        raise ValueError("protected record permissions are unsafe")


def mkdir_private(path: Path) -> None:
    checked_path(path)
    if os.name == "nt":
        with closing(WindowsSecurity()) as security:
            with security.private_object(path, directory=True):
                pass
        return
    path.mkdir(mode=0o700)
    safe_parent(path, private=True)


def write_private(path: Path, data: bytes) -> None:
    checked_path(path)
    if os.name == "nt":
        with closing(WindowsSecurity()) as security:
            with security.private_object(path) as handle:
                offset = 0
                while offset < len(data):
                    written = security.write_chunk(handle, data[offset:offset + 1024 * 1024])
                    if not 0 < written <= min(len(data) - offset, 1024 * 1024):
                        raise ValueError("Windows private write made no progress")
                    offset += written
                security.ok(security.k.FlushFileBuffers(handle))
        return
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    private_file(path)


def fsync_directory(path: Path) -> None:
    if os.name == "posix":
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def atomic_json(path: Path, value: dict) -> None:
    if os.name == "nt":
        with closing(WindowsSecurity()) as security:
            with security.parents(path):
                _atomic_json(path, value)
    else:
        _atomic_json(path, value)


def _atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + ".new-" + uuid.uuid4().hex)
    write_private(temporary, (json.dumps(value, sort_keys=True, indent=2) + "\n").encode())
    try:
        checked_path(path)
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink()
        raise
    fsync_directory(path.parent)


def record_bytes(value: dict) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def atomic_record_bytes(path: Path, raw: bytes, *, before_replace=None) -> None:
    if not isinstance(raw, bytes):
        raise ValueError("record publication requires exact bytes")
    checked_path(path)
    temporary = path.parent / (".runtime-record-" + uuid.uuid4().hex)
    def publish():
        write_private(temporary, raw)
        if before_replace is not None:
            before_replace()
        os.replace(temporary, path)
        fsync_directory(path.parent)
    if os.name == "nt":
        with closing(WindowsSecurity()) as security:
            with security.parents(path):
                publish()
    else:
        publish()


@contextmanager
def local_lock(purelib: Path):
    path = checked_path(purelib / LOCK)
    if os.name == "nt":
        with closing(WindowsSecurity()) as security:
            with security.private_object(path, reopen=True):
                # Share-zero is the OS-released lock. OPEN_EXISTING never
                # truncates and is checked on the same exclusive handle.
                yield
        return
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        private_file(path)
        try:
            if os.name == "nt":
                import msvcrt
                # locking a byte beyond EOF is supported and does not change file content.
                msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise ValueError("another local operator holds the runtime lock") from None
        yield
    finally:
        os.close(descriptor)


def fingerprint(path: Path) -> dict:
    checked_path(path)
    if not path.exists():
        return {"kind": "absent"}
    if path.is_file():
        return {"kind": "file", "sha256": sha(path), **permissions(path)}
    files = tree_files(path)
    return {"kind": "tree", **permissions(path),
            "files": {name: {"sha256": sha(file), **permissions(file)}
                      for name, file in sorted(files.items())},
            "directories": {child.relative_to(path).as_posix(): permissions(child)
                            for child in sorted(path.rglob("*")) if child.is_dir()}}


def protected_snapshot_fingerprint(before: dict) -> dict:
    if before.get("kind") == "file":
        return {"kind": "file", "sha256": before["sha256"], "mode": 0o600}
    if before.get("kind") == "tree":
        return {
            "kind": "tree", "mode": 0o700,
            "files": {name: {"sha256": value["sha256"], "mode": 0o600}
                      for name, value in before["files"].items()},
            "directories": {name: {"mode": 0o700} for name in before["directories"]},
        }
    return before


def normalized_public_fingerprint(actual: dict) -> dict:
    if actual.get("kind") != "tree":
        return actual
    return {
        **actual,
        "files": {name: value for name, value in actual["files"].items()
                  if not name.startswith("__pycache__/")},
        "directories": {name: value for name, value in actual["directories"].items()
                        if name != "__pycache__"},
    }


def manifest_source_names(manifest: dict) -> set[str]:
    return {name.removeprefix("t3_fleet/") for name in manifest["modules"]}


def package_source_mtimes_ns(package: Path, manifest: dict) -> dict[str, int]:
    expected = manifest_source_names(manifest)
    files = tree_files(package)
    if not expected or not expected <= files.keys():
        raise ValueError("upgrade snapshot source timestamp set changed")
    result = {name: files[name].stat().st_mtime_ns for name in sorted(expected)}
    if (set(result) != expected or any(type(value) is not int or value < 0
                                       for value in result.values())):
        raise ValueError("upgrade snapshot source timestamp set changed")
    return result


def runtime_fingerprints(inputs: list[dict]) -> dict:
    return {entry["destination"]: fingerprint(Path(entry["destination"]))
            for entry in inputs if entry["runtime"]}


def windows_runtime_baseline(inputs: list[dict], *, saved: bool = False) -> None:
    """Only runtime rollback material needs protected-exact reconstruction."""
    if os.name != "nt":
        return
    with closing(WindowsSecurity()) as security:
        for entry in inputs:
            if not entry["runtime"]:
                continue
            if saved:
                before = entry["before"]
                if before.get("kind") != (entry["kind"] if entry["exists"] else "absent"):
                    raise ValueError("Windows runtime baseline fingerprint is unsupported")
                if not entry["exists"]:
                    continue
                values = [before]
                if entry["kind"] == "tree":
                    values += list(before["directories"].values()) + list(before["files"].values())
                for value in values:
                    security.restoration_descriptor(value.get("acl"))
            else:
                path = checked_path(Path(entry["destination"]))
                if not path.exists():
                    continue
                paths = [path, *path.rglob("*")] if path.is_dir() else [path]
                for current in paths:
                    checked_path(current, exists=True)
                    handle = security.open(current)
                    try:
                        security.restoration_descriptor(handle=handle)
                    finally:
                        security.k.CloseHandle(handle)


def private_or_restored(path: Path, actual: dict, restored: dict, *, directory: bool) -> bool:
    if actual == restored:
        return True
    if "acl" in actual:
        try:
            windows_acl(path)
        except ValueError:
            return False
        return True
    return actual == {"mode": 0o700 if directory else 0o600}


def private_stage_identity(modules: dict) -> dict:
    if os.name == "nt":
        # A privacy classification is verified natively at cleanup. Exact
        # restored ACL fingerprints keep their separate equality contract.
        return {"kind": "private-tree", "files": {
            name.removeprefix("t3_fleet/"): digest for name, digest in modules.items()}}
    return {"kind": "tree", "mode": 0o700, "directories": {}, "files": {
        name.removeprefix("t3_fleet/"): {"sha256": digest, "mode": 0o600}
        for name, digest in modules.items()}}


def matches_private_stage(path: Path, actual: dict, expected: dict) -> bool:
    if os.name != "nt" or expected.get("kind") != "private-tree" or actual.get("kind") != "tree":
        return False
    if actual["directories"] or {name: value["sha256"] for name, value in actual["files"].items()} != expected["files"]:
        return False
    windows_acl(path)
    for file in tree_files(path).values():
        private_file(file)
    return True


def binding(args) -> dict:
    result = {key: [str(item) for item in value] if isinstance(value, list) else
              str(value) if isinstance(value, Path) else value
              for key, value in vars(args).items()
              if not key.startswith("_") and key not in {"action", "apply", "backup", "pin"}}
    result["selected_target"] = selected_target_identity(args)
    return result


def require_binding(args, expected: dict) -> None:
    if binding(args) != expected:
        raise ValueError("saved target identity or operator inventory changed")


def snapshot(transaction: Path, inputs: list[dict], args, *, launcher_contract: dict | None = None,
             preserve_mtime: bool = False) -> dict:
    saved = []
    for entry in inputs:
        item = dict(entry)
        path = Path(item["destination"])
        item["before"] = fingerprint(path)
        folder = transaction / item["label"]
        mkdir_private(folder)
        if item["exists"]:
            if item["kind"] == "sqlite":
                destination = folder / "data"
                write_private(destination, b"")
                # mode=ro keeps sources read-only and includes committed WAL pages.
                with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)) as source:
                    with closing(sqlite3.connect(destination)) as target:
                        source.backup(target, pages=256, sleep=0.01)
                        if target.execute("pragma integrity_check").fetchall() != [("ok",)]:
                            raise ValueError("SQLite snapshot integrity check failed")
                        # Archive a standalone main file. A copied WAL-mode
                        # header otherwise creates auxiliary files on inspection.
                        if target.execute("pragma journal_mode=delete").fetchone() != ("delete",):
                            raise ValueError("SQLite snapshot could not be made self-contained")
                with destination.open("rb") as handle:
                    os.fsync(handle.fileno())
                item["snapshot"] = fingerprint(destination)
            elif item["kind"] == "tree":
                mkdir_private(folder / "data")
                for name in sorted(item["before"]["directories"],
                                   key=lambda name: (name.count("/"), name)):
                    mkdir_private(folder / "data" / name)
                for name, file in tree_files(path).items():
                    source_info = file.stat()
                    source_identity = (
                        source_info.st_dev, source_info.st_ino, source_info.st_mode,
                        source_info.st_size, source_info.st_mtime_ns,
                        source_info.st_ctime_ns,
                    )
                    output = folder / "data" / name
                    for parent in reversed(output.parent.relative_to(folder / "data").parents):
                        candidate = folder / "data" / parent
                        if not candidate.exists():
                            mkdir_private(candidate)
                    if not output.parent.exists():
                        mkdir_private(output.parent)
                    content = file.read_bytes()
                    current_info = file.stat()
                    if (
                        current_info.st_dev, current_info.st_ino, current_info.st_mode,
                        current_info.st_size, current_info.st_mtime_ns,
                        current_info.st_ctime_ns,
                    ) != source_identity:
                        raise ValueError("snapshot source changed while being copied")
                    write_private(output, content)
                    if preserve_mtime:
                        os.utime(output, ns=(source_info.st_atime_ns, source_info.st_mtime_ns))
                item["snapshot"] = fingerprint(folder / "data")
            else:
                write_private(folder / "data", path.read_bytes())
                item["snapshot"] = fingerprint(folder / "data")
        saved.append(item)
    inventory = {"schema": 1, "binding": binding(args), "inputs": saved,
                 "execution_policy": 1, "launcher_contract": launcher_contract}
    atomic_json(transaction / "inventory.json", inventory)
    for directory in sorted((path for path in transaction.rglob("*") if path.is_dir()),
                            key=lambda path: len(path.parts), reverse=True):
        fsync_directory(directory)
    fsync_directory(transaction)
    return inventory


def stage_artifact(transaction: Path, prepared: dict) -> Path:
    staged = transaction / "artifacts"
    mkdir_private(staged)
    artifact = prepared["artifact"]
    wheel_name = BUILDER["wheel_name"](artifact["pin"])
    write_private(staged / wheel_name, artifact["wheel"].read_bytes())
    write_private(staged / BUILDER["MANIFEST"], json.dumps(artifact["manifest"], sort_keys=True).encode())
    BUILDER["verify_artifacts"](staged, artifact["pin"], artifact["source_files"])
    fsync_directory(staged)
    fsync_directory(transaction)
    return staged / wheel_name


def install(args, prepared: dict, wheel: Path, transaction: Path,
            expected_binding: dict, *, authority=None) -> None:
    anchor = guard_interpreter_anchor(args, "install-failed")
    layout = prepared["layout"]
    package = layout["package"]
    token = transaction.name.removeprefix("transaction-")
    old = package.parent / (".t3-fleet-old-" + token)
    staged = package.parent / (".t3-fleet-stage-" + token)
    if old.exists() or staged.exists():
        raise ValueError("staging destination already occupied")
    if args.layout == "source-wrapper":
        mkdir_private(staged)
        for name, content in prepared["artifact"]["contents"].items():
            if name in prepared["artifact"]["manifest"]["modules"]:
                write_private(staged / Path(name).name, content)
        if {"t3_fleet/" + name: sha(file) for name, file in tree_files(staged).items()} != prepared["artifact"]["manifest"]["modules"]:
            raise ValueError("staged source package mismatch")
        expected_stage = private_stage_identity(prepared["artifact"]["manifest"]["modules"])
        actual_stage = fingerprint(staged)
        if actual_stage != expected_stage and not matches_private_stage(
                staged, actual_stage, expected_stage):
            raise ValueError("staged source package fingerprint mismatch")
        fsync_directory(staged)
        fsync_directory(package.parent)
    require_binding(args, expected_binding)
    if authority is not None:
        authority()
    if args.layout == "source-wrapper":
        actual_stage = fingerprint(staged)
        if actual_stage != expected_stage and not matches_private_stage(
                staged, actual_stage, expected_stage):
            raise ValueError("staged source package changed before displacement")
    # The complete baseline is already protected. Retaining this rename also
    # prevents pip from leaving old unrecorded modules or bytecode active.
    os.replace(package, old)
    fsync_directory(package.parent)
    if args.layout == "source-wrapper":
        if authority is not None:
            authority()
        actual_stage = fingerprint(staged)
        if actual_stage != expected_stage and not matches_private_stage(
                staged, actual_stage, expected_stage):
            raise ValueError("staged source package changed before publication")
        os.replace(staged, package)
        for launcher, module in zip(layout["launchers"], ("cli", "agent")):
            if launcher in layout.get("wrapper_migration", []):
                if authority is not None:
                    authority()
                migrate_source_wrapper(launcher, args.python, package.parent, module)
    else:
        # The isolated bootstrap intentionally exposes no ambient site root.
        # Pip therefore cannot discover/remove the previous distribution.
        # Move its strictly owned metadata aside under the protected contract,
        # just as for the package, so old/new versions cannot coexist.
        os.replace(layout["dist"], package.parent / (".t3-fleet-old-dist-" + token))
        fsync_directory(package.parent)
        # Run existing pip without site initialization or executable .pth files.
        # The selected scheme was independently checked above. sys.prefix needs
        # restoring under -S on Python <3.14 before pip consults sysconfig.
        bootstrap = (
            "import sys,site,runpy,importlib.util,os; from pathlib import Path; "
            "os.umask(0o022) if os.name == 'posix' else None; "
            "prefix,origin,user=sys.argv[1:4]; "
            "sys.prefix=prefix; sys.exec_prefix=prefix; "
            "site.ENABLE_USER_SITE=(user=='yes'); "
            "spec=importlib.util.spec_from_file_location('pip',origin,submodule_search_locations=[str(Path(origin).parent)]); "
            "module=importlib.util.module_from_spec(spec); sys.modules['pip']=module; "
            "spec.loader.exec_module(module); sys.argv=['pip',*sys.argv[4:]]; "
            "runpy.run_module('pip',run_name='__main__')"
        )
        argv = ["-c", bootstrap,
                str(args.prefix), str(layout["pip_origin"]), "yes" if args.layout == "windows-user-site" else "no",
                "--isolated", "install",
                "--no-index", "--no-deps", "--no-build-isolation", "--no-compile",
                "--disable-pip-version-check", "--no-cache-dir", "--force-reinstall"]
        if args.layout == "windows-user-site":
            argv.append("--user")
        argv.append(str(wheel))
        run_anchored_python(anchor, argv, cwd=transaction)
    fsync_directory(package.parent)


CACHE_DISCOVER_PROGRAM = r'''
import sys, types, json, hashlib
from pathlib import Path
content = Path(sys.argv[1]).read_bytes()
if hashlib.sha256(content).hexdigest() != sys.argv[2]:
    raise SystemExit(7)
groups = {}
def visit(code):
    for value in code.co_consts:
        if isinstance(value, types.CodeType):
            visit(value)
        elif isinstance(value, frozenset) and value and all(type(s) is str for s in value):
            groups[value] = groups.get(value, 0) + 1
        elif isinstance(value, tuple):
            pending = list(value)
            while pending:
                item = pending.pop()
                if isinstance(item, (frozenset, types.CodeType)):
                    raise SystemExit(7)
                if isinstance(item, tuple):
                    pending.extend(item)
visit(compile(content, sys.argv[4], 'exec', dont_inherit=True, optimize=int(sys.argv[3])))
repeated = [value for value, count in groups.items() if count > 1]
strings = sorted(set().union(*repeated))
if len(strings) > 10 or (1 << len(strings)) > 1024:
    raise SystemExit(7)
print(json.dumps(strings))
'''


CACHE_CANDIDATE_PROGRAM = r'''
import sys, marshal, hashlib, json
from pathlib import Path
if sys.implementation.name != 'cpython' or marshal.version not in (4, 5):
    raise SystemExit(3)
content = Path(sys.argv[1]).read_bytes()
if hashlib.sha256(content).hexdigest() != sys.argv[4]:
    raise SystemExit(7)
keep = [sys.intern(s) for s in json.loads(sys.argv[3])]
def serialize():
    code = compile(content, sys.argv[5], 'exec', dont_inherit=True, optimize=int(sys.argv[2]))
    root_reference = code
    return marshal.dumps(code)
sys.stdout.buffer.write(serialize())
'''


def _compiler_child(anchor: dict, deadline: float, program: str, *arguments) -> bytes:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ValueError("trusted compiler budget exhausted")
    result = run_anchored_python(anchor, ["-c", program, *map(str, arguments)],
                                 cwd=Path(anchor["python"]).parent,
                                 timeout=10, deadline=deadline)
    if time.monotonic() >= deadline:
        raise ValueError("trusted compiler budget exhausted")
    return result


def _matches_trusted_payload(anchor: dict, deadline: float, payload: bytes, source: Path,
                             expected: str, optimization: int,
                             logical_source: Path | None = None) -> bool:
    logical_source = source if logical_source is None else logical_source
    try:
        strings = json.loads(_compiler_child(anchor, deadline, CACHE_DISCOVER_PROGRAM,
                                             source, expected, optimization, logical_source))
    except (ValueError, UnicodeError):
        raise ValueError("trusted compiler discovery refused") from None
    if (not isinstance(strings, list) or any(type(value) is not str for value in strings)
            or len(strings) > 10 or strings != sorted(set(strings))):
        raise ValueError("trusted compiler discovery refused")
    total = 1 << len(strings)
    if total > 1024:
        raise ValueError("trusted compiler context cap exceeded")
    seen = set()
    for mask in [0, total - 1, *(1 << index for index in range(len(strings))), *range(total)]:
        if mask in seen:
            continue
        seen.add(mask)
        context = [value for index, value in enumerate(strings) if mask & (1 << index)]
        candidate = _compiler_child(anchor, deadline, CACHE_CANDIDATE_PROGRAM, source,
                                    optimization, json.dumps(context), expected, logical_source)
        if payload == candidate:
            return True
    return False


def public_package_files(args, artifact: dict, *, package: Path | None = None,
                         logical_package: Path | None = None) -> dict[str, Path]:
    """Verify sources first, then narrow CPython cache derivatives without loading them."""
    package_budget = PACKAGE_DEADLINE_WINDOWS if os.name == "nt" else PACKAGE_DEADLINE_POSIX
    deadline = time.monotonic() + package_budget
    anchor = guard_interpreter_anchor(args, getattr(args, "_code", "runtime-drift"),
                                      deadline=deadline)
    package = args.purelib / "t3_fleet" if package is None else checked_path(package, exists=True)
    logical_package = package if logical_package is None else checked_path(logical_package)
    execution_integrity(package)
    files = tree_files(package)
    sources = {name: path for name, path in files.items() if not name.startswith("__pycache__/")}
    if {"t3_fleet/" + name: sha(path) for name, path in sources.items()} != artifact["manifest"]["modules"]:
        raise ValueError("installed runtime exact module set or hashes mismatch")
    directories = {path.relative_to(package).as_posix() for path in package.rglob("*") if path.is_dir()}
    if not directories <= {"__pycache__"}:
        raise ValueError("unknown runtime directory")
    caches = sorted(set(files) - set(sources))
    if caches:
        if anchor["implementation"] != "cpython" or anchor["marshal"] not in (4, 5):
            raise ValueError("selected interpreter cache format refused")
        for name in caches:
            if time.monotonic() >= deadline:
                raise ValueError("trusted compiler budget exhausted")
            pattern = (r"__pycache__/([A-Za-z0-9_]+)\." + re.escape(anchor["cache_tag"])
                       + r"(?:\.opt-([12]))?\.pyc")
            match = re.fullmatch(pattern, name)
            if not match:
                raise ValueError("unrecognized selected-interpreter cache")
            source = package / (match[1] + ".py")
            data, content = (package / name).read_bytes(), source.read_bytes()
            expected = artifact["manifest"]["modules"].get("t3_fleet/" + source.name)
            if hashlib.sha256(content).hexdigest() != expected:
                raise ValueError("trusted cache source changed")
            if len(data) < 16 or data[:4] != importlib.util.MAGIC_NUMBER:
                raise ValueError("trusted cache header refused")
            flags = struct.unpack("<I", data[4:8])[0]
            if flags == 0:
                valid = data[8:16] == struct.pack(
                    "<II", int(source.stat().st_mtime) & 0xffffffff, len(content) & 0xffffffff)
            elif flags in (1, 3):
                valid = data[8:16] == importlib.util.source_hash(content)
            else:
                valid = False
            if (not valid or not _matches_trusted_payload(
                    anchor, deadline, data[16:], source, expected, int(match[2] or 0),
                    logical_package / source.name)):
                raise ValueError("trusted cache payload refused")
    return sources


def public_fingerprint(args, artifact: dict, actual: dict) -> dict:
    public_package_files(args, artifact)
    return normalized_public_fingerprint(actual)


def verify_installed(args, prepared: dict) -> None:
    layout = inspect_layout(args)
    if layout.get("launcher_contract") != prepared.get("launcher_contract", prepared.get("layout", {}).get("launcher_contract")):
        raise ValueError("installed launcher toolchain contract changed")
    execution_inputs(inventory_backup(args, layout))
    expected = prepared["artifact"]["manifest"]
    sources = public_package_files(args, prepared["artifact"])
    if layout["dist"]:
        dist_dir = BUILDER["dist_dir"](prepared["artifact"]["pin"])
        if layout["dist"].name != dist_dir.rstrip("/"):
            raise ValueError("installed distribution version mismatch")
        contents = prepared["artifact"]["contents"]
        for name, data in contents.items():
            if name.startswith(dist_dir) and not name.endswith("/RECORD"):
                if (layout["purelib"] / name).read_bytes() != data:
                    raise ValueError("installed metadata differs from validated artifact")
        allowed = {name.removeprefix(dist_dir) for name in contents if name.startswith(dist_dir)}
        if tree_files(layout["dist"]).keys() != allowed | {"INSTALLER", "REQUESTED", "direct_url.json"}:
            raise ValueError("unexpected installed distribution files")
        if ((layout["dist"] / "INSTALLER").read_bytes() != b"pip\n"
                or (layout["dist"] / "REQUESTED").read_bytes() != b""):
            raise ValueError("unrecognized pip installation metadata")
        direct = json.loads((layout["dist"] / "direct_url.json").read_text())
        if not isinstance(direct, dict) or not isinstance(direct.get("archive_info"), dict):
            raise ValueError("malformed installed wheel provenance")
        digest = expected["wheel"]["sha256"]
        archive = direct.get("archive_info", {})
        if (set(direct) != {"url", "archive_info"}
                or direct["url"] != prepared.get("staged_wheel", prepared["artifact"]["wheel"]).as_uri()
                or archive.get("hashes") != {"sha256": digest}
                or not archive.keys() <= {"hash", "hashes"}
                or ("hash" in archive and archive["hash"] != "sha256=" + digest)):
            raise ValueError("installed wheel provenance mismatch")
        paths = {layout["package"] / name for name in sources}
        paths.update(layout["dist"] / name for name in tree_files(layout["dist"]))
        paths.update(layout["launchers"])
        rows = list(csv.reader(io.StringIO((layout["dist"] / "RECORD").read_text())))
        if {Path(os.path.abspath(args.purelib / row[0])) for row in rows} != paths:
            raise ValueError("installed RECORD exact inventory mismatch")
        for name, digest, size in rows:
            path = Path(os.path.abspath(args.purelib / name))
            if path == layout["dist"] / "RECORD":
                if digest or size:
                    raise ValueError("installed RECORD self hash mismatch")
            else:
                expected_hash = base64.urlsafe_b64encode(hashlib.sha256(path.read_bytes()).digest()).rstrip(b"=").decode()
                if digest != "sha256=" + expected_hash or size != str(path.stat().st_size):
                    raise ValueError("installed RECORD hash mismatch")
    for entry in prepared["inputs"]:
        if not entry["runtime"] and entry["kind"] != "sqlite":
            path = Path(entry["destination"])
            if fingerprint(path) != entry["before"]:
                raise ValueError("private input or outer launcher changed during installation")


def apply(args) -> str:
    prepared = plan(args)  # All preflight refusals precede even lock creation.
    if prepared.get("record", {}).get("schema") == 2:
        initial_raw_sha = hashlib.sha256(
            (args.purelib / RECOVERY).read_bytes()).hexdigest()
        initial_binding = binding(args)
        args._phase, args._writes, args._code = "locking", "possible", "lock-unavailable"
        with local_lock(args.purelib):
            prepared = plan(args)
            raw = (args.purelib / RECOVERY).read_bytes()
            if (hashlib.sha256(raw).hexdigest() != initial_raw_sha
                    or binding(args) != initial_binding):
                raise ValueError("upgrade authority changed under apply lock")
            args._phase, args._code = "verified", "owned-cleanup-failed"
            cleanup_upgrade_owned(
                args, prepared["record"], prepared["inventory"],
                initial_raw_sha, initial_binding)
        return "unchanged"
    initial_contract = prepared["layout"].get("launcher_contract")
    args._phase, args._writes, args._code = "locking", "possible", "lock-unavailable"
    with local_lock(args.purelib):
        prepared = plan(args)  # Reinspect after acquiring the OS lock.
        if prepared["layout"].get("launcher_contract") != initial_contract:
            raise ValueError("launcher toolchain contract changed under apply lock")
        previous = read_record(args.purelib)
        if previous:
            args._code = "runtime-drift"
            _, inventory = validate_backup(args, previous)
            check_rollback_drift(args, previous, inventory, prepared["artifact"])
            previous_artifact = Path(previous["backup"]) / "artifacts"
            previous_manifest = json.loads(
                (previous_artifact / BUILDER["MANIFEST"]).read_text(encoding="utf-8"))
            prepared["staged_wheel"] = previous_artifact / previous_manifest["wheel"]["filename"]
        if args.hash_record.exists():
            try:
                same_identity = json.loads(args.hash_record.read_text()) == prepared["artifact"]["manifest"]
            except (ValueError, UnicodeError):
                same_identity = False
            if same_identity:
                if prepared["layout"].get("wrapper_migration"):
                    raise ValueError("legacy source wrappers require the explicit upgrade action")
                try:
                    # Supply current non-runtime fingerprints for a write-free no-op.
                    for entry in prepared["inputs"]:
                        entry["before"] = fingerprint(Path(entry["destination"]))
                    verify_installed(args, prepared)
                except ValueError:
                    if previous and previous["phase"] == "verified":
                        raise
                else:
                    if previous:
                        args._code = "backup-invalid"
                        require_binding(args, previous["binding"])
                        cleanup_owned(args, previous, inventory)
                    return "unchanged"
        if previous and previous["phase"] == "verified":
            raise ValueError("verified identity drift requires explicit recovery")
        args._code = "inventory-invalid"
        protected_binding = binding(args)
        transaction = args.backup_parent / ("transaction-" + uuid.uuid4().hex)
        args._phase, args._code = "preparing", "backup-preparation-failed"
        mkdir_private(transaction)
        args._writes = "started"
        record = {"schema": 1, "phase": "preparing", "backup": str(transaction),
                  "binding": protected_binding}
        record_path = args.purelib / RECOVERY
        atomic_json(record_path, record)
        args._recovery = "preserve-record-and-backup"
        try:
            args._code = "snapshot-failed"
            inventory = snapshot(transaction, prepared["inputs"], args,
                                 launcher_contract=prepared["layout"].get("launcher_contract"))
            if inventory["binding"] != record["binding"]:
                raise ValueError("target identity changed during backup")
            prepared["inputs"] = inventory["inputs"]
            wheel = stage_artifact(transaction, prepared)
            prepared["staged_wheel"] = wheel
            record["inventory_sha256"] = sha(transaction / "inventory.json")
            token = transaction.name.removeprefix("transaction-")
            package = prepared["layout"]["package"]
            record["owned"] = {
                str(package.parent / (".t3-fleet-old-" + token)): inventory["inputs"][0]["before"],
                str(package.parent / (".t3-fleet-stage-" + token)): (
                    private_stage_identity(prepared["artifact"]["manifest"]["modules"])
                    if args.layout == "source-wrapper" else {"kind": "absent"}),
            }
            if prepared["layout"]["dist"]:
                metadata = next(entry for entry in inventory["inputs"]
                                if entry["category"] == "distribution-metadata" and entry["exists"])
                record["owned"][str(package.parent / (".t3-fleet-old-dist-" + token))] = metadata["before"]
            if any(checked_path(Path(path)).exists() for path in record["owned"]):
                raise ValueError("transaction-owned staging path is occupied")
            # The parent owns the transaction UUID name. Sync it only after all
            # baseline and staged-artifact contents, before backup publication.
            fsync_directory(transaction)
            fsync_directory(args.backup_parent)
            record["phase"] = "backup-complete"
            atomic_json(record_path, record)
            args._phase, args._code = "backup-complete", "baseline-changed"
            # Recheck executable trust and target authority at the mutation edge.
            current_layout = inspect_layout(args)
            if current_layout.get("launcher_contract") != initial_contract:
                raise ValueError("launcher toolchain contract changed before mutation")
            args._code = "baseline-changed"
            execution_inputs(prepared["inputs"])
            execution_inputs(prepared["inputs"], saved=True)
            # Recheck the baseline immediately before the mutation edge.
            if runtime_fingerprints(prepared["inputs"]) != {
                    entry["destination"]: entry["before"] for entry in prepared["inputs"] if entry["runtime"]}:
                raise ValueError("runtime changed after snapshot")
            require_binding(args, record["binding"])
            record["phase"] = "mutation-started"
            atomic_json(record_path, record)
            args._phase, args._code = "mutation-started", "install-failed"
            install(args, prepared, wheel, transaction, record["binding"])
            args._code = "verification-failed"
            verify_installed(args, prepared)
            args._code = "identity-publication-failed"
            guard_interpreter_anchor(args, "identity-publication-failed")
            require_binding(args, record["binding"])
            atomic_json(args.hash_record, prepared["artifact"]["manifest"])
            execution_inputs(prepared["inputs"])
            record["after"] = runtime_fingerprints(prepared["inputs"])
            require_binding(args, record["binding"])
            record["phase"] = "verified"
            atomic_json(record_path, record)
            args._code = "owned-cleanup-failed"
            cleanup_owned(args, record, inventory)
        except (OSError, ValueError, sqlite3.Error, zipfile.BadZipFile):
            if record["phase"] == "mutation-started":
                # A caught failure's exact footprint makes explicit recovery
                # possible. A process kill leaves no such claim, so recovery is
                # stricter and only admits known baseline/staged file bytes.
                record["after"] = runtime_fingerprints(prepared["inputs"])
                atomic_json(record_path, record)
            raise
    return "installed"


def validate_backup(args, record: dict) -> tuple[Path, dict]:
    anchor = guard_interpreter_anchor(args, getattr(args, "_code", "backup-invalid"))
    backup = checked_path(args.backup or Path(record["backup"]), exists=True)
    safe_parent(backup, private=True)
    current_binding = binding(args)
    if (backup.parent != args.backup_parent or not re.fullmatch(r"transaction-[0-9a-f]{32}", backup.name)
            or record["backup"] != str(backup) or record["binding"] != current_binding):
        raise ValueError("backup does not belong to this runtime and operator inventory")
    manifest = backup / "inventory.json"
    private_file(manifest)
    if sha(manifest) != record.get("inventory_sha256"):
        raise ValueError("backup inventory is incomplete or changed")
    inventory = json.loads(manifest.read_text())
    if inventory["schema"] != 1 or inventory["binding"] != current_binding:
        raise ValueError("backup inventory binding mismatch")
    if inventory.get("execution_policy") != 1:
        raise ValueError("backup lacks executable integrity policy")
    execution_inputs(inventory["inputs"], saved=True)
    if args.layout == "windows-user-site":
        check_interpreter_overrides(args.python)
        probe = probe_interpreter(args.python, args.layout, args.prefix, anchor=anchor)
        contract = windows_launcher_contract(probe, inspect_pip(probe, args.purelib, windows=True), args.python)
        if inventory.get("launcher_contract") != contract:
            raise ValueError("saved launcher toolchain contract changed")
    previous_code = getattr(args, "_code", "backup-invalid")
    if os.name == "nt":
        args._code = "windows-baseline-ineligible"
    windows_runtime_baseline(inventory["inputs"], saved=True)
    args._code = previous_code
    allowed_runtime = {args.purelib / "t3_fleet", args.hash_record,
                       args.fleet_launcher, args.agent_launcher}
    for index, entry in enumerate(inventory["inputs"]):
        if entry["label"] != f"input-{index:04d}":
            raise ValueError("unsafe backup label")
        destination = checked_path(Path(entry["destination"]))
        if entry["runtime"] and destination not in allowed_runtime:
            if (args.layout == "source-wrapper" or destination.parent != args.purelib
                    or not re.fullmatch(r"t3_fleet-[A-Za-z0-9_.+]+\.dist-info", destination.name)):
                raise ValueError("backup destination is outside runtime footprint")
        if entry["exists"]:
            saved = checked_path(backup / entry["label"] / "data", exists=True)
            if fingerprint(saved) != entry["snapshot"]:
                raise ValueError("backup content integrity mismatch")
            for file in ([saved] if saved.is_file() else tree_files(saved).values()):
                private_file(file)
    return backup, inventory


def check_rollback_drift(args, record: dict, inventory: dict, artifact: dict) -> None:
    execution_inputs(inventory["inputs"])
    current = runtime_fingerprints(inventory["inputs"])
    if "rollback" in record and record["phase"] != "rolled-back":
        # A rollback publishes its starting fingerprints before staging or
        # swaps. The only active states are that start, the complete baseline,
        # or the known gap after moving a complete tree aside.
        for entry in inventory["inputs"]:
            if not entry["runtime"]:
                continue
            destination = entry["destination"]
            progress = record["rollback"][destination]
            stage, retired = restore_paths(Path(record["backup"]), entry)
            if progress["stage"] != str(stage) or progress["retired"] != str(retired):
                raise ValueError("rollback paths outside transaction footprint")
            actual = current[destination]
            if actual == entry["before"] or actual == progress["start"]:
                continue
            if actual["kind"] == "absent" and fingerprint(retired) == progress["start"]:
                continue
            raise ValueError("unknown interrupted rollback destination")
        return
    if "after" in record:
        for destination, actual in current.items():
            if actual == record["after"][destination]:
                continue
            if destination == str(args.purelib / "t3_fleet") and actual["kind"] == "tree":
                if public_fingerprint(args, artifact, actual) == record["after"][destination]:
                    continue
            raise ValueError("runtime destination drift requires manual review")
        return
    complete_pip_target = False
    if args.layout != "source-wrapper":
        # Derive trust from the protected, validated wheel and the existing
        # launcher grammar, never an operator-supplied replacement fingerprint.
        staged = Path(record["backup"]) / "artifacts"
        BUILDER["verify_artifacts"](staged, artifact["pin"], artifact["source_files"])
        try:
            verify_installed(args, {"artifact": artifact, "inputs": [],
                                    "staged_wheel": staged / BUILDER["wheel_name"](artifact["pin"]),
                                    "launcher_contract": inventory.get("launcher_contract")})
            target = args.purelib / BUILDER["dist_dir"](artifact["pin"]).rstrip("/")
            for entry in inventory["inputs"]:
                path = Path(entry["destination"])
                if entry["category"] == "distribution-metadata" and path != target and path.exists():
                    raise ValueError("old metadata remains beside target")
            complete_pip_target = True
        except (ValueError, OSError):
            pass
    # An abruptly killed source swap may leave old, absent, or exact staged
    # package bytes. Unknown pip-generated partial files are deliberately refused.
    for entry in inventory["inputs"]:
        if not entry["runtime"]:
            continue
        destination = entry["destination"]
        actual = current[destination]
        if actual == entry["before"] or actual["kind"] == "absent":
            continue
        if complete_pip_target and entry["category"] in {"runtime-package", "distribution-metadata", "entrypoint"}:
            continue
        if destination == str(args.purelib / "t3_fleet") and actual["kind"] == "tree":
            public_package_files(args, artifact)
            continue
        if destination == str(args.hash_record) and actual["kind"] == "file":
            if json.loads(args.hash_record.read_text()) == artifact["manifest"]:
                continue
        raise ValueError("unknown interrupted runtime footprint requires manual review")


def restore_paths(backup: Path, entry: dict) -> tuple[Path, Path]:
    destination = Path(entry["destination"])
    token = backup.name.removeprefix("transaction-") + "-" + entry["label"]
    return (destination.parent / (".t3-fleet-restore-" + token),
            destination.parent / (".t3-fleet-displaced-" + token))


def cleanup_owned(args, record: dict, inventory: dict) -> None:
    backup = Path(record["backup"])
    token = backup.name.removeprefix("transaction-")
    allowed = {args.purelib / (".t3-fleet-" + kind + "-" + token) for kind in ("old", "stage", "old-dist")}
    owned = {Path(path): expected for path, expected in record.get("owned", {}).items()}
    if not owned.keys() <= allowed:
        raise ValueError("cleanup path outside transaction footprint")
    for entry in inventory["inputs"]:
        if entry["runtime"] and "rollback" in record:
            stage, retired = restore_paths(backup, entry)
            progress = record["rollback"][entry["destination"]]
            if progress["stage"] != str(stage) or progress["retired"] != str(retired):
                raise ValueError("cleanup paths outside transaction footprint")
            owned[stage] = entry["before"]
            owned[retired] = progress["start"]
    # Check the entire set before removing anything. A mismatch stays available
    # for private review; other transactions are never selected by a glob.
    for path, expected in owned.items():
        actual = fingerprint(path)
        if (actual["kind"] != "absent" and actual != expected
                and not matches_private_stage(path, actual, expected)):
            raise ValueError("transaction-owned cleanup content changed")
    require_binding(args, record["binding"])
    for path in owned:
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
        fsync_directory(path.parent)


def restore_entry(args, backup: Path, entry: dict, progress: dict,
                  expected_binding: dict, *, authority=None,
                  source_mtimes_ns: dict[str, int] | None = None,
                  source_names: set[str] | None = None) -> None:
    windows_runtime_baseline([entry], saved=True)
    execution_inputs([entry], saved=True)
    execution_inputs([entry])
    destination = Path(entry["destination"])
    checked_path(destination)
    stage, retired = restore_paths(backup, entry)
    checked_path(stage)
    checked_path(retired)
    if fingerprint(destination) == entry["before"]:
        return
    if not entry["exists"]:
        # No baseline bytes to stage. Preserve a complete current tree until
        # the whole rollback has verified, rather than deleting it mid-restore.
        if destination.is_dir():
            if retired.exists():
                raise ValueError("rollback displaced destination occupied")
            require_binding(args, expected_binding)
            if authority is not None:
                authority()
            os.replace(destination, retired)
        elif destination.exists():
            require_binding(args, expected_binding)
            if authority is not None:
                authority()
            destination.unlink()
        fsync_directory(destination.parent)
        return
    saved = backup / entry["label"] / "data"
    if entry["kind"] == "tree":
        if not stage.exists():
            mkdir_private(stage)
        # A killed staging pass may leave a subset of verified baseline files.
        # Accept only those exact bytes and private or restored permissions.
        staged = fingerprint(stage)
        expected = entry["before"]
        if (staged["kind"] != "tree" or not staged["files"].keys() <= expected["files"].keys()
                or not staged["directories"].keys() <= expected["directories"].keys()
                or not private_or_restored(stage, permissions(stage),
                                           {key: expected[key] for key in permissions(stage)}, directory=True)):
            raise ValueError("unknown rollback staging tree")
        for name, value in staged["files"].items():
            if (value["sha256"] != expected["files"][name]["sha256"] or not private_or_restored(
                    stage / name, {key: field for key, field in value.items() if key != "sha256"},
                    {key: field for key, field in expected["files"][name].items() if key != "sha256"}, directory=False)):
                raise ValueError("unknown rollback staged file")
        for name, value in staged["directories"].items():
            if not private_or_restored(stage / name, value, expected["directories"][name], directory=True):
                raise ValueError("unknown rollback staged directory")
        for name in sorted(entry["before"]["directories"], key=lambda name: name.count("/")):
            if not (stage / name).exists():
                mkdir_private(stage / name)
        for name, file in tree_files(saved).items():
            output = stage / name
            if not output.exists():
                write_private(output, file.read_bytes())
            restore_permissions(output, entry["before"]["files"][name])
        for name, value in entry["before"]["directories"].items():
            restore_permissions(stage / name, value)
        if entry["category"] == "runtime-package" and source_mtimes_ns is not None:
            if source_names is None or set(source_mtimes_ns) != source_names:
                raise ValueError("upgrade snapshot source timestamp set changed")
            for name, value in source_mtimes_ns.items():
                if type(value) is not int or value < 0 or not (stage / name).is_file():
                    raise ValueError("upgrade snapshot source timestamp set changed")
                os.utime(stage / name, ns=(value, value))
    else:
        if stage.exists():
            if sha(stage) != sha(saved):
                raise ValueError("unknown rollback staged file")
        else:
            write_private(stage, saved.read_bytes())
    restore_permissions(stage, entry["before"])
    execution_integrity(stage, identity=entry.get("category") == "hash-record")
    if fingerprint(stage) != entry["before"]:
        raise ValueError("complete staged restore fingerprint mismatch")
    if stage.is_dir():
        for directory in sorted((path for path in stage.rglob("*") if path.is_dir()),
                                key=lambda path: len(path.parts), reverse=True):
            fsync_directory(directory)
        fsync_directory(stage)
    fsync_directory(destination.parent)
    destination_is_dir = destination.is_dir()
    if destination_is_dir:
        if retired.exists():
            raise ValueError("rollback displaced destination occupied")
    elif retired.exists() and fingerprint(retired) != progress["start"]:
        raise ValueError("rollback displaced tree changed")
    require_binding(args, expected_binding)
    if authority is not None:
        authority()
    if fingerprint(stage) != entry["before"]:
        raise ValueError("staged restore changed before displacement")
    if destination_is_dir:
        os.replace(destination, retired)
        fsync_directory(destination.parent)
        if authority is not None:
            authority()
    if fingerprint(stage) != entry["before"]:
        raise ValueError("staged restore changed before publication")
    os.replace(stage, destination)
    execution_inputs([entry])
    fsync_directory(destination.parent)


def upgrade_rollback_map(backup: Path, inventory: dict, value: dict | None = None) -> dict:
    entries = [entry for entry in inventory["inputs"] if entry["runtime"]]
    if value is None:
        return {
            entry["destination"]: {
                "start": fingerprint(Path(entry["destination"])),
                "stage": str(restore_paths(backup, entry)[0]),
                "retired": str(restore_paths(backup, entry)[1]),
            }
            for entry in entries
        }
    if not isinstance(value, dict) or set(value) != {entry["destination"] for entry in entries}:
        raise ValueError("upgrade rollback progress is invalid")
    for entry in entries:
        progress = value[entry["destination"]]
        stage, retired = restore_paths(backup, entry)
        if (not isinstance(progress, dict) or set(progress) != {"start", "stage", "retired"}
                or not isinstance(progress["start"], dict)
                or progress["stage"] != str(stage) or progress["retired"] != str(retired)):
            raise ValueError("upgrade rollback progress is invalid")
    return value


def upgrade_owned(args, backup: Path, inventory: dict, successor: dict) -> dict:
    token = backup.name.removeprefix("transaction-")
    package_entry = next(entry for entry in inventory["inputs"]
                         if entry["category"] == "runtime-package")
    package = Path(package_entry["destination"])
    return {
        str(package.parent / (".t3-fleet-old-" + token)): package_entry["before"],
        str(package.parent / (".t3-fleet-stage-" + token)):
            private_stage_identity(successor["manifest"]["modules"]),
    }


def upgrade_ready_bytes(record: dict, inventory: dict, runtime: dict) -> bytes:
    value = {
        "schema": 2,
        "operation": "upgrade",
        "backup": record["backup"],
        "inventory_sha256": record["inventory_sha256"],
        "predecessor_record_sha256": record["predecessor"]["record_sha256"],
        "predecessor_backup": record["predecessor"]["backup"],
        "predecessor_inventory_sha256": record["predecessor"]["inventory_sha256"],
        "predecessor": record["predecessor"],
        "successor": record["successor"],
        "runtime": runtime,
    }
    return record_bytes(value)


def validate_upgrade_ready(args, backup: Path, record: dict | None) -> dict:
    backup = checked_path(backup, exists=True)
    if (backup.parent != args.backup_parent
            or not re.fullmatch(r"transaction-[0-9a-f]{32}", backup.name)):
        raise ValueError("selected upgrade transaction is invalid")
    receipt_path = backup / "rollback-ready.json"
    private_file(receipt_path)
    receipt_raw = receipt_path.read_bytes()
    receipt = json.loads(receipt_raw)
    receipt_fields = {
        "schema", "operation", "backup", "inventory_sha256", "predecessor_record_sha256",
        "predecessor_backup", "predecessor_inventory_sha256", "predecessor",
        "successor", "runtime",
    }
    if (not isinstance(receipt, dict) or set(receipt) != receipt_fields
            or type(receipt.get("schema")) is not int or receipt["schema"] != 2
            or receipt.get("operation") != "upgrade" or receipt.get("backup") != str(backup)):
        raise ValueError("upgrade rollback receipt is invalid")
    inventory_path = backup / "inventory.json"
    private_file(inventory_path)
    inventory = json.loads(inventory_path.read_bytes())
    if (not SHA256.fullmatch(str(receipt["inventory_sha256"]))
            or sha(inventory_path) != receipt["inventory_sha256"]
            or not isinstance(inventory, dict)
            or inventory.get("schema") != 2 or inventory.get("operation") != "upgrade"
            or inventory.get("predecessor") != receipt["predecessor"]
            or inventory.get("successor") != receipt["successor"]):
        raise ValueError("upgrade rollback receipt inventory changed")
    synthetic = {
        "schema": 2, "operation": "upgrade", "phase": "verified",
        "backup": str(backup), "binding": inventory.get("binding"),
        "inventory_sha256": receipt["inventory_sha256"],
        "owned": upgrade_owned(args, backup, inventory, receipt["successor"]),
        "predecessor": receipt["predecessor"], "successor": receipt["successor"],
        "after": receipt["runtime"],
    }
    selected_backup = args.backup
    args.backup = backup
    try:
        validated_backup, inventory = validate_upgrade_backup(args, synthetic)
    finally:
        args.backup = selected_backup
    predecessor_raw = (backup / "predecessor-record.json").read_bytes()
    if (hashlib.sha256(predecessor_raw).hexdigest() != receipt["predecessor_record_sha256"]
            or receipt["predecessor_record_sha256"] != receipt["predecessor"]["record_sha256"]
            or receipt["predecessor_backup"] != receipt["predecessor"]["backup"]
            or receipt["predecessor_inventory_sha256"]
                != receipt["predecessor"]["inventory_sha256"]):
        raise ValueError("upgrade rollback receipt predecessor changed")
    predecessor_record = json.loads(predecessor_raw)
    identity_entry = next(entry for entry in inventory["inputs"]
                          if entry["category"] == "hash-record")
    validate_preserved_outer_wrappers(inventory)
    predecessor = validate_predecessor(
        args, predecessor_raw, predecessor_record,
        identity_bytes=(validated_backup / identity_entry["label"] / "data").read_bytes(),
        saved_launchers=saved_launcher_fingerprints(inventory),
    )
    expected_runtime = {entry["destination"]: entry["before"]
                        for entry in inventory["inputs"] if entry["runtime"]}
    if receipt["runtime"] != expected_runtime:
        raise ValueError("restored predecessor runtime changed")
    actual_runtime = runtime_fingerprints(inventory["inputs"])
    completed = not (record is not None and record.get("schema") == 2)
    for entry in inventory["inputs"]:
        if not entry["runtime"]:
            continue
        actual = actual_runtime[entry["destination"]]
        expected = expected_runtime[entry["destination"]]
        if actual == expected:
            continue
        if (completed and entry["category"] == "runtime-package"
                and actual.get("kind") == "tree"
                and public_fingerprint(
                    predecessor["args"], predecessor["artifact"], actual)
                    == normalized_public_fingerprint(expected)):
            continue
        raise ValueError("restored predecessor runtime changed")
    public_package_files(predecessor["args"], predecessor["artifact"])
    for path in synthetic["owned"]:
        if fingerprint(Path(path))["kind"] != "absent":
            raise ValueError("upgrade-owned cleanup is incomplete")
    rollback = upgrade_rollback_map(backup, inventory)
    for progress in rollback.values():
        for key in ("stage", "retired"):
            if fingerprint(Path(progress[key]))["kind"] != "absent":
                raise ValueError("upgrade rollback cleanup is incomplete")
    active_raw, active = read_record_bytes(args.purelib)
    if record is not None and record.get("schema") == 2:
        if (active != record or active_raw is None or record.get("phase") != "predecessor-ready"
                or record.get("backup") != str(backup)
                or record.get("rollback_ready_sha256") != hashlib.sha256(receipt_raw).hexdigest()):
            raise ValueError("predecessor-ready authority changed")
    elif active_raw != predecessor_raw or active != predecessor_record:
        raise ValueError("completed upgrade rollback is not the active predecessor")
    return {"backup": validated_backup, "inventory": inventory, "receipt": receipt,
            "receipt_raw": receipt_raw, "predecessor": predecessor,
            "predecessor_raw": predecessor_raw, "predecessor_record": predecessor_record,
            "runtime": expected_runtime}


def require_upgrade_ready_edge(args, record: dict, ready: dict,
                               expected_record_sha256: str,
                               expected_binding: dict) -> None:
    require_upgrade_authority(args, expected_record_sha256, expected_binding)
    backup = ready["backup"]
    if ((backup / "rollback-ready.json").read_bytes() != ready["receipt_raw"]
            or (backup / "predecessor-record.json").read_bytes() != ready["predecessor_raw"]
            or sha(backup / "inventory.json") != record["inventory_sha256"]
            or runtime_fingerprints(ready["inventory"]["inputs"]) != ready["runtime"]):
        raise ValueError("upgrade rollback readiness changed at publication edge")


def rollback_upgrade(args, record: dict | None = None) -> str:
    if os.name != "posix" or args.layout != "source-wrapper" or args.backup is None:
        raise ValueError("upgrade rollback supports only the selected POSIX source transaction")
    backup = checked_path(args.backup, exists=True)
    if record is None:
        record = read_record(args.purelib)
    if record and (record.get("schema") == 1 or (
        record.get("schema") == 2 and record.get("backup") != str(backup)
        and (backup / "rollback-ready.json").is_file()
    )):
        args._code = "recovery-invalid"
        validate_upgrade_ready(args, backup, None)
        fsync_directory(args.purelib)
        return "unchanged"
    if not record or record.get("schema") != 2 or record.get("backup") != str(backup):
        raise ValueError("selected upgrade transaction is not active")
    raw = (args.purelib / RECOVERY).read_bytes()
    if record["phase"] == "predecessor-ready":
        ready = validate_upgrade_ready(args, backup, record)
        initial_sha = hashlib.sha256(raw).hexdigest()
    else:
        state = validate_upgrade_state(args, raw, record)
        upgrade_rollback_map(backup, state["inventory"], record.get("rollback")) \
            if "rollback" in record else None
        ready = None
        initial_sha = hashlib.sha256(raw).hexdigest()
    expected_binding = binding(args)
    args._phase, args._writes, args._code = "locking", "possible", "lock-unavailable"
    with local_lock(args.purelib):
        active_raw, record = read_record_bytes(args.purelib)
        if (active_raw is None or hashlib.sha256(active_raw).hexdigest() != initial_sha
                or record.get("backup") != str(backup)):
            raise ValueError("upgrade rollback authority changed under lock")
        if record["phase"] == "predecessor-ready":
            ready = validate_upgrade_ready(args, backup, record)
        else:
            state = validate_upgrade_state(args, active_raw, record)
            inventory = state["inventory"]
            if record["phase"] != "rollback-started":
                rollback_map = upgrade_rollback_map(backup, inventory)
                for progress in rollback_map.values():
                    if any(checked_path(Path(progress[key])).exists()
                           for key in ("stage", "retired")):
                        raise ValueError("upgrade rollback staging destination occupied")
                record = dict(record)
                record.pop("after", None)
                record["phase"] = "rollback-started"
                record["rollback"] = rollback_map
                require_upgrade_authority(args, hashlib.sha256(active_raw).hexdigest(), expected_binding)
                previous_sha = hashlib.sha256(active_raw).hexdigest()
                active_raw = record_bytes(record)
                atomic_record_bytes(
                    args.purelib / RECOVERY, active_raw,
                    before_replace=lambda: require_upgrade_authority(
                        args, previous_sha, expected_binding),
                )
            else:
                upgrade_rollback_map(backup, inventory, record["rollback"])
            expected_sha = hashlib.sha256(active_raw).hexdigest()
            args._phase, args._writes, args._code = "rollback-started", "started", "rollback-failed"
            authority = lambda: require_upgrade_authority(args, expected_sha, expected_binding)
            source_names = manifest_source_names(record["predecessor"]["manifest"])
            for entry in inventory["inputs"]:
                if entry["runtime"] and Path(entry["destination"]) != args.hash_record:
                    restore_entry(args, backup, entry, record["rollback"][entry["destination"]],
                                  expected_binding, authority=authority,
                                  source_mtimes_ns=inventory["source_mtimes_ns"],
                                  source_names=source_names)
            for entry in inventory["inputs"]:
                if entry["runtime"] and Path(entry["destination"]) == args.hash_record:
                    restore_entry(args, backup, entry, record["rollback"][entry["destination"]],
                                  expected_binding, authority=authority,
                                  source_mtimes_ns=inventory["source_mtimes_ns"],
                                  source_names=source_names)
            expected_runtime = {entry["destination"]: entry["before"]
                                for entry in inventory["inputs"] if entry["runtime"]}
            if runtime_fingerprints(inventory["inputs"]) != expected_runtime:
                raise ValueError("upgrade predecessor restoration verification failed")
            public_package_files(state["predecessor"]["args"], state["predecessor"]["artifact"])
            cleanup_upgrade_owned(args, record, inventory, expected_sha, expected_binding)
            predecessor_raw = (backup / "predecessor-record.json").read_bytes()
            validate_predecessor(
                args, predecessor_raw, json.loads(predecessor_raw),
                identity_bytes=args.hash_record.read_bytes())
            authority()
            receipt_raw = upgrade_ready_bytes(record, inventory, expected_runtime)
            receipt_path = backup / "rollback-ready.json"
            if receipt_path.exists():
                private_file(receipt_path)
                if receipt_path.read_bytes() != receipt_raw:
                    raise ValueError("upgrade rollback receipt already contains unknown bytes")
            else:
                def require_receipt_edge():
                    authority()
                    if receipt_path.exists():
                        raise ValueError("upgrade rollback receipt appeared during publication")
                atomic_record_bytes(
                    receipt_path, receipt_raw, before_replace=require_receipt_edge)
                fsync_directory(backup)
            authority()
            record = dict(record)
            record["phase"] = "predecessor-ready"
            record["rollback_ready_sha256"] = hashlib.sha256(receipt_raw).hexdigest()
            previous_sha = expected_sha
            active_raw = record_bytes(record)
            atomic_record_bytes(
                args.purelib / RECOVERY, active_raw,
                before_replace=lambda: require_upgrade_authority(
                    args, previous_sha, expected_binding),
            )
            ready = validate_upgrade_ready(args, backup, record)
        expected_sha = hashlib.sha256(active_raw).hexdigest()
        require_upgrade_authority(args, expected_sha, expected_binding)
        ready = validate_upgrade_ready(args, backup, record)
        require_upgrade_ready_edge(args, record, ready, expected_sha, expected_binding)
        atomic_record_bytes(
            args.purelib / RECOVERY, ready["predecessor_raw"],
            before_replace=lambda: require_upgrade_ready_edge(
                args, record, ready, expected_sha, expected_binding),
        )
    return "rolled-back"


def rollback(args) -> str:
    guard_interpreter_anchor(args, "artifact-invalid")
    args._code = "artifact-invalid"
    artifact = validate_artifact(args)
    args._code = "recovery-invalid"
    checked_path(args.purelib, exists=True)
    record = read_record(args.purelib)
    if record:
        args._recovery = "preserve-record-and-backup"
    if not record or args.backup is None or record["phase"] == "preparing":
        raise ValueError("complete transaction backup required for rollback")
    if (record.get("schema") == 2
            or (args.backup / "rollback-ready.json").exists()):
        return rollback_upgrade(args, record)
    args._code = "backup-invalid"
    backup, inventory = validate_backup(args, record)
    check_boundaries(args, inventory["inputs"])
    args._code = "runtime-drift"
    check_rollback_drift(args, record, inventory, artifact)
    args._phase, args._writes, args._code = "locking", "possible", "lock-unavailable"
    with local_lock(args.purelib):
        args._code = "recovery-recheck-failed"
        record = read_record(args.purelib)
        backup, inventory = validate_backup(args, record)
        check_rollback_drift(args, record, inventory, artifact)
        if record["phase"] == "rolled-back":
            require_binding(args, record["binding"])
            cleanup_owned(args, record, inventory)
            return "unchanged"
        if "rollback" not in record:
            record["rollback"] = {
                entry["destination"]: {"start": fingerprint(Path(entry["destination"])),
                                       "stage": str(restore_paths(backup, entry)[0]),
                                       "retired": str(restore_paths(backup, entry)[1])}
                for entry in inventory["inputs"] if entry["runtime"]}
            # Every sibling name is derived from this transaction and label.
            # Refuse pre-existing objects before claiming ownership of a swap.
            for entry in inventory["inputs"]:
                if entry["runtime"] and any(checked_path(path).exists() for path in restore_paths(backup, entry)):
                    raise ValueError("rollback staging destination occupied")
        record.pop("after", None)
        guard_interpreter_anchor(args, "rollback-failed")
        require_binding(args, record["binding"])
        record["phase"] = "mutation-started"
        args._phase, args._writes, args._code = "mutation-started", "started", "rollback-failed"
        atomic_json(args.purelib / RECOVERY, record)
        for entry in inventory["inputs"]:
            if entry["runtime"] and Path(entry["destination"]) != args.hash_record:
                guard_interpreter_anchor(args, "rollback-failed")
                restore_entry(args, backup, entry, record["rollback"][entry["destination"]],
                              record["binding"])
        for entry in inventory["inputs"]:
            if entry["runtime"] and Path(entry["destination"]) == args.hash_record:
                guard_interpreter_anchor(args, "rollback-failed")
                restore_entry(args, backup, entry, record["rollback"][entry["destination"]],
                              record["binding"])
        execution_inputs(inventory["inputs"])
        expected = {entry["destination"]: entry["before"] for entry in inventory["inputs"] if entry["runtime"]}
        if runtime_fingerprints(inventory["inputs"]) != expected:
            raise ValueError("runtime restoration verification failed")
        require_binding(args, record["binding"])
        record["phase"] = "rolled-back"
        record["after"] = expected
        atomic_json(args.purelib / RECOVERY, record)
        args._code = "owned-cleanup-failed"
        require_binding(args, record["binding"])
        cleanup_owned(args, record, inventory)
    return "rolled-back"


class OperatorParser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's default error repeats untrusted argument values.
        raise ValueError("invalid command arguments")


def target_host_id(value: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", value):
        raise argparse.ArgumentTypeError("invalid target host")
    return value


def parser() -> argparse.ArgumentParser:
    result = OperatorParser(description=__doc__)
    result.add_argument("action", choices=("plan", "apply", "upgrade", "rollback"), nargs="?", default="plan")
    result.add_argument("--layout", choices=("venv", "source-wrapper", "windows-user-site"), required=True)
    for flag in ("python", "prefix", "purelib", "scripts", "fleet-launcher", "agent-launcher",
                 "artifact-dir", "public-source", "hash-record", "backup-parent"):
        result.add_argument("--" + flag, type=Path, required=True)
    result.add_argument("--source-root", type=Path)
    result.add_argument("--pin", type=Path, required=True,
                        help="explicit approved pin outside the checkout; no default pin is shipped")
    result.add_argument("--target-host", type=target_host_id, required=True)
    result.add_argument("--config", type=Path, action="append", required=True)
    for flag in ("private-file", "payload-file", "wrapper", "state-db", "optional-state-db"):
        result.add_argument("--" + flag, type=Path, action="append", default=[])
    result.add_argument("--backup", type=Path)
    result.add_argument("--apply", action="store_true", help="authorize local writes, without certifying external quiescence")
    return result


def main(argv: list[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
    except ValueError:
        print(json.dumps({"status": "refused", "code": "arguments-invalid", "phase": "preflight",
                          "writes": "none", "recovery": "none-created"}, sort_keys=True))
        return 1
    args._phase, args._writes, args._code = "preflight", "none", "invalid-mode"
    args._recovery = "none-created"
    try:
        args._anchor = establish_interpreter_anchor(args)
        load_builder()
        details = {}
        if args.action == "plan":
            if args.apply or args.backup:
                raise ValueError("plan accepts no mutation flags")
            prepared = plan(args)
            status = "planned"
            revision = prepared["artifact"]["manifest"]["revision"]
            details = plan_report(args, prepared)
        elif args.action == "upgrade":
            if args.backup is None:
                raise ValueError("upgrade requires an explicit transaction")
            if args.apply:
                status = upgrade(args)
                revision = BUILDER["load_pin"](args.pin)["revision"]
            else:
                prepared = plan_upgrade(args)
                status = "planned"
                revision = prepared["artifact"]["manifest"]["revision"]
                details = {**plan_report(args, prepared), "operation": prepared["operation"]}
        else:
            if not args.apply or (args.action == "apply" and args.backup):
                raise ValueError("explicit apply intent and correct recovery mode required")
            status = apply(args) if args.action == "apply" else rollback(args)
            revision = BUILDER["load_pin"](args.pin)["revision"]
        print(json.dumps({"status": status, "layout": args.layout, "revision": revision, **details}, sort_keys=True))
        return 0
    except (ValueError, OSError, KeyError, TypeError, UnicodeError, RuntimeError, SyntaxError,
            sqlite3.Error, zipfile.BadZipFile):
        print(json.dumps({"status": "refused", "code": args._code, "phase": args._phase,
                          "writes": args._writes, "recovery": args._recovery}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
