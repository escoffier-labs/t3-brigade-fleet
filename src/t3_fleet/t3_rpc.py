"""Bounded unary Effect RPC over RFC 6455, using only the standard library."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import ssl
import struct
import time
import urllib.parse
from typing import Any

from .errors import T3HttpError
from .t3_origin import validate_t3_origin

MAX_BYTES = 1024 * 1024
MAX_UPGRADE_BYTES = 16 * 1024


def websocket_rpc(base_url: str, token: str, method: str, payload: dict[str, Any], timeout: float) -> Any:
    try:
        parsed = urllib.parse.urlsplit(validate_t3_origin(base_url))
    except ValueError:
        raise T3HttpError("invalid T3 origin") from None
    if any(c in token for c in "\r\n") or timeout <= 0 or timeout > 30:
        raise T3HttpError("invalid T3 WebSocket credentials or timeout")
    request_body = json.dumps({"_tag": "Request", "id": "1", "tag": method,
                               "payload": payload, "headers": []}, separators=(",", ":")).encode()
    if len(request_body) > MAX_BYTES:
        raise T3HttpError("T3 RPC request exceeded size limit")
    deadline = time.monotonic() + timeout
    connection = None
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        connection = socket.create_connection((parsed.hostname, port), timeout=timeout)
        if parsed.scheme == "https":
            connection = ssl.create_default_context().wrap_socket(connection, server_hostname=parsed.hostname)
        def remaining() -> None:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError()
            connection.settimeout(left)

        def read_exact(count: int) -> bytes:
            data = bytearray()
            while len(data) < count:
                remaining()
                part = connection.recv(count - len(data))
                if not part:
                    raise T3HttpError("T3 RPC connection closed before response")
                data.extend(part)
            return bytes(data)

        def send_frame(opcode: int, data: bytes) -> None:
            n = len(data)
            length = bytes([128 | n]) if n < 126 else (b"\xfe" + struct.pack("!H", n) if n < 65536 else b"\xff" + struct.pack("!Q", n))
            mask = os.urandom(4)
            masked = bytes(v ^ mask[i % 4] for i, v in enumerate(data))
            remaining()
            connection.sendall(bytes([128 | opcode]) + length + mask + masked)

        key = base64.b64encode(os.urandom(16)).decode()
        hostname = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
        path = parsed.path.rstrip("/") + "/ws?orchestrationProtocol=2"
        upgrade = (f"GET {path} HTTP/1.1\r\nHost: {hostname}:{port}\r\nUpgrade: websocket\r\n"
                   f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
                   f"Authorization: Bearer {token}\r\n\r\n").encode()
        if len(upgrade) > MAX_UPGRADE_BYTES:
            raise T3HttpError("T3 WebSocket upgrade exceeded size limit")
        remaining()
        connection.sendall(upgrade)
        head = bytearray()
        while not head.endswith(b"\r\n\r\n"):
            if len(head) >= MAX_UPGRADE_BYTES:
                raise T3HttpError("T3 WebSocket upgrade response exceeded size limit")
            head.extend(read_exact(1))
        lines = head.decode("ascii").split("\r\n")
        if lines[0].split()[:2] != ["HTTP/1.1", "101"]:
            raise T3HttpError("T3 rejected WebSocket upgrade")
        headers = {}
        for line in lines[1:-2]:
            name, value = line.split(":", 1)
            name = name.lower()
            if name in headers:
                raise T3HttpError("T3 returned duplicate WebSocket upgrade headers")
            headers[name] = value.strip()
        expected = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
        if headers.get("sec-websocket-accept") != expected or headers.get("upgrade", "").lower() != "websocket" or "upgrade" not in {v.strip() for v in headers.get("connection", "").lower().split(",")}:
            raise T3HttpError("invalid T3 WebSocket upgrade response")
        send_frame(1, request_body)
        message = bytearray()
        fragmented = False
        consumed = 0
        # Bound control frames, unrelated RPC messages, and fragmented data too.
        for _ in range(1024):
            first, second = read_exact(2)
            fin, opcode = bool(first & 128), first & 15
            if first & 112 or second & 128:
                raise T3HttpError("invalid T3 WebSocket frame flags")
            size = second & 127
            if size == 126:
                size = struct.unpack("!H", read_exact(2))[0]
            elif size == 127:
                size = struct.unpack("!Q", read_exact(8))[0]
            consumed += size
            if consumed > MAX_BYTES:
                raise T3HttpError("T3 RPC response exceeded size limit")
            if opcode >= 8 and (not fin or size > 125):
                raise T3HttpError("invalid T3 WebSocket control frame")
            data = read_exact(size)
            if opcode == 8:
                raise T3HttpError("T3 RPC closed before response")
            if opcode == 9:
                send_frame(10, data)
                continue
            if opcode == 10:
                continue
            if opcode == 1 and not fragmented:
                message = bytearray(data)
            elif opcode == 0 and fragmented:
                message.extend(data)
            else:
                raise T3HttpError("unsupported T3 WebSocket frame")
            fragmented = not fin
            if not fin:
                continue
            decoded = json.loads(message)
            # Effect's JSON serializer can also batch messages into an array.
            items = decoded if isinstance(decoded, list) else [decoded]
            for item in items:
                if not isinstance(item, dict):
                    raise T3HttpError("invalid T3 RPC response")
                if item.get("_tag") == "Ping":
                    send_frame(1, b'{"_tag":"Pong"}')
                    continue
                if item.get("_tag") == "Defect":
                    raise T3HttpError("T3 RPC server defect")
                if item.get("_tag") != "Exit" or str(item.get("requestId")) != "1":
                    continue
                result = item.get("exit")
                if not isinstance(result, dict) or result.get("_tag") != "Success" or "value" not in result:
                    raise T3HttpError("T3 RPC rejected command")
                return result["value"]
        raise T3HttpError("T3 RPC response frame limit exceeded")
    except T3HttpError:
        raise
    except (OSError, ValueError, UnicodeError, struct.error) as error:
        # Never echo server payloads, upgrade credentials, or provider errors.
        raise T3HttpError("T3 RPC transport failed") from error
    finally:
        if connection is not None:
            connection.close()
