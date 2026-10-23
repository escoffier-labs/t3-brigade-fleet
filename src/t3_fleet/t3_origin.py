"""The target-local HTTP(S) origin shared by config, HTTP and WebSocket clients."""
from __future__ import annotations

import ipaddress
import re
import urllib.parse

_AUTHORITY = re.compile(r"(?:\[[0-9a-fA-F:]+\]|[0-9.]+)(?::[0-9]+)?")


def validate_t3_origin(value: object) -> str:
    """Accept only literal loopback origins, without DNS or URL normalization."""
    try:
        if not isinstance(value, str) or not value or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value):
            raise ValueError
        if "?" in value or "#" in value:
            raise ValueError
        parsed = urllib.parse.urlsplit(value)
        if (parsed.scheme not in {"http", "https"}
                or not _AUTHORITY.fullmatch(parsed.netloc)
                or parsed.path not in {"", "/"}
                or parsed.username is not None or parsed.password is not None):
            raise ValueError
        address = ipaddress.ip_address(parsed.hostname)
        if not address.is_loopback or (parsed.port is not None and not 1 <= parsed.port <= 65535):
            raise ValueError
    except (ValueError, TypeError):
        raise ValueError("invalid T3 origin") from None
    return value
