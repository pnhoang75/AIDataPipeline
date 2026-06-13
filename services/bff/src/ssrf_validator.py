import ipaddress
from urllib.parse import urlparse

from fastapi import HTTPException

_BLOCKED_SCHEMES = frozenset({"file", "gopher", "dict", "ldap", "ldaps", "ftp", "netdoc"})
_SVC_SUFFIXES = (".svc", ".svc.cluster.local", ".internal")

_BLOCKED_NETWORKS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]


def _is_blocked_ip(host: str) -> bool:
    try:
        addr = ipaddress.ip_address(host)
        return any(addr in net for net in _BLOCKED_NETWORKS)
    except ValueError:
        return False


def _is_blocked_hostname(host: str) -> bool:
    lower = host.lower()
    if lower == "localhost":
        return True
    return any(lower.endswith(s) for s in _SVC_SUFFIXES)


def _block(msg: str) -> None:
    raise HTTPException(
        status_code=400,
        detail={"error": "SSRF_BLOCKED", "message": msg},
    )


def validate_endpoint(endpoint: str) -> None:
    """Raise HTTP 400 SSRF_BLOCKED if endpoint targets an internal/restricted address."""
    try:
        parsed = urlparse(endpoint)
    except Exception:
        _block("Invalid endpoint URL")

    scheme = (parsed.scheme or "").lower()
    if scheme in _BLOCKED_SCHEMES:
        _block(f"Scheme '{scheme}' is not permitted")

    host = parsed.hostname or ""
    if not host:
        _block("No hostname found in endpoint")

    if _is_blocked_ip(host) or _is_blocked_hostname(host):
        _block(f"Internal address blocked: {host}")
