"""Lightweight client for EasyProxy management API (lease-based proxy rotation)."""
from __future__ import annotations

import base64
import ipaddress
import json
import os
import re
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


EASY_PROXY_BASE_URL = (
    os.environ.get("EASY_PROXY_BASE_URL") or "http://127.0.0.1:29888"
).strip()
EASY_PROXY_API_KEY = (
    os.environ.get("EASY_PROXY_MANAGEMENT_PASSWORD")
    or os.environ.get("EASY_PROXY_API_KEY")
    or ""
).strip()
EASY_PROXY_HOST_ID = (
    os.environ.get("EASY_PROXY_HOST_ID") or "python-protocol-buy-service"
).strip()
EASY_PROXY_TTL_MINUTES = int(os.environ.get("EASY_PROXY_TTL_MINUTES") or "30")
DEFAULT_EASY_PROXY_READY_TIMEOUT_SECONDS = 90
DEFAULT_EASY_PROXY_READY_PROBE_INTERVAL_SECONDS = 2
DEFAULT_EASY_PROXY_API_TIMEOUT_SECONDS = 10.0
DEFAULT_INITIAL_PROXY_PROBE_MAX_ATTEMPTS = 4
DEFAULT_INITIAL_PROXY_PROBE_BACKOFF_SECONDS = 1.0


class EasyProxyAPIError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        error_code: str = "",
    ) -> None:
        super().__init__(message)
        self.status_code = int(status_code)
        self.error_code = str(error_code or "").strip()


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(str(os.environ.get(name) or default).strip()))
    except (TypeError, ValueError):
        return max(minimum, default)


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        return max(minimum, int(str(os.environ.get(name) or default).strip()))
    except (TypeError, ValueError):
        return max(minimum, default)


def _resolve_api_timeout_seconds() -> float:
    return _env_float(
        "EASY_PROXY_API_TIMEOUT_SECONDS",
        DEFAULT_EASY_PROXY_API_TIMEOUT_SECONDS,
        minimum=0.1,
    )


def _resolve_initial_probe_max_attempts() -> int:
    return _env_int(
        "EASY_PROXY_INITIAL_PROBE_MAX_ATTEMPTS",
        DEFAULT_INITIAL_PROXY_PROBE_MAX_ATTEMPTS,
    )


def _resolve_initial_probe_backoff_seconds() -> float:
    return _env_float(
        "EASY_PROXY_INITIAL_PROBE_BACKOFF_SECONDS",
        DEFAULT_INITIAL_PROXY_PROBE_BACKOFF_SECONDS,
    )


def _management_username() -> str:
    return str(os.environ.get("EASY_PROXY_MANAGEMENT_USERNAME") or "").strip()


DEFAULT_CANONICAL_MANAGEMENT_USERNAME = "easyproxy"


def _redact_sensitive_payload(value: Any, *, key: str = "") -> Any:
    normalized_key = re.sub(r"[^a-z0-9]", "", str(key or "").lower())
    if normalized_key and any(
        marker in normalized_key
        for marker in (
            "password",
            "token",
            "secret",
            "apikey",
            "authorization",
            "cookie",
            "proxyurl",
        )
    ):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {
            str(child_key): _redact_sensitive_payload(child_value, key=str(child_key))
            for child_key, child_value in value.items()
        }
    if isinstance(value, list):
        return [_redact_sensitive_payload(item) for item in value]
    return value


def redact_easy_proxy_error(value: object) -> str:
    text = str(value or "")
    text = re.sub(
        r"(?i)\b((?:https?|socks5h?|mixed)://)([^\s/:@]+):([^\s/@]+)@",
        r"\1[REDACTED]:[REDACTED]@",
        text,
    )
    text = re.sub(
        r"(?i)(authorization|proxy-authorization|password|token|secret|api[_-]?key|cookie|proxyurl)"
        r"(\s*[:=]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;}]+)",
        r"\1\2[REDACTED]",
        text,
    )
    return text


def _http_error_details(exc: urllib.error.HTTPError) -> tuple[str, str]:
    raw = ""
    try:
        raw = exc.read().decode("utf-8", errors="replace")[:4096]
    except Exception:
        pass
    payload: Any = None
    if raw:
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            payload = None
    error_code = ""
    if isinstance(payload, dict):
        error_code = str(payload.get("error") or payload.get("code") or "").strip()
        detail = json.dumps(
            _redact_sensitive_payload(payload),
            ensure_ascii=False,
            separators=(",", ":"),
        )[:500]
    else:
        detail = redact_easy_proxy_error(raw)[:500]
    return error_code, detail


def _api_error(
    method: str,
    path: str,
    exc: urllib.error.HTTPError,
) -> EasyProxyAPIError:
    error_code, detail = _http_error_details(exc)
    suffix = f": {detail}" if detail else ""
    return EasyProxyAPIError(
        f"EasyProxy API {method} {path} returned {exc.code}{suffix}",
        status_code=exc.code,
        error_code=error_code,
    )


def _discover_management_auth(
    base_url: str,
    opener: urllib.request.OpenerDirector,
) -> dict[str, Any]:
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/auth",
        headers={"Accept": "application/json"},
        method="GET",
    )
    try:
        payload = _read_json_response(opener, req)
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 405):
            return {"legacy_auth": True}
        raise _api_error("GET", "/api/auth", exc) from exc
    if not isinstance(payload, dict):
        raise RuntimeError("EasyProxy GET /api/auth returned a non-object response")
    return payload


def _management_headers(
    base_url: str,
    *,
    api_key: str,
    opener: urllib.request.OpenerDirector,
    discovery: dict[str, Any] | None = None,
    credential: str | None = None,
) -> dict[str, str]:
    discovery = discovery if discovery is not None else _discover_management_auth(base_url, opener)
    headers = {"Accept": "application/json"}
    if bool(discovery.get("no_password")):
        return headers
    effective_key = (credential if credential is not None else (api_key or EASY_PROXY_API_KEY)).strip()
    if not effective_key:
        return headers
    if discovery.get("legacy_auth"):
        headers["Authorization"] = f"Bearer {effective_key}"
        return headers
    username = _management_username()
    if str(discovery.get("auth_mode") or "").strip() == "canonical_pair":
        username = username or DEFAULT_CANONICAL_MANAGEMENT_USERNAME
        encoded = base64.b64encode(f"{username}:{effective_key}".encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Basic {encoded}"
        return headers
    headers["Authorization"] = effective_key
    return headers


def _management_credentials(api_key: str) -> list[str]:
    """Return the configured credential followed by the legacy alias, if distinct.

    Deployments made before the management-password rename can retain both
    variables, with only ``EASY_PROXY_API_KEY`` still matching the NAS service.
    The first value remains authoritative; callers may retry the alias only
    after an explicit authentication failure.
    """
    credentials: list[str] = []
    for value in (
        api_key,
        os.environ.get("EASY_PROXY_MANAGEMENT_PASSWORD"),
        os.environ.get("EASY_PROXY_API_KEY"),
    ):
        normalized = str(value or "").strip()
        if normalized and normalized not in credentials:
            credentials.append(normalized)
    return credentials


def _management_header_candidates(
    base_url: str,
    *,
    api_key: str,
    opener: urllib.request.OpenerDirector,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    discovery = _discover_management_auth(base_url, opener)
    credentials = _management_credentials(api_key)
    if not credentials:
        return discovery, [_management_headers(base_url, api_key="", opener=opener, discovery=discovery)]
    return discovery, [
        _management_headers(
            base_url,
            api_key=api_key,
            opener=opener,
            discovery=discovery,
            credential=credential,
        )
        for credential in credentials
    ]


def _api_request(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    base_url: str = "",
    api_key: str = "",
    wait_for_ready: bool = True,
) -> dict[str, Any]:
    effective_base = (base_url or EASY_PROXY_BASE_URL).rstrip("/")
    if wait_for_ready and _should_wait_for_easy_proxy(path):
        _wait_easy_proxy_ready(effective_base, api_key=api_key)
    url = f"{effective_base}{path}"
    opener = _build_management_opener(effective_base)
    data = json.dumps(body).encode("utf-8") if body is not None else None
    _, header_candidates = _management_header_candidates(
        effective_base,
        api_key=api_key,
        opener=opener,
    )
    last_error: urllib.error.HTTPError | None = None
    for index, headers in enumerate(header_candidates):
        headers = dict(headers)
        headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            return _read_json_response(opener, req)
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code != 401 or index + 1 >= len(header_candidates):
                raise _api_error(method, path, exc) from exc
    if last_error is not None:
        raise _api_error(method, path, last_error) from last_error
    raise RuntimeError(f"EasyProxy API {method} {path} produced no response")


def _read_json_response(opener: urllib.request.OpenerDirector, req: urllib.request.Request) -> dict[str, Any]:
    with opener.open(req, timeout=_resolve_api_timeout_seconds()) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _should_wait_for_easy_proxy(path: str) -> bool:
    normalized = str(path or "").strip().lower()
    return normalized.startswith("/api/nodes") or normalized.startswith("/proxy/leases/checkout")


def _resolve_easy_proxy_ready_timeout_seconds() -> int:
    raw = str(os.environ.get("EASY_PROXY_READY_TIMEOUT_SECONDS") or "").strip()
    try:
        return max(1, int(raw or DEFAULT_EASY_PROXY_READY_TIMEOUT_SECONDS))
    except Exception:
        return DEFAULT_EASY_PROXY_READY_TIMEOUT_SECONDS


def _resolve_easy_proxy_ready_probe_interval_seconds() -> int:
    raw = str(os.environ.get("EASY_PROXY_READY_PROBE_INTERVAL_SECONDS") or "").strip()
    try:
        return max(1, int(raw or DEFAULT_EASY_PROXY_READY_PROBE_INTERVAL_SECONDS))
    except Exception:
        return DEFAULT_EASY_PROXY_READY_PROBE_INTERVAL_SECONDS


def _wait_easy_proxy_ready(base_url: str, *, api_key: str = "") -> None:
    deadline = time.time() + _resolve_easy_proxy_ready_timeout_seconds()
    interval = _resolve_easy_proxy_ready_probe_interval_seconds()
    last_error: Exception | None = None
    opener = _build_management_opener(base_url)
    _, header_candidates = _management_header_candidates(
        base_url,
        api_key=api_key,
        opener=opener,
    )

    filtered_probe_url = f"{base_url.rstrip('/')}/api/nodes?only_available=1&prefer_available=1"
    fallback_probe_url = f"{base_url.rstrip('/')}/api/nodes"
    while time.time() < deadline:
        for headers in header_candidates:
            for probe_url, allow_local_filter in (
                (filtered_probe_url, False),
                (fallback_probe_url, True),
            ):
                try:
                    req = urllib.request.Request(probe_url, headers=headers, method="GET")
                    payload = _read_json_response(opener, req)
                    available_nodes = int(payload.get("available_nodes") or 0)
                    if allow_local_filter and available_nodes <= 0:
                        available_nodes = len(_normalize_node_list(payload, only_available=True, prefer_available=True))
                    if available_nodes > 0:
                        return
                    last_error = RuntimeError(f"EasyProxy not ready: available_nodes={available_nodes}")
                    if allow_local_filter:
                        break
                except urllib.error.HTTPError as exc:
                    last_error = exc
                    if exc.code != 401:
                        break
                except Exception as exc:
                    last_error = exc
                    if allow_local_filter:
                        break
        time.sleep(interval)
    raise RuntimeError(f"EasyProxy not ready after wait: {last_error}") from last_error


def _build_management_opener(base_url: str) -> urllib.request.OpenerDirector:
    parsed = urllib.parse.urlsplit(str(base_url or "").strip())
    host = str(parsed.hostname or "").strip()
    should_bypass_proxy = host in (
        "127.0.0.1",
        "localhost",
        "::1",
        "0.0.0.0",
        "easy-proxy",
        "easy-proxy-monorepo-service",
    )
    if not should_bypass_proxy and host:
        try:
            ip = ipaddress.ip_address(host)
            should_bypass_proxy = bool(ip.is_loopback or ip.is_private or ip.is_link_local)
        except ValueError:
            should_bypass_proxy = False
    if should_bypass_proxy:
        return urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return urllib.request.build_opener()


def _node_marked_available(node: dict[str, Any]) -> bool:
    return bool(node.get("effective_available") or node.get("available"))


def _node_sort_key(node: dict[str, Any]) -> tuple[int, int]:
    try:
        score = int(node.get("availability_score") or 0)
    except Exception:
        score = 0
    return (0 if _node_marked_available(node) else 1, -score)


def _normalize_node_list(
    payload: dict[str, Any],
    *,
    only_available: bool,
    prefer_available: bool,
) -> list[dict[str, Any]]:
    nodes = payload.get("nodes") or []
    normalized = [node for node in nodes if isinstance(node, dict)]
    if only_available:
        normalized = [node for node in normalized if _node_marked_available(node)]
    if prefer_available:
        normalized.sort(key=_node_sort_key)
    return normalized


def checkout_proxy(
    *,
    host_id: str = "",
    ttl_minutes: int = 0,
    base_url: str = "",
    api_key: str = "",
    metadata: dict[str, str] | None = None,
    require_dedicated_node: bool = False,
) -> dict[str, Any]:
    body = {
        "hostId": host_id or EASY_PROXY_HOST_ID,
        "providerTypeKey": "easy-proxies",
        "provisionMode": "reuse-only",
        "bindingMode": "shared-instance",
        "protocol": "http",
        "ttlMinutes": ttl_minutes or EASY_PROXY_TTL_MINUTES,
        "metadata": metadata or {"source": "python-protocol-buy-service"},
    }
    result: dict[str, Any] | None = None
    max_attempts = _resolve_initial_probe_max_attempts()
    backoff_seconds = _resolve_initial_probe_backoff_seconds()
    for attempt in range(max_attempts):
        try:
            result = _api_request(
                "POST",
                "/proxy/leases/checkout",
                body,
                base_url=base_url,
                api_key=api_key,
                wait_for_ready=attempt == 0,
            )
            break
        except EasyProxyAPIError as exc:
            # EasyProxy emits this before request parsing and lease creation; transport
            # failures and other 503 responses remain non-retryable.
            pending = (
                exc.status_code == 503
                and exc.error_code.upper() == "INITIAL_PROXY_PROBE_PENDING"
            )
            if not pending or attempt + 1 >= max_attempts:
                raise
            time.sleep(backoff_seconds * (2**attempt))
    if result is None:
        raise RuntimeError("EasyProxy checkout produced no response")
    lease = (result.get("result") or {}).get("lease") or {}
    try:
        _validate_checkout_lease(
            lease,
            result=result,
            require_dedicated_node=require_dedicated_node,
        )
    except Exception:
        lease_id = str(lease.get("id") or "").strip()
        if lease_id:
            release_lease(lease_id, base_url=base_url, api_key=api_key)
        raise
    return lease


def get_settings(
    *,
    base_url: str = "",
    api_key: str = "",
) -> dict[str, Any]:
    return _api_request("GET", "/api/settings", base_url=base_url, api_key=api_key)


def _should_fallback_filtered_nodes_error(exc: Exception) -> bool:
    if isinstance(exc, TypeError):
        return True
    if not isinstance(exc, EasyProxyAPIError):
        return False
    if exc.status_code in {400, 404, 405, 501}:
        return True
    return (
        exc.status_code == 503
        and exc.error_code.upper() == "INITIAL_PROXY_PROBE_PENDING"
    )


def list_available_nodes(
    *,
    base_url: str = "",
    api_key: str = "",
    only_available: bool = True,
    prefer_available: bool = True,
) -> list[dict[str, Any]]:
    query = []
    if only_available:
        query.append("only_available=1")
    if prefer_available:
        query.append("prefer_available=1")
    suffix = f"?{'&'.join(query)}" if query else ""
    path = f"/api/nodes{suffix}"
    fallback_used = False
    try:
        payload = _api_request(
            "GET",
            path,
            base_url=base_url,
            api_key=api_key,
            wait_for_ready=False,
        )
        if suffix:
            filtered_nodes = payload.get("nodes")
            if filtered_nodes is not None and not isinstance(filtered_nodes, list):
                raise TypeError("EasyProxy nodes response must be a list")
    except (EasyProxyAPIError, TypeError) as exc:
        if not suffix or not _should_fallback_filtered_nodes_error(exc):
            raise
        payload = _api_request(
            "GET",
            "/api/nodes",
            base_url=base_url,
            api_key=api_key,
            wait_for_ready=False,
        )
        fallback_used = True
    normalized_nodes = _normalize_node_list(
        payload,
        only_available=only_available,
        prefer_available=prefer_available,
    )
    if not fallback_used and suffix and not normalized_nodes:
        payload = _api_request(
            "GET",
            "/api/nodes",
            base_url=base_url,
            api_key=api_key,
            wait_for_ready=False,
        )
        fallback_used = True
        normalized_nodes = _normalize_node_list(
            payload,
            only_available=only_available,
            prefer_available=prefer_available,
        )
    if fallback_used:
        return normalized_nodes
    nodes = payload.get("nodes") or []
    if not isinstance(nodes, list):
        return []
    return [node for node in nodes if isinstance(node, dict)]


def checkout_random_node_proxy(
    *,
    base_url: str = "",
    api_key: str = "",
    runtime_host: str = "",
    host_id: str = "",
    excluded_proxy_urls: set[str] | None = None,
) -> dict[str, Any]:
    settings = get_settings(base_url=base_url, api_key=api_key)
    nodes = list_available_nodes(base_url=base_url, api_key=api_key, only_available=True, prefer_available=True)
    if not nodes:
        raise RuntimeError("EasyProxy random node checkout found no available nodes")

    rng = secrets.SystemRandom()
    traffic_proven = [node for node in nodes if bool(node.get("traffic_proven_usable"))]
    remaining = [node for node in nodes if not bool(node.get("traffic_proven_usable"))]
    rng.shuffle(traffic_proven)
    rng.shuffle(remaining)
    candidates = traffic_proven + remaining
    excluded = {
        _proxy_url_comparison_key(item)
        for item in (excluded_proxy_urls or set())
        if str(item).strip()
    }

    protocol = str(
        settings.get("multi_port_protocol")
        or settings.get("listener_protocol")
        or "http"
    ).strip() or "http"
    username = str(
        settings.get("multi_port_username")
        or settings.get("listener_username")
        or ""
    ).strip()
    username = _proxy_username_for_host(username, host_id)
    password = str(
        settings.get("multi_port_password")
        or settings.get("listener_password")
        or api_key
        or EASY_PROXY_API_KEY
        or ""
    ).strip()
    host = _resolve_runtime_host(base_url=base_url, runtime_host=runtime_host)

    for node in candidates:
        try:
            port = int(node.get("port") or 0)
        except Exception:
            port = 0
        if port <= 0:
            continue
        proxy_url = _build_proxy_url(
            protocol=protocol,
            host=host,
            port=port,
            username=username,
            password=password,
        )
        if _proxy_url_comparison_key(proxy_url) in excluded:
            continue
        return {
            "id": "",
            "proxyUrl": proxy_url,
            "host": host,
            "port": port,
            "protocol": protocol,
            "username": username,
            "password": password,
            "metadata": {
                "selectedNodeTag": str(node.get("tag") or "").strip(),
                "selectedNodeName": str(node.get("name") or "").strip(),
                "selectedNodePort": str(port),
                "selectedNodeMode": "dedicated-node",
                "selectedNodeAvailability": str(bool(node.get("available"))).lower(),
                "selectedNodeAvailabilityScore": str(node.get("availability_score") or ""),
                "selectedNodeRegion": str(node.get("region") or "").strip(),
                "selectedNodeCountry": str(node.get("country") or "").strip(),
                "selectedNodeProtocolFamily": str(node.get("protocol_family") or "").strip(),
                "selectedNodeDomainFamily": str(node.get("domain_family") or "").strip(),
                "selectedNodeSourceRef": str(node.get("source_ref") or "").strip(),
                "selectedNodeSelectionTier": "random-node",
            },
        }

    raise RuntimeError("EasyProxy random node checkout exhausted available nodes")


def release_lease(
    lease_id: str,
    *,
    base_url: str = "",
    api_key: str = "",
) -> None:
    if not lease_id:
        return
    try:
        _api_request("POST", f"/proxy/leases/{lease_id}/release", {}, base_url=base_url, api_key=api_key)
    except Exception:
        pass


def report_usage(
    lease_id: str,
    *,
    success: bool,
    latency_ms: int = 0,
    error_code: str = "",
    service_key: str = "",
    stage: str = "",
    failure_class: str = "",
    route_confidence: str = "",
    base_url: str = "",
    api_key: str = "",
) -> None:
    if not lease_id:
        return
    try:
        _api_request(
            "POST",
            "/proxy/leases/report",
            {
                "leaseId": lease_id,
                "success": success,
                "latencyMs": latency_ms,
                "errorCode": error_code,
                "serviceKey": service_key,
                "stage": stage,
                "failureClass": failure_class,
                "routeConfidence": route_confidence,
            },
            base_url=base_url,
            api_key=api_key,
        )
    except Exception:
        pass


def _resolve_runtime_host(*, base_url: str, runtime_host: str) -> str:
    value = str(runtime_host or "").strip()
    if value:
        return value
    parsed = urllib.parse.urlsplit((base_url or EASY_PROXY_BASE_URL).strip())
    host = str(parsed.hostname or "127.0.0.1").strip()
    if host in ("", "0.0.0.0", "::", "[::]", "localhost"):
        return "127.0.0.1"
    return host


def _proxy_username_for_host(base: str, host_id: str) -> str:
    """Mirror EasyProxy's Local Server device-username compatibility contract."""
    username = str(base or "").strip()
    normalized_host_id = str(host_id or "").strip().lower()
    if not username or not 1 <= len(normalized_host_id) <= 64:
        return username
    if re.fullmatch(r"[a-z0-9._-]+", normalized_host_id) is None:
        return username
    return f"{username}+dev={normalized_host_id}"


def _proxy_url_comparison_key(proxy_url: object) -> str:
    raw = str(proxy_url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urllib.parse.urlsplit(raw)
    except Exception:
        return raw.lower()
    if str(parsed.scheme or "").strip().lower() != "mixed":
        return raw.lower()
    return urllib.parse.urlunsplit(
        (
            "http",
            parsed.netloc,
            parsed.path or "",
            parsed.query or "",
            parsed.fragment or "",
        )
    ).lower()


def _build_proxy_url(
    *,
    protocol: str,
    host: str,
    port: int,
    username: str,
    password: str,
) -> str:
    scheme = str(protocol or "http").strip() or "http"
    if username:
        quoted_user = urllib.parse.quote(username, safe="")
        quoted_password = urllib.parse.quote(password, safe="")
        return f"{scheme}://{quoted_user}:{quoted_password}@{host}:{port}"
    return f"{scheme}://{host}:{port}"


def _coerce_port(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _validate_checkout_lease(
    lease: dict[str, Any],
    *,
    result: dict[str, Any],
    require_dedicated_node: bool,
) -> None:
    if not lease.get("proxyUrl"):
        safe_result = json.dumps(
            _redact_sensitive_payload(result),
            ensure_ascii=False,
            separators=(",", ":"),
        )[:500]
        raise RuntimeError(f"EasyProxy checkout returned no proxyUrl: {safe_result}")
    if not require_dedicated_node:
        return

    metadata = lease.get("metadata") or {}
    selected_mode = str(metadata.get("selectedNodeMode") or "").strip().lower()
    selected_port = _coerce_port(metadata.get("selectedNodePort") or lease.get("port"))
    lease_port = _coerce_port(lease.get("port"))
    if selected_mode and selected_mode != "dedicated-node":
        raise RuntimeError(
            f"EasyProxy checkout returned non-dedicated route: {selected_mode or 'unknown'}"
        )
    if selected_port <= 0 or lease_port <= 0:
        raise RuntimeError(
            f"EasyProxy checkout returned invalid dedicated port selected={selected_port} lease={lease_port}"
        )
    if selected_port == 2323 or lease_port == 2323:
        raise RuntimeError(
            f"EasyProxy checkout returned shared listener port selected={selected_port} lease={lease_port}"
        )
