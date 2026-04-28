from __future__ import annotations

from pathlib import Path

from others.config import DashboardSettings
from others.dashboard_http import DashboardHTTPServer
from others.dashboard_state import ServiceRuntimeState
from others.dashboard_state import WorkerRuntimeState
from others.dashboard_state import dashboard_state_root as _dashboard_state_root
from others.dashboard_state import instance_root as _instance_root
from others.dashboard_state import json_default as _json_default
from others.dashboard_state import parse_iso8601 as _parse_iso8601
from others.dashboard_state import prune_recent_uploads as _prune_recent_uploads
from others.dashboard_state import read_json as _read_json
from others.dashboard_state import service_state_path as _service_state_path
from others.dashboard_state import utcnow as _utcnow
from others.dashboard_state import worker_state_path as _worker_state_path
from others.paths import resolve_shared_root as _shared_root_from_output_root


def _dashboard_listen_default() -> str:
    return "127.0.0.1:9790"


def _listen_targets_remote_host(listen: str) -> bool:
    normalized = str(listen or "").strip()
    if not normalized:
        return False
    host, _, _port_text = normalized.rpartition(":")
    candidate = (host or normalized).strip().lower()
    if candidate.startswith("[") and candidate.endswith("]"):
        candidate = candidate[1:-1].strip().lower()
    return candidate in {"0.0.0.0", "::", "*", "+"}


def _control_token_is_secure(token: str) -> bool:
    normalized = str(token or "").strip()
    if not normalized:
        return False
    return normalized not in {"123456"}


def start_dashboard_server_if_enabled(
    *,
    output_root: Path,
    easy_protocol_base_url: str,
    easy_protocol_token: str,
    easy_protocol_actor: str,
) -> DashboardHTTPServer | None:
    settings = DashboardSettings.from_env()
    if not settings.enabled:
        return None
    if not _control_token_is_secure(easy_protocol_token):
        return None
    listen = settings.listen or _dashboard_listen_default()
    if _listen_targets_remote_host(listen) and not settings.allow_remote:
        return None
    server = DashboardHTTPServer(
        listen=listen,
        shared_root=_shared_root_from_output_root(output_root),
        easy_protocol_base_url=easy_protocol_base_url,
        easy_protocol_token=easy_protocol_token,
        easy_protocol_actor=easy_protocol_actor,
        recent_window_seconds=settings.recent_window_seconds,
    )
    server.start()
    return server
