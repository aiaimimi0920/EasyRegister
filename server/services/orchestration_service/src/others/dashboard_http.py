from __future__ import annotations

import json
import threading
import urllib.request
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from others.dashboard_state import dashboard_state_root
from others.dashboard_state import json_default
from others.dashboard_state import parse_iso8601
from others.dashboard_state import read_json
from others.dashboard_state import utcnow


class DashboardHTTPServer:
    def __init__(
        self,
        *,
        listen: str,
        shared_root: Path,
        easy_protocol_base_url: str,
        easy_protocol_token: str,
        easy_protocol_actor: str,
        recent_window_seconds: int,
    ) -> None:
        host, port_text = self._parse_listen(listen)
        self._shared_root = shared_root.resolve()
        self._easy_protocol_base_url = easy_protocol_base_url.strip()
        self._easy_protocol_token = easy_protocol_token.strip()
        self._easy_protocol_actor = easy_protocol_actor.strip() or "register-dashboard"
        self._recent_window_seconds = max(60, int(recent_window_seconds or 900))
        self._httpd = ThreadingHTTPServer((host, int(port_text)), self._handler_factory())
        self._thread: threading.Thread | None = None

    @staticmethod
    def _parse_listen(listen: str) -> tuple[str, int]:
        normalized = str(listen or "").strip() or "127.0.0.1:9790"
        if ":" not in normalized:
            return normalized, 9790
        host, _, port_text = normalized.rpartition(":")
        try:
            return host or "0.0.0.0", int(port_text)
        except Exception:
            return host or "0.0.0.0", 9790

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._httpd.serve_forever, name="register-dashboard", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _handler_factory(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/" or self.path == "/index.html":
                    payload = server._render_html()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                if self.path == "/api/status":
                    body = json.dumps(server._build_status_payload(), ensure_ascii=False, default=json_default).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                self.send_response(404)
                self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:
                return

        return Handler

    def _build_status_payload(self) -> dict[str, Any]:
        now = utcnow()
        services_root = dashboard_state_root(self._shared_root)
        pipeline_payload: dict[str, Any] = {}
        recent_cutoff = now - timedelta(seconds=self._recent_window_seconds)
        recent_uploads: list[dict[str, Any]] = []

        if services_root.is_dir():
            for service_dir in sorted([item for item in services_root.iterdir() if item.is_dir()], key=lambda item: item.name.lower()):
                service_state = read_json(service_dir / "service.json")
                workers_dir = service_dir / "workers"
                workers = []
                if workers_dir.is_dir():
                    for worker_file in sorted(workers_dir.glob("*.json"), key=lambda item: item.name.lower()):
                        worker_state = read_json(worker_file)
                        if worker_state:
                            workers.append(worker_state)
                            for upload in worker_state.get("recentUploads") or []:
                                if not isinstance(upload, dict):
                                    continue
                                finished_at = parse_iso8601(upload.get("finishedAt"))
                                if finished_at is None or finished_at < recent_cutoff:
                                    continue
                                recent_uploads.append(
                                    {
                                        "instanceId": str(service_state.get("instanceId") or service_dir.name),
                                        "instanceRole": str(service_state.get("instanceRole") or service_dir.name),
                                        "workerId": str(worker_state.get("workerId") or ""),
                                        **upload,
                                    }
                                )
                configured_workers = int(service_state.get("workerCountConfigured") or len(workers) or 0)
                active_workers = sum(1 for item in workers if str(item.get("status") or "").strip().lower() == "running")
                sleeping_workers = sum(1 for item in workers if str(item.get("status") or "").strip().lower() == "sleeping")
                failed_workers = sum(1 for item in workers if str(item.get("status") or "").strip().lower() in {"failed", "crashed"})
                role = str(service_state.get("instanceRole") or service_dir.name)
                pipeline_payload[role] = {
                    "instanceId": str(service_state.get("instanceId") or service_dir.name),
                    "configuredWorkers": configured_workers,
                    "activeWorkers": active_workers,
                    "sleepingWorkers": sleeping_workers,
                    "failedWorkers": failed_workers,
                    "workers": workers,
                }

        small_success_pool_dir = self._shared_root / "small-success-pool"
        small_success_pool_size = len(list(small_success_pool_dir.glob("*.json"))) if small_success_pool_dir.is_dir() else 0

        easy_protocol_stats = self._fetch_easy_protocol_stats()
        executor_rows = []
        for item in (easy_protocol_stats.get("services") or []):
            if not isinstance(item, dict):
                continue
            name = str(item.get("service") or "").strip()
            if not name.startswith("PythonProtocol-"):
                continue
            success_count = int(item.get("success_count") or 0)
            failure_count = int(item.get("failure_count") or 0)
            executor_rows.append(
                {
                    "service": name,
                    "activeRequests": int(item.get("active_requests") or 0),
                    "hitCount": success_count + failure_count,
                    "successCount": success_count,
                    "failureCount": failure_count,
                    "cooldownCount": int(item.get("cooldown_count") or 0),
                }
            )
        executor_rows.sort(key=lambda row: row["service"])
        recent_uploads.sort(key=lambda item: str(item.get("finishedAt") or ""), reverse=True)

        return {
            "generatedAt": now.isoformat(),
            "pipelines": pipeline_payload,
            "smallSuccessPool": {
                "path": str(small_success_pool_dir),
                "size": small_success_pool_size,
            },
            "recentUploads": {
                "windowSeconds": self._recent_window_seconds,
                "count": len(recent_uploads),
                "items": recent_uploads[:20],
            },
            "executors": executor_rows,
            "easyProtocol": {
                "baseUrl": self._easy_protocol_base_url,
            },
        }

    def _fetch_easy_protocol_stats(self) -> dict[str, Any]:
        base = self._easy_protocol_base_url.rstrip("/")
        if base.endswith("/api/public/request"):
            base = base[: -len("/api/public/request")]
        url = base + "/api/internal/stats"
        req = urllib.request.Request(
            url,
            method="GET",
            headers={
                "Authorization": f"Bearer {self._easy_protocol_token}",
                "X-EasyProtocol-Actor": self._easy_protocol_actor,
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return json.loads(resp.read().decode("utf-8", errors="replace"))
        except Exception:
            return {}

    def _render_html(self) -> bytes:
        html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Register Dashboard</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #07111f;
      --panel: #0d1728;
      --panel-2: #111f35;
      --text: #e7eefc;
      --muted: #8ea4c7;
      --line: rgba(255,255,255,0.08);
      --ok: #44d67b;
      --warn: #ffcc66;
      --bad: #ff6b6b;
      --accent: #4db5ff;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Segoe UI, system-ui, sans-serif;
      background: linear-gradient(180deg, #07111f 0%, #0b1730 100%);
      color: var(--text);
    }}
    .wrap {{
      max-width: 1500px;
      margin: 0 auto;
      padding: 20px;
    }}
    h1, h2 {{
      margin: 0 0 12px;
      font-weight: 600;
    }}
    .sub {{
      color: var(--muted);
      margin-bottom: 20px;
    }}
    .grid {{
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      margin-bottom: 16px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 16px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.18);
    }}
    .metric-label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    .metric-value {{
      font-size: 32px;
      font-weight: 700;
      margin-top: 8px;
    }}
    .section {{
      margin-top: 18px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      overflow: hidden;
    }}
    th, td {{
      text-align: left;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      font-size: 14px;
    }}
    th {{
      background: var(--panel-2);
      color: var(--muted);
      font-weight: 600;
    }}
    .tag {{
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 12px;
      border: 1px solid var(--line);
      color: var(--muted);
    }}
    .ok {{ color: var(--ok); }}
    .warn {{ color: var(--warn); }}
    .bad {{ color: var(--bad); }}
    code {{
      font-family: Consolas, monospace;
      font-size: 12px;
      color: var(--accent);
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Register Dashboard</h1>
    <div class="sub">Real-time orchestration, executor, pool, and upload status</div>

    <div class="grid" id="summary"></div>

    <div class="section">
      <h2>Pipelines</h2>
      <table>
        <thead>
          <tr>
            <th>Role</th>
            <th>Configured</th>
            <th>Active</th>
            <th>Sleeping</th>
            <th>Failed</th>
          </tr>
        </thead>
        <tbody id="pipelines-body"></tbody>
      </table>
    </div>

    <div class="section">
      <h2>Executors</h2>
      <table>
        <thead>
          <tr>
            <th>Service</th>
            <th>Active Requests</th>
            <th>Hit Count</th>
            <th>Success</th>
            <th>Failure</th>
          </tr>
        </thead>
        <tbody id="executors-body"></tbody>
      </table>
    </div>

    <div class="section">
      <h2>Recent Uploads</h2>
      <table>
        <thead>
          <tr>
            <th>Finished At</th>
            <th>Role</th>
            <th>Worker</th>
            <th>Object Key</th>
          </tr>
        </thead>
        <tbody id="uploads-body"></tbody>
      </table>
    </div>
  </div>

  <script>
    async function refresh() {{
      const response = await fetch('/api/status', {{ cache: 'no-store' }});
      const data = await response.json();

      const pipelines = data.pipelines || {{}};
      const executors = data.executors || [];
      const uploads = (data.recentUploads && data.recentUploads.items) || [];

      const summary = [
        ['Main Active / Configured', `${{(pipelines.main?.activeWorkers ?? 0)}} / ${{(pipelines.main?.configuredWorkers ?? 0)}}`],
        ['Continue Active / Configured', `${{(pipelines.continue?.activeWorkers ?? 0)}} / ${{(pipelines.continue?.configuredWorkers ?? 0)}}`],
        ['Small Success Pool Size', `${{data.smallSuccessPool?.size ?? 0}}`],
        ['Recent Upload Successes', `${{data.recentUploads?.count ?? 0}}`],
      ];
      document.getElementById('summary').innerHTML = summary.map(([label, value]) => `
        <div class="card">
          <div class="metric-label">${{label}}</div>
          <div class="metric-value">${{value}}</div>
        </div>
      `).join('');

      document.getElementById('pipelines-body').innerHTML = Object.entries(pipelines).map(([role, item]) => `
        <tr>
          <td><span class="tag">${{role}}</span></td>
          <td>${{item.configuredWorkers ?? 0}}</td>
          <td class="ok">${{item.activeWorkers ?? 0}}</td>
          <td class="warn">${{item.sleepingWorkers ?? 0}}</td>
          <td class="bad">${{item.failedWorkers ?? 0}}</td>
        </tr>
      `).join('');

      document.getElementById('executors-body').innerHTML = executors.map((item) => `
        <tr>
          <td><code>${{item.service}}</code></td>
          <td>${{item.activeRequests}}</td>
          <td>${{item.hitCount}}</td>
          <td class="ok">${{item.successCount}}</td>
          <td class="bad">${{item.failureCount}}</td>
        </tr>
      `).join('');

      document.getElementById('uploads-body').innerHTML = uploads.map((item) => `
        <tr>
          <td>${{item.finishedAt ?? ''}}</td>
          <td>${{item.instanceRole ?? ''}}</td>
          <td>${{item.workerId ?? ''}}</td>
          <td><code>${{item.objectKey ?? ''}}</code></td>
        </tr>
      `).join('');
    }}
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>"""
        return html.encode("utf-8")
