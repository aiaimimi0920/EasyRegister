from __future__ import annotations

import json
import sys
import tempfile
import unittest
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


SRC_ROOT = Path(__file__).resolve().parents[1] / "server" / "services" / "orchestration_service" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import dashboard_server  # noqa: E402


class DashboardIntegrationTests(unittest.TestCase):
    def test_dashboard_http_server_serves_live_status_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            shared_root = Path(tmp_dir) / "shared"
            output_root = Path(tmp_dir) / "output"
            service_state = dashboard_server.ServiceRuntimeState(
                shared_root=shared_root,
                instance_id="instance-main",
                instance_role="main",
                flow_path="flow.json",
                output_root=str(output_root),
                worker_count=1,
                delay_seconds=1.0,
                worker_stagger_seconds=0.0,
                small_success_pool_dir=str(shared_root / "small-success-pool"),
            )
            service_state.started(pid=1234, max_runs=0)
            worker_state = dashboard_server.WorkerRuntimeState(
                shared_root=shared_root,
                instance_id="instance-main",
                instance_role="main",
                worker_id="worker-01",
            )
            worker_state.started(pid=2222, output_root=str(output_root), team_auth_pinned=False)
            worker_state.run_finished(
                task_index=1,
                result={
                    "ok": True,
                    "outputs": {
                        "upload-oauth-artifact": {
                            "ok": True,
                            "object_key": "oauth/artifact.json",
                            "bucket": "bucket-a",
                            "target_folder": "oauth",
                        }
                    },
                },
                output_dir=str(output_root / "run-1"),
                finished_at=datetime.now(timezone.utc).isoformat(),
            )
            (shared_root / "small-success-pool").mkdir(parents=True, exist_ok=True)
            (shared_root / "small-success-pool" / "artifact.json").write_text("{}", encoding="utf-8")

            with mock.patch.object(
                dashboard_server.DashboardHTTPServer,
                "_fetch_easy_protocol_stats",
                return_value={
                    "services": [
                        {
                            "service": "PythonProtocol-1",
                            "active_requests": 2,
                            "success_count": 5,
                            "failure_count": 1,
                            "cooldown_count": 0,
                        }
                    ]
                },
            ):
                server = dashboard_server.DashboardHTTPServer(
                    listen="127.0.0.1:0",
                    shared_root=shared_root,
                    easy_protocol_base_url="http://example.test/api/public/request",
                    easy_protocol_token="secure-token",
                    easy_protocol_actor="actor",
                    recent_window_seconds=900,
                )
                server.start()
                try:
                    port = int(server._httpd.server_address[1])
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status", timeout=5) as response:
                        payload = json.loads(response.read().decode("utf-8"))
                finally:
                    server.stop()

        self.assertEqual("instance-main", payload["pipelines"]["main"]["instanceId"])
        self.assertEqual(1, payload["pipelines"]["main"]["configuredWorkers"])
        self.assertEqual(0, payload["pipelines"]["main"]["activeWorkers"])
        self.assertEqual(1, payload["recentUploads"]["count"])
        self.assertEqual("oauth/artifact.json", payload["recentUploads"]["items"][0]["objectKey"])
        self.assertEqual(1, payload["smallSuccessPool"]["size"])
        self.assertEqual("PythonProtocol-1", payload["executors"][0]["service"])
