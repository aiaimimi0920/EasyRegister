"""Build and verify a one-shot dependency diagnostic without replacing workers."""
from __future__ import annotations

import hashlib
import json
import shlex
import sys
from pathlib import Path

import paramiko


ROOT = Path(__file__).resolve().parents[1]
BASE = "easy-register/easy-register:nas-dst-20260908-003"
BASE_ID = "sha256:dab0f426e409201315c933d2840710780b2d6dd9bfd8dc305aa167c6171320a0"
CANDIDATE = "easy-register/diagnostics:nas-dst-preflight-20260908-001"
SOURCE = "server/services/orchestration_service/src/nas_dst_smoke.py"
FLOW = "server/services/orchestration_service/flows/nas-dependencies-smoke-v1.semantic-flow.json"

REMOTE = r'''
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import time
from datetime import datetime, timezone

payload = json.load(sys.stdin)
summary = {
    "scope": "packaged dependency DST; no account registration or SMS",
    "baseImage": payload["base"],
    "baseImageId": payload["baseId"],
    "imageTag": payload["candidate"],
    "sourceSha256": payload["sourceSha256"],
    "flowSha256": payload["flowSha256"],
    "runs": [],
    "ok": False,
}
names = ["easy-register", "easy-register-protocol-python", "easy-register-protocol", "easy-register-sms"]
created = []
before = None

def docker(*args, **kwargs):
    return subprocess.run(["docker", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=kwargs.pop("timeout", 30), **kwargs)

def require(condition, code):
    if not condition:
        raise RuntimeError(code)

def inspect(kind, name):
    response = docker(kind, "inspect", name)
    require(response.returncode == 0, "inspect_failed")
    return json.loads(response.stdout)[0]

def snapshot():
    result = {}
    for name in names:
        item = inspect("container", name)
        result[name] = {"id": item["Id"], "imageId": item["Image"],
                        "startedAt": item["State"]["StartedAt"], "running": item["State"]["Running"]}
    return result

def run_diagnostic(name, network, environment):
    require(docker("container", "inspect", name).returncode != 0, "diagnostic_name_in_use")
    created.append(name)
    command = ["run", "--rm", "--name", name, "--network", network, "--read-only",
               "--tmpfs", "/tmp:rw,nosuid,nodev,size=128m", "--cap-drop", "ALL",
               "--security-opt", "no-new-privileges", "--memory", "512m", "--pids-limit", "128"]
    for key in environment:
        command.extend(["--env", key])
    # Only names enter argv; Docker takes these four values from its environment.
    host_environment = {**os.environ, **environment, "DOCKER_HOST": "unix:///var/run/docker.sock"}
    started = time.monotonic()
    response = docker(*command, summary["imageId"], env=host_environment, timeout=240)
    result = json.loads(response.stdout)
    allowed = ("ok", "steps", "errorMarkers", "taskAttempts", "recoveryIdentityPreserved",
               "leasedProxyUsed", "cleanupErrors", "missingEnvironment", "errorType")
    return {"exitCode": response.returncode, "elapsedSeconds": round(time.monotonic() - started, 2),
            **{key: result[key] for key in allowed if key in result}}

try:
    before = snapshot()
    summary["containersBefore"] = before
    require(all(item["running"] for item in before.values()), "worker_not_running")
    require(before["easy-register"]["imageId"] == payload["baseId"], "live_image_changed")
    base = inspect("image", payload["base"])
    require(base["Id"] == payload["baseId"], "base_image_changed")
    require(not base["Config"].get("Entrypoint"), "unexpected_base_entrypoint")
    require(docker("image", "inspect", payload["candidate"]).returncode != 0, "candidate_already_exists")
    flow = docker("exec", "easy-register", "sha256sum", "/app/" + payload["flowPath"])
    require(flow.returncode == 0 and flow.stdout.decode().split()[0] == payload["flowSha256"], "flow_changed")
    live = inspect("container", "easy-register")
    require("easyregister-pc2" in live["NetworkSettings"]["Networks"], "network_changed")
    live_environment = dict(item.split("=", 1) for item in live["Config"]["Env"])
    keys = ("MAILBOX_SERVICE_BASE_URL", "MAILBOX_SERVICE_API_KEY",
            "EASY_PROXY_BASE_URL", "EASY_PROXY_MANAGEMENT_PASSWORD")
    require(all(live_environment.get(key, "").strip() for key in keys), "dependency_environment_missing")
    environment = {key: live_environment[key] for key in keys}
    require(hashlib.sha256(payload["source"].encode()).hexdigest() == payload["sourceSha256"], "source_changed")
    dockerfile = ("FROM " + payload["base"] + "\nCOPY nas_dst_smoke.py /app/" + payload["sourcePath"]
                  + '\nCMD ["python", "-m", "nas_dst_smoke"]\n')
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as bundle:
        for name, content in {"Dockerfile": dockerfile, "nas_dst_smoke.py": payload["source"]}.items():
            data = content.encode()
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), 0o644
            bundle.addfile(info, io.BytesIO(data))
    build = docker("build", "--network", "none", "--pull=false", "-t", payload["candidate"], "-",
                   input=archive.getvalue(), timeout=180)
    require(build.returncode == 0, "image_build_failed")
    candidate = inspect("image", payload["candidate"])
    summary["imageId"] = candidate["Id"]
    base_layers = base["RootFS"]["Layers"]
    require(candidate["RootFS"]["Layers"][:len(base_layers)] == base_layers, "base_layers_changed")
    require(candidate["Config"]["Cmd"] == ["python", "-m", "nas_dst_smoke"], "diagnostic_command_changed")
    require(candidate["Config"].get("Entrypoint") == base["Config"].get("Entrypoint"), "entrypoint_changed")
    require(candidate["Config"].get("Env") == base["Config"].get("Env"), "image_environment_changed")
    image_hash = docker("run", "--rm", "--network", "none", "--read-only", candidate["Id"],
                        "sha256sum", "/app/" + payload["sourcePath"])
    require(image_hash.returncode == 0 and image_hash.stdout.decode().split()[0] == payload["sourceSha256"],
            "packaged_source_mismatch")
    print(json.dumps({"phase": "image_verified", "imageId": candidate["Id"]}), flush=True)

    preflight = run_diagnostic("nas-dst-preflight-20260908-001", "none", {})
    summary["missingCredentialPreflight"] = preflight
    require(preflight["exitCode"] == 1 and preflight.get("ok") is False
            and preflight.get("missingEnvironment") == ["EASY_EMAIL_API_KEY", "EASY_PROXY_MANAGEMENT_PASSWORD"]
            and "steps" not in preflight, "missing_credential_gate_failed")
    expected_steps = {name: "ok" for name in ("acquire-proxy-chain", "acquire-mailbox", "recover-mailbox",
                      "release-recovered-mailbox", "release-mailbox", "release-proxy-chain")}
    for index in range(1, 4):
        result = run_diagnostic("nas-dst-diagnostic-20260908-001-" + str(index), "easyregister-pc2", environment)
        summary["runs"].append({"index": index, **result})
        print(json.dumps({"phase": "run_finished", "index": index, **result}), flush=True)
        require(result["exitCode"] == 0 and result.get("ok") is True and result.get("steps") == expected_steps
                and result.get("taskAttempts") == 1 and result.get("recoveryIdentityPreserved") is True
                and result.get("leasedProxyUsed") is True and result.get("cleanupErrors") == []
                and result.get("errorMarkers") == {}, "dependency_run_failed")
    summary["ok"] = True
except Exception as exc:
    summary["errorType"] = type(exc).__name__
    if isinstance(exc, RuntimeError) and re.fullmatch(r"[a-z_]+", str(exc)):
        summary["errorCode"] = str(exc)
finally:
    remaining = []
    for name in created:
        try:
            exists = docker("container", "inspect", name).returncode == 0
            if exists:
                docker("rm", "-f", name)
            if docker("container", "inspect", name).returncode == 0:
                remaining.append(name)
        except Exception:
            remaining.append(name)
    summary["remainingDiagnosticContainers"] = remaining
    try:
        summary["containersAfter"] = snapshot()
        summary["workersUnchanged"] = before is not None and before == summary["containersAfter"]
    except Exception:
        summary["workersUnchanged"] = False
    summary["ok"] = bool(summary["ok"] and not remaining and summary["workersUnchanged"])
    summary["capturedAt"] = datetime.now(timezone.utc).isoformat()
    print(json.dumps({"phase": "complete", **summary}), flush=True)
sys.exit(0 if summary["ok"] else 1)
'''


def main() -> int:
    compile(REMOTE, "pc2-dst-diagnostic-remote", "exec")
    source = (ROOT / SOURCE).read_text(encoding="utf-8")
    compile(source, SOURCE, "exec")
    payload = {
        "base": BASE, "baseId": BASE_ID, "candidate": CANDIDATE,
        "sourcePath": SOURCE, "source": source, "flowPath": FLOW,
        "sourceSha256": hashlib.sha256(source.encode()).hexdigest(),
        "flowSha256": hashlib.sha256((ROOT / FLOW).read_bytes()).hexdigest(),
    }
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.104", username="mjc",
                       key_filename=str(Path.home() / ".ssh" / "id_ed25519"),
                       look_for_keys=False, allow_agent=False, timeout=15)
        stdin, stdout, stderr = client.exec_command("python3 -c " + shlex.quote(REMOTE), timeout=1200)
        stdin.write(json.dumps(payload))
        stdin.flush()
        stdin.channel.shutdown_write()
        for line in stdout:
            print(line.rstrip(), flush=True)
        if stderr.read():
            print(json.dumps({"remoteStderrPresent": True}), flush=True)
        return stdout.channel.recv_exit_status()
    finally:
        client.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"ok": False, "errorType": type(exc).__name__}), flush=True)
        sys.exit(1)
