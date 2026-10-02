"""Stage the verified EasyRegister image on PC2 and run dependency diagnostics.

Does not start registration workers. Credentials are read from the environment,
never printed, and stored only in a mode-0600 runtime file on the target.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone

import paramiko


HOST = "192.168.15.104"
ROOT = "/home/mjc/easyregister"
IMAGE = "easy-register/easy-register:nas-dst-20260908-002"
IMAGE_ID = "sha256:b02b8f06b82727245d5d24ddcdb81c9a92d9f153a4f6dbc1fcee2ed6d80f9677"
REPO = Path(__file__).resolve().parents[1]


def main() -> int:
    credentials = {
        key: os.environ[key]
        for key in ("EASY_EMAIL_API_KEY", "EASY_PROXY_MANAGEMENT_PASSWORD")
    }
    if any(not value.strip() or "\n" in value or "\r" in value for value in credentials.values()):
        raise ValueError("Missing or multiline credential")
    archive = REPO / "deploy-evidence/easyregister-nas-dst-20260908-002.tar"
    digest = hashlib.sha256()
    with archive.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(HOST, username="mjc", key_filename=str(Path.home() / ".ssh/id_ed25519"),
                   look_for_keys=False, allow_agent=False, timeout=15)

    def run(command: str, timeout: int = 120) -> str:
        _, stdout, stderr = client.exec_command(command, timeout=timeout)
        result = stdout.read().decode("utf-8", errors="replace")
        stderr.read()
        if stdout.channel.recv_exit_status() != 0:
            # Docker diagnostics may contain interpolated secrets: don't echo them.
            raise RuntimeError("Remote operation failed; inspect privately on PC2")
        return result

    def put_text(sftp: paramiko.SFTPClient, name: str, contents: str) -> None:
        target = f"{ROOT}/{name}"
        temporary = target + ".new"
        with sftp.open(temporary, "w") as output:
            output.write(contents.encode("utf-8"))
        sftp.chmod(temporary, 0o600)
        sftp.posix_rename(temporary, target)

    try:
        before = json.loads(run("docker ps -aq | xargs -r docker inspect") or "[]")
        if any(item["Name"] == "/easy-register" for item in before):
            raise RuntimeError("Existing easy-register container requires explicit reconciliation")
        run(f"umask 077; mkdir -p {ROOT}/images {ROOT}/output {ROOT}/team-auth; chmod 700 {ROOT}")
        # Refuse to overwrite an earlier runtime configuration.
        run(f"test ! -e {ROOT}/runtime.env")
        # Stream directly into Docker. PC2 has severe local read I/O pressure;
        # rereading the staged archive timed out. SSH protects transport integrity,
        # Docker verifies its content and the exact loaded image ID is checked.
        print("Streaming image directly into PC2 Docker", flush=True)
        with archive.open("rb") as source:
            loaded = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                 "-o", "ConnectTimeout=15", "-i", str(Path.home() / ".ssh/id_ed25519"),
                 f"mjc@{HOST}", "docker load"],
                stdin=source, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=1800,
            )
        if loaded.returncode:
            raise RuntimeError("Docker image stream load failed")
        actual_id = run(f"docker image inspect {IMAGE} --format '{{{{.Id}}}}'").strip()
        if actual_id != IMAGE_ID:
            raise RuntimeError("Loaded image identity mismatch")
        print("Loaded image identity verified", flush=True)
        with client.open_sftp() as sftp:
            config = {
                "REGISTER_SERVICE_IMAGE": IMAGE,
                "REGISTER_OUTPUT_DIR_HOST": ROOT + "/output",
                "REGISTER_TEAM_AUTH_DIR_HOST": ROOT + "/team-auth",
                "REGISTER_DOCKER_NETWORK_NAME": "easyregister-pc2",
                "REGISTER_DOCKER_NETWORK_EXTERNAL": "false",
                "REGISTER_DASHBOARD_PORT_HOST": "127.0.0.1:19790",
                "MAILBOX_SERVICE_BASE_URL": "http://192.168.15.200:18081",
                "MAILBOX_SERVICE_API_KEY": credentials["EASY_EMAIL_API_KEY"],
                "EASY_PROXY_BASE_URL": "http://192.168.15.201:29888",
                "EASY_PROXY_MANAGEMENT_USERNAME": "easyproxy",
                "EASY_PROXY_MANAGEMENT_PASSWORD": credentials["EASY_PROXY_MANAGEMENT_PASSWORD"],
                "EASY_PROTOCOL_BASE_URL": "http://easy-protocol:9788",
                "EASY_PROTOCOL_CONTROL_TOKEN": "",
                "SMS_SERVICE_BASE_URL": "http://easy-sms:8080",
                "SMS_SERVICE_API_KEY": "",
                "REGISTER_SMS_ALLOW_PAID": "false",
                "REGISTER_WORKER_COUNT": "1",
                "REGISTER_MAIN_CONCURRENCY_LIMIT": "1",
            }
            # Single-quoted Compose env values are literal, including dollar signs.
            def quote(value: str) -> str:
                return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
            put_text(sftp, "runtime.env", "".join(f"{key}={quote(value)}\n" for key, value in config.items()))
            put_text(sftp, "compose.yaml", (REPO / "compose/docker-compose.yaml").read_text(encoding="utf-8-sig"))
            put_text(sftp, "staged.yaml", 'services:\n  easy-register:\n    restart: "no"\n    labels:\n      easyregister.deployment-state: "staged-missing-protocol-and-sms"\n')
            # Docker --env-file differs from Compose: no value quoting/interpolation.
            put_text(sftp, "diagnostic.env", "".join(f"{key}={value}\n" for key, value in credentials.items()))
        try:
            diagnostic = json.loads(run(
                f"docker run --rm --name easyregister-pc2-dst-check --read-only --tmpfs /tmp "
                f"--cap-drop ALL --security-opt no-new-privileges --env-file {ROOT}/diagnostic.env "
                f"{IMAGE} python -m nas_dst_smoke", timeout=600))
        finally:
            # Only the disposable credential file created above is removed.
            run(f"rm -f {ROOT}/diagnostic.env")
        if not diagnostic.get("ok") or diagnostic.get("cleanupErrors"):
            raise RuntimeError("PC2 DST dependency diagnostic did not pass")
        compose = f"docker compose --project-name easy-register --env-file {ROOT}/runtime.env -f {ROOT}/compose.yaml -f {ROOT}/staged.yaml"
        run(compose + " config --quiet")
        run(compose + " create --no-build easy-register")
        container = json.loads(run("docker inspect easy-register"))[0]
        if container["Image"] != IMAGE_ID or container["State"]["Status"] != "created":
            raise RuntimeError("Unexpected staged container state")
        after = json.loads(run("docker ps -aq | xargs -r docker inspect") or "[]")
        after_by_id = {item["Id"]: item for item in after}
        preserved = all(item["Id"] in after_by_id and item["State"]["StartedAt"] == after_by_id[item["Id"]]["State"]["StartedAt"] for item in before)
        evidence = {
            "recordedAtUtc": datetime.now(timezone.utc).isoformat(),
            "host": HOST, "hostname": run("hostname").strip(), "root": ROOT,
            "image": IMAGE, "imageId": IMAGE_ID, "sourceArchiveSha256": digest.hexdigest(),
            "transport": "binary SSH stdin into docker load; exact image ID verified",
            "remoteArchiveHashVerified": False,
            "dstDiagnostic": diagnostic, "containerState": container["State"]["Status"],
            "productionSchedulerStarted": False,
            "unrelatedContainersPreserved": preserved,
            "unrelatedContainerCount": len(before),
            "blockers": ["EasyProtocol endpoint and control authentication unconfigured", "EasySMS endpoint and API authentication unconfigured"],
        }
        payload = json.dumps(evidence, indent=2, ensure_ascii=True) + "\n"
        (REPO / "deploy-evidence/pc2-staged-20260908.json").write_text(payload, encoding="utf-8")
        with client.open_sftp() as sftp:
            put_text(sftp, "deployment-evidence.json", payload)
        print(payload)
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"Deployment stopped: {type(error).__name__}; credentials not logged", file=sys.stderr)
        sys.exit(1)
