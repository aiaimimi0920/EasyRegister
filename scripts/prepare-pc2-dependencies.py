"""Copy controlled dependency configuration to PC2 without starting workloads."""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from pathlib import Path
import secrets

REPO = Path(__file__).resolve().parents[1]
ROOT = "/home/mjc/easyregister"


def configure_external_python_provider(gateway: dict) -> None:
    provider = copy.deepcopy(gateway["services"][0])
    if provider.get("name") != "PythonProtocol-001":
        raise RuntimeError("Unexpected source provider registry")
    provider.update(endpoint="http://easy-protocol-python-001:9100", enabled=True)
    gateway["services"] = [provider]
    gateway["managed_provider_runtime"] = {"enabled": False}
    # Disabling Docker management otherwise falls back to bundled processes.
    # YAML null clears Go's default provider map; an empty map does not.
    gateway["provider_pool"] = {"providers": None}


def main() -> None:
    import paramiko
    import yaml

    gateway = yaml.safe_load(
        (REPO.parent / "EasyProtocol/deploy/service/base/config/config.yaml").read_text(encoding="utf-8-sig")
    )
    sms = yaml.safe_load(
        (REPO.parent / "EasySms/deploy/service/base/config/config.yaml").read_text(encoding="utf-8-sig")
    )
    control_token = secrets.token_urlsafe(32)
    sms_token = secrets.token_urlsafe(32)
    gateway["unified_api"] = {"listen": "0.0.0.0:9788", "password": secrets.token_urlsafe(32)}
    gateway["control_plane"].update(
        enabled=True, read_token=control_token, mutate_token=control_token,
        require_actor=True, localhost_only=False, allowlist=[],
    )
    configure_external_python_provider(gateway)
    gateway["strategy"]["max_fallback_attempts"] = 1
    for policy in gateway["strategy"].get("operation_policies", {}).values():
        policy["max_fallback_attempts"] = 1
    sms["server"].update(host="0.0.0.0", port=8080, apiKey=sms_token)
    sms["persistence"].update(enabled=True, filePath="/var/lib/easy-sms/state/easy-sms-state.json")
    sms["maintenance"].update(enabled=False, activeProbeEnabled=False)

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect("192.168.15.104", username="mjc", key_filename=str(Path.home() / ".ssh/id_ed25519"),
                   look_for_keys=False, allow_agent=False, timeout=15)
    try:
        command = (
            f"umask 077; mkdir -p {ROOT}/dependencies/protocol/config {ROOT}/dependencies/protocol/data "
            f"{ROOT}/dependencies/sms/config {ROOT}/dependencies/sms/data"
        )
        # Check before creating directories or writing runtime credentials.
        _, output, error = client.exec_command(f"test ! -e {ROOT}/dependencies/protocol/config/config.yaml && test ! -e {ROOT}/dependencies/sms/config/config.yaml")
        output.read()
        error.read()
        if output.channel.recv_exit_status():
            raise RuntimeError("Existing dependency configuration requires reconciliation")
        _, output, error = client.exec_command(command)
        output.read()
        error.read()
        if output.channel.recv_exit_status():
            raise RuntimeError("Cannot prepare dependency directories")
        with client.open_sftp() as sftp:
            def put(name: str, text: str) -> None:
                target = f"{ROOT}/{name}"
                with sftp.open(target + ".new", "w") as destination:
                    destination.write(text.encode("utf-8"))
                sftp.chmod(target + ".new", 0o600)
                sftp.posix_rename(target + ".new", target)

            with sftp.open(f"{ROOT}/runtime.env") as source:
                original = source.read().decode("utf-8")
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            put(f"runtime.env.before-dependencies-{timestamp}", original)
            updates = {
                "EASY_PROTOCOL_BASE_URL": "http://easy-protocol:9788",
                "EASY_PROTOCOL_CONTROL_TOKEN": control_token,
                "EASY_PROTOCOL_CONTROL_ACTOR": "easyregister-pc2",
                "SMS_SERVICE_BASE_URL": "http://easy-sms:8080",
                "SMS_SERVICE_API_KEY": sms_token,
                "REGISTER_PROTOCOL_BRIDGE_DIR": "/shared/register-output/easyregister-bridge",
                "REGISTER_PROTOCOL_BRIDGE_TARGET_DIR": "/shared/register-output/easyregister-bridge",
                "REGISTER_PROTOCOL_OUTPUT_TARGET_DIR": "/shared/register-output",
                "REGISTER_SMS_ALLOW_PAID": "false",
            }
            lines = []
            for line in original.splitlines():
                key = line.split("=", 1)[0]
                if key in updates:
                    lines.append(f"{key}='{updates.pop(key)}'")
                else:
                    lines.append(line)
            lines.extend(f"{key}='{value}'" for key, value in updates.items())
            put("dependencies/protocol/config/config.yaml", yaml.safe_dump(gateway, sort_keys=False))
            put("dependencies/sms/config/config.yaml", yaml.safe_dump(sms, sort_keys=False))
            put("runtime.env", "\n".join(lines) + "\n")
            put("dependencies.yaml", (REPO / "compose/docker-compose.pc2-dependencies.yaml").read_text(encoding="utf-8"))
        print("Dependency configuration staged; runtime backup retained; no workloads started")
    finally:
        client.close()


if __name__ == "__main__":
    main()
