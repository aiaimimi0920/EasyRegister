"""Verify a one-function OTP response diagnostic backport on the current PC2 base."""

from __future__ import annotations

import importlib.util
import json
import shlex
from pathlib import Path

import paramiko


ROOT = Path(__file__).resolve().parents[1]

PATCH_REMOTE = r'''
def patch_source(text, entry):
    parsed=ast.parse(text)
    nodes={node.name:node for node in parsed.body if isinstance(node,ast.FunctionDef)}
    node=nodes['_resend_passwordless_email_otp']
    original=ast.get_source_segment(text,node)
    require(hashlib.sha256(original.encode()).hexdigest()==entry['functionBeforeHash'],'resend_function_changed')
    lines=text.splitlines(keepends=True)
    lines[node.lineno-1:node.end_lineno]=[entry['source']+'\n']
    result=''.join(lines)
    before=[ast.dump(n) for n in parsed.body if n is not node]
    after=[ast.dump(n) for n in ast.parse(result).body
           if not (isinstance(n,ast.FunctionDef) and n.name==node.name)]
    require(before==after,'unrelated_source_changed')
    return result
'''


def main() -> int:
    spec = importlib.util.spec_from_file_location("resend_builder", ROOT / "scripts/verify-pc2-otp-resend.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("builder_module_missing")
    resend = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(resend)
    builder = resend.load_builder()
    previous = json.loads((ROOT / "deploy-evidence/pc2-otp-resend-candidates-20260911-001.json").read_text(encoding="utf-8"))
    flow_path = ROOT.parent / "EasyProtocol/providers/python/src/new_protocol_register/protocol_small_success.py"
    flow = flow_path.read_text(encoding="utf-8")
    target = "/app/src/new_protocol_register/protocol_small_success.py"
    payload = {
        "providerProbe": builder.PROVIDER_PROBE,
        "registerProbe": builder.REGISTER_PROBE,
        "otpProbe": resend.test_probe(ROOT.parent / "EasyProtocol/tests/test_otp_resend_recovery.py", "otp_resend_tests"),
        "retryProbe": resend.test_probe(ROOT / "tests/test_protocol_error_boundary.py", "retry_boundary_tests"),
        "specs": {},
    }
    for key, container, repository in (
        ("provider", "easy-register-protocol-python", "easy-protocol-python"),
        ("register", "easy-register", "easy-register"),
    ):
        base = previous["images"][key]
        files = []
        if key == "provider":
            files.append({
                "path": target, "patchKind": "resend-response",
                "beforeHash": base["files"][target],
                "functionBeforeHash": "b66090a3bb31bb65064b2bb381c8de5e0158d0c7d0c478aebcf45053fc97c997",
                "source": resend.function_source(flow, "_resend_passwordless_email_otp"),
            })
        payload["specs"][key] = {
            "container": container, "base": base["tag"], "baseId": base["id"],
            "candidate": f"easy-register/{repository}:pc2-otp-resend-20260911-002",
            "preservedFiles": base["files"], "files": files,
        }
    resend.PATCH_REMOTE = PATCH_REMOTE
    remote = resend.remote_program(builder)
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.104", username="mjc", key_filename=str(Path.home() / ".ssh/id_ed25519"),
                       look_for_keys=False, allow_agent=False, timeout=15)
        stdin, stdout, stderr = client.exec_command("python3 -c " + shlex.quote(remote), timeout=1200)
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
    raise SystemExit(main())
