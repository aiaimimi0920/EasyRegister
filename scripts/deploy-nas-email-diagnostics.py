"""Install only redacted HTTP diagnostics over the verified NAS image.

Read the SSH/sudo password from stdin. Never print or persist credentials.
The existing application, configuration and state are preserved.
"""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import shlex
import sys
import tarfile

import paramiko


ROOT = "/volume1/docker/easyemail-sdk"
BASE = "easyemail/easy-email-service:nas-http-diagnostics-20260908-001"
BASE_ID = "sha256:5f93ec9d4c441fcaf786b068a302e3aa56e5613f52b606220aedef2b50c906ef"
CANDIDATE = "easyemail/easy-email-service:nas-http-diagnostics-20260908-002"


def main() -> None:
    password = sys.stdin.readline().lstrip("\ufeff").rstrip("\r\n")
    if not password:
        raise RuntimeError("A password is required on stdin")
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect("192.168.15.200", username="mjc", password=password,
                   look_for_keys=False, allow_agent=False, timeout=15)

    def run(command: str, payload: bytes = b"") -> str:
        stdin, stdout, stderr = client.exec_command("sudo -S -p '' " + command)
        stdin.write(password + "\n")
        stdin.flush()
        if payload:
            stdin.write(payload)
            stdin.flush()
        stdin.channel.shutdown_write()
        output = stdout.read().decode("utf-8")
        errors = stderr.read().decode("utf-8")
        status = stdout.channel.recv_exit_status()
        if status:
            raise RuntimeError(f"Remote operation failed with exit {status}; output withheld")
        return output

    docker = "/usr/local/bin/docker "
    compose = (
        f"/usr/local/bin/docker-compose --env-file {ROOT}/deploy/service.env "
        f"-p easyemail-sdk -f {ROOT}/deploy/docker-compose.yaml "
    )
    try:
        current = json.loads(run(docker + "inspect easyemail-sdk"))[0]
        if current["Image"] != BASE_ID:
            raise RuntimeError("Current image changed; refusing to patch a different release")
        base_image = json.loads(run(docker + "image inspect " + BASE))[0]
        if base_image["Id"] != BASE_ID:
            raise RuntimeError("Base image tag changed")
        source = run(docker + "exec easyemail-sdk cat /app/dist/src/http/server.js")
        original_hash = hashlib.sha256(source.encode()).hexdigest()
        old_import = 'import { describeHttpFailure } from "./failure-diagnostic.js";'
        old_call = "console.error(JSON.stringify(describeHttpFailure(method, path, error)));"
        if source.count(old_import) != 1 or source.count(old_call) != 2:
            raise RuntimeError("Unexpected HTTP diagnostic server content")
        source = source.replace(old_import, 'import { reportHttpFailure } from "./failure-diagnostic.js";')
        source = source.replace(old_call, "reportHttpFailure(method, path, error);")
        helper = (Path(__file__).resolve().parents[2] / "EasyEmail/service/base/dist/src/http/failure-diagnostic.js").read_bytes()
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            for name, data in {
                "Dockerfile": (f"FROM {BASE}\nCOPY server.js failure-diagnostic.js /app/dist/src/http/\n").encode(),
                "server.js": source.encode(),
                "failure-diagnostic.js": helper,
            }.items():
                info = tarfile.TarInfo(name)
                info.size = len(data)
                info.mode = 0o644
                tar.addfile(info, io.BytesIO(data))
        run(docker + f"build -t {CANDIDATE} -", archive.getvalue())

        smoke = """
import assert from 'node:assert/strict';
import {createEasyEmailHttpServer} from './dist/src/http/server.js';
const logs=[]; console.error=(line)=>logs.push(line);
const server=await createEasyEmailHttpServer({openMailbox:async()=>{
  throw new TypeError('fetch failed',{cause:Object.assign(new Error('synthetic-private-value'),{code:'ECONNRESET'})});
}});
try {
  const response=await fetch(server.baseUrl+'/mail/mailboxes/open',{
    method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({hostId:'diagnostic-smoke'})});
  assert.equal(response.status,500);
  assert.equal((await response.json()).error,'fetch failed');
  assert.equal(logs.length,1);
  assert.ok(logs[0].includes('ECONNRESET'));
  assert.ok(!logs[0].includes('synthetic-private-value'));
  console.error=()=>{throw new Error('synthetic-closed-pipe')};
  const second=await fetch(server.baseUrl+'/mail/mailboxes/open',{
    method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({hostId:'diagnostic-smoke'})});
  assert.equal(second.status,500);
  await second.arrayBuffer();
  console.log('isolated HTTP failure diagnostic smoke passed');
} finally { await server.close(); }
"""
        result = run(docker + f"run --rm --network none --entrypoint node {CANDIDATE} "
                     "--input-type=module -e " + shlex.quote(smoke))
        if "diagnostic smoke passed" not in result:
            raise RuntimeError("Candidate smoke did not confirm success")
        print(result.strip(), flush=True)

        env_path = ROOT + "/deploy/service.env"
        backup = env_path + ".before-http-diagnostics-20260908-002"
        env = run("cat " + env_path)
        old = "EASY_EMAIL_SERVICE_IMAGE=" + BASE
        if env.splitlines().count(old) != 1:
            raise RuntimeError("Unexpected image pin in service.env")
        run("test ! -e " + backup)
        run("cp -p " + env_path + " " + backup)
        new_env = "\n".join("EASY_EMAIL_SERVICE_IMAGE=" + CANDIDATE if line == old else line
                            for line in env.splitlines()) + "\n"
        run("tee " + env_path, new_env.encode())
        try:
            run(compose + "up -d --no-build --no-deps easy-email")
            state = json.loads(run(docker + "inspect easyemail-sdk"))[0]
            if not state["State"]["Running"] or state["Config"]["Image"] != CANDIDATE:
                raise RuntimeError("Candidate container is not running")
        except Exception:
            run("cp -p " + backup + " " + env_path)
            run(compose + "up -d --no-build --no-deps easy-email")
            raise
        print(json.dumps({"image": state["Image"], "imageTag": CANDIDATE,
                          "baseServerSha256": original_hash,
                          "patchedServerSha256": hashlib.sha256(source.encode()).hexdigest(),
                          "helperSha256": hashlib.sha256(helper).hexdigest(),
                          "rollbackEnv": backup}))
    finally:
        client.close()


if __name__ == "__main__":
    main()
