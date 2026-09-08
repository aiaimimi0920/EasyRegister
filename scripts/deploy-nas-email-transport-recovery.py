"""Deploy the bounded Cloudflare transport fix over the exact NAS release.

The SSH/sudo password is read from stdin and is never persisted or printed.
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
BASE = "easyemail/easy-email-service:nas-http-diagnostics-20260908-002"
BASE_ID = "sha256:47b0982593811d9ce384b0dcb68bddc34213276f428222a9f2230d9039abbc72"
CANDIDATE = "easyemail/easy-email-service:nas-transport-recovery-20260908-001"

SMOKE = """
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {once} from 'node:events';
import {requestJson} from './dist/src/providers/cloudflare_temp_email/connector/http.js';
import {fetchProviderResourceWithRecovery} from './dist/src/providers/http-policy.js';
await import('./dist/src/providers/cloudflare_temp_email/connector/client.js');
const nativeFetch=globalThis.fetch;
const config={baseUrl:'https://fixture.invalid',timeoutSeconds:3};
const failure=code=>new TypeError('fetch failed',{cause:Object.assign(new Error('fixture'),{code})});
let calls=0; let originalSignal;
globalThis.fetch=async (url,init)=>{
  calls++;
  assert.equal(init.redirect,'error');
  if(calls===1){originalSignal=init.signal;throw failure('UND_ERR_CONNECT_TIMEOUT')}
  assert.equal(init.signal,originalSignal);
  assert.equal(init.body,JSON.stringify({fixture:true}));
  return new Response(JSON.stringify({fixture:true}));
};
const recovered=await requestJson(config,'POST','/api/new_address',{jsonBody:{fixture:true}});
assert.equal(recovered.status,200); assert.equal(recovered.body.fixture,true); assert.equal(calls,2);
calls=0;
globalThis.fetch=async ()=>{calls++;throw failure('UND_ERR_SOCKET')};
await assert.rejects(()=>requestJson(config,'POST','/api/new_address',{jsonBody:{fixture:true}}));
assert.equal(calls,1);
globalThis.fetch=nativeFetch;
for(const method of ['GET','POST']){
  let received=0;
  const server=createServer((request,response)=>{
    request.resume();
    request.on('end',()=>{received++;if(received===1)request.socket.destroy();else response.end('recovered')});
  });
  server.listen(0,'127.0.0.1'); await once(server,'listening');
  try{
    const pending=fetchProviderResourceWithRecovery('http://127.0.0.1:'+server.address().port+'/',{
      method,signal:AbortSignal.timeout(5000),...(method==='POST'?{body:'fixture'}:{})});
    if(method==='GET'){assert.equal(await (await pending).text(),'recovered');assert.equal(received,2)}
    else {await assert.rejects(()=>pending);assert.equal(received,1)}
  }finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve))}
}
console.log('isolated transport recovery smoke passed');
"""

HEALTH = """
const fs=require('fs'),yaml=require('yaml');
(async()=>{const c=yaml.parse(fs.readFileSync('/etc/easy-email/config.yaml','utf8'));
for(let attempt=0;attempt<10;attempt++){
  try{const r=await fetch('http://127.0.0.1:8080/mail/catalog',{
    headers:{Authorization:'Bearer '+c.server.apiKey},signal:AbortSignal.timeout(2000)});
    const j=await r.json();if(r.status===200&&j.catalog&&typeof j.catalog==='object'){
      console.log('authenticated catalog passed');return}}
  catch{}
  await new Promise(resolve=>setTimeout(resolve,500));
}process.exitCode=1})().catch(()=>{process.exitCode=1});
"""


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
        stderr.read()
        status = stdout.channel.recv_exit_status()
        if status:
            raise RuntimeError(f"Remote operation failed with exit {status}; output withheld")
        return output

    docker = "/usr/local/bin/docker "
    compose = (f"/usr/local/bin/docker-compose --env-file {ROOT}/deploy/service.env "
               f"-p easyemail-sdk -f {ROOT}/deploy/docker-compose.yaml ")
    try:
        current = json.loads(run(docker + "inspect easyemail-sdk"))[0]
        image = json.loads(run(docker + "image inspect " + BASE))[0]
        if current["Image"] != BASE_ID or image["Id"] != BASE_ID:
            raise RuntimeError("The deployment base changed")
        prefix = "/app/dist/src/providers/"
        old_policy = run(docker + "exec easyemail-sdk cat " + prefix + "http-policy.js")
        local = Path(__file__).resolve().parents[2] / "EasyEmail/service/base/dist/src/providers/http-policy.js"
        policy = local.read_text(encoding="utf-8")
        old_implementation = old_policy.split("//# sourceMappingURL=", 1)[0].rstrip()
        if not policy.startswith(old_implementation + "\n"):
            raise RuntimeError("The existing default transport must remain unchanged")
        files = {"http-policy.js": policy.encode()}
        old_diagnostic = run(docker + "exec easyemail-sdk cat /app/dist/src/http/failure-diagnostic.js")
        if hashlib.sha256(old_diagnostic.encode()).hexdigest() != "58398fcf77bf8ccab84c681c9c2c56362bc056d8ebbeb30617d01097a4ad9ebd":
            raise RuntimeError("Deployed diagnostics changed")
        diagnostic = local.parents[1] / "http/failure-diagnostic.js"
        files["failure-diagnostic.js"] = diagnostic.read_bytes()
        for short_name, path, count in (
            ("cloudflare-http.js", "cloudflare_temp_email/connector/http.js", 3),
            ("cloudflare-client.js", "cloudflare_temp_email/connector/client.js", 2),
        ):
            text = run(docker + "exec easyemail-sdk cat " + prefix + path)
            if "WithRecovery" in text or text.count("fetchProviderResource") != count:
                raise RuntimeError("Unexpected deployed Cloudflare transport references")
            files[short_name] = text.replace("fetchProviderResource", "fetchProviderResourceWithRecovery").encode()
        files["Dockerfile"] = (
            f"FROM {BASE}\nCOPY http-policy.js {prefix}\n"
            "COPY failure-diagnostic.js /app/dist/src/http/failure-diagnostic.js\n"
            f"COPY cloudflare-http.js {prefix}cloudflare_temp_email/connector/http.js\n"
            f"COPY cloudflare-client.js {prefix}cloudflare_temp_email/connector/client.js\n"
        ).encode()
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            for name, data in files.items():
                info = tarfile.TarInfo(name)
                info.size, info.mode = len(data), 0o644
                tar.addfile(info, io.BytesIO(data))
        run(docker + f"build -t {CANDIDATE} -", archive.getvalue())
        result = run(docker + f"run --rm --network none --entrypoint node {CANDIDATE} "
                     "--input-type=module -e " + shlex.quote(SMOKE))
        if "isolated transport recovery smoke passed" not in result:
            raise RuntimeError("Candidate smoke did not pass")
        print(result.strip(), flush=True)
        env_path = ROOT + "/deploy/service.env"
        backup = env_path + ".before-transport-recovery-20260908-001"
        staged = env_path + ".transport-recovery-staged"
        env = run("cat " + env_path)
        old = "EASY_EMAIL_SERVICE_IMAGE=" + BASE
        if env.splitlines().count(old) != 1:
            raise RuntimeError("Image pin changed")
        run("test ! -e " + backup)
        run("test ! -e " + staged)
        run("cp -p " + env_path + " " + backup)
        new_env = "\n".join("EASY_EMAIL_SERVICE_IMAGE=" + CANDIDATE if line == old else line
                            for line in env.splitlines()) + "\n"
        try:
            run("cp -p " + env_path + " " + staged)
            run("tee " + staged, new_env.encode())
            run("mv " + staged + " " + env_path)
            run(compose + "up -d --no-build --no-deps easy-email")
            state = json.loads(run(docker + "inspect easyemail-sdk"))[0]
            if not state["State"]["Running"] or state["Config"]["Image"] != CANDIDATE:
                raise RuntimeError("Candidate is not running")
            print(run(docker + "exec easyemail-sdk node -e " + shlex.quote(HEALTH)).strip(), flush=True)
        except Exception:
            run("cp -p " + backup + " " + env_path)
            run(compose + "up -d --no-build --no-deps easy-email")
            raise
        print(json.dumps({"image": state["Image"], "imageTag": CANDIDATE,
                          "artifacts": {name: hashlib.sha256(data).hexdigest() for name, data in files.items()
                                        if name.endswith(".js")}, "rollbackEnv": backup}))
    finally:
        client.close()


if __name__ == "__main__":
    main()
