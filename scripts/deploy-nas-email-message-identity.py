"""Deploy only the session-scoped Cloudflare message identity repair.

Read the NAS SSH/sudo password from stdin. Credentials never enter the image,
evidence, or a new host file. The existing environment backup stays protected.
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
BASE = "easyemail/easy-email-service:nas-transport-recovery-20260908-001"
BASE_ID = "sha256:0362e4a98c9e66afa925c0300582c62c5fc3200b73c68ac9037e977d7e2ee8c5"
BASE_CLIENT_SHA = "fde6b080c25901ef14dcec35e78d9f7684421cbf490e499d5d2af85bb6462527"
CANDIDATE = "easyemail/easy-email-service:nas-message-identity-20260911-002"
MODULE = "/app/dist/src/providers/cloudflare_temp_email/connector/client.js"
SMOKE_NAME = "easyemail-message-identity-smoke-20260911-002"

SMOKE = r"""
import assert from 'node:assert/strict';
import { CloudflareTempEmailCreateClient } from './dist/src/providers/cloudflare_temp_email/connector/client.js';
import { MailRegistry } from './dist/src/domain/registry.js';
import { createFileMailStateStore } from './dist/src/persistence/file-state-store.js';
const client = new CloudflareTempEmailCreateClient({
  baseUrl: 'https://fixture.invalid', domains: [], randomSubdomainDomains: [], timeoutSeconds: 5,
});
let cases = 0;
for (const location of ['subject', 'body']) {
  const registry = new MailRegistry();
  let code = '123456';
  globalThis.fetch = async input => {
    const path = new URL(input).pathname;
    if (path === '/api/mails') return Response.json({ results: [{
      id: 7, source: 'sender@fixture.example',
      subject: location === 'subject' ? `Verification code: ${code}` : 'Verification message',
    }] });
    assert.equal(path, '/api/mail/7');
    return Response.json({ raw: [
      'Date: Thu, 10 Sep 2026 12:00:00 +0000', 'Content-Type: text/plain; charset=utf-8', '',
      location === 'body' ? `Your verification code is ${code}.` : 'Fixture body',
    ].join('\r\n') });
  };
  const mailbox = { address: 'first@fixture.example', jwt: 'fixture-first' };
  const first = await client.tryReadLatestCode('session-a', mailbox, 'instance-a');
  assert.equal(first?.extractedCode, code);
  registry.saveMessage(first);
  const repeated = await client.tryReadLatestCode('session-a', mailbox, 'instance-a');
  assert.equal(repeated.id, first.id);
  registry.saveMessage(repeated);
  assert.equal(registry.listMessages().length, 1);
  code = '654321';
  const second = await client.tryReadLatestCode('session-b', mailbox, 'instance-a');
  const third = await client.tryReadLatestCode('session-a', mailbox, 'instance-b');
  registry.saveMessage(second);
  registry.saveMessage(third);
  assert.equal(registry.listMessages().length, 3);
  assert.deepEqual(registry.listMessagesBySession('session-b'), [second]);
  const store = createFileMailStateStore({ filePath: `/tmp/${location}.json` });
  await store.saveSnapshot(registry.snapshot());
  const restored = new MailRegistry(await store.loadSeed());
  const sorted = messages => messages.sort((a, b) => a.id.localeCompare(b.id));
  assert.deepEqual(sorted(restored.listMessages()), sorted(JSON.parse(JSON.stringify(registry.listMessages()))));
  assert.equal(restored.listMessagesBySession('session-b').length, 1);
  cases++;
}
console.log(JSON.stringify({ identityCases: cases, fileRoundTrips: cases, network: 'none', ok: true }));
"""

STATE = r"""
const fs = require('fs'), yaml = require('yaml'), crypto = require('crypto');
(async () => {
  const config = yaml.parse(fs.readFileSync('/etc/easy-email/config.yaml', 'utf8'));
  async function get(path) {
    const r = await fetch('http://127.0.0.1:8080' + path, {
      headers: { Authorization: 'Bearer ' + config.server.apiKey }, signal: AbortSignal.timeout(10000),
    });
    if (r.status !== 200) throw new Error('Authenticated state request failed');
    return await r.json();
  }
  let catalog;
  for (let attempt = 0; attempt < 10; attempt++) {
    try { catalog = await get('/mail/catalog'); break; }
    catch (error) {
      if (attempt === 9) throw error;
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }
  if (!catalog.catalog || typeof catalog.catalog !== 'object') throw new Error('Invalid catalog');
  const { messages } = await get('/mail/query/observed-messages?sync=false');
  console.log(JSON.stringify({ catalogOk: true, messages: Object.fromEntries(messages.map(m => [
    m.id, crypto.createHash('sha256').update(JSON.stringify(m)).digest('hex'),
  ])) }));
})().catch(() => { console.error('State check failed; details withheld'); process.exitCode = 1; });
"""


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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
        stdin, stdout, stderr = client.exec_command("sudo -S -p '' " + command, timeout=240)
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
        base_image = json.loads(run(docker + "image inspect " + BASE))[0]
        if current["Image"] != BASE_ID or base_image["Id"] != BASE_ID or not current["State"]["Running"]:
            raise RuntimeError("The running deployment base changed")
        if run(docker + "images -q " + CANDIDATE).strip():
            raise RuntimeError("Candidate tag already exists")
        if run(docker + "ps -aq --filter name=^/" + SMOKE_NAME + "$").strip():
            raise RuntimeError("Diagnostic container name already exists")
        original = run(docker + "exec easyemail-sdk cat " + MODULE)
        if sha(original.encode()) != BASE_CLIENT_SHA:
            raise RuntimeError("The deployed connector changed")
        method = "    async tryReadLatestCode(sessionId, mailbox, providerInstanceId, fromContains) {\n"
        replacements = (
            (method, method + "        // Upstream numeric IDs can be reused after a mailbox is deleted.\n"
             "        const messagePrefix = `cloudflare_temp_email:${encodeURIComponent(providerInstanceId)}:${encodeURIComponent(sessionId)}`;\n"),
            ('id: `cloudflare_temp_email:${String(mail.id ?? `${sessionId}:subject`)}`',
             'id: `${messagePrefix}:${String(mail.id ?? "subject")}`'),
            ('id: `cloudflare_temp_email:${mailId}`', 'id: `${messagePrefix}:${mailId}`'),
        )
        patched = original
        for old, new in replacements:
            if patched.count(old) != 1:
                raise RuntimeError("Unexpected compiled message-identity patch location")
            patched = patched.replace(old, new)
        local = Path(__file__).resolve().parents[2] / "EasyEmail/service/base/dist/src/providers/cloudflare_temp_email/connector/client.js"
        tested = local.read_text(encoding="utf-8")
        if patched.split(method, 1)[1] != tested.split(method, 1)[1]:
            raise RuntimeError("Candidate method differs from the locally tested build")
        files = {"client.js": patched.encode(), "Dockerfile": f"FROM {BASE}\nCOPY client.js {MODULE}\n".encode()}
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            for name, data in files.items():
                info = tarfile.TarInfo(name)
                info.size, info.mode = len(data), 0o644
                tar.addfile(info, io.BytesIO(data))
        run(docker + f"build -t {CANDIDATE} -", archive.getvalue())
        candidate = json.loads(run(docker + "image inspect " + CANDIDATE))[0]
        if candidate["RootFS"]["Layers"][:-1] != base_image["RootFS"]["Layers"]:
            raise RuntimeError("Candidate does not extend the exact base by one layer")
        try:
            smoke = json.loads(run(
                docker + f"run --rm --name {SMOKE_NAME} --network none --read-only "
                "--tmpfs /tmp:rw,nosuid,size=32m --cap-drop ALL --security-opt no-new-privileges=true "
                f"--entrypoint node {CANDIDATE} --input-type=module -e " + shlex.quote(SMOKE)))
        finally:
            if run(docker + "ps -aq --filter name=^/" + SMOKE_NAME + "$").strip():
                run(docker + "rm -f " + SMOKE_NAME)
        if smoke.get("ok") is not True or smoke.get("fileRoundTrips") != 2:
            raise RuntimeError("Candidate persistence smoke failed")
        print(json.dumps({"candidateImageId": candidate["Id"], "smoke": smoke}), flush=True)
        env_path = ROOT + "/deploy/service.env"
        backup = env_path + ".before-message-identity-20260911-002"
        staged = env_path + ".message-identity-staged"
        env = run("cat " + env_path)
        old_pin = "EASY_EMAIL_SERVICE_IMAGE=" + BASE
        if env.splitlines().count(old_pin) != 1:
            raise RuntimeError("The image pin changed")
        before = json.loads(run(docker + "exec easyemail-sdk node -e " + shlex.quote(STATE)))
        run("test ! -e " + backup)
        run("test ! -e " + staged)
        run("cp -p " + env_path + " " + backup)
        new_env = "\n".join("EASY_EMAIL_SERVICE_IMAGE=" + CANDIDATE if line == old_pin else line
                            for line in env.splitlines()) + "\n"
        try:
            run("cp -p " + env_path + " " + staged)
            run("tee " + staged, new_env.encode())
            run("mv " + staged + " " + env_path)
            run(compose + "up -d --no-build --no-deps easy-email")
            state = json.loads(run(docker + "inspect easyemail-sdk"))[0]
            if not state["State"]["Running"] or state["Image"] != candidate["Id"]:
                raise RuntimeError("The candidate is not running")
            live_sha = run(docker + "exec easyemail-sdk sha256sum " + MODULE).split()[0]
            if live_sha != sha(patched.encode()):
                raise RuntimeError("The running connector hash differs from the candidate")
            after = json.loads(run(docker + "exec easyemail-sdk node -e " + shlex.quote(STATE)))
            if any(after["messages"].get(key) != value for key, value in before["messages"].items()):
                raise RuntimeError("Existing message records changed during deployment")
        except Exception:
            run("cp -p " + backup + " " + env_path)
            run(compose + "up -d --no-build --no-deps easy-email")
            raise
        print(json.dumps({"imageTag": CANDIDATE, "imageId": state["Image"], "containerId": state["Id"],
                          "startedAt": state["State"]["StartedAt"], "connectorSha256": live_sha,
                          "preservedMessageRecords": len(before["messages"]), "catalogOk": after["catalogOk"],
                          "rollbackEnv": backup, "smoke": smoke}))
    finally:
        client.close()


if __name__ == "__main__":
    main()
