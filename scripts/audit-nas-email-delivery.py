"""Read-only, redacted NAS mailbox identity and upstream-receipt audit.

Read the existing NAS SSH/sudo password from stdin. This does not refresh stored
mail, open/recover/release a mailbox, send mail, or start a registration task.
"""

from __future__ import annotations

import json
import shlex
import sys

import paramiko


AUDIT = r"""
import fs from 'node:fs';
import { createHash } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { loadEasyEmailServiceRuntimeConfigFromEnvironment } from './dist/src/runtime/config.js';
import { CloudflareTempEmailCreateClient, decodeCloudflareTempMailboxRef } from './dist/src/providers/cloudflare_temp_email/connector/client.js';
const config = await loadEasyEmailServiceRuntimeConfigFromEnvironment();
async function get(path) {
  const response = await fetch('http://127.0.0.1:8080' + path, {
    headers: { Authorization: 'Bearer ' + config.apiKey }, signal: AbortSignal.timeout(20000),
  });
  if (response.status !== 200) throw new Error('Authenticated audit request failed');
  return await response.json();
}
const { snapshot } = await get('/mail/snapshot');
const { sessions } = await get('/mail/query/mailbox-sessions?newestFirst=false');
const { messages } = await get('/mail/query/observed-messages?sync=false&newestFirst=false');
const bySession = new Map(sessions.map(s => [s.id, s]));
const byMessage = new Map(messages.map(m => [m.id, m]));
const collisions = [];
for (const session of sessions) {
  const message = byMessage.get(session.metadata?.lastCodeMessageId);
  if (session.status !== 'resolved' || !message || message.sessionId === session.id) continue;
  const owner = bySession.get(message.sessionId);
  collisions.push({
    sessionCreatedAt: session.createdAt,
    currentOwnerCreatedAt: owner?.createdAt,
    differentMailbox: owner ? owner.emailAddress !== session.emailAddress : null,
    sameProviderInstance: message.providerInstanceId === session.providerInstanceId,
    scopedMessageId: /^cloudflare_temp_email:[^:]+:[^:]+:/.test(message.id),
  });
}
const upstreamChecks = [];
for (const session of sessions.filter(s => s.status === 'open' && s.providerTypeKey === 'cloudflare_temp_email').slice(-2)) {
  const instance = snapshot.instances.find(i => i.id === session.providerInstanceId);
  const mailbox = decodeCloudflareTempMailboxRef(session.mailboxRef, session.providerInstanceId);
  const client = instance && CloudflareTempEmailCreateClient.fromInstance(instance);
  const result = {
    sessionHash: createHash('sha256').update(session.id).digest('hex').slice(0, 16),
    sessionCreatedAt: session.createdAt, sessionExpiresAt: session.expiresAt,
    sessionAgeSeconds: Math.floor((Date.now() - Date.parse(session.createdAt)) / 1000),
    storedMessageCount: messages.filter(m => m.sessionId === session.id).length,
    descriptorAddressMatchesSession: mailbox
      ? mailbox.address.toLowerCase() === session.emailAddress.toLowerCase() : false,
  };
  if (!mailbox || !client) result.descriptorAvailable = false;
  else {
    try {
      const mails = await client.listMails(mailbox.jwt);
      result.upstreamQueryOk = true;
      result.upstreamMessageCount = mails.length;
      result.upstreamMessageShapes = mails.map(mail => ({
        fields: Object.keys(mail).sort(),
        rawLength: typeof mail.raw === 'string' ? mail.raw.length : 0,
        envelopeMentionsOpenAI: /openai/i.test(String(mail.source ?? mail.from ?? '')),
        subjectMentionsOpenAI: /openai/i.test(String(mail.subject ?? '')),
      }));
    } catch (error) {
      result.upstreamQueryOk = false;
      result.errorType = error instanceof Error ? error.name : 'unknown';
      const status = /status (\d{3})\b/.exec(error instanceof Error ? error.message : '');
      if (status) result.upstreamStatus = Number(status[1]);
    }
  }
  upstreamChecks.push(result);
}
const persistence = config.persistence ?? {};
const fileStoreVerification = { checked: false };
if (persistence.enabled && persistence.driver === 'file') {
  try {
    const persisted = JSON.parse(fs.readFileSync(persistence.filePath, 'utf8'));
    if (!Array.isArray(persisted.messages)) throw new Error('Invalid file snapshot');
    const diskById = new Map(persisted.messages.map(message => [message.id, message]));
    Object.assign(fileStoreVerification, {
      checked: true, diskMessageCount: persisted.messages.length,
      observedMessagesMissingOrDifferentOnDisk: messages
        .filter(message => !isDeepStrictEqual(diskById.get(message.id), message)).length,
    });
  } catch (error) {
    fileStoreVerification.errorType = error instanceof Error ? error.name : 'unknown';
  }
}
console.log(JSON.stringify({
  capturedAt: new Date().toISOString(),
  sessionCount: sessions.length, messageCount: messages.length,
  resolvedSessionCount: sessions.filter(s => s.status === 'resolved').length,
  legacyMessageCount: messages.filter(m => /^cloudflare_temp_email:\d+$/.test(m.id)).length,
  scopedMessageCount: messages.filter(m => /^cloudflare_temp_email:[^:]+:[^:]+:/.test(m.id)).length,
  crossSessionReferences: collisions.length,
  crossMailboxReferences: collisions.filter(c => c.differentMailbox === true).length,
  scopedCrossSessionReferences: collisions.filter(c => c.scopedMessageId).length,
  collisionExamples: collisions.slice(-5), upstreamChecks,
  fileStoreVerification,
  persistence: Object.fromEntries(['enabled', 'driver', 'pythonCommand', 'sqliteHelperScriptPath']
    .filter(key => persistence[key] !== undefined).map(key => [key, persistence[key]])),
}));
"""


def main() -> int:
    password = sys.stdin.readline().lstrip("\ufeff").rstrip("\r\n")
    if not password:
        raise RuntimeError("A password is required on stdin")
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.200", username="mjc", password=password,
                       look_for_keys=False, allow_agent=False, timeout=15)
        command = "/usr/local/bin/docker exec easyemail-sdk node --input-type=module -e " + shlex.quote(AUDIT)
        stdin, stdout, stderr = client.exec_command("sudo -S -p '' " + command, timeout=100)
        stdin.write(password + "\n")
        stdin.flush()
        stdin.channel.shutdown_write()
        output = stdout.read().decode("utf-8")
        errors = stderr.read()
        status = stdout.channel.recv_exit_status()
        if status:
            print(json.dumps({"ok": False, "remoteExit": status, "stderrPresent": bool(errors)}))
            return 1
        print(json.dumps(json.loads(output), indent=2))
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
