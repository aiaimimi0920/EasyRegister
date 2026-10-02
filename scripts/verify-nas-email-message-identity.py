"""Verify two self-owned mail fixtures retain separate observed records.

Read the NAS SSH/sudo password from stdin. The test creates three diagnostic
mailboxes, sends two synthetic messages, and releases its mailboxes in finally.
It never registers an external account or requests a real verification code.
"""

from __future__ import annotations

import json
import shlex
import sys

import paramiko


VERIFY = r"""
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { randomUUID, createHash } from 'node:crypto';
import yaml from 'yaml';
const config = yaml.parse(fs.readFileSync('/etc/easy-email/config.yaml', 'utf8'));
const opened = [], released = new Set(), markers = [];
const result = { scope: 'two self-owned synthetic messages', ok: false, cleanupErrors: [] };
let stage = 'baseline';
async function request(path, body) {
  const response = await fetch('http://127.0.0.1:8080' + path, {
    method: body ? 'POST' : 'GET',
    headers: { Authorization: 'Bearer ' + config.server.apiKey, 'Content-Type': 'application/json' },
    ...(body ? { body: JSON.stringify(body) } : {}), signal: AbortSignal.timeout(20000),
  });
  if (response.status !== 200) {
    const body = await response.text();
    throw Object.assign(new Error('Service HTTP failure'), {
      httpStatus: response.status, upstreamStatus: /with status (\d{3})\b/.exec(body)?.[1],
    });
  }
  return await response.json();
}
const fingerprint = message => createHash('sha256').update(JSON.stringify(message)).digest('hex');
async function open(requestedDomain) {
  const { result: value } = await request('/mail/mailboxes/open', {
    hostId: 'message-identity-diagnostic-20260911', providerTypeKey: 'cloudflare_temp_email',
    provisionMode: 'reuse-only', bindingMode: 'shared-instance', ttlMinutes: 10,
    ...(requestedDomain ? { requestedDomain } : {}),
    metadata: { source: 'self-owned-message-identity-fixture' },
  });
  assert.ok(value?.session?.id);
  opened.push(value.session);
  if (requestedDomain) assert.equal(value.session.emailAddress.split('@')[1], requestedDomain);
  return value.session;
}
async function release(session) {
  if (released.has(session.id)) return;
  const { result: value } = await request('/mail/mailboxes/release', {
    sessionId: session.id, reason: 'self-owned-identity-fixture-complete',
  });
  assert.equal(value?.released, true);
  assert.ok(['resolved', 'expired'].includes(value.session.status));
  released.add(session.id);
}
async function readFixture(session, expected) {
  for (let attempt = 0; attempt < 20; attempt++) {
    const { code } = await request(`/mail/mailboxes/${encodeURIComponent(session.id)}/code`);
    if (code?.code === expected) return code;
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  throw new Error('Synthetic fixture delivery timed out');
}
try {
  const baseline = (await request('/mail/query/observed-messages?sync=false')).messages;
  const baselineHashes = new Map(baseline.map(m => [m.id, fingerprint(m)]));
  stage = 'open-sender';
  const sender = await open('tx-mail.aiaimimi.com');
  const codes = [];
  for (const [index, expected] of ['123456', '654321'].entries()) {
    stage = `fixture-${index + 1}-open`;
    const recipient = await open();
    const marker = 'owned-identity-fixture-' + randomUUID();
    markers.push({ session: recipient, marker, expected });
    stage = `fixture-${index + 1}-send`;
    await request('/mail/mailboxes/send', {
      sessionId: sender.id, toEmailAddress: recipient.emailAddress,
      subject: `Synthetic verification fixture ${expected}`,
      textBody: `Self-owned test. Verification code: ${expected}. Marker: ${marker}`,
      fromName: 'Diagnostic',
    });
    stage = `fixture-${index + 1}-receive`;
    codes.push(await readFixture(recipient, expected));
    stage = `fixture-${index + 1}-release`;
    await release(recipient);
  }
  stage = 'stored-record-verification';
  const after = (await request('/mail/query/observed-messages?sync=false')).messages;
  assert.notEqual(codes[0].observedMessageId, codes[1].observedMessageId);
  for (const { session, marker, expected } of markers) {
    const records = after.filter(m => m.sessionId === session.id);
    assert.equal(records.length, 1);
    assert.equal(records[0].extractedCode, expected);
    assert.ok(`${records[0].textBody ?? ''}${records[0].htmlBody ?? ''}`.includes(marker));
  }
  const afterById = new Map(after.map(m => [m.id, fingerprint(m)]));
  for (const [id, hash] of baselineHashes) assert.equal(afterById.get(id), hash);
  Object.assign(result, {
    ok: true, receivedFixtures: 2, observedFixtureRecords: 2, distinctMessageIds: true,
    reusedUpstreamNumericId: codes[0].observedMessageId.split(':').at(-1)
      === codes[1].observedMessageId.split(':').at(-1),
    preservedExistingRecords: baseline.length,
  });
} catch (error) {
  Object.assign(result, { ok: false, failedStage: stage,
    errorType: error instanceof Error ? error.name : 'unknown',
    httpStatus: error?.httpStatus, upstreamStatus: error?.upstreamStatus });
} finally {
  for (const session of opened) {
    try { await release(session); }
    catch { result.cleanupErrors.push('diagnostic-mailbox-release-failed'); }
  }
  result.releasedSessions = released.size;
  result.createdSessions = opened.length;
  result.capturedAt = new Date().toISOString();
  result.ok = result.ok && result.cleanupErrors.length === 0 && released.size === opened.length;
  console.log(JSON.stringify(result));
  process.exitCode = result.ok ? 0 : 1;
}
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
        command = "/usr/local/bin/docker exec easyemail-sdk node --input-type=module -e " + shlex.quote(VERIFY)
        stdin, stdout, stderr = client.exec_command("sudo -S -p '' " + command, timeout=240)
        stdin.write(password + "\n")
        stdin.flush()
        stdin.channel.shutdown_write()
        output = stdout.read().decode("utf-8")
        errors = stderr.read()
        status = stdout.channel.recv_exit_status()
        if output.strip():
            print(json.dumps(json.loads(output), indent=2))
        else:
            print(json.dumps({"ok": False, "remoteExit": status, "stderrPresent": bool(errors)}))
        return status
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
