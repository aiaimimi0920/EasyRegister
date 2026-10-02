"""Verify self-owned mail identity through both deployed PC2 callers.

Create one sender and two recipients, send synthetic fixtures, then release all
three upstream mailboxes. No registration, real OTP, or SMS request is made.
Credentials and mailbox contents stay inside the existing caller containers and
the remote diagnostic process; only an allowlisted result leaves PC2.
"""

from __future__ import annotations

import json
import sys

import paramiko


REQUEST = r"""
import json, os, sys, urllib.error, urllib.request
request = json.load(sys.stdin)
base = (os.environ.get('MAILBOX_SERVICE_BASE_URL') or os.environ['EASY_EMAIL_BASE_URL']).rstrip('/')
key = os.environ.get('MAILBOX_SERVICE_API_KEY') or os.environ['EASY_EMAIL_API_KEY']
body = request.get('body')
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
http = urllib.request.Request(base + request['path'],
    data=json.dumps(body).encode() if body is not None else None,
    headers={'Authorization': 'Bearer ' + key, 'Content-Type': 'application/json'})
try:
    with opener.open(http, timeout=20) as response:
        print(json.dumps({'status': response.status, 'payload': json.load(response)}))
except urllib.error.HTTPError as error:
    print(json.dumps({'status': error.code}))
    raise SystemExit(1)
except Exception as error:
    print(json.dumps({'errorType': type(error).__name__}))
    raise SystemExit(1)
"""

VERIFY = r"""
import hashlib, json, subprocess, time, uuid
from datetime import datetime, timezone

callers = ['easy-register', 'easy-register-protocol-python']
opened, released, fixtures, caller_checks = [], set(), [], []
result = {'scope': 'self-owned mail identity through both PC2 callers', 'ok': False, 'cleanupErrors': []}
stage = 'baseline'

def api(container, path, body=None):
    child = subprocess.run(['docker', 'exec', '-i', container, 'python', '-c', REQUEST_SOURCE],
        input=json.dumps({'path': path, 'body': body}), capture_output=True, text=True, timeout=35)
    try:
        response = json.loads(child.stdout)
    except ValueError:
        raise RuntimeError('Caller response unavailable') from None
    if child.returncode or response.get('status') != 200:
        error = RuntimeError('Caller request failed')
        error.http_status = response.get('status')
        raise error
    return response['payload']

def containers():
    child = subprocess.run(['docker', 'inspect', *callers], capture_output=True, text=True, timeout=20)
    if child.returncode:
        raise RuntimeError('Caller inspection failed')
    return [{'name': item['Name'].lstrip('/'), 'id': item['Id'], 'imageId': item['Image'],
        'startedAt': item['State']['StartedAt'], 'running': item['State']['Running'],
        'restartCount': item['RestartCount']} for item in json.loads(child.stdout)]

def open_mailbox(domain=None):
    body = {'hostId': 'pc2-message-identity-diagnostic', 'providerTypeKey': 'cloudflare_temp_email',
        'provisionMode': 'reuse-only', 'bindingMode': 'shared-instance', 'ttlMinutes': 10,
        'metadata': {'source': 'self-owned-pc2-message-identity-fixture'}}
    if domain:
        body['requestedDomain'] = domain
    session = api(callers[0], '/mail/mailboxes/open', body)['result']['session']
    opened.append(session)
    if domain:
        assert session['emailAddress'].split('@')[1] == domain
    return session

def release(session):
    if session['id'] in released:
        return
    value = api(callers[0], '/mail/mailboxes/release', {
        'sessionId': session['id'], 'reason': 'self-owned-pc2-identity-fixture-complete'})['result']
    assert value['released'] is True
    assert value['session']['status'] in ('resolved', 'expired')
    released.add(session['id'])

def fingerprint(message):
    return hashlib.sha256(json.dumps(message, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

try:
    before = containers()
    assert all(item['running'] for item in before)
    baseline = api(callers[0], '/mail/query/observed-messages?sync=false')['messages']
    baseline_hashes = {item['id']: fingerprint(item) for item in baseline}
    stage = 'open-sender'
    sender = open_mailbox('tx-mail.aiaimimi.com')
    for index, expected in enumerate(('123456', '654321'), 1):
        stage = f'fixture-{index}-open'
        recipient = open_mailbox()
        marker = 'pc2-owned-identity-' + uuid.uuid4().hex
        stage = f'fixture-{index}-send'
        api(callers[0], '/mail/mailboxes/send', {'sessionId': sender['id'],
            'toEmailAddress': recipient['emailAddress'], 'fromName': 'Diagnostic',
            'subject': f'Synthetic verification fixture {expected}',
            'textBody': f'Self-owned test. Verification code: {expected}. Marker: {marker}'})
        codes = []
        for container in callers:
            stage = f'fixture-{index}-read-{container}'
            deadline = time.monotonic() + 40
            while True:
                code = api(container, '/mail/mailboxes/' + recipient['id'] + '/code').get('code') or {}
                if code.get('code') == expected:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError('Synthetic fixture delivery timed out')
                time.sleep(1)
            assert code['sessionId'] == recipient['id']
            assert code.get('observedMessageId')
            codes.append(code)
            caller_checks.append({'fixture': index, 'container': container, 'status': 200,
                'syntheticCodeMatched': True, 'sessionMatched': True})
        assert codes[0]['observedMessageId'] == codes[1]['observedMessageId']
        fixtures.append((recipient, marker, expected, codes[0]['observedMessageId']))
        stage = f'fixture-{index}-release'
        release(recipient)
    stage = 'stored-record-verification'
    after = api(callers[0], '/mail/query/observed-messages?sync=false')['messages']
    assert fixtures[0][3] != fixtures[1][3]
    for session, marker, expected, message_id in fixtures:
        records = [message for message in after if message['sessionId'] == session['id']]
        assert len(records) == 1 and records[0]['id'] == message_id
        assert records[0]['extractedCode'] == expected
        assert marker in (records[0].get('textBody') or '') + (records[0].get('htmlBody') or '')
    after_hashes = {item['id']: fingerprint(item) for item in after}
    assert all(after_hashes.get(key) == value for key, value in baseline_hashes.items())
    result.update(ok=True, receivedFixtures=2, observedFixtureRecords=2, distinctMessageIds=True,
        reusedUpstreamNumericId=fixtures[0][3].rsplit(':', 1)[-1] == fixtures[1][3].rsplit(':', 1)[-1],
        preservedExistingRecords=len(baseline), callerChecks=caller_checks)
except Exception as error:
    result.update(ok=False, failedStage=stage, errorType=type(error).__name__)
    if getattr(error, 'http_status', None) is not None:
        result['httpStatus'] = error.http_status
finally:
    for session in opened:
        try:
            release(session)
        except Exception:
            result['cleanupErrors'].append('diagnostic-mailbox-release-failed')
    try:
        result['callerContainers'] = containers()
        result['callerContainersUnchanged'] = result['callerContainers'] == before
    except Exception:
        result['callerContainersUnchanged'] = False
    result.update(createdSessions=len(opened), releasedSessions=len(released),
        capturedAt=datetime.now(timezone.utc).isoformat())
    result['ok'] = (result['ok'] and not result['cleanupErrors']
        and len(released) == len(opened) and result['callerContainersUnchanged'])
    print(json.dumps(result))
    raise SystemExit(0 if result['ok'] else 1)
"""


def main() -> int:
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.104", username="mjc",
                       key_filename="C:/Users/vmjcv/.ssh/id_ed25519",
                       look_for_keys=False, allow_agent=False, timeout=15)
        stdin, stdout, stderr = client.exec_command("python3 -", timeout=300)
        stdin.write("REQUEST_SOURCE = " + repr(REQUEST) + "\n" + VERIFY)
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
    sys.exit(main())
