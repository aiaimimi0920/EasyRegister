"""Read-only, redacted task and artifact audit after a PC2 production restart."""

from __future__ import annotations

import argparse
import json

import paramiko


ARTIFACTS = r"""
import collections, hashlib, json, re, sys
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, '/app/server/services/orchestration_service/src')
from others.common_runtime import validate_openai_oauth_seed_payload
# Docker emits nanoseconds; the deployed Python 3.10 accepts microseconds.
normalized_since = re.sub(r'(\.\d{6})\d+', r'\1', SINCE).replace('Z', '+00:00')
cutoff = datetime.fromisoformat(normalized_since).timestamp()
root = Path('/shared/register-output/openai/pending')
counts, reasons, fresh = collections.Counter(), collections.Counter(), []
for path in sorted(root.rglob('*.json')) if root.is_dir() else []:
    counts['fileCount'] += 1
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
        valid, reason = validate_openai_oauth_seed_payload(payload, enforce_max_age=False)
        current, _ = validate_openai_oauth_seed_payload(payload)
        counts['milestoneValidCount'] += int(valid)
        counts['currentAgeValidCount'] += int(current)
        created = datetime.fromisoformat(str(payload.get('createdAt', '')).replace('Z', '+00:00'))
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if path.stat().st_mtime >= cutoff and created.timestamp() >= cutoff:
            counts['newFileCount'] += 1
            counts['newMilestoneValidCount'] += int(valid)
            counts['newCurrentAgeValidCount'] += int(current)
            if valid:
                fresh.append({'sha256': hashlib.sha256(raw).hexdigest(),
                    'createdAt': payload['createdAt'], 'currentAgeValid': current})
            else:
                reasons[reason] += 1
    except Exception:
        counts['unreadableOrInvalidArtifact'] += 1
for key in ('fileCount', 'milestoneValidCount', 'currentAgeValidCount',
            'newFileCount', 'newMilestoneValidCount', 'newCurrentAgeValidCount'):
    counts.setdefault(key, 0)
print(json.dumps({**counts, 'newInvalidReasons': dict(reasons), 'newValidArtifacts': fresh}))
"""

OBSERVE = r"""
import collections, json, re, subprocess, urllib.request
from datetime import datetime, timezone
stage = 'container-inspection'
def run(args, source=None):
    child = subprocess.run(args, input=source, capture_output=True, text=True, timeout=45)
    if child.returncode:
        raise RuntimeError('Observation command failed')
    return child.stdout
def safe_label(value):
    return value if isinstance(value,str) and re.fullmatch(r'[a-z_]{1,96}',value) else None
try:
    names = ['easy-register', 'easy-register-protocol-python', 'easy-register-protocol', 'easy-register-sms']
    containers = json.loads(run(['docker', 'inspect', *names]))
    stage = 'task-log-read'
    logs = run(['docker', 'logs', '--timestamps', '--since', SINCE, 'easy-register'])
    started, finished = {}, {}
    events = collections.Counter()
    for line in logs.splitlines():
        offset = line.find('{')
        if offset < 0:
            continue
        try:
            record = json.loads(line[offset:])
        except ValueError:
            continue
        event = record.get('event')
        if event:
            events[event] += 1
        if event == 'register_run_started':
            started[record['taskIndex']] = record['startedAt']
        elif event == 'register_run_finished':
            value = record.get('result') or {}
            steps, step = value.get('steps') or {}, value.get('errorStep')
            error = (value.get('stepErrors') or {}).get(step) or {}
            metadata = error if isinstance(error,dict) else {}
            message = str(metadata.get('message') or '').lower()
            finished[record['taskIndex']] = {
                'taskIndex': record['taskIndex'], 'startedAt': record.get('startedAt'),
                'finishedAt': record.get('finishedAt'), 'completeSuccess': record.get('ok') is True,
                'smallSuccess': all(steps.get(key) == 'ok' for key in ('create-openai-account',
                    'initialize-platform-organization', 'initialize-chatgpt-login-session')),
                'terminalStep': step, 'terminalCode': error.get('code') if isinstance(error, dict) else None,
                'terminalStage': safe_label(metadata.get('stage')),
                'terminalDetail': safe_label(metadata.get('detail')),
                'terminalCategory': safe_label(metadata.get('category')),
                'terminalHttpStatuses': sorted({int(code) for code in re.findall(r'(?:status(?:_code)?[=: ]+|http[=: ]+)([1-5][0-9]{2})',message)}),
                'terminalCloudflareChallenge': bool(re.search(r'\bcf[-_]mitigated\s*[:=]\s*challenge\b', message)),
                'diagnosticSignals': [signal for signal in ('invalid_state','missing_login_session','otp_timeout',
                    'phone_verification_required','add_phone','invalid_grant','email_otp_resend',
                    'browser_verification_required','callback_url','login_session','rate_limit','timeout') if signal in message],
                'mailboxAcquireOk': steps.get('acquire-mailbox') == 'ok',
                'mailboxReleaseOk': steps.get('release-mailbox') == 'ok',
                'proxyReleaseOk': steps.get('release-proxy-chain') == 'ok'}
    stage = 'artifact-validation'
    artifact_code = 'SINCE = ' + repr(SINCE) + '\n' + ARTIFACT_SOURCE
    artifacts = json.loads(run(['docker', 'exec', '-i', 'easy-register', 'python', '-'], artifact_code))
    dashboard = {'ok': False}
    try:
        register = next(item for item in containers if item['Name'].lstrip('/') == 'easy-register')
        environment = dict(item.split('=', 1) for item in register['Config'].get('Env', []))
        control_token = environment.get('EASY_PROTOCOL_CONTROL_TOKEN', '').strip()
        request = urllib.request.Request('http://127.0.0.1:19790/api/status',
            headers={'Authorization': 'Bearer ' + control_token})
        with urllib.request.urlopen(request, timeout=5) as response:
            dashboard = {'status': response.status,
                'ok': response.status == 200 and isinstance(json.load(response), dict)}
    except Exception as error:
        dashboard['errorType'] = type(error).__name__
    provider_otp = {'ok': False}
    try:
        provider_logs = run(['docker','logs','--timestamps','--since',SINCE,'easy-register-protocol-python'])
        markers = {'resendRequested': 'requesting email OTP after an empty-inbox grace period',
                   'resendAccepted': '[mailbox] wait_openai_code requested one resend',
                   'codeReceived': '[mailbox] wait_openai_code received ',
                   'snapshotCodeReceived': '[mailbox] wait_openai_code snapshot_fallback '}
        matches = [{'at': line.split(' ',1)[0], 'event': event}
                   for line in provider_logs.splitlines() for event,marker in markers.items() if marker in line]
        provider_otp = {'ok': True, 'counts': dict(collections.Counter(item['event'] for item in matches)),
                        'recentEvents': matches[-40:]}
    except Exception as error:
        provider_otp['errorType'] = type(error).__name__
    print(json.dumps({'ok': True, 'capturedAt': datetime.now(timezone.utc).isoformat(), 'since': SINCE,
        'containers': [{'name': item['Name'].lstrip('/'), 'id': item['Id'], 'imageId': item['Image'],
            'running': item['State']['Running'], 'startedAt': item['State']['StartedAt'],
            'restartCount': item['RestartCount']} for item in containers],
        'dashboard': dashboard, 'providerOtp': provider_otp, 'completedTasks': list(finished.values()),
        'activeTasks': [{'taskIndex': task, 'startedAt': start}
            for task, start in started.items() if task not in finished],
        'eventCounts': dict(events), 'artifacts': artifacts}))
except Exception as error:
    print(json.dumps({'ok': False, 'failedStage': stage, 'errorType': type(error).__name__}))
    raise SystemExit(1)
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", required=True, help="Docker restart timestamp in UTC")
    args = parser.parse_args()
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.104", username="mjc",
                       key_filename="C:/Users/vmjcv/.ssh/id_ed25519",
                       look_for_keys=False, allow_agent=False, timeout=15)
        stdin, stdout, stderr = client.exec_command("python3 -", timeout=220)
        stdin.write("SINCE = " + repr(args.since) + "\nARTIFACT_SOURCE = " + repr(ARTIFACTS) + "\n" + OBSERVE)
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
