"""Prepare the fixed first PC2 paid canary; this script never starts its flow."""

from __future__ import annotations

import json
import shlex
from pathlib import Path

import paramiko
import yaml


ROOT = Path(__file__).resolve().parents[1]
REMOTE = "/home/mjc/easyregister/paid-sms-canary/20260912-001"

PREPARE = r'''
import hashlib
import json
import os
import secrets
import subprocess
from pathlib import Path

os.umask(0o077)
base = Path('/home/mjc/easyregister/paid-sms-canary/20260912-001')
tag = '20260912-001'
out = Path('/home/mjc/easyregister/output/paid-sms-canary-' + tag)
target = '/shared/register-output/paid-sms-canary-' + tag
network = 'easyregister-paid-' + tag
guard = 'easyregister-paid-guard-' + tag
sms = 'easyregister-paid-sms-' + tag
once = 'easyregister-paid-once-' + tag

def run(args, *, data=None, timeout=90):
    result = subprocess.run(args, input=data, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        (base / 'private-command-error.json').write_text(json.dumps({
            'args': args, 'stdout': result.stdout, 'stderr': result.stderr,
        }), encoding='utf-8')
        raise RuntimeError('command_failed_' + args[0])
    return result.stdout

def save(path, value):
    with path.open('x', encoding='utf-8', newline='\n') as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())

def inspect(name):
    return json.loads(run(['docker', 'inspect', name]))[0]

if (base / 'prepared.json').exists():
    raise RuntimeError('canary_already_prepared')
if (base / 'guard-state' / 'attempt.json').exists() or (out / 'run.started.json').exists():
    raise RuntimeError('canary_already_consumed')

production = inspect('easy-register')
sms_production = inspect('easy-register-sms')
env = dict(item.split('=', 1) for item in production['Config']['Env'] if '=' in item)
if env.get('REGISTER_SMS_ALLOW_PAID') != 'false' or env.get('REGISTER_SMS_BUSINESS_POLICIES_JSON'):
    raise RuntimeError('production_paid_boundary_changed')
for path in (base / 'guard-state', base / 'sms-config', base / 'sms-data', out):
    path.mkdir(mode=0o700, parents=True, exist_ok=False)
for name in ('seed-pool', 'private-seed-backup'):
    (out / name).mkdir(mode=0o700)

key = (base / 'herosms.key').read_text(encoding='utf-8')
if len(key) != 32 or key.strip() != key:
    raise RuntimeError('saved_key_format_invalid')
bearer = secrets.token_urlsafe(32)
node = "(async()=>console.log(JSON.stringify(await(await import('/app/dist/src/runtime/config.js')).loadEasySmsConfig())))().catch(()=>process.exit(1));"
config = json.loads(run(['docker', 'exec', 'easy-register-sms', 'node', '-e', node]))
config['server'].update({'host': '0.0.0.0', 'port': 8080, 'apiKey': bearer})
config['maintenance'].update({'enabled': False, 'activeProbeEnabled': False})
config['persistence'].update({'enabled': True, 'filePath': '/var/lib/easy-sms/state.json', 'intervalMs': 2000})
config['providers']['enabledProviders'] = ['hero_sms']
config['providers']['heroSms'].update({
    'enabled': True, 'apiKey': key,
    'baseUrl': 'http://' + guard + ':19881/stubs/handler_api.php',
    'defaultService': 'dr', 'defaultCountry': 16, 'reuseEnabled': False,
    'defaultMaxBindingsPerPhone': 1,
})
save(base / 'sms-config' / 'config.yaml', json.dumps(config, indent=2) + '\n')
save(base / 'guard.env', 'HEROSMS_API_KEY=' + key + '\n')

for name, value in list(env.items()):
    if value.startswith('/shared/register-output'):
        env[name] = target + value[len('/shared/register-output'):]
env.update({
    'SMS_SERVICE_BASE_URL': 'http://' + sms + ':8080',
    'SMS_SERVICE_API_KEY': bearer,
    'SMS_SERVICE_REQUEST_ATTEMPTS': '1',
    'SMS_SERVICE_SELECTION_PLAN_ATTEMPTS': '1',
    'REGISTER_SMS_ALLOW_PAID': 'true',
    'REGISTER_SMS_ALLOW_REUSE': 'false',
    'REGISTER_SMS_MAX_BINDINGS_PER_PHONE': '1',
    'REGISTER_SMS_COUNTRY_CODES': 'gb',
    'REGISTER_SMS_COUNTRY_ID': '16',
    'REGISTER_SMS_MAX_PRICE': '0.05',
    'REGISTER_SMS_PROVIDER_BLACKLIST': 'onlinesim,smstome,receive_smss,receive_sms_free_cc,sms24,yunduanxin',
    'REGISTER_SMS_BUSINESS_POLICIES_JSON': json.dumps({'openai': {
        'enabled': True, 'allowPaid': True, 'allowReuse': False,
        'maxBindingsPerPhone': 1, 'countryCodes': ['gb'], 'countryId': 16, 'maxPrice': 0.05,
    }}, separators=(',', ':')),
    'REGISTER_SMS_STATE_PATH': target + '/sms-state.json',
    'REGISTER_SMS_SESSION_LOCAL_RETRY_ATTEMPTS': '1',
    'REGISTER_PHONE_VERIFICATION_TERMINAL_RETRY_ATTEMPTS': '1',
    'REGISTER_PHONE_VERIFICATION_SMS_CODE_WAIT_RETRY_ATTEMPTS': '1',
    'REGISTER_PHONE_VERIFICATION_SAME_SESSION_CODE_RETRY_ATTEMPTS': '0',
    'REGISTER_PHONE_VERIFICATION_SMS_CODE_WAIT_TIMEOUT_SECONDS': '420',
    'REGISTER_OPENAI_OAUTH_POOL_DIR': target + '/seed-pool',
    'REGISTER_SMALL_SUCCESS_POOL_DIR': target + '/seed-pool',
    'REGISTER_OUTPUT_ROOT': target + '/run',
    'REGISTER_FLOW_PATH': '/canary/flow.json',
    'REGISTER_TEAM_INVITE_ENABLED': 'false',
    'REGISTER_R2_UPLOAD_ENABLED': 'false',
    'REGISTER_FREE_STOP_AFTER_VALIDATE': 'false',
})
if any('\n' in value or '\r' in value for value in env.values()):
    raise RuntimeError('multiline_environment_not_supported')
save(base / 'register.env', ''.join(name + '=' + value + '\n' for name, value in sorted(env.items())))
flow_path = '/app/server/services/orchestration_service/flows/codex-openai-oauth-continue-v1.semantic-flow.json'
flow = json.loads(run(['docker', 'exec', 'easy-register', 'cat', flow_path]))
definition = flow['definition']
definition.setdefault('metadata', {}).setdefault('taskRetry', {})['maxAttempts'] = 1
for step in definition['steps']:
    step.setdefault('metadata', {}).setdefault('retry', {})['maxAttempts'] = 1
save(base / 'flow.json', json.dumps(flow, indent=2) + '\n')
save(base / 'manifest.json', json.dumps({
    'outputRoot': target, 'smsBaseUrl': env['SMS_SERVICE_BASE_URL'], 'maxPrice': 0.05, 'countryId': 16,
}, indent=2) + '\n')

proof = run(['docker', 'run', '--rm', '--network', 'none',
    '--mount', 'type=bind,src=' + str(base) + ',dst=/canary,readonly',
    '--workdir', '/canary', '--env', 'PYTHONDONTWRITEBYTECODE=1',
    '--entrypoint', 'python', production['Image'],
    '-m', 'unittest', 'discover', '-s', 'tests', '-p', 'test_paid_sms_canary_guard.py'], timeout=90)
run(['docker', 'run', '--rm', '--network', 'none',
    '--mount', 'type=bind,src=' + str(base) + ',dst=/canary,readonly',
    '--env', 'PYTHONDONTWRITEBYTECODE=1', '--entrypoint', 'python', production['Image'],
    '-c', "import sys;sys.path.insert(0,'/canary');import pc2_paid_sms_once;print('imports_ok')"])
run(['docker', 'network', 'create', '--internal', '--label', 'easyregister.paid.canary=' + tag, network])
run(['docker', 'create', '--name', guard, '--restart', 'no', '--network', 'easyregister-pc2',
    '--env-file', str(base / 'guard.env'),
    '--mount', 'type=bind,src=' + str(base) + ',dst=/canary,readonly',
    '--mount', 'type=bind,src=' + str(base / 'guard-state') + ',dst=/guard-state',
    '--entrypoint', 'python', production['Image'], '/canary/paid_sms_canary_guard.py',
    '--ledger', '/guard-state/attempt.json', '--max-price', '0.05', '--country', '16',
    '--listen', '0.0.0.0', '--port', '19881'])
run(['docker', 'network', 'connect', network, guard])
run(['docker', 'create', '--name', sms, '--restart', 'no', '--network', network,
    '--env', 'EASY_SMS_CONFIG_PATH=/etc/easy-sms/config.yaml', '--env', 'EASY_SMS_STATE_DIR=/var/lib/easy-sms',
    '--mount', 'type=bind,src=' + str(base / 'sms-config') + ',dst=/etc/easy-sms,readonly',
    '--mount', 'type=bind,src=' + str(base / 'sms-data') + ',dst=/var/lib/easy-sms',
    sms_production['Image']])
run(['docker', 'create', '--name', once, '--restart', 'no', '--network', 'easyregister-pc2',
    '--env-file', str(base / 'register.env'),
    '--mount', 'type=bind,src=' + str(base) + ',dst=/canary,readonly',
    '--mount', 'type=bind,src=' + str(out) + ',dst=' + target,
    '--entrypoint', 'python', production['Image'], '/canary/pc2_paid_sms_once.py'])
run(['docker', 'network', 'connect', network, once])
run(['docker', 'start', guard, sms])
result = {
    'canary': tag, 'guard': guard, 'sms': sms, 'once': once,
    'guardRunning': inspect(guard)['State']['Running'],
    'smsRunning': inspect(sms)['State']['Running'],
    'onceStatus': inspect(once)['State']['Status'],
    'internalNetwork': json.loads(run(['docker', 'network', 'inspect', network]))[0]['Internal'],
    'ledgerExists': (base / 'guard-state' / 'attempt.json').exists(),
    'linuxGuardTestsPassed': True, 'onceRunnerImportsPassed': True,
    'sourceHashes': {name: hashlib.sha256((base / name).read_bytes()).hexdigest()
        for name in ('paid_sms_canary_guard.py', 'pc2_paid_sms_once.py')},
}
save(base / 'prepared.json', json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
'''


def main() -> int:
    config = yaml.safe_load((ROOT.parent / "EasySMS" / "config.yaml").read_text(encoding="utf-8"))
    key = config["serviceBase"]["runtime"]["providers"]["heroSms"]["apiKey"]
    if not isinstance(key, str) or len(key) != 32:
        raise RuntimeError("saved_key_format_invalid")
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.104", username="mjc", key_filename=str(Path.home() / ".ssh/id_ed25519"),
                       look_for_keys=False, allow_agent=False, timeout=15)
        command = "umask 077; mkdir -p " + shlex.quote(REMOTE + "/tests")
        _, stdout, _ = client.exec_command(command)
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError("remote_directory_creation_failed")
        with client.open_sftp() as sftp:
            for name in ("paid_sms_canary_guard.py", "pc2_paid_sms_once.py"):
                sftp.put(str(ROOT / "scripts" / name), REMOTE + "/" + name)
                sftp.chmod(REMOTE + "/" + name, 0o600)
            sftp.mkdir(REMOTE + "/scripts", mode=0o700)
            sftp.put(str(ROOT / "scripts" / "paid_sms_canary_guard.py"), REMOTE + "/scripts/paid_sms_canary_guard.py")
            sftp.put(str(ROOT / "tests" / "test_paid_sms_canary_guard.py"), REMOTE + "/tests/test_paid_sms_canary_guard.py")
            with sftp.open(REMOTE + "/herosms.key", "wx") as stream:
                stream.write(key)
            sftp.chmod(REMOTE + "/herosms.key", 0o600)
        stdin, stdout, stderr = client.exec_command("python3 -", timeout=180)
        stdin.write(PREPARE)
        stdin.flush()
        stdin.channel.shutdown_write()
        output = stdout.read().decode("utf-8")
        status = stdout.channel.recv_exit_status()
        if status:
            print(json.dumps({"prepared": False, "remoteExit": status}))
            return status
        print(output.strip())
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
