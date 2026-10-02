"""Wait for a fresh, validator-approved PC2 small-success artifact; read-only."""

from __future__ import annotations

import argparse
import importlib.util
import json
import shlex
from pathlib import Path

import paramiko


ROOT = Path(__file__).resolve().parents[1]
WATCH = r'''
import json,subprocess,sys,time
payload=json.load(sys.stdin)
SINCE=payload['since'];ARTIFACT_SOURCE=payload['artifacts']
deadline=time.monotonic()+payload['timeoutSeconds']
next_report=0.0
while True:
    source='SINCE='+repr(SINCE)+'\n'+ARTIFACT_SOURCE
    child=subprocess.run(['docker','exec','-i','easy-register','python','-'],
        input=source,capture_output=True,text=True,timeout=50)
    if child.returncode:
        print(json.dumps({'phase':'complete','ok':False,'error':'artifact_read_failed'}),flush=True)
        raise SystemExit(1)
    result=json.loads(child.stdout)
    if result['newCurrentAgeValidCount']>0 or time.monotonic()>=deadline:
        break
    if time.monotonic()>=next_report:
        print(json.dumps({'phase':'waiting','newFiles':result['newFileCount'],
            'newMilestoneValid':result['newMilestoneValidCount']}),flush=True)
        next_report=time.monotonic()+120
    time.sleep(min(15,max(0,deadline-time.monotonic())))
exec(compile(payload['observe'],'pc2-final-observation','exec'))
raise SystemExit(0 if result['newCurrentAgeValidCount']>0 else 2)
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    args = parser.parse_args()
    if not 1 <= args.timeout_seconds <= 3600:
        parser.error("--timeout-seconds must be between 1 and 3600")
    spec = importlib.util.spec_from_file_location("pc2_audit", ROOT / "scripts/audit-pc2-restart-results.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("audit_module_missing")
    audit = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(audit)
    payload = {"since": args.since, "timeoutSeconds": args.timeout_seconds,
               "artifacts": audit.ARTIFACTS, "observe": audit.OBSERVE}
    compile(WATCH, "pc2-small-success-watch", "exec")
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.104", username="mjc", key_filename=str(Path.home() / ".ssh/id_ed25519"),
                       look_for_keys=False, allow_agent=False, timeout=15)
        stdin, stdout, stderr = client.exec_command("python3 -c " + shlex.quote(WATCH), timeout=args.timeout_seconds + 180)
        stdin.write(json.dumps(payload)); stdin.flush(); stdin.channel.shutdown_write()
        for line in stdout:
            print(line.rstrip(), flush=True)
        if stderr.read():
            print(json.dumps({"remoteStderrPresent": True}), flush=True)
        return stdout.channel.recv_exit_status()
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
