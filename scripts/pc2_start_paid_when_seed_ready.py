"""Arm the already prepared first canary once; never retry a started run."""

from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


BASE = Path("/home/mjc/easyregister/paid-sms-canary/20260912-001")
PAID = "easyregister-paid-once-20260912-001-verified"
PROVIDER_IMAGE = "sha256:84aa0d7e469e82c73fa3aef3523b171ba01e7dcd41f1add9751d75cb6e3fac94"
ORCHESTRATOR_IMAGE = "sha256:7839c7cc9e76fc29322cd5a4514d89c96f7048730fba450afcf215e06249b13d"
CONTROLLER_STEM = "first-paid-controller-002"

SEED_STATUS = """
import json,hashlib
from pathlib import Path
from datetime import datetime,timezone
from others.common_runtime import validate_openai_oauth_seed_payload
root=Path('/shared/register-output/paid-sms-canary-20260912-001')
if not (root/'seed-ready.json').exists():
 print(json.dumps({'ready':False}));raise SystemExit(0)
raw=(root/'seed-pool/seed.json').read_bytes();payload=json.loads(raw)
valid,_=validate_openai_oauth_seed_payload(payload)
age=(datetime.now(timezone.utc)-datetime.fromisoformat(payload['createdAt'].replace('Z','+00:00'))).total_seconds()
ready=json.loads((root/'seed-ready.json').read_text())
print(json.dumps({'ready':valid and 0<=age<=300,'ageSeconds':age,
 'backupMatches':raw==(root/'private-seed-backup/seed-original.json').read_bytes(),
 'hashMatches':hashlib.sha256(raw).hexdigest()==ready['sha256'],
 'started':(root/'run.started.json').exists(),'seedSha256':hashlib.sha256(raw).hexdigest()}))
"""

QUOTE = """
import os,json,urllib.request,urllib.parse
from decimal import Decimal
from datetime import datetime,timezone
root='http://127.0.0.1:19881/stubs/handler_api.php'
def query(action,**kwargs):
 params={'api_key':os.environ['HEROSMS_API_KEY'],'action':action,**kwargs}
 with urllib.request.urlopen(root+'?'+urllib.parse.urlencode(params),timeout=25) as r:
  assert r.status==200
  return r.read().decode()
balance=Decimal(query('getBalance').split(':',1)[1])
entry=json.loads(query('getPrices',service='dr',country='16'))['16']['dr']
cost=Decimal(str(entry['cost']))
active=json.loads(query('getActiveActivations'))['data']
assert isinstance(active,list) and not active
assert balance>=cost and Decimal('0')<cost<=Decimal('0.05') and int(entry['count'])>0
print(json.dumps({'balanceUsd':str(balance),'quotedCostUsd':str(cost),'activeOrders':0,
 'country':16,'service':'dr','maxPriceUsd':'0.05','checkedAt':datetime.now(timezone.utc).isoformat()}))
"""


def inspect(name: str) -> dict:
    return json.loads(subprocess.check_output(["docker", "inspect", name], stderr=subprocess.PIPE))[0]


def in_container(name: str, code: str, timeout: int = 20) -> dict:
    result = subprocess.run(
        ["docker", "exec", name, "python", "-c", code],
        capture_output=True, text=True, timeout=timeout,
    )
    if result.returncode:
        raise RuntimeError("canary_preflight_command_failed")
    return json.loads(result.stdout)


def verify_runtime() -> None:
    paid = inspect(PAID)
    if paid["State"]["Status"] != "created" or paid["Image"] != ORCHESTRATOR_IMAGE:
        raise RuntimeError("first_paid_container_not_unstarted_verified_image")
    provider = inspect("easyregister-oauth-provider-20260912-001")
    if provider["State"]["Status"] != "running" or provider["Image"] != PROVIDER_IMAGE:
        raise RuntimeError("browser_candidate_not_active")
    sms = inspect("easyregister-paid-sms-20260912-001")
    if sms["State"]["Status"] != "running" or set(sms["NetworkSettings"]["Networks"]) != {"easyregister-paid-20260912-001"}:
        raise RuntimeError("isolated_sms_route_not_active")
    guard = inspect("easyregister-paid-guard-20260912-001")
    if guard["State"]["Status"] != "running" or (BASE / "guard-state/attempt.json").exists():
        raise RuntimeError("original_guard_not_available_for_first_attempt")


def main() -> int:
    os.umask(0o077)
    verify_runtime()
    with (BASE / f"{CONTROLLER_STEM}.started.json").open("x", encoding="utf-8") as stream:
        json.dump({"armedAt": datetime.now(timezone.utc).isoformat(), "container": PAID}, stream)
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({"event": "first_paid_controller_armed", "maxOrders": 1, "maxPriceUsd": "0.05"}), flush=True)
    deadline = time.monotonic() + 1800
    while time.monotonic() < deadline:
        seed = in_container("easy-register", SEED_STATUS)
        if seed.get("ready"):
            if not seed.get("backupMatches") or not seed.get("hashMatches") or seed.get("started"):
                raise RuntimeError("first_seed_integrity_not_verified")
            verify_runtime()
            quote = in_container("easyregister-paid-guard-20260912-001", QUOTE, timeout=90)
            with (BASE / f"{CONTROLLER_STEM}.preflight.json").open("x", encoding="utf-8") as stream:
                json.dump({"seed": seed, "quote": quote}, stream, indent=2)
            subprocess.run(["docker", "start", PAID], capture_output=True, check=True)
            print(json.dumps({"event": "first_paid_container_started", "seed": seed, "quote": quote}), flush=True)
            return 0
        time.sleep(5)
    print(json.dumps({"event": "first_paid_controller_expired", "containerStarted": False}), flush=True)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
