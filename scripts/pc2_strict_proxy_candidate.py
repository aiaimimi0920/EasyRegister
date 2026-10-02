"""Build and offline-test the narrowly scoped PC2 proxy preflight candidate."""

from __future__ import annotations

from pathlib import Path

import paramiko


ROOT = Path(__file__).resolve().parents[1]
WORK = "/home/mjc/easyregister/paid-sms-canary/20260912-001/oauth-candidate-001/strict-adapter-fix"

REMOTE = r'''
import hashlib, json, os, subprocess
from datetime import datetime, timezone
from pathlib import Path

os.umask(0o077)
work = Path("/home/mjc/easyregister/paid-sms-canary/20260912-001/oauth-candidate-001/strict-adapter-fix")
base = "sha256:7839c7cc9e76fc29322cd5a4514d89c96f7048730fba450afcf215e06249b13d"
base_tag = "easyregister:strict-probe-20260912-001"
old = "easyregister-paid-once-20260912-001-verified"
src = "/app/server/services/orchestration_service/src"
flows = "/app/server/services/orchestration_service/flows"

def run(args, timeout=90):
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError("candidate_command_failed:" + args[0])
    return p.stdout

def inspect(name):
    return json.loads(run(["docker", "inspect", name]))[0]

def source(path):
    code = "from pathlib import Path;import sys;sys.stdout.write(Path(" + repr(path) + ").read_text(encoding='utf-8'))"
    return run(["docker", "run", "--rm", "--network", "none", "--entrypoint", "python", base, "-c", code])

def snapshot():
    return {name: {"id": c["Id"], "image": c["Image"], "startedAt": c["State"]["StartedAt"]}
            for name in ["easy-register", "easy-register-sms"] for c in [inspect(name)]}

assert inspect(old)["Image"] == base
assert json.loads(run(["docker", "image", "inspect", base_tag]))[0]["Id"] == base
assert inspect(old)["State"]["Status"] == "exited"
assert not (work.parents[1] / "guard-state/attempt.json").exists()
before_runtime = snapshot()
before = source(src + "/easyproxy_flow.py")
old_policy = """        allow_openai_auth_challenge = str(
            step_input.get("allow_openai_auth_challenge")
            or step_input.get("allowOpenaiAuthChallenge")
            or ""
        ).strip().lower() in {"1", "true", "yes", "on"}
"""
new_policy = """        challenge_policy = step_input.get("allow_openai_auth_challenge")
        if challenge_policy is None:
            challenge_policy = step_input.get("allowOpenaiAuthChallenge")
        allow_openai_auth_challenge = (
            None
            if challenge_policy is None
            else str(challenge_policy).strip().lower() in {"1", "true", "yes", "on"}
        )
"""
old_forward = '            if allow_openai_auth_challenge:\n                proxy_kwargs["allow_openai_auth_challenge"] = True\n'
new_forward = '            if allow_openai_auth_challenge is not None:\n                proxy_kwargs["allow_openai_auth_challenge"] = allow_openai_auth_challenge\n'
assert before.count(old_policy) == 1 and before.count(old_forward) == 1
after = (work / "easyproxy_flow.py").read_text(encoding="utf-8")
assert after == before.replace(old_policy, new_policy).replace(old_forward, new_forward)
(work / "source-before.py").write_text(before, encoding="utf-8")

names = ["codex-openai-account-v1.semantic-flow.json", "codex-openai-oauth-continue-v1.semantic-flow.json"]
for name in names:
    original = json.loads(source(flows + "/" + name))
    step = next(s for s in original["definition"]["steps"] if s["type"] == "acquire_proxy_chain")
    step["input"]["probe_urls"] = ["https://auth.openai.com/log-in-or-create-account", "https://chatgpt.com/auth/login", "https://chatgpt.com/api/auth/csrf"]
    step["input"]["allow_openai_auth_challenge"] = False
    assert original == json.loads((work / name).read_text(encoding="utf-8"))

dockerfile = "FROM " + base_tag + "\nCOPY easyproxy_flow.py " + src + "/easyproxy_flow.py\n"
dockerfile += "".join("COPY " + name + " " + flows + "/" + name + "\n" for name in names)
(work / "Dockerfile").write_text(dockerfile, encoding="utf-8")
tag = "easyregister:strict-chatgpt-preflight-20260912-001"
p = subprocess.run(["docker", "build", "--network", "none", "-t", tag, str(work)], capture_output=True, text=True, timeout=180)
(work / "build.log").write_text(p.stdout + p.stderr, encoding="utf-8")
assert p.returncode == 0
image = json.loads(run(["docker", "image", "inspect", tag]))[0]["Id"]
tests = []
for name in ["test_easyproxy_flow.py", "test_runtime_proxy_acquire.py", "test_runtime_proxy_probe.py"]:
    p = subprocess.run(["docker", "run", "--rm", "--network", "none", "--env", "PYTHONDONTWRITEBYTECODE=1", "--mount", "type=bind,src=" + str(work) + ",dst=/candidate,readonly", "--entrypoint", "python", image, "-m", "unittest", "discover", "-s", "/candidate/tests", "-p", name], capture_output=True, text=True, timeout=90)
    (work / (name + ".log")).write_text(p.stdout + p.stderr, encoding="utf-8")
    assert p.returncode == 0, "candidate_test_failed:" + name
    tests.append({"file": name, "passed": True})
assert snapshot() == before_runtime
proof = {"observedAt": datetime.now(timezone.utc).isoformat(), "baseImage": base, "image": image, "tag": tag, "adapterBeforeSha256": hashlib.sha256(before.encode()).hexdigest(), "adapterSha256": hashlib.sha256(after.encode()).hexdigest(), "changedFiles": ["easyproxy_flow.py", *names], "offlineTests": tests, "productionChanged": False}
with (work / "build.json").open("x", encoding="utf-8") as f:
    json.dump(proof, f, indent=2)
print(json.dumps(proof), flush=True)
'''


def main() -> int:
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect(
            "192.168.15.104", username="mjc",
            key_filename=str(Path.home() / ".ssh/id_ed25519"),
            look_for_keys=False, allow_agent=False, timeout=15,
        )
        with client.open_sftp() as sftp:
            sftp.mkdir(WORK, mode=0o700)
            sftp.mkdir(WORK + "/tests", mode=0o700)
            sources = {"server/services/orchestration_service/src/easyproxy_flow.py": "easyproxy_flow.py"}
            for name in ("codex-openai-account-v1.semantic-flow.json", "codex-openai-oauth-continue-v1.semantic-flow.json"):
                sources["server/services/orchestration_service/flows/" + name] = name
            for name in ("test_easyproxy_flow.py", "test_runtime_proxy_acquire.py", "test_runtime_proxy_probe.py"):
                sources["tests/" + name] = "tests/" + name
            for local, remote in sources.items():
                sftp.put(str(ROOT / local), WORK + "/" + remote)
                sftp.chmod(WORK + "/" + remote, 0o600)
        stdin, stdout, stderr = client.exec_command("python3 -", timeout=360)
        stdin.write(REMOTE)
        stdin.flush()
        stdin.channel.shutdown_write()
        print(stdout.read().decode("utf-8"), end="")
        status = stdout.channel.recv_exit_status()
        if status:
            error = stderr.read().decode("utf-8", errors="replace").splitlines()
            print("candidate_failed:" + (error[-1].split(":")[0] if error else "unknown"))
        return status
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
