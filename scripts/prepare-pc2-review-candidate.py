"""Build and test a source-pinned PC2 review candidate without changing live services."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import shlex
from pathlib import Path

import paramiko


ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = "server/services/orchestration_service/src/"
SOURCES = {
    "others/artifact_pool_claims.py": (
        "8d9c8b3261b9fa7e140346f853045d2e190a7a4371325cb627c9fcb2d5ab940c",
        "9459f5ae8719b8b0a2f7c7df17554a74d18ae1932e597c1739b6427c6ce08ead",
    ),
    "others/dashboard_http.py": (
        "e4d66e7377b7f2a081878763153cdbf79f701569479ba7ecf6975b671ba90583",
        "f3bc4e2e62781bd54687bea71122513cea83420b0608c7761308777391d2a49d",
    ),
    "others/common_credentials.py": (
        "3b089c72d00d4f1c21d47d0c68992cd87cf1842f85ec3115f2dc6760873c1f80",
        "617aac8db46c1555398b9937c9da36c52bf86dc5f649c357197d7fbd1c2632c7",
    ),
    "dashboard_server.py": (
        "a6f90653985e00efd5f7429c92bccb56ff8211863b0855951413a99bd302a48c",
        "81346a9d6a44dff44cee1db9520c957436fb6f9d36968b25a7db1bd8cf7c2e29",
    ),
}
TESTS = (
    "test_artifact_pool_modules.py",
    "test_dashboard_auth_transport.py",
    "test_dashboard_integration.py",
    "test_common_credentials_security.py",
    "test_security_defaults.py",
    "test_paid_sms_canary_guard.py",
    "test_dst_flow_integration.py",
)

TEST_PROGRAM = r'''
import base64, contextlib, io, json, sys, unittest
from pathlib import Path
payload=json.load(sys.stdin)
for name,encoded in payload['tests'].items():
    (Path('/app/tests')/name).write_bytes(base64.b64decode(encoded))
Path('/app/scripts/paid_sms_canary_guard.py').write_bytes(base64.b64decode(payload['guard']))
sys.path[:0]=['/app/tests','/app/server/services/orchestration_service/src']
loader=unittest.TestLoader();suite=unittest.TestSuite()
for name in payload['tests']:
    if name=='test_dst_flow_integration.py':
        continue
    suite.addTests(loader.loadTestsFromName(name.removesuffix('.py')))
module=__import__('test_dst_flow_integration')
method='test_run_dst_flow_once_collects_openai_pool_as_soon_as_small_success_is_created'
classes=[value for value in vars(module).values() if isinstance(value,type) and issubclass(value,unittest.TestCase) and hasattr(value,method)]
assert len(classes)==1
suite.addTest(classes[0](method))
log=io.StringIO()
with contextlib.redirect_stdout(log),contextlib.redirect_stderr(log):
    result=unittest.TextTestRunner(stream=log,verbosity=2).run(suite)
print(json.dumps({'ok':result.wasSuccessful(),'testsRun':result.testsRun,'failures':len(result.failures),
 'errors':len(result.errors),'skipped':len(result.skipped),'failedTests':[test.id() for test,_ in result.failures+result.errors]}))
sys.stderr.write(log.getvalue())
raise SystemExit(0 if result.wasSuccessful() else 1)
'''

REMOTE = r'''
import base64, hashlib, io, json, os, re, subprocess, sys, tarfile
from datetime import datetime, timezone
from pathlib import Path
payload=json.load(sys.stdin);os.umask(0o077)
names=['easy-register','easy-register-protocol-python','easy-register-protocol','easy-register-sms']
summary={'ok':False,'productionPromoted':False,'images':{}};stage='preflight';before=None
def require(value,code):
    if not value: raise RuntimeError(code)
def run(args,source=None,timeout=60):
    result=subprocess.run(args,input=source,capture_output=True,timeout=timeout)
    if result.returncode: raise RuntimeError('command_failed')
    return result.stdout
def inspect(name,kind='container'):
    return json.loads(run(['docker',kind,'inspect',name]))[0]
def snapshot():
    result={}
    for name in names:
        c=inspect(name)
        result[name]={'id':c['Id'],'imageId':c['Image'],'startedAt':c['State']['StartedAt'],'running':c['State']['Running']}
    return result
def image_hashes(image,paths):
    code='import hashlib,json\nfrom pathlib import Path\nprint(json.dumps({p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in '+repr(paths)+'}))'
    return json.loads(run(['docker','run','--rm','--network','none','--read-only','--entrypoint','python',image,'-B','-c',code]))
try:
    require(re.fullmatch(r'\d{8}-\d{3}',payload['version']) is not None,'version_invalid')
    before=snapshot();summary['containersBefore']=before
    live=inspect('easy-register');base=inspect(live['Config']['Image'],'image')
    require(live['Image']==payload['baseId']==base['Id'],'live_base_changed')
    require(all(item['running'] for item in before.values()),'production_not_running')
    env=dict(item.split('=',1) for item in live['Config'].get('Env',[]) if '=' in item)
    require(env.get('REGISTER_SMS_ALLOW_PAID')=='false','production_paid_boundary_changed')
    require(len(env.get('EASY_PROTOCOL_CONTROL_TOKEN','').strip())>=16,'shared_token_too_short')
    base_root=Path('/home/mjc/easyregister/paid-sms-canary/20260912-001')
    guard=(base_root/'paid_sms_canary_guard.py').read_bytes()
    require(hashlib.sha256(guard).hexdigest()==payload['guardSha256'],'original_guard_source_changed')
    work=Path('/home/mjc/easyregister')/('review-candidate-'+payload['version'])
    if payload.get('verifyExisting'):
        require(work.is_dir(),'candidate_evidence_directory_missing')
    else:
        work.mkdir(mode=0o700,exist_ok=False)
    files={}
    for entry in payload['files']:
        path=entry['path'];current=run(['docker','exec','easy-register','cat',path])
        require(hashlib.sha256(current).hexdigest()==entry['beforeHash'],'live_source_changed')
        source=base64.b64decode(entry['content']);compile(source,path,'exec')
        require(hashlib.sha256(source).hexdigest()==entry['afterHash'],'candidate_source_changed')
        files[path]=source
    stage='build';tag='easy-register/easy-register:pc2-auth-boundary-'+payload['version']
    existing=subprocess.run(['docker','image','inspect',tag],capture_output=True)
    if payload.get('verifyExisting'):
        require(existing.returncode==0 and json.loads(existing.stdout)[0]['Id']==payload['expectedCandidateId'],'existing_candidate_changed')
    else:
        require(existing.returncode!=0,'candidate_tag_already_exists')
    archive=io.BytesIO();dockerfile='FROM '+live['Config']['Image']+'\n'
    with tarfile.open(fileobj=archive,mode='w') as bundle:
        for index,(destination,source) in enumerate(files.items()):
            name='source_'+str(index)+'.py';item=tarfile.TarInfo(name);item.size=len(source);item.mode=0o644
            bundle.addfile(item,io.BytesIO(source));dockerfile+='COPY '+name+' '+destination+'\n'
        raw=dockerfile.encode();item=tarfile.TarInfo('Dockerfile');item.size=len(raw);item.mode=0o644
        bundle.addfile(item,io.BytesIO(raw))
    if not payload.get('verifyExisting'):
        built=subprocess.run(['docker','build','--network','none','--pull=false','-t',tag,'-'],input=archive.getvalue(),capture_output=True,timeout=240)
        (work/'build.log').write_bytes(built.stdout+built.stderr)
        require(built.returncode==0,'candidate_build_failed')
    candidate=inspect(tag,'image')
    require(candidate['RootFS']['Layers'][:len(base['RootFS']['Layers'])]==base['RootFS']['Layers'],'base_layers_changed')
    for key in ('Cmd','Entrypoint','Env','WorkingDir','User'):
        require(candidate['Config'].get(key)==base['Config'].get(key),'image_config_changed')
    hashes={path:hashlib.sha256(source).hexdigest() for path,source in files.items()}
    require(image_hashes(candidate['Id'],list(files))==hashes,'candidate_file_mismatch')
    provider=inspect('easy-register-protocol-python')
    summary['images']={'register':{'tag':tag,'id':candidate['Id'],'baseId':base['Id'],'files':hashes},
                      'provider':{'tag':provider['Config']['Image'],'id':provider['Image'],'files':{},'reused':True}}
    print(json.dumps({'phase':'image_built','imageId':candidate['Id'],'sourceFiles':len(files)}),flush=True)
    stage='offline_tests'
    test_input=json.dumps({'tests':payload['tests'],'guard':base64.b64encode(guard).decode()}).encode()
    checked=subprocess.run(['docker','run','--rm','-i','--network','none','--read-only',
        '--tmpfs','/tmp:rw,nosuid,nodev,size=128m','--tmpfs','/app/tests:rw,nosuid,nodev,size=16m',
        '--tmpfs','/app/scripts:rw,nosuid,nodev,size=4m','--cap-drop','ALL','--security-opt','no-new-privileges',
        '--workdir','/tmp','--entrypoint','python',candidate['Id'],'-B','-c',payload['testProgram']],
        input=test_input,capture_output=True,timeout=180)
    log_name='offline-tests-verified.log' if payload.get('verifyExisting') else 'offline-tests.log'
    with (work/log_name).open('xb') as stream: stream.write(checked.stdout+checked.stderr)
    try: summary['offlineTests']=json.loads(checked.stdout)
    except ValueError: summary['offlineTests']={'ok':False,'outputParsed':False}
    require(checked.returncode==0 and summary['offlineTests'].get('ok') is True,'candidate_tests_failed')
    summary.update({'ok':True,'scope':'four reviewed orchestrator source files','originalGuardSha256':payload['guardSha256']})
except Exception as error:
    summary.update({'failedStage':stage,'errorType':type(error).__name__})
    if isinstance(error,RuntimeError) and re.fullmatch(r'[a-z_]+',str(error)): summary['errorCode']=str(error)
finally:
    summary['containersAfter']=snapshot();summary['productionUnchanged']=before==summary['containersAfter']
    summary['ok']=bool(summary['ok'] and summary['productionUnchanged'])
    summary['capturedAt']=datetime.now(timezone.utc).isoformat()
    print(json.dumps({'phase':'complete',**summary}),flush=True)
raise SystemExit(0 if summary['ok'] else 1)
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", default="20260913-001")
    parser.add_argument("--verify-existing", action="store_true", help="Recheck the pinned existing image without rebuilding it")
    args = parser.parse_args()
    if not re.fullmatch(r"\d{8}-\d{3}", args.version):
        raise ValueError("Invalid candidate version")
    original_output = ROOT / "deploy-evidence" / f"pc2-review-candidates-{args.version}.json"
    output = original_output.with_name(original_output.stem + "-verified.json") if args.verify_existing else original_output
    if output.exists():
        raise FileExistsError("Candidate evidence already exists")
    entries = []
    for name, (before_hash, after_hash) in SOURCES.items():
        source = (ROOT / SOURCE_ROOT / name).read_bytes()
        if hashlib.sha256(source).hexdigest() != after_hash:
            raise RuntimeError("Reviewed local source changed")
        entries.append({"path": "/app/" + SOURCE_ROOT + name, "beforeHash": before_hash,
                        "afterHash": after_hash, "content": base64.b64encode(source).decode("ascii")})
    payload = {
        "version": args.version,
        "verifyExisting": args.verify_existing,
        "baseId": "sha256:a2da348213fe60d102c157ad290455527b94deb3928025e29606ffe8a793f20c",
        "files": entries,
        "guardSha256": "01bf733d1db421fdf39de90b5ef3eb090f25988a52beadea245ff9013221e13c",
        "tests": {name: base64.b64encode((ROOT / "tests" / name).read_bytes()).decode("ascii") for name in TESTS},
        "testProgram": TEST_PROGRAM,
    }
    if args.verify_existing:
        previous = json.loads(original_output.read_text(encoding="utf-8"))
        payload["expectedCandidateId"] = previous["images"]["register"]["id"]
    compile(REMOTE, "pc2-review-build", "exec")
    compile(TEST_PROGRAM, "pc2-review-tests", "exec")
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    summary = None
    try:
        client.connect("192.168.15.104", username="mjc", key_filename=str(Path.home() / ".ssh/id_ed25519"),
                       look_for_keys=False, allow_agent=False, timeout=15)
        stdin, stdout, stderr = client.exec_command("python3 -c " + shlex.quote(REMOTE), timeout=600)
        stdin.write(json.dumps(payload)); stdin.flush(); stdin.channel.shutdown_write()
        for line in stdout:
            event = json.loads(line)
            print(json.dumps(event), flush=True)
            if event.get("phase") == "complete":
                summary = event
        if stderr.read():
            print(json.dumps({"remoteStderrPresent": True}), flush=True)
        status = stdout.channel.recv_exit_status()
        if summary is not None:
            with output.open("x", encoding="utf-8", newline="\n") as stream:
                json.dump(summary, stream, indent=2)
                stream.write("\n")
        return status
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
