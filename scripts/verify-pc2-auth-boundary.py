"""Build auth-error hotfix candidates from the exact live images and test offline."""
from __future__ import annotations

import ast
import hashlib
import json
import shlex
import subprocess
from pathlib import Path

import paramiko


ROOT = Path(__file__).resolve().parents[1]
PROTOCOL_ROOT = ROOT.parent / "EasyProtocol"
SMALL_SUCCESS = "providers/python/src/new_protocol_register/protocol_small_success.py"
REGISTER_FILES = [
    "server/services/orchestration_service/src/others/error_catalog.py",
    "server/services/orchestration_service/src/others/easyprotocol_runtime.py",
    "server/services/orchestration_service/src/others/dst_flow_runtime.py",
    "server/services/orchestration_service/src/others/runner_failures.py",
]

PROVIDER_PROBE = r'''
import json, os, queue
from types import SimpleNamespace
from unittest import mock
import worker_pool
from new_protocol_register import protocol_small_success as flow
from protocol_runtime.errors import ProtocolRuntimeError
response = SimpleNamespace(status_code=403, headers={'cf-mitigated':'challenge'}, text='private fixture')
with mock.patch.object(flow, '_session_request', return_value=response):
    try:
        flow._open_platform_login(session=SimpleNamespace(headers={}), explicit_proxy=None)
        raise AssertionError('challenge_was_accepted')
    except ProtocolRuntimeError as caught:
        error = caught
assert 'browser_verification_required' in str(error)
assert 'private fixture' not in str(error)
assert (error.stage, error.detail, error.category) == ('stage_platform_login','platform_login','blocked')
assert not flow._openai_login_init_error_is_retryable(error)
with mock.patch.object(flow, '_login_session_cookie') as cookie:
    try:
        flow._validate_platform_authorize_response(session=SimpleNamespace(), response=response)
        raise AssertionError('challenge_was_accepted')
    except ProtocolRuntimeError as caught:
        assert 'browser_verification_required' in str(caught)
        assert not flow._should_retry_after_authorize_error(caught, time_remaining_seconds=300)
    cookie.assert_not_called()
tasks, results = queue.Queue(), queue.Queue()
tasks.put({'task_id':'fixture','step_type':'create_openai_account','step_input':{}})
tasks.put(None)
with mock.patch.dict(os.environ), mock.patch.object(worker_pool, '_dispatch_step', side_effect=error):
    worker_pool._worker_process_main('fixture-worker', tasks, results)
packet = results.get_nowait()
assert packet['error']['details']['protocol_error'] == error.to_response_payload()
print(json.dumps({'ok':True,'checks':5,'error':packet['error']}))
'''

REGISTER_PROBE = r'''
import io, json, urllib.error
from unittest import mock
import dst_flow
from errors import ErrorCodes, ProtocolRuntimeError, build_error_details
from others import easyprotocol_runtime, runner_failures
payload = {'status':'failed','error':PROVIDER_ERROR}
for status in (200,502):
    raw=json.dumps(payload).encode()
    kwargs={'return_value':io.BytesIO(raw)} if status == 200 else {
        'side_effect':urllib.error.HTTPError('http://fixture/request',status,'failed',{},io.BytesIO(raw))}
    with mock.patch.object(easyprotocol_runtime.urllib.request,'urlopen',**kwargs):
        try:
            easyprotocol_runtime.invoke_easyprotocol(step_type='create_openai_account',step_input={})
            raise AssertionError('failed_request_was_accepted')
        except ProtocolRuntimeError as error:
            details=build_error_details(step_type='create_openai_account',message=str(error),
                stage=error.stage,detail=error.detail,category=error.category)
    assert details['code'] == ErrorCodes.BROWSER_VERIFICATION_REQUIRED
    assert details['stage'] == 'stage_platform_login'
    assert details['detail'] == 'platform_login'
    assert details['category'] == 'blocked'
retry={'maxAttempts':3,'retryOnCodes':[ErrorCodes.BROWSER_VERIFICATION_REQUIRED]}
statement=dst_flow.DstStatement(step_id='create-openai-account',step_type='create_openai_account',metadata={'retry':retry})
plan=dst_flow.DstPlan(steps=[statement],metadata={'taskRetry':retry})
assert not dst_flow._should_retry_step(statement=statement,error_details=details,attempt_index=1)
assert not dst_flow._should_retry_task(plan=plan,error_step=statement.step_id,error_details=details,attempt_index=1)
result={'errorStep':statement.step_id,'stepErrors':{statement.step_id:details}}
with mock.patch.object(runner_failures,'result_payload',return_value=result):
    assert runner_failures.extra_failure_cooldown_seconds(result=result) == runner_failures._cleanup_runtime_config().oauth_blocked_cooldown_seconds
print(json.dumps({'ok':True,'checks':5,'errorDetails':details}))
'''

REMOTE = r'''
import ast, hashlib, io, json, re, subprocess, sys, tarfile
from datetime import datetime, timezone
payload=json.load(sys.stdin)
summary={'ok':False,'productionPromoted':False,'images':{},'scope':'offline auth boundary validation'}
names=['easy-register','easy-register-protocol-python','easy-register-protocol','easy-register-sms']
def docker(*args,**kwargs):
    return subprocess.run(['docker',*args],capture_output=True,timeout=kwargs.pop('timeout',60),**kwargs)
def require(value,code):
    if not value: raise RuntimeError(code)
def inspect(kind,name):
    p=docker(kind,'inspect',name);require(p.returncode == 0,'inspect_failed')
    return json.loads(p.stdout)[0]
def snapshot():
    result={}
    for name in names:
        c=inspect('container',name)
        result[name]={'id':c['Id'],'imageId':c['Image'],'startedAt':c['State']['StartedAt'],'running':c['State']['Running']}
    return result
def read_source(container,path):
    command='from pathlib import Path; import sys; sys.stdout.buffer.write(Path('+repr(path)+').read_bytes())'
    p=docker('exec',container,'python','-c',command)
    require(p.returncode == 0,'source_read_failed')
    return p.stdout.decode('utf-8').replace('\r\n','\n')
def patch_provider(text):
    functions={n.name:n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef)}
    lines=text.splitlines(keepends=True);edits=[]
    for name,entry in payload['functions'].items():
        require(name in functions,'missing_function_'+name)
        node=functions[name]
        original=''.join(lines[node.lineno-1:node.end_lineno]).strip()
        require(original == entry['before'].strip(),'function_baseline_mismatch_'+name)
        edits.append((node.lineno-1,node.end_lineno,entry['after'].rstrip()+'\n'))
    anchor=functions['_raise_if_unexpected_http'].lineno-1
    edits.append((anchor,anchor,'\n\n'.join(payload['newFunctions'])+'\n\n'))
    main=functions['run_protocol_small_success_once'];matched=0
    for parent in ast.walk(main):
        for field in ('body','orelse','finalbody'):
            body=getattr(parent,field,None)
            if not isinstance(body,list): continue
            for index,node in enumerate(body):
                if not isinstance(node,ast.Assign) or not any(isinstance(t,ast.Name) and t.id == 'login_session' for t in node.targets): continue
                if not isinstance(node.value,ast.Call) or not isinstance(node.value.func,ast.Name) or node.value.func.id != '_get_session_cookie': continue
                following=body[index+1:index+3]
                require(len(following)==2 and isinstance(following[0],ast.If) and isinstance(following[1],ast.Expr),'authorize_guard_shape_changed')
                last=following[1]
                require(isinstance(last.value,ast.Call) and isinstance(last.value.func,ast.Name) and last.value.func.id == '_raise_if_unexpected_http','authorize_http_guard_changed')
                edits.append((node.lineno-1,last.end_lineno,' '*node.col_offset+'_validate_platform_authorize_response(session=session, response=authorize_response)\n'))
                matched+=1
    require(matched == 1,'authorize_guard_count_changed')
    submit=functions['_submit_openai_login_init_authorize_continue_with_retry'];matched=0
    for node in submit.body:
        if isinstance(node,ast.Assign) and isinstance(node.value,ast.Call) and isinstance(node.value.func,ast.Name) and node.value.func.id == '_submit_authorize_continue_login_or_create_account':
            edits.append((node.end_lineno,node.end_lineno,' '*node.col_offset+'_raise_if_browser_verification_required(response, stage="stage_auth_continue", detail="authorize_continue")\n'));matched+=1
    require(matched == 1,'submit_guard_count_changed')
    for start,end,replacement in sorted(edits,key=lambda item:(item[0],item[1]),reverse=True):
        lines[start:end]=[replacement]
    text=''.join(lines)
    anchor='    _response_has_cloudflare_challenge,\n'
    require(text.count(anchor)==1,'response_import_changed')
    text=text.replace(anchor,anchor+'    _response_header,\n',1)
    return text
def build(spec,files):
    base=inspect('image',spec['base']);require(base['Id']==spec['baseId'],'base_image_changed')
    existing=docker('image','inspect',spec['candidate'])
    if existing.returncode == 0:
        candidate=json.loads(existing.stdout)[0]
        require(candidate['RootFS']['Layers'][:len(base['RootFS']['Layers'])]==base['RootFS']['Layers'],'existing_candidate_base_changed')
        for key in ('Cmd','Entrypoint','Env','WorkingDir','User'):
            require(candidate['Config'].get(key)==base['Config'].get(key),'existing_candidate_config_changed')
        expected={path:hashlib.sha256(source.encode()).hexdigest() for path,source in files.items()}
        source='import hashlib,json\nfrom pathlib import Path\nprint(json.dumps({p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in '+repr(list(files))+'}))'
        checked=docker('run','--rm','--network','none','--read-only','--entrypoint','python',candidate['Id'],'-B','-c',source)
        require(checked.returncode == 0 and json.loads(checked.stdout)==expected,'existing_candidate_files_changed')
        return {'tag':spec['candidate'],'id':candidate['Id'],'baseId':base['Id'],'files':expected,'reused':True}
    archive=io.BytesIO();dockerfile='FROM '+spec['base']+'\n'
    with tarfile.open(fileobj=archive,mode='w') as bundle:
        contents={}
        for index,(destination,source) in enumerate(files.items()):
            name='source_'+str(index)+'.py';contents[name]=source
            dockerfile+='COPY '+name+' '+destination+'\n'
        contents['Dockerfile']=dockerfile
        for name,source in contents.items():
            data=source.encode();item=tarfile.TarInfo(name);item.size=len(data);item.mode=0o644
            bundle.addfile(item,io.BytesIO(data))
    p=docker('build','--network','none','--pull=false','-t',spec['candidate'],'-',input=archive.getvalue(),timeout=240)
    require(p.returncode == 0,'image_build_failed')
    candidate=inspect('image',spec['candidate'])
    require(candidate['RootFS']['Layers'][:len(base['RootFS']['Layers'])]==base['RootFS']['Layers'],'base_layers_changed')
    for key in ('Cmd','Entrypoint','Env','WorkingDir','User'):
        require(candidate['Config'].get(key)==base['Config'].get(key),'image_config_changed')
    return {'tag':spec['candidate'],'id':candidate['Id'],'baseId':base['Id'],
            'files':{path:hashlib.sha256(source.encode()).hexdigest() for path,source in files.items()}}
def probe(image,source):
    p=docker('run','--rm','-i','--network','none','--read-only','--tmpfs','/tmp:rw,nosuid,nodev,size=64m',
             '--cap-drop','ALL','--security-opt','no-new-privileges','--entrypoint','python',image,'-B','-',
             input=source.encode(),timeout=90)
    require(p.returncode == 0,'offline_probe_failed')
    return json.loads(p.stdout)
before=None
try:
    before=snapshot();summary['containersBefore']=before
    for key,spec in payload['specs'].items():
        if spec.get('reuseImage'):
            candidate=spec['reuseImage']
            require(before[spec['container']]['imageId']==candidate['id'],'reused_live_image_changed')
            require(inspect('image',candidate['tag'])['Id']==candidate['id'],'reused_image_changed')
            for path,wanted in candidate['files'].items():
                require(hashlib.sha256(read_source(spec['container'],path).encode()).hexdigest()==wanted,'reused_live_source_changed')
            summary['images'][key]={**candidate,'reused':True}
            continue
        require(before[spec['container']]['imageId']==spec['baseId'],'live_image_changed')
        files={}
        for entry in spec['files']:
            original=read_source(spec['container'],entry['path'])
            require(hashlib.sha256(original.encode()).hexdigest()==entry['beforeHash'],'live_source_changed')
            source=patch_provider(original) if entry.get('patchProvider') else entry['source']
            compile(source,entry['path'],'exec');files[entry['path']]=source
        summary['images'][key]=build(spec,files)
        print(json.dumps({'phase':'image_built','kind':key,**summary['images'][key]}),flush=True)
    provider=probe(summary['images']['provider']['id'],payload['providerProbe'])
    register=probe(summary['images']['register']['id'],'PROVIDER_ERROR = '+repr(provider['error'])+'\n'+payload['registerProbe'])
    summary['providerProbe']={k:v for k,v in provider.items() if k != 'error'}
    summary['registerProbe']=register
    require(provider['ok'] and register['ok'],'offline_probe_not_ok')
    summary['ok']=True
except Exception as error:
    summary['errorType']=type(error).__name__
    if isinstance(error,KeyError) and error.args and re.fullmatch(r'[A-Za-z_]+',str(error.args[0])):
        summary['missingSymbol']=str(error.args[0])
    if isinstance(error,RuntimeError) and re.fullmatch(r'[A-Za-z_]+',str(error)):
        summary['errorCode']=str(error)
finally:
    summary['containersAfter']=snapshot()
    summary['productionUnchanged']=before == summary['containersAfter']
    summary['ok']=bool(summary['ok'] and summary['productionUnchanged'])
    summary['capturedAt']=datetime.now(timezone.utc).isoformat()
    print(json.dumps({'phase':'complete',**summary}),flush=True)
sys.exit(0 if summary['ok'] else 1)
'''


def function_sources(text: str) -> dict[str, str]:
    return {
        node.name: ast.get_source_segment(text, node) or ""
        for node in ast.parse(text).body if isinstance(node, ast.FunctionDef)
    }


def source_entry(repo: Path, relative: str, destination: str, before_hash: str | None = None) -> dict[str, str]:
    original = subprocess.check_output(
        ["rtk", "proxy", "git", "show", f"HEAD:{relative}"], cwd=repo
    ).decode("utf-8").replace("\r\n", "\n")
    source = (repo / relative).read_text(encoding="utf-8")
    compile(source, relative, "exec")
    return {"path": destination, "beforeHash": before_hash or hashlib.sha256(original.encode()).hexdigest(), "source": source}


def main() -> int:
    previous = json.loads((ROOT / "deploy-evidence/pc2-auth-boundary-candidates-20260911-002.json").read_text(encoding="utf-8"))
    previous_register = previous["images"]["register"]
    functions = function_sources((PROTOCOL_ROOT / SMALL_SUCCESS).read_text(encoding="utf-8"))
    before = {
        "_raise_if_unexpected_http": functions["_raise_if_unexpected_http"].replace(
            "    _raise_if_browser_verification_required(response, stage=stage, detail=detail)\n", ""),
        "_openai_login_init_error_is_retryable": functions["_openai_login_init_error_is_retryable"].replace(
            ' or "browser_verification_required" in text', ""),
        "_should_retry_after_authorize_error": functions["_should_retry_after_authorize_error"].replace(
            '    if "browser_verification_required" in str(exc).lower():\n        return False\n', ""),
    }
    payload = {
        "functions": {name: {"before": original, "after": functions[name]} for name, original in before.items()},
        "newFunctions": [functions[name] for name in (
            "_raise_if_browser_verification_required", "_validate_platform_authorize_response")],
        "providerProbe": PROVIDER_PROBE,
        "registerProbe": REGISTER_PROBE,
        "specs": {
            "provider": {
                "container": "easy-register-protocol-python",
                "reuseImage": previous["images"]["provider"],
            },
            "register": {
                "container": "easy-register",
                "base": previous_register["tag"],
                "baseId": previous_register["id"],
                "candidate": "easy-register/easy-register:pc2-auth-boundary-20260911-003",
                "files": [source_entry(ROOT, path, "/app/" + path, previous_register["files"].get("/app/" + path)) for path in REGISTER_FILES],
            },
        },
    }
    compile(REMOTE, "auth-boundary-remote", "exec")
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.104", username="mjc", key_filename=str(Path.home() / ".ssh/id_ed25519"),
                       look_for_keys=False, allow_agent=False, timeout=15)
        stdin, stdout, stderr = client.exec_command("python3 -c " + shlex.quote(REMOTE), timeout=1200)
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
