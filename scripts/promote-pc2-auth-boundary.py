"""Promote the verified auth-error candidates after draining current work."""
from __future__ import annotations

import argparse
import json
import shlex
from pathlib import Path

import paramiko


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "deploy-evidence/pc2-auth-boundary-candidates-20260911-003.json"

REMOTE = r'''
import json, os, re, subprocess, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
payload=json.load(sys.stdin)
root=Path('/home/mjc/easyregister')
summary={'ok':False,'promoted':False,'rollbackAttempted':False}
stage='preflight';changed=False;specs={}
names=['easy-register','easy-register-protocol-python','easy-register-protocol','easy-register-sms']
def run(args,source=None,timeout=90):
    p=subprocess.run(args,input=source,capture_output=True,text=True,timeout=timeout)
    if p.returncode: raise RuntimeError('command_failed')
    return p.stdout
def require(ok,code):
    if not ok: raise RuntimeError(code)
def inspect(name,kind='container'):
    return json.loads(run(['docker',kind,'inspect',name]))[0]
def snapshot():
    result={}
    for name in names:
        c=inspect(name)
        result[name]={'id':c['Id'],'imageId':c['Image'],'startedAt':c['State']['StartedAt'],'running':c['State']['Running']}
    return result
def write_override(path,service,image):
    require(path.parent.resolve()==root.resolve(),'override_path_invalid')
    text=json.dumps({'services':{service:{'image':image}}},indent=2)+'\n'
    if path.exists():
        require(path.read_text()==text,'override_already_differs')
    else:
        with path.open('x',encoding='utf-8') as f: f.write(text)
        path.chmod(0o600)
def compose(spec,path,action):
    args=['docker','compose','--project-name',spec['project'],'--env-file',str(root/'runtime.env')]
    for file in spec['files']+[str(path)]: args+=['-f',file]
    return run(args+action,timeout=180)
def image_hashes(image,files,live=False):
    source='import hashlib,json\nfrom pathlib import Path\nprint(json.dumps({p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in '+repr(list(files))+'}))'
    args=['docker','exec',image,'python','-B','-c',source] if live else [
        'docker','run','--rm','--network','none','--read-only','--entrypoint','python',image,'-B','-c',source]
    return json.loads(run(args))
def provider_health():
    source="import json,os,urllib.request\nwith urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('PYTHON_PROTOCOL_PORT','11003')+'/health',timeout=5) as r: print(json.dumps(json.load(r)))"
    return json.loads(run(['docker','exec','easy-register-protocol-python','python','-c',source],timeout=15))
def active_tasks(since):
    raw=run(['docker','logs','--since',since,'easy-register'],timeout=45);active=set()
    for line in raw.splitlines():
        offset=line.find('{')
        if offset<0: continue
        try: event=json.loads(line[offset:])
        except ValueError: continue
        if event.get('event')=='register_run_started': active.add(event.get('taskIndex'))
        elif event.get('event')=='register_run_finished': active.discard(event.get('taskIndex'))
    return len(active)
def wait_healthy():
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
        try:
            require(provider_health().get('status')=='ok','provider_not_healthy')
            environment=dict(item.split('=',1) for item in inspect('easy-register')['Config'].get('Env',[]))
            request=urllib.request.Request('http://127.0.0.1:19790/api/status',
                headers={'Authorization':'Bearer '+environment.get('EASY_PROTOCOL_CONTROL_TOKEN','').strip()})
            with urllib.request.urlopen(request,timeout=5) as response:
                require(response.status==200 and isinstance(json.load(response),dict),'dashboard_not_healthy')
            return
        except Exception: time.sleep(2)
    raise RuntimeError('health_timeout')
before=None
try:
    require(payload.get('ok') is True and payload.get('productionPromoted') is False,'candidate_evidence_invalid')
    release_prefix=payload.get('releasePrefix','auth-boundary')
    require(release_prefix in ('auth-boundary','otp-resend'),'release_prefix_invalid')
    before=snapshot();summary['containersBefore']=before
    require(before==payload['containersAfter'],'live_container_state_changed')
    for key,name in [('provider','easy-register-protocol-python'),('register','easy-register')]:
        candidate=payload['images'][key];live=inspect(name);labels=live['Config']['Labels']
        require(live['Image']==before[name]['imageId'],'live_image_changed')
        image=inspect(candidate['tag'],'image');require(image['Id']==candidate['id'],'candidate_image_changed')
        require(image_hashes(candidate['id'],candidate['files'])==candidate['files'],'candidate_files_changed')
        require(labels['com.docker.compose.project.working_dir']==str(root),'compose_root_changed')
        spec={'container':name,'project':labels['com.docker.compose.project'],'service':labels['com.docker.compose.service'],
              'files':labels['com.docker.compose.project.config_files'].split(','),
              'candidate':candidate,'baseTag':live['Config']['Image'],'beforeImageId':live['Image'],
              'unchanged':live['Image']==candidate['id']}
        require(all(Path(p).parent.resolve()==root.resolve() for p in spec['files']),'compose_file_path_changed')
        if spec['unchanged']:
            specs[key]=spec
            continue
        version=candidate['tag'].rsplit(':',1)[1].removeprefix('pc2-'+release_prefix+'-')
        require(re.fullmatch(r'\d{8}-\d{3}',version) is not None,'candidate_version_invalid')
        spec['override']=root/(release_prefix+'-'+key+'-'+version+'.yaml')
        spec['rollback']=root/(release_prefix+'-'+key+'-rollback-'+version+'.yaml')
        write_override(spec['override'],spec['service'],candidate['tag'])
        write_override(spec['rollback'],spec['service'],spec['baseTag'])
        config=json.loads(compose(spec,spec['override'],['config','--format','json']))
        service=config['services'][spec['service']]
        expected=dict(item.split('=',1) for item in image['Config'].get('Env') or [])
        expected.update({k:str(v) if v is not None else '' for k,v in service.get('environment',{}).items()})
        actual=dict(item.split('=',1) for item in live['Config'].get('Env') or [])
        require(expected==actual,'environment_drift')
        mounts={(m['Destination'],m['Source'],m['RW']) for m in live.get('Mounts') or [] if m['Type']=='bind'}
        declared={(v['target'],v['source'],not v.get('read_only',False)) for v in service.get('volumes') or [] if v['type']=='bind'}
        require(mounts==declared,'mount_drift')
        expected_command=service.get('command') if service.get('command') is not None else image['Config'].get('Cmd')
        require(expected_command==live['Config'].get('Cmd'),'command_drift')
        specs[key]=spec
    stage='drain';deadline=time.monotonic()+1800
    while True:
        active=active_tasks(before['easy-register']['startedAt'])
        busy=int((provider_health().get('pool') or {}).get('busyWorkers',-1))
        if active==0 and busy==0: break
        require(time.monotonic()<deadline,'drain_timeout')
        time.sleep(5)
    summary['drained']={'activeTasks':active,'busyWorkers':busy}
    require(snapshot()==before,'live_state_changed_before_stop')
    print(json.dumps({'phase':'drained','activeTasks':0,'busyWorkers':0}),flush=True)
    stage='stop_orchestrator';changed=True
    run(['docker','stop','--time','60',before['easy-register']['id']],timeout=90)
    require(not inspect('easy-register')['State']['Running'],'orchestrator_not_stopped')
    require(int((provider_health().get('pool') or {}).get('busyWorkers',-1))==0,'provider_became_busy')
    for key in ('provider','register'):
        stage='promote_'+key;spec=specs[key]
        if spec['unchanged']:
            if key=='provider' and payload.get('restartProvider') is True:
                run(['docker','restart','--time','60',before[spec['container']]['id']],timeout=90)
                deadline=time.monotonic()+90
                while True:
                    try:
                        if provider_health().get('status')=='ok': break
                    except Exception: pass
                    require(time.monotonic()<deadline,'provider_restart_health_timeout')
                    time.sleep(2)
                summary['providerRestarted']=True
            continue
        compose(spec,spec['override'],['up','-d','--no-deps','--no-build','--pull','never',spec['service']])
        require(inspect(spec['container'])['Image']==spec['candidate']['id'],'promoted_image_mismatch')
    if specs['register']['unchanged']:
        run(['docker','start',before['easy-register']['id']])
    stage='health';wait_healthy()
    for spec in specs.values():
        require(image_hashes(spec['container'],spec['candidate']['files'],live=True)==spec['candidate']['files'],'live_files_mismatch')
    after=snapshot()
    for name in ('easy-register-protocol','easy-register-sms'):
        require(after[name]==before[name],'unrelated_container_changed')
    summary.update({'ok':True,'promoted':True,'dashboardStatus':200,
                    'images':payload['images'],'overrides':{k:str(s['override']) for k,s in specs.items() if not s['unchanged']},
                    'rollbackOverrides':{k:str(s['rollback']) for k,s in specs.items() if not s['unchanged']}})
except Exception as error:
    summary['failedStage']=stage;summary['errorType']=type(error).__name__
    if isinstance(error,RuntimeError) and re.fullmatch(r'[a-z_]+',str(error)): summary['errorCode']=str(error)
    if changed:
        summary['rollbackAttempted']=True
        try:
            for key in ('provider','register'):
                spec=specs[key]
                if spec['unchanged']: continue
                compose(spec,spec['rollback'],['up','-d','--no-deps','--no-build','--pull','never',spec['service']])
                require(inspect(spec['container'])['Image']==spec['beforeImageId'],'rollback_image_mismatch')
            if specs['register']['unchanged']:
                run(['docker','start',before['easy-register']['id']])
            wait_healthy();summary['rollbackCompleted']=True
        except Exception:
            summary['rollbackCompleted']=False
finally:
    summary['containersAfter']=snapshot();summary['capturedAt']=datetime.now(timezone.utc).isoformat()
    print(json.dumps({'phase':'complete',**summary}),flush=True)
sys.exit(0 if summary['ok'] else 1)
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, default=EVIDENCE)
    parser.add_argument("--release-prefix", choices=("auth-boundary", "otp-resend"), default="auth-boundary")
    parser.add_argument("--restart-provider", action="store_true", help="Restart an unchanged provider after draining work")
    args = parser.parse_args()
    payload = json.loads(args.evidence.read_text(encoding="utf-8"))
    payload["releasePrefix"] = args.release_prefix
    payload["restartProvider"] = args.restart_provider
    compile(REMOTE, "auth-boundary-promotion", "exec")
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.104", username="mjc", key_filename=str(Path.home() / ".ssh/id_ed25519"),
                       look_for_keys=False, allow_agent=False, timeout=15)
        stdin, stdout, stderr = client.exec_command("python3 -c " + shlex.quote(REMOTE), timeout=2400)
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
