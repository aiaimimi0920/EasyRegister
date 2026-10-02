"""Apply an OpenAI-only mailbox domain pool after a guarded idle boundary."""

from __future__ import annotations

import ast
import json
import shlex
from pathlib import Path

import paramiko


ROOT = Path(__file__).resolve().parents[1]
DOMAINS = ["clay.yamiyu.pp.ua", "leaf.yamiyu.pp.ua", "flora.neuroloom.pp.ua", "lake.neuroloom.pp.ua"]

POLICY_PROBE = r'''
import json,os,sys
sys.path.insert(0,'/app/server/services/orchestration_service/src')
from others.config_runtime_sections import _parse_relaxed_mailbox_business_policy_map
key='REGISTER_MAILBOX_BUSINESS_POLICIES_JSON'
raw=os.environ.get(key,'')
try:
    policies=json.loads(raw) if raw.strip() else {}
except ValueError:
    policies=_parse_relaxed_mailbox_business_policy_map(raw)
if not isinstance(policies,dict): raise RuntimeError('business_policy_not_object')
matches=[name for name in policies if name.strip().lower()=='openai']
if len(matches)>1: raise RuntimeError('duplicate_openai_policy')
name=matches[0] if matches else 'openai'
policy=dict(policies.get(name) or {})
policy['domainPool']=DOMAINS
policies[name]=policy
value=json.dumps(policies,separators=(',',':'))
os.environ[key]=value
from others import runtime_mailbox as mailbox
pool=list(mailbox._resolve_business_mailbox_domain_pool(business_key='openai'))
state=mailbox._load_mailbox_domain_state()
allowed=all(not mailbox._mailbox_domain_is_business_blacklisted(d,state,business_key='openai')
    and d not in mailbox._resolve_mailbox_explicit_blacklist_domains(business_key='openai')
    for d in DOMAINS)
selected,reason=mailbox._select_business_mailbox_domain_for_provider('cloudflare_temp_email',business_key='openai')
print(json.dumps({'value':value,'domainPool':pool,'allowed':allowed,
    'selectionValid':selected in DOMAINS,'selectionReason':reason}))
'''

APPLY = r'''
key='REGISTER_MAILBOX_BUSINESS_POLICIES_JSON'
summary={'ok':False,'promoted':False,'rollbackAttempted':False}
stage='preflight';changed=False;before=None;spec=None
override=root/'small-success-mailbox-policy-20260911-001.yaml'
rollback=root/'small-success-mailbox-policy-rollback-20260911-001.yaml'
def env_map(items):
    return dict(item.split('=',1) for item in items or [])
def write_policy(path,value):
    require(path.parent.resolve()==root.resolve(),'override_path_invalid')
    text=json.dumps({'services':{spec['service']:{'environment':{key:value}}}},indent=2)+'\n'
    if path.exists(): require(path.read_text()==text,'override_already_differs')
    else:
        with path.open('x',encoding='utf-8') as output: output.write(text)
        path.chmod(0o600)
def rendered_env(path):
    config=json.loads(compose(spec,path,['config','--format','json']))
    service=config['services'][spec['service']]
    result=env_map(inspect(service['image'],'image')['Config'].get('Env'))
    for name,value in service.get('environment',{}).items():
        if value is None: result.pop(name,None)
        else: result[name]=str(value)
    return result,service
try:
    before=snapshot();summary['containersBefore']=before
    require(before==payload['expectedContainers'],'live_container_state_changed')
    live=inspect('easy-register');labels=live['Config']['Labels']
    require(labels['com.docker.compose.project.working_dir']==str(root),'compose_root_changed')
    spec={'container':'easy-register','project':labels['com.docker.compose.project'],
          'service':labels['com.docker.compose.service'],
          'files':labels['com.docker.compose.project.config_files'].split(',')}
    require(all(Path(p).parent.resolve()==root.resolve() for p in spec['files']),'compose_file_path_changed')
    actual=env_map(live['Config'].get('Env'))
    probe=json.loads(run(['docker','exec','-i','easy-register','python','-'],
        'DOMAINS='+repr(payload['domains'])+'\n'+payload['policyProbe']))
    require(probe['domainPool']==payload['domains'] and probe['allowed'] and probe['selectionValid'],'domain_policy_probe_failed')
    summary['policyProbe']={name:value for name,value in probe.items() if name!='value'}
    desired=dict(actual);desired[key]=probe['value']
    require(desired!=actual,'policy_already_active')
    write_policy(override,probe['value']);write_policy(rollback,actual.get(key))
    rendered,service=rendered_env(override)
    require(rendered==desired,'environment_drift')
    require(inspect(service['image'],'image')['Id']==live['Image'],'image_drift')
    require(rendered_env(rollback)[0]==actual,'rollback_environment_drift')
    mounts={(m['Destination'],m['Source'],m['RW']) for m in live.get('Mounts') or [] if m['Type']=='bind'}
    declared={(v['target'],v['source'],not v.get('read_only',False)) for v in service.get('volumes') or [] if v['type']=='bind'}
    require(mounts==declared,'mount_drift')
    image=inspect(live['Image'],'image')
    command=service.get('command') if service.get('command') is not None else image['Config'].get('Cmd')
    require(command==live['Config'].get('Cmd'),'command_drift')
    print(json.dumps({'phase':'preflight','ok':True,'businessKey':'openai','domains':payload['domains']}),flush=True)
    stage='drain';deadline=time.monotonic()+1800
    while True:
        active=active_tasks(before['easy-register']['startedAt'])
        busy=int((provider_health().get('pool') or {}).get('busyWorkers',-1))
        if active==0 and busy==0: break
        require(time.monotonic()<deadline,'drain_timeout')
        time.sleep(5)
    require(snapshot()==before,'live_state_changed_before_switch')
    summary['drained']={'activeTasks':active,'busyWorkers':busy}
    print(json.dumps({'phase':'drained','activeTasks':0,'busyWorkers':0}),flush=True)
    stage='apply_policy';changed=True
    compose(spec,override,['up','-d','--no-deps','--no-build','--pull','never',spec['service']])
    stage='health';wait_healthy()
    current=inspect('easy-register')
    require(current['Image']==before['easy-register']['imageId'],'running_image_changed')
    require(env_map(current['Config'].get('Env'))==desired,'running_environment_mismatch')
    for image_key,name in (('register','easy-register'),('provider','easy-register-protocol-python')):
        files=payload['images'][image_key]['files']
        require(image_hashes(name,files,live=True)==files,'live_source_changed')
    after=snapshot()
    for name in names[1:]: require(after[name]==before[name],'unrelated_container_changed')
    summary.update({'ok':True,'promoted':True,'businessKey':'openai','domains':payload['domains'],
        'changedEnvironmentKeys':[key],'override':str(override),'rollbackOverride':str(rollback),
        'sourceHashesVerified':True,'dashboardStatus':200})
except Exception as error:
    summary['failedStage']=stage;summary['errorType']=type(error).__name__
    if isinstance(error,RuntimeError) and re.fullmatch(r'[a-z_]+',str(error)): summary['errorCode']=str(error)
    if changed:
        summary['rollbackAttempted']=True
        try:
            compose(spec,rollback,['up','-d','--no-deps','--no-build','--pull','never',spec['service']])
            wait_healthy();summary['rollbackCompleted']=True
        except Exception: summary['rollbackCompleted']=False
finally:
    summary['containersAfter']=snapshot();summary['capturedAt']=datetime.now(timezone.utc).isoformat()
    print(json.dumps({'phase':'complete',**summary}),flush=True)
sys.exit(0 if summary['ok'] else 1)
'''


def remote_program() -> str:
    source = (ROOT / "scripts/promote-pc2-auth-boundary.py").read_text(encoding="utf-8")
    remote = next(ast.literal_eval(node.value) for node in ast.parse(source).body
                  if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "REMOTE" for target in node.targets))
    names = {"run", "require", "inspect", "snapshot", "compose", "image_hashes", "provider_health", "active_tasks", "wait_healthy"}
    helpers = [ast.get_source_segment(remote, node) for node in ast.parse(remote).body
               if isinstance(node, ast.FunctionDef) and node.name in names]
    if len(helpers) != len(names):
        raise RuntimeError("promotion_helpers_changed")
    header = "\n".join([
        "import json,re,subprocess,sys,time,urllib.request",
        "from datetime import datetime,timezone", "from pathlib import Path",
        "payload=json.load(sys.stdin)", "root=Path('/home/mjc/easyregister')",
        "names=['easy-register','easy-register-protocol-python','easy-register-protocol','easy-register-sms']",
    ])
    program = "\n\n".join([header, *helpers, APPLY])
    compile(program, "mailbox-policy-promotion", "exec")
    compile("DOMAINS=[]\n" + POLICY_PROBE, "mailbox-policy-probe", "exec")
    return program


def main() -> int:
    evidence = json.loads((ROOT / "deploy-evidence/pc2-otp-response-promotion-20260911-002.json").read_text(encoding="utf-8"))
    if not evidence.get("ok"):
        raise RuntimeError("baseline_evidence_invalid")
    payload = {"expectedContainers": evidence["containersAfter"], "images": evidence["images"],
               "domains": DOMAINS, "policyProbe": POLICY_PROBE}
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.104", username="mjc", key_filename=str(Path.home() / ".ssh/id_ed25519"),
                       look_for_keys=False, allow_agent=False, timeout=15)
        stdin, stdout, stderr = client.exec_command("python3 -c " + shlex.quote(remote_program()), timeout=2400)
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
