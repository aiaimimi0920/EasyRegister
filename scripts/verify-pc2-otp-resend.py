"""Build and verify an isolated OTP resend candidate from the running PC2 images."""

from __future__ import annotations

import ast
import importlib.util
import json
import shlex
import textwrap
from pathlib import Path

import paramiko


ROOT = Path(__file__).resolve().parents[1]
PROTOCOL_ROOT = ROOT.parent / "EasyProtocol"
VERSION = "20260911-001"
PROVIDER_ROOT = PROTOCOL_ROOT / "providers/python"


def load_builder():
    spec = importlib.util.spec_from_file_location("auth_boundary_builder", ROOT / "scripts/verify-pc2-auth-boundary.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("builder_module_missing")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def function_source(source: str, name: str) -> str:
    node = next(node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == name)
    return ast.get_source_segment(source, node) or ""


def test_probe(path: Path, name: str) -> str:
    return "\n".join([
        "import contextlib, io, json, types, unittest",
        f"module = types.ModuleType({name!r})",
        f"module.__file__ = '/tmp/{name}.py'",
        f"exec(compile({path.read_text(encoding='utf-8')!r}, module.__file__, 'exec'), module.__dict__)",
        "suite = unittest.defaultTestLoader.loadTestsFromModule(module)",
        "output = io.StringIO()",
        "with contextlib.redirect_stdout(output):",
        "    result = unittest.TextTestRunner(stream=output).run(suite)",
        "print(json.dumps({'ok': result.wasSuccessful(), 'testsRun': result.testsRun,",
        "    'failures': [test.id() for test, _ in result.failures],",
        "    'errors': [test.id() for test, _ in result.errors]}))",
        "raise SystemExit(0 if result.wasSuccessful() else 1)",
    ])


PATCH_REMOTE = r'''
def patch_source(text, entry):
    functions={node.name:node for node in ast.parse(text).body if isinstance(node,ast.FunctionDef)}
    lines=text.splitlines(keepends=True)
    if entry['patchKind']=='request':
        node=functions['_session_request']
        before=''.join(lines[node.lineno-1:node.end_lineno]).strip()
        require(hashlib.sha256(before.encode()).hexdigest()==entry['functionBeforeHash'],'request_function_changed')
        replacement=before
        for old,new in entry['edits']:
            require(replacement.count(old)==1,'request_edit_anchor_changed')
            replacement=replacement.replace(old,new,1)
        lines[node.lineno-1:node.end_lineno]=[replacement+'\n']
    elif entry['patchKind']=='passwordless':
        node=functions['_prepare_passwordless_email_otp']
        before=ast.get_source_segment(text,node)
        require(hashlib.sha256(before.encode()).hexdigest()==entry['prepareBeforeHash'],'prepare_function_changed')
        require('_resend_passwordless_email_otp' not in functions,'resend_function_exists')
        edits=[(node.lineno-1,node.end_lineno,entry['prepare']+'\n\n\n'+entry['resend']+'\n')]
        matches=[]
        for call in ast.walk(functions['run_protocol_small_success_once']):
            if not isinstance(call,ast.Call) or not isinstance(call.func,ast.Name) or call.func.id!='wait_openai_code':
                continue
            if any(key.arg=='min_mail_id' and isinstance(key.value,ast.Name) and key.value.id=='otp_min_mail_id' for key in call.keywords):
                matches.append(call)
        require(len(matches)==1,'passwordless_wait_call_changed')
        call=matches[0]
        require(not any(key.arg=='resend_callback' for key in call.keywords),'resend_callback_exists')
        closing=lines[call.end_lineno-1]
        indent=closing[:len(closing)-len(closing.lstrip())]+'    '
        callback=''.join(indent+line+'\n' for line in entry['callback'].splitlines())
        edits.append((call.end_lineno-1,call.end_lineno-1,callback))
        for start,end,replacement in sorted(edits,reverse=True):
            lines[start:end]=[replacement]
        text=''.join(lines)
        imports=[node for node in ast.parse(text).body if isinstance(node,ast.ImportFrom)
                 and node.module=='protocol_runtime.protocol_register']
        require(len(imports)==1,'otp_import_changed')
        imported={alias.name for alias in imports[0].names}
        missing=[name for name in ('EMAIL_OTP_SEND_URL','PROTOCOL_ENABLE_EMAIL_OTP_SEND_ENV') if name not in imported]
        lines=text.splitlines(keepends=True)
        lines[imports[0].end_lineno-1:imports[0].end_lineno-1]=['    '+name+',\n' for name in missing]
        text=''.join(lines)
        parsed=ast.parse(text)
        callbacks=[key for node in ast.walk(parsed) if isinstance(node,ast.Call) and isinstance(node.func,ast.Name)
                   and node.func.id=='wait_openai_code' for key in node.keywords if key.arg=='resend_callback']
        require(len(callbacks)==1,'callback_count_changed')
        expected=ast.parse('wait_openai_code(\n'+entry['callback']+'\n)').body[0].value.keywords[0]
        require(ast.dump(callbacks[0])==ast.dump(expected),'callback_wiring_changed')
        return text
    else:
        raise RuntimeError('unknown_patch_kind')
    return ''.join(lines)
'''


BUILD_REMOTE = r'''
before=None
stage='snapshot'
try:
    before=snapshot();summary['containersBefore']=before
    for key,spec in payload['specs'].items():
        stage='build_'+key
        require(before[spec['container']]['imageId']==spec['baseId'],'live_image_changed')
        files={}
        for entry in spec['files']:
            original=read_source(spec['container'],entry['path'])
            require(hashlib.sha256(original.encode()).hexdigest()==entry['beforeHash'],'live_source_changed')
            source=patch_source(original,entry) if entry.get('patchKind') else entry['source']
            compile(source,entry['path'],'exec')
            files[entry['path']]=source
        image=build(spec,files)
        image['files']={**spec['preservedFiles'],**image['files']}
        summary['images'][key]=image
        print(json.dumps({'phase':'image_built','kind':key,**image}),flush=True)
    stage='offline_probes'
    provider=probe(summary['images']['provider']['id'],payload['providerProbe'])
    register=probe(summary['images']['register']['id'],'PROVIDER_ERROR = '+repr(provider['error'])+'\n'+payload['registerProbe'])
    summary['providerProbe']={key:value for key,value in provider.items() if key!='error'}
    summary['registerProbe']=register
    summary['otpProbe']=probe(summary['images']['provider']['id'],payload['otpProbe'])
    summary['retryProbe']=probe(summary['images']['register']['id'],payload['retryProbe'])
    require(all(summary[key]['ok'] for key in ('providerProbe','registerProbe','otpProbe','retryProbe')),'offline_probe_not_ok')
    summary['ok']=True
except Exception as error:
    summary['failedStage']=stage;summary['errorType']=type(error).__name__
    if isinstance(error,RuntimeError) and re.fullmatch(r'[A-Za-z_]+',str(error)):
        summary['errorCode']=str(error)
finally:
    summary['containersAfter']=snapshot()
    summary['productionUnchanged']=before==summary['containersAfter']
    summary['ok']=bool(summary['ok'] and summary['productionUnchanged'])
    summary['capturedAt']=datetime.now(timezone.utc).isoformat()
    print(json.dumps({'phase':'complete',**summary}),flush=True)
sys.exit(0 if summary['ok'] else 1)
'''


def remote_program(builder) -> str:
    names = {"docker", "require", "inspect", "snapshot", "read_source", "build", "probe"}
    helpers = [ast.get_source_segment(builder.REMOTE, node) for node in ast.parse(builder.REMOTE).body
               if isinstance(node, ast.FunctionDef) and node.name in names]
    if len(helpers) != len(names):
        raise RuntimeError("build_helpers_changed")
    header = "\n".join([
        "import ast, hashlib, io, json, re, subprocess, sys, tarfile",
        "from datetime import datetime, timezone",
        "payload=json.load(sys.stdin)",
        "summary={'ok':False,'productionPromoted':False,'images':{},'scope':'offline OTP resend validation'}",
        "names=['easy-register','easy-register-protocol-python','easy-register-protocol','easy-register-sms']",
    ])
    source = "\n\n".join([header, *helpers, PATCH_REMOTE, BUILD_REMOTE])
    compile(source, "otp-resend-candidate", "exec")
    return source


def main() -> int:
    builder = load_builder()
    previous = json.loads((ROOT / "deploy-evidence/pc2-auth-boundary-candidates-20260911-003.json").read_text(encoding="utf-8"))
    flow = (PROVIDER_ROOT / "src/new_protocol_register/protocol_small_success.py").read_text(encoding="utf-8")
    callback = next(key for node in ast.walk(ast.parse(flow)) if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name) and node.func.id == "wait_openai_code"
                    for key in node.keywords if key.arg == "resend_callback")
    callback_source = textwrap.dedent("\n".join(flow.splitlines()[callback.lineno-1:callback.end_lineno]))
    provider_files = [
        {"path": "/app/src/protocol_runtime/protocol_register.py", "patchKind": "request",
         "beforeHash": "1c3d8019a1fcb91ce73b61440f4c4d6df8966bec9421d084c752f4839457494b",
         "functionBeforeHash": "f26d62b84b34a16b5913f9836bef32da58f9901b72412aad857006ccd93940e1",
         "edits": [
             ["    request_label: str,\n", "    request_label: str,\n    allow_transport_fallback: bool = True,\n"],
             ["if not is_tls_transport_failure:", "if not allow_transport_fallback or not is_tls_transport_failure:"],
         ]},
        {"path": "/app/src/new_protocol_register/protocol_small_success.py", "patchKind": "passwordless",
         "beforeHash": previous["images"]["provider"]["files"]["/app/src/new_protocol_register/protocol_small_success.py"],
         "prepareBeforeHash": "b835c6862c414e561c57f6c08d62dde4a471fd899e1e5ad6868aa7d2826c8622",
         "prepare": function_source(flow, "_prepare_passwordless_email_otp"),
         "resend": function_source(flow, "_resend_passwordless_email_otp"),
         "callback": callback_source},
        {"path": "/app/python_shared/src/shared_mailbox/easy_email_client.py",
         "beforeHash": "1c35b459bbe31ec8763d7ed5711d83ff2e95e541acc047744231121c67c4420d",
         "source": (PROVIDER_ROOT / "python_shared/src/shared_mailbox/easy_email_client.py").read_text(encoding="utf-8")},
    ]
    relative = "server/services/orchestration_service/src/others/dst_flow_runtime.py"
    register_files = [{"path": "/app/" + relative,
                       "beforeHash": previous["images"]["register"]["files"]["/app/" + relative],
                       "source": (ROOT / relative).read_text(encoding="utf-8")}]
    payload = {
        "providerProbe": builder.PROVIDER_PROBE, "registerProbe": builder.REGISTER_PROBE,
        "otpProbe": test_probe(PROTOCOL_ROOT / "tests/test_otp_resend_recovery.py", "otp_resend_tests"),
        "retryProbe": test_probe(ROOT / "tests/test_protocol_error_boundary.py", "retry_boundary_tests"),
        "specs": {},
    }
    for key, container, repository, files in (
        ("provider", "easy-register-protocol-python", "easy-protocol-python", provider_files),
        ("register", "easy-register", "easy-register", register_files),
    ):
        base = previous["images"][key]
        payload["specs"][key] = {
            "container": container, "base": base["tag"], "baseId": base["id"],
            "candidate": f"easy-register/{repository}:pc2-otp-resend-{VERSION}",
            "preservedFiles": base["files"], "files": files,
        }
    remote = remote_program(builder)
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.104", username="mjc", key_filename=str(Path.home() / ".ssh/id_ed25519"),
                       look_for_keys=False, allow_agent=False, timeout=15)
        stdin, stdout, stderr = client.exec_command("python3 -c " + shlex.quote(remote), timeout=1200)
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
