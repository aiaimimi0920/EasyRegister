"""Read the actual upstream inbox for a currently waiting OTP, without logging secrets."""
from __future__ import annotations

import argparse
import json
import shlex
from pathlib import Path

import paramiko


INBOX = r'''
import contextlib, hashlib, io, json, re, urllib.error, urllib.request
from datetime import datetime, timezone
from email import policy
from email.parser import Parser
from urllib.parse import unquote, urlsplit
from shared_mailbox import easy_email_client as client
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None
def get(path):
    with contextlib.redirect_stdout(io.StringIO()):
        return client._get_json(path)
sessions=get('/mail/query/mailbox-sessions?newestFirst=false').get('sessions',[])
session=next((s for s in sessions if s.get('id')==SESSION_ID),None)
if not session or session.get('status')!='open':
    print(json.dumps({'retryObservation':True,'reason':'session_not_open'}));raise SystemExit(0)
snapshot=get('/mail/snapshot').get('snapshot',{})
instance=next((i for i in snapshot.get('instances',[]) if i.get('id')==session['providerInstanceId']),None)
if not instance:raise RuntimeError('instance_missing')
metadata=instance.get('metadata') or {}
base=metadata.get('baseUrl') or instance.get('connectionRef')
custom=metadata.get('customAuth')
parsed=urlsplit(base) if isinstance(base,str) else None
if not parsed or parsed.scheme!='https' or parsed.hostname!='mail.aiaimimi.com' or parsed.query or parsed.fragment:
    print(json.dumps({'ok':False,'reason':'unverified_upstream_origin'}));raise SystemExit(0)
if custom and (not isinstance(custom,str) or 'redacted' in custom.lower() or set(custom)=={'*'}):
    print(json.dumps({'ok':False,'reason':'upstream_credential_unavailable'}));raise SystemExit(0)
prefix='cloudflare_temp_email:'+session['providerInstanceId']+':'
ref=str(session.get('mailboxRef') or '')
if not ref.startswith(prefix):raise RuntimeError('mailbox_reference_mismatch')
mailbox=json.loads(unquote(ref[len(prefix):]))
if str(mailbox.get('address') or '').lower()!=session['emailAddress'].lower():
    raise RuntimeError('mailbox_address_mismatch')
headers={'Accept':'application/json, text/plain, */*','Authorization':'Bearer '+mailbox['jwt']}
if custom:headers['x-custom-auth']=custom
request=urllib.request.Request(base.rstrip('/')+'/api/mails?limit=20&offset=0',headers=headers)
try:
    with urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect()).open(request,timeout=25) as response:
        status=response.status;body=json.loads(response.read())
except urllib.error.HTTPError as error:
    raw=error.read(2048).decode('utf-8',errors='replace')
    secrets=[mailbox['jwt'],custom,metadata.get('adminAuth'),session['emailAddress']]
    for secret in secrets:
        if isinstance(secret,str) and secret:raw=raw.replace(secret,'[REDACTED_SECRET]')
    def scrub(value,depth=0):
        if depth>3:return '[truncated]'
        if isinstance(value,dict):return {k:scrub(v,depth+1) for k,v in value.items() if k in ('error','message','detail','code','status','type','success')}
        if isinstance(value,str):
            for secret in secrets:
                if isinstance(secret,str) and secret:value=value.replace(secret,'[REDACTED_SECRET]')
            value=re.sub(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}','[REDACTED_EMAIL]',value)
            value=re.sub(r'[A-Za-z0-9_+/=-]{32,}','[REDACTED_VALUE]',value)
            return value[:400]
        return value if isinstance(value,(bool,int,float,type(None))) else type(value).__name__
    try:
        detail=json.loads(raw)
        message=detail.get('error') or detail.get('message') if isinstance(detail,dict) else ''
        error_fields=scrub(detail)
    except ValueError:
        message=raw.strip()
        error_fields={}
    safe=message if isinstance(message,str) and re.fullmatch(r'[A-Za-z_ .:!\-]{1,160}',message) else None
    print(json.dumps({'ok':False,'stage':'upstream_list','httpStatus':error.code,'safeError':safe,
        'cfChallenge':error.headers.get('cf-mitigated')=='challenge',
        'htmlResponse':'<html' in raw.lower(),'jsonResponse':raw.lstrip().startswith('{'),
        'errorFields':error_fields}));raise SystemExit(0)
except urllib.error.URLError as error:
    print(json.dumps({'ok':False,'stage':'upstream_list','errorType':type(error).__name__,
                     'reasonType':type(error.reason).__name__}));raise SystemExit(0)
items=body if isinstance(body,list) else body.get('results',body.get('emails',body.get('data',[])))
messages=[]
for item in items if isinstance(items,list) else []:
    if not isinstance(item,dict):continue
    raw=str(item.get('raw') or '')
    subject=str(item.get('subject') or '')
    envelope=str(item.get('source') or item.get('from') or '')
    parsed_mail=Parser(policy=policy.default).parsestr(raw)
    texts=[]
    for part in parsed_mail.walk():
        if part.get_content_type() not in ('text/plain','text/html'):continue
        try:
            content=part.get_content()
            if isinstance(content,str):texts.append(content)
        except Exception:pass
    text='\n'.join(texts)
    author=str(parsed_mail.get('From') or '')
    recipient=str(parsed_mail.get('To') or '')
    messages.append({'fieldNames':sorted(item),'rawLength':len(raw),'subjectLength':len(subject),
        'envelopeMentionsOpenAI':'openai' in envelope.lower(),'authorMentionsOpenAI':'openai' in author.lower(),
        'subjectMentionsOpenAI':'openai' in (subject+' '+str(parsed_mail.get('Subject') or '')).lower(),
        'recipientMatches':session['emailAddress'].lower() in recipient.lower(),
        'decodedTextLength':len(text),'decodedSixDigitCandidates':len(re.findall(r'(?<!\d)\d{6}(?!\d)',text)),
        'mimeTypes':sorted({part.get_content_type() for part in parsed_mail.walk()})})
after=get('/mail/query/mailbox-sessions?newestFirst=false').get('sessions',[])
after_session=next((s for s in after if s.get('id')==SESSION_ID),None)
if not after_session or after_session.get('status')!='open':
    print(json.dumps({'retryObservation':True,'reason':'session_changed_during_probe'}));raise SystemExit(0)
observed=get('/mail/query/observed-messages?sync=false&newestFirst=false').get('messages',[])
print(json.dumps({'ok':True,'capturedAt':datetime.now(timezone.utc).isoformat(),'taskIndex':TASK_INDEX,
    'taskStartedAt':TASK_START,'sessionHash':hashlib.sha256(SESSION_ID.encode()).hexdigest()[:16],
    'sessionCreatedAt':session['createdAt'],'sessionExpiresAt':session.get('expiresAt'),
    'recipientDomain':session['emailAddress'].rsplit('@',1)[-1],'minMailIdFloor':FLOOR,
    'upstreamStatus':status,'upstreamRootType':type(body).__name__,
    'upstreamFieldNames':sorted(body) if isinstance(body,dict) else [],
    'upstreamMessageCount':len(items) if isinstance(items,list) else None,
    'storedMessageCount':sum(m.get('sessionId')==SESSION_ID for m in observed),'messages':messages}))
'''

REMOTE = r'''
import json,re,subprocess,sys,time
payload=json.load(sys.stdin)
def run(args,source=None):
    p=subprocess.run(args,input=source,capture_output=True,text=True,timeout=80)
    if p.returncode:raise RuntimeError('read_command_failed')
    return p.stdout
deadline=time.monotonic()+payload['waitSeconds']
while True:
    container=json.loads(run(['docker','inspect','easy-register']))[0]
    active={}
    for line in run(['docker','logs','--since',container['State']['StartedAt'],'easy-register']).splitlines():
        offset=line.find('{')
        if offset<0:continue
        try:record=json.loads(line[offset:])
        except ValueError:continue
        if record.get('event')=='register_run_started':active[record['taskIndex']]=record['startedAt']
        elif record.get('event')=='register_run_finished':active.pop(record['taskIndex'],None)
    if active:
        task,start=list(active.items())[-1]
        logs=run(['docker','logs','--since',start,'easy-register-protocol-python'])
        polls=re.findall(r'\[mailbox\] wait_openai_code[^\r\n]*?session_id=([^\s\\]+)[^\r\n]*?min_mail_id_floor=(\d+)',logs)
        if polls:
            source='SESSION_ID = '+repr(polls[-1][0])+'\nTASK_INDEX = '+repr(task)+'\nTASK_START = '+repr(start)+'\nFLOOR = '+polls[-1][1]+'\n'+payload['source']
            result=json.loads(run(['docker','exec','-i','easy-register-protocol-python','python','-'],source))
            if not result.get('retryObservation'):
                print(json.dumps(result));sys.exit(0 if result.get('ok') else 1)
    if time.monotonic()>=deadline:
        print(json.dumps({'ok':False,'reason':'no_open_otp_session_in_window'}));sys.exit(2)
    time.sleep(10)
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait-seconds", type=int, default=0)
    args = parser.parse_args()
    wait = max(0, min(600, args.wait_seconds))
    compile(INBOX, "upstream-inbox-audit", "exec")
    compile(REMOTE, "otp-inbox-audit-remote", "exec")
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.104", username="mjc", key_filename=str(Path.home() / ".ssh/id_ed25519"),
                       look_for_keys=False, allow_agent=False, timeout=15)
        stdin, stdout, stderr = client.exec_command("python3 -c " + shlex.quote(REMOTE), timeout=wait+180)
        stdin.write(json.dumps({"waitSeconds":wait,"source":INBOX}));stdin.flush();stdin.channel.shutdown_write()
        output=stdout.read().decode("utf-8");errors=stderr.read();status=stdout.channel.recv_exit_status()
        if output.strip():
            print(json.dumps(json.loads(output),indent=2))
        else:
            print(json.dumps({"ok":False,"remoteExit":status,"stderrPresent":bool(errors)}))
        return status
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
