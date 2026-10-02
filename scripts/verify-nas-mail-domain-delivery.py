"""Test owned mail delivery on a failing recipient domain and the primary domain."""
from __future__ import annotations

import argparse
import json
import shlex
import sys

import paramiko


VERIFY = r'''
import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { loadEasyEmailServiceRuntimeConfigFromEnvironment } from './dist/src/runtime/config.js';
import { CloudflareTempEmailCreateClient, decodeCloudflareTempMailboxRef } from './dist/src/providers/cloudflare_temp_email/connector/client.js';
const config=await loadEasyEmailServiceRuntimeConfigFromEnvironment();
const opened=[],cleanup=[];
let stage='snapshot';
const result={ok:false,runs:[]};
const hash=value=>createHash('sha256').update(value).digest('hex').slice(0,16);
async function request(path,body){
  const response=await fetch('http://127.0.0.1:8080'+path,{
    method:body===undefined?'GET':'POST',
    headers:{Authorization:'Bearer '+config.apiKey,...(body===undefined?{}:{'Content-Type':'application/json'})},
    ...(body===undefined?{}:{body:JSON.stringify(body)}),signal:AbortSignal.timeout(35000),
  });
  if(response.status!==200)throw new Error('http_'+response.status);
  return await response.json();
}
async function open(domain,instanceId){
  const response=await request('/mail/mailboxes/open',{
    hostId:'self-owned-domain-delivery-20260911',providerTypeKey:'cloudflare_temp_email',
    provisionMode:'reuse-only',bindingMode:'shared-instance',ttlMinutes:10,requestedDomain:domain,
    metadata:{source:'self-owned-domain-delivery-fixture'},
  });
  const session=response.result.session;opened.push(session);
  assert.equal(session.providerInstanceId,instanceId);
  assert.equal(session.emailAddress.split('@')[1].toLowerCase(),domain.toLowerCase());
  return session;
}
try{
  const {snapshot}=await request('/mail/snapshot');
  const {sessions}=await request('/mail/query/mailbox-sessions?newestFirst=false');
  const reference=sessions.find(s=>hash(s.id)===REFERENCE_HASH);
  assert.ok(reference,'reference_session_missing');
  const instance=snapshot.instances.find(i=>i.id===reference.providerInstanceId);
  assert.ok(instance);
  const client=CloudflareTempEmailCreateClient.fromInstance(instance);assert.ok(client);
  const target=reference.emailAddress.split('@')[1].toLowerCase();
  const primary=String(instance.metadata.domain).toLowerCase();
  result.referenceSessionHash=REFERENCE_HASH;result.recipientDomains=[...new Set([target,primary])];
  const {messages:baseline}=await request('/mail/query/observed-messages?sync=false');
  stage='open-sender';const sender=await open('tx-mail.aiaimimi.com',instance.id);
  for(const domain of result.recipientDomains){
    stage='open-recipient';const recipient=await open(domain,instance.id);
    const mailbox=decodeCloudflareTempMailboxRef(recipient.mailboxRef,instance.id);assert.ok(mailbox);
    const marker='owned-domain-check-'+randomUUID();const expected='482601';
    stage='send';await request('/mail/mailboxes/send',{
      sessionId:sender.id,toEmailAddress:recipient.emailAddress,fromName:'Diagnostic',
      subject:'Self-owned verification fixture '+expected,
      textBody:'This is an owned delivery test. Verification code: '+expected+'. Marker: '+marker,
    });
    stage='receive';let delivered=false,codeMatched=false,bodyMatched=false,upstreamCount=0;
    const deadline=Date.now()+90000;
    while(Date.now()<deadline){
      const mails=await client.listMails(mailbox.jwt);upstreamCount=mails.length;
      if(upstreamCount>0){
        delivered=true;
        const {code}=await request('/mail/mailboxes/'+encodeURIComponent(recipient.id)+'/code');
        codeMatched=code?.code===expected;
        if(codeMatched){
          const {messages}=await request('/mail/query/observed-messages?sync=false');
          bodyMatched=messages.some(m=>m.sessionId===recipient.id&&
            (String(m.textBody??'')+' '+String(m.htmlBody??'')).includes(marker));
          if(bodyMatched)break;
        }
      }
      await new Promise(resolve=>setTimeout(resolve,1500));
    }
    result.runs.push({domain,sessionHash:hash(recipient.id),delivered,upstreamCount,codeMatched,bodyMatched});
  }
  const {messages:after}=await request('/mail/query/observed-messages?sync=false');
  const byId=new Map(after.map(m=>[m.id,m]));
  result.preservedExistingMessages=baseline.every(m=>isDeepStrictEqual(m,byId.get(m.id)));
  result.baselineMessageCount=baseline.length;
  result.ok=result.preservedExistingMessages&&result.runs.every(r=>r.delivered&&r.codeMatched&&r.bodyMatched);
}catch(error){
  result.failedStage=stage;result.errorType=error?.name??'unknown';
  const code=/^http_(\d{3})$/.exec(error instanceof Error?error.message:'');
  if(code)result.httpStatus=Number(code[1]);
}finally{
  for(const session of opened.reverse()){
    try{
      const {result:released}=await request('/mail/mailboxes/release',{
        sessionId:session.id,reason:'self-owned-domain-delivery-complete',
      });
      cleanup.push({sessionHash:hash(session.id),released:released?.released===true,status:released?.session?.status});
    }catch(error){cleanup.push({sessionHash:hash(session.id),released:false,errorType:error?.name??'unknown'});}
  }
  result.cleanup=cleanup;result.ok=Boolean(result.ok&&cleanup.length===opened.length&&cleanup.every(c=>c.released));
  result.capturedAt=new Date().toISOString();console.log(JSON.stringify(result));
}
if(!result.ok)process.exitCode=1;
'''


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-session-hash",required=True)
    args=parser.parse_args()
    password=sys.stdin.readline().lstrip("\ufeff").rstrip("\r\n")
    if not password:raise RuntimeError("Previously authorized NAS credential required on stdin")
    client=paramiko.SSHClient();client.load_system_host_keys();client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect("192.168.15.200",username="mjc",password=password,look_for_keys=False,allow_agent=False,timeout=15)
        code="const REFERENCE_HASH = "+json.dumps(args.reference_session_hash)+";\n"+VERIFY
        command="sudo -S -p '' /usr/local/bin/docker exec easyemail-sdk node --input-type=module -e "+shlex.quote(code)
        stdin,stdout,stderr=client.exec_command(command,timeout=360)
        stdin.write(password+"\n");stdin.flush();stdin.channel.shutdown_write()
        output=stdout.read().decode("utf-8");errors=stderr.read();status=stdout.channel.recv_exit_status()
        if output.strip():print(json.dumps(json.loads(output),indent=2))
        else:print(json.dumps({"ok":False,"remoteExit":status,"stderrPresent":bool(errors)}))
        return status
    finally:client.close()


if __name__=="__main__":
    raise SystemExit(main())
