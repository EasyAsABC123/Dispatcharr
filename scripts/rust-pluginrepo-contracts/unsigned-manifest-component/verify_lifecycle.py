"""Offline actual receipts, exact origin mapping and LIMITED packet evidence."""
import datetime,gzip,hashlib,json,math
from pathlib import Path
import verify as V
ROOT=Path(__file__).resolve().parent
need=V.need;H=V.H
def load(epoch,path,json_value=True):
 need(H((ROOT/'lifecycle-policy.json').read_bytes())=='86aa6a50c26785467f588167316f771407bf3e37c595860e25e17b3183c11ff0','ExactLifecyclePolicy');row=json.loads((ROOT/'lifecycle-policy.json').read_bytes())['epochs'][epoch][path];data=(ROOT/row['file']).read_bytes();need(H(data)==row['selected_compressed_sha256'],'ExactLifecycleCompressed');raw=gzip.decompress(data);need(H(raw)==row['selected_decompressed_sha256']and len(raw)==row['selected_decompressed_bytes'],'ExactLifecycleRaw');return json.loads(raw)if json_value else raw
def instant(value):
 t=datetime.datetime.fromisoformat(value.replace('Z','+00:00'));need(t.utcoffset()==datetime.timedelta(0),'UTC');return t.timestamp()
def limited_packets(source,network):
 need(network['capture_complete']is True and network['socket_closed']is True and network['stopped_by_signal']is True and network['invalid']is None,'ActualCompleteObserver');need(type(network['observed_packets'])is int and network['kernel_received_packets']==network['observed_packets']and network['kernel_drops']==0 and network['kernel_packet_count_equals_observed']is True,'ActualKernelCounterCoverage');exceptions=network['exceptions'];need(len(exceptions)==8,'LiteralEightUnparsedExceptions');phase=instant(source['phase_started_utc'])
 for row in exceptions:
  need(type(row['epoch'])in(int,float)and math.isfinite(row['epoch'])and row['epoch']<phase,'EveryUnsupportedFrameBeforeAPIPhase');m=row['metadata'];need(m['kind']=='IPv6'and m['protocol']==0 and m['unsupported_extension_headers']is True and row['raw_payload_retained']is False,'LiteralUnsupportedPayloadNotReclassified')
 return {'limited_API_phase_only':True,'observed_and_kernel_packets':network['observed_packets'],'unsupported_payloads_UNMEASURED':8,'original_full_lifecycle_gate':'FAILED; not repaired','retrospective_MLD_or_noDNS_whole_lifecycle_claim':False}
def validate():
 summary={};pol=json.loads((ROOT/'lifecycle-policy.json').read_bytes())['epochs'];files=0
 for epoch,items in pol.items():
  for name in items:load(epoch,name,False);files+=1
  e=load(epoch,'execution-report.json');is_source=epoch.startswith('Source');expected=1 if is_source else 0;need(type(e['actual_controller_exit'])is int and e['actual_controller_exit']==expected and e['passed']is(not is_source),'LiteralActualHostOutcome');need(e.get('error')is None,'NoHiddenControllerError');a='default-before-private.json'if is_source else'default-before.json';b='default-after-private.json'if is_source else'default-after.json';need(load(epoch,a,False)==load(epoch,b,False),'DefaultFull6259And11LiteralBytes');default=load(epoch,a);need(len(default['public62']['tables'])==62 and len(default['public62']['sequences'])==59 and len(default['named11']['tables'])==len(default['named11']['sequences'])==11,'LiteralDefaultScope')
  rows=e['processes'];need(len(rows)==e['actual_command_count']and 0<=e['cleanup_elapsed_seconds']<=90,'BoundedActualCleanup');
  for i,row in enumerate(rows,1):
   need(row==load(epoch,f'command-{i:03d}.json'),'ExactCLIReceipt');exit_key='actual_exit'if'actual_exit'in row else'actual_wait_exit';need(type(row[exit_key])is int and row[exit_key]==0 and row['physically_settled']is True and row['readers_settled']is True and row['writer_settled']is True and row['invalid']is None,'ActualCLIWait0AndPipes');start=row['observed_starttime']if is_source else row['starttime'];need(type(start)is str and start.isdecimal()and int(start)>0,'ActualObservedPIDGeneration')
  originpath='origin/origin-requests.jsonl'if is_source else'origin-requests.jsonl';origin=[json.loads(line)for line in load(epoch,originpath,False).splitlines()];count=17 if epoch.endswith('24')else 1;need(len(origin)==count,'Actual17OrFresh1OriginNotOriginal18');
  for row in origin:
   body=bytes.fromhex(row['body_hex']);need(H(body)==row['body_sha256']and row['method']=='GET'and row['valid']is True,'ActualGETBodySHA')
  loop=load(epoch,'loopback/loopback-terminal.json');need(type(loop['accepted_connections'])is int and loop['accepted_connections']==0 and loop['socket_closed']is True,'ActualLoopbackZero');summary[epoch]={'actual_host_exit':expected,'CLI_waits0':len(rows),'actual_origin_requests':count,'literal_default6259_named11':True,'actual_loopback_contacts':0}
  if is_source:
   need(e['cleanup_errors']==[{'label':'actual-origin-DNS-and-loopback-observations','type':'ValueError','message':'CompletePassiveDNSExternalPacketCoverage'}],'OriginalKnownNegativeOnly');s=V.load_projection(epoch);summary[epoch]['limited_packets']=limited_packets(s,load(epoch,'network/network-observer-terminal.json'));mapping={str(row['index']):[]for row in s['requests']}
   for row in origin:
    owners=[r for r in s['requests']if instant(r['began_utc'])<=row['began_utc_epoch']<=row['completed_utc_epoch']<=instant(r['completed_utc'])];need(len(owners)==1,'EachActualOriginContactUniqueAPIWindow');mapping[str(owners[0]['index'])].append(row['path'])
   if epoch=='Source24':need(mapping['11']==[],'OldCase11DuplicateURLNotFetch404')
   summary[epoch]['actual_origin_paths_by_request']=mapping
  else:need(not e.get('cleanup_errors')and load(epoch,'owned-backends-while-ELF-after-close.json')==load(epoch,'owned-backends-after-ELF.json')==[],'ActualBothBackendAbsenceEpochs')
 return {'passed_limited_lifecycle_review':True,'exact_files':files,'epochs':summary,'fresh_current_Docker_or_PID_checks':'not performed by portable offline verifier; retained independent review receipts carry original query/generation results','Main_fullM8_performance_or_Source_TCP_claim':False}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
