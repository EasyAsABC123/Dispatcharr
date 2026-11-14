"""Scoped actual production Main Core51 comparison, without peer control claims."""
import hashlib,json
from pathlib import Path
from gpg_trace import need

def content(headers):return next((v for k,v in headers.items()if k.lower()=='content-type'),None)
def changed(a,b):return sorted(k for k in a if a[k]!=b[k])
def frame_ids(value):return{r['raw_frame_sha256']for r in value['frames']}
def channel_ids(value):return{r['raw_sha256']for r in value['frames']}
def qualify(out):
    base=Path('/home/justin/github/Dispatcharr');binding=json.loads((out/'core51-config/definition/native-binding.json').read_bytes())['Source51'];plan=json.loads((out/'core51-config/definition/plan.json').read_bytes());rows=[];task_cases=[];channel_cases=[];requests=0
    for cohort in plan['cohorts']:
        group=cohort['group'];ref=binding[group];path=base/ref['path'];need(hashlib.sha256(path.read_bytes()).hexdigest()==ref['sha256'],'RetainedSource51ExactReference');source=json.loads(path.read_bytes());native=json.loads((out/'core51'/(group+'-Main-private.json')).read_bytes());need(source['capture_valid']and native['capture_valid'],'BothWholeCaptures');need(len(source['requests'])==len(native['requests'])==len(cohort['case_ids']),'ExactlyFixedGroupSubset')
        for original,actual in zip(source['requests'],native['requests']):
            checks={'fixed_case_id':original['id']==actual['id'],'literal_request_body':original['request_body_hex']==actual['request_body_hex'],'fixed_method':original['method']==actual['method'],'actual_TCP_source':actual['actual_TCP_source_ip']==plan['auth']['roles'][actual['role']]['REMOTE_ADDR'],'HTTP_status':original['status']==actual['status'],'literal_response_body':original['response_body_hex']==actual['response_body_hex'],'content_type':content(original['headers'])==content(actual['headers']),'actual_socket_close':'physical_close'in actual and 'exception'not in actual and 'close_exception'not in actual}
            old,new=original['before']['SQL']['full6259'],original['after']['SQL']['full6259'];before,after=actual['before']['SQL']['full6259'],actual['after']['SQL']['full6259'];checks['SQL_changed_table_set']=changed(old['tables'],new['tables'])==changed(before['tables'],after['tables']);checks['SQL_changed_sequence_set']=changed(old['sequences'],new['sequences'])==changed(before['sequences'],after['sequences']);checks['SQL_schema_unchanged']=all(before[k]==after[k]for k in ['columns','indexes','constraints'])
            task_delta=frame_ids(actual['after']['broker'])-frame_ids(actual['before']['broker']);ref_delta=frame_ids(original['after']['broker'])-frame_ids(original['before']['broker']);checks['actual_unconsumed_task_delta_count']=len(task_delta)==len(ref_delta)
            chan_delta=channel_ids(actual['after']['Channels'])-channel_ids(actual['before']['Channels']);ref_chan=channel_ids(original['after']['Channels'])-channel_ids(original['before']['Channels']);checks['actual_unconsumed_Channel_delta_count']=len(chan_delta)==len(ref_chan)
            if task_delta:task_cases.append(actual['id'])
            if chan_delta:channel_cases.append(actual['id'])
            rows.append({'case':actual['id'],'group':group,'status':actual['status'],'source_status':original['status'],'checks':checks,'passed':all(checks.values())});requests+=1
    checks={'all51_actual_production_Main_requests':requests==51,'all51_scoped_predicates':all(r['passed']for r in rows),'exact_five_producer_cases':task_cases==[38,46,47,49,50],'exact_three_Channel_cases':channel_cases==[11,15,18]}
    result={'passed':all(checks.values()),'checks':checks,'cases':rows,'actual_requests':requests,'actual_task_publication_cases':task_cases,'actual_Channel_publication_cases':channel_cases,'Source_API_calls':0,'peer_private_control_requests':0,'scope':'Literal production Main HTTP and source-socket, SQL/schema change sets, unconsumed task/Channels delta counts; raw snapshots retained','not_claimed':['Prior290 peer private-state/physical controls','Source local proxy/timezone state as Native','Full SQL-row-content normalization or raw frame semantic/private membership recomputation','Adversarial or late-reply drains','Full M5/M8 acceptance','Performance']};(out/'core51/Main-scoped-review.json').write_text(json.dumps(result,indent=2)+'\n');return result
