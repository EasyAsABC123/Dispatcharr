"""Captured row-field, queue option/lineage, and explicit header-gap review."""
import datetime,hashlib,json,sys,uuid
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr');out=Path(sys.argv[1]);refs=json.loads((out/'core51-config/definition/native-binding.json').read_bytes())['Source51'];checks=[];header_gaps=[];task_cases=[]
def add(value,label):checks.append({'passed':bool(value),'label':label})
def u(value):
 try:return str(uuid.UUID(value))==value
 except (ValueError,TypeError,AttributeError):return False
for group,reference in refs.items():
 p=ROOT/reference['path'];assert hashlib.sha256(p.read_bytes()).hexdigest()==reference['sha256'];source=json.loads(p.read_bytes());native=json.loads((out/'core51'/(group+'-Main-private.json')).read_bytes())
 for a,b in zip(source['requests'],native['requests']):
  case=str(b['id']);sb,sa=a['before'],a['after'];nb,na=b['before'],b['after']
  for table,old in sb['SQL']['named11']['tables'].items():
   if old['raw_rows']==sa['SQL']['named11']['tables'][table]['raw_rows']:add(nb['SQL']['named11']['tables'][table]['raw_rows']==na['SQL']['named11']['tables'][table]['raw_rows'],'NamedTableLiteralNoMutation:'+case+':'+table)
  source_notices={x['id']:x for x in sb['SQL']['notices']};native_notices={x['id']:x for x in nb['SQL']['notices']}
  for s,n in zip(sa['SQL']['notices'],na['SQL']['notices']):
   before=source_notices.get(s['id']);own=native_notices.get(n['id'])
   if before is None:
    created=datetime.datetime.fromisoformat(n['created_at']).timestamp();add(b['began_epoch']<=created<=b['completed_epoch'],'NewNoticeCreatedOwnRequestClock:'+case)
   elif before['updated_at']==s['updated_at']:add(own is not None and own['updated_at']==n['updated_at'],'NoticeUnchangedUpdatedClock:'+case)
  prior=lambda snap:{x['raw_frame_sha256']:x for x in snap['broker']['frames']}
  src_before,nat_before=prior(sb),prior(nb);left=[v for k,v in prior(sa).items()if k not in src_before];right=[v for k,v in prior(na).items()if k not in nat_before]
  add(len(left)==len(right),'ExactNewTaskCount:'+case)
  for s,n in zip(left,right):
   task_cases.append(b['id']);sh,nh=s['headers'],n['headers'];sp,np=s['properties'],n['properties'];tag=case+':'+sh['task']
   semantic=['lang','task','shadow','eta','expires','group','group_index','retries','timelimit','parent_id','ignore_result']
   add(all(k in sh and k in nh and sh[k]==nh[k]for k in semantic),'TaskNameSchedulingLineageOptions:'+tag)
   add(s['decoded_body'][2]==n['decoded_body'][2],'TaskCallbacksErrbacksChainChord:'+tag)
   add(all(sh[k]==sh['id']for k in ['root_id'])and sp['correlation_id']==sh['id']and all(nh[k]==nh['id']for k in ['root_id'])and np['correlation_id']==nh['id']and u(sh['id'])and u(nh['id']),'DeclaredOwnTaskIDRootCorrelationBindings:'+tag)
   add(all(k in sp and k in np and sp[k]==np[k]for k in ['delivery_mode','delivery_info','priority','body_encoding']),'TaskDeliveryOptionsExact:'+tag)
   # Reply/delivery IDs are diagnostic epoch metadata here: no extra binding is
   # invented to turn full header/property byte inequality into a pass.
   header_gaps.append({'case':b['id'],'task':sh['task'],'missing_Source_header_fields':sorted(set(sh)-set(nh)),'extra_Main_header_fields':sorted(set(nh)-set(sh)),'reply_to_UUID_versions':{'Source':uuid.UUID(sp['reply_to']).version if u(sp['reply_to'])else None,'Main':uuid.UUID(np['reply_to']).version if u(np['reply_to'])else None},'delivery_tag_valid_UUIDs':u(sp['delivery_tag'])and u(np['delivery_tag']),'full_header_property_parity_claimed':False})
result={'passed':all(x['passed']for x in checks),'predicates':len(checks),'failed':[x for x in checks if not x['passed']],'checks':checks,'task_publication_cases':task_cases,'full_transport_header_property_parity':False,'explicit_transport_metadata_gaps':header_gaps,'scope':'Literal stable named-table rows, notice own clock fields; actual task name/scheduling/lineage options, callback-chain-chord body, declared own task-id/root/correlation ties and delivery options; complements the retained args/kwargs/token+raw Redis and exact Channels/file review','applications_replayed':0,'full_M5_M8_accepted':False};(out/'core51/Main-readonly-row-task-review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'passed':result['passed'],'predicates':len(checks),'failed':result['failed'],'metadata_gaps':header_gaps}));sys.exit(0 if result['passed']else 2)
