"""Typed metadata12 projection; no application imports, network or Source replay.
Original Dispatcharr and dependency authors retain implementation credit.
"""
import base64,copy,datetime,hashlib,json,re,uuid
from pathlib import Path
LABELS=('anonymous-preview','standard-preview','trusted-preview','untrusted-preview','wrong-SAN-preview','redirect-preview','blocked-loopback-preview','mixed-DNS-preview','create','refresh','disable','delete')
TABLES={'accounts_user','core_coresettings','core_notificationdismissal','core_systemevent','core_systemnotification','plugins_pluginconfig','plugins_pluginrepo'}
SECRET_FIELDS=('username','password','api_key')
def load(p):return json.loads(Path(p).read_text())
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def timestamp(value):
 assert type(value)is str
 d=datetime.datetime.fromisoformat(value.replace('Z','+00:00'));assert d.tzinfo is not None and d.utcoffset()==datetime.timedelta(0);return d

def server_time(cache):
 sec,usec=cache['0']['server_time'];assert type(sec)is int and type(usec)is int and 0<=usec<1000000
 return datetime.datetime.fromtimestamp(sec,datetime.timezone.utc)+datetime.timedelta(microseconds=usec)

def receiver(proof,ws,fixture,used):
 name=proof['receiver'];m=re.fullmatch(r'specific\.aggregate_([a-f0-9]{32})!receiver',name);assert m and uuid.UUID(hex=m[1]).version==4 and name not in used;used.add(name)
 assert proof['group']=='updates' and proof['returned']=={'actual_group_add_completed':True,'receiver':name}
 receipt=Path(proof['actual_receipt']['path']).resolve();stdout=Path(proof['actual_stdout']['path']).resolve()
 assert receipt.parent==fixture.resolve() and stdout.parent==(fixture/'private').resolve() and receipt.stem==stdout.stem
 assert sha(receipt)==proof['actual_receipt']['sha256']and sha(stdout)==proof['actual_stdout']['sha256']
 r=load(receipt);assert r['actual_exit']==0 and r['failure']is None and r['stdout_EOF']and r['stderr_EOF']and r['process_group_settled']and not r['output_bytes_discarded']
 raw=base64.b64decode(proof['raw_stdout_base64'],validate=True);assert raw==stdout.read_bytes()and json.loads(raw)==proof['returned']
 assert ws=={'actual_receiver':name,'events':[],'raw_events_base64':[]}

def project(case_dir,config=None):
 case_dir=Path(case_dir);private=case_dir/'private';fixture=case_dir.parent;setup=load(private/'setup.json');config=config or setup['config'];initial=setup['SQL'];assert set(initial['tables'])==TABLES and len(initial['sequences'])==7
 assert config['actual_origin_and_DNS_started']and config['TLS_verification_enabled']and config['empty_plugins_manifests']and not config['proxy_selected']and not config['public_or_forwarded_network_contact']and not config['plugin_install_or_execution_enabled']
 # Existing startup_hooks runs after prepare_context's setup snapshot. Its
 # exact declared fence delta must be checked, never hidden as initial entropy.
 prepared=copy.deepcopy(initial);ready=load(private/'anonymous-preview-SQL-before.json');expected=copy.deepcopy(prepared)
 expected['tables']['core_coresettings'].append({'id':19,'key':'plugin_repo_settings','name':'Plugin Repository Settings','value':{'refresh_interval_hours':0}})
 expected['sequences']['public.core_coresettings_id_seq']={'last_value':19,'is_called':True}
 expected['tables']['plugins_pluginrepo'][0]['enabled']=False
 notices=ready['tables']['core_systemnotification'];assert len(notices)==1
 notice_time=notices[0]['updated_at'];assert timestamp(prepared['tables']['core_systemnotification'][0]['updated_at'])<timestamp(notice_time)<=server_time(load(private/'anonymous-preview-cache-before.json'))
 expected['tables']['core_systemnotification'][0]['updated_at']=notice_time
 assert canonical(ready)==canonical(expected),'ExactDeclaredStartupFenceDelta'
 initial=ready
 network=load(config['owned_receipts']['network_scope']['path'])
 assert sha(config['owned_receipts']['network_scope']['path'])==config['owned_receipts']['network_scope']['sha256']
 actual_network=network['actual_network'];assert len(actual_network)==1 and actual_network[0]['Internal']is True
 subnet=actual_network[0]['IPAM']['Config'][0]['Subnet'];assert subnet in('11.254.249.0/29','11.254.252.0/29')
 base=subnet.removesuffix('.0/29');assert network['expected_DNS_TLS_IP']==base+'.4'and network['expected_baseline_IP']==base+'.2'
 # Bootstrap credentials are explicit seed entropy, not model updates. Every raw
 # occurrence must equal initial before tokenization; username/role namespace is fixed.
 users=initial['tables']['accounts_user'];assert [r['id']for r in users]==[1,2,3,4]
 roles=('admin','standard','viewer','inactive');levels=(10,1,0,10)
 for row,role,level in zip(users,roles,levels):
  assert row['username']=='rust-probe-'+role and row['user_level']==level
  assert re.fullmatch(r'pbkdf2_sha256\$[1-9][0-9]*\$[A-Za-z0-9]+\$[A-Za-z0-9+/]+=*',row['password'])
  assert re.fullmatch(r'[A-Za-z0-9_-]{43}',row['api_key'])
 clocks={};initial_tokens={}
 for table,fields in {'accounts_user':('date_joined','last_login'),'core_systemnotification':('created_at','updated_at'),'plugins_pluginrepo':('created_at','updated_at')}.items():
  for row in initial['tables'][table]:
   for field in fields:
    if row[field]is not None:timestamp(row[field]);initial_tokens[(table,row['id'],field)]=row[field]
 initial_seq=initial['sequences'];assert initial_seq['public.plugins_pluginrepo_id_seq']=={'last_value':1,'is_called':True}
 assert len(initial['tables']['plugins_pluginrepo'])==1 and initial['tables']['plugins_pluginrepo'][0]['id']==1
 previous=initial;used=set();phases=[];created=None;current_clocks={}
 for label in LABELS:
  before=load(private/(label+'-SQL-before.json'));after=load(private/(label+'-SQL-after.json'));assert canonical(before)==canonical(previous),'RawSQLChain:'+label
  assert set(after['tables'])==TABLES and set(after['sequences'])==set(initial_seq)
  for table in TABLES-{'plugins_pluginrepo'}:assert canonical(before['tables'][table])==canonical(after['tables'][table])==canonical(initial['tables'][table]),'UnrelatedTable:'+table
  assert before['tables']['plugins_pluginrepo'][0]==after['tables']['plugins_pluginrepo'][0]==initial['tables']['plugins_pluginrepo'][0]
  cb=load(private/(label+'-cache-before.json'));ca=load(private/(label+'-cache-after.json'));lo,hi=server_time(cb),server_time(ca);assert lo<hi
  for key,value in after['sequences'].items():
   expected={'last_value':2,'is_called':True}if key=='public.plugins_pluginrepo_id_seq'and LABELS.index(label)>=8 else initial_seq[key]
   assert type(value['last_value'])is int and type(value['is_called'])is bool and canonical(value)==canonical(expected),'ExactSequence:'+label+':'+key
  if label=='create':
   assert len(before['tables']['plugins_pluginrepo'])==1 and len(after['tables']['plugins_pluginrepo'])==2
   row=after['tables']['plugins_pluginrepo'][1];assert row['id']==2
   created=row['created_at'];assert lo<=timestamp(created)<timestamp(row['last_fetched'])<timestamp(row['updated_at'])<=hi
   current_clocks={k:row[k]for k in('created_at','last_fetched','updated_at')}
   clocks.update({v:'@repository2.'+k+'.create'for k,v in current_clocks.items()})
  elif label=='refresh':
   assert len(before['tables']['plugins_pluginrepo'])==len(after['tables']['plugins_pluginrepo'])==2
   old,row=before['tables']['plugins_pluginrepo'][1],after['tables']['plugins_pluginrepo'][1];assert row['created_at']==created and old['created_at']==created
   assert lo<=timestamp(row['last_fetched'])<timestamp(row['updated_at'])<=hi and timestamp(row['last_fetched'])>timestamp(old['last_fetched'])and timestamp(row['updated_at'])>timestamp(old['updated_at'])
   assert {k:v for k,v in row.items()if k not in('last_fetched','updated_at')}=={k:v for k,v in old.items()if k not in('last_fetched','updated_at')}
   clocks.update({row[k]:'@repository2.'+k+'.refresh'for k in('last_fetched','updated_at')});current_clocks={k:row[k]for k in current_clocks}
  elif label=='delete':assert len(before['tables']['plugins_pluginrepo'])==2 and after['tables']['plugins_pluginrepo']==initial['tables']['plugins_pluginrepo']
  else:assert canonical(before)==canonical(after),'UnexpectedSQLMutation:'+label
  wire=load(private/(label+'-wire.json'));assert not wire['body_truncated'];raw=base64.b64decode(wire['body_base64'],validate=True);body=json.loads(raw)if raw else None
  headers={k.lower():v for k,v in wire['headers']};assert len(headers)==len(wire['headers'])and int(headers['content-length'])==len(raw)and headers['content-type']=='application/json';headers.pop('content-length')
  request=load(private/(label+'-request.json'));auth=request['headers'].get('Authorization');assert (auth is None)==(label=='anonymous-preview')
  if auth is not None:assert auth.startswith('Bearer ')and len(auth.split('.'))==3
  if label in('create','refresh'):
   row=after['tables']['plugins_pluginrepo'][1]
   for key in('created_at','last_fetched','updated_at'):assert timestamp(body[key])==timestamp(row[key]);body[key]=clocks[row[key]]
  proof=load(private/(label+'-receiver-proof.json'));ws=load(private/(label+'-WS.json'));receiver(proof,ws,fixture,used)
  journal=load(private/(label+'-journal.json'));assert journal['publications']==[]and journal['recording_recovery_offset_publications']==[]and journal['strict_Beat_equality_relaxed']is False
  term=journal['terminal'];assert term['database_filter']==1 and term['observer_error']is None and term['terminal_reader_error']is None and all(term[k]for k in('terminal_barrier_fsynced','terminal_barrier_observed','terminal_marker_read','thread_settled','writer_closed'))
  stop=journal['observer_stop'];assert stop['actual_exit']==0 and stop['actual_wait_completed']and stop['shutdown_settled']and stop['all_original_process_generations_absent']and stop['stdout_EOF']and stop['stderr_EOF']
  def sql_project(value):
   value=copy.deepcopy(value)
   for table,rows in value['tables'].items():
    for row in rows:
     if table=='accounts_user':
      seed=users[row['id']-1]
      for key in SECRET_FIELDS:assert row[key]==seed[key];row[key]='@seed-user-'+str(row['id'])+'.'+key
     for key,v in list(row.items()):
      token=(table,row['id'],key)
      if token in initial_tokens:assert v==initial_tokens[token];row[key]='@initial.'+table+'.'+str(row['id'])+'.'+key
      if table=='plugins_pluginrepo'and row['id']==2 and key in('created_at','last_fetched','updated_at'):assert v in clocks;row[key]=clocks[v]
     if table=='core_coresettings'and row['key']=='network_access':
      # Only the actual inspected owned transport CIDR differs between fixture networks.
      text=row['value']['STREAMS'];parts=text.split(',');assert parts.count(subnet)==1;row['value']['STREAMS']=','.join('@owned-fixture-CIDR'if p==subnet else p for p in parts)
   return value
  phases.append({'stage':label,'method':request['method'],'path':request['path'],'request_body_base64':request['body_base64'],'credential_role':None if label=='anonymous-preview'else'standard'if label=='standard-preview'else'admin','status':wire['status'],'headers':headers,'body':body,'SQL_before':sql_project(before),'SQL_after':sql_project(after),'WS_events':[],'producer_publications':[]})
  previous=after
 return {'declared_startup_delta':{'CoreSettings_row19_plugin_repo_settings_added':True,'CoreSettings_sequence_before':18,'CoreSettings_sequence_after':19,'official_repository_enabled_before':True,'official_repository_enabled_after':False,'existing_notification_updated_causally_before_first_request':True},'scope':'Metadata12 exact selected HTTP/SQL/publications/WS; Redis cache and worker-poll timing diagnostic only','transport':transport(fixture,config),'phases':phases,'actual_requests':12,'all7_selected_sequences_exact':True,'all_raw_SQL_chains_exact':True,'owned_receiver_proofs':12,'cache_equivalence_qualified':False,'plugin_install_actions_GPG_qualified':False}

def first_difference(left,right,path='$'):
 if type(left)is not type(right):return path+':type'
 if isinstance(left,dict):
  if set(left)!=set(right):return path+':keys'
  for key in sorted(left):
   diff=first_difference(left[key],right[key],path+'.'+str(key))
   if diff:return diff
 elif isinstance(left,list):
  if len(left)!=len(right):return path+':length'
  for i,(a,b)in enumerate(zip(left,right)):
   diff=first_difference(a,b,path+'['+str(i)+']')
   if diff:return diff
 elif left!=right:return path
 return None

def compare_current_Main(f,case_dir,oracle):
 for ref in oracle.values():assert sha(ref['path'])==ref['sha256']
 actual=project(case_dir);expected=load(oracle['oracle']['path']);diff=first_difference(actual,expected);assert diff is None,'StrictSourceMetadata12Projection:'+str(diff)
 # Prove current seeded secrets match the actual Native fixture before entropy mapping.
 initial=load(Path(case_dir)/'private/setup.json')['SQL']['tables']['accounts_user']
 for row,role in zip(initial,('admin','standard','viewer','inactive')):
  seeded=f.accounts[role];assert row['username']==seeded['username']and row['api_key']==seeded['api_key']
  algorithm,iterations,salt,digest=row['password'].split('$');assert algorithm=='pbkdf2_sha256'
  got=base64.b64encode(hashlib.pbkdf2_hmac('sha256',seeded['password'].encode(),salt.encode(),int(iterations))).decode();assert got==digest
 return {'passed':True,'actual_projection_sha256':hashlib.sha256(canonical(actual)).hexdigest(),'Source_projection_sha256':hashlib.sha256(canonical(expected)).hexdigest(),'actual_compared_requests':12,'all7_selected_sequences_exact':True,'credential_stability_checked_before_mapping':True,'current_fixture_receivers_verified':True,'cache_equivalence_qualified':False,'install_actions_GPG_or_performance_qualified':False}

def transport(fixture,config):
    """Exact declared DNS answer domain and successful request method/target/headers.
    Connection IDs, UDP transaction IDs, process IDs, TLS key bytes and poll counts
    are retained diagnostic facts rather than cross-runtime identity assertions.
    """
    fixture=Path(fixture);root=fixture/'plugin-transport-evidence/servers'
    network=load(config['owned_receipts']['network_scope']['path']);ip=network['expected_DNS_TLS_IP'];source=network['expected_baseline_IP']
    dns=[json.loads(line)for line in(root/'dns/queries.jsonl').read_text().splitlines()]
    seen=set();allowed={'trusted','untrusted','mismatch','loopback','mixed'}
    for row in dns:
        assert row['kind']in('ingress','response-planned')and row['peer'][0]==source
        if row['kind']=='response-planned':
            host=row['name'].removesuffix('.plugin-fixture.test');assert host in allowed and row['name']==host+'.plugin-fixture.test'and row['qtype']in(1,28)
            answer=[]if row['qtype']==28 else['127.0.0.1']if host=='loopback'else[ip,'127.0.0.1']if host=='mixed'else[ip]
            assert row['addresses']==answer;seen.add(host)
    assert seen==allowed
    events=[json.loads(line)for line in(root/'http/events.jsonl').read_text().splitlines()];requests=[];sni=[]
    for row in events:
        if row['kind']=='accepted':assert row['peer'][0]==source and row['port']==8443
        elif row['kind']=='sni':
            assert row['requested_name']in{n+'.plugin-fixture.test'for n in('trusted','untrusted','mismatch')};assert row['selected_certificate']==row['requested_name'].split('.')[0];sni.append(row['requested_name'])
        elif row['kind']=='request':
            raw=base64.b64decode(row['raw_b64'],validate=True);lines=raw.decode('ascii').split('\r\n');assert lines[-2:]==['',''];method,target,version=lines[0].split(' ')
            assert method=='GET'and version=='HTTP/1.1'and target==row['target']
            headers={k.lower():v.strip()for line in lines[1:-2]for k,v in[line.split(':',1)]};assert len(headers)==len(lines[1:-2])
            requests.append({'method':method,'target':target,'headers':headers,'response_sha256':row['response_sha256']})
    assert len(requests)==5 and sni.count('trusted.plugin-fixture.test')==5 and sni.count('untrusted.plugin-fixture.test')==sni.count('mismatch.plugin-fixture.test')==1
    return {'successful_requests':requests,'DNS_answer_domains_and_mixed_loopback_rejection_validated':True,'TLS_SNI_certificate_split_validated':True,'raw_DNS_transaction_connection_timing_bytes_retained_not_cross_runtime_identity':True}
