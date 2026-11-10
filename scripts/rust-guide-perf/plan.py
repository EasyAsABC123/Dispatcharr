#!/usr/bin/env python3
"""Proposed stored-guide experiment policy; no execution or gain claim.
Original Dispatcharr authors retain full application credit and AGPL licensing.
"""
import datetime,hashlib,json,math
from pathlib import Path
SOURCE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE='sha256:0844627cf65913ddea6a3bc01fbf8bdd596055735765cac6bd593921b620f8e2'
TREE='59d5db446ffc4eaa3c68c20ecc552c98d6725aa2beb788ffddba89762e5a1c82'
ARCHIVE='5fa1034dd2407cca5e733338576f88f2bbf71b8663d06a0d7d66bc4ff5b51bb2'
POLICY={
 'format_version':1,'scope':'Stored-guide Current channel branch and XMLTV export only; not browser or full migration acceptance',
 'historical_candidate_image':IMAGE,'candidate_source_tree':TREE,'original_revision':SOURCE,
 'fixture':{'channels':500,'shared_epg_data':50,'channels_per_epg':10,'current_duration_hours':4,'future_half_hour_programmes_per_epg':44,'stored_programmes':2250,'expected_current_rows':500,'expected_xmltv_channels':500,'expected_xmltv_programmes':22500,'window_days':1,'window_prev_days':0,'source_kind':'xmltv','dummy_sources':0,'remote_fetches':0,'fixture_max_age_seconds':3600},
 'workloads':[{'name':'current-500-shared50','kind':'json','method':'POST','path':'/api/epg/current-programs/','users':10,'payload':'explicit500 channel_uuids; actual UUIDs private fixture only','cache_state':'not a content-cached endpoint','period_candidates_seconds':[10,5,2.5,1.25]},*({'name':f'xmltv-{state}-{users}','kind':'xml','method':'GET','path':'/output/epg?days=1&prev_days=0','users':users,'cache_state':state,'period_candidates_seconds':[10,5,2,1]if state=='hot'else[10,15,20]}for state in ['cold','hot']for users in [1,10])],
 'host_header':'guide-perf.invalid:8088','accept_encoding':'identity','client_read_chunk_bytes':65536,'max_json_bytes':4<<20,'max_xml_bytes':32<<20,'request_timeout_seconds':10,
 'warmup_seconds':15,'measurement_seconds':60,'trials':3,'orders':[['source','candidate'],['candidate','source'],['source','candidate']],'sample_seconds':1,
 'service_limits':{'backend':{'memory_bytes':512*1024**2,'nano_cpus':2*10**9},'infrastructure':{'memory_bytes':1024**3,'nano_cpus':10**9},'origin':{'memory_bytes':1024**3,'nano_cpus':2*10**9}},
 'selected_services':{'source':['baseline','origin','source'],'candidate':['baseline','origin','candidate']},
 'qualification':{'source_only':True,'warmup_seconds':5,'duration_seconds':15,'max_errors':0,'min_pacing_fraction':.95,'max_backend_cpu_percent_mean':170,'max_backend_memory_fraction':.90,'max_throttled_period_fraction':.05,'selection':'highest feasible fixed rate; cold chooses smallest feasible epoch period; no candidate values are consulted'},
 'budgets':{'max_cpu_ratio':1.0,'max_memory_ratio':1.0,'max_ttfb_p95_ratio':1.10,'ttfb_floor_ms':50,'max_completion_p95_ratio':1.10,'min_throughput_ratio':.95,'min_pacing_fraction':.95,'max_errors':0,'minimum_samples':50,'max_oom_kills':0,'max_counter_resets':0},
 'reset':'Stop both web backends; clear only disposable Redis; same committed fixture/schema, source web only in separate2CPU512MiB cgroup; start one backend, inspect actual limits/image/bootID. No concurrent measurements.',
 'hot':'Generate and fully verify one export outside timing. Preserve original300s sliding TTL; before/after ready/status/list byte counts must prove cache-ready and no generation change. No TTL patch.',
 'cold':'Barriered waves of1/10 complete simultaneous consumers. Before each wave, delete only the four captured XML cache-entry keys after previous wave fully finished. Leave grouped settings/auth caches warm. Do not mutate programmes or generation during a read.',
 'correctness':'Exact typed JSON/order on every Current response; XML status/MIME, complete byte length and SHA256 on every export, initial out-of-timing XML channel/programme count parse. Fixed Host and identity encoding. All500 current rows must stay active through matrix.',
 'acceptance':'All3 pairs of every workload must meet every predeclared gate; retain invalid/failed trials, never retune budgets after candidate results. Medians are supplementary, not substitutes. No measurements until a fresh root quiet authorization.'
}
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(value):return hashlib.sha256(canonical(value)).hexdigest()
def select_period(workload,rows):
 expected=workload['period_candidates_seconds']
 if len(rows)!=len(expected)or{row.get('period_seconds')for row in rows}!=set(expected):raise ValueError('Incomplete source-only qualification grid')
 feasible=[]
 for row in rows:
  if row.get('backend')!='source'or any(key in row for key in ['candidate','candidate_metrics','native_metrics']):raise ValueError('Candidate results cannot choose pacing')
  values=[row.get(key)for key in ['period_seconds','pacing_fraction','cpu_percent_mean','memory_fraction','throttled_period_fraction']]
  if any(type(v)not in [int,float]or not math.isfinite(v)for v in values):raise ValueError('Invalid qualification numeric field')
  if row.get('errors')==0 and row.get('correct_responses')is True and row['pacing_fraction']>=.95 and row['cpu_percent_mean']<=170 and row['memory_fraction']<=.9 and row['throttled_period_fraction']<=.05:feasible.append(row['period_seconds'])
 if not feasible:raise ValueError('Source cannot sustain any declared pacing; no matrix acceptance')
 return min(feasible)
def freeze(path,qualification,manifest,fixture_binding=None):
 if manifest.get('passed')is not True or manifest.get('source_tree_sha256')!=TREE or manifest.get('source_archive_sha256')!=ARCHIVE:raise ValueError('Exact configured source tree/archive required')
 if set(qualification)!={work['name']for work in POLICY['workloads']}:raise ValueError('Missing workload qualification')
 periods={work['name']:select_period(work,qualification[work['name']])for work in POLICY['workloads']}
 record={'candidate_image':manifest['image_id'],'candidate_manifest_canonical_sha256':digest(manifest),'policy':POLICY,'policy_sha256':digest(POLICY),'source_qualification_sha256':digest(qualification),'selected_periods_seconds':periods,'measurements_started':False}
 if fixture_binding is not None:record['fixture_binding']=fixture_binding
 record['plan_sha256']=digest(record)
 Path(path).write_text(json.dumps(record,indent=2)+'\n');return record
def validate_frozen(record,qualification,manifest):
 value=dict(record);claimed=value.pop('plan_sha256',None)
 if claimed!=digest(value)or value.get('policy')!=POLICY or value.get('policy_sha256')!=digest(POLICY)or value.get('measurements_started')is not False:raise ValueError('Frozen policy changed')
 if value.get('candidate_image')!=manifest.get('image_id')or value.get('candidate_manifest_canonical_sha256')!=digest(manifest)or manifest.get('passed')is not True or manifest.get('source_tree_sha256')!=TREE or manifest.get('source_archive_sha256')!=ARCHIVE:raise ValueError('Candidate image/source identity changed')
 if value.get('source_qualification_sha256')!=digest(qualification):raise ValueError('Source qualification changed')
 for work in POLICY['workloads']:
  if value['selected_periods_seconds'].get(work['name'])!=select_period(work,qualification[work['name']]):raise ValueError('Pacing changed after qualification')
 return claimed

def authorize_quiet(path,plan_sha256,image_id):
 raw=Path(path).read_bytes();value=json.loads(raw);expiry=datetime.datetime.fromisoformat(value['expires_utc']);boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
 if value.get('scope')!='guide-resource-measurement'or value.get('authorized')is not True or value.get('candidate_image')!=image_id or value.get('plan_sha256')!=plan_sha256 or value.get('boot_id')!=boot or expiry.tzinfo is None or datetime.datetime.now(datetime.timezone.utc)>=expiry:raise ValueError('Fresh matching coordinated quiet authorization required')
 return hashlib.sha256(raw).hexdigest()
