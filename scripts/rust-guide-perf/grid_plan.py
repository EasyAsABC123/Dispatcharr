#!/usr/bin/env python3
"""Separate predeclared stored GRID experiment; original Dispatcharr authors retain credit."""
import copy,datetime,hashlib,json,math
from pathlib import Path
from plan import POLICY as BASE_POLICY
SOURCE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE='sha256:56c2d22e0f347230a7e009ae982af2ce354166c560b422be2abf0dcb57a0e3ef'
TREE='38919c0c2a59ce7f9fb3746d9332c69b81b2f417ce4e712a4ff61a2509dc989c'
ARCHIVE='220183084b0b7f9d526f8979b738b82b6983ccfddabf25e0ad964e2a2069d62a'
POLICY=copy.deepcopy(BASE_POLICY)
POLICY.update(scope='Stored React guide GRID JSON only; separate from Current/XMLTV and full migration acceptance',historical_candidate_image=IMAGE,candidate_source_tree=TREE,
 workloads=[{'name':f'grid-2250-shared50-{users}','kind':'json','method':'GET','path':'/api/epg/grid/','users':users,'query_window':'fixed seed clock minus1hour through plus24hours; encoded start/end ISO8601 UTC','cache_state':'not a content-cached endpoint','period_candidates_seconds':[10,5,2,1]}for users in [1,10]],
 correctness='Every GRID response has exact typed/order JSON matching pinned Source, envelope data2250 stored programme rows/50 EPG IDs shared by500 channels. Fixed explicit seed-clock window; no XML requests.',
 acceptance='All3 alternating pairs for each GRID users1/10 workload meet unchanged predeclared budgets. Source-only pacing selects from10/5/2/1seconds. Retain failures; no nativeCPU tuning or measurements without separate fresh root quiet authorization.')
POLICY['fixture'].update(expected_grid_rows=2250,stored_tvg_identifiers=50,grid_window_start_hours=-1,grid_window_end_hours=24)
for unused in ['hot','cold']:POLICY.pop(unused)
for unused in ['expected_current_rows','expected_xmltv_channels','expected_xmltv_programmes']:POLICY['fixture'].pop(unused)
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
 if manifest.get('image_id')!=IMAGE or manifest.get('image_id')!=IMAGE or manifest.get('passed')is not True or manifest.get('source_tree_sha256')!=TREE or manifest.get('source_archive_sha256')!=ARCHIVE:raise ValueError('Exact configured source tree/archive required')
 if set(qualification)!={work['name']for work in POLICY['workloads']}:raise ValueError('Missing workload qualification')
 periods={work['name']:select_period(work,qualification[work['name']])for work in POLICY['workloads']}
 record={'candidate_image':manifest['image_id'],'candidate_manifest_canonical_sha256':digest(manifest),'policy':POLICY,'policy_sha256':digest(POLICY),'source_qualification_sha256':digest(qualification),'selected_periods_seconds':periods,'measurements_started':False}
 if fixture_binding is not None:record['fixture_binding']=fixture_binding
 record['plan_sha256']=digest(record)
 Path(path).write_text(json.dumps(record,indent=2)+'\n');return record
def validate_frozen(record,qualification,manifest):
 value=dict(record);claimed=value.pop('plan_sha256',None)
 if claimed!=digest(value)or value.get('policy')!=POLICY or value.get('policy_sha256')!=digest(POLICY)or value.get('measurements_started')is not False:raise ValueError('Frozen policy changed')
 if value.get('candidate_image')!=manifest.get('image_id')or value.get('candidate_manifest_canonical_sha256')!=digest(manifest)or manifest.get('image_id')!=IMAGE or manifest.get('passed')is not True or manifest.get('source_tree_sha256')!=TREE or manifest.get('source_archive_sha256')!=ARCHIVE:raise ValueError('Candidate image/source identity changed')
 if value.get('source_qualification_sha256')!=digest(qualification):raise ValueError('Source qualification changed')
 for work in POLICY['workloads']:
  if value['selected_periods_seconds'].get(work['name'])!=select_period(work,qualification[work['name']]):raise ValueError('Pacing changed after qualification')
 return claimed

def authorize_quiet(path,plan_sha256,image_id):
 raw=Path(path).read_bytes();value=json.loads(raw);expiry=datetime.datetime.fromisoformat(value['expires_utc']);boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
 if value.get('scope')!='grid-resource-measurement'or value.get('authorized')is not True or value.get('candidate_image')!=image_id or value.get('plan_sha256')!=plan_sha256 or value.get('boot_id')!=boot or expiry.tzinfo is None or datetime.datetime.now(datetime.timezone.utc)>=expiry:raise ValueError('Fresh matching coordinated quiet authorization required')
 return hashlib.sha256(raw).hexdigest()
