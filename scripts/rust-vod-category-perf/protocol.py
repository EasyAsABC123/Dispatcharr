"""Pure category experiment protocol; importing never starts workloads.
Original Dispatcharr authors retain application credit and AGPL attribution.
The fixed budget/fixture spec exists before binding canonical image34.
"""
import copy,datetime,hashlib,json,math,re
from pathlib import Path
SPEC=json.loads(Path(__file__).with_name('policy.json').read_text())['policy']
PATHS=SPEC['workloads'][0]['paths']
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(value):return hashlib.sha256(canonical(value)).hexdigest()
def validate_manifest(manifest,revision):
 if manifest.get('passed')is not True or not re.fullmatch('[0-9a-f]{40}',revision)or manifest.get('git_revision')!=revision:raise ValueError('Exact committed passing canonical revision required')
 for key in ['source_tree_sha256','source_archive_sha256']:
  if not re.fullmatch('[0-9a-f]{64}',manifest.get(key,'')):raise ValueError('Exact source/archive identity required')
 if not re.fullmatch('sha256:[0-9a-f]{64}',manifest.get('image_id','')):raise ValueError('Exact image digest required')
def bind(manifest,revision,path):
 """Call only after reviewed image34 functional gates and before sampling."""
 validate_manifest(manifest,revision);policy=copy.deepcopy(SPEC)
 if policy['candidate_binding']['image_id']is not None:raise ValueError('Draft policy binding was changed')
 policy['candidate_binding']={'status':'bound before candidate sampling; fresh Root cues still required','image_id':manifest['image_id'],'source_tree_sha256':manifest['source_tree_sha256'],'source_archive_sha256':manifest['source_archive_sha256'],'git_revision':revision,'manifest_canonical_sha256':digest(manifest)}
 value={'spec_sha256':digest(SPEC),'policy':policy,'policy_sha256':digest(policy),'measurements_started':False}
 with Path(path).open('x')as stream:stream.write(json.dumps(value,indent=2)+'\n')
 return value
def identity(record,manifest):
 policy=record['policy'];binding=policy['candidate_binding'];validate_manifest(manifest,binding['git_revision'])
 expected=copy.deepcopy(policy);expected['candidate_binding']=copy.deepcopy(SPEC['candidate_binding'])
 if expected!=SPEC or record.get('spec_sha256')!=digest(SPEC)or record.get('policy_sha256')!=digest(policy)or record.get('measurements_started')is not False:raise ValueError('Declared spec or bound policy changed')
 if any(binding[key]!=manifest[source]for key,source in [('image_id','image_id'),('source_tree_sha256','source_tree_sha256'),('source_archive_sha256','source_archive_sha256')])or binding.get('manifest_canonical_sha256')!=digest(manifest):raise ValueError('Candidate manifest differs from declared binding')
def select_period(record,workload,rows):
 policy=record['policy']
 if workload not in policy['workloads']or len(rows)!=4 or{row.get('period_seconds')for row in rows}!=set(workload['period_candidates_seconds']):raise ValueError('All declared Source-only candidates required')
 feasible=[]
 for row in rows:
  if row.get('backend')!='source'or any('candidate'in key or'native'in key for key in row):raise ValueError('Native values cannot select pacing')
  for key in ['period_seconds','pacing_fraction','cpu_percent_mean','memory_fraction','throttled_period_fraction']:
   if type(row.get(key))not in [int,float]or not math.isfinite(row[key])or row[key]<0:raise ValueError('Invalid Source qualification counters')
  limits=policy['qualification']
  if row.get('errors')==0 and row.get('correct_responses')is True and row['pacing_fraction']>=limits['min_pacing_fraction']and row['cpu_percent_mean']<=limits['max_backend_cpu_percent_mean']and row['memory_fraction']<=limits['max_backend_memory_fraction']and row['throttled_period_fraction']<=limits['max_throttled_period_fraction']:feasible.append(row['period_seconds'])
 if not feasible:raise ValueError('Original cannot sustain declared workload')
 return min(feasible)
def authorize(path,record,manifest,scope,plan_sha256=None):
 identity(record,manifest)
 if scope not in ['vod-category-source-qualification','vod-category-resource-measurement']:raise ValueError('Unknown coordination scope')
 raw=Path(path).read_bytes()
 if len(raw)>16384:raise ValueError('Authorization byte bound')
 value=json.loads(raw);expiry=datetime.datetime.fromisoformat(value['expires_utc']);binding=record['policy']['candidate_binding']
 if value.get('authorized')is not True or value.get('scope')!=scope or value.get('candidate_image')!=binding['image_id']or value.get('candidate_manifest_canonical_sha256')!=digest(manifest)or value.get('policy_sha256')!=record['policy_sha256']or value.get('boot_id')!=Path('/proc/sys/kernel/random/boot_id').read_text().strip()or expiry.tzinfo is None or expiry<=datetime.datetime.now(datetime.timezone.utc):raise ValueError('Fresh matching parent quiet authorization required')
 if scope=='vod-category-resource-measurement'and(not plan_sha256 or value.get('plan_sha256')!=plan_sha256):raise ValueError('Frozen Source-selected plan required')
 return hashlib.sha256(raw).hexdigest()

def freeze(path,record,qualification,manifest,fixture):
 identity(record,manifest);policy=record['policy']
 if set(qualification)!={work['name']for work in policy['workloads']}:raise ValueError('Missing Source workload qualification')
 periods={work['name']:select_period(record,work,qualification[work['name']])for work in policy['workloads']}
 value={'binding':record,'candidate_manifest_sha256':digest(manifest),'policy_sha256':record['policy_sha256'],'qualification_sha256':digest(qualification),'selected_periods':periods,'fixture':fixture,'measurements_started':False}
 value['plan_sha256']=digest(value)
 with Path(path).open('x')as stream:stream.write(json.dumps(value,indent=2)+'\n')
 return value
def validate_plan(plan,record,qualification,manifest,fixture):
 identity(record,manifest);value=dict(plan);claimed=value.pop('plan_sha256',None)
 if claimed!=digest(value)or value.get('binding')!=record or value.get('candidate_manifest_sha256')!=digest(manifest)or value.get('qualification_sha256')!=digest(qualification)or value.get('fixture')!=fixture or value.get('measurements_started')is not False:raise ValueError('Frozen category plan changed')
 for work in record['policy']['workloads']:
  if value['selected_periods'].get(work['name'])!=select_period(record,work,qualification[work['name']]):raise ValueError('Source pacing changed')
 return claimed
