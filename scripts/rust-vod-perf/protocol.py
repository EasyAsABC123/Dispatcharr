"""Predeclared ordinary VOD read experiment; import never starts work.
Original Dispatcharr authors retain application credit and AGPL licensing.
"""
import copy,datetime,hashlib,importlib.util,json,math
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
_spec=importlib.util.spec_from_file_location('vod_existing_guide_policy',ROOT/'scripts/rust-guide-perf/plan.py');_guide=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_guide)
IMAGE='sha256:5d4a6f84842b960a9d1b39f35d099a598149fb2e189f3ba0d8dafd0b4e48b02a'
TREE='9c8a48c474f0bdfa04b1172fd59a1657c7e9e5385c70e040a52dff645a3456e6'
ARCHIVE='0645251fb7c85d8886482157577041aa5da6b198466327c6ea13f150e82eecc0'
PATHS=['/api/vod/movies/?ordering=name&page_size=24','/api/vod/series/?ordering=name&page_size=24']
POLICY={key:copy.deepcopy(_guide.POLICY[key])for key in ['service_limits','selected_services','qualification','budgets','warmup_seconds','measurement_seconds','trials','orders','sample_seconds']}
POLICY['budgets']['minimum_samples']=60
POLICY.update(format_version=1,scope='Ordinary Movie/Series list JSON reads only; no browser, media, category/unified/action/artwork or wholeapp claim',candidate_image=IMAGE,candidate_source_tree=TREE,candidate_source_archive=ARCHIVE,
 fixture={'movies':5000,'series':5000,'episodes_per_series':10,'episodes':50000,'active_movies':4500,'active_series':4500,'logos':25,'accounts':3,'active_provider_duplicate_stride':10,'logo_stride':4,'unique_names':True,'page_size':24,'source_default_page_size':20,'source_page_cap':100,'remote_provider_requests':0,'shared_standard_account':True},
 workloads=[{'name':f'ordinary-lists-{users}','users':users,'paths':PATHS,'period_candidates_seconds':[10,5,2,1]}for users in [1,10]],
 host_header='vod-perf.invalid:8088',encoding='identity',body_limit_bytes=4<<20,request_timeout_seconds=10,
 pacing='Existing API scheduler: closed-loop per-reader period, deterministic alternating endpoint; index offset and identical source/native staggering. No catch-up bursts. Separate functional10 barrier verifies admission.',
 correctness='Every request matches exact original status/MIME/raw-byte SHA/length and parsed JSON order/types; count4500/results24 for each endpoint, unique names/explicit ordering=name. Fixed fixture across phases and fingerprint before/after.',
 reset='Stop both backends, clear only owned disposable Redis, inspect finite cgroups, force-recreate one backend. Warm grouped auth/settings caches using the same exact endpoint checks before15s warmup. No metadata/content-cache reset inside measurement.',
 observer='Unchanged rust-guide-perf observer/cgroup/docker_stats and rust-perf sampler; infrastructure+origin+one backend whole-group CPU and Docker working memory, plus components. Optional processRSS diagnostic is not summed as primary memory.',
 acceptance='All3 alternating paired trials for each1/10 virtual-reader workload,6pairs/12phases, must pass every budget. Each measured phase also requires late_completed==0. Per-endpoint latency/throughput checks cannot be hidden by aggregate results.',
 authorization='Source qualification needs fresh parent quiet cue bound to policy/manifest/boot. Freeze source-only pacing before any native resource values. Measurement needs separate fresh parent cue bound to frozen plan; no implicit launch from functional mode.',
 exclusions=['No WebSocket/frontend execution','No VOD video playback or provider refresh','No category/unified/actions/artwork','No accepted gains until whole predeclared matrix passes'])
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(value):return hashlib.sha256(canonical(value)).hexdigest()
def identity(manifest):
 if manifest.get('passed')is not True or manifest.get('image_id')!=IMAGE or manifest.get('source_tree_sha256')!=TREE or manifest.get('source_archive_sha256')!=ARCHIVE:raise ValueError('Exact passing image31 manifest required')
def select_period(workload,rows):
 if workload not in POLICY['workloads']or len(rows)!=4 or{r.get('period_seconds')for r in rows}!=set(workload['period_candidates_seconds']):raise ValueError('Complete declared source-only qualification required')
 feasible=[]
 for row in rows:
  if row.get('backend')!='source'or any('candidate'in k or'native'in k for k in row):raise ValueError('Native metrics cannot select pacing')
  if any(type(row.get(k))not in [int,float]or not math.isfinite(row[k])or row[k]<0 for k in ['period_seconds','pacing_fraction','cpu_percent_mean','memory_fraction','throttled_period_fraction']):raise ValueError('Invalid qualification counters')
  if row.get('errors')==0 and row.get('correct_responses')is True and row['pacing_fraction']>=.95 and row['cpu_percent_mean']<=170 and row['memory_fraction']<=.9 and row['throttled_period_fraction']<=.05:feasible.append(row['period_seconds'])
 if not feasible:raise ValueError('Original cannot sustain declared workload')
 return min(feasible)
def authorize(path,manifest,scope,plan_sha256=None):
 if scope not in ['vod-source-qualification','vod-resource-measurement']:raise ValueError('Unknown coordination scope')
 identity(manifest);raw=Path(path).read_bytes()
 if len(raw)>16384:raise ValueError('Authorization bound')
 value=json.loads(raw);expiry=datetime.datetime.fromisoformat(value['expires_utc'])
 if value.get('authorized')is not True or value.get('scope')!=scope or value.get('candidate_image')!=IMAGE or value.get('candidate_manifest_canonical_sha256')!=digest(manifest)or value.get('policy_sha256')!=digest(POLICY)or value.get('boot_id')!=Path('/proc/sys/kernel/random/boot_id').read_text().strip()or expiry.tzinfo is None or expiry<=datetime.datetime.now(datetime.timezone.utc):raise ValueError('Fresh matching parent quiet cue required')
 if scope=='vod-resource-measurement'and(not plan_sha256 or value.get('plan_sha256')!=plan_sha256):raise ValueError('Frozen source-selected plan required')
 return hashlib.sha256(raw).hexdigest()

def freeze(path,qualification,manifest,fixture):
 identity(manifest)
 if set(qualification)!={work['name']for work in POLICY['workloads']}:raise ValueError('Missing source qualification')
 periods={work['name']:select_period(work,qualification[work['name']])for work in POLICY['workloads']}
 record={'candidate_manifest_sha256':digest(manifest),'candidate_image':IMAGE,'policy':POLICY,'policy_sha256':digest(POLICY),'qualification_sha256':digest(qualification),'selected_periods':periods,'fixture':fixture,'measurements_started':False}
 record['plan_sha256']=digest(record);Path(path).write_text(json.dumps(record,indent=2)+'\n');return record
def validate_plan(record,qualification,manifest,fixture):
 identity(manifest);value=dict(record);claimed=value.pop('plan_sha256',None)
 if claimed!=digest(value)or value.get('candidate_manifest_sha256')!=digest(manifest)or value.get('policy')!=POLICY or value.get('policy_sha256')!=digest(POLICY)or value.get('qualification_sha256')!=digest(qualification)or value.get('fixture')!=fixture or value.get('measurements_started')is not False:raise ValueError('Frozen plan changed')
 for work in POLICY['workloads']:
  if value['selected_periods'].get(work['name'])!=select_period(work,qualification[work['name']]):raise ValueError('Pacing changed')
 return claimed
