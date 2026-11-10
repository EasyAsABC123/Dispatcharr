"""Source-qualified ordinary VOD trials; all entrypoints require scoped cues.
Original Dispatcharr authors retain application credit and AGPL licensing.
"""
import importlib.util,json,sys,time
from pathlib import Path
import protocol,acceptance

def save(path,value):Path(path).write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')
def modules(tools):
 sys.path.insert(0,str(Path(tools)/'scripts/rust-guide-perf'))
 import observer
 spec=importlib.util.spec_from_file_location('vod_existing_api_scheduler',Path(tools)/'scripts/rust-integration/api_load.py');api=importlib.util.module_from_spec(spec);spec.loader.exec_module(api)
 return observer,api

def normalize(load,counters,workload,period,duration,checks,api):
 summary=load['summary'];traces=load['request_traces'];per_endpoint={}
 for index in range(2):
  rows=[r for r in traces if r['path_index']==index and r['disposition']=='success']
  per_endpoint[str(index)]={'completion_p95_ms':api.percentile([r['completion_ms']for r in rows],.95),'ttfb_p95_ms':api.percentile([r['ttfb_ms']for r in rows],.95),'throughput_per_second':len(rows)/duration,'completed':len(rows)}
 value={'cpu_percent_mean':counters['cpu_percent_mean'],'peak_working_memory_bytes':max((r['memory_bytes']for r in load['resource_samples']),default=0),
 'ttfb_p95_ms':summary['ttfb_ms_p95'],'completion_p95_ms':summary['roundtrip_ms_p95'],'throughput_per_second':summary['requests_per_second'],'pacing_fraction':min(1,summary['completed']/(duration*workload['users']/period)),
 'samples':len(load['resource_samples']),'errors':summary['errors'],'sample_errors':len(load['sample_errors']),'late_completed':summary['late_completed'],
 'oom_kills':0,'counter_resets':0,'correct_responses':summary['errors']==0 and summary['users_without_success']==0,
 'cache_state_verified':True,'fixture_unchanged':checks,'limits_verified':True}
 return value,per_endpoint

def execute(*,output,manifest,tools,reference,start,stop,helper,select_services,reader,quiet,measurement_cue):
 # Called only after independent functional gates; still reject missing cue
 # before opening an observer or starting any measured/qualification phase.
 protocol.authorize(quiet,manifest,'vod-source-qualification')
 output=Path(output);output.mkdir();observer,api=modules(tools);qualification={};fixture=None
 def phase(backend,workload,period,duration,warmup,destination):
  nonlocal fixture
  destination.mkdir();base,control=start(backend)
  binding={key:control[key]for key in ['fixture_sha256','schema_sha256']}
  if fixture is None:fixture=binding
  if binding!=fixture:raise ValueError('Fixture/schema changed before phase')
  if time.time()-__import__('datetime').datetime.fromisoformat(control['clock']).timestamp()>30:raise ValueError('Fresh phase token required')
  def read(base,path,token,timeout,expected):return reader.request(base,path,token,timeout,expected)
  warm=api.phase(base,protocol.PATHS,control['token'],reference,users=workload['users'],period=period,duration=warmup,timeout=10,read_request=read,stagger=True,require_complete_before_deadline=True,retain_traces=True)
  if warm['summary']['errors']:raise ValueError('Warmup failed')
  selected=select_services(backend);observed=observer.Observer(selected,tools)
  try:
   before=observed.counters()
   loaded=api.phase(base,protocol.PATHS,control['token'],reference,users=workload['users'],period=period,duration=duration,timeout=10,read_request=read,stagger=True,require_complete_before_deadline=True,retain_traces=True,sample=observed.sample,sample_interval=1)
   try:counters=observed.finish(before)
   except Exception as error:
    save(destination/'counter-failure.json',{'error_type':type(error).__name__,'before':before,'load':loaded});raise
  finally:pass  # Existing one-shot observer closes each socket; no persistent sampler.
  after=helper('phase');unchanged=all(after[key]==binding[key]for key in binding)
  value,endpoints=normalize(loaded,counters,workload,period,duration,unchanged,api)
  save(destination/'phase.json',{'backend':backend,'workload':workload,'period_seconds':period,'normalized':value,'per_endpoint':endpoints,'resources':counters,'load':loaded,'warmup':warm,'fixture_binding':binding})
  stop();return value,endpoints,counters
 # Source-only capability check under the qualification cue. Existing
 # Observer.smoke validates CLI/direct working-memory agreement and cadence,
 # deliberately not exposing magnitudes to pacing selection.
 start('source');capability=observer.Observer(select_services('source'),tools).smoke();save(output/'observer-smoke.json',capability);stop()
 for work in protocol.POLICY['workloads']:
  rows=[]
  for period in work['period_candidates_seconds']:
   value,endpoints,counters=phase('source',work,period,15,5,output/f"qualification-{work['name']}-{period}")
   failures=[f for f in acceptance.phase(value)if f not in ['insufficient_samples','late_completed']]
   source=counters['services']['source'];rows.append({'backend':'source','period_seconds':period,'errors':value['errors']+len(failures),'correct_responses':not failures,'pacing_fraction':value['pacing_fraction'],'cpu_percent_mean':source['cpu_percent_mean'],'memory_fraction':0, 'throttled_period_fraction':source['throttled_period_fraction'],'late_completed':value['late_completed']})
   # Backend-only memory bound is read from the retained per-container samples,
   # not inferred from whole-group memory divided by the backend limit.
   raw=json.loads((output/f"qualification-{work['name']}-{period}"/'phase.json').read_text());backend_id=counters['selected_services']['source']['id']
   memories=[item['memory_bytes']for sample in raw['load']['resource_samples']for item in sample['containers']if backend_id.startswith(item['id'])]
   if not memories:raise ValueError('Source backend memory samples missing')
   rows[-1]['memory_fraction']=max(memories)/protocol.POLICY['service_limits']['backend']['memory_bytes']
  qualification[work['name']]=rows;save(output/'source-qualification.json',qualification)
 plan=protocol.freeze(output/'frozen-plan.json',qualification,manifest,fixture)
 print(json.dumps({'state':'awaiting-parent-measurement-quiet-cue','plan_sha256':plan['plan_sha256'],'frozen_plan':str(output/'frozen-plan.json')}),flush=True)
 deadline=time.monotonic()+1200
 while not Path(measurement_cue).exists():
  if time.monotonic()>=deadline:raise TimeoutError('Measurement cue not issued in20minutes')
  time.sleep(1)
 protocol.validate_plan(plan,qualification,manifest,fixture);cue=protocol.authorize(measurement_cue,manifest,'vod-resource-measurement',plan['plan_sha256']);save(output/'measurement-started.json',{'plan_sha256':plan['plan_sha256'],'authorization_sha256':cue})
 pairs=[]
 for work in protocol.POLICY['workloads']:
  for trial,order in enumerate(protocol.POLICY['orders'],1):
   phases={};endpoint_phases={}
   for backend in order:
    value,endpoints,_=phase(backend,work,plan['selected_periods'][work['name']],60,15,output/f"matrix-{work['name']}-{trial}-{backend}")
    phases[backend]=value;endpoint_phases[backend]=endpoints
   result=acceptance.compare(phases['source'],phases['candidate']);endpoint_results={}
   for index in ['0','1']:
    old=endpoint_phases['source'][index];new=endpoint_phases['candidate'][index];fail=[]
    if not old['completed']or not new['completed']:fail.append('empty_endpoint')
    else:
     if new['completion_p95_ms']>old['completion_p95_ms']*1.10:fail.append('completion')
     if new['ttfb_p95_ms']>max(old['ttfb_p95_ms'],50)*1.10:fail.append('ttfb')
     if new['throughput_per_second']<old['throughput_per_second']*.95:fail.append('throughput')
    endpoint_results[index]={'source':old,'candidate':new,'passed':not fail,'failures':fail}
   pair={'workload':work['name'],'trial':trial,'order':order,'phases':phases,'acceptance':result,'per_endpoint':endpoint_results,'passed':result['passed']and all(v['passed']for v in endpoint_results.values())};pairs.append(pair);save(output/'matrix.json',{'passed':len(pairs)==6 and all(p['passed']for p in pairs),'pairs':pairs})
   print(json.dumps({'state':'pair-complete','workload':work['name'],'trial':trial,'passed':pair['passed']}),flush=True)
   if not pair['passed']:return {'passed':False,'pairs':pairs,'performance_verified':False}
 return {'passed':len(pairs)==6,'pairs':pairs,'performance_verified':len(pairs)==6}
