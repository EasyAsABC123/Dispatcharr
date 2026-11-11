"""Guide pacing adapter over the accepted API scheduler; no CLI starts trials.
Root quiet authorization and frozen policy are mandatory in the future orchestrator.
"""
import concurrent.futures,importlib.util,math,threading,time
from pathlib import Path
path=Path(__file__).resolve().parents[1]/'rust-integration/api_load.py';spec=importlib.util.spec_from_file_location('guide_api_scheduler',path);api=importlib.util.module_from_spec(spec);spec.loader.exec_module(api)
from reader import request

def paced(base,spec,token,expected,*,users,period,duration,sample=None):
 def adapted(base,_label,token,timeout,expected):return request(base,spec,token,timeout,expected)
 return api.phase(base,['guide'],token,{'guide':expected},users=users,period=period,duration=duration,timeout=10,sample=sample,sample_interval=1,read_request=adapted,stagger=True,require_complete_before_deadline=True,retain_traces=True)

def cold_waves(base,spec,expected,*,users,period,duration,before_wave,sample=None):
 if users not in [1,10]or period not in [10,15,20]or not 0<duration<=60 or not math.isfinite(duration)or not callable(before_wave):raise ValueError('Invalid frozen cold-wave bounds')
 stop=threading.Event();samples=[];sample_errors=[];began=time.monotonic();deadline=began+duration;rows=[]
 def observe():
  while not stop.is_set():
   try:
    value=sample()
    if time.monotonic()<=deadline:samples.append(value)
   except Exception as error:sample_errors.append(type(error).__name__)
   stop.wait(1)
 observer=threading.Thread(target=observe,daemon=True)if sample else None
 if observer:observer.start()
 try:
  with concurrent.futures.ThreadPoolExecutor(max_workers=users)as pool:
   epoch=0
   while began+epoch*period<deadline:
    stop.wait(max(0,began+epoch*period-time.monotonic()))
    if time.monotonic()>=deadline:break
    # No previous futures remain: never invalidate under active readers.
    before_wave()
    def consume(index):
     started=time.monotonic()
     try:
      _,first,elapsed=request(base,spec,timeout=10,expected=expected)
      ended=time.monotonic();within=ended<=deadline
      return {'passed':within,'late':not within,'ttfb_ms':first,'completion_ms':elapsed,
              'user_index':index,'wave_index':epoch,'started_seconds':started-began,
              'verified_complete_seconds':ended-began,'in_window':within,'error_type':None,
              'disposition':'success'if within else'late'}
     except Exception as error:
      ended=time.monotonic()
      return {'passed':False,'late':False,'ttfb_ms':None,'completion_ms':None,
              'user_index':index,'wave_index':epoch,'started_seconds':started-began,
              'verified_complete_seconds':ended-began,'in_window':ended<=deadline,
              'error_type':type(error).__name__[:64],'disposition':'error'}
    results=list(pool.map(consume,range(users)));rows.append({'epoch':epoch,'consumers':results});epoch+=1
   stop.wait(max(0,deadline-time.monotonic()))
 finally:
  stop.set()
  if observer:
   observer.join(15)
   if observer.is_alive():raise RuntimeError('Guide resource observer did not stop')
 flat=[row for wave in rows for row in wave['consumers']];completed=sum(row['passed']for row in flat)
 return {'completed':completed,'errors':sum(row.get('error_type')is not None for row in flat),'late_completed':sum(row['late']for row in flat),'expected_waves':math.ceil(duration/period),'waves':rows,'resource_samples':samples,'sample_errors':sample_errors,'performance_verified':False,'request_traces':flat,'trace_limit':60,'trace_scope':'Timing/indices only; no URL/token/body/exception text. Late verified work excluded from completed count.'}
