"""Pinned interpreter/gevent/Redis LOCAL controls only; no Django/WSGI app."""
"""Actual passive relay OS-thread control, no gevent monkey patch."""
import json,os,platform,subprocess,sys,time,unittest
from pathlib import Path
if os.environ.get('CAUSAL_PINNED_PROBE')!='playback-fixture-only':raise RuntimeError('OwnPinnedProbeGuard')
port=6397;server=None;passed=False;clean=False;report={'Source_application_executed':False,'WSGI_busy_case_executed':False,'forced_GC':False,'Source_method_or_Redis_method_replacement':False,'Python':sys.version,'gevent':None,'threading_Thread_module':__import__('threading').Thread.__module__,'threading_local_module':__import__('threading').local.__module__,'tests':None}
try:
 server=subprocess.Popen(['redis-server','--port',str(port),'--bind','127.0.0.1','--save','','--appendonly','no','--dir','/tmp'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
 from raw_redis import Client
 end=time.monotonic()+5
 while True:
  try:c=Client(port);assert c.send('PING')==b'PONG';c.close();break
  except OSError:
   if server.poll()is not None or time.monotonic()>end:raise RuntimeError('OwnRedisReady')
   time.sleep(.02)
 os.environ.update(CAUSAL_REDIS_CONTROL='playback-fixture-only',CAUSAL_REDIS_PORT=str(port))
 loader=unittest.TestLoader();suite=loader.loadTestsFromName('test_wire_attribution');result=unittest.TextTestRunner(verbosity=2).run(suite);passed=result.wasSuccessful();report['tests']={'run':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped)}
finally:
 if server is not None:
  if server.poll()is None:server.terminate()
  try:server.wait(timeout=3)
  except subprocess.TimeoutExpired:server.kill();server.wait(timeout=2)
  clean=server.poll()is not None
 import pinned_source,hashlib
 selected=pinned_source.rules(os.environ['CAUSAL_PINNED_SOURCE'])
 report['pinned_code_registry']=[{'qualname':code.co_qualname,'firstline':code.co_firstlineno,'filename':code.co_filename,'code_sha256':hashlib.sha256(code.co_code).hexdigest(),'kind':kind,'markers':markers}for code,kind,markers in selected]
 import redis_binding
 report['redis_py_wire_binding']=redis_binding.describe()[0]
 report['interpreter_yield_profile']='CPython3.13 trace-return at adjacent RESUME after YIELD_VALUE; direct YIELD_VALUE also recognized; unknown nonexception generator return invalidates coverage'
 report.update(passed=passed and clean,owned_Redis_PID_reaped=clean,Django_or_Source_modules_imported=any(name=='django'or name.startswith('django.')or name.startswith('apps.')for name in sys.modules))
 report['Django_framework_signal_control_only']=False
 report['actual_standard_threads_wire_control']=True
 report['Source_application_modules_imported']=any(name=='apps'or name.startswith('apps.')for name in sys.modules)
 if report['Source_application_modules_imported']:report['passed']=False
 Path('/evidence/pinned-control-proof.json').write_text(json.dumps(report,indent=2)+'\n')
if not report['passed']:raise SystemExit(1)
