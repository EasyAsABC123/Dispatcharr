#!/usr/bin/env python3
"""Postquiet read-only pinned Kombu restoration implementation capture.
Original Dispatcharr authors retain application credit. Framework authors retain
all framework credit/licensing. No broker, task, database or application starts.
This tool is not executed during the resource experiment quiet window.
"""
import argparse,hashlib,json,os,subprocess,sys,uuid
from pathlib import Path
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
CODE=r'''
import hashlib,importlib.metadata,inspect,json,platform
from pathlib import Path
from kombu.transport import redis as transport
from kombu.transport.virtual import base
wanted=[('redis.QoS',transport.QoS,['append','ack','restore_by_tag','restore_visible','restore_unacked']),('redis.Channel',transport.Channel,['_do_restore_message','_restore','_restore_at_beginning','_put','_get']),('virtual.QoS',base.QoS,['__init__','append','restore_unacked','restore_unacked_once'])]
records={};files={}
for label,cls,names in wanted:
 for name in names:
  key=label+'.'+name
  if not hasattr(cls,name):records[key]={'exists':False};continue
  member=getattr(cls,name);filename=inspect.getsourcefile(member);source=inspect.getsource(member);raw=Path(filename).read_bytes();files[filename]=hashlib.sha256(raw).hexdigest()
  records[key]={'exists':True,'module':member.__module__,'qualname':member.__qualname__,'file':filename,'first_line':inspect.getsourcelines(member)[1],'source':source,'source_sha256':hashlib.sha256(source.encode()).hexdigest()}
print(json.dumps({'format_version':1,'scope':'Read-only pinned framework implementation only; no queue ordering observation','versions':{name:importlib.metadata.version(name)for name in ['celery','kombu','redis']},'python':platform.python_version(),'functions':records,'files_sha256':files,'broker_connected':False,'application_started':False,'actual_restore_order_observed':False},indent=2))
'''
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);args=parser.parse_args();out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
 script=Path(__file__).read_bytes();(out/'captured-tool.py').write_bytes(script);(out/'captured-observer.py').write_text(CODE)
 container='dispatcharr-kombu-capture-'+uuid.uuid4().hex[:12]
 report={'passed':False,'scope':'Pinned Source framework static implementation capture; no native acceptance','image':IMAGE,'tool_sha256':hashlib.sha256(script).hexdigest(),'observer_sha256':hashlib.sha256(CODE.encode()).hexdigest(),'actual_restore_order_observed':False,'native_eta_consumer_verified':False,'performance_verified':False}
 try:
  actual=json.loads(subprocess.check_output(['docker','image','inspect',IMAGE],text=True,timeout=15))[0];report['image_id']=actual['Id']
  result=subprocess.run(['docker','run','--name',container,'--rm','--network','none','--read-only','--cap-drop','ALL','--pids-limit','64','--memory','256m','--cpus','1','--user','65534:65534','-e','PYTHONDONTWRITEBYTECODE=1','--entrypoint','/dispatcharrpy/bin/python',IMAGE,'-c',CODE],text=True,capture_output=True,timeout=30)
  (out/'capture.log').write_text(result.stderr);result.check_returncode();raw=json.loads(result.stdout);(out/'implementation.json').write_text(json.dumps(raw,indent=2)+'\n')
  required=['redis.QoS.restore_visible','redis.QoS.restore_by_tag','redis.QoS.restore_unacked','redis.Channel._do_restore_message','virtual.QoS.__init__']
  if not all(raw['functions'].get(key,{}).get('exists')for key in required):raise ValueError('Required pinned implementation absent')
  report.update(passed=True,versions=raw['versions'],implementation_sha256=hashlib.sha256((out/'implementation.json').read_bytes()).hexdigest())
 except(OSError,ValueError,subprocess.SubprocessError)as error:report['error_type']=type(error).__name__
 finally:
  try:
   subprocess.run(['docker','rm','--force',container],text=True,capture_output=True,timeout=15)
   present=subprocess.run(['docker','container','inspect',container],text=True,capture_output=True,timeout=15)
   if present.returncode==0:report.update(passed=False,cleanup='owned container still present')
   elif 'No such' in present.stderr:report['cleanup']='owned container absent'
   else:report.update(passed=False,cleanup='could not confirm owned container absence')
  except subprocess.SubprocessError:report.update(passed=False,cleanup='cleanup check failed')
  (out/'proof.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps({'passed':report['passed'],'evidence':str(out)}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
