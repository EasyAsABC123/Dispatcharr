"""UNEXECUTED modal gate controller for an already-owned canonical fixture.
Does not create containers, sample resources, or change runtime. Caller supplies
normal Original/native bases and must destroy its exact owned project afterward.
"""
import hashlib,json,secrets,subprocess
from pathlib import Path

def probe(command,compose,environment,evidence,tools,source_base,candidate_base,origin_ip):
 password=secrets.token_urlsafe(32);evidence=Path(evidence);evidence.mkdir(parents=True,exist_ok=True)
 helper=compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/provider_modal_fixture.py']
 report={'passed':False,'scope':'Actual unchanged React movie/series modals, provider selection, decoded proxied artwork; no resource claim','no_Accept_override':True,'normal_login_count':2 if candidate_base else 1,'observations':{}}
 try:
  result=command(helper,payload=json.dumps({'op':'prepare','password':password,'origin_ip':origin_ip}),timeout=30);fixture=json.loads(result.stdout.splitlines()[-1]);report['fixture']=fixture
  for target,base in [('Original',source_base)]+([('native',candidate_base)]if candidate_base else[]):
   process=subprocess.run(['node',str(Path(tools)/'scripts/rust-vod-api-contracts/provider_modal_browser.js')],input=json.dumps({**fixture,'password':password,'url':base})+'\n',cwd=tools,env=environment,text=True,capture_output=True,timeout=180)
   values=[json.loads(line)for line in process.stdout.splitlines()if line.startswith('{')];observation=values[-1]if values else{'passed':False,'error':'Missing browser output'}
   report['observations'][target]=observation;(evidence/('provider-modal-'+target+'.json')).write_text(json.dumps({'capability':values[0]if values else None,'result':observation,'exit_code':process.returncode},indent=2)+'\n')
   if process.returncode or not observation.get('passed'):raise RuntimeError('Actual modal browser gate failed: '+target)
  if candidate_base:
   reference=report['observations']['Original']['results'];native=report['observations']['native']['results']
   report['exact_ordered_modal_observations']=reference==native
   if reference!=native:raise RuntimeError('Source/native modal observations differ; no sorting or normalization applied')
  report['passed']=True
 finally:
  # Preparation can fail after private state creation: always attempt guarded
  # restoration; never silently waive an unavailable restoration response.
  restored=command(helper,payload=json.dumps({'op':'restore'}),timeout=30);report['restoration']=json.loads(restored.stdout.splitlines()[-1]);report['passed']=report['passed']and report['restoration'].get('restored',False)and report['restoration'].get('no_provider_contact',False)
  (evidence/'provider-modal-browser.json').write_text(json.dumps(report,indent=2)+'\n')
 return report
