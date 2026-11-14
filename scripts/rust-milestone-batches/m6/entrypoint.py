"""M6 owner export/probe API for Root's ONE global milestone coordinator.
No Docker/build/worker startup here; common candidate and resource ownership are
provided by Root. Missing implementations remain required blocked cells.
"""
import argparse,base64,datetime,hashlib,json,os,re,stat,time,urllib.error,urllib.parse,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parent
H=lambda b:hashlib.sha256(b).hexdigest()
def save(path,value):
 data=(json.dumps(value,indent=2)+'\n').encode();fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
 with os.fdopen(fd,'wb')as f:f.write(data);f.flush();os.fsync(f.fileno())
 directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(directory)
 finally:os.close(directory)
def export():
 plan=json.loads((ROOT/'matrix.json').read_text());nodes=[]
 for group,cases in plan['contexts'].items():
  previous=None
  for case in cases:
   # Ordered real-model publications reuse one fixture in each context.
   # Missing Source TVG/version authority is one grouped prerequisite node.
   dependencies=[]if previous is None else['m6/'+previous]
   if case['id']in('channel-signal-tvg-programmes','version200-update-no-update-malformed'):dependencies.append('m6/Source-missing-domains')
   nodes.append({**case,'depends_on':dependencies,'global_id':'m6/'+case['id'],'owner':'M6','context':group,'runner':['python','scripts/rust-milestone-batches/m6/entrypoint.py','--run-node',case['id'],'--inputs','<ROOT_COMMON_CONTEXT>','--output','<ROOT_NODE_RECEIPT>'],'candidate':'same Root integrated production Main/image for entire milestone','dispatch_authority':'Root global coordinator only','shared_fixture':'Root-owned context, no owner Compose bootstrap','probe_result_does_not_pass_required_cell':True,'implementation_status':'safe-production-probe'if case['id']in('api202-publication','default-job-graph')else'blocked' })
   previous=case['id']
 nodes.insert(0,{'id':'Source-missing-domains','global_id':'m6/Source-missing-domains','owner':'M6','context':'Source-prerequisites','depends_on':[],'implementation_status':'blocked','required':True,'runner':['python','scripts/rust-milestone-batches/m6/entrypoint.py','--run-node','Source-missing-domains','--inputs','<ROOT_COMMON_CONTEXT>','--output','<ROOT_NODE_RECEIPT>'],'reason':'one grouped actual TVG/version Source cohort; Root binds guest run_source_extension.py to copied fixture; no prior qualified DVR replay'})
 return {'owner':'M6','global_slots_owned_by':'Root','own_concurrency_pool':False,'nodes':nodes,'required_cells':len(nodes),'functional_pass_requires_all_required_cells':True,'missing_capabilities':json.loads((ROOT/'missing-capabilities.json').read_text()),'performance':{'requires':['all functional cells pass','Root exclusive quiet admission','fixed Source and candidate workloads','equal useful import rows/events and whole decoded media work','all workers/media/database/cache/compatibility services inside boundary'],'resource_gain_inherited':False,'functional_media_bounds_not_performance_equivalence':True}}
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):return None
def inputs(path):
 p=Path(path);raw=p.read_bytes();v=json.loads(raw)
 if v.get('root_authorized')is not True or v.get('cohort_owner')!='Root' or not re.fullmatch('[a-f0-9]{64}',v.get('candidate_source_map_sha256','')):raise ValueError('RootCommonContextRequired')
 u=urllib.parse.urlsplit(v['base_url'])
 if u.scheme!='http'or u.hostname not in ('127.0.0.1','localhost')or u.port not in v['owned_loopback_ports']or u.username or u.password or u.path not in ('','/')or u.query or u.fragment:raise ValueError('ExactOwnedLoopbackContext')
 if type(v['source_id'])is not int or not 1<=v['source_id']<=2147483647:raise ValueError('OwnedPositiveSource')
 return v,H(raw)
def run(node,path,out):
 if out.exists()or out.is_symlink()or out.with_suffix(out.suffix+'.attempt.json').exists()or out.with_suffix(out.suffix+'.attempt.json').is_symlink():raise ValueError('FreshOwnedNodeReceiptRequiredBeforeNetwork')
 v,digest=inputs(path);plan=export();known={n['id']:n for n in plan['nodes']}
 if node not in known:raise ValueError('DeclaredNodeOnly')
 receipt={'node':node,'context':known[node]['context'],'input_sha256':digest,'candidate_source_map_sha256':v['candidate_source_map_sha256'],'candidate_image_id':v['candidate_image_id'],'status':'blocked','attempted':False,'completed':False,'passed':False,'reason':'required production implementation/fixture binding not yet complete; no Source/component pass substituted'}
 if node=='default-job-graph':
  from fifo_probe import observe
  receipt.update(attempted=True,reason='actual read-only FIFO probe; not worker completion or full graph acceptance')
  save(out.with_suffix(out.suffix+'.attempt.json'),receipt)
  try:receipt.update(probe=observe(v),probe_passed=True,completed=True,status='partial')
  except Exception as error:receipt.update(status='failed',probe_passed=False,error_type=type(error).__name__,error=str(error))
  save(out,receipt);return 0 if receipt.get('probe_passed')else 1
 if node!='api202-publication':save(out,receipt);return 2
 auth=Path(v['authorization_file']);st=auth.lstat()
 if not stat.S_ISREG(st.st_mode)or st.st_size>8192:raise ValueError('BoundedOwnedAuthorizationFile')
 authorization=auth.read_text().strip()
 if '\r'in authorization or '\n'in authorization or not authorization:raise ValueError('AuthorizationHeaderShape')
 body=json.dumps({'id':v['source_id'],'force':False},separators=(',',':')).encode();request=urllib.request.Request(v['base_url'].rstrip('/')+'/api/epg/import/',data=body,method='POST',headers={'Authorization':authorization,'Content-Type':'application/json','Accept':'application/json'})
 opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect());receipt.update(attempted=True,began_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),request={'method':'POST','path':'/api/epg/import/','body_base64':base64.b64encode(body).decode(),'authorization_sha256':H(authorization.encode())})
 # Preserve attempt BEFORE network. The final receipt never asserts that a
 # disconnected HTTP observer settled its accepted server task.
 attempt=out.with_suffix(out.suffix+'.attempt.json');save(attempt,receipt)
 try:
  try:response=opener.open(request,timeout=10)
  except urllib.error.HTTPError as error:response=error
  with response:
   raw=response.read(65537)
   if len(raw)>65536:raise ValueError('BoundedHTTPResponse64KiB')
   receipt['response']={'status':response.status,'headers':list(response.headers.items()),'body_base64':base64.b64encode(raw).decode(),'body_sha256':H(raw)}
  receipt['completed']=True;expected={'success':True,'message':'EPG data refresh initiated.'}
  try:parsed=json.loads(raw)
  except(ValueError,UnicodeDecodeError):parsed=None
  receipt['probe_passed']=response.status==202 and parsed==expected;receipt['passed']=False;receipt['status']='partial'if receipt['probe_passed']else'failed';receipt['reason']='one actual production HTTP probe only; full Source37 API contract and worker/FIFO completion remain required'
 except Exception as error:receipt.update(status='failed',error_type=type(error).__name__,error=str(error))
 receipt['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save(out,receipt);return 0 if receipt.get('probe_passed')else 1
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--export-matrix',action='store_true');p.add_argument('--run-node');p.add_argument('--inputs',type=Path);p.add_argument('--output',type=Path);a=p.parse_args()
 if a.export_matrix:print(json.dumps(export(),indent=2))
 else:
  if not a.run_node or not a.inputs or not a.output:p.error('run-node requires inputs/output')
  raise SystemExit(run(a.run_node,a.inputs,a.output))
