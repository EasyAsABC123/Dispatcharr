"""Retained seven-case authority and absolute HTTP budget, no apps on import."""
import hashlib,json,socket,time,urllib.error,urllib.request
from pathlib import Path
SOURCE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
CASES=['head','full','first-range','middle-range','suffix-range','unsatisfiable','if-range-mismatch']
def checked(item,folder):
 path=Path(item['path']);path=path if path.is_absolute()else folder/path
 if path.is_symlink()or not path.is_file():raise ValueError('Retained authority must be a regular existing file')
 data=path.read_bytes()
 if len(data)>8*1024*1024 or hashlib.sha256(data).hexdigest()!=item['sha256']:raise ValueError('Retained authority bytes changed')
 return json.loads(data),{'path':str(path.resolve()),'sha256':item['sha256']}
def load_authority(path):
 path=Path(path);raw=path.read_bytes();binding=json.loads(raw)
 if binding.get('schema')!=1 or binding.get('Source_replay_permitted')is not False:raise ValueError('Explicit retained-only authority required')
 observation,observed=checked(binding['observation'],path.parent);qualification,qualified=checked(binding['qualification'],path.parent)
 if observation.get('source_commit')!=SOURCE or observation.get('source_image_id')!=IMAGE or observation.get('passed')is not True or observation.get('cleanup_passed')is not True:raise ValueError('Source observation identity/capture/cleanup is unqualified')
 if qualification.get('source_commit')!=SOURCE or qualification.get('source_image_id')!=IMAGE or qualification.get('passed')is not True or qualification.get('cleanup_passed')is not True or qualification.get('wire_comparison',{}).get('captured_wire_matches')is not True:raise ValueError('Retained Source seven-case qualification missing')
 decoders=qualification.get('decoders',[])
 if not any(x.get('backend')=='baseline'and x.get('strict_decoder_passed')is True and x.get('audio_and_video_mapped')is True and x.get('frames',0)>0 for x in decoders):raise ValueError('Retained Source useful-media decoder authority missing')
 events=observation['wire']['events']
 if len(events)!=7 or[row['case']for row in events]!=CASES:raise ValueError('Source seven-case inventory changed')
 full=next(row for row in events if row['case']=='full');fixture=observation['fixture']
 if full['status']!=200 or full['body_sha256']!=fixture['fixture_sha256']or full['bytes']!=fixture['fixture_bytes']:raise ValueError('Source body/fixture identity mismatch')
 return{'observation':observation,'proof':{'scope':'Retained Source seven selected-wire cases only; no new Source application request','Source_replayed':False,'source_commit':SOURCE,'source_image_id':IMAGE,'binding_sha256':hashlib.sha256(raw).hexdigest(),'observation':observed,'qualification':qualified,'header_scope':'Existing probe selected-header dictionaries only; duplicate/WorkerID/Connection/full-header parity remains open'}}
def validate_fixture(authority,actual):
 expected=authority['observation']['fixture']
 for key in('fixture_sha256','fixture_bytes','duration_seconds','video','audio'):
  if actual.get(key)!=expected.get(key):raise ValueError('Owned origin differs from actual retained Source fixture: '+key)
def install_absolute_probe(module,deadline):
 def remaining():
  seconds=deadline-time.monotonic()
  if seconds<=0:raise TimeoutError('Absolute M4 functional deadline')
  return min(20,seconds)
 def transport_socket(value):
  seen=set()
  for _ in range(8):
   if isinstance(value,socket.socket):return value
   if value is None or id(value)in seen:return None
   seen.add(id(value));value=next((getattr(value,key)for key in('fp','raw','_sock')if getattr(value,key,None)is not None),None)
  return None
 def fetch(url,method='GET',headers=None,token=None):
  remaining()
  headers=dict(headers or{})
  if token:headers['Authorization']='Bearer '+token
  request=urllib.request.Request(url,headers=headers,method=method)
  try:response=urllib.request.build_opener(module.NoRedirect).open(request,timeout=remaining())
  except urllib.error.HTTPError as error:response=error
  with response:
   chunks=[];total=0;wire_socket=transport_socket(response)
   while True:
    seconds=remaining()
    if wire_socket is not None:wire_socket.settimeout(seconds)
    chunk=response.read1(min(65536,8*1024*1024+1-total))
    if not chunk:break
    total+=len(chunk)
    if total>8*1024*1024:raise ValueError('Fixture exceeds existing8MiB probe bound')
    chunks.append(chunk)
   return response.status,dict(response.headers),b''.join(chunks)
 module.fetch=fetch

def validate_candidate_bundle(build_path,proof_path,inputs_path):
 def document(path):
  path=Path(path)
  if path.is_symlink()or not path.is_file():raise ValueError('Candidate binding must be a regular existing file')
  raw=path.read_bytes();return json.loads(raw),hashlib.sha256(raw).hexdigest()
 build,build_sha=document(build_path);proof,proof_sha=document(proof_path);inputs,inputs_sha=document(inputs_path)
 if build.get('passed')is not True or build.get('exit_code')!=0 or proof.get('passed')is not True or proof.get('actual_build_wait_exit')!=0 or proof.get('all_archive_files_byteexact_Git')is not True:raise ValueError('Actual successful build/physical Git package proof required')
 for key in('image_id','git_revision','source_tree_sha256'):
  if not build.get(key)or proof.get(key)!=build[key]:raise ValueError('Candidate build/package binding differs: '+key)
 actual=build['source_sha256'];runtime={key:value for key,value in actual.items()if key.startswith('rust/')}
 selected={key:value for key,value in inputs.items()if key.startswith('rust/')}
 if not runtime or runtime!=selected:raise ValueError('Candidate full Rust input map differs')
 if any(key not in actual or actual[key]!=value for key,value in inputs.items()):raise ValueError('Coupled candidate inputs differ')
 return{'build_sha256':build_sha,'physical_package_sha256':proof_sha,'source_inputs_sha256':inputs_sha,'image_id':build['image_id'],'git_revision':build['git_revision'],'source_tree_sha256':build['source_tree_sha256'],'Rust_inputs':len(runtime),'working_tree_runtime_adopted':False}
