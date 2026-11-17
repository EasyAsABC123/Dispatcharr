"""Portable exact inputs for Native Comskip epochs; no service on import.
Original Dispatcharr/FFmpeg/Comskip authors retain all work credit.
"""
import hashlib,json,os
from pathlib import Path
HERE=Path(__file__).resolve().parent
RECIPE=json.loads((HERE/'recipe.json').read_text())

def digest(path):
 h=hashlib.sha256();size=0
 with Path(path).open('rb')as source:
  for block in iter(lambda:source.read(65536),b''):h.update(block);size+=len(block)
 return {'sha256':h.hexdigest(),'bytes':size}

def copy_exact(source,target,expected):
 before=source.stat();h=hashlib.sha256();size=0
 with source.open('rb')as incoming,target.open('xb')as outgoing:
  for block in iter(lambda:incoming.read(65536),b''):outgoing.write(block);h.update(block);size+=len(block)
 after=source.stat()
 if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)or {'sha256':h.hexdigest(),'bytes':size}!=expected:raise ValueError('ExactRetainedAssetCopyRequired')
 target.chmod(0o644)
 return {'source':str(source),'new_owned_path':str(target),**expected,'streamed_exact_copy':True}

def prepare_in_existing_role(command,compose,host_data):
 """Use the acceptance fixture's existing pinned baseline, never start another.
 host_data must be its already-owned `/shared` bind source. The caller binds the
 actual baseline image/FFmpeg identity; this function independently checks both.
 """
 root=Path(host_data)/'comskip-positive-media';root.mkdir(mode=0o755);root.chmod(0o755)
 cid=command(compose+['ps','-q','baseline'],timeout=15).strip()
 image=json.loads(command([compose[0],'inspect','--format','{{json .Image}}',cid],timeout=15))
 if image!=RECIPE['Source_image']:raise ValueError('PinnedAssetToolImageRequired')
 probe="import hashlib,json,shutil;from pathlib import Path;p=Path(shutil.which('ffmpeg')).resolve();h=hashlib.sha256();f=p.open('rb');[h.update(b)for b in iter(lambda:f.read(65536),b'')];print(json.dumps({'path':str(p),'sha256':h.hexdigest()}))"
 tool=json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',probe],timeout=20))
 if tool['sha256']!='5831afcbcb9dc46107b633dc09f276794dc094b70aacfda6096eeceb83f013f0':raise ValueError('ExactExecutedAssetToolRequired')
 expected={k:RECIPE['expected_files']['master'][k]for k in ['sha256','bytes']}
 master=copy_exact(HERE/'fixtures/master.mkv',root/'master.mkv',expected)
 ini_source=HERE/'fixtures/positive.ini';ini=copy_exact(ini_source,root/'positive.ini',{'sha256':RECIPE['INI']['selected_sha256'],'bytes':ini_source.stat().st_size})
 argv=RECIPE['commands'][1]
 command(compose+['exec','-T','baseline',*argv],timeout=120)
 feed=digest(root/'feed.ts')
 if feed!={k:RECIPE['expected_files']['feed'][k]for k in ['sha256','bytes']}:raise ValueError('ExactRetainedFeedRequiredNoTuning')
 return {'master':master,'INI':ini,'feed':{'new_owned_path':str(root/'feed.ts'),**feed},'actual_feed_argv':argv,'actual_tool':tool,'actual_tool_image':image,'Source_business_replayed':False,'master_regenerated':False,'new_role_started':False}
