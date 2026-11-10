"""Owned original producer/path oracle for explicit copied-volume rehearsal.
Original Dispatcharr authors retain algorithm/application credit. No production
DB/files/volumes are accessed; every row/path has a fresh fixture namespace.
"""
import base64,datetime as dt,hashlib,json,os,sys,uuid
from pathlib import Path
from unittest.mock import patch
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only'or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_')or os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Owned copied fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from django.conf import settings
from apps.epg import tasks
from apps.epg.models import EPGData,EPGSource,EPGSourceIndex
from celery.result import AsyncResult
from dispatcharr.celery import app
request=json.load(sys.stdin);op=request['op'];broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
seed_path=Path('/evidence/index-volume-seed-private.json')
def owned(source_id):
 source=EPGSource.objects.get(pk=source_id)
 if not source.name.startswith('rust-index-volume-'):raise RuntimeError('Owned source namespace required')
 return source

def current(entry,now):
 with patch.object(tasks.timezone,'now',return_value=now):return tasks.find_current_program_for_tvg_id(entry)

def snapshot_file(path):
 path=Path(path)
 return {'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'bytes':path.stat().st_size,'mode':path.stat().st_mode&0o777,'uid':path.stat().st_uid,'gid':path.stat().st_gid}
if op=='seed':
 if seed_path.exists():raise RuntimeError('Fresh fixture required')
 broker.flushdb();backend.flushdb();nonce=uuid.uuid4().hex;root=Path('/app/media')/('rust-index-volume-'+nonce);root.mkdir(parents=True);(root/'sub').mkdir();(root/'target'/'sub').mkdir(parents=True);persistent=Path('/data/uploads/epgs')/('rust-index-volume-'+nonce);persistent.mkdir(parents=True)
 now=dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
 def content(title):return ('<tv><programme channel="owned" start="'+(now-dt.timedelta(hours=1)).strftime('%Y%m%d%H%M%S +0000')+'" stop="'+(now+dt.timedelta(hours=1)).strftime('%Y%m%d%H%M%S +0000')+'"><title>'+title+'</title></programme></tv>').encode()
 (root/'guide.xml').write_bytes(content('ordinary'));(root/'target'/'guide.xml').write_bytes(content('symlink-parent'));(persistent/'guide.xml').write_bytes(content('persistent-data'));(root/'link').symlink_to(root/'target'/'sub',target_is_directory=True);(root/'broken').symlink_to(root/'absent')
 relative=lambda path:os.path.relpath(path,'/app')
 definitions=[('absolute',str(root/'guide.xml'),None),('relative',relative(root/'guide.xml'),None),('dotdot',relative(root)+'/sub/../guide.xml',None),('symlink-parent',relative(root)+'/link/../guide.xml',None),('extracted',relative(root/'guide.xml'),relative(root)+'/link/../guide.xml'),('missing-preferred',relative(root/'guide.xml'),relative(root/'absent')),('null-cache',None,None),('empty-cache','',''),('persistent-absolute',str(persistent/'guide.xml'),None),('persistent-relative',relative(persistent/'guide.xml'),None),('broken',relative(root/'broken'),None)]
 cases=[];files=[]
 for name,path,extracted in definitions:
  source=EPGSource(name='rust-index-volume-'+name,source_type='xmltv',is_active=False,file_path=path,extracted_file_path=extracted);EPGSource.objects.bulk_create([source]);entry=EPGData.objects.create(name='rust-index-volume-'+name,tvg_id='owned',epg_source=source);selected=tasks._resolve_source_file(source)
  if name.endswith('-cache'):Path(selected).write_bytes(content('cache'));files.append(snapshot_file(selected))
  tasks.build_programme_index_task.run(source.pk);index=EPGSourceIndex.objects.filter(source_id=source.pk).values_list('data',flat=True).first();expected=current(entry,now)
  cases.append({'name':name,'source_id':source.pk,'epg_entry_id':entry.pk,'file_path':path,'extracted_file_path':extracted,'index':index,'current':expected})
 for tree in [root,persistent]:
  for path in sorted(tree.rglob('*')):
   if path.is_symlink():files.append({'path':str(path),'symlink_target':os.readlink(path)})
   elif path.is_file():files.append(snapshot_file(path))
 # Explicit source-owned legacyUID representation on synthetic files only.
 # The native copy is separately chowned; original ownership remains unchanged.
 for tree in [root,persistent]:
  for path in [tree,*tree.rglob('*')]:
   if not path.is_symlink():os.chown(path,1000,1000)
 for source in cases:
  if source['name'].endswith('-cache'):os.chown(Path('/app/media/cached_epg')/f"{source['source_id']}.tmp",1000,1000)
 for item in files:
  if 'symlink_target'not in item:item.update(uid=Path(item['path']).stat().st_uid,gid=Path(item['path']).stat().st_gid)
 result={'format_version':1,'scope':'Owned Original index/current path oracle and copied-volume input','now':now.isoformat(),'source_cwd':os.getcwd(),'media_root':str(settings.MEDIA_ROOT),'root':str(root),'persistent_root':str(persistent),'cases':cases,'files':files,'application_hook_verified':False}
 seed_path.write_text(json.dumps(result,indent=2))
elif op=='publish':
 source=owned(request['source_id']);EPGSourceIndex.objects.filter(source_id=source.pk).delete();issued=tasks.build_programme_index_task.delay(source.pk);result={'task_id':issued.id}
elif op=='verify':
 source=owned(request['source_id']);entry=EPGData.objects.get(pk=request['epg_entry_id'],epg_source=source);now=dt.datetime.fromisoformat(request['now']);value=AsyncResult(request['task_id'],app=app).get(timeout=5);meta=backend.get(app.backend.get_key_for_task(request['task_id']));result={'index':EPGSourceIndex.objects.filter(source_id=source.pk).values_list('data',flat=True).first(),'current':current(entry,now),'normal_null_result':value is None,'backend':json.loads(meta),'file_lock_exists':bool(redis.Redis(host='localhost',password='playback-fixture-only',db=0).exists('task_lock_epg_source_file_'+str(source.pk)))}
elif op=='rollback':
 seed=json.loads(seed_path.read_text());checks=[]
 for item in seed['files']:
  if 'symlink_target'in item:checks.append(os.readlink(item['path'])==item['symlink_target'])
  else:checks.append(hashlib.sha256(Path(item['path']).read_bytes()).hexdigest()==item['sha256'])
 result={'all_original_files_unchanged':all(checks),'source_ownership':{'uid':Path(seed['root']).stat().st_uid,'gid':Path(seed['root']).stat().st_gid},'source_current':[{'name':case['name'],'current':current(EPGData.objects.get(pk=case['epg_entry_id']),dt.datetime.fromisoformat(seed['now']))}for case in seed['cases']]}
else:raise RuntimeError('Unknown owned operation')
print(json.dumps(result,default=str))
