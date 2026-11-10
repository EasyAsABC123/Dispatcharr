"""Original index-worker plus owned path-resolution/current-file oracle.
Original Dispatcharr authors retain all application/algorithm credit.
"""
import base64,builtins,datetime as dt,json,os,shutil,subprocess,sys,uuid
from pathlib import Path
from unittest.mock import patch
# Run the captured original-worker lifecycle first, retaining its DB/guards and
# unchanged real-worker checks. This module adds owned file observations only.
import source_index_worker as fixture
from apps.epg import tasks
from apps.epg.models import EPGSource,EPGData,EPGSourceIndex
NOW=dt.datetime(2026,11,10,12,tzinfo=dt.timezone.utc)
root=Path('/app/media')/('rust-path-owned-'+uuid.uuid4().hex);root.mkdir(parents=True)
report=json.loads(Path('/evidence/source-index-worker-private.json').read_text());cases=[]
def programme(title,start='20261110110000',stop='20261110130000',channel='one'):
 return f'<programme channel="{channel}" start="{start} +0000" stop="{stop} +0000"><title>{title}</title></programme>'.encode()
content=programme('ordinary');(root/'guide.xml').write_bytes(content);(root/'sub').mkdir();(root/'target').mkdir();(root/'target'/'sub').mkdir();(root/'target'/'guide.xml').write_bytes(programme('symlink-parent'));(root/'link').symlink_to(root/'target'/'sub',target_is_directory=True);(root/'broken').symlink_to(root/'absent.xml')
relative=lambda path:os.path.relpath(path,'/app')
def run_case(name,path,extracted=None,mode='normal',mutation=None):
 source=EPGSource(name='rust-path-'+name,source_type='xmltv',is_active=False,file_path=path,extracted_file_path=extracted);EPGSource.objects.bulk_create([source]);epg=EPGData.objects.create(name='rust-path-'+name,tvg_id='one',epg_source=source)
 cache=source.get_cache_file()if not path and not extracted else None
 if cache:Path(cache).write_bytes(content)
 index={'channels':{'one':[0]},'interleaved_channels':[]}
 if mode=='two-open':index['interleaved_channels']=['one']
 EPGSourceIndex.objects.create(source=source,data=index)
 files=[]
 for item in sorted(root.rglob('*')):
  if item.is_symlink():files.append({'path':str(item),'kind':'symlink','target':os.readlink(item)})
  elif item.is_file():files.append({'path':str(item),'kind':'file','bytes':base64.b64encode(item.read_bytes()).decode(),'mode':item.stat().st_mode&0o777})
  elif item.is_dir():files.append({'path':str(item),'kind':'directory'})
 if cache:files.append({'path':cache,'kind':'file','bytes':base64.b64encode(content).decode(),'mode':0o600})
 calls=[];opens=[];real_open=builtins.open;original_index=EPGSource.programme_index.fget;original_offsets=tasks._read_programs_at_offsets
 def clock():
  calls.append('clock')
  if mutation=='index':EPGSourceIndex.objects.filter(source=source).update(data={'channels':{'other':[0]}})
  return NOW
 def fresh(instance):calls.append('index');return original_index(instance)
 def observed_open(file,*args,**kwargs):
  if isinstance(file,(str,os.PathLike))and (str(root)in str(file)or 'rust-path-owned-'in str(file)or str(file)==cache):opens.append(str(file));calls.append('open')
  return real_open(file,*args,**kwargs)
 def offsets(*args,**kwargs):
  value=original_offsets(*args,**kwargs)
  if mutation=='replace':
   replacement=root/'replacement.xml';replacement.write_bytes(programme('reopened'));os.replace(replacement,root/'two-open.xml');calls.append('replace')
  if mutation=='delete':os.unlink(root/'two-open.xml');calls.append('delete')
  return value
 result=None;failure=None
 with patch.object(tasks.timezone,'now',side_effect=clock),patch.object(EPGSource,'programme_index',new=property(fresh)),patch.object(builtins,'open',side_effect=observed_open),patch.object(tasks,'_read_programs_at_offsets',side_effect=offsets):
  try:result=tasks.find_current_program_for_tvg_id(epg)
  except Exception as error:failure={'type':type(error).__name__,'message':str(error)}
 selected=tasks._resolve_source_file(source)
 row={'name':name,'source_id':source.pk,'epg_entry_id':epg.pk,'file_path':path,'extracted_file_path':extracted,'cache_file_path':cache,'selected':selected,'cwd':os.getcwd(),'calls':calls,'opens':opens,'result':result,'failure':failure,'mode':mode,'mutation':mutation,'files_before':files,'initial_index':index,'now':NOW.isoformat()}
 cases.append(row)
 if cache:Path(cache).unlink(missing_ok=True)
 return row
try:
 for name,path in [('absolute',str(root/'guide.xml')),('relative',relative(root/'guide.xml')),('dot',relative(root)+'/./guide.xml'),('dotdot',relative(root)+'/sub/../guide.xml'),('symlink-dotdot',relative(root)+'/link/../guide.xml'),('broken-symlink',relative(root/'broken')),('missing',relative(root/'absent.xml'))]:run_case(name,path)
 for text in ['Ünicode & space+%.xml','~literal.xml','$HOME.xml']:
  (root/text).write_bytes(content);run_case('literal-'+str(len(cases)),relative(root/text))
 run_case('preferred-extracted',relative(root/'guide.xml'),relative(root)+'/link/../guide.xml')
 run_case('missing-preferred',relative(root/'guide.xml'),relative(root/'absent.xml'))
 run_case('whitespace-preferred',relative(root/'guide.xml'),' ')
 run_case('null-cache',None,None);run_case('empty-cache','','')
 run_case('fresh-index-after-clock',relative(root/'guide.xml'),mutation='index')
 ended=programme('ended','20261110090000','20261110100000');other=programme('other',channel='other');late=programme('later')
 for name,mutation in [('relative-two-open',None),('relative-replaced','replace'),('relative-deleted','delete')]:
  (root/'two-open.xml').write_bytes(ended+other+late);run_case(name,relative(root/'two-open.xml'),mode='two-open',mutation=mutation)
 # Root can read chmod000; capture that actual UID behavior rather than claiming
 # permission denial without an unprivileged fixture. Separate UID gate remains.
 locked=root/'locked.xml';locked.write_bytes(content);locked.chmod(0);permission=run_case('root-mode000',relative(locked))
 code="import os,sys,json;sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from apps.epg.tasks import find_current_program_for_tvg_id;\ntry:\n value=find_current_program_for_tvg_id(int(sys.argv[1]));print(json.dumps({'result':value,'uid':os.getuid()}))\nexcept Exception as error:print(json.dumps({'failure_type':type(error).__name__,'uid':os.getuid()}))"
 def nobody():os.setgid(65534);os.setuid(65534)
 process=subprocess.run(['/dispatcharrpy/bin/python','-c',code,str(permission['epg_entry_id'])],env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'),preexec_fn=nobody,text=True,capture_output=True,timeout=30);process.check_returncode();unprivileged=json.loads(process.stdout.splitlines()[-1]);locked.chmod(0o600)
 if unprivileged.get('failure_type')!='PermissionError':raise AssertionError('Actual unprivileged Source permission witness')
 report['path_context']={'cwd':os.getcwd(),'media_root':str(__import__('django.conf',fromlist=['settings']).settings.MEDIA_ROOT),'media_is_symlink':Path('/app/media').is_symlink(),'media_realpath':os.path.realpath('/app/media'),'uid':os.getuid(),'owned_root':str(root),'cases':cases,'permission_denial_verified':True,'permission_observation':unprivileged,'volume_migration_verified':False,'worker_cwd_verified':all(row.get('cwd')=='/app'for row in report['observer']['index-prerun.jsonl']),'worker_uids':sorted({row.get('uid')for row in report['observer']['index-prerun.jsonl']})}
 report['checks'].append({'name':'owned-path-corpus-captured','passed':len(cases)==20})
 report['passed']=report['passed']and len(cases)==20
finally:
 shutil.rmtree(root)
 Path('/evidence/source-index-worker-private.json').write_text(json.dumps(report,indent=2,default=str))
