"""Actual Original index-task path/cache selection plus current-file result.
Original Dispatcharr authors retain all algorithm/application credit. All files,
source rows and cache entries belong to this disposable copied fixture.
"""
import base64,datetime as dt,json,os,shutil,uuid
from pathlib import Path
from unittest.mock import patch
import source_index_worker as f
from apps.epg import tasks
from apps.epg.models import EPGData,EPGSource,EPGSourceIndex
now=dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
root=Path('/app/media')/('rust-index-path-task-'+uuid.uuid4().hex);root.mkdir(parents=True);(root/'sub').mkdir();(root/'target'/'sub').mkdir(parents=True)
def content(title):return ('<tv><programme channel="owned" start="'+(now-dt.timedelta(hours=1)).strftime('%Y%m%d%H%M%S +0000')+'" stop="'+(now+dt.timedelta(hours=1)).strftime('%Y%m%d%H%M%S +0000')+'"><title>'+title+'</title></programme></tv>').encode()
(root/'guide.xml').write_bytes(content('ordinary'));(root/'target'/'guide.xml').write_bytes(content('symlink-parent'));(root/'link').symlink_to(root/'target'/'sub',target_is_directory=True);(root/'broken').symlink_to(root/'absent')
relative=lambda p:os.path.relpath(p,'/app')
definitions=[('absolute',str(root/'guide.xml'),None),('relative',relative(root/'guide.xml'),None),('dotdot',relative(root)+'/sub/../guide.xml',None),('symlink-parent',relative(root)+'/link/../guide.xml',None),('extracted',relative(root/'guide.xml'),relative(root)+'/link/../guide.xml'),('missing-preferred',relative(root/'guide.xml'),relative(root/'absent')),('whitespace',relative(root/'guide.xml'),' '),('null-cache',None,None),('empty-cache','',''),('broken',relative(root/'broken'),None)]
report=f.report;report['passed']=False;report['scope']='Original index-task first-truthy/cache/relative-path and current-file result oracle; no native worker/application acceptance';cases=[];report['index_path_task_cases']=cases;report['source_path_context']={'source_cwd':os.getcwd(),'media_root':'/app/media','owned_root':str(root),'now':now.isoformat()}
try:
 for name,path,extracted in definitions:
  source=EPGSource(name='rust-index-path-task-'+name,source_type='xmltv',is_active=False,file_path=path,extracted_file_path=extracted);EPGSource.objects.bulk_create([source]);entry=EPGData.objects.create(name='rust-index-path-task-'+name,tvg_id='owned',epg_source=source);selected=tasks._resolve_source_file(source)
  if name.endswith('-cache'):Path(selected).write_bytes(content('cache'))
  before_index=EPGSourceIndex.objects.filter(source_id=source.pk).exists();result=tasks.build_programme_index_task.run(source.pk);index=EPGSourceIndex.objects.filter(source_id=source.pk).values_list('data',flat=True).first()
  with patch.object(tasks.timezone,'now',return_value=now):current=tasks.find_current_program_for_tvg_id(entry)
  files=[]
  for item in sorted(root.rglob('*')):
   if item.is_symlink():files.append({'path':str(item),'kind':'symlink','target':os.readlink(item)})
   elif item.is_file():files.append({'path':str(item),'kind':'file','bytes':base64.b64encode(item.read_bytes()).decode(),'mode':item.stat().st_mode&0o777})
   elif item.is_dir():files.append({'path':str(item),'kind':'directory'})
  if name.endswith('-cache'):files.append({'path':selected,'kind':'file','bytes':base64.b64encode(Path(selected).read_bytes()).decode(),'mode':Path(selected).stat().st_mode&0o777})
  cases.append({'name':name,'source_id':source.pk,'epg_entry_id':entry.pk,'file_path':path,'extracted_file_path':extracted,'selected':selected,'now':now.isoformat(),'index_before':before_index,'task_return':result,'index':index,'current':current,'files':files})
  f.check('original-index-path-'+name,result is None and not before_index and (index is None)==(name in ['missing-preferred','whitespace','broken']))
  if name.endswith('-cache'):Path(selected).unlink()
 report['passed']=True
finally:
 shutil.rmtree(root);Path('/evidence/source-index-worker-private.json').write_text(json.dumps(report,indent=2,default=str))
