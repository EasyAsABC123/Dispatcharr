"""Paired normal HTTP workflow inside the existing owned Fixture only.
No app, service or task body starts on import. Original authors retain credit.
"""
import hashlib,json,re,socket,threading,time,urllib.error,urllib.parse,urllib.request,uuid
from pathlib import Path

def save(path,value):Path(path).write_text(json.dumps(value,indent=2,default=str)+'\n')
def prepare_context(f):
 if f.case!='stream-file-crud':raise ValueError('OwnedStreamFileContextRequired')
 target='/shared/'+f.case+'/uploads'
 actual=json.loads(f.python("import os,pathlib,json;root=pathlib.Path("+repr(target)+");root.mkdir(mode=0o755);os.chown(root,65532,65532);root.chmod(0o755);p=pathlib.Path('/app/media/uploads');\nif p.is_symlink():p.unlink()\nelif p.exists():\n if any(p.iterdir()):raise RuntimeError('UnownedExistingUploadData')\n p.rmdir()\np.symlink_to(root,target_is_directory=True);print(json.dumps({'Source_MEDIA_uploads_resolved':str(p.resolve()),'owned_target':str(root),'Native_data_only_target':'/data/uploads'}))"))
 save(f.output/'stream-files-storage-binding.json',actual)

def encoded(fields,files):
 boundary='OwnedStreamBoundary'+uuid.uuid4().hex;chunks=[]
 for key,value in fields:
  chunks.append(('--'+boundary+'\r\nContent-Disposition: form-data; name="'+key+'"\r\n\r\n'+str(value)+'\r\n').encode())
 for key,name,data in files:
  escaped=name.replace('\\','\\\\').replace('"','\\"')
  chunks.extend([('--'+boundary+'\r\nContent-Disposition: form-data; name="'+key+'"; filename="'+escaped+'"\r\nContent-Type: application/octet-stream\r\n\r\n').encode(),data,b'\r\n'])
 chunks.append(('--'+boundary+'--\r\n').encode());return b''.join(chunks),'multipart/form-data; boundary='+boundary

def run(f):
 out=f.output/'stream-file-crud';out.mkdir();private=out/'private';private.mkdir(mode=0o700);report={'status':'running','cells':[],'full_M5_passed':False,'performance_verified':False};created=[]
 def persist():save(out/'report.json',report)
 def snapshot():
  value=json.loads(f.python("from pathlib import Path;import hashlib,json,stat;root=Path("+repr('/shared/'+f.case+'/uploads')+");rows=[]\nfor p in sorted(root.iterdir()):\n if p.is_symlink()or not p.is_file():raise RuntimeError('UnexpectedOwnedUploadEntry')\n if p.stat().st_size>5242880:raise RuntimeError('OwnedUploadObserverCapacity')\n m=p.stat();rows.append({'name':p.name,'bytes':m.st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'mode':oct(stat.S_IMODE(m.st_mode)),'UID':m.st_uid,'GID':m.st_gid})\nprint(json.dumps(rows))"));return value
 def request(label,method,path,fields=(),files=(),role='admin'):
  data,mime=encoded(fields,files);headers={'Content-Type':mime}
  if role is not None:headers['Authorization']='Bearer '+f.accounts[role]['access']
  req=urllib.request.Request(f.base_url+path,data=data,headers=headers,method=method);row={'label':label,'method':method,'path':path,'role':role,'request_sha256':hashlib.sha256(data).hexdigest(),'status':None};report['cells'].append(row);persist()
  try:r=urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=f.remaining(25))
  except urllib.error.HTTPError as e:r=e
  with r:
   raw=r.read(1048577);row['status']=r.status;row['content_type']=r.headers.get('Content-Type');row['body_sha256']=hashlib.sha256(raw).hexdigest()
  captured=private/(label+'.bin');captured.write_bytes(raw);captured.chmod(0o600)
  assert len(raw)<=1048576
  try:value=json.loads(raw)
  except ValueError:value=None
  row['JSON']=value;row['files_after']=snapshot();persist();return row,value
 def api(label,method,path,role='admin'):
  # Preserve raw normal CRUD responses privately BEFORE JSON decoding/assertions.
  headers={'Authorization':'Bearer '+f.accounts[role]['access']}
  req=urllib.request.Request(f.base_url+path,headers=headers,method=method)
  try:response=urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=f.remaining(25))
  except urllib.error.HTTPError as error:response=error
  with response:raw=response.read(1048577);status=response.status
  captured=private/(label+'.bin');captured.write_bytes(raw);captured.chmod(0o600)
  save(private/(label+'.json'),{'status':status,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
  assert len(raw)<=1048576
  value=json.loads(raw)if raw else None
  return {'status':status,'body':value}
 def file_for(ident):
  return f.sql("from apps.channels.models import Stream;s=Stream.objects.get(pk="+str(ident)+");print(json.dumps({'id':s.pk,'local_file':s.local_file.name,'name':s.name,'is_custom':s.is_custom,'m3u_account':s.m3u_account_id}))")
 base='/api/channels/streams/';A=b'owned original uploaded bytes\n'*31;B=b'owned second uploaded bytes\n'*29
 try:
  before=snapshot();assert before==[],before
  row,_=request('standard-denied','POST',base,[('name','Owned denied upload')],[('local_file','denied.bin',A)],'standard');assert row['status']==403 and row['files_after']==before
  row,value=request('create-original','POST',base,[('name','Owned upload original')],[('local_file','owned-original.bin',A)]);assert row['status']==201,(row,value);first=value['id'];created.append(first);one=file_for(first);assert one['local_file']=='uploads/owned-original.bin';assert next(x for x in row['files_after']if x['name']=='owned-original.bin')['sha256']==hashlib.sha256(A).hexdigest()
  report['first_SQL']=one;report['returned_file_URL']=value['local_file']
  observed=api('standard-read','GET',base+str(first)+'/',role='standard');assert observed['status']==200 and observed['body']['local_file']==value['local_file']and'url'not in observed['body'];report['actual_standard_read_hides_provider_URL_but_preserves_FileField_URL']=observed['status']
  # Observe normal deployment behavior. The Source URL can hit its XC alias or
  # React fallback; do not invent an unauthenticated binary media route.
  try:response=urllib.request.build_opener(urllib.request.ProxyHandler({})).open(value['local_file'],timeout=f.remaining(10))
  except urllib.error.HTTPError as e:response=e
  with response:raw=response.read(262145);captured=private/'returned-file-URL.bin';captured.write_bytes(raw);captured.chmod(0o600);report['actual_returned_URL_HTTP']={'status':response.status,'content_type':response.headers.get('Content-Type'),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()};assert len(raw)<=262144
  row,value=request('same-name-no-overwrite','POST',base,[('name','Owned upload collision')],[('local_file','owned-original.bin',B)]);assert row['status']==201;second=value['id'];created.append(second);two=file_for(second);assert re.fullmatch(r'uploads/owned-original_[A-Za-z0-9]{7}\.bin',two['local_file']);assert next(x for x in row['files_after']if x['name']=='owned-original.bin')['sha256']==hashlib.sha256(A).hexdigest();report['collision_SQL']=two
  row,value=request('replace-keeps-old','PATCH',base+str(first)+'/',[],[('local_file','owned-replacement.tar.gz',B)]);assert row['status']==200;replaced=file_for(first);assert replaced['local_file']=='uploads/owned-replacement.tar.gz'and any(x['name']=='owned-original.bin'for x in row['files_after'])
  row,value=request('put-retains-missing-file','PUT',base+str(first)+'/',[('name','Owned upload PUT')]);assert row['status']==200 and file_for(first)['local_file']==replaced['local_file']
  row,value=request('clear-keeps-file','PATCH',base+str(first)+'/',[('local_file','')]);assert row['status']==200 and not file_for(first)['local_file']and any(x['name']=='owned-replacement.tar.gz'for x in row['files_after'])
  row,value=request('multipart-JSON-stats','PATCH',base+str(first)+'/',[('stream_stats','{"codec":"owned","width":1280}'),('stream_stats_updated_at','')]);assert row['status']==200 and value['stream_stats']=={'codec':'owned','width':1280}
  old=snapshot();row,value=request('empty-file-validation','POST',base,[('name','Owned empty upload')],[('local_file','owned-empty.bin',b'')]);assert row['status']==400 and value['local_file']==['The submitted file is empty.']and row['files_after']==old
  row,value=request('long-name-validation','POST',base,[('name','Owned long upload')],[('local_file','x'*101,A)]);assert row['status']==400 and row['files_after']==old
  row,value=request('missing-object-before-file','PATCH',base+'9223372036854775807/',[],[('local_file','owned-missing.bin',A)]);assert row['status']==404 and row['files_after']==old
  row,value=request('unknown-upload-not-stored','POST',base,[('name','Owned ignored upload')],[('unrecognized','owned-ignored.bin',A)]);assert row['status']==201 and row['files_after']==old;created.append(value['id'])
  row,value=request('multi-suffix-collision','POST',base,[('name','Owned suffix upload')],[('local_file','owned-replacement.tar.gz',A)]);assert row['status']==201;created.append(value['id']);suffix=file_for(value['id']);assert re.fullmatch(r'uploads/owned-replacement_[A-Za-z0-9]{7}\.tar\.gz',suffix['local_file']);report['multi_suffix_SQL']=suffix
  large=(b'owned large upload exact-byte block\n'*100000)[:3000000]
  row,value=request('disk-sized-upload','POST',base,[('name','Owned large upload')],[('local_file','owned-large.bin',large)]);assert row['status']==201;created.append(value['id']);assert next(x for x in row['files_after']if x['name']=='owned-large.bin')['sha256']==hashlib.sha256(large).hexdigest()
  row,value=request('basename-and-valid-name','POST',base,[('name','Owned sanitized upload')],[('local_file','../owned space.bin',A)]);assert row['status']==201;created.append(value['id']);assert file_for(value['id'])['local_file']=='uploads/owned_space.bin'
  row,value=request('storage-prefix-truncation','POST',base,[('name','Owned storage length upload')],[('local_file','x'*93+'.bin',A)]);assert row['status']==201;created.append(value['id']);long=file_for(value['id'])['local_file'];assert len(long)==100 and re.fullmatch(r'uploads/x{80}_[A-Za-z0-9]{7}\.bin',long);report['storage_prefix_truncation_SQL']=long
  before=snapshot();actual=api('delete-collision','DELETE',base+str(second)+'/');assert actual['status']==204 and snapshot()==before;created.remove(second);report['model_delete_keeps_orphan_file']=True
  # Real owned PG failure AFTER FileField storage, not a fake worker exception.
  f.sql("with connection.cursor()as c:\n c.execute(\"CREATE FUNCTION owned_stream_file_failure()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.name='Owned forced upload failure' THEN RAISE EXCEPTION 'owned file SQL rejection';END IF;RETURN NEW;END;$$\")\n c.execute('CREATE TRIGGER owned_stream_file_failure BEFORE INSERT ON dispatcharr_channels_stream FOR EACH ROW EXECUTE FUNCTION owned_stream_file_failure()')\nprint(json.dumps(True))")
  try:
   row,value=request('SQL-failure-keeps-orphan','POST',base,[('name','Owned forced upload failure')],[('local_file','owned-SQL-orphan.bin',A)]);assert row['status']==500;assert any(x['name']=='owned-SQL-orphan.bin'for x in row['files_after'])
   assert f.sql("from apps.channels.models import Stream;print(json.dumps(Stream.objects.filter(name='Owned forced upload failure').count()))")==0
  finally:f.sql("with connection.cursor()as c:c.execute('DROP TRIGGER owned_stream_file_failure ON dispatcharr_channels_stream');c.execute('DROP FUNCTION owned_stream_file_failure()')\nprint(json.dumps(True))")
  report['final_files']=snapshot();assert all(x['mode']=='0o644'for x in report['final_files']);report['status']='passed'
  report['required_unmeasured']=['Unicode/entity/control/over255 filenames and non-UTF8 form charset','storage exhaustion, permission/rendering variants, global concurrent filename races and symlink root','unknown SQL/transport/panic quarantine and restart admission; no pool8 physical claim from logical guards','absolute-body slow upload and caller-loss physical lifecycle (separate final held cell)','returned URL behavior is actual characterization until paired comparison; no invented media serving']
 except BaseException as error:
  report['status']='failed';report['failure_type']=type(error).__name__;raise
 finally:
  report['actual_row_cleanup']=[]
  for ident in created:
   try:report['actual_row_cleanup'].append({'id':ident,'status':api('cleanup-'+str(ident),'DELETE',base+str(ident)+'/')['status']})
   except Exception as error:report['actual_row_cleanup'].append({'id':ident,'error':type(error).__name__})
  if any(row.get('status')!=204 for row in report['actual_row_cleanup']):report['status']='failed'
  persist()
 return report
