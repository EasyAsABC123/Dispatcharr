"""Actual Recording playback requests, using media made by the real DVR worker.
Original Dispatcharr authors retain all application and algorithm credit.
No apps start on import. Query credentials and token-bearing replies stay private.
"""
import hashlib,json,socket,time,urllib.request,urllib.error,urllib.parse,uuid
from pathlib import Path
from workflow import save
from budget import bounded_read
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args):return None

def digest(path):
 h=hashlib.sha256()
 with path.open('rb')as source:
  for block in iter(lambda:source.read(65536),b''):h.update(block)
 return h.hexdigest()

def run(fixture,ident,active=False):
 row=next(r for r in fixture.read_rows()if r['id']==ident);cp=row.get('custom_properties')or{}
 directory=fixture.output/('playback-'+str(ident)+('-active'if active else '-final'));directory.mkdir(exist_ok=False)
 cells=[];opener=urllib.request.build_opener(NoRedirect)
 def request(label,path,method='GET',headers=None,token='admin',query=None,expected=None,status=200,maximum=None):
  auth={}if token is None else{'Authorization':'Bearer '+(fixture.admin if token=='admin'else fixture.accounts[token]['access'])}
  if headers:auth.update(headers)
  secret=query is not None
  url=fixture.base_url+path+('?' +urllib.parse.urlencode(query)if query else'')
  record={'cell':label,'method':method,'path':path,'credential_role':token,'authorization_present':'Authorization'in auth,'api_key_present':'X-API-Key'in auth,'query_credentials_present':secret,'status':'failed'};cells.append(record)
  response=None;raw=b''
  try:
   try:response=opener.open(urllib.request.Request(url,headers=auth,method=method),timeout=fixture.remaining(30))
   except urllib.error.HTTPError as error:response=error
   with response:
    limit=maximum if maximum is not None else(max(65536,len(expected)+1)if expected is not None else 1048576)
    raw=bounded_read(response,limit,fixture.remaining)
    record.update(actual_status=response.status,actual_bytes=len(raw),body_sha256=hashlib.sha256(raw).hexdigest())
    public_headers={k:v for k,v in response.headers.items()if k.lower()not in('set-cookie','location')}
    record['headers']=public_headers
    if 'Location'in response.headers:
     location=response.headers['Location'];record['redirect_path']=urllib.parse.urlsplit(location).path;record['redirect_query_present']=bool(urllib.parse.urlsplit(location).query)
    if secret:
     private=directory/'private';private.mkdir(mode=0o700,exist_ok=True);(private/(label+'.body')).write_bytes(raw)
     save(private/(label+'.headers.json'),dict(response.headers))
    else:(directory/(label+'.body')).write_bytes(raw)
    record['status']='passed'if response.status==status and len(raw)<=limit and(expected is None or raw==expected)else'failed'
  except Exception as error:record['failure']=type(error).__name__
  finally:save(directory/(label+'.json'),record)
  return record,raw
 root=f'/api/channels/recordings/{ident}/'
 try:
  before=fixture.snapshot('before-'+directory.name)
  if active:
   hls=fixture.case_data/Path(cp['_hls_dir']).relative_to('/data')
   # Read a completed segment referenced by an atomically replaced actual playlist;
   # do not compare a growing final segment or synthesize an HLS fixture.
   playlist=(hls/'index.m3u8').read_text();names=[line.strip()for line in playlist.splitlines()if line.strip()and not line.startswith('#')]
   if not names:raise RuntimeError('ActualPlaylistHasNoCompletedSegment')
   segment=names[0];expected=(hls/segment).read_bytes()
   def playlist_request(label,token=False):
    before_index=(hls/'index.m3u8').read_text()
    record,body=request(label,root+'hls/index.m3u8',token=None if token else'admin',query={'token':fixture.admin}if token else None)
    after_index=(hls/'index.m3u8').read_text();suffix='?'+urllib.parse.urlencode({'token':fixture.admin})if token else''
    versions=[]
    for index in (before_index,after_index):
     versions.append(''.join((fixture.base_url+root+'hls/'+line.strip()+suffix+'\n')if line.strip()and not line.strip().startswith('#')else line for line in index.splitlines(keepends=True)).encode())
    record['actual_comment_blank_segment_rewrite_matches_observed_atomic_version']=body in versions
    record['observed_playlist_sha256']=[hashlib.sha256(v.encode()).hexdigest()for v in (before_index,after_index)]
    if body not in versions:record['status']='failed'
    return record,body
   result,raw=playlist_request('hls-playlist');text=raw.decode('utf-8')
   result['absolute_segment_urls']=all(line.startswith(fixture.base_url+root+'hls/')for line in text.splitlines()if line and not line.startswith('#'))
   if not result['absolute_segment_urls']:result['status']='failed'
   token_result,token_body=playlist_request('hls-query-token',token=True)
   token_urls=[urllib.parse.urlsplit(line)for line in token_body.decode().splitlines()if line and not line.startswith('#')]
   token_result['actual_query_propagation']=bool(token_urls)and all(urllib.parse.parse_qs(u.query)=={'token':[fixture.admin]}for u in token_urls)
   if not token_result['actual_query_propagation']:token_result['status']='failed'
   request('hls-ts',root+'hls/'+segment,expected=expected)
   request('hls-ts-range-ignored',root+'hls/'+segment,headers={'Range':'bytes=2-7'},expected=expected)
   actual_segment='/shared/'+fixture.case+'/'+str((hls/segment).relative_to(fixture.case_data))
   fixture.command(fixture.compose+['exec','-T','baseline','ffmpeg','-hide_banner','-v','error','-xerror','-i',actual_segment,'-map','0:v:0','-map','0:a:0','-f','null','-'],60,allow=True)
   decoder=fixture.cli[-1];cells.append({'cell':'actual-HLS-segment-decode','actual_command':decoder,'status':'passed'if decoder['actual_exit']==0 and decoder['process_group_settled']and decoder['stdout_EOF']and decoder['stderr_EOF']else'failed'})
   ttl=fixture.redis(0,"return {redis.call('GET','dvr:hls_viewer:'..ARGV[1])or false,redis.call('TTL','dvr:hls_viewer:'..ARGV[1])}",ident)
   cells.append({'cell':'hls-heartbeat','actual':ttl,'status':'passed'if ttl[0]=='1'and 0<ttl[1]<=20 else'failed'})
   redirect,_=request('file-to-hls',root+'file/',status=302)
   if redirect.get('redirect_path')!=root+'hls/index.m3u8':redirect['status']='failed'
   request('hls-missing',root+'hls/normal-never-created.ts',status=404)
   # Declared owned filesystem faults exercise realpath checks without mutating
   # media or replacing the actual producer/playlist.
   guest_hls='/shared/'+fixture.case+'/'+str(hls.relative_to(fixture.case_data))
   fixture.python("from pathlib import Path;p=Path("+repr(guest_hls)+");(p/'normal-unsupported.txt').write_text('owned unsupported extension\\n');(p/'normal-escape.ts').symlink_to('/etc/passwd')")
   try:
    request('hls-unsupported',root+'hls/normal-unsupported.txt',status=404)
    request('hls-symlink-escape',root+'hls/normal-escape.ts',status=403)
    request('hls-traversal',root+'hls/%2E%2E/normal-outside.ts',status=403)
   finally:fixture.python("from pathlib import Path;p=Path("+repr(guest_hls)+");(p/'normal-escape.ts').unlink();(p/'normal-unsupported.txt').unlink()")
  else:
   file=fixture.case_data/Path(cp['file_path']).relative_to('/data');size=file.stat().st_size
   # Transfer the full actual object within a budget derived from that object.
   result,raw=request('file-full',root+'file/',maximum=size)
   result['actual_owned_file_bytes']=size;result['actual_owned_file_sha256']=digest(file)
   if len(raw)!=size or hashlib.sha256(raw).hexdigest()!=result['actual_owned_file_sha256']:result['status']='failed'
   request('file-head',root+'file/',method='HEAD',expected=b'')
   with file.open('rb')as source:
    source.seek(2);small=source.read(6)
   request('file-closed-range',root+'file/',headers={'Range':'bytes=2-7'},status=206,expected=small)
   request('file-range-head',root+'file/',method='HEAD',headers={'Range':'bytes=2-7'},status=206,expected=b'')
   with file.open('rb')as source:suffix=source.read(6)
   request('file-source-suffix-quirk',root+'file/',headers={'Range':'bytes=-5'},status=206,expected=suffix)
   # Full hashes verify open and malformed fallback without a second in-memory
   # copy of the owned recording file.
   for name,range_value,offset,expected_status in [('file-open-range','bytes=2-',2,206),('file-multirange','bytes=0-1,3-4',0,200),('file-case-range','Bytes=0-1',0,200)]:
    record,body=request(name,root+'file/',headers={'Range':range_value},status=expected_status,maximum=size)
    h=hashlib.sha256()
    with file.open('rb')as source:
     source.seek(offset)
     for block in iter(lambda:source.read(65536),b''):h.update(block)
    if len(body)!=size-offset or hashlib.sha256(body).hexdigest()!=h.hexdigest():record['status']='failed'
   redirect,_=request('hls-to-file',root+'hls/index.m3u8',status=302)
   if redirect.get('redirect_path')!=root+'file/':redirect['status']='failed'
   request('anonymous',root+'file/',token=None,status=403)
   request('invalid-query',root+'file/',token=None,query={'token':'invalid.owned.fixture'},status=403)
   record,body=request('valid-query',root+'file/',token=None,query={'token':fixture.admin},maximum=size)
   if hashlib.sha256(body).hexdigest()!=digest(file):record['status']='failed'
   request('header-precedence',root+'file/',headers={'Authorization':'Bearer invalid.owned.fixture'},query={'token':fixture.admin},status=401)
   request('dvr-none',root+'file/',token='viewer',status=403)
   record,body=request('valid-api-key',root+'file/',token=None,headers={'X-API-Key':fixture.accounts['admin']['api_key']},maximum=size)
   if hashlib.sha256(body).hexdigest()!=digest(file):record['status']='failed'
   record,body=request('standard-default-view',root+'file/',token='standard',maximum=size)
   if hashlib.sha256(body).hexdigest()!=digest(file):record['status']='failed'
   old_level=fixture.sql("from apps.channels.models import Channel;print(json.dumps(Channel.objects.get(pk="+str(fixture.channel_id)+").user_level))")
   fixture.sql("from apps.channels.models import Channel;Channel.objects.filter(pk="+str(fixture.channel_id)+").update(user_level=10);print(json.dumps(True))")
   try:request('hidden-channel-playback',root+'file/',token='standard',status=403)
   finally:fixture.sql("from apps.channels.models import Channel;Channel.objects.filter(pk="+str(fixture.channel_id)+").update(user_level="+str(old_level)+");print(json.dumps(True))")
   fixture.apply_actual_streams_deny_policy_for_loopback()
   try:request('playback-STREAMS-denial',root+'file/',status=403)
   finally:fixture.restore_actual_streams_policy()
   for label,value in [('range-reversed-transport','bytes=7-2'),('range-outside-transport','bytes='+str(size+100)+'-'+str(size+110))]:
    record={'cell':label,'range':value,'actual_file_size':size,'status':'observed_unqualified','socket_closed':False};cells.append(record)
    endpoint=urllib.parse.urlsplit(fixture.base_url);sock=socket.create_connection((endpoint.hostname,endpoint.port),timeout=fixture.remaining(10));capture=bytearray();began=time.monotonic();transport_deadline=began+fixture.remaining(10)
    try:
     wire='GET '+root+'file/ HTTP/1.1\r\nHost: '+endpoint.netloc+'\r\nAuthorization: Bearer '+fixture.admin+'\r\nRange: '+value+'\r\nConnection: close\r\n\r\n';sock.sendall(wire.encode('ascii'))
     while len(capture)<65536 and time.monotonic()<transport_deadline:
      sock.settimeout(fixture.remaining(max(.001,transport_deadline-time.monotonic())))
      part=sock.recv(min(8192,65536-len(capture)))
      if not part:record['actual_peer_EOF']=True;break
      capture.extend(part)
    except Exception as error:record['transport_failure']=type(error).__name__
    finally:sock.close();record['socket_closed']=True
    (directory/(label+'.wire')).write_bytes(capture);record.update(actual_wire_bytes=len(capture),actual_wire_sha256=hashlib.sha256(capture).hexdigest(),actual_header_bytes=capture.split(b'\r\n\r\n',1)[0].decode('latin1'),actual_body_bytes=len(capture.split(b'\r\n\r\n',1)[1])if b'\r\n\r\n'in capture else None)
   request('missing-recording','/api/channels/recordings/9223372036854775806/file/',status=404)
  # Check protocol headers independently of body hashes. The expected filename
  # comes from this actual row, not a generated fixture spelling.
  for cell in cells:
   headers={key.lower():value for key,value in cell.get('headers',{}).items()}
   name=cell['cell'];problems=[]
   if name in('hls-playlist','hls-query-token')and cell.get('actual_status')==200:
    if headers.get('content-type','').split(';')[0]!='application/x-mpegURL':problems.append('playlist MIME')
    if headers.get('cache-control')!='no-cache':problems.append('playlist cache policy')
   if name in('hls-ts','hls-ts-range-ignored')and cell.get('actual_status')==200:
    if headers.get('content-type','').split(';')[0]!='video/mp2t':problems.append('TS MIME')
    if headers.get('content-length')!=str(len(expected)):problems.append('TS length')
    if headers.get('content-disposition')!='inline; filename="'+Path(segment).name+'"':problems.append('TS inline basename')
   if not active and name in('file-full','file-head','file-closed-range','file-range-head','file-source-suffix-quirk','file-open-range','file-multirange','file-case-range'):
    mime='video/x-matroska'if file.suffix=='.mkv'else'video/mp4'if file.suffix=='.mp4'else None
    if mime and headers.get('content-type','').split(';')[0]!=mime:problems.append('file MIME')
    if headers.get('accept-ranges')!='bytes':problems.append('Accept-Ranges')
    if headers.get('content-disposition')!='inline; filename="'+str(cp.get('file_name')or'recording')+'"':problems.append('inline filename')
    first,last=(2,7)if name in('file-closed-range','file-range-head')else(0,5)if name=='file-source-suffix-quirk'else(2,size-1)if name=='file-open-range'else(0,size-1)
    if headers.get('content-length')!=str(last-first+1):problems.append('Content-Length')
    if cell.get('actual_status')==206 and headers.get('content-range')!=f'bytes {first}-{last}/{size}':problems.append('Content-Range')
   if problems:cell['status']='failed';cell['header_failures']=problems
  after=fixture.snapshot('after-'+directory.name)
  before_ffmpeg={(p.get('host_pid',p.get('pid')),p['starttime'],p['argv_text'])for p in before['actual_processes']if 'ffmpeg'in p['argv_text']}
  after_ffmpeg={(p.get('host_pid',p.get('pid')),p['starttime'],p['argv_text'])for p in after['actual_processes']if 'ffmpeg'in p['argv_text']}
  cells.append({'cell':'no-playback-converter','before_actual_ffmpeg':list(before_ffmpeg),'after_actual_ffmpeg':list(after_ffmpeg),'status':'passed'if before_ffmpeg==after_ffmpeg else'failed','scope':'actual complete FFmpeg process generation inventory before/after playback, supported by pinned no-spawn route implementation; transient process absence not inferred from snapshots alone'})
 except BaseException as error:
  save(directory/'partial-result.json',{'actual_cells':cells,'failure':{'type':type(error).__name__,'message':str(error)[:1024]},'full_playback_passed':False});raise
 # Final statuses may change after byte/hash checks; persist the complete result.
 unmeasured=['reversed/outside-range captured transport requires paired Source qualification','physical slow-reader shutdown']
 report={'actual_cells':cells,'required_unmeasured':unmeasured,'executed_slice_passed':all(c['status']in('passed','observed_unqualified')for c in cells),'full_playback_passed':False}
 save(directory/'result.json',report);return report
