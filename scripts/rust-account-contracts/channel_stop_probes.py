"""Real MPEG-TS stop-before-delete behavior using the disposable origin stream."""
import time
import urllib.request
import uuid
from urllib.parse import urlsplit


def packets(data):
    for start in range(min(188,len(data))):
        count=min(20,(len(data)-start)//188)
        if count>=5 and all(data[start+index*188]==0x47 for index in range(count)):
            return count
    return 0


def run(http,baseline,candidate,accounts,record,seed):
    root='/api/channels/channels/';admin=accounts['admin']['access']
    target=str(uuid.UUID(urlsplit(seed['path']).path.rstrip('/').rsplit('/',1)[-1]))
    status,channels=http(baseline+root,token=admin)
    sources=[channel for channel in channels if str(uuid.UUID(channel['uuid']))==target] if status==200 and isinstance(channels,list) else []
    if len(sources)!=1 or not sources[0]['streams']:
        record('channel-stop:fixture-source',False,{'status':status});return
    source=sources[0]
    prefix='rust-probe-stop-'+uuid.uuid4().hex[:12]
    def scenario(base,label,stop,active):
        created=None;name=prefix+'-'+label;response=None
        try:
            status,value=http(baseline+root,body={'name':name,'streams':source['streams'],
                'stream_profile_id':source['stream_profile_id'],'channel_profile_ids':[]},token=admin,method='POST')
            if status!=201:raise AssertionError('Channel fixture creation failed')
            created=value;path=root+str(created['id'])+'/'
            packet_count=0
            if active:
                request=urllib.request.Request(base+'/proxy/ts/stream/'+created['uuid']+'?output_format=ts',
                    headers={'Authorization':'Bearer '+admin})
                began=time.monotonic()
                response=urllib.request.urlopen(request,timeout=65)
                initial=response.read(8192);packet_count=packets(initial)
                startup_ms=round((time.monotonic()-began)*1000,3)
                # Initial readiness follows original60s allowance; after media
                # arrives keep stop/EOF checks independently strict.
                response.fp.raw._sock.settimeout(3)
                if response.status!=200 or packet_count<5:raise AssertionError('Active fixture media invalid')
            deleted,body=http(base+path+('?stop_stream=true' if stop else ''),token=admin,method='DELETE')
            if deleted!=204:return {'passed':False,'delete_status':deleted,'packets_checked':packet_count}
            if not active:return {'passed':True,'delete_status':deleted,'packets_checked':0}
            if stop:
                deadline=time.monotonic()+10;drained=0;ended=False
                while time.monotonic()<deadline and drained<16*1024*1024:
                    chunk=response.read(8192)
                    if not chunk:ended=True;break
                    drained+=len(chunk)
                return {'passed':ended,'delete_status':deleted,'packets_checked':packet_count,'stream_ended':ended,'initial_media_ms':startup_ms}
            deadline=time.monotonic()+2;continued=True
            while time.monotonic()<deadline:
                chunk=response.read(8192)
                if not chunk:continued=False;break
                if packets(chunk)<5:raise AssertionError('Post-delete fixture media invalid')
            return {'passed':continued,'delete_status':deleted,'packets_checked':packet_count,'stream_continued':continued,'initial_media_ms':startup_ms}
        finally:
            if response is not None:response.close()
            if created is not None:
                # This control addresses the known created UUID even after its
                # DB row was deleted, so the origin reader is released promptly.
                try:http(base+'/proxy/ts/stop/'+created['uuid'],body={},token=admin,method='POST')
                except Exception:pass
                value=http(baseline+root+str(created['id'])+'/',token=admin)
                if value[0]!=404:
                    if value[0]!=200 or value[1].get('name')!=name:raise AssertionError('Unsafe fixture cleanup refused')
                    if http(baseline+root+str(created['id'])+'/',token=admin,method='DELETE')[0]!=204:raise AssertionError('Channel fixture cleanup failed')
    for name,stop,active in [('inactive-stop-delete',True,False),('active-stop-delete',True,True),('active-delete-keeps-stream',False,True)]:
        results={}
        for label,base in [('python',baseline),('rust',candidate)]:
            try:results[label]=scenario(base,label+'-'+name,stop,active)
            except Exception as error:results[label]={'passed':False,'error_type':type(error).__name__}
        record('channel-stop:'+name,all(result['passed'] for result in results.values()),results)
