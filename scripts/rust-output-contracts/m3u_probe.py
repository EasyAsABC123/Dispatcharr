"""Actual original M3U wire capture; never persist credential-bearing URLs/bodies."""
import hashlib
import json
import time
from urllib.request import Request,build_opener,HTTPRedirectHandler
from urllib.error import HTTPError
from urllib.parse import urlencode,quote,urlsplit,parse_qsl,unquote
import re


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args):return None


def capture(base,control,label,error_path=None):
    opener=build_opener(NoRedirect());events=[]
    fixture=control('begin',label)
    mode={'catchup':True,'allow_none':False}
    profile=fixture['profile_name'];users=fixture['users'];password='export-fixture-only'
    def xc(kind='standard',secret=password,**params):return '/get.php?'+urlencode({'username':users[kind],'password':secret,**params})
    def record(case,path='/output/m3u',*,method='GET',raw=None,headers=None,suffix=True):
        if suffix:path+=('&' if '?' in path else '?')+'_fixture_case='+quote(case)
        request=Request(base+path,data=raw,method=method,headers={'Host':'export.fixture.invalid:8088','User-Agent':'export-fixture-'+case,**(headers or {})})
        try:response=opener.open(request,timeout=15)
        except HTTPError as error:response=error
        with response:
            body=response.read(1_048_577)
            if len(body)>1_048_576:raise RuntimeError('bounded M3U capture exceeded')
            if error_path is not None and case=='post-empty' and response.status==403 and b'CSRF verification failed' in body:
                text=body.decode('utf-8')
                if password in text or any(user in text for user in users.values()):raise RuntimeError('unexpected credential echo in CSRF template')
                error_path.write_bytes(body)
            if error_path is not None and case=='post-invalid-csrf' and response.status==403 and b'CSRF verification failed' in body:
                error_path.with_name('source-csrf-invalid-token.html').write_bytes(body)
            event={'case':case,'method':method,'status':response.status,'headers':{key.lower():value for key,value in response.headers.items() if key.lower() in ('content-type','content-length','content-disposition','allow','cache-control','etag','www-authenticate','retry-after','x-content-type-options','location')},'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()}
            if response.status==200 and method!='HEAD':
                text=body.decode('utf-8');event['channels']=text.count('#EXTINF:');event['starts_with_extm3u']=text.startswith('#EXTM3U ')
                event['catchup_default']=text.count('catchup="default"');event['catchup_xc']=text.count('catchup="xc"');event['radio']=text.count('radio="true"')
                parsed=urlsplit(path);pairs=parse_qsl(parsed.query,keep_blank_values=True)
                username=dict(pairs).get('username') if parsed.path=='/get.php' else None
                kind=next((kind for kind,name in users.items() if name==username),None)
                epg_match=re.search(r'x-tvg-url="([^"]*)"',text)
                origin=urlsplit(epg_match.group(1))
                if not case.startswith('cache-'):event['render_fixture']={'query':[(key,value) for key,value in pairs if key not in ('username','password')],'user_kind':kind,'profile_name':unquote(parsed.path[len('/output/m3u/'):]).rstrip('/') or None if parsed.path.startswith('/output/m3u/') else None,'origin':origin.scheme+'://'+origin.netloc,'catchup':mode['catchup'],'provider_allow_none':mode['allow_none']}
            elif response.headers.get('Content-Type','').startswith('application/json'):event['body']=json.loads(body) if body else None
            events.append(event)
        return event
    try:
        record('anonymous-basic');record('anonymous-trailing','/output/m3u/')
        record('profile','/output/m3u/'+profile);record('profile-trailing','/output/m3u/'+profile+'/')
        for case,name in [('profile-ampersand','Kids & News'),('profile-plus','Plus+News'),('profile-unicode','Café 東京'),('profile-percent-once','Percent%26News')]:record(case,'/output/m3u/'+quote(name,safe='+&'))
        record('unknown-profile','/output/m3u/does-not-exist');record('unknown-profile-head','/output/m3u/does-not-exist',method='HEAD')
        record('head',method='HEAD');record('options',method='OPTIONS');record('put',method='PUT')
        record('post-empty',method='POST',raw=b'');record('post-braces',method='POST',raw=b'{}');record('post-spaced-braces',method='POST',raw=b'{} ')
        csrf={'Cookie':'csrftoken='+'a'*32,'X-CSRFToken':'a'*32}
        record('post-valid-csrf-empty',method='POST',raw=b'',headers=csrf)
        record('post-valid-csrf-braces',method='POST',raw=b'{}',headers=csrf)
        record('post-valid-csrf-spaced',method='POST',raw=b'{} ',headers=csrf)
        record('put-valid-csrf',method='PUT',headers=csrf)
        record('post-invalid-csrf',method='POST',raw=b'{}',headers={**csrf,'X-CSRFToken':'b'*32})
        record('anonymous-bearer-ignored',headers={'Authorization':'Bearer invalid'})
        record('anonymous-query-auth-ignored','/output/m3u?username=wrong&password=wrong')
        for case,query in [('direct','direct=true'),('uncached-logos','cachedlogos=false'),('tvg-id','tvg_id_source=tvg_id'),('gracenote','tvg_id_source=gracenote'),('output-legacy','output=hls&output_profile=23'),('output-format-wins','output=hls&output_format=fmp4&output_profile=a%2Ab~+c'),('epg-params','prev_days=2&tvg_id_source=tvg_id&cachedlogos=false&days=3'),('duplicates','output_format=ts&output_format=hls&tvg_id_source=bad&tvg_id_source=gracenote')]:record(case,'/output/m3u/'+profile+'?'+query)
        record('trusted-forwarded-origin',headers={'X-Forwarded-Host':'forwarded.fixture.invalid:443','X-Forwarded-Proto':'https'})
        record('xc-missing','/get.php');record('xc-wrong',xc(secret='wrong-fixture-only'))
        for kind in ['standard','admin','inactive']:record('xc-'+kind,xc(kind))
        record('xc-standard-direct-refused',xc(direct='true'));record('xc-admin-direct',xc('admin',direct='true'))
        record('xc-head',xc(),method='HEAD');record('xc-post',xc(),method='POST',raw=b'{}')
        control('catchup-off',label);mode['catchup']=False;record('xc-catchup-disabled',xc());control('catchup-on',label);mode['catchup']=True
        control('admin-allow-none',label);mode['allow_none']=True;record('xc-direct-empty-allowlist',xc('admin',direct='true'));control('admin-unrestricted',label);mode['allow_none']=False
        cache_path='/output/m3u/'+profile+'?_fixture_case=cache-probe'
        cold=record('cache-cold',cache_path,suffix=False)
        began=time.monotonic()
        control('mutate-name',label)
        warm=record('cache-warm-stale',cache_path,suffix=False)
        warm_inside_ttl=time.monotonic()-began<2
        time.sleep(2.05)
        expired=record('cache-expired-new-name',cache_path,suffix=False)
        cache_staleness={'warm_probe_inside_two_seconds':warm_inside_ttl,'cached_body_unchanged':cold['sha256']==warm['sha256'],'expired_body_changed':cold['sha256']!=expired['sha256']}
        control('restore-name',label)
        control('deny-m3u',label);record('m3u-network-before-missing-profile','/output/m3u/missing');record('xc-not-m3u-network',xc());control('allow',label)
        control('deny-xc',label);record('xc-network-before-password',xc(secret='wrong-fixture-only'));record('anonymous-not-xc-network');control('allow',label)
        cache=control('cache-roundtrip',label)['cache']
        cache_interop=cache['python_type']=='str' and cache['sha256']==events[-1]['sha256']
        system_events=control('events',label)['counts']
        return {'format_version':1,'scope':'Original anonymous and XC M3U HTTP/export-byte contracts','capture_complete':len(events)==50 and cache_interop and all(cache_staleness.values()),'events':events,'django_cache_string_interop':cache_interop,'two_second_cache':cache_staleness,'system_event_counts':system_events,'sensitive_request_urls_and_export_bodies_not_persisted':True,'application_parity_verified':False,'performance_verified':False}
    finally:control('restore',label)


def compare(source,candidate):
    differences=[]
    if not source.get('capture_complete') or not candidate.get('capture_complete'):differences.append({'case':'capture','detail':'incomplete'})
    if len(source.get('events',[]))!=len(candidate.get('events',[])):differences.append({'case':'capture','detail':'event count mismatch'})
    for left,right in zip(source['events'],candidate['events']):
        a={key:value for key,value in left.items() if key!='render_fixture'};b={key:value for key,value in right.items() if key!='render_fixture'}
        # Existing application JSON error contracts compare objects; each
        # transport Content-Length is still checked against its own bytes.
        if 'body' in a and isinstance(a['body'],(dict,list)):
            for value in [a,b]:
                length=value['headers'].get('content-length')
                if length is not None and int(length)!=value['bytes']:differences.append({'case':left['case'],'field':'invalid Content-Length'})
                for key in ['bytes','sha256']:value.pop(key,None)
                value['headers']=dict(value['headers']);value['headers'].pop('content-length',None)
        if a!=b:differences.append({'case':left['case'],'source':a,'candidate':b})
    for key in ['django_cache_string_interop','two_second_cache','system_event_counts','large_catalog']:
        if source.get(key)!=candidate.get(key):differences.append({'case':key,'source':source.get(key),'candidate':candidate.get(key)})
    return {'captured_m3u_http_matches':not differences,'differences':differences,'normalizations':['JSON error objects compared semantically; their own Content-Length remains validated'],'application_parity_verified':False,'performance_verified':False}


def capture_large(base,control,label,error_path=None):
    """A real multi-megabyte catalog, including original Django cache reads."""
    control('begin',label)
    events=[]
    def record(case):
        request=Request(base+'/output/m3u/rust-export-large?_fixture_case=large-catalog',headers={'Host':'export.fixture.invalid:8088','User-Agent':'large-export-'+case})
        try:response=build_opener(NoRedirect()).open(request,timeout=30)
        except HTTPError as error:response=error
        with response:
            body=response.read(16*1024*1024+1)
            if len(body)>16*1024*1024:raise RuntimeError('large catalog capture exceeded 16MiB')
            event={'case':case,'method':'GET','status':response.status,'headers':{key.lower():value for key,value in response.headers.items() if key.lower() in ('content-type','content-length','content-disposition','cache-control','etag','x-content-type-options')},'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'channels':body.count(b'#EXTINF:'),'render_fixture':{'query':[['_fixture_case','large-catalog']],'user_kind':None,'profile_name':'rust-export-large','origin':'http://export.fixture.invalid:8088','catchup':True,'provider_allow_none':False}}
            events.append(event)
            return event
    try:
        fixture=control('large-catalog',label)['large_catalog']
        cold=record('large-cold')
        cache=control('large-cache-read',label)['cache']
        warm=record('large-warm')
        time.sleep(2.05)
        expired=record('large-expired')
        counts=control('events',label)['counts']
        interop=cache['python_type']=='str' and cache['bytes']==cold['bytes'] and cache['sha256']==cold['sha256']
        valid=all(event['status']==200 and event['bytes']>=2*1024*1024 and event['channels']==6000 for event in events)
        return {'format_version':1,'scope':'6000-channel multi-megabyte M3U export and Django string-cache interoperability','large_catalog':fixture,'capture_complete':valid and interop and cold['sha256']==warm['sha256']==expired['sha256'] and counts=={'m3u_download':2,'m3u_blocked':0},'events':events,'django_cache_string_interop':interop,'cache_representation':cache,'system_event_counts':counts,'application_parity_verified':False,'performance_verified':False,'sensitive_request_urls_and_export_bodies_not_persisted':True}
    finally:control('restore',label)
