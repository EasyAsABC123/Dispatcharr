"""Original XC catch-up route capture; never persist credential-bearing URLs."""
import hashlib
import json
from datetime import datetime,timezone
from urllib.request import Request,build_opener
from urllib.error import HTTPError
from urllib.parse import quote,urlencode
import uuid
from direct_probe import NoRedirect,normalized_location,compare


def capture(base,control,label):
    opener=build_opener(NoRedirect());events=[]
    seeded=control('begin',label)
    users=seeded['usernames'];channel=str(seeded['channel_id']);password='xc-fixture-only'
    timestamp='2026-11-09:11-00'
    def path(user='owner',secret=password,stream=None,stamp=timestamp,duration='30'):
        return '/timeshift/'+ '/'.join(quote(value,safe='') for value in [users[user],secret,duration,stamp,(stream or channel)+'.ts'])
    def query(**extra):
        return '/streaming/timeshift.php?'+urlencode({'username':users['owner'],'password':password,'stream':channel,'start':'2026-11-09T11:00:00Z','duration':'30',**extra})
    def fetch(case,target,*,headers=None,method='GET'):
        request=Request(base+target,headers={'User-Agent':'fixture-xc-'+case,**(headers or {})},method=method)
        try:response=opener.open(request,timeout=20)
        except HTTPError as error:response=error
        with response:
            body=response.read(8193)
            if len(body)>8192:raise ValueError('XC capture exceeds bounded body')
            event={'case':case,'method':method,'status':response.status,'headers':{key.lower():(normalized_location(value) if key.lower()=='location' else value) for key,value in response.headers.items() if key.lower() in ('content-type','content-length','content-range','accept-ranges','allow','location','cache-control','www-authenticate','retry-after','x-content-type-options')},'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()}
            if response.status not in (200,206):
                event['body']=body.decode('utf-8','replace')
                if password in event['body']:raise ValueError('XC error unexpectedly echoed fixture credentials')
            events.append(event)
        if response.status==206:control('wait-idle',label)
    try:
        control('proxy',label)
        fetch('query-missing-required','/streaming/timeshift.php')
        fetch('path-wrong-password',path(secret='wrong-fixture-only'))
        fetch('query-wrong-password',query(password='wrong-fixture-only'))
        fetch('path-bad-channel',path(stream='bad'))
        fetch('path-missing-channel',path(stream='2147483647'))
        fetch('path-hidden-channel',path(user='viewer'))
        fetch('path-disabled-catchup',path(user='disabled'))
        fetch('path-invalid-timestamp',path(stamp='bad'))
        fetch('path-mint-proxy',path())
        fetch('query-mint-proxy',query())
        epoch=str(int(datetime(2026,11,9,11,tzinfo=timezone.utc).timestamp()))
        fetch('query-utc-fallback',query(start='',utc=epoch))
        fetch('path-ignores-bad-bearer',path(),headers={'Authorization':'Bearer invalid'})
        fetch('path-client-session',path()+'?session_id='+str(uuid.uuid4()),headers={'Range':'bytes=0-255'})
        fetch('query-client-session',query(session_id=str(uuid.uuid4())),headers={'Range':'bytes=0-255'})
        fetch('inactive-xc-user-still-streams',path(user='inactive')+'?session_id='+str(uuid.uuid4()),headers={'Range':'bytes=0-255'})
        fetch('path-head-mint',path(),method='HEAD')
        control('deny-streams',label)
        fetch('xc-does-not-use-streams-network',path())
        control('allow',label)
        control('deny-user',label)
        fetch('user-network-before-password',path(secret='wrong-fixture-only'))
        control('allow',label)
        control('redirect',label)
        fetch('path-bare-redirect',path())
        fetch('query-bare-redirect',query())
        control('redirect-empty',label)
        fetch('bare-redirect-no-allowed-profile',path())
        control('deny-xc',label)
        fetch('system-network-before-channel-password',path(secret='wrong-fixture-only',stream='bad'))
        fetch('query-required-before-system-network','/streaming/timeshift.php')
        return {'format_version':1,'scope':'original XC catch-up PATH/QUERY authentication, redirects, caller sessions and network precedence','capture_complete':len(events)==23,'events':events,'credentials_and_opaque_locations_normalized':True,'application_parity_verified':False,'performance_verified':False}
    finally:control('restore',label)
