"""Original native direct-auth wire characterization; never save access tokens."""
import hashlib
import json
from urllib.request import Request,build_opener,HTTPRedirectHandler
from urllib.error import HTTPError
from urllib.parse import parse_qsl,urlencode,urlsplit,urlunsplit
import uuid


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None


def normalized_location(location):
    if not location:return None
    parsed=urlsplit(location)
    path=parsed.path
    if path.startswith('/timeshift/'):
        segments=path.split('/')
        if len(segments)>=4:segments[2:4]=['<provider-user>','<provider-password>']
        path='/'.join(segments)
    query=[]
    for key,value in parse_qsl(parsed.query,keep_blank_values=True):
        if key=='session_id':value='<session>'
        if key=='token':value='<access-token>'
        if key in ('username','password'):value='<provider-credential>'
        query.append((key,value))
    return urlunsplit((parsed.scheme,parsed.netloc,path,urlencode(query),parsed.fragment))


def capture(base,seed,control,label):
    opener=build_opener(NoRedirect())
    path='/proxy/catchup/'+seed['channel_uuid']
    token=seed['users']['owner']['token']
    events=[]
    def fetch(case,path,auth=None,extra=None):
        headers={'User-Agent':'fixture-native-direct-'+case,**(extra or {})}
        if auth:headers['Authorization']='Bearer '+auth
        try:response=opener.open(Request(base+path,headers=headers),timeout=20)
        except HTTPError as error:response=error
        with response:
            payload=response.read(8193)
            if len(payload)>8192:raise ValueError('direct wire capture exceeds bounded body')
            event={'case':case,'status':response.status,'headers':{k.lower():(normalized_location(v) if k.lower()=='location' else v) for k,v in response.headers.items() if k.lower() in ('content-type','content-length','location','cache-control','x-content-type-options','allow')},'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest()}
            if response.status not in (200,206):event['body']=payload.decode('utf-8','replace')
            events.append(event)
            return response.headers.get('Location')
    control('begin',label)
    try:
        control('proxy',label)
        start='?'+urlencode({'start':'2026-11-09T11:00:00Z','duration':'30'})
        fetch('missing-auth',path+start)
        fetch('missing-start',path,token)
        fetch('invalid-start',path+'?start=bad',token)
        fetch('invalid-header-auth',path+start,'invalid')
        fetch('invalid-query-auth',path+start+'&token=invalid')
        fetch('disabled-user',path+start,seed['users']['disabled']['token'])
        fetch('unsupported-channel','/proxy/catchup/'+seed['unsupported_uuid']+start,token)
        fetch('missing-channel','/proxy/catchup/00000000-0000-0000-0000-000000000000'+start,token)
        location=fetch('mint-proxy-header',path+start+'&foo=one&blank=&foo=two',token)
        if location:fetch('mint-is-not-api-capability',location)
        fetch('mint-proxy-query-token',path+start+'&'+urlencode({'token':token}))
        fetch('authenticated-client-session',path+start+'&session_id='+str(uuid.uuid4()),token,{'Range':'bytes=0-255'})
        control('redirect',label)
        fetch('bare-redirect',path+start,token)
        control('redirect-empty',label)
        fetch('bare-redirect-no-allowed-profile',path+start,token)
        control('deny-streams',label)
        fetch('global-network-denial',path+start,token)
        return {'format_version':1,'scope':'native direct-auth validation, bare redirects and authenticated client-supplied session only','capture_complete':len(events)==15,'events':events,'opaque_sessions_and_query_tokens_normalized':True,'application_parity_verified':False,'performance_verified':False}
    finally:control('restore',label)
