"""Actual HTTP XC short/simple EPG wire oracle. Original Dispatcharr credit."""
import hashlib,http.client,json
from urllib.parse import urlencode

def exercise(base,control):
    seed=control.call({'op':'seed'});rows=[];stored={str(pk)for pk in seed['programs']}
    def request(name,pairs=(),role='standard',method='GET',headers=None):
        parameters=list(pairs)
        if role is not None:
            user=seed['users'][role];parameters.extend([('username',user['username']),('password',user['password'])])
        target='/player_api.php?'+urlencode(parameters)
        conn=http.client.HTTPConnection(base.removeprefix('http://'),timeout=15);conn.request(method,target,headers={'Host':'xc.fixture.invalid:8088',**(headers or {})});response=conn.getresponse();body=response.read(16*1024*1024);assert len(body)<16*1024*1024;conn.close()
        result={'case':name,'method':method,'params':list(pairs),'role':role,'status':response.status,'headers':{key.lower():value for key,value in response.getheaders()if key.lower()in ['content-type','allow','retry-after']},'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest()}
        if body:
            try:parsed=json.loads(body)
            except (ValueError,UnicodeError):result['body_text']=body.decode('utf-8')
            else:
                if isinstance(parsed,dict)and'epg_listings'in parsed:
                    synthetic=False
                    for row in parsed['epg_listings']:
                        if row['id']not in stored:row['id']='<process-random-synthetic>';synthetic=True
                    if synthetic:
                        # Only generated IDs are normalized, with original
                        # Python JSON serialization of every other field.
                        result['synthetic_ids_normalized']=True
                        result['comparison_body']=json.dumps(parsed)
                        result.pop('body_bytes');result.pop('body_sha256')
                result['json']=parsed
        rows.append(result)
    actions=['get_short_epg','get_simple_data_table']
    for role in ['viewer','standard','admin','limited','inactive']:
        for index in [0,1,3,6,7,9]:
            for action in actions:request(role+' '+str(index)+' '+action,[('action',action),('stream_id',str(seed['channels'][index]))],role)
    for action in actions:
        for value in ['0','1','-1','bad','١','1_0']:request('limit '+value+' '+action,[('action',action),('stream_id',str(seed['channels'][0])),('limit',value)])
        for raw in ['', 'bad','999999999999999999999999']:
            request('stream '+raw+' '+action,[('action',action),('stream_id',raw)])
        request('missing stream '+action,[('action',action)])
        request('missing credentials '+action,[('action',action),('stream_id',str(seed['channels'][0]))],None)
        request('invalid presented Bearer ignored '+action,[('action',action),('stream_id',str(seed['channels'][0]))],headers={'Authorization':'Bearer invalid'})
        request('HEAD '+action,[('action',action),('stream_id',str(seed['channels'][0]))],method='HEAD')
        target=str(seed['channels'][0]);arabic=''.join(chr(0x660+int(ch))for ch in target)
        request('Arabic stream spelling '+action,[('action',action),('stream_id',arabic)])
    return rows
