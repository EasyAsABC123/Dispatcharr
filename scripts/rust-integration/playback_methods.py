"""Source-backed AllowAny credential/HEAD checks. Original authors retain credit."""
import http.client
import json
import urllib.parse


def probe(base, channel_path, admin_token):
    results=[]
    paths=[channel_path,'/proxy/hls/missing/index.m3u8','/proxy/hls/missing/1.ts','/proxy/ts/status']
    for path in paths:
        for label,token in [('anonymous',None),('invalid','invalid-fixture-token'),('valid',admin_token)]:
            for method in ('HEAD','GET') if '/hls/' in path else ('HEAD',):
                url=urllib.parse.urlsplit(base+path)
                connection=http.client.HTTPConnection(url.hostname,url.port,timeout=5)
                try:
                    connection.request(method,urllib.parse.urlunsplit(('','',url.path,url.query,'')),headers={'Authorization':'Bearer '+token} if token else {})
                    response=connection.getresponse();body=response.read(8192)
                    expected=401 if label=='invalid' or (path=='/proxy/ts/status' and label=='anonymous') else 405 if method=='HEAD' else 410
                    result={'method':method,'path':path,'credentials':label,'status':response.status,'expected':expected,
                            'headers':{name:response.getheader(name) for name in ('Content-Type','WWW-Authenticate','Allow')},'body':body.decode('utf8',errors='replace')}
                    result['passed']=response.status==expected and (method!='HEAD' or not body) and (expected!=401 or response.getheader('WWW-Authenticate')=='Bearer realm="api"')
                    if method=='GET' and expected==410:result['passed']=result['passed'] and json.loads(body)=={'error':'Session expired'}
                    results.append(result)
                finally:connection.close()
    return results
