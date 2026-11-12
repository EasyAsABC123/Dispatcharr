"""Bounded local-origin readiness/media qualification; runs only on explicit entry."""
import hashlib,json,os,re,urllib.request
PREFIX_SHA='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f'
FILE_SHA='4f3236e89fd6d9fee4f18bb1cd1e9759113a204dc64b74efa7a704fd4c6fc11c'
def validate(stats,status,headers,body):
 vod=stats['vod']
 if vod['fixture_sha256']!=FILE_SHA or type(vod['fixture_bytes'])is not int or vod['fixture_bytes']<=8192:raise ValueError('OriginFixtureBinding')
 if status!=206 or len(body)!=8192 or hashlib.sha256(body).hexdigest()!=PREFIX_SHA:raise ValueError('OriginMediaPrefix')
 match=re.fullmatch(r'bytes 0-8191/([0-9]+)',headers.get('Content-Range',''))
 if match is None or int(match[1])!=vod['fixture_bytes']:raise ValueError('OriginRangeBinding')
 return{'passed':True,'scope':'owned origin readiness and one direct bounded Range preflight, not Source/native playback','fixture_sha256':FILE_SHA,'fixture_bytes':vod['fixture_bytes'],'status':status,'prefix_bytes':8192,'prefix_sha256':PREFIX_SHA,'preflight_origin_GET_requests':1}
def main():
 if os.environ.get('PENDING_MAIN_FIXTURE')!='playback-fixture-only':raise RuntimeError('OwnedOriginFixtureRequired')
 with urllib.request.urlopen('http://origin:8080/stats',timeout=3)as r:
  raw=r.read(65537)
  if len(raw)>65536:raise ValueError('OriginStatsCapacity')
  stats=json.loads(raw)
 request=urllib.request.Request('http://origin:8080/movie/fixture/fixture/fixture.mp4',headers={'Range':'bytes=0-8191'})
 with urllib.request.urlopen(request,timeout=3)as r:proof=validate(stats,r.status,dict(r.headers),r.read(8193))
 print(json.dumps(proof))
if __name__=='__main__':main()
