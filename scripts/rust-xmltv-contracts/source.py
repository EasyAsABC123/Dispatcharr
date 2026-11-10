#!/usr/bin/env python3
"""Actual original XMLTV HTTP/cache characterization; original author credit.

No native equivalence is claimed. Credentials remain memory-only.
"""
import argparse,hashlib,http.client,json,os,shutil,subprocess,sys,time,uuid,xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlencode,quote
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(Path(__file__).resolve().parent),str(ROOT/'scripts/rust-integration'),str(ROOT/'scripts/rust-realtime-contracts')]
from shared_ts import BASELINE,IMAGE,fixture_source,http as ready_http,wait
from fixture_ports import write_port_overlay
from interop import Peer

def exercise(base,control,out):
    seed=control.call({'op':'seed'});rows=[]
    def request(name,path='/output/epg',params=(),role=None,headers=None,method='GET'):
        pairs=list(params)
        if role:
            user=seed['users'][role];pairs.extend([('username',user['username']),('password',user['password'])])
        conn=http.client.HTTPConnection(base.removeprefix('http://'),timeout=20);target=path+('?' +urlencode(pairs) if pairs else '')
        conn.request(method,target,headers={'Host':'xmltv.fixture.invalid:8088',**(headers or {})});response=conn.getresponse();error=None
        try:body=response.read(8*1024*1024)
        except http.client.IncompleteRead as failure:body=failure.partial;error='IncompleteRead'
        assert len(body)<8*1024*1024
        row={'case':name,'path':path,'parameters':list(params),'role':role,'method':method,'status':response.status,'headers':{key.lower():value for key,value in response.getheaders() if key.lower() in ['content-type','content-disposition','cache-control','allow','retry-after']},'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest()}
        if error:row['read_error']=error
        if body.lstrip().startswith(b'<?xml'):
            filename='body-'+str(len(rows)).zfill(3)+'.xml';(out/filename).write_bytes(body);row['body_file']=filename
            try:tree=ET.fromstring(body)
            except ET.ParseError:row['xml_complete']=False
            else:
                row.update(xml_complete=True,root_attributes=tree.attrib,channel_ids=[node.attrib['id'] for node in tree.findall('channel')],programmes=len(tree.findall('programme')),programme_titles=[node.findtext('title') for node in tree.findall('programme')])
        elif body:
            try:row['json']=json.loads(body)
            except (UnicodeError,ValueError):pass
        conn.close();rows.append(row);return row
    snapshots=[]
    def snapshot(name):snapshots.append({'after':name,**control.call({'op':'cache'})})
    snapshot('seed')
    request('normal HEAD cold',method='HEAD');snapshot('normal HEAD cold')
    first=request('normal first');snapshot('normal first')
    second=request('normal replay');assert first['body_sha256']==second['body_sha256'];snapshot('normal replay')
    request('normal HEAD cached',method='HEAD')
    for path in ['/output/epg/','/output/epg/'+quote('XC assigned profile',safe=''),'/output/epg/'+quote('XC assigned profile',safe='')+'/','/output/epg/missing-profile']:
        request('path '+path,path=path)
    request('HEAD missing profile',path='/output/epg/missing-profile',method='HEAD')
    for params in [[('cachedlogos','false')],[('cachedlogos','FALSE')],[('cachedlogos','0')],[('tvg_id_source','tvg_id')],[('tvg_id_source','gracenote')],[('tvg_id_source','invalid')],[('days','1')],[('days','0')],[('days','bad')],[('days','-1')],[('days','١')],[('days','999999999999999999999999')],[('prev_days','1')],[('days','1'),('prev_days','1')],[('prev_days','bad')]]:
        request('query '+repr(params),params=params)
    for role in ['viewer','standard','admin','limited','inactive']:
        request('XC '+role,path='/xmltv.php',role=role)
    request('XC HEAD cold days2',path='/xmltv.php',role='standard',method='HEAD',params=[('days','2')]);snapshot('XC HEAD cold days2')
    request('XC HEAD',path='/xmltv.php',role='standard',method='HEAD');snapshot('XC HEAD')
    request('XC invalid Bearer ignored',path='/xmltv.php',role='standard',headers={'Authorization':'Bearer invalid'})
    request('normal invalid Bearer ignored',headers={'Authorization':'Bearer invalid'})
    request('normal browser accept ignored',headers={'Accept':'text/html'})
    for method in ['OPTIONS','POST']:request('normal method '+method,method=method)
    csrf='A'*32;request('normal valid-CSRF POST',method='POST',headers={'Cookie':'csrftoken='+csrf,'X-CSRFToken':csrf})
    request('XC missing credentials',path='/xmltv.php')
    for values in [{'epg_days':1,'epg_prev_days':1},{'epg_days':0,'epg_prev_days':0}]:
        control.call({'op':'preferences','user':'standard','values':values});request('XC saved window '+repr(values),path='/xmltv.php',role='standard')
    control.call({'op':'preferences','user':'standard','values':{}})
    control.call({'op':'mutate_program','title':'Mutated guide title'})
    cached=request('mutation before invalidation');assert cached['body_sha256']==first['body_sha256'];snapshot('mutation before invalidation')
    control.call({'op':'invalidate'});fresh=request('mutation after invalidation');assert fresh['body_sha256']!=first['body_sha256'];snapshot('mutation after invalidation')
    control.call({'op':'network','values':{'M3U_EPG':'192.0.2.0/24'}})
    request('normal network deny');request('normal HEAD network deny',method='HEAD');request('XC bypasses M3U_EPG',path='/xmltv.php',role='standard')
    control.call({'op':'network','values':{'M3U_EPG':'0.0.0.0/0,::/0','XC_API':'192.0.2.0/24'}})
    request('XC network deny',path='/xmltv.php',role='standard');request('normal ignores XC_API')
    control.call({'op':'network','values':{'XC_API':'0.0.0.0/0,::/0'}});snapshot('finished')
    return rows,snapshots

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();docker=shutil.which('docker');assert docker
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True);assert not any(out.iterdir())
    report={'scope':'actual original XMLTV HTTP/cache/real+defaultdummy characterization only','passed':False,'native_verified':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'commands':[]}
    tools=[Path(__file__).resolve(),Path(__file__).with_name('control.py'),Path(__file__).with_name('render_source.py'),Path(__file__).with_name('cache_source.py'),ROOT/'scripts/rust-xc-contracts/control.py',ROOT/'scripts/rust-realtime-contracts/interop.py',ROOT/'scripts/rust-integration/shared_ts.py',ROOT/'scripts/rust-integration/fixture_ports.py',ROOT/'docker/compose.rust-playback.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']],ROOT/'scripts/rust-baseline/dependencies.py']
    copies=out/'tools'
    for path in tools:target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256((copies/path.relative_to(ROOT)).read_bytes()).hexdigest() for path in tools}
    project='dispatcharr-xmltv-'+uuid.uuid4().hex[:12];report['project']=project;control=None
    overlay=out/'overlay.json';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(copies/'scripts/rust-xmltv-contracts')+':/xmltv-probe:ro',str(copies/'scripts/rust-xc-contracts')+':/xc-probe:ro']}}}));ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080]})
    with fixture_source(report,out,'xmltv-source.json') as temporary:
        source=Path(temporary)/'source';source.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb') as file:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=file,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True);(source/'media').mkdir(exist_ok=True)
        env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='180')
        compose=[docker,'compose','-p',project,*[value for path in [copies/'docker/compose.rust-playback.yml',overlay,ports] for value in ['-f',str(path)]]]
        def command(values,timeout=120):report['commands'].append(values);return subprocess.run(values,cwd=ROOT,env=env,text=True,capture_output=True,check=True,timeout=timeout).stdout
        try:
            command(compose+['up','-d','baseline','origin'],180);base='http://'+command(compose+['port','baseline','5656']).strip();wait(lambda:(out/'seed.json').exists() and ready_http(base+'/proxy/ts/stream/missing')[0]==404,300,'original ready')
            control=Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/xmltv-probe/control.py'],env,out/'control.log');report['cases'],report['cache_snapshots']=exercise(base,control,out);report['render_vectors']=control.call({'op':'render_vectors'});report['cache_vectors']=control.call({'op':'cache_vectors'});report['passed']=True
        except Exception as error:report.update(failure=type(error).__name__,failure_message=str(error))
        finally:
            if control:control.close()
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='owned Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'cases':len(report.get('cases',[])),'output':str(out)}));return 0 if report['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
