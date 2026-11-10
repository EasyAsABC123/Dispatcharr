"""Pinned original byte-offset helpers, using owned temporary files only.
Original Dispatcharr authors retain all application and algorithm credit.
"""
import argparse,ast,base64,builtins,hashlib,html,json,logging,os,re,tempfile,time
from datetime import datetime,timedelta,timezone as dt_timezone
from pathlib import Path
from lxml import etree
from django.utils import timezone

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    original=Path('/source/apps/epg/tasks.py').read_bytes()
    names={'_build_html_entity_doctype','_parse_programme_element','_decode_channel_id','_find_programme_tag','_programme_to_dict','_read_programs_at_offsets','_scan_from_offset_for_tvg_id','parse_xmltv_time'}
    nodes=[n for n in ast.parse(original).body if isinstance(n,ast.FunctionDef) and n.name in names];assert {n.name for n in nodes}==names
    class Logger:
        def __getattr__(self,name):return lambda *args,**kwargs:None
    scope={'html':html,'etree':etree,'_XML_ENTITIES':{'amp','lt','gt','quot','apos'},'datetime':datetime,'timedelta':timedelta,'dt_timezone':dt_timezone,'timezone':timezone,'time':time,'logger':Logger(),'_CHANNEL_ATTR_RE':re.compile(rb'''channel\s*=\s*(?:"([^"]+)"|'([^']+)')'''),'_PROGRAMME_TAG':b'<programme','_PROGRAMME_TAG_LEN':10,'_TAG_FOLLOW':b' \t\n\r>/','_MAX_START_TAG':4096}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<original byte-offset reader>','exec'),scope);scope['_HTML_ENTITY_DOCTYPE']=scope['_build_html_entity_doctype']()
    def prog(title='current',channel='one',start='20261110110000 +0000',stop='20261110130000 +0000',attrs='',body=None):
        return f'<programme channel="{channel}" start="{start}" stop="{stop}" {attrs}>{body if body is not None else "<title>"+title+"</title>"}</programme>'.encode()
    now='2026-11-10T12:00:00+00:00';bodies=[]
    def add(name,data,offsets=(0,),interleaved=False,tvg='one',at=now):bodies.append((name,data,list(offsets),interleaved,tvg,at))
    add('empty',b'');add('basic',prog());add('title-direct',prog(body='<title> leading <b>nested</b>tail</title><desc> x </desc><sub-title> y </sub-title>'))
    add('no-title',prog(body='<desc/>'));add('namespaced-title',prog(body='<x:title xmlns:x="urn:x">invisible</x:title><title>visible</title>'))
    add('html4',prog(title='&eacute; &amp; &#169;'));add('unknown-entity',prog(title='&unknown;')+prog(title='valid'))
    add('invalid-time',prog(start='bad'));add('invalid-stop',prog(stop='bad'));add('missing-stop-skips-invalid-start',b'<programme channel="one" start="bad"><title>x</title></programme>')
    add('empty-time',prog(start=''));add('invalid-xml-skip',b'<programme channel="one"><title></programme>'+prog())
    add('start-inclusive',prog(),at='2026-11-10T11:00:00+00:00');add('stop-exclusive',prog(),at='2026-11-10T13:00:00+00:00')
    add('timezone-positive',prog(start='20261110120000 +0100',stop='20261110140000 +0100'))
    add('timezone-negative',prog(start='20261110060000 -0500',stop='20261110080000 -0500'))
    add('missing-close',b'<programme channel="one">unfinished');add('false-start',b'<programmeXYZ/>'+prog());add('comment-raw-match',b'<!--'+prog()+b'-->')
    add('other-channel-stops',prog(channel='two')+prog());add('other-channel-forward',prog(channel='two')+prog(),interleaved=True)
    add('space-channel',prog(channel=' one '));add('entity-channel',prog(channel='a&amp;b'),tvg='a&b');add('raw-tvg-not-stripped',prog(),tvg=' one ')
    add('self-closing-prefix',b'<programme channel="one"/>'+prog());add('duplicate-channel',prog(attrs='channel="two"')+prog(title='later'))
    for n in (2*1024*1024-11,2*1024*1024-1,2*1024*1024+3):add('chunk-tag-'+str(n),b' '*n+prog())
    add('tag-too-long',b'<programme '+b' '*4096+prog())
    for i in range(24):
        old=prog(title='old',stop='20261110115959 +0000');other=prog(channel='two');current=prog(title='current-'+str(i));data=old+other+current
        add('offset-'+str(i),data,offsets=([0,len(old)+len(other)] if i%3==0 else [0]),interleaved=bool(i%2))
    add('original-large-programme-description',prog(title='Large Programme Current',body='<title>Large Programme Current</title><desc>'+('x'*(2*1024*1024+1024))+'</desc>'))
    add('large-forward-description',prog(channel='two')+prog(body='<title>Large Forward</title><desc>'+('x'*(2*1024*1024+1024))+'</desc>'),interleaved=True)
    add('default-namespace-fields-not-visible',prog(attrs='xmlns="urn:fixture"'))
    add('channel-quoted-greater-than',prog(channel='one>two'),tvg='one>two')
    add('invalid-utf8-channel-fragment',prog().replace(b'channel="one"',b'channel="\xff"'),tvg='\ufffd')
    controlled={}
    previous=prog(title='past',stop='20261110115959 +0000')
    for name,old,new,offset in [
        ('atomic-replace-before-forward',previous,prog(title='replacement'),0),
        ('atomic-replace-other-channel',prog(channel='two'),prog(title='replacement'),0),
        ('first-hit-does-not-reopen',prog(title='first-hit'),prog(title='replacement'),0),
        ('atomic-replace-forward-missing',previous,previous,0),
        ('atomic-replace-preserves-last-offset',b' '*64+previous,b' '*64+prog(title='replacement'),64),
        ('atomic-delete-before-forward',previous,None,0),
    ]:
        add(name,old,offsets=[offset],interleaved=True)
        controlled[name]={'delete_on_second_open':new is None,'replace_forward_base64':None if new is None else base64.b64encode(new).decode()}
    cases=[]
    with tempfile.TemporaryDirectory(prefix='owned-current-reader-') as directory:
        path=Path(directory)/'guide.xml'
        for name,data,offsets,interleaved,tvg,at in bodies:
            path.write_bytes(data)
            opens=[]
            if name in controlled:
                def controlled_open(filename,*args,**kwargs):
                    if os.fspath(filename)!=os.fspath(path):raise RuntimeError('Only owned guide may be opened')
                    opens.append('open')
                    if len(opens)==2:
                        if controlled[name]['delete_on_second_open']:path.unlink()
                        else:
                            replacement=path.with_name('replacement.xml');replacement.write_bytes(base64.b64decode(controlled[name]['replace_forward_base64']));replacement.replace(path)
                    return builtins.open(filename,*args,**kwargs)
                scope['open']=controlled_open
            try:
                result=scope['_read_programs_at_offsets'](str(path),tvg,offsets,datetime.fromisoformat(at))
                if result is None and interleaved:result=scope['_scan_from_offset_for_tvg_id'](str(path),tvg,offsets[-1],datetime.fromisoformat(at))
                expected={'kind':'timeout'} if result=='timeout' else {'kind':'missing'} if result is None else {'kind':'found','programme':result}
            except Exception as error:expected={'kind':'error','error':type(error).__name__}
            if name in controlled:expected['opens']=len(opens);del scope['open']
            cases.append({'name':name,**controlled.get(name,{}),'input_base64':base64.b64encode(data).decode(),'offsets':offsets,'interleaved':interleaved,'tvg_id':tvg,'now':at,'expected':expected})
    args.output.write_text(json.dumps({'format_version':1,'scope':'Original pure byte-offset current-programme helpers only','source_sha256':hashlib.sha256(original).hexdigest(),'tool_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'cases':cases,'python':os.sys.version,'libxml':etree.LIBXML_VERSION},indent=2)+'\n')
if __name__=='__main__':main()
