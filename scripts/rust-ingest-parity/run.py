#!/usr/bin/env python3
"""Compare bounded Rust M3U parsing against unchanged source AST functions."""
import argparse
import ast
import copy
import hashlib
import gzip
import io
import json
import logging
import lzma
import math
import os
from pathlib import Path
import random
import re
import subprocess
import sys
import tempfile
import time
import types
import unicodedata
import warnings
import zipfile

ROOT=Path(__file__).resolve().parents[2]


def original():
    scope={'re':re,'logger':logging.getLogger('original-m3u')}
    scope['logger'].disabled=True
    tasks=ROOT/'apps/m3u/tasks.py';utils=ROOT/'apps/m3u/utils.py'
    scope['logger'].trace=lambda *args,**kwargs:None
    for path,names in [(utils,{'normalize_stream_url','parse_is_adult'}),(tasks,{'parse_extinf_line','iter_m3u_entries','get_case_insensitive_attr'}),(ROOT/'core/utils.py',{'truncate_with_warning'})]:
        tree=ast.parse(path.read_text())
        nodes=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in names]
        if path==tasks:
            nodes[:0]=[node for node in tree.body if isinstance(node,ast.Assign) and any(isinstance(target,ast.Name) and target.id=='_EXTINF_ATTR_RE' for target in node.targets)]
        exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),scope)
    tree=ast.parse((ROOT/'apps/channels/models.py').read_text())
    stream=next(node for node in tree.body if isinstance(node,ast.ClassDef) and node.name=='Stream')
    stream.bases=[];stream.keywords=[]
    stream.body=[node for node in stream.body if isinstance(node,ast.FunctionDef) and node.name=='generate_hash_key']
    utility_tree=ast.parse(utils.read_text())
    utility_nodes=[node for node in utility_tree.body if (isinstance(node,ast.FunctionDef) and node.name=='parse_m3u_hash_keys') or (isinstance(node,ast.Assign) and any(isinstance(target,ast.Name) and target.id=='M3U_HASH_KEY_FIELDS' for target in node.targets))]
    utility_scope={}
    exec(compile(ast.Module(body=utility_nodes,type_ignores=[]),str(utils),'exec'),utility_scope)
    sys.modules['apps.m3u.utils']=types.SimpleNamespace(parse_m3u_hash_keys=utility_scope['parse_m3u_hash_keys'])
    scope.update(json=json,hashlib=hashlib)
    exec(compile(ast.Module(body=[stream],type_ignores=[]),'<original-stream-hash>','exec'),scope)
    fetch=next(node for node in ast.parse(tasks.read_text()).body if isinstance(node,ast.FunctionDef) and node.name=='fetch_m3u_lines')
    nodes=list(ast.walk(fetch))
    def assignment(name):
        return next(node for node in nodes if isinstance(node,ast.Assign) and any(isinstance(target,ast.Name) and target.id==name for target in node.targets) and (name!='is_valid_m3u' or isinstance(node.value,ast.Constant) and node.value.value is False))
    ready=next(node for node in nodes if isinstance(node,ast.If) and ast.unparse(node.test).startswith('head_lines and'))
    error=next(node for node in nodes if isinstance(node,ast.If) and isinstance(node.test,ast.BoolOp) and isinstance(node.test.values[0],ast.Compare) and isinstance(node.test.values[0].left,ast.Constant) and node.test.values[0].left.value=='<html')
    validation_nodes=[assignment(name) for name in ['head_str','head_lines','head_lower','is_valid_m3u']]+[ready]
    validation_nodes.append(ast.If(test=ast.UnaryOp(op=ast.Not(),operand=ast.Name(id='is_valid_m3u',ctx=ast.Load())),body=[error],orelse=[]))
    code=compile(ast.fix_missing_locations(ast.Module(body=validation_nodes,type_ignores=[])),'<original-download-head-validation>','exec')
    def validation(data,url):
        values={'head_bytes':data[:32768],'account':types.SimpleNamespace(server_url=url),'error_msg':None,'logger':scope['logger']}
        exec(code,values)
        return {'valid':values['is_valid_m3u'],'error':values['error_msg']}
    scope['validate_head']=validation
    batch_function=next(node for node in ast.parse(tasks.read_text()).body if isinstance(node,ast.FunctionDef) and node.name=='process_m3u_batch_direct')
    loop=next(node for node in batch_function.body if isinstance(node,ast.For) and isinstance(node.target,ast.Name) and node.target.id=='stream_info')
    projection=compile(ast.Module(body=[loop],type_ignores=[]),'<original-import-row-projection>','exec')
    def project(entry,context):
        values=dict(scope,batch=[entry],account_id=context['account_id'],account=types.SimpleNamespace(account_type=context['account_type']),
                    groups=context['groups'],hash_keys=context['hash_keys'],name_max_length=context['max_name_characters'],compiled_filters=[],stream_hashes={},
                    M3UAccount=types.SimpleNamespace(Types=types.SimpleNamespace(XC='XC')))
        exec(projection,values)
        rows=list(values['stream_hashes'].values())
        if not rows:return None
        row={key:value for key,value in rows[0].items() if key not in {'m3u_account','is_stale'}}
        number=row.get('stream_chno')
        if isinstance(number,float) and not math.isfinite(number):
            row['stream_chno']='__float:'+('nan' if math.isnan(number) else ('inf' if number>0 else '-inf'))
        return row
    scope['project']=project
    fetch_code=compile(ast.Module(body=[fetch],type_ignores=[]),str(tasks),'exec')
    def cache_download(chunks,status,url):
        with tempfile.TemporaryDirectory(prefix='dispatcharr-source-cache-') as directory:
            path=Path(directory)/'1.m3u';path.write_bytes(b'previous cache')
            messages=[]
            response=types.SimpleNamespace(status_code=status,headers={},url=url,raise_for_status=lambda:None,
                                           iter_content=lambda **kwargs:iter(chunks))
            account=types.SimpleNamespace(id=1,name='fixture',server_url=url,file_path=None,
                                          get_user_agent_string=lambda:'fixture-agent',save=lambda **kwargs:None)
            values=dict(scope,os=os,time=time,m3u_dir=directory,
                        requests=types.SimpleNamespace(get=lambda *args,**kwargs:response,
                            exceptions=types.SimpleNamespace(HTTPError=type('HTTPError',(Exception,),{}),RequestException=type('RequestException',(Exception,),{}))),
                        M3UAccount=types.SimpleNamespace(Status=types.SimpleNamespace(FETCHING='fetching',ERROR='error')),
                        send_m3u_update=lambda *args,**kwargs:None,
                        _set_m3u_account_status=lambda account_id,status,message,**kwargs:messages.append(message))
            exec(fetch_code,values)
            _,success=values['fetch_m3u_lines'](account)
            return {'success':success,'error':None if success else messages[-1],
                    'lines':sum(1 for _ in path.open('rb')) if success else None,
                    'cache_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
                    'temporary_exists':Path(str(path)+'.tmp').exists()}
    scope['cache_download']=cache_download
    open_source=next(node for node in ast.parse(tasks.read_text()).body if isinstance(node,ast.FunctionDef) and node.name=='_open_m3u_text_source')
    def uploaded(data,suffix):
        with tempfile.TemporaryDirectory(prefix='dispatcharr-source-upload-') as directory:
            path=Path(directory)/('fixture'+suffix);path.write_bytes(data)
            account=types.SimpleNamespace(id=1,name='fixture',server_url=None,file_path=str(path))
            values=dict(scope,os=os,gzip=gzip,lzma=lzma,zipfile=zipfile,m3u_dir=directory,
                        M3UAccount=types.SimpleNamespace(Status=types.SimpleNamespace(ERROR='error')),
                        _set_m3u_account_status=lambda *args,**kwargs:None)
            exec(fetch_code,values)
            exec(compile(ast.Module(body=[open_source],type_ignores=[]),str(tasks),'exec'),values)
            try:
                source,success=values['fetch_m3u_lines'](account)
                if not success:return {'success':False}
                if isinstance(source,list):entries=list(scope['iter_m3u_entries'](source))
                else:
                    with values['_open_m3u_text_source'](source) as reader:
                        entries=list(scope['iter_m3u_entries'](reader))
                return {'success':True,'entries':entries}
            except Exception:return {'success':False}
    scope['uploaded']=uploaded
    refresh=next(node for node in ast.parse(tasks.read_text()).body if isinstance(node,ast.FunctionDef) and node.name=='refresh_m3u_groups')
    group_loop=copy.deepcopy(next(node for node in ast.walk(refresh) if isinstance(node,ast.For) and isinstance(node.iter,ast.Name) and node.iter.id=='entry_iter'))
    group_code=compile(ast.Module(body=[group_loop],type_ignores=[]),str(tasks),'exec')
    def prepared(data):
        reader=io.TextIOWrapper(io.BytesIO(data),encoding='utf-8',errors='ignore',newline=None)
        values=dict(scope,entry_iter=scope['iter_m3u_entries'](reader),valid_stream_count=0,
                    extinf_data=[],groups={'Default Group':{}},account_id=1,account=types.SimpleNamespace(id=1))
        exec(group_code,values)
        return {'cache':{'extinf_data':values['extinf_data'],'groups':values['groups']},
                'groups':list(values['groups']),'entries':values['valid_stream_count']}
    scope['prepared']=prepared
    return scope


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary',type=Path,default=ROOT/'rust/target/debug/examples/ingest_parity')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--capture-only',action='store_true',help='Capture actual source goldens without executing Rust')
    parser.add_argument('--goldens',type=Path,help='Compare against a separately captured original-runtime golden file')
    args=parser.parse_args();oracle=original();rng=random.Random(20261109)
    cases=[]
    lines=['#EXTINF:-1,Title','#EXTINF:12','#EXTINF:-1 tvg-name="EPG",Title',
           "#EXTINF:-1 TVG-NAME='EPG' tvc-guide-title='Guide',",'#EXTINF:-1 group-title="",',
           '#EXTINF:-1 tvg-name="first" TVG-NAME="last",', '#extinf:-1,ignored',
           '#EXTINF:-1 a="comma,in-value",Comma,in,title','#EXTINF:-1 a="x\ny",name',
           '#EXTINF:\x1c-1\x1fa="one"\x1d,\x1eDisplay\x1f']
    for _ in range(800):
        tokens=[]
        for _ in range(rng.randrange(8)):
            key=rng.choice(['tvg-name','TVG-NAME','TVC-GUIDE-TITLE','group-title','tvg-id','other','İD','a=b'])
            quote=rng.choice(['"',"'"]);value=rng.choice(['','hello','comma,name','😀','a=b','name with spaces',"a'b",'a"b'])
            tokens.append(key+rng.choice(['=',' = ','\t=\x1f'])+quote+value+quote)
        lines.append('#EXTINF:'+rng.choice(['-1','0',''])+' '+ ' '.join(tokens)+rng.choice([',Display',',',' trailing name','']))
    for line in lines:
        cases.append(({'kind':'extinf','line':line},oracle['parse_extinf_line'](line)))
    for _ in range(160):
        playlist=['#EXTM3U']
        for _ in range(rng.randrange(1,15)):
            playlist.extend([rng.choice(lines),rng.choice(['','#EXTGRP: Group','#EXTVLCOPT: http-referrer=fixture','#KODIPROP:ignored','#EXTINF malformed']),rng.choice(['https://fixture/live','udp://@239.0.0.1:1234','HTTP://ignored','rtspGarbage','rtp://fixture','not-a-url'])])
        cases.append(({'kind':'playlist','lines':playlist},list(oracle['iter_m3u_entries'](playlist))))
    for _ in range(500):
        values=['','Name','Café','😀','\x00\x1f\x7f','Quote" slash\\ /','漢字','İstanbul']
        identity={'name':rng.choice(values),'url':rng.choice(['https://one/user/password/1.ts','udp://@239.0.0.1:1234','']),
                  'tvg_id':rng.choice(values),'m3u_id':rng.choice([None,0,1,-1,9223372036854775807]),
                  'group':rng.choice([None,*values]),'account_type':rng.choice(['XC','STD','xc','']),
                  'stream_id':rng.choice([None,0,1,-1,-9223372036854775808,9223372036854775807])}
        keys=[rng.choice(['name','url','tvg_id','m3u_id','group','unknown']) for _ in range(rng.randrange(10))]
        cases.append(({'kind':'hash','identity':identity,'keys':keys},oracle['Stream'].generate_hash_key(**identity,keys=keys)))
    samples=[b'',b'  ',b'#extm3u\n',b'#EXTM3Ugarbage',b'#EXTINF:-1,Title',b'httpNotAUrl',b'HTTP://not-recognized',b'<HTML>error',b'not found',b'\xff\xfe',bytes(range(256)),b'\x1chttp://fixture\x1f',b' '*32768+b'#EXTM3U']
    for _ in range(250):
        data=b'\n'.join(rng.choices(samples[:-1],k=rng.randrange(1,5)))
        samples.append(data) if len(samples)<25 else None
        url='http://fixture/playlist.m3u'
        cases.append(({'kind':'validation','bytes':list(data),'url':url},oracle['validate_head'](data,url)))
    for data in samples[:13]:
        url='http://fixture/playlist.m3u'
        cases.append(({'kind':'validation','bytes':list(data),'url':url},oracle['validate_head'](data,url)))
    for index in range(400):
        attrs={'tvg-id':'fixture-'+str(index),'tvg-logo':'http://fixture/logo.png',
               'tv_archive':rng.choice(['','0','1','True','true','TRUE']),
               'tv_archive_duration':rng.choice(['','0','7','-1','٠١_٢','bad']),
               'is_adult':rng.choice(['','0','1','True','١','+0001','²']),
               'radio':rng.choice(['','0','1','True','TRUE']),
               'stream_type':rng.choice(['live','radio_streams','RADIO_STREAMS']),
               'stream_id':rng.choice(['','0','-1','17','١٧','1_000','bad']),
               'num':rng.choice(['','0','2.1','٢_٠.٥','1e2','nan','-Infinity','bad'])}
        if index%3:attrs['group-title']=rng.choice(['Group','','Disabled'])
        if index%2:attrs['tvg-chno']=rng.choice(['','2','3.2','１２','nan','infinity','bad'])
        attrs['channel-number']='7.5'
        entry={'name':rng.choice(['Title','Café 😀'*4,'','x'*520]),'display_name':'Fixture','attributes':attrs,'url':'http://fixture/live'}
        if index%7==0:entry['vlc_opts']=[' http-referrer=http://fixture','network-caching=1000']
        if index%11==0:entry['url']='http://fixture/'+'😀'*4097
        context={'account_id':12,'account_type':rng.choice(['STD','XC','xc']),'groups':{'Default Group':7,'Group':8,'':9},
                 'hash_keys':rng.choice([['name','url','tvg_id','m3u_id','group'],['url'],['tvg_id'],[]]),'max_name_characters':rng.choice([0,3,512])}
        cases.append(({'kind':'projection','entry':entry,'context':context},oracle['project'](entry,context)))
    cache_samples=[b'',b'#EXTM3U',b'#EXTM3U\nhttp://fixture\n',b'#EXTM3U\rhttp://fixture\r',
                   b'#EXTM3U\r\nhttp://fixture\r\n',b'<html>error',b'error: unauthorized',b'   ',
                   b'\xff#EXTM3U\n',b'\xff\xfe',b' '*32768+b'#EXTM3U',b'#EXTM3U'+b'x'*65536,
                   b'<html>\nhttp://fixture/error-page-link',b'HTTP://fixture',b'udpGarbage']
    for status in [200,206]:
        for data in cache_samples:
            chunks=[data[index:index+8192] for index in range(0,len(data),8192)]
            url='http://fixture/playlist.m3u'
            cases.append(({'kind':'cache','chunks':[list(chunk) for chunk in chunks],'status':status,'url':url},oracle['cache_download'](chunks,status,url)))
    playlist=b'#EXTM3U\r\n#EXTINF:-1 tvg-id="one",Caf\xc3\xa9\r\nhttp://fixture/one\r\n'
    second=b'#EXTINF:-1,Two\nhttp://fixture/two\n'
    uploads=[('.m3u',playlist),('.m3u',b''),('.m3u',b'\xff'+playlist),('.m3u',playlist.replace(b'\r\n',b'\r')),
             ('.gz',gzip.compress(playlist)),('.gz',gzip.compress(playlist)+gzip.compress(second)),
             ('.gz',b''),('.gz',gzip.compress(b'')),('.gz',gzip.compress(playlist)[:-4]),
             ('.gz',gzip.compress(playlist)+b'junk'),('.GZ',gzip.compress(playlist)),
             ('.xz',lzma.compress(playlist)),('.xz',lzma.compress(playlist)+lzma.compress(second)),
             ('.xz',b''),('.xz',lzma.compress(b'')),('.xz',lzma.compress(playlist)[:-4]),
             ('.xz',lzma.compress(playlist)+b'junk'),('.xz',lzma.compress(playlist)+b'\0'*4),
             ('.xz',lzma.compress(playlist,format=lzma.FORMAT_ALONE)),
             ('.xz',lzma.compress(playlist)+lzma.compress(second,format=lzma.FORMAT_ALONE))]
    for compression in [zipfile.ZIP_STORED,zipfile.ZIP_DEFLATED,zipfile.ZIP_BZIP2,zipfile.ZIP_LZMA]:
        for members in [[('one.m3u',playlist)],[('upper.M3U',second),('nested/one.m3u',playlist)],
                        [('one.m3u',playlist),('one.m3u',second),('two.m3u',playlist)],
                        [('one.M3U',playlist)],[]]:
            buffer=io.BytesIO()
            with warnings.catch_warnings():
                warnings.simplefilter('ignore',UserWarning)
                with zipfile.ZipFile(buffer,'w',compression=compression) as archive:
                    for name,data in members:archive.writestr(name,data)
            uploads.append(('.zip',buffer.getvalue()))
    for suffix,data in uploads:
        cases.append(({'kind':'upload','suffix':suffix,'bytes':list(data)},oracle['uploaded'](data,suffix)))
    for index in range(80):
        playlist=['#EXTM3U']
        for row in range(index%9):
            group=rng.choice(['Default Group','Group A','Group B','', '  ', 'Café 😀'])
            playlist.extend([f'#EXTINF:-1 GROUP-TITLE="{group}",Fixture {row}','http://fixture/'+str(row)])
        data=rng.choice(['\r','\n','\r\n']).join(playlist).encode('utf-8')
        if index%5==0:data=b'\xff'+data
        cases.append(({'kind':'prepared','bytes':list(data)},oracle['prepared'](data)))
    oracle_runtime={'python':sys.version,'unicode_version':unicodedata.unidata_version}
    if args.goldens:
        captured=json.loads(args.goldens.read_text())
        for name,expected in captured['source_sha256'].items():
            if hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=expected:
                raise RuntimeError('Golden source hash differs: '+name)
        cases=[(case['request'],case['expected']) for case in captured['cases']]
        oracle_runtime=captured['oracle_runtime']
    if args.capture_only:
        captured={'scope':'unchanged-original-M3U-AST-goldens','oracle_runtime':oracle_runtime,'capture_tool_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  'source_sha256':{str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in [ROOT/'apps/m3u/tasks.py',ROOT/'apps/m3u/utils.py',ROOT/'apps/channels/models.py',ROOT/'core/utils.py']},
                  'cases':[{'request':case,'expected':expected} for case,expected in cases]}
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(captured)+'\n')
        print(json.dumps({'captured':len(cases),'oracle_runtime':oracle_runtime}));return 0
    result=subprocess.run([str(args.binary)],input=''.join(json.dumps(case)+'\n' for case,_ in cases),text=True,capture_output=True,check=True)
    responses=[json.loads(line) for line in result.stdout.splitlines()]
    if len(responses)!=len(cases):raise RuntimeError('Incomplete Rust oracle responses')
    failures=[{'case':i,'request':case,'expected':expected,'actual':actual} for i,((case,expected),actual) in enumerate(zip(cases,responses)) if expected!=actual]
    paths=[ROOT/'apps/m3u/tasks.py',ROOT/'apps/m3u/utils.py',ROOT/'apps/channels/models.py',ROOT/'core/utils.py',Path(__file__).resolve(),ROOT/'rust/Cargo.lock',ROOT/'rust/rust-toolchain.toml',ROOT/'rust/crates/ingest-core/Cargo.toml',*sorted((ROOT/'rust/crates/ingest-core').rglob('*.rs'))]
    report={'scope':'M3U-parser-component-differential','passed':not failures,'cases':len(cases),'failures':failures,'binary_sha256':hashlib.sha256(args.binary.read_bytes()).hexdigest(),'source_sha256':{str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in paths},'oracle_runtime':oracle_runtime,'goldens_sha256':hashlib.sha256(args.goldens.read_bytes()).hexdigest() if args.goldens else None,'application_performance_verified':False,'ingestion_jobs_migrated':False}
    if args.goldens:report['golden_capture_tool_sha256']=captured.get('capture_tool_sha256')
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'cases':len(cases),'failures':len(failures)}));return bool(failures)


if __name__=='__main__':raise SystemExit(main())
