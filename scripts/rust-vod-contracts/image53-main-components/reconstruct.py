"""Read-only exact historical tool/Git input restoration. Never execute a cue or app."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
HERE=Path(__file__).resolve().parent
CLOSURES_SHA='c9c30ff5e52e64ffa0119c082befa07474d2c8f569ad7306ff0f77adcd36bb73'
APPLICATION_SHA='52763e65ba2b6f628975158c850c53c7a69b2713d5f7893e3100e1fc15d21b65'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def fresh(p):
    p=Path(p)
    if p.exists()or p.is_symlink():raise ValueError('FreshOutputRequired')
    if any(x.is_symlink()for x in p.parents):raise ValueError('NoOutputParentAlias')
    return p

def tools(name,output,executed=False,source=HERE):
    source=Path(source);output=fresh(output)
    if sha(source/'tool-closures.json')!=CLOSURES_SHA:raise ValueError('ExactClosureAuthority')
    meta=json.loads((source/'tool-closures.json').read_text())
    if name not in meta:raise ValueError('DeclaredClosureOnly')
    root=source/'inputs'/name;selection=root/'blocked-selection.json'
    if sha(selection)!=meta[name]['blocked_selection_sha256']:raise ValueError('ExactBlockedSelection')
    rows=json.loads(selection.read_text())['files']
    if len(rows)!=meta[name]['payload_count']or len({r['path']for r in rows})!=len(rows):raise ValueError('MandatoryUniquePayloads')
    for row in rows:
        rel=Path(row['path']);p=root/'blocked'/rel
        if rel.is_absolute()or '..'in rel.parts or p.is_symlink()or not p.is_file()or p.stat().st_size>4194304 or sha(p)!=row['sha256']:raise ValueError('ExactRegularPayload')
    launch=root/'historical-executed-launcher.py';blocked=root/'blocked/launch_once.py'
    if sha(launch)!=meta[name]['historical_executed_launcher_sha256']or launch.read_bytes().replace(b'\nEXECUTION_BLOCKED=False\n',b'\nEXECUTION_BLOCKED=True\n')!=blocked.read_bytes():raise ValueError('OnlyHistoricalFlagChanged')
    output.mkdir(parents=True,exist_ok=False)
    for row in rows:
        dst=output/row['path'];dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(root/'blocked'/row['path'],dst)
    shutil.copyfile(selection,output/'selection.json')
    if executed:shutil.copyfile(launch,output/'launch_once.py')
    return {'restored_files':len(rows),'kind':name,'mode':'historical-executed-not-authorized'if executed else'blocked','application_executed':False,'consumed_cue_not_installed':True,'new_authorization_granted':False}

def application(repo,output,source=HERE):
    source=Path(source);output=fresh(output)
    if sha(source/'application.json')!=APPLICATION_SHA:raise ValueError('ExactApplicationMap')
    app=json.loads((source/'application.json').read_text());expected=app['source_sha256'];data={}
    if len(expected)!=1288 or sum(k.startswith('rust/')for k in expected)!=791:raise ValueError('Exact1288And791')
    # Explicit immutable Git objects only, never HEAD, worktree or private fallback.
    process=subprocess.Popen(['git','-C',str(Path(repo).resolve()),'cat-file','--batch'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
    try:
        for name,wanted in sorted(expected.items()):
            rel=Path(name)
            if rel.is_absolute()or '..'in rel.parts:raise ValueError('SafeGitInputPath')
            revision=app['Original_revision']if name.startswith('frontend/')or name=='LICENSE'else app['git_revision']
            process.stdin.write((revision+':'+name+'\n').encode());process.stdin.flush();header=process.stdout.readline(1024).split()
            if len(header)!=3 or header[1]!=b'blob':raise ValueError('RequiredImmutableBlob')
            size=int(header[2])
            if not 0<=size<=33554432:raise ValueError('BoundedBlob')
            raw=process.stdout.read(size)
            if process.stdout.read(1)!=b'\n'or len(raw)!=size or hashlib.sha256(raw).hexdigest()!=wanted:raise ValueError('ExactGitBlob:'+name)
            data[name]=raw
    finally:
        process.stdin.close();process.stdout.close();code=process.wait(timeout=10)
        if code:raise ValueError('GitObjectReaderFailed')
    output.mkdir(parents=True,exist_ok=False)
    for name,raw in data.items():p=output/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
    with tarfile.open(output/'source.tar','w')as tar:
        for name,raw in sorted(data.items()):
            info=tarfile.TarInfo(name);info.size=len(raw);info.mode=0o644;info.mtime=0;tar.addfile(info,io.BytesIO(raw))
    if sha(output/'source.tar')!=app['source_archive_sha256']:raise ValueError('ExactNormalizedApplicationArchive')
    return {'restored_app_files':1288,'runtime_inputs':791,'archive_sha256':sha(output/'source.tar'),'application_executed':False,'new_authorization_granted':False}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--kind',choices=['main4s','header-failed','header-native','notification-failed','notification-fixed','application'],required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--repo',type=Path);p.add_argument('--historical-executed',action='store_true');a=p.parse_args()
    if a.kind=='application':
        if a.repo is None:raise ValueError('ExplicitGitRepositoryRequired')
        result=application(a.repo,a.output)
    else:result=tools(a.kind,a.output,a.historical_executed)
    print(json.dumps(result,indent=2))
