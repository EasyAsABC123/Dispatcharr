#!/usr/bin/env python3
"""Remove completed Cargo output only; preserve migration evidence/source.
Original Dispatcharr authors retain application credit. Default is inventory;
--execute is an explicit generated-output cleanup authorization.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import time

ROOT=Path(__file__).resolve().parents[2]
ARTIFACTS=ROOT/'artifacts/rust'
SKIP={'tooling','.git','node_modules','registry','rustup','docker-data','docker-exec','__pycache__'}
BINARY_KEYS={'binary','binary_path','executable','executable_path','native_binary','test_binary'}

def inside(path,parent):
    return path==parent or parent in path.parents

def digest(path):
    value=hashlib.sha256()
    with path.open('rb')as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):value.update(block)
    return value.hexdigest()

def process_paths():
    records=[];protected=[]
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit():continue
        try:
            executable=Path(os.readlink(entry/'exe'));comm=(entry/'comm').read_text().strip()
            if comm not in {'cargo','rustc','rustdoc'} and not inside(executable,ROOT):continue
            arguments=(entry/'cmdline').read_bytes().split(b'\0')
            environment=(entry/'environ').read_bytes().split(b'\0')
            target=next((Path(value.split(b'=',1)[1].decode())for value in environment if value.startswith(b'CARGO_TARGET_DIR=')),None)
            if target is None and comm=='cargo':
                for index,value in enumerate(arguments):
                    if value.startswith(b'--target-dir='):
                        target=Path(value.split(b'=',1)[1].decode())
                    elif value==b'--target-dir'and index+1<len(arguments):
                        target=Path(arguments[index+1].decode())
            if target and not target.is_absolute():target=Path(os.readlink(entry/'cwd'))/target
            paths=[p for p in [target,executable]if p and inside(p,ROOT)]
            if comm in {'cargo','rustc','rustdoc'}or any('/target' in str(p)or'cargo-target'in str(p)for p in paths):
                records.append({'pid':int(entry.name),'comm':comm,'exe':str(executable),'target':str(target)if target else None})
                protected+=paths
                # Cargo may be planning/linking with no rustc process alive.
                # An unspecified/configured default target cannot be inferred
                # reliably from its executable path. Wait for that Cargo run.
                if comm=='cargo'and target is None:
                    protected.extend([ARTIFACTS,ROOT/'rust/target'])
                for index,value in enumerate(arguments[:-1]):
                    if value==b'--out-dir':protected.append(Path(arguments[index+1].decode()))
        except (OSError,UnicodeError):continue
    return records,protected

def docker_mounts():
    """Fresh live bind sources only; never collect container environment data."""
    ids=subprocess.check_output(['docker','ps','--quiet'],text=True).split()
    if not ids:return []
    raw=subprocess.check_output(['docker','inspect','--format','{{json .Mounts}}',*ids],text=True)
    return sorted({Path(m['Source']).resolve()for line in raw.splitlines()
                   for m in json.loads(line)if m.get('Type')=='bind'and m.get('Source')})

def cargo_output(path):
    if path.is_symlink()or not path.is_dir():return False
    if (path/'Cargo.toml').exists():return False
    marker=(path/'.rustc_info.json').is_file()or(path/'CACHEDIR.TAG').is_file()
    return marker and any((path/child).is_dir()for child in ['debug','release'])

def discover():
    found=[];todo=[ARTIFACTS]
    if cargo_output(ROOT/'rust/target'):found.append(ROOT/'rust/target')
    while todo:
        parent=todo.pop()
        try:entries=list(os.scandir(parent))
        except (PermissionError,FileNotFoundError):continue
        for item in entries:
            if not item.is_dir(follow_symlinks=False)or item.name in SKIP:continue
            child=Path(item.path)
            if cargo_output(child):found.append(child)
            else:todo.append(child)
    return sorted(set(found))

def usage(path):
    files=0;apparent=0;allocated=0;seen=set()
    for parent,dirs,names in os.walk(path,followlinks=False):
        for name in names:
            entry=Path(parent)/name
            try:info=entry.lstat()
            except FileNotFoundError:continue
            files+=1;identity=(info.st_dev,info.st_ino)
            if identity in seen:continue
            seen.add(identity);apparent+=info.st_size;allocated+=info.st_blocks*512
    return {'files':files,'apparent_bytes':apparent,'allocated_bytes':allocated}

def outside_files(candidates):
    excluded=set(candidates)
    for parent,dirs,names in os.walk(ARTIFACTS,followlinks=False):
        base=Path(parent)
        dirs[:]=[name for name in dirs if name not in SKIP and base/name not in excluded]
        for name in names:
            path=base/name
            if not path.is_symlink():yield path

def disk():
    result=shutil.disk_usage(ROOT)
    return {'total_bytes':result.total,'used_bytes':result.used,'free_bytes':result.free}

def working_files():
    result={}
    raw=subprocess.check_output(['git','status','--porcelain=v1','-z','--untracked-files=all'],cwd=ROOT)
    rows=raw.split(b'\0');index=0
    while index<len(rows):
        row=rows[index];index+=1
        if not row:continue
        name=os.fsdecode(row[3:]);path=ROOT/name
        if row[:1]in[b'R',b'C']or row[1:2]in[b'R',b'C']:index+=1
        if path.is_file()and not path.is_symlink()and not inside(path,ARTIFACTS):result[name]=digest(path)
    return result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--protect',action='append',default=[])
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args();out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
    if (out/'manifest.json').exists():parser.error('Use a new output directory')
    before=disk();wip=working_files();processes,active=process_paths()
    # A stale or absent caller-supplied snapshot must not make mounted targets
    # look idle. Fail before deletion if the live Docker inventory is unavailable.
    mounts=docker_mounts()
    (out/'docker-before.json').write_text(json.dumps([str(p)for p in mounts],indent=2)+'\n')
    protected=[(ROOT/entry).resolve()if not Path(entry).is_absolute()else Path(entry).resolve()for entry in args.protect]+active+mounts
    candidates=discover();rows=[]
    if any(inside(out,path)for path in candidates):
        parser.error('Evidence output must be outside every Cargo target')
    for index,path in enumerate(candidates):
        blocked=[str(p)for p in protected if inside(path,p)or inside(p,path)]
        counts=usage(path);row={'path':str(path.relative_to(ROOT)),**counts,'protected_by':blocked,'deleted':False};rows.append(row)
        if index%10==0:print(json.dumps({'inventory_progress':index+1,'targets':len(candidates)}),flush=True)
    # Preserve explicit recorded target-local evidence, not every compiler JSONL
    # executable. Also hash outer evidence binaries; none are deletion candidates.
    external=[];binary_index={};references=[];scanned=0
    for path in outside_files(candidates):
        scanned+=1
        if scanned%25000==0:print(json.dumps({'evidence_files_scanned':scanned,'outside_binaries':len(external)}),flush=True)
        try:info=path.stat()
        except FileNotFoundError:continue
        if info.st_mode&0o111 and info.st_size>1024*1024:
            with path.open('rb')as stream:is_elf=stream.read(4)==b'\x7fELF'
            if is_elf:
                value=digest(path);external.append({'path':str(path.relative_to(ROOT)),'sha256':value,'bytes':info.st_size});binary_index.setdefault(value,[]).append(str(path.relative_to(ROOT)))
        if path.suffix!='.json'or info.st_size>64*1024*1024:continue
        try:value=json.loads(path.read_text())
        except (OSError,UnicodeError,json.JSONDecodeError):continue
        def walk(node,context=()):
            if isinstance(node,dict):
                for key,item in node.items():
                    if isinstance(item,str)and(key.lower()in BINARY_KEYS or(key.lower()=='path'and any('binar'in part.lower()or'executable'in part.lower()for part in context))):
                        options=[Path(item)]if Path(item).is_absolute()else[ROOT/item,path.parent/item]
                        for candidate in options:
                            candidate=candidate.resolve()
                            if candidate.is_file()and any(inside(candidate,root)for root in candidates):
                                references.append({'path':str(candidate),'report':str(path.relative_to(ROOT))});break
                    walk(item,(*context,str(key)))
            elif isinstance(node,list):
                for item in node:walk(item,context)
        walk(value)
    preserved=[];store=out/'evidence-binaries';store.mkdir(exist_ok=True)
    for reference in {r['path']:r for r in references}.values():
        path=Path(reference['path']);value=digest(path);copies=binary_index.get(value,[])
        if not copies:
            destination=store/(value+'.bin');shutil.copyfile(path,destination);destination.chmod(0o755)
            if digest(destination)!=value:raise RuntimeError('Evidence copy verification')
            copies=[str(destination.relative_to(ROOT))]
        preserved.append({**reference,'sha256':value,'preserved_at':copies})
    report={'format_version':1,'started_unix':time.time(),'root':str(ROOT),'before_disk':before,
            'processes_before':processes,'protected_paths':[str(p)for p in protected],
            'targets':rows,'outside_evidence_binaries':external,'preserved_target_evidence':preserved,
            'wip_before_sha256':wip,'execute':args.execute,'policy':'Completed Cargo output only, regardless of age; no generic executable preservation; no source/evidence/toolchain/Docker deletion'}
    (out/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'candidates':len(rows),'deletable_apparent_bytes':sum(r['apparent_bytes']for r in rows if not r['protected_by']),
                     'deletable_allocated_bytes':sum(r['allocated_bytes']for r in rows if not r['protected_by']),
                     'outer_evidence_binaries':len(external),'target_evidence_copied_or_matched':len(preserved)}),flush=True)
    journal=out/'deletions.jsonl'
    if args.execute:
        for row in sorted(rows,key=lambda r:r['allocated_bytes'],reverse=True):
            if row['protected_by']:continue
            path=ROOT/row['path'];current,live=process_paths()
            blockers=[str(p)for p in live+docker_mounts()if inside(path,p)or inside(p,path)]
            if blockers:row['protected_by']=blockers;continue
            if not cargo_output(path)or path.is_symlink():raise RuntimeError('Output identity changed: '+str(path))
            tracked=subprocess.check_output(['git','ls-files','--',row['path']],cwd=ROOT,text=True)
            if tracked.strip():raise RuntimeError('Tracked files in generated output')
            started=time.monotonic();shutil.rmtree(path);row['deleted']=True;row['delete_seconds']=time.monotonic()-started
            with journal.open('a')as stream:stream.write(json.dumps(row)+'\n');stream.flush()
            print(json.dumps({'deleted':row['path'],'apparent_bytes':row['apparent_bytes'],'allocated_bytes':row['allocated_bytes']}),flush=True)
    report['after_disk']=disk();report['wip_after_sha256']=working_files()
    report['wip_concurrent_changes']=[name for name,value in wip.items()if report['wip_after_sha256'].get(name)!=value]
    report['deleted_targets']=sum(row['deleted']for row in rows)
    report['deleted_apparent_bytes']=sum(row['apparent_bytes']for row in rows if row['deleted'])
    report['deleted_allocated_bytes']=sum(row['allocated_bytes']for row in rows if row['deleted'])
    report['filesystem_free_delta_bytes']=report['after_disk']['free_bytes']-before['free_bytes']
    report['finished_unix']=time.time();(out/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({key:report[key]for key in ['deleted_targets','deleted_apparent_bytes','deleted_allocated_bytes','filesystem_free_delta_bytes','wip_concurrent_changes']}),flush=True)
if __name__=='__main__':main()
