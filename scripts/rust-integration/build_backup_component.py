#!/usr/bin/env python3
"""Immutable standalone backup/PG17 closure build; never builds the application.
Original Dispatcharr authors retain full credit and AGPL licensing.
"""
import argparse,hashlib,io,json,subprocess,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--tag',required=True);a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
 if any(out.iterdir()):p.error('Output must be empty')
 build_tool=Path(__file__).read_bytes();(out/'build-tool.py').write_bytes(build_tool)
 paths=[f for f in (ROOT/'rust').rglob('*')if f.is_file()and not set(f.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}]+[ROOT/'LICENSE',ROOT/'ATTRIBUTION.md'];captured={str(f.relative_to(ROOT)):f.read_bytes()for f in paths};hashes={k:hashlib.sha256(v).hexdigest()for k,v in captured.items()};tree=hashlib.sha256(json.dumps(hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest();report={'passed':False,'build_tool_sha256':hashlib.sha256(build_tool).hexdigest(),'tag':a.tag,'source_sha256':hashes,'source_tree_sha256':tree,'scope':'standalone disposable database-only backup and pinned PG17 ELF closure; application and jobs unwired'}
 with tarfile.open(out/'source.tar','w')as archive:
  for name,data in sorted(captured.items()):info=tarfile.TarInfo(name);info.mode=0o644;info.size=len(data);info.mtime=0;archive.addfile(info,io.BytesIO(data))
 try:
  with (out/'source.tar').open('rb')as source,(out/'build.log').open('w')as log:subprocess.run(['docker','build','-f','rust/Dockerfile.backup-tests','-t',a.tag,'--label','org.dispatcharr.rust.source-sha256='+tree,'-'],stdin=source,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=900)
  inspection=json.loads(subprocess.check_output(['docker','image','inspect',a.tag],text=True))[0];report['image_id']=inspection['Id']
  versions={}
  for binary in ['pg_dump','pg_restore','psql']:
   output=subprocess.check_output(['docker','run','--rm','--network','none','--entrypoint','/usr/local/bin/'+binary,a.tag,'--version'],text=True,stderr=subprocess.PIPE,timeout=20).strip();assert '(PostgreSQL) 17.'in output;versions[binary]=output
  report['versions']=versions
  raw=subprocess.check_output(['docker','run','--rm','--network','none','--entrypoint','/bin/sh',a.tag,'-c','test ! -e /dispatcharrpy && test ! -e /usr/bin/python3 && test ! -e /usr/local/bin/python3 && cat /usr/share/doc/dispatcharr-postgres/runtime-packages.json'],text=True,stderr=subprocess.PIPE,timeout=20);manifest=json.loads(raw);(out/'runtime-packages.json').write_text(json.dumps(manifest,indent=2)+'\n');report.update(passed=True,python_runtime_absent=True,runtime_manifest_sha256=hashlib.sha256((out/'runtime-packages.json').read_bytes()).hexdigest(),source_archive_sha256=hashlib.sha256((out/'source.tar').read_bytes()).hexdigest())
 except (OSError,ValueError,AssertionError,subprocess.SubprocessError)as error:report['error_type']=type(error).__name__
 (out/'build.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'passed':report['passed'],'evidence':str(out)}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
