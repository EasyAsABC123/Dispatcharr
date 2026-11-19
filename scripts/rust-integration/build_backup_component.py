#!/usr/bin/env python3
"""Immutable standalone backup/PG17 closure build; never builds the application.
Original Dispatcharr authors retain full credit and AGPL licensing.
"""
import argparse,hashlib,io,json,subprocess,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
POSTGRES_MANIFEST = '/usr/share/doc/dispatcharr/postgres-runtime.json'
POSTGRES_CLIENTS = ('pg_dump', 'pg_restore', 'psql')


def inspect_postgres_runtime(image_id):
    command = ['docker', 'run', '--rm', '--network', 'none', '--entrypoint']
    shell_check = (
        'test ! -e /dispatcharrpy && test ! -e /usr/bin/python3 && '
        'test ! -e /usr/local/bin/python3 && cat ' + POSTGRES_MANIFEST
    )
    options = {'text': True, 'stderr': subprocess.PIPE, 'timeout': 20}
    raw = subprocess.check_output([*command, '/bin/sh', image_id, '-c', shell_check], **options)
    manifest = json.loads(raw)
    binaries = manifest.get('binaries')
    if not isinstance(binaries, dict) or set(binaries) != set(POSTGRES_CLIENTS):
        raise ValueError('Unexpected PostgreSQL runtime manifest binaries')
    versions = {}
    for name in POSTGRES_CLIENTS:
        output = subprocess.check_output(
            [*command, '/usr/local/bin/' + name, image_id, '--version'], **options
        ).strip()
        binary = binaries[name]
        if (not isinstance(binary, dict) or binary.get('version') != output
                or not output.startswith(name + ' (PostgreSQL) 17.')):
            raise ValueError('Packaged PostgreSQL version differs from manifest: ' + name)
        versions[name] = output
    return manifest, versions


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
  manifest,versions=inspect_postgres_runtime(report['image_id'])
  report['versions']=versions
  manifest_path=out/'runtime-packages.json'
  manifest_path.write_text(json.dumps(manifest,indent=2)+'\n')
  report.update(passed=True,python_runtime_absent=True,runtime_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),source_archive_sha256=hashlib.sha256((out/'source.tar').read_bytes()).hexdigest())
 except (OSError,ValueError,AssertionError,subprocess.SubprocessError)as error:report['error_type']=type(error).__name__
 (out/'build.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'passed':report['passed'],'evidence':str(out)}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
