"""Assemble only the byte-exact native donor media inventory and available notices."""
from hashlib import file_digest,sha256
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

HERE=Path(__file__).resolve().parent
ROOT=Path('/closure')
PREFIX='/opt/dispatcharr-media'
LOADER='ld-linux-aarch64.so.1'
INPUTS='95064285b5a2bac0b7f1ae235b845caccc6b6bf452c52fcb449511977b6b0d0a'
CREDITS='1202a089caf31b613a39ac819c8b362cd939cf828328a3880d1ffa16a45ffea7'
ORIGINAL='sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace'
CONFIG='sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43'


def stamp(path):
    with path.open('rb') as stream:
        return file_digest(stream,'sha256').hexdigest()


def copy(source,target,digest,mode):
    path=ROOT/target.removeprefix('/')
    assert path.is_relative_to(ROOT) and not path.exists()
    assert source.is_file() and stamp(source)==digest
    path.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(source,path)
    path.chmod(mode)
    assert stamp(path)==digest
    return path


def main():
    assert os.uname().machine=='aarch64' and os.getuid()==65532
    source=HERE/'inputs.json'
    assert stamp(source)==INPUTS
    inventory=json.loads(source.read_bytes())
    candidate=subprocess.run([sys.executable,'-I','-B',HERE/'collect_media_arm64.py','--donor-oci-manifest',ORIGINAL,
                              '--donor-oci-config',CONFIG],check=True,capture_output=True,timeout=135)
    assert not candidate.stderr and candidate.stdout==source.read_bytes()
    output={}
    tools=inventory['binaries']; libs=inventory['library_inputs_by_SONAME']
    assert len(libs)==55 and set(tools)=={'ffmpeg','ffprobe'} and LOADER in libs
    for name,row in libs.items():
        assert re.fullmatch('[a-zA-Z0-9_.+-]+',name) and name not in ('.','..')
        target=f'{PREFIX}/lib/{name}'
        copy(Path(row['source']),target,row['sha256'],0o555)
        output[target]=row['sha256']
    for name,row in tools.items():
        target=f'{PREFIX}/bin/{name}'
        copy(Path(row['source']),target,row['sha256'],0o555)
        output[target]=row['sha256']
        wrapper=ROOT/'usr/local/bin'/name
        wrapper.parent.mkdir(parents=True,exist_ok=True)
        wrapper.write_text(f'#!/bin/sh\nexec {PREFIX}/lib/{LOADER} --inhibit-cache --library-path {PREFIX}/lib {PREFIX}/bin/{name} "$@"\n')
        wrapper.chmod(0o555)
        output[f'/usr/local/bin/{name}']=stamp(wrapper)
    docs='/usr/share/doc/dispatcharr-standalone-media'
    for pkg,row in inventory['available_installed_notice_inputs'].items():
        assert re.fullmatch(r'[a-z0-9][a-z0-9+.-]*(?::(?:arm64|all))?',pkg)
        target=f'{docs}/media-copyright/{pkg}/copyright'
        copy(Path(row['path']),target,row['sha256'],0o444)
        output[target]=row['sha256']
    reported=subprocess.check_output(['/usr/local/bin/ffmpeg','-L'],env={'LC_ALL':'C','PATH':'/usr/local/bin:/usr/bin:/bin'},timeout=20)
    expected=inventory['FFmpeg_reported_license_sha256']
    assert sha256(reported).hexdigest()==expected
    lic=ROOT/docs.removeprefix('/')/'FFmpeg-reported-license.txt'
    lic.write_bytes(reported)
    lic.chmod(0o444)
    output[f'{docs}/FFmpeg-reported-license.txt']=expected
    copy(HERE/'ATTRIBUTION.md',f'{docs}/ATTRIBUTION.md',CREDITS,0o444)
    output[f'{docs}/ATTRIBUTION.md']=CREDITS
    assert inventory['inputs_without_installed_package_notice']==['ffmpeg','ffprobe'] and len(output)==112
    projection={path:digest for path,digest in sorted(output.items())}
    manifest=ROOT/docs.removeprefix('/')/'unit-inputs.json'
    manifest.write_text(json.dumps({'inventory_sha256':INPUTS,'collected_files':projection,
                                   'tool_package_notice_gaps':['ffmpeg','ffprobe'],'distribution_qualified':False,
                                   'credit':'Original Dispatcharr, FFmpeg and dependency authors retain full credit.'},indent=2,sort_keys=True)+'\n')
    manifest.chmod(0o444)


if __name__=='__main__':
    main()
