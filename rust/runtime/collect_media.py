"""Collect the pinned baseline's exact media ELF closure into an isolated prefix."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
root=Path('/media-runtime')
prefix=root/'opt/dispatcharr-media'
(prefix/'bin').mkdir(parents=True)
(prefix/'lib').mkdir()
(root/'usr/local/bin').mkdir(parents=True)
docs=root/'usr/share/doc/dispatcharr'
docs.mkdir(parents=True)
manifest={'source_image':IMAGE,'platform':'linux/amd64','libraries':{},'binaries':{}}
loader=None
for executable in ('ffmpeg','ffprobe'):
    source=Path(shutil.which(executable) or '/missing-media-binary')
    shutil.copy2(source,prefix/'bin'/executable)
    manifest['binaries'][executable]={'source':str(source),'sha256':hashlib.sha256(source.read_bytes()).hexdigest()}
    output=subprocess.check_output(['ldd',str(source)],text=True)
    for line in output.splitlines():
        for token in line.split():
            path=Path(token)
            if not token.startswith('/') or not path.is_file():continue
            destination=prefix/'lib'/path.name
            digest=hashlib.sha256(path.read_bytes()).hexdigest()
            if destination.exists() and hashlib.sha256(destination.read_bytes()).hexdigest()!=digest:
                raise RuntimeError('Conflicting media dependency basenames: '+path.name)
            shutil.copy2(path,destination)
            manifest['libraries'][path.name]={'source':str(path),'sha256':digest}
            if path.name.startswith('ld-linux-'):loader=path.name
if not loader:raise RuntimeError('Expected ELF interpreter not found')
for executable in ('ffmpeg','ffprobe'):
    wrapper=root/'usr/local/bin'/executable
    wrapper.write_text('#!/bin/sh\nexec /opt/dispatcharr-media/lib/'+loader+' --library-path /opt/dispatcharr-media/lib /opt/dispatcharr-media/bin/'+executable+' "$@"\n')
    wrapper.chmod(0o755)
manifest['ffmpeg_version']=subprocess.check_output(['ffmpeg','-version'],text=True).splitlines()[0]
(docs/'runtime-packages.txt').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
license_run=subprocess.run(['ffmpeg','-L'],text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,check=True)
(docs/'FFmpeg-LICENSE.txt').write_text(license_run.stdout)
# Preserve available Debian dependency copyright notices alongside their exact
# binary hashes; the application author attribution is copied independently.
source_docs=Path('/usr/share/doc')
for copyright in sorted(source_docs.glob('*/copyright')):
    if copyright.is_file():
        target=docs/'media-copyright'/copyright.parent.name;target.mkdir(parents=True,exist_ok=True);shutil.copy2(copyright,target/'copyright')
common=Path('/usr/share/common-licenses')
if common.is_dir():shutil.copytree(common,docs/'common-licenses')
certificates=Path('/etc/ssl/certs/ca-certificates.crt')
if certificates.is_file():
    target=root/'etc/ssl/certs';target.mkdir(parents=True);shutil.copy2(certificates,target/certificates.name)
