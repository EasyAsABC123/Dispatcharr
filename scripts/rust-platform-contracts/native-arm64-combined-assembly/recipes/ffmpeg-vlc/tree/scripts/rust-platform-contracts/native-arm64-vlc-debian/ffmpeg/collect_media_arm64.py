"""Read the exact native Original ARM64 FFmpeg/ffprobe collector input closure."""
import argparse
from hashlib import file_digest, sha256
import json
import os
from pathlib import Path
import re
import shutil
import struct
import subprocess

MANIFEST='sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace'
CONFIG='sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43'
STATUS='08b76d06fbdd1143a48babca07ae3c41c105adc6357f5ed2d98a1a6daee64e0b'
INTERPRETER='/lib/ld-linux-aarch64.so.1'
ROOTS=tuple(map(Path,('/usr/lib/aarch64-linux-gnu','/usr/local/lib','/usr/local/ffmpeg/lib')))
ENV={'LC_ALL':'C','TZ':'UTC','PATH':'/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'}
NAME=re.compile(r'[a-zA-Z0-9_.+-]+')
PACKAGE=re.compile(r'[a-z0-9][a-z0-9+.-]*(?::(?:arm64|all))?')


def require(ok,label):
    if not ok:
        raise ValueError(label)


def sha(path):
    with path.open('rb') as stream:
        return file_digest(stream,'sha256').hexdigest()


def run(*args):
    result=subprocess.run(args,env=ENV,timeout=25,capture_output=True)
    require(result.returncode==0,'native media inventory command failed')
    require(len(result.stdout)<=250000,'native media inventory output too large')
    return result.stdout


def elf(path,require_interpreter=False):
    with path.open('rb') as handle:
        header=handle.read(64)
        require(len(header)==64 and header[:6]==b'\x7fELF\x02\x01' and header[18:20]==b'\xb7\x00','native AArch64 ELF64 required')
        if not require_interpreter:
            return None
        fields=struct.unpack('<16sHHIQQQIHHHHHH',header)
        offset,size,count=fields[5],fields[9],fields[10]
        require(0<offset<65536 and size==56 and 0<count<=128,'invalid ELF program header table')
        found=[]
        for number in range(count):
            handle.seek(offset+number*size)
            raw=handle.read(size)
            require(len(raw)==size,'short ELF program header')
            kind,_,start,_,_,length,_,_=struct.unpack('<IIQQQQQQ',raw)
            if kind==3:
                require(0<length<=256,'invalid ELF interpreter length')
                handle.seek(start)
                found.append(handle.read(length))
        require(found==[(INTERPRETER+'\0').encode()],'unexpected ARM64 media interpreter')
    return INTERPRETER


def read_links(raw):
    require('not found' not in raw,'unresolved donor media dependency')
    names={}
    for line in raw.splitlines():
        fields=line.split()
        if not fields or fields[0]=='linux-vdso.so.1':
            continue
        if len(fields)==4:
            name,relation,filename,_=fields
            require(relation=='=>','invalid loader relationship')
        else:
            require(len(fields)==2,'invalid loader interpreter line')
            filename=fields[0]
            name=Path(filename).name
        require(NAME.fullmatch(name) and name not in names,'invalid or duplicate media SONAME')
        require(Path(filename).is_absolute(),'media loader did not resolve an absolute file')
        names[name]=Path(filename)
    require(Path(INTERPRETER).name in names and 5<=len(names)<=250,'incomplete media loader closure')
    return names


def ownership(paths):
    lookup={}
    for path in paths:
        candidates={str(path),str(path.resolve(strict=True))}
        # usrmerge can give dpkg and the loader different lexical aliases.
        for item in tuple(candidates):
            if item.startswith('/usr/lib/'):
                candidates.add(item[4:])
            elif item.startswith('/lib/'):
                candidates.add('/usr'+item)
        lookup[path]=candidates
    queries=sorted(set().union(*lookup.values()))
    found=subprocess.run(['/usr/bin/dpkg-query','-S',*queries],env=ENV,timeout=30,text=True,capture_output=True)
    require(found.returncode in (0,1),'cannot read media file package ownership')
    owners={}
    expected=set(queries)
    for line in found.stdout.splitlines():
        package,separator,name=line.rpartition(': ')
        parsed=[item.strip() for item in package.split(',')]
        if separator and name in expected and all(PACKAGE.fullmatch(item) for item in parsed):
            owners.setdefault(name,set()).update(parsed)
    return {path:sorted(set().union(*(owners.get(name,set()) for name in aliases))) for path,aliases in lookup.items()}


def collect():
    require(os.uname().sysname=='Linux' and os.uname().machine=='aarch64' and os.getuid()==65532,'native non-root Linux AArch64 required')
    require(sha(Path('/var/lib/dpkg/status'))==STATUS,'donor installed-package inventory differs')
    libraries={}
    path_names={}
    binaries={}
    for tool in ('ffmpeg','ffprobe'):
        found=shutil.which(tool,path=ENV['PATH'])
        require(found is not None,'native donor media executable missing')
        path=Path(found).resolve(strict=True)
        require(any(path.is_relative_to(prefix) for prefix in ('/usr/local/bin','/usr/bin','/usr/local/ffmpeg/bin')),'unrecognized donor media executable directory')
        interpreter=elf(path,require_interpreter=True)
        version=run(path,'-version').decode().splitlines()[0]
        require(len(version)<200 and version.startswith(f'{tool} version '),'unexpected native media version label')
        links=read_links(run('/usr/bin/ldd',path).decode())
        binaries[tool]={'source':str(path),'sha256':sha(path),'version':version,'interpreter':interpreter,'libraries':sorted(links)}
        path_names[tool]={Path(found),path}
        for name,filename in links.items():
            real=filename.resolve(strict=True)
            require(any(real.is_relative_to(prefix) for prefix in ROOTS),'dependency outside the known native donor library roots')
            elf(real)
            library={'source':str(real),'sha256':sha(real),'filename':real.name}
            require(name not in libraries or libraries[name]==library,'different media binaries resolve one SONAME differently')
            libraries[name]=library
            path_names.setdefault(name,set()).add(filename)
    owners=ownership(set().union(*path_names.values()))
    notices={}
    for name,record in {**libraries,**binaries}.items():
        package_names=sorted(set().union(*(owners[path] for path in path_names[name])))
        record['owning_packages']=package_names
        notice_packages=[]
        for owner in package_names:
            package=owner.split(':',1)[0]
            notice=Path('/usr/share/doc')/package/'copyright'
            if notice.is_file():
                notices[owner]={'path':str(notice),'sha256':sha(notice)}
                notice_packages.append(owner)
        record['notice_packages']=notice_packages
    license_text=run(Path(binaries['ffmpeg']['source']),'-L')
    missing=sorted(name for name,row in {**libraries,**binaries}.items() if not row['notice_packages'])
    return {'scope':'native read-only collector inputs from Original ARM64 FFmpeg and ffprobe',
            'donor_manifest':MANIFEST,'donor_config':CONFIG,'native_platform':'linux/arm64','uid':65532,
            'donor_dpkg_status_sha256':STATUS,'binaries':binaries,'library_inputs_by_SONAME':libraries,
            'available_installed_notice_inputs':notices,'inputs_without_installed_package_notice':missing,
            'FFmpeg_reported_license_sha256':sha256(license_text).hexdigest(),
            'historically_attested_FFmpeg_builder_raw_OCI_authorized':False,
            'wrapped_Debian_runtime_verified':False,'distribution_qualified':False,
            'credit':'Original Dispatcharr, FFmpeg and dependency authors retain full credit.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--donor-oci-manifest',required=True,choices=[MANIFEST])
    parser.add_argument('--donor-oci-config',required=True,choices=[CONFIG])
    parser.parse_args()
    try:
        receipt=collect()
    except ValueError as error:
        parser.exit(1,f'ARM64 media input: {error}\n')
    print(json.dumps(receipt,indent=2,sort_keys=True))


if __name__=='__main__':
    main()
