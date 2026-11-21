"""Qualify the source-built ARM64 Comskip input in the exact native donor stage."""
import argparse
from hashlib import file_digest, sha256
import json
import os
from pathlib import Path
import re
import subprocess


LOCK = Path(__file__).resolve().with_name('comskip-arm64-abi-inputs.json')
LOCK_SHA256 = 'cbfe929a644b4a1134b34af05c00dfe6b03e5b6ea4ceef83ef9f1a26f6aa75fd'
LIB = Path('/usr/lib/aarch64-linux-gnu')
NOTICE = Path('/usr/share/doc')
PACKAGE = re.compile(r'[a-z0-9][a-z0-9+.-]*(?::(?:arm64|all))?')
SONAME = re.compile(r'[a-zA-Z0-9_.+-]+')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    with path.open('rb') as stream:
        return file_digest(stream,'sha256').hexdigest()


def json_digest(value):
    data=json.dumps(value,sort_keys=True,separators=(',',':')).encode()
    return sha256(data).hexdigest()


def load_lock():
    raw=LOCK.read_bytes()
    require(sha256(raw).hexdigest()==LOCK_SHA256,'ARM64 Comskip input lock digest changed')
    lock=json.loads(raw)
    require(lock['architecture']=='linux/arm64' and lock['distribution_qualified'] is False,'unexpected ARM64 input lock scope')
    return lock


def arm_elf(path):
    with path.open('rb') as stream:
        header=stream.read(20)
    require(header[:6]==b'\x7fELF\x02\x01' and header[18:20]==b'\xb7\x00','expected a little-endian AArch64 ELF64')


def check_files(directory,lock):
    expected=lock['input_files_sha256']
    require({path.name for path in directory.iterdir()}==set(expected),'unexpected replacement input file roster')
    for name,value in expected.items():
        path=directory/name
        require(path.is_file() and not path.is_symlink() and 0<path.stat().st_size<=32*1024*1024,'invalid replacement input file')
        require(digest(path)==value,f'replacement input digest changed: {name}')
    arm_elf(directory/'comskip')


def inspect_closure(binary):
    env={'PATH':'/usr/sbin:/usr/bin:/sbin:/bin','LC_ALL':'C'}
    links=subprocess.check_output(['/usr/bin/ldd',binary],env=env,timeout=20,text=True)
    require('not found' not in links,'unresolved ARM64 Comskip library')
    rows={}
    libraries={}
    paths={}
    for line in links.splitlines():
        fields=line.split()
        if not fields or fields[0]=='linux-vdso.so.1':
            continue
        if len(fields)==4:
            name,separator,raw,_=fields
            require(separator=='=>','invalid loader destination')
        else:
            require(len(fields)==2,'invalid loader interpreter line')
            raw=fields[0]
            name=Path(raw).name
        require(SONAME.fullmatch(name) and name not in rows,'invalid or duplicate library name')
        real=Path(raw).resolve(strict=True)
        require(real.is_relative_to(LIB),'ARM64 loader library outside the pinned distro root')
        arm_elf(real)
        rows[name]={'filename':real.name,'sha256':digest(real)}
        libraries[name]=str(real)
        paths[name]={raw,str(real)}
    all_paths=sorted(set().union(*paths.values()))
    query=subprocess.run(['/usr/bin/dpkg-query','-S',*all_paths],env=env,timeout=25,text=True,capture_output=True)
    require(query.returncode in (0,1),'package ownership query failed')
    owners={}
    requested=set(all_paths)
    for line in query.stdout.splitlines():
        package,separator,path=line.rpartition(': ')
        names=[item.strip() for item in package.split(',')]
        if separator and path in requested and all(PACKAGE.fullmatch(item) for item in names):
            owners.setdefault(path,set()).update(names)
    notices={}
    files={}
    for name,locations in paths.items():
        packages=sorted(set().union(*(owners.get(path,set()) for path in locations)))
        matched=[]
        for package in packages:
            notice=NOTICE/package.split(':',1)[0]/'copyright'
            if notice.is_file():
                notices[package]=digest(notice)
                files[package]=str(notice)
                matched.append(package)
        rows[name]['notice_packages']=matched
    return rows,notices,libraries,files


def verify_closure(rows,notices,lock):
    require(len(rows)==lock['native_closure_count'] and json_digest(rows)==lock['native_closure_sha256'],
            'replacement ELF library closure differs from the native acceptance')
    require(len(notices)==lock['installed_notice_count'] and json_digest(notices)==lock['installed_notice_inventory_sha256'],
            'installed package notices differ from the native acceptance')
    require(all(row['notice_packages'] for row in rows.values()),'resolved library has no installed package notice')
    for name,expected in lock['direct_runtime_inventory'].items():
        require(rows[name]['sha256']==expected['library_sha256'] and rows[name]['filename']==expected['real_library_basename'],
                'exact donor direct runtime identity differs')
        actual={notices[pkg] for pkg in rows[name]['notice_packages']}
        require(expected['already_installed_package_notice_sha256'] in actual,'exact donor direct runtime notice differs')


def collect(directory,manifest,config):
    lock=load_lock()
    donor=lock['original_runtime']
    require((manifest,config)==(donor['manifest'],donor['config']),'unexpected donor OCI manifest or config from build launcher')
    require(os.uname().sysname=='Linux' and os.uname().machine=='aarch64','native Linux ARM64 collector required')
    require(digest(Path('/var/lib/dpkg/status'))==lock['runtime_dpkg_status_sha256'],'donor installed-package database changed')
    check_files(directory,lock)
    rows,notices,libraries,notice_files=inspect_closure(directory/'comskip')
    verify_closure(rows,notices,lock)
    return {'provenance':lock,'copy_input_files':{name:str(directory/name) for name in lock['input_files_sha256']},
            'copy_library_files_by_SONAME':libraries,'copy_notice_files_by_package':notice_files,
            'resolved_libraries':rows,'verified_notice_sha256':notices,
            'scope':'exact native ARM64 replacement collector intake','distribution_qualified':False,
            'credit':'Original Dispatcharr, Erik Kaashoek and Comskip contributors, FFmpeg, Ubuntu and dependency authors retain full credit.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-root',type=Path,required=True)
    parser.add_argument('--donor-oci-manifest',required=True)
    parser.add_argument('--donor-oci-config',required=True)
    args=parser.parse_args()
    try:
        report=collect(args.input_root.resolve(strict=True),args.donor_oci_manifest,args.donor_oci_config)
    except (ValueError,OSError,subprocess.SubprocessError) as error:
        parser.exit(1,f'ARM64 Comskip collector input failed ({type(error).__name__})\n')
    print(json.dumps(report,indent=2,sort_keys=True))


if __name__=='__main__':
    main()
