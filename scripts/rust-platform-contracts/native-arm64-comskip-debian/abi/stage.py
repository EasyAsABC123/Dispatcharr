"""Exact package acquisition, offline native compile and generated-media proof."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from hashlib import file_digest, sha256
from io import BytesIO
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
from tempfile import TemporaryDirectory
from urllib.request import HTTPRedirectHandler, Request, build_opener

HERE=Path(__file__).resolve().parent
LOCK_SHA='a1459b29656b3ece310368d2eb4f8d47fccc81af8dcee843e9cf79fa11557776'
LIB='/usr/lib/aarch64-linux-gnu'
INTERPRETER='/lib/ld-linux-aarch64.so.1'
SONAME_LOADER='ld-linux-aarch64.so.1'
APP=Path('/opt/dispatcharr-comskip-abi')


def stamp(path):
    with Path(path).open('rb') as stream:
        return file_digest(stream,'sha256').hexdigest()


def stable(data):
    return json.dumps(data,indent=2,sort_keys=True)+'\n'


def locked(path):
    assert stamp(path)==LOCK_SHA
    inputs=json.loads(path.read_bytes())
    plan=inputs['toolchain_plan']
    assert sha256(stable(plan).encode()).hexdigest()==inputs['toolchain_plan_sha256']
    assert plan['proposed_package_count']==len(plan['proposed_packages'])==51
    assert len(inputs['direct_runtime_inventory'])==6
    assert plan['non_pinned_existing_version_changes']==[] and plan['removals_proposed']==0
    return inputs


def call(args,**kwargs):
    return subprocess.check_output(args,text=True,timeout=kwargs.pop('timeout',15),**kwargs)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*_):
        raise ValueError('unexpected redirect of pinned official acquisition')


def fetch(url,size):
    req=Request(url,headers={'User-Agent':'Dispatcharr-ARM64-ABI-candidate/1'})
    with build_opener(NoRedirect()).open(req,timeout=90) as reply:
        data=reply.read(size+1)
        assert reply.status==200 and 0<len(data)<=size
        return data


def source(context,root,identity):
    raw=fetch('https://codeload.github.com/erikkaashoek/Comskip/tar.gz/'+identity['revision'],1048576)
    assert sha256(raw).hexdigest()==identity['source_archive_sha256']
    destination=context/'source'
    destination.mkdir()
    prefix='Comskip-'+identity['revision']
    files=set()
    with tarfile.open(fileobj=BytesIO(raw),mode='r:gz') as archive:
        for item in archive:
            parts=PurePosixPath(item.name)
            assert not parts.is_absolute() and parts.parts[0]==prefix and '..' not in parts.parts
            target=destination.joinpath(*parts.parts[1:])
            if item.isdir():
                target.mkdir(parents=True,exist_ok=True)
                continue
            assert item.isfile() and item.size<1024*1024 and target not in files
            files.add(target)
            target.parent.mkdir(parents=True,exist_ok=True)
            with target.open('xb') as output:
                output.write(archive.extractfile(item).read())
            target.chmod(0o755 if item.mode&0o111 else 0o644)
    assert len(files)==identity['files']==42
    assert stamp(destination/'LICENSE')==identity['upstream_license_sha256']
    recipe=root/'rust/runtime/comskip-Original-DispatcharrBase'
    assert stamp(recipe)==identity['original_recipe_sha256']
    edit=re.search(rb"    && (sed -i \\\n(?:[^\n]*\\\n)+        mpeg2dec.c) \\",recipe.read_bytes())
    assert edit and sha256(edit[1]).hexdigest()==identity['compatibility_patch_command_sha256']
    subprocess.run(['bash','-euc',edit[1]],check=True,cwd=destination,timeout=20)
    assert stamp(destination/'mpeg2dec.c')==identity['patched_mpeg2dec_sha256']
    shutil.copyfile(recipe,context/'Original-DispatcharrBase')
    shutil.copyfile(destination/'LICENSE',context/'Comskip-LICENSE')


def verify_package(path,name,row):
    assert path.stat().st_size==row['size'] and stamp(path)==row['sha256']
    for field,expected in [('Package',name),('Architecture',row['architecture']),('Version',row['version'])]:
        assert call(['dpkg-deb','-f',path,field]).strip()==expected


def package_path(directory,row):
    name=Path(row['filename']).name
    assert re.fullmatch(r'pool/[a-z0-9.-]+/[a-z0-9.-]+/[a-z0-9.+-]+/[a-zA-Z0-9.+:~_-]+_(arm64|all)\.deb',row['filename'])
    assert 0<row['size']<32*1024*1024
    return directory/name


def acquire(root,context):
    inputs=locked(HERE/'inputs.json')
    plan=inputs['toolchain_plan']
    source(context,root,inputs['source_identity'])
    for name in ('stage.py','inputs.json','Dockerfile'):
        shutil.copyfile(HERE/name,context/name)
    ini=root/'rust/runtime/comskip-source.ini'
    assert stamp(ini)==inputs['detector_ini_sha256']
    shutil.copyfile(ini,context/'comskip.ini')
    credits=root/'ATTRIBUTION.md'
    assert stamp(credits)=='1202a089caf31b613a39ac819c8b362cd939cf828328a3880d1ffa16a45ffea7'
    shutil.copyfile(credits,context/'Dispatcharr-ATTRIBUTION.md')
    packages=context/'packages'
    packages.mkdir()
    snapshot='https://snapshot.ubuntu.com/ubuntu/'+plan['Ubuntu_signed_snapshot']

    def download(item):
        name,row=item
        assert row['architecture'] in ('arm64','all') and row['previous_version'] is None
        path=package_path(packages,row)
        data=fetch(snapshot+'/'+row['filename'],row['size'])
        assert sha256(data).hexdigest()==row['sha256']
        with path.open('xb') as output:
            output.write(data)
        verify_package(path,name,row)

    with ThreadPoolExecutor(max_workers=4) as worker:
        list(worker.map(download,plan['proposed_packages'].items()))
    assert len(list(packages.iterdir()))==51
    return {'input_plan_sha256':inputs['toolchain_plan_sha256'],'verified_package_count':51,
            'verified_package_bytes':sum(file.stat().st_size for file in packages.iterdir()),
            'source_archive_sha256':inputs['source_identity']['source_archive_sha256']}


def installed():
    rows=call(['dpkg-query','-W','-f=${db:Status-Status}\t${Package}\t${Architecture}\t${Version}\n'])
    out={}
    for line in rows.splitlines():
        status,name,arch,version=line.split('\t')
        if status=='installed':
            assert name not in out and arch in ('arm64','all')
            out[name]=(arch,version)
    return out


def local_install_command(work, archives):
    for name in ('sources.d','lists','archives'):
        (work/name).mkdir()
    sources=work/'sources.list'
    sources.touch()
    options={'Dir::Etc::sourcelist':sources,'Dir::Etc::sourceparts':work/'sources.d',
             'Dir::State::lists':work/'lists','Dir::Cache::archives':work/'archives',
             'Dir::Cache::pkgcache':'','Dir::Cache::srcpkgcache':'','Acquire::Retries':0}
    command=['apt-get','-y','--no-remove','--no-install-recommends']
    for key,value in options.items():
        command.extend(('-o',f'{key}={value}'))
    return [*command,'install',*map(str,archives)]


def install():
    plan=locked(HERE/'inputs.json')['toolchain_plan']
    assert os.geteuid()==0 and os.uname().machine=='aarch64'
    assert stamp('/var/lib/dpkg/status')==plan['base_donor_status_sha256']
    before=installed()
    assert len(before)==426
    packages=plan['proposed_packages']
    assert not packages.keys() & before.keys()
    complete={}
    for name,row in packages.items():
        file=package_path(HERE/'packages',row)
        verify_package(file,name,row)
        complete[name]=file
    env={**os.environ,'LC_ALL':'C','DEBIAN_FRONTEND':'noninteractive','DEBCONF_NONINTERACTIVE_SEEN':'true'}
    # Apt must acquire the local file item before giving dpkg an absolute path.
    # The containing build step has no network and the solver sees only these files.
    with TemporaryDirectory(prefix='dispatcharr-verified-local-apt-') as scratch:
        command=local_install_command(Path(scratch),complete.values())
        subprocess.run(command,env=env,check=True,timeout=240)
    after=installed()
    assert after=={**before,**{name:(row['architecture'],row['version']) for name,row in packages.items()}}
    facts={'base_installed_count':len(before),'new_installed_count':len(packages),'post_install_count':len(after),
           'preexisting_installed_packages_unchanged':True,'signed_archive_bytes_checked':sum(row['size'] for row in packages.values())}
    proof=Path('/out/install.json')
    proof.write_text(stable(facts))
    proof.chmod(0o444)


def elf(path):
    with Path(path).open('rb') as stream:
        data=stream.read(20)
    assert data[:6]==b'\x7fELF\x02\x01' and data[18:20]==b'\xb7\x00'


def build():
    assert os.getuid()==65532 and os.uname().machine=='aarch64'
    inputs=locked(HERE/'inputs.json')
    src=Path('/work/source')
    assert stamp(src/'mpeg2dec.c')==inputs['source_identity']['patched_mpeg2dec_sha256']
    assert stamp(src/'LICENSE')==inputs['source_identity']['upstream_license_sha256']
    env={**os.environ,'LC_ALL':'C','TZ':'UTC','SOURCE_DATE_EPOCH':'1744985293','PKG_CONFIG_LIBDIR':LIB+'/pkgconfig',
         'LDFLAGS':f'-L{LIB} -Wl,-rpath-link,{LIB} -Wl,-rpath,{LIB} -Wl,--disable-new-dtags',
         'argtable2_CFLAGS':'-I/usr/include','argtable2_LIBS':f'-L{LIB} -largtable2'}
    for key in ('PKG_CONFIG_PATH','LD_LIBRARY_PATH','LD_PRELOAD'):
        env.pop(key,None)
    target=call(['gcc','-dumpmachine'],env=env).strip()
    version=call(['gcc','-dumpfullversion'],env=env).strip()
    assert target=='aarch64-linux-gnu' and version=='15.2.0'
    expected={'libavformat':'62.3.100','libavcodec':'62.11.100','libavutil':'60.8.100','libswscale':'9.1.100'}
    assert {name:call(['pkg-config','--modversion',name],env=env).strip() for name in expected}==expected
    for command in (['bash','./autogen.sh'],['./configure'],['make','-j2']):
        subprocess.run(command,cwd=src,env=env,check=True,timeout=180)
    binary=src/'comskip'
    elf(binary)
    text=call(['readelf','-W','-l','-d',binary],env=env)
    direct=sorted(re.findall(r'\(NEEDED\).*?\[([^]]+)\]',text))
    required={*inputs['direct_runtime_inventory'],SONAME_LOADER}
    assert required <= set(direct) <= required | {'libm.so.6','libgcc_s.so.1'}
    assert re.findall(r'\(RPATH\).*?\[([^]]+)\]',text)==[LIB] and '(RUNPATH)' not in text
    assert f'Requesting program interpreter: {INTERPRETER}]' in text
    shutil.copyfile(binary,'/out/comskip')
    Path('/out/comskip').chmod(0o755)
    facts={'compiler_target':target,'compiler_version':version,'distribution_libav':expected,'DT_NEEDED':direct,'DT_RPATH':LIB,
           'native_interpreter':INTERPRETER,'candidate_sha256':stamp('/out/comskip'),'installed_toolchain':json.loads(Path('/out/install.json').read_bytes())}
    proof=Path('/out/build.json')
    proof.write_text(stable(facts))
    proof.chmod(0o444)


def closure(binary,inventory):
    rows={}
    paths={}
    env={name:value for name,value in os.environ.items() if name not in ('LD_LIBRARY_PATH','LD_PRELOAD')}
    links=call(['ldd',binary],env=env)
    assert 'not found' not in links
    for line in links.splitlines():
        tokens=line.split()
        if not tokens or tokens[0]=='linux-vdso.so.1':
            continue
        if len(tokens)==4:
            name,arrow,raw,_=tokens
            assert arrow=='=>'
        else:
            assert len(tokens)==2
            raw=tokens[0]
            name=Path(raw).name
        assert name not in rows and re.fullmatch('[a-zA-Z0-9_.+-]+',name)
        file=Path(raw).resolve(strict=True)
        assert file.is_relative_to(LIB)
        elf(file)
        rows[name]={'filename':file.name,'sha256':stamp(file)}
        paths[name]={raw,str(file)}
    for name,facts in inventory.items():
        assert rows[name]=={'filename':facts['real_library_basename'],'sha256':facts['library_sha256']}
        notice=Path('/usr/share/doc')/facts['runtime_package']/'copyright'
        assert stamp(notice)==facts['already_installed_package_notice_sha256']
    requested=sorted(set().union(*paths.values()))
    query=subprocess.run(['dpkg-query','-S',*requested],text=True,capture_output=True,timeout=25)
    assert query.returncode in (0,1)
    owners={}
    for line in query.stdout.splitlines():
        package,separator,path=line.rpartition(': ')
        if separator and path in requested:
            clean=[item.strip() for item in package.split(',')]
            if all(re.fullmatch(r'[a-z0-9][a-z0-9+.-]*(?::(?:arm64|all))?',item) for item in clean):
                owners.setdefault(path,set()).update(clean)
    all_notices={}
    missing=[]
    for name,locations in paths.items():
        packages=sorted(set().union(*(owners.get(path,set()) for path in locations)))
        found=[]
        for package in packages:
            documentation=Path('/usr/share/doc')/package.split(':',1)[0]/'copyright'
            if documentation.is_file():
                all_notices[package]=stamp(documentation)
                found.append(package)
        rows[name]['notice_packages']=found
        if not found:
            missing.append(name)
    return rows, {'installed_package_notice_sha256':all_notices,'libraries_without_package_notice':sorted(missing)}


def smoke():
    inputs=locked(HERE/'inputs.json')
    plan=inputs['toolchain_plan']
    assert os.getuid()==65532 and os.uname().machine=='aarch64'
    assert stamp('/var/lib/dpkg/status')==plan['base_donor_status_sha256']
    binary=APP/'comskip'
    note=inputs['source_identity']
    assert stamp(APP/'notices/Comskip-LICENSE')==note['upstream_license_sha256']
    assert stamp(APP/'notices/Original-DispatcharrBase')==note['original_recipe_sha256']
    assert stamp(APP/'comskip.ini')==inputs['detector_ini_sha256']
    credits=APP/'notices/Dispatcharr-ATTRIBUTION.md'
    assert credits.stat().st_size>0
    compiled=json.loads((APP/'build.json').read_bytes())
    assert stamp(binary)==compiled['candidate_sha256']; elf(binary)
    libs,notices=closure(binary,inputs['direct_runtime_inventory'])
    ffmpeg=Path(shutil.which('ffmpeg')).resolve(strict=True)
    ffprobe=Path(shutil.which('ffprobe')).resolve(strict=True)
    elf(ffmpeg); elf(ffprobe)
    environment={**os.environ,'LC_ALL':'C','TZ':'UTC'}
    with TemporaryDirectory(prefix='dispatcharr-comskip-media-',dir='/tmp') as scratch:
        cwd=Path(scratch)
        media=cwd/'owned.ts'
        opts={'cwd':cwd,'text':True,'capture_output':True,'env':environment,'timeout':60}
        subprocess.run([ffmpeg,'-hide_banner','-v','error','-f','lavfi','-i','testsrc2=size=320x240:rate=25',
                        '-f','lavfi','-i','sine=frequency=450:sample_rate=44100','-t','8','-threads','1',
                        '-c:v','mpeg2video','-c:a','mp2','-f','mpegts',media],check=True,**opts)
        shape=subprocess.run([ffprobe,'-v','error','-show_entries','stream=codec_name:format=duration','-of','json',media],check=True,**opts)
        info=json.loads(shape.stdout)
        duration=float(info['format']['duration'])
        assert sorted(row['codec_name'] for row in info['streams'])==['mp2','mpeg2video'] and 7.9<=duration<=8.2
        source=stamp(media)
        candidate_env={key:value for key,value in environment.items() if key not in ('LD_LIBRARY_PATH','LD_PRELOAD')}
        candidate_opts={**opts,'env':candidate_env}
        help_run=subprocess.run([binary,'--help'],**candidate_opts)
        assert help_run.returncode==2 and re.search(r'^Comskip 0\.83\.001',help_run.stderr,re.MULTILINE)
        detection=subprocess.run([binary,f'--ini={APP / "comskip.ini"}',media.name],**candidate_opts)
        assert detection.returncode==1 and (cwd/'owned.edl').stat().st_size==0 and stamp(media)==source
    return {'scope':'native target ABI replacement compile and no-commercials smoke on untouched exact Original runtime',
            'compiled':compiled,'UID':os.getuid(),'release_donor_package_status_preserved':True,
            'candidate_source':note,'loader_closure':libs,'runtime_notice_inventory':notices,'direct_donor_licenses_verified':len(inputs['direct_runtime_inventory']),
            'retained_candidate_source_and_recipe_notices':True,'retained_attribution_sha256':stamp(credits),
            'media':{'duration_seconds':duration,'codec_names':['mp2','mpeg2video'],'sha256':source,'original_unchanged':True,
                     'empty_EDL':True,'help_exit':help_run.returncode,'detector_exit':detection.returncode},
            'historical_binary_provenance_resolved':False,'distribution_qualified':False,'full_ARM64_image_verified':False,
            'credit':'Original Dispatcharr, Erik Kaashoek and Comskip contributors, FFmpeg, Ubuntu and dependency authors retain full credit.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase',choices=['acquire','install','build','smoke'])
    parser.add_argument('--context',type=Path)
    parser.add_argument('--root',type=Path)
    args=parser.parse_args()
    if args.phase=='acquire':
        print(stable(acquire(args.root,args.context)),end='')
    elif args.phase=='install':
        install()
    elif args.phase=='build':
        build()
    else:
        print(stable(smoke()),end='')


if __name__=='__main__':
    main()
