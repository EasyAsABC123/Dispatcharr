"""Resolve a private simulated ARM64 build against the verified donor package database."""
import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import subprocess
from urllib.request import HTTPRedirectHandler, Request, build_opener

HERE=Path(__file__).resolve().parent
LOCK_SHA='2a4f3c9dfd0e39f4c6002f2a6d80292a6f9540ba3b2e5e08bc92fd38d24d54c5'
SUITES=('resolute','resolute-updates','resolute-security')
TOOLS=('gcc','binutils','autoconf','automake','libtool','make','pkg-config')
PACKAGE=re.compile(r'[a-z0-9][a-z0-9.+-]*(?::(?:arm64|all))?')
VERSION=re.compile(r'[a-zA-Z0-9.+:~_-]+')
INST=re.compile(r'Inst ([^ ]+)(?: \[([^\]]+)\])? \(([^ )]+)(?: [^\r\n]*)?\)')


def stamp(data):
    return sha256(data).hexdigest()


def records(raw):
    for body in raw.split('\n\n'):
        if not body.strip():
            continue
        row={}
        name=None
        for line in body.splitlines():
            if line.startswith((' ','\t')):
                assert name
                row[name]+= '\n'+line.lstrip()
                continue
            name,value=line.split(':',1)
            assert name not in row
            row[name]=value.lstrip()
        yield row


def installed(status,fixed):
    rows={}
    for row in records(status.decode()):
        if row.get('Status')!='install ok installed':
            continue
        name,arch,version=(row[key] for key in ('Package','Architecture','Version'))
        assert PACKAGE.fullmatch(name) and arch in ('arm64','all') and VERSION.fullmatch(version)
        assert name not in rows
        rows[name]={'architecture':arch,'version':version}
    assert len(rows)>=50
    assert {name:rows[name]['version'] for name in fixed}==fixed
    return rows


def proposals(raw):
    result={}
    for line in raw.splitlines():
        assert not line.startswith(('Remv ','Purg '))
        if not line.startswith('Inst '):
            continue
        row=INST.fullmatch(line)
        assert row and PACKAGE.fullmatch(row[1]) and VERSION.fullmatch(row[3])
        name=row[1].split(':',1)[0]
        assert name not in result
        result[name]={'previous_version':row[2],'version':row[3]}
    assert 0 < len(result) <= 256
    return result


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*_):
        raise ValueError('unexpected Canonical key redirect')


def trusted_key(work,focus):
    url=f'https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x{focus}'
    with build_opener(NoRedirect()).open(Request(url),timeout=20) as response:
        raw=response.read(65537)
        assert response.status==200 and 0 < len(raw) <= 65536
    keyfile=work/'archive.asc'
    keyfile.write_bytes(raw)
    home=work/'gpg'
    home.mkdir(mode=0o700)
    gpg=['gpg','--homedir',str(home),'--batch','--no-options']
    shown=subprocess.check_output([*gpg,'--show-keys','--with-colons',str(keyfile)],stderr=subprocess.PIPE,text=True,timeout=10)
    lines=shown.splitlines()
    primary=[row.split(':')[9] for i,row in enumerate(lines) if row.startswith('fpr:') and lines[i-1].startswith('pub:')]
    assert primary==[focus]
    ring=work/'archive.gpg'
    subprocess.run([*gpg,'--dearmor','--output',str(ring),str(keyfile)],capture_output=True,check=True,timeout=10)
    return ring,home


def indexes(work,ring,home,focus):
    result={}
    for release in (work/'lists').glob('*_InRelease'):
        [suite]=[suite for suite in SUITES if release.name.endswith(f'_dists_{suite}_InRelease')]
        assert release.name.startswith('snapshot.ubuntu.com_') and suite not in result
        raw=subprocess.run(['gpgv','--homedir',str(home),'--status-fd','2','--keyring',str(ring),'--output','-',str(release)],
                           check=True,capture_output=True,text=True,timeout=12)
        assert any(line.startswith('[GNUPG:] VALIDSIG ') and focus in line.split()[2:] for line in raw.stderr.splitlines())
        [row]=list(records(raw.stdout))
        assert row['Origin']=='Ubuntu' and row['Codename']=='resolute' and row['Suite']==suite
        assert 'arm64' in row['Architectures'].split()
        result[suite]={'sha256':stamp(release.read_bytes()),'date':row['Date']}
    assert set(result)==set(SUITES)
    return result


def apt(work,command,stage,output):
    config=work/'apt.conf'
    env={**os.environ,'LC_ALL':'C','APT_CONFIG':str(config)}
    run=subprocess.run(command,capture_output=True,text=True,timeout=180,env=env)
    if run.returncode:
        data={'phase':stage,'error':'apt-exit','literal_apt_status':run.returncode}
        # Only bounded ordinary apt dependency names and relations enter the artifact.
        diagnostic=re.compile(r"\s*([a-zA-Z0-9.+:-]+)\s*:\s*(Depends|PreDepends|Breaks|Conflicts): ([a-zA-Z0-9 .:+~_()=<>-]{1,140})")
        matches=[match.groups() for line in (run.stdout+'\n'+run.stderr).splitlines() if (match:=diagnostic.fullmatch(line))]
        if matches:
            data['selected_dependency_messages']=matches[:16]
        (output/'failed-resolution.json').write_text(json.dumps(data,indent=2,sort_keys=True)+'\n')
        raise SystemExit(1)
    return run.stdout


def selected_metadata(raw,name,version):
    candidates=[]
    for row in records(raw):
        if row.get('Package')!=name or row.get('Version')!=version or 'Filename' not in row:
            continue
        architecture=row['Architecture']
        assert architecture in ('arm64','all')
        digest, filename, size=(row[key] for key in ('SHA256','Filename','Size'))
        assert re.fullmatch('[a-f0-9]{64}',digest) and 0<int(size)<100*1024*1024
        assert re.fullmatch(r'pool/[a-z0-9.-]+/[a-z0-9.-]+/[a-z0-9.+-]+/[a-zA-Z0-9.+:~_-]+_(arm64|all)\.deb',filename)
        source=row.get('Source',name).split(' ',1)[0]
        assert PACKAGE.fullmatch(source)
        facts={'architecture':architecture,'version':version,'sha256':digest,'filename':filename,'size':int(size),'source':source}
        if facts not in candidates:
            candidates.append(facts)
    [selected]=candidates
    return selected


def prepare(work,status,snapshot,ring):
    base='https://snapshot.ubuntu.com/ubuntu/'+snapshot
    for folder in ('lists/partial','archives/partial'):
        (work/folder).mkdir(parents=True)
    source=work/'ubuntu.list'
    source.write_text(''.join(f'deb [arch=arm64 signed-by={ring}] {base} {suite} main universe\n' for suite in SUITES))
    config=work/'apt.conf'
    config.write_text(f'''Dir::Etc::parts "-";
Dir::Etc::main "-";
Dir::Etc::sourcelist "{source}";
Dir::Etc::sourceparts "-";
Dir::State::status "{status}";
Dir::State::lists "{work/'lists'}";
Dir::State::extended_states "{work/'extended_states'}";
Dir::Cache::archives "{work/'archives'}";
Dir::Cache::pkgcache "{work/'pkgcache.bin'}";
Dir::Cache::srcpkgcache "{work/'srcpkgcache.bin'}";
APT::Architecture "arm64";
APT::Architectures {{ "arm64"; }};
Acquire::Languages "none";
APT::Update::Error-Mode "any";
Acquire::AllowInsecureRepositories "false";
APT::Get::AllowUnauthenticated "false";
Debug::NoLocking "true";
''')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work',type=Path,required=True)
    parser.add_argument('--status',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    work=args.work.resolve()
    work.mkdir(mode=0o700)
    status=args.status.read_bytes()
    assert 0<len(status)<4*1024*1024
    raw=(HERE/'headers.json').read_bytes()
    assert stamp(raw)==LOCK_SHA
    lock=json.loads(raw)
    fixed=lock['fixed_preinstalled_runtime_versions']
    before=installed(status,fixed)
    (args.output/'resolution-phase').write_text('archive-key\n')
    ring,home=trusted_key(work,lock['canonical_signer_fingerprint'])
    prepare(work,args.status.resolve(),lock['snapshot'],ring)
    (args.output/'resolution-phase').write_text('signed-apt-indexes\n')
    apt(work,['apt-get','update'],'index',args.output)
    signed=indexes(work,ring,home,lock['canonical_signer_fingerprint'])
    headers=lock['headers']
    pins={**fixed,**{name:fields['version'] for name,fields in headers.items()}}
    command=['apt-get','--simulate','--no-remove','--no-install-recommends','install',*TOOLS,
             *(f'{part}:arm64={version}' for part,version in sorted(pins.items()))]
    (args.output/'resolution-phase').write_text('simulate-install\n')
    proposed=proposals(apt(work,command,'solver',args.output))
    assert not set(fixed)&set(proposed)
    (args.output/'resolution-phase').write_text('signed-proposed-package-metadata\n')
    installed_headers={}
    for name,fields in headers.items():
        if name not in proposed:
            assert before[name]['architecture']=='arm64' and before[name]['version']==fields['version']
            installed_headers[name]=fields['version']
        else:
            assert proposed[name]['version']==fields['version']
    finalized={}
    for name,row in sorted(proposed.items()):
        version=row['version']
        info=apt(work,['apt-cache','show',f'{name}={version}'],'signed-metadata',args.output)
        selected=selected_metadata(info,name,version)
        assert row['previous_version']==before.get(name,{}).get('version')
        if name in headers:
            assert selected==headers[name]
        finalized[name]={**row,**selected}
    assert args.status.read_bytes()==status
    selected_tools={tool:finalized.get(tool,before.get(tool)) for tool in TOOLS}
    assert all(tool is not None for tool in selected_tools.values())
    result={'scope':'native ARM64 donor-based signed package selection simulation',
            'native_candidate_build_executed':False,'packages_installed_or_downloaded':False,'distribution_qualified':False,
            'base_donor_status_sha256':stamp(status),'base_installed_package_count':len(before),
            'exact_runtime_receipt_sha256':lock['authoritative_runtime_receipt_SHA256'],
            'exact_header_material_receipt_sha256':lock['previous_header_receipt_SHA256'],
            'Ubuntu_signed_snapshot':lock['snapshot'],'signed_suite_metadata':signed,
            'exact_preinstalled_runtime_versions_preserved':fixed,'preinstalled_exact_headers':installed_headers,
            'installed_or_selected_tool_versions':{n:d['version'] for n,d in selected_tools.items()},
            'proposed_package_count':len(finalized),'proposed_packages':finalized,
            'non_pinned_existing_version_changes':sorted(n for n,x in finalized.items() if x['previous_version'] is not None),
            'removals_proposed':0,'credit':'Original Dispatcharr, Erik Kaashoek and Comskip contributors, FFmpeg, Ubuntu and dependency authors retain full credit.'}
    (args.output/'toolchain-plan.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    (args.output/'resolution-phase').write_text('complete\n')


if __name__=='__main__':
    main()
