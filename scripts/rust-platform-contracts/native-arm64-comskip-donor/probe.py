"""Read a verified native Original image's direct ARM64 Comskip package inputs."""
import argparse
from hashlib import file_digest
import json
import os
from pathlib import Path
import re
import subprocess

# The enclosing probe launcher must verify the exact immutable image and config.
# These duplicate a small immutable allowlist so the guest has no artifact deps.
MANIFEST = 'sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace'
CONFIG = 'sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43'
BINARY = '75bc4daf372684c366fbd86a3d53106f12d1be571a63ed4d55bc28d400ab4692'
LIB = Path('/usr/lib/aarch64-linux-gnu')
PACKAGES = {
    'libargtable2.so.0': ('libargtable2-0', 'libargtable2-dev'),
    'libavcodec.so.62': ('libavcodec62', 'libavcodec-dev'),
    'libavformat.so.62': ('libavformat62', 'libavformat-dev'),
    'libavutil.so.60': ('libavutil60', 'libavutil-dev'),
    'libswscale.so.9': ('libswscale9', 'libswscale-dev'),
    'libc.so.6': ('libc6', 'libc6-dev'),
}
FIELD = r'[a-zA-Z0-9.+:~_-]+'


def command(*args):
    return subprocess.check_output(args,text=True,timeout=8,env={'PATH':'/usr/bin:/bin','LC_ALL':'C'})


def sha(path):
    with path.open('rb') as stream:
        return file_digest(stream,'sha256').hexdigest()


def elf64_aarch64(path):
    with path.open('rb') as stream:
        header = stream.read(20)
    assert header[:6] == b'\x7fELF\x02\x01' and header[18:20] == b'\xb7\x00'


def packages():
    data = {}
    for soname,(runtime,headers) in PACKAGES.items():
        requested = f'{runtime}:arm64'
        row = command('/usr/bin/dpkg-query','-W','-f=${db:Status-Status}\t${Package}\t${Architecture}\t${Version}\t${source:Package}\t${source:Version}\n',requested)
        fields = row.rstrip('\n').split('\t')
        assert len(fields) == 6 and fields[:3] == ['installed',runtime,'arm64']
        version, source, source_version = fields[3:]
        assert all(re.fullmatch(FIELD,item) for item in (version,source,source_version))
        names = set(command('/usr/bin/dpkg-query','-L',requested).splitlines())
        named = LIB / soname
        actual = named.resolve(strict=True)
        assert actual.is_relative_to(LIB) and any(str(path) in names for path in (named,actual))
        elf64_aarch64(actual)
        doc = Path('/usr/share/doc') / runtime / 'copyright'
        data[soname] = {'runtime_package':runtime,'architecture':'arm64','version':version,'source_package':source,
                        'source_version':source_version,'real_library_basename':actual.name,'library_sha256':sha(actual),
                        'already_installed_package_notice_sha256':sha(doc) if doc.is_file() else None,
                        'matching_build_headers_to_acquire':headers}
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verified-oci-manifest', required=True, choices=[MANIFEST])
    parser.add_argument('--verified-oci-config', required=True, choices=[CONFIG])
    args = parser.parse_args()
    assert os.uname().machine == 'aarch64' and os.uname().sysname == 'Linux' and os.getuid() != 0
    assert command('/usr/bin/dpkg','--print-architecture').strip() == 'arm64'
    original = Path('/usr/local/bin/comskip')
    assert sha(original) == BINARY
    elf64_aarch64(original)
    release = Path('/etc/os-release').read_text()
    def read_field(name):
        [value] = re.findall(rf'^{name}="?([a-zA-Z0-9.]+)"?$', release, re.MULTILINE)
        return value
    assert read_field('ID') == 'ubuntu'
    data = packages()
    ffmpeg_versions = {data[name]['source_version'] for name in data if re.match(r'lib(?:av|swscale)',name)}
    assert len(ffmpeg_versions) == 1
    interpreter = Path('/lib/ld-linux-aarch64.so.1').resolve(strict=True)
    assert interpreter.is_relative_to(LIB)
    elf64_aarch64(interpreter)
    print(json.dumps({'scope':'direct ordinary donor ABI and installed package ownership only',
                      'oci_identity_from_qualified_launcher':{'manifest':args.verified_oci_manifest,'config':args.verified_oci_config},
                      'native_platform':'linux/arm64','ubuntu_version':read_field('VERSION_ID'),
                      'ubuntu_codename':read_field('VERSION_CODENAME'), 'donor_original_binary_sha256':BINARY,
                      'direct_SONAMEs':data,'glibc_interpreter_sha256':sha(interpreter),
                      'signed_build_inputs_qualified':False,'candidate_binary_executed':False,
                      'historical_detector_executed':False,'redistribution_qualified':False},
                     indent=2,sort_keys=True))


if __name__ == '__main__':
    main()
