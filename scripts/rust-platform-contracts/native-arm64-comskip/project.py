"""Qualify the generated media and project the native dependency closure."""
from hashlib import file_digest
import json
from pathlib import Path
import re
import subprocess
import sys


def command(*args):
    return subprocess.check_output(args, text=True, timeout=15)


def sha(path):
    with path.open('rb') as stream:
        return file_digest(stream, 'sha256').hexdigest()


def arm_elf(path):
    header = command('readelf', '-h', path)
    assert re.search(r'^\s*Class:\s+ELF64\s*$', header, re.MULTILINE)
    assert re.search(r'^\s*Machine:\s+AArch64\s*$', header, re.MULTILINE)


def closure(binary, sysroot):
    arm_elf(binary)
    rows = {}
    report = command('ldd', binary)
    assert 'not found' not in report
    for line in report.splitlines():
        fields = line.strip().split()
        if not fields or fields[0] == 'linux-vdso.so.1':
            continue
        assert len(fields) in (2, 4), fields
        if len(fields) == 4:
            name, arrow, location, _ = fields
            assert arrow == '=>'
        else:
            location, _ = fields
            name = Path(location).name
        assert re.fullmatch(r'[A-Za-z0-9_+.-]+', name) and name not in rows
        library = Path(location).resolve(strict=True)
        if library.is_relative_to(sysroot):
            origin = 'signed-ubuntu-package'
        else:
            assert library.is_relative_to('/usr/lib/aarch64-linux-gnu')
            origin = 'ubuntu-arm64-runner'
        arm_elf(library)
        rows[name] = {'filename': library.name, 'sha256': sha(library), 'origin': origin}
    assert len(rows) >= 5
    return rows


def main():
    work, output, binary, ffmpeg = map(Path, sys.argv[1:])
    media = json.loads((work / 'media.json').read_text())
    assert sorted(stream['codec_name'] for stream in media['streams']) == ['mp2', 'mpeg2video']
    duration = float(media['format']['duration'])
    assert 7.9 <= duration <= 8.2
    media_hash = sha(work / 'owned.ts')
    assert (work / 'before.sha256').read_text().split()[0] == media_hash
    assert (work / 'owned.edl').stat().st_size == 0
    assert re.search(r'^Comskip 0\.83\.001', (work / 'help.stderr').read_text(), re.MULTILINE)
    dependencies = closure(binary, work / 'sysroot')
    required = ('libavcodec.so.', 'libavformat.so.', 'libavutil.so.', 'libswscale.so.', 'libargtable2.so.')
    assert all(any(name.startswith(prefix) for name in dependencies) for prefix in required)
    dynamic = command('readelf', '--dynamic', binary)
    assert not re.search(r'\((?:RUNPATH|RPATH)\)', dynamic)
    direct = sorted(re.findall(r'\(NEEDED\).*?\[([^]]+)\]', dynamic))
    assert direct and all(name in dependencies for name in direct)
    arm_elf(ffmpeg)
    compiler = (output / 'compiler-version').read_text().strip()
    assert re.fullmatch(r'\d+\.\d+(?:\.\d+)?', compiler)
    help_code, detector_code = map(int, (output / 'detector-statuses').read_text().split())
    assert (help_code, detector_code) == (2, 1)
    signed_index = (output / 'signed-indexes.sha256').read_text().splitlines()
    assert len(signed_index) == 3
    assert (output / 'fixture-loader-exit').read_text() == '0\n'
    assert (output / 'fixture-unresolved-sonames.txt').stat().st_size == 0
    print(json.dumps({
        'source': json.loads((output / 'source.json').read_text()),
        'validation_revision': (output / 'validation-commit').read_text().strip(),
        'validation_parent': (output / 'validation-parent').read_text().strip(),
        'platform': {'runner': 'ubuntu-24.04-arm', 'native_uname': 'aarch64',
                     'dpkg_architecture': 'arm64', 'gcc_target': 'aarch64-linux-gnu',
                     'gcc_version': compiler, 'ELF': 'ELF64 AArch64'},
        'comskip': {'version': '0.83.001', 'sha256': sha(binary),
                    'direct_DT_NEEDED': direct, 'runtime_closure': dependencies},
        'apt_packages_sourced_from_runner_or_signed_ubuntu_archive': True,
        'ffmpeg_generator_sha256': sha(ffmpeg),
        'fixture_ffmpeg_and_ffprobe_loader_exit': 0,
        'media': {'codec_names': ['mp2', 'mpeg2video'], 'duration_seconds': duration,
                  'sha256': media_hash, 'input_unchanged': True, 'empty_EDL': True,
                  'help_exit': help_code, 'detector_exit': detector_code},
        'original_binary_reproduced': False,
        'historical_donor_source_identified': False,
        'distribution_qualified': False,
        'full_ARM64_package_verified': False,
    }, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
