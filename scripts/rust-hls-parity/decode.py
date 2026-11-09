#!/usr/bin/env python3
"""Generate pinned valid codec fixtures, segment with Rust, decode each TS file.

The segmenter runs locally using the pinned Rust toolchain. FFmpeg generation
and decoding use the existing digest-pinned Docker baseline image. This checks
the packet-copy component, not capability URLs or complete HLS playback parity.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[2]
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--codecs', nargs='+', choices=['mpeg2', 'h264', 'hevc', 'aac'],
                        default=['mpeg2', 'h264', 'hevc', 'aac'])
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error('Output must be empty')
    report = dict(scope='TS-HLS-segmenter-decoder-component', passed=False,
                  application_performance_verified=False, image=IMAGE, codecs=[])
    report['rustc'] = subprocess.check_output(['rustc', '--version'], text=True).strip()
    report['cargo'] = subprocess.check_output(['cargo', '--version'], text=True).strip()
    report['source_sha256'] = {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((ROOT / 'rust/crates/hls-core').rglob('*'))
        if path.is_file() and path.suffix in ('.rs', '.toml')
    }
    target = Path(os.environ.get('CARGO_TARGET_DIR', ROOT / 'rust/target'))
    if not target.is_absolute():
        target = ROOT / target

    def execute(command, log, timeout=120):
        result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=timeout)
        log.write_text(result.stdout + result.stderr)
        if result.returncode:
            raise RuntimeError('Command failed; inspect ' + str(log))
        return result.stdout

    def ffmpeg(arguments, log):
        name = 'dispatcharr-hls-codec-' + uuid.uuid4().hex[:12]
        try:
            return execute([args.docker, 'run', '--rm', '--name', name, '--network', 'none', '-v',
                            str(output) + ':/evidence', '--entrypoint', 'ffmpeg', IMAGE,
                            '-nostdin', '-hide_banner', *arguments], log)
        except BaseException:
            subprocess.run([args.docker, 'rm', '-f', name], text=True, capture_output=True, timeout=30)
            raise

    try:
        execute(['cargo', 'build', '--manifest-path', str(ROOT / 'rust/Cargo.toml'), '--locked',
                 '--release', '-p', 'dispatcharr-hls-core', '--example', 'segment_file'], output / 'build.log')
        ffmpeg(['-version'], output / 'ffmpeg-version.txt')
        for codec in args.codecs:
            directory = output / codec
            directory.mkdir()
            source = directory / 'input.ts'
            source_path = f'/evidence/{codec}/input.ts'
            generated = ['-loglevel', 'error']
            if codec != 'aac':
                generated += ['-f', 'lavfi', '-i', 'testsrc2=size=320x180:rate=25']
            generated += ['-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=48000', '-t', '16']
            if codec == 'mpeg2':
                generated += ['-c:v', 'mpeg2video', '-g', '50', '-bf', '0', '-b:v', '1000k',
                              '-threads', '1', '-c:a', 'mp2', '-b:a', '128k']
            elif codec == 'h264':
                generated += ['-c:v', 'libx264', '-preset', 'veryfast', '-g', '50', '-keyint_min', '50',
                              '-sc_threshold', '0', '-bf', '0', '-threads', '1', '-c:a', 'aac', '-b:a', '96k']
            elif codec == 'hevc':
                generated += ['-c:v', 'libx265', '-preset', 'ultrafast', '-threads', '1',
                              '-x265-params', 'pools=none:frame-threads=1:keyint=50:min-keyint=50:scenecut=0:bframes=0:log-level=error',
                              '-c:a', 'aac', '-b:a', '96k']
            else:
                generated += ['-c:a', 'aac', '-b:a', '96k']
            generated += ['-fflags', '+bitexact', '-flags:a', '+bitexact']
            if codec != 'aac':
                generated += ['-flags:v', '+bitexact', '-muxrate', '2000000']
            generated += ['-f', 'mpegts', source_path]
            ffmpeg(generated, directory / 'generate.log')
            execute([str(target / 'release/examples/segment_file'), str(source),
                     str(directory / 'segments')], directory / 'segment.log')
            descriptors = json.loads((directory / 'segments/segments.json').read_text())['segments']
            if len(descriptors) < 3:
                raise RuntimeError('Too few independently decodable segments for ' + codec)
            results = []
            for descriptor in descriptors:
                filename = descriptor['filename']
                segment = directory / 'segments' / filename
                text = ffmpeg(['-loglevel', 'error', '-xerror', '-progress', 'pipe:1', '-nostats',
                               '-i', f'/evidence/{codec}/segments/{filename}', '-f', 'null', '-'],
                              directory / ('decode-' + filename + '.log'))
                progress = {}
                for line in text.splitlines():
                    key, _, value = line.partition('=')
                    progress[key] = value
                if progress.get('progress') != 'end' or int(progress.get('out_time_us', '0')) <= 0:
                    raise RuntimeError('Decoder produced no timed samples for ' + codec + '/' + filename)
                if codec != 'aac' and int(progress.get('frame', '0')) <= 0:
                    raise RuntimeError('Decoder produced no video for ' + codec + '/' + filename)
                results.append(dict(**descriptor, sha256=hashlib.sha256(segment.read_bytes()).hexdigest(),
                                    decoded_frames=progress.get('frame'), out_time_us=int(progress['out_time_us'])))
            report['codecs'].append(dict(codec=codec, fixture_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                                         segments=results, independently_decoded_segments=len(results)))
        report['passed'] = True
    except (OSError, subprocess.SubprocessError, RuntimeError) as error:
        report['error'] = str(error)
    finally:
        (output / 'evidence.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'output': str(output),
                      'decoded_codecs': [codec['codec'] for codec in report['codecs']],
                      'error': report.get('error')}, indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
