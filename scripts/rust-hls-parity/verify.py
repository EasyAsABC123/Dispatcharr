#!/usr/bin/env python3
"""Run unchanged upstream HLS tests against a Rust adapter plus the real oracle.

Every adapter call compares segment bytes, durations, flags, parser results, and
open-segment state with the pinned original Python implementation. No Django
modules are imported. This proves component fidelity, not HLS HTTP/session parity.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import select
import subprocess
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[2]
REFERENCE = ROOT / 'apps/proxy/live_proxy/output/hls/segmenter.py'
TESTS = ROOT / 'apps/proxy/live_proxy/tests/test_hls_segmenter.py'
REFERENCE_SHA = '593a6faadbfbb1c638a5338c8bdf640478cceb8657e081940af2cc6d9a89a19c'
TESTS_SHA = 'bc511ce6085d9ddf0851e89486547ef9b30388f91a34a767778a28119d7260f6'


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def compare(actual, expected, context):
    if actual == expected:
        return
    if isinstance(actual, dict) and isinstance(expected, dict) and actual.keys() == expected.keys():
        for key in actual:
            compare(actual[key], expected[key], context + '.' + str(key))
    if isinstance(actual, list) and isinstance(expected, list) and len(actual) == len(expected):
        for index, (left, right) in enumerate(zip(actual, expected)):
            compare(left, right, context + '[' + str(index) + ']')
    if isinstance(actual, (str, bytes)) and isinstance(expected, type(actual)):
        offset = next((index for index, pair in enumerate(zip(actual, expected)) if pair[0] != pair[1]),
                      min(len(actual), len(expected)))
        raise AssertionError(f'{context}: first difference at {offset}; lengths {len(actual)}/{len(expected)}')
    raise AssertionError(f'{context}: Rust={actual!r}, Python={expected!r}')


class Rpc:
    def __init__(self, binary):
        self.process = subprocess.Popen([str(binary)], cwd=ROOT, text=True, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, bufsize=1)
        self.calls = 0

    def call(self, **command):
        self.process.stdin.write(json.dumps(command, separators=(',', ':')) + '\n')
        self.process.stdin.flush()
        if not select.select([self.process.stdout], [], [], 15)[0]:
            raise TimeoutError('Rust HLS adapter did not respond')
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError('Rust HLS adapter exited before response')
        result = json.loads(line)
        self.calls += 1
        if 'error' in result:
            raise AssertionError('Rust adapter error: ' + result['error'])
        return result

    def close(self):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)


def normalize(segment):
    return None if segment is None else dict(data=segment.data.hex(), duration=segment.duration,
                                             discontinuity=segment.discontinuity)


def install_adapter(rpc, oracle):
    def oracle_state(segmenter):
        return dict(collecting=segmenter._collecting, current=bytes(segmenter._current).hex(),
                    video_detected=segmenter.video_detected, audio_only=segmenter.audio_only,
                    param_cache_complete=segmenter._param_cache_complete())

    def segment(value):
        return None if value is None else types.SimpleNamespace(data=bytes.fromhex(value['data']),
                                                               duration=value['duration'],
                                                               discontinuity=value['discontinuity'])

    class RustSegmenter:
        def __init__(self, target_duration=4.0, max_segment_duration=None, startup_keyframe_cuts=4):
            self.oracle = oracle.TSSegmenter(target_duration, max_segment_duration, startup_keyframe_cuts)
            result = rpc.call(op='new', target=target_duration, ceiling=max_segment_duration,
                              startup_cuts=startup_keyframe_cuts)
            self._state = result['state']
            compare(self._state, oracle_state(self.oracle), 'new.state')

        def feed(self, data):
            expected = [normalize(item) for item in self.oracle.feed(data)]
            result = rpc.call(op='feed', data=data.hex())
            compare(result['segments'], expected, 'feed.segments')
            self._state = result['state']
            compare(self._state, oracle_state(self.oracle), 'feed.state')
            return [segment(item) for item in result['segments']]

        def flag_discontinuity(self):
            expected = normalize(self.oracle.flag_discontinuity())
            result = rpc.call(op='discontinuity')
            compare(result['segment'], expected, 'discontinuity.segment')
            self._state = result['state']
            compare(self._state, oracle_state(self.oracle), 'discontinuity.state')
            return segment(result['segment'])

        @property
        def _collecting(self):
            return self._state['collecting']

        @property
        def _current(self):
            return bytes.fromhex(self._state['current'])

        @property
        def video_detected(self):
            return self._state['video_detected']

        @property
        def audio_only(self):
            return self._state['audio_only']

        def _param_cache_complete(self):
            return self._state['param_cache_complete']

    def packet_result(packet, stream_type=0):
        result = rpc.call(op='packet', data=packet.hex(), stream_type=stream_type)
        expected = dict(pid=oracle.packet_pid(packet), cc=oracle.packet_continuity_counter(packet),
                        pat=oracle.parse_pat(packet), pmt=list(oracle.parse_pmt(packet)),
                        streams=oracle.parse_pmt_streams(packet), pts=oracle.extract_pts(packet),
                        keyframe=oracle.starts_keyframe(packet, stream_type),
                        parameters={str(kind): value.hex() for kind, value in oracle.extract_parameter_sets(packet, stream_type).items()},
                        decoder_init=oracle.packet_has_decoder_init(packet, stream_type))
        if expected['streams'] is not None:
            expected['streams'] = [list(item) for item in expected['streams']]
        compare(result, expected, 'parser')
        return result

    def render(window, target_duration, segment_name='{seq}.ts', adv_target=None,
               disc_sequence=0, start_behind_seconds=None):
        expected = oracle.render_media_playlist(window, target_duration, segment_name, adv_target,
                                                disc_sequence, start_behind_seconds)
        value = rpc.call(op='playlist', window=window, target=target_duration, segment_name=segment_name,
                         adv_target=adv_target, disc_sequence=disc_sequence,
                         start_behind_seconds=start_behind_seconds)['playlist']
        compare(value, expected, 'playlist')
        return value

    name = 'apps.proxy.live_proxy.output.hls.segmenter'
    pieces = name.split('.')
    for count in range(1, len(pieces)):
        package = types.ModuleType('.'.join(pieces[:count]))
        package.__path__ = []
        sys.modules[package.__name__] = package
    adapter = types.ModuleType(name)
    adapter.TSSegmenter = RustSegmenter
    adapter.TS_PACKET_SIZE = 188
    adapter.render_media_playlist = render
    adapter.packet_pid = lambda packet: packet_result(packet)['pid']
    adapter.packet_continuity_counter = lambda packet: packet_result(packet)['cc']
    adapter.parse_pat = lambda packet: packet_result(packet)['pat']
    adapter.parse_pmt = lambda packet: tuple(packet_result(packet)['pmt'])
    adapter.parse_pmt_streams = lambda packet: (None if (value := packet_result(packet)['streams']) is None
                                             else [tuple(item) for item in value])
    adapter.extract_pts = lambda packet: packet_result(packet)['pts']
    adapter.starts_keyframe = lambda packet, kind: packet_result(packet, kind)['keyframe']
    adapter.extract_parameter_sets = lambda packet, kind: {
        int(key): bytes.fromhex(value) for key, value in packet_result(packet, kind)['parameters'].items()}
    adapter.packet_has_decoder_init = lambda packet, kind: packet_result(packet, kind)['decoder_init']
    sys.modules[name] = adapter
    return adapter, packet_result


def generated_cases(adapter, fixtures, packet_result, seed):
    rng = random.Random(seed)
    sequences = 0
    for codec in (fixtures.H264, fixtures.HEVC, fixtures.MPEG2, fixtures.AAC):
        for startup in (0, 2, 4):
            segmenter = adapter.TSSegmenter(target_duration=2, startup_keyframe_cuts=startup)
            packet = fixtures.make_audio_pmt() if codec == fixtures.AAC else fixtures.make_pmt(codec)
            segmenter.feed(fixtures.make_pat() + packet)
            packets = []
            for index in range(100):
                pts = (index / 10 if index < 50 else (index - 50) / 10)
                if codec == fixtures.AAC:
                    packet = fixtures.make_audio_pes(pts)
                elif codec == fixtures.H264:
                    packet = fixtures.make_h264_pes(pts, [7, 8, 5] if index % 10 == 0 else [9, 7, 8, 1])
                elif codec == fixtures.HEVC:
                    packet = fixtures.make_hevc_pes(pts, [32, 33, 34, 21] if index % 10 == 0 else [34, 1])
                else:
                    packet = fixtures.make_mpeg2_pes(pts, [fixtures.mpeg2_picture(1 if index % 10 == 0 else 2)])
                packets.append(packet)
            data = b''.join(packets)
            offset = 0
            while offset < len(data):
                count = rng.choice((1, 7, 187, 188, 189, 377, 8192))
                segmenter.feed(data[offset:offset + count])
                offset += count
            segmenter.flag_discontinuity()
            sequences += 1
    for _ in range(1000):
        packet = bytearray(rng.randbytes(188))
        packet[0] = 0x47
        packet_result(bytes(packet), rng.choice((0x1b, 0x24, 1, 2, 0)))
    return sequences


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--seed', type=int, default=20261109)
    parser.add_argument('--fixture', action='append', type=Path, default=[],
                        help='Also compare a real TS fixture through both segmenters')
    args = parser.parse_args()
    for path, fingerprint in ((REFERENCE, REFERENCE_SHA), (TESTS, TESTS_SHA)):
        if hashlib.sha256(path.read_bytes()).hexdigest() != fingerprint:
            raise RuntimeError('HLS source differs from pinned baseline: ' + str(path))
    subprocess.run(['cargo', 'build', '--manifest-path', str(ROOT / 'rust/Cargo.toml'), '--locked',
                    '--release', '-p', 'dispatcharr-hls-core', '--example', 'hls_parity'], check=True, cwd=ROOT)
    target = Path(os.environ.get('CARGO_TARGET_DIR', ROOT / 'rust/target'))
    if not target.is_absolute():
        target = ROOT / target
    rpc = Rpc(target / 'release/examples/hls_parity')
    report = dict(scope='HLS-segmenter-component-parity', passed=False,
                  application_performance_verified=False, reference_sha256=REFERENCE_SHA,
                  unchanged_tests_sha256=TESTS_SHA, seed=args.seed)
    report['rustc'] = subprocess.check_output(['rustc', '--version'], text=True).strip()
    try:
        oracle = load(REFERENCE, '_dispatcharr_hls_reference')
        adapter, packet_result = install_adapter(rpc, oracle)
        tests = load(TESTS, '_dispatcharr_unchanged_hls_tests')
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(tests))
        report['unchanged_upstream_tests'] = result.testsRun
        if not result.wasSuccessful():
            raise AssertionError('Unchanged upstream test expectations failed against Rust')
        report['generated_sequences'] = generated_cases(adapter, tests, packet_result, args.seed)
        report['generated_parser_packets'] = 1000
        report['real_fixtures'] = []
        for fixture in args.fixture:
            segmenter = adapter.TSSegmenter()
            count = 0
            with fixture.open('rb') as stream:
                while data := stream.read(64 * 1024):
                    count += len(segmenter.feed(data))
            count += segmenter.flag_discontinuity() is not None
            report['real_fixtures'].append(dict(name=fixture.name,
                sha256=hashlib.sha256(fixture.read_bytes()).hexdigest(), segments=count))
        report['rpc_comparisons'] = rpc.calls
        report['passed'] = True
    finally:
        rpc.close()
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
