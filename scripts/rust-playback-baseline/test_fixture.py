"""Fixture controls and matrix validation; no application parity claims."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('playback_origin', HERE / 'origin.py')
origin = importlib.util.module_from_spec(spec)
spec.loader.exec_module(origin)
spec_supervisor = importlib.util.spec_from_file_location('playback_supervisor', HERE / 'supervisor.py')
supervisor = importlib.util.module_from_spec(spec_supervisor)
spec_supervisor.loader.exec_module(supervisor)
spec_runner = importlib.util.spec_from_file_location('playback_runner', HERE / 'run.py')
runner = importlib.util.module_from_spec(spec_runner)
spec_runner.loader.exec_module(runner)
spec_mixed = importlib.util.spec_from_file_location('playback_mixed', HERE / 'mixed_probe.py')
mixed = importlib.util.module_from_spec(spec_mixed)
spec_mixed.loader.exec_module(mixed)
spec_fault = importlib.util.spec_from_file_location('playback_fault', HERE / 'fault_probe.py')
fault = importlib.util.module_from_spec(spec_fault)
spec_fault.loader.exec_module(fault)
spec_protocol = importlib.util.spec_from_file_location('playback_protocol', HERE / 'protocol_probe.py')
protocol = importlib.util.module_from_spec(spec_protocol)
spec_protocol.loader.exec_module(protocol)


class FixtureTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        clip = Path(self.directory.name) / 'fixture.ts'
        clip.write_bytes((bytes([0x47]) + bytes(187)) * 10000)
        self.mp4 = Path(self.directory.name) / 'fixture.mp4'
        self.mp4_bytes = bytes(range(256)) * 200
        self.mp4.write_bytes(self.mp4_bytes)
        self.state = origin.FixtureState(clip, 1800, mp4=self.mp4)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), origin.handler_for(self.state))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_address[1]}'

    def tearDown(self):
        self.state.control('/live/0.ts', 'drop')
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.directory.cleanup()

    def control(self, action, **extra):
        payload = json.dumps(dict(path='/live/0.ts', action=action, **extra)).encode()
        with urllib.request.urlopen(urllib.request.Request(self.url + '/control', data=payload,
                                                           headers={'Content-Type': 'application/json'}), timeout=2) as response:
            return json.load(response)

    def test_vod_head_and_single_ranges_preserve_exact_bytes_and_validators(self):
        request = urllib.request.Request(self.url + '/vod.mp4', method='HEAD',
                                         headers={'Range': 'bytes=0-10'})
        with urllib.request.urlopen(request, timeout=2) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.read(), b'')
            self.assertEqual(int(response.headers['Content-Length']), len(self.mp4_bytes))
            etag = response.headers['ETag']
        for header, expected, content_range in [
                ('bytes=2-10', self.mp4_bytes[2:11], f'bytes 2-10/{len(self.mp4_bytes)}'),
                ('bytes=-17', self.mp4_bytes[-17:], f'bytes {len(self.mp4_bytes)-17}-{len(self.mp4_bytes)-1}/{len(self.mp4_bytes)}')]:
            request = urllib.request.Request(self.url + '/vod.mp4', headers={'Range': header, 'If-Range': etag})
            with urllib.request.urlopen(request, timeout=2) as response:
                self.assertEqual(response.status, 206)
                self.assertEqual(response.read(), expected)
                self.assertEqual(response.headers['Content-Range'], content_range)
                self.assertEqual(response.headers['ETag'], etag)
                self.assertEqual(response.headers['Content-Type'], 'video/mp4')

    def test_vod_malformed_unsupported_and_unsatisfiable_ranges_are_distinct(self):
        for value in ['bytes=bad', 'items=0-10', 'bytes=0-1,4-5', 'bytes=10-2']:
            request = urllib.request.Request(self.url + '/vod.mp4', headers={'Range': value})
            with urllib.request.urlopen(request, timeout=2) as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(response.read(), self.mp4_bytes)
        request = urllib.request.Request(self.url + '/vod.mp4',
                                         headers={'Range': f'bytes={len(self.mp4_bytes)}-'})
        with self.assertRaises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(request, timeout=2)
        with error.exception as response:
            self.assertEqual(response.code, 416)
            self.assertEqual(response.headers['Content-Range'], f'bytes */{len(self.mp4_bytes)}')
            self.assertEqual(response.read(), b'')

    def test_vod_etag_conditionals_do_not_return_wrong_partial_representation(self):
        with urllib.request.urlopen(self.url + '/vod.mp4', timeout=2) as response:
            etag = response.headers['ETag']
        request = urllib.request.Request(self.url + '/vod.mp4',
                                         headers={'Range': 'bytes=0-3', 'If-Range': '"old"'})
        with urllib.request.urlopen(request, timeout=2) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.read(), self.mp4_bytes)
        request = urllib.request.Request(self.url + '/vod.mp4', headers={'If-None-Match': 'W/' + etag})
        with self.assertRaises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(request, timeout=2)
        with error.exception as response:
            self.assertEqual(response.code, 304)
            self.assertEqual(response.read(), b'')

    def test_fault_probe_records_resumed_packets_without_claiming_decoder_proof(self):
        result = fault.probe(self.url + '/live/0.ts', self.url, '/live/0.ts', '/live/100.ts',
                             Path(self.directory.name) / 'stall-probe', 'stall', timeout=2,
                             recovery_observation=.2, stall_seconds=.1, prefault_bytes=1316)
        self.assertTrue(result['passed'], result)
        self.assertFalse(result['decoder_verified'])
        self.assertGreater(result['capture']['packets'], 0)

    def test_no_alternate_is_a_failed_failover_probe(self):
        result = fault.probe(self.url + '/live/0.ts', self.url, '/live/0.ts', '/live/100.ts',
                             Path(self.directory.name) / 'failed-probe', 'failover', timeout=1,
                             recovery_observation=.1, prefault_bytes=1316)
        self.assertFalse(result['passed'], result)
        self.assertTrue(result['failures'])

    def test_mixed_reader_probe_detects_actual_slow_fast_independence(self):
        result = mixed.run_probe(self.url + '/live/0.ts', 2, 1, .2, 1, .04, .1, 100000)
        self.assertTrue(result['passed'], result)
        self.assertGreater(result['summary']['fast_bytes_per_second_min'], 100000)
        self.assertLess(result['summary']['slow_bytes_per_second_max'], 30000)
        self.assertFalse(result['memory_bound_proven'])

    def test_fault_control_fails_and_recovers_only_requested_path(self):
        self.control('fail')
        with self.assertRaises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(self.url + '/live/0.ts', timeout=2)
        self.assertEqual(error.exception.code, 503)
        error.exception.close()
        self.control('recover')
        with urllib.request.urlopen(self.url + '/live/0.ts', timeout=2) as response:
            self.assertEqual(response.read(188)[0], 0x47)
        with urllib.request.urlopen(self.url + '/live/1.ts', timeout=2) as response:
            self.assertEqual(response.read(188)[0], 0x47)
        deadline = time.monotonic() + 2
        while self.state.snapshot()['active'] and time.monotonic() < deadline:
            time.sleep(.01)
        stats = self.state.snapshot()
        self.assertEqual(stats['active'], 0)
        self.assertEqual(stats['paths']['/live/0.ts']['connections'], 1)
        self.assertEqual(stats['paths']['/live/1.ts']['connections'], 1)

    def test_drop_ends_existing_connection_and_stall_is_bounded(self):
        self.control('stall', seconds=.1)
        began = time.monotonic()
        with urllib.request.urlopen(self.url + '/live/0.ts', timeout=2) as response:
            self.assertEqual(response.read(188)[0], 0x47)
            self.assertGreaterEqual(time.monotonic() - began, .08)
            self.control('drop')
            response.read()
        self.assertEqual(self.state.snapshot()['eof_connections'], 0)

    def test_invalid_controls_and_timestamp_wrap_are_rejected(self):
        for path, action, seconds in [('/other', 'stall', 1), ('/live.ts', 'bad', 0),
                                      ('/live.ts', 'stall', float('nan'))]:
            with self.assertRaises(ValueError):
                self.state.control(path, action, seconds)
        self.assertFalse(origin.valid_stream_path('/live/../1.ts'))
        self.assertTrue(origin.valid_stream_path('/live/10.ts'))


class SupervisorTests(unittest.TestCase):
    def test_stop_restart_touches_only_owned_process_group(self):
        sentinel = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
        web = supervisor.WebProcess([sys.executable, '-c', 'import time; time.sleep(30)'])
        try:
            first = web.start()['pid']
            self.assertTrue(web.status()['running'])
            self.assertFalse(web.stop()['running'])
            self.assertIsNone(sentinel.poll())
            self.assertNotEqual(web.start()['pid'], first)
            self.assertTrue(web.status()['running'])
        finally:
            web.stop()
            sentinel.terminate()
            sentinel.wait(timeout=3)


class ProtocolParsingTests(unittest.TestCase):
    def test_incomplete_mp4_fragment_is_not_passed_to_decoder(self):
        box = lambda kind, payload: (len(payload) + 8).to_bytes(4, 'big') + kind + payload
        complete = box(b'ftyp', b'isom') + box(b'moov', b'init')
        partial = box(b'moof', b'fragment')[:-3]
        length, boxes = protocol.complete_mp4_prefix(complete + partial)
        self.assertEqual(length, len(complete))
        self.assertEqual([value['type'] for value in boxes], ['ftyp', 'moov'])
        with self.assertRaises(ValueError):
            protocol.complete_mp4_prefix(bytes(8))

    def test_native_hls_requires_complete_media_durations(self):
        playlist = '#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:3.96,\n1.ts\n'
        self.assertEqual(protocol.playlist_segments(playlist), ['1.ts'])
        for invalid in ['#EXTM3U\n', '#EXTM3U\n1.ts\n',
                        '#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:nan,\n1.ts\n']:
            with self.assertRaises(ValueError):
                protocol.playlist_segments(invalid)


class LoadProcessTests(unittest.TestCase):
    def test_timeout_kills_only_owned_load_process(self):
        sentinel = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
        try:
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.run_load_command([sys.executable, '-c', 'import time; time.sleep(30)'],
                                        dict(os.environ), .05)
            self.assertIsNone(sentinel.poll())
        finally:
            sentinel.terminate()
            sentinel.wait(timeout=3)


class BuildManifestTests(unittest.TestCase):
    def manifest(self):
        hashes = {'rust/Cargo.toml': 'a' * 64}
        digest = origin.hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        return dict(format_version=1, passed=True, image_id='sha256:' + 'b' * 64,
                    source_sha256=hashes, source_tree_sha256=digest)

    def test_matching_image_and_source_label_are_bound(self):
        manifest = self.manifest()
        self.assertEqual(runner.validate_build_manifest(manifest, manifest['image_id'],
                         manifest['source_tree_sha256']), manifest['source_tree_sha256'])

    def test_changed_image_hash_or_source_label_is_rejected(self):
        manifest = self.manifest()
        for image, label in [('sha256:' + 'c' * 64, manifest['source_tree_sha256']),
                             (manifest['image_id'], 'wrong')]:
            with self.assertRaises(ValueError):
                runner.validate_build_manifest(manifest, image, label)
        manifest['source_sha256']['rust/Cargo.toml'] = 'd' * 64
        with self.assertRaises(ValueError):
            runner.validate_build_manifest(manifest, manifest['image_id'], manifest['source_tree_sha256'])


class MatrixValidationTests(unittest.TestCase):
    def test_required_comparison_cannot_use_one_short_trial(self):
        result = subprocess.run([sys.executable, str(HERE / 'run.py'), '--output', '/tmp/not-used',
                                 '--require-comparison', '--candidate-overlay', '/tmp/not-used.yml',
                                 '--trials', '1', '--measurement-quality', 'load-smoke'],
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('required comparison needs', result.stderr)

    def test_isolated_candidate_requires_bound_build_manifest(self):
        result = subprocess.run([sys.executable, str(HERE / 'run.py'), '--output', '/tmp/not-used',
                                 '--candidate-overlay', '/tmp/not-used.yml',
                                 '--measurement-quality', 'isolated-characterization'],
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('requires --candidate-build-manifest', result.stderr)

    def test_insufficient_fixture_fails_before_starting_docker(self):
        result = subprocess.run([sys.executable, str(HERE / 'run.py'), '--output', '/tmp/not-used',
                                 '--fixture-seconds', '60'], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('insufficient', result.stderr)

    def test_nonfinite_duration_fails_before_starting_docker(self):
        result = subprocess.run([sys.executable, str(HERE / 'run.py'), '--output', '/tmp/not-used',
                                 '--duration', 'nan'], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('invalid timing', result.stderr)


if __name__ == '__main__':
    unittest.main()
