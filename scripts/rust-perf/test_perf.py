import http.server
import threading
import time
import unittest
from unittest.mock import patch

import perf


def report():
    return {'workload': dict(viewers=10, url_count=1, scenario='same-channel', warmup=15,
                             duration=60, slow_delay=0, timeout=3, fixture_id='fixture-hash-copy',
                             sample_interval=1),
            'environment': {'host': 'same-machine'},
            'measurement': {'summary': dict(errors=0, empty_responses=0, viewers_without_data=0,
                                            cpu_percent_mean=40, memory_bytes_peak=1000,
                                            first_byte_ms_median=20, bytes_per_second=100),
                            'sample_errors': [], 'resource_samples': [{}, {}, {}]}}


class PerformanceTests(unittest.TestCase):
    gates = {'cpu_percent_mean': .8, 'memory_bytes_peak': .8, 'first_byte_ms_median': 1.1}

    def test_warmup_ramp_keeps_first_viewer_and_one_shared_deadline(self):
        began = time.monotonic()
        calls = []

        def viewer(url, deadline, *_):
            calls.append((url, time.monotonic() - began, deadline))
            time.sleep(max(0, deadline - time.monotonic()))
            return dict(bytes=1, requests=1, errors=0, empty_responses=0, first_byte_ms=[1])

        with patch.object(perf, 'viewer', side_effect=viewer):
            result = perf.phase(['first', 'second', 'third'], 3, .15, .1, 0, warmup_ramp=.05)
        by_url = {row[0]: row for row in calls}
        self.assertGreaterEqual(by_url['second'][1] - by_url['first'][1], .04)
        self.assertGreaterEqual(by_url['third'][1] - by_url['first'][1], .04)
        self.assertEqual(len({row[2] for row in calls}), 1)
        self.assertEqual(result['summary']['viewers_without_data'], 0)

    def test_different_warmup_ramp_is_not_equivalent_work(self):
        baseline, candidate = report(), report()
        candidate['workload']['warmup_ramp'] = 4
        comparison = perf.compare(baseline, candidate, self.gates)
        self.assertIn('Workload mismatch: warmup_ramp', comparison['failures'])

    def test_binary_and_decimal_memory_units(self):
        self.assertEqual(perf.memory_bytes('1.5MiB / 4GiB'), 1.5 * 1024**2)
        self.assertEqual(perf.memory_bytes('1.5MB / 4GB'), 1500000)
        self.assertEqual(perf.memory_bytes('0B / 4GiB'), 0)
        with self.assertRaises(ValueError):
            perf.memory_bytes('unknown')

    def test_resources_include_all_services(self):
        raw = '\n'.join(['{"ID":"a","CPUPerc":"120%","MemUsage":"1MiB / 2GiB"}',
                         '{"ID":"b","CPUPerc":"20%","MemUsage":"2MiB / 2GiB"}'])
        with patch.object(perf, 'command', return_value=raw):
            result = perf.resources(['a', 'b'])
            self.assertEqual(result['cpu_percent'], 140)
            self.assertEqual(result['memory_bytes'], 3 * 1024**2)
        with patch.object(perf, 'command', return_value=raw.splitlines()[0]):
            with self.assertRaises(RuntimeError):
                perf.resources(['a', 'b'])

    def candidate(self):
        result = report()
        result['measurement']['summary'].update(cpu_percent_mean=30, memory_bytes_peak=700)
        return result

    def test_measured_improvement_passes(self):
        self.assertTrue(perf.compare(report(), self.candidate(), self.gates)['passed'])

    def test_dropped_output_does_not_look_like_improvement(self):
        candidate = self.candidate()
        candidate['measurement']['summary']['bytes_per_second'] = 50
        self.assertFalse(perf.compare(report(), candidate, self.gates)['passed'])

    def test_negative_and_nonfinite_metrics_or_gates_fail(self):
        for value in (-1, float('nan'), float('inf')):
            candidate = self.candidate()
            candidate['measurement']['summary']['cpu_percent_mean'] = value
            self.assertFalse(perf.compare(report(), candidate, self.gates)['passed'])
        for value in (-1, float('nan'), float('inf')):
            self.assertFalse(perf.compare(report(), self.candidate(),
                                          {'cpu_percent_mean': value})['passed'])

    def test_failures_and_incompatible_workload_reject_comparison(self):
        changes = [('summary', 'errors', 1), ('summary', 'viewers_without_data', 1),
                   ('summary', 'empty_responses', 1), ('summary', 'cpu_percent_mean', None)]
        for section, key, value in changes:
            candidate = self.candidate()
            candidate['measurement'][section][key] = value
            self.assertFalse(perf.compare(report(), candidate, self.gates)['passed'])
        candidate = self.candidate()
        candidate['workload']['viewers'] = 1
        self.assertFalse(perf.compare(report(), candidate, self.gates)['passed'])
        candidate = self.candidate()
        candidate['measurement']['resource_samples'] = []
        self.assertFalse(perf.compare(report(), candidate, self.gates)['passed'])

    def test_real_http_phase_drives_independent_viewers(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                time.sleep(.01)
                self.send_response(200)
                self.send_header('Content-Length', '8')
                self.end_headers()
                try:
                    self.wfile.write(b'fixture!')
                except (BrokenPipeError, ConnectionResetError):
                    pass  # Deadline cancellation can close an in-flight response.

            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            url = f'http://127.0.0.1:{server.server_port}/stream'
            outcome = perf.phase([url], 2, .1, 1, 0)
            self.assertEqual(outcome['summary']['errors'], 0)
            self.assertEqual(outcome['summary']['viewers_without_data'], 0)
            self.assertGreater(outcome['summary']['bytes'], 0)
            self.assertEqual(len(outcome['viewers']), 2)
        finally:
            server.shutdown()
            thread.join()
            server.server_close()


if __name__ == '__main__':
    unittest.main()
