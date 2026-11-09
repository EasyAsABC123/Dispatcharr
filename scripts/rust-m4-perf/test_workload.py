"""Fault injection for M4 measurement acceptance, not production implementation tests."""
import copy
import datetime
import json
from pathlib import Path
import tempfile
import unittest
from plan import PLAN, authorize_quiet, digest, freeze, validate_plan
from workload import compare_pair, validate_phase


class ExperimentFaultTests(unittest.TestCase):
    def phase(self):
        return {'viewers': [{'measured_bytes': 500000 * 60, 'status': 200,
                            'errors': [], 'unexpected_eof': False,
                            'prefix_matches': True, 'first_byte_ms': 100}],
                'requests': 1, 'continuous_connections': True, 'measurement_seconds': 60,
                'total_measured_bytes': 500000 * 60, 'median_first_byte_ms': 100,
                'correctness_passed': True,
                'samples': [{'cpu_percent': 10, 'memory_bytes': 1000000,
                             'containers': [{'id': name} for name in ('db', 'origin', 'web')]}
                            for _ in range(30)]}

    def validate(self, phase):
        return validate_phase(phase, 1, 500000, PLAN['budgets'], ['db', 'origin', 'web'])

    def test_corrupt_payload_fails_even_when_resource_ratios_pass(self):
        phase = self.phase()
        phase['viewers'][0]['prefix_matches'] = False
        self.assertIn('viewer_0_delivery', self.validate(phase))
        phase['correctness_passed'] = False
        self.assertFalse(compare_pair(self.phase(), phase, PLAN['budgets'])['passed'])

    def test_short_fixture_eof_and_reconnect_each_fail(self):
        for key, value in [('unexpected_eof', True), ('errors', ['TimeoutError'])]:
            phase = self.phase()
            phase['viewers'][0][key] = value
            self.assertIn('viewer_0_delivery', self.validate(phase))
        phase = self.phase()
        phase['requests'] = 2
        self.assertIn('connection_count_or_reconnect', self.validate(phase))

    def test_omitted_pg_or_origin_cannot_improve_memory(self):
        phase = self.phase()
        phase['samples'][7]['containers'].pop(0)
        self.assertIn('incomplete_service_footprint', self.validate(phase))

    def test_low_delivery_cannot_pass_by_reducing_cpu(self):
        phase = self.phase()
        phase['viewers'][0]['measured_bytes'] //= 2
        self.assertIn('viewer_0_rate', self.validate(phase))

    def test_single_trial_and_posthoc_budget_tampering_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            record = freeze(Path(temporary) / 'plan.json')
            self.assertEqual(validate_plan(record), PLAN)
            changed = copy.deepcopy(record)
            changed['plan']['trials'] = 1
            changed['plan_sha256'] = digest(changed['plan'])
            with self.assertRaises(ValueError):
                validate_plan(changed)
            changed = copy.deepcopy(record)
            changed['plan']['budgets']['max_memory_ratio'] = 1.2
            changed['plan_sha256'] = digest(changed['plan'])
            with self.assertRaises(ValueError):
                validate_plan(changed)

    def test_explicit_quiet_authorization_required_for_exact_plan_and_image(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'quiet.json'
            record = {'scope': 'm4-resource-measurement', 'authorized': True,
                      'plan_sha256': digest(PLAN), 'image_id': 'fixture-image',
                      'expires_utc': (datetime.datetime.now(datetime.timezone.utc) +
                                      datetime.timedelta(minutes=1)).isoformat()}
            path.write_text(json.dumps(record))
            self.assertTrue(authorize_quiet(path, digest(PLAN), 'fixture-image'))
            for key, value in [('image_id', 'other-image'), ('authorized', False),
                               ('expires_utc', '2020-01-01T00:00:00+00:00')]:
                changed = dict(record)
                changed[key] = value
                path.write_text(json.dumps(changed))
                with self.assertRaises(ValueError):
                    authorize_quiet(path, digest(PLAN), 'fixture-image')

    def test_invalid_resource_samples_fail_closed(self):
        for invalid in (float('nan'), float('inf'), -1):
            phase = self.phase()
            phase['samples'][0]['cpu_percent'] = invalid
            self.assertIn('invalid_resource_sample', self.validate(phase))
            self.assertFalse(compare_pair(self.phase(), phase, PLAN['budgets'])['passed'])

    def test_memory_failure_stays_visible(self):
        source, candidate = self.phase(), self.phase()
        for row in candidate['samples']:
            row['cpu_percent'] = 1
        result = compare_pair(source, candidate, PLAN['budgets'])
        self.assertFalse(result['passed'])
        self.assertEqual(result['ratios']['memory_ratio'], 1)


if __name__ == '__main__':
    unittest.main()


class RealContinuousTransportTests(unittest.TestCase):
    def test_one_real_http_request_survives_warmup_and_measurement(self):
        import hashlib
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        import threading
        import time
        from workload import run_continuous
        chunk = bytes(range(256)) * 4
        requests = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass
            def do_GET(self):
                requests.append(self.path)
                self.send_response(200)
                self.send_header('Content-Length', str(len(chunk) * 1000))
                self.end_headers()
                try:
                    for _ in range(1000):
                        self.wfile.write(chunk)
                        self.wfile.flush()
                        time.sleep(.005)
                except (BrokenPipeError, ConnectionResetError):
                    pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            prefix = hashlib.sha256(chunk).hexdigest()
            endpoint = (f'http://127.0.0.1:{server.server_port}/fixture', {}, prefix)
            sample = lambda: {'containers': [{'id': 'test'}], 'cpu_percent': 0, 'memory_bytes': 0}
            result = run_continuous([endpoint], .05, .15, sample, len(chunk), timeout=1, sample_period=.02)
            self.assertEqual(requests, ['/fixture'])
            self.assertEqual(result['requests'], 1)
            self.assertTrue(result['continuous_connections'])
            self.assertTrue(result['viewers'][0]['prefix_matches'])
            self.assertGreater(result['viewers'][0]['warmup_bytes'], 0)
            self.assertGreater(result['viewers'][0]['measured_bytes'], 0)
            self.assertFalse(result['viewers'][0]['unexpected_eof'])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)


class WholeExperimentTests(unittest.TestCase):
    def test_missing_or_duplicated_trial_cannot_pass(self):
        from plan import final_acceptance
        rows = [{'protocol': work['protocol'], 'viewers': work['viewers'], 'trial': trial, 'passed': True}
                for work in PLAN['workloads'] for trial in range(1, 4)]
        self.assertTrue(final_acceptance(rows, PLAN))
        self.assertFalse(final_acceptance(rows[:-1], PLAN))
        self.assertFalse(final_acceptance(rows[:-1] + [rows[0]], PLAN))
        rows[-1]['passed'] = False
        self.assertFalse(final_acceptance(rows, PLAN))

    def test_unequal_actual_backend_cgroup_cannot_pass(self):
        from plan import validate_limits
        limits = {name: copy.deepcopy(PLAN['service_limits'][name if name in ('origin', 'infrastructure') else 'backend'])
                  for name in ('infrastructure', 'origin', 'source', 'candidate')}
        validate_limits(limits, PLAN)
        limits['candidate']['memory_bytes'] *= 2
        with self.assertRaises(ValueError):
            validate_limits(limits, PLAN)


class ObserverFaultTests(unittest.TestCase):
    def fake(self, event):
        import time
        class Client:
            def __init__(self, *args, **kwargs):
                self.status = 101
                self.count = 0
            def receive(self, **kwargs):
                return {'message': {'type': 'connection_established'}}
            def optional(self, *args):
                self.count += 1
                if self.count == 1:
                    return {'message': event}
                time.sleep(.005)
                return None
            def close(self):
                pass
        return Client

    def test_actual_message_wrapper_and_source_success_semantics(self):
        from observer import Observer
        import time
        event = {'type': 'update', 'data': {'type': 'vod_stats', 'stats': json.dumps({'total_connections': 1})}}
        observer = Observer('http://127.0.0.1:1234', 'private-fixture-token', self.fake(event))
        time.sleep(.02)
        result = observer.close()
        self.assertEqual(result['kinds'], {'vod_stats': 1})
        self.assertEqual(result['last_connection_counts'], {'vod_stats': 1})
        self.assertEqual(result['errors'], [])
        self.assertNotIn('private-fixture-token', json.dumps(result))
        event['data']['success'] = True
        observer = Observer('http://127.0.0.1:1234', 'private-fixture-token', self.fake(event))
        time.sleep(.02)
        self.assertEqual(observer.close()['errors'], ['ValueError'])
