"""Draft offline negative controls. NOT RUN during the quiet window."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('m0_idle_draft', HERE/'run.py')
controller = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controller)
POLICY = json.loads((HERE/'policy.json').read_text())


def samples():
    result = []
    for label in ['idle-before-first-playback', 'idle-after-playback', 'idle-after-churn']:
        for _ in range(5):
            result.append(dict(phase=label, phase_stable=True, cpu_percent=1, memory_bytes=100*1024**2,
                containers=[dict(memory_usage_bytes=100*1024**2, memory_limit_bytes=1024**3)],
                census=dict(source=dict(processes=3, threads=3, fds=20,
                    socket_descriptors=4, tcp_states={'06': 0}))))
    return result


class Controls(unittest.TestCase):
    def passed(self, rows):
        return all(row['passed'] for row in controller.evaluate_samples(rows, POLICY))

    def test_census_descriptor_race_is_not_complete(self):
        rows = samples()
        rows[0]['census']['source']['raced_descriptors'] = 1
        self.assertFalse(self.passed(rows))

    def test_phase_epoch_detects_return_to_same_label(self):
        sampler = controller.Sampler(None, None, None, POLICY, Path('/unused'))
        sampler.label = 'idle'
        epoch = sampler.phase_epoch
        sampler.label = 'other'
        sampler.label = 'idle'
        self.assertGreater(sampler.phase_epoch, epoch)

    def test_resource_policy_unchanged_from_draft2(self):
        import hashlib
        self.assertEqual(hashlib.sha256((HERE/'policy.json').read_bytes()).hexdigest(), '6984f5e73d31048f47ebac884c3936112e26b262da842e637645d363d99fbeab')

    def test_cross_uid_census_merge_counts_once(self):
        part = dict(processes=1, threads=2, fds=3, socket_descriptors=1, raced_processes=0, raced_descriptors=0, process_ids=[10], socket_inodes=['socket:[1]'])
        inventory = dict(process_ids=[10], raced_processes=0, tcp_states={'06': 2})
        value = controller.merge_census(inventory, [part])
        self.assertEqual(value['processes'], 1)
        self.assertEqual(value['raced_processes'], 0)
        self.assertEqual(value['tcp_states'], {'06': 2})
        inventory['process_ids'].append(11)
        self.assertEqual(controller.merge_census(inventory, [part])['raced_processes'], 2)
        with self.assertRaises(ValueError):
            controller.merge_census(inventory, [part, part])

    def test_incomplete_rows_excluded_with_enough_complete_samples(self):
        rows = samples()
        extra = copy.deepcopy(rows[0])
        extra['census']['source'].update(raced_processes=1, complete=False)
        rows.append(extra)
        self.assertTrue(self.passed(rows))
        self.assertFalse(controller.complete_census(extra))

    def test_fixed_three_repetitions(self):
        self.assertEqual(POLICY['repetitions'], 3)
        self.assertEqual(POLICY['budgets']['maximum_errors'], 0)
        self.assertEqual(POLICY['scope'].split(';')[0], 'Original-only disposable fixture qualification')

    def test_valid_synthetic_observer(self):
        self.assertTrue(self.passed(samples()))

    def test_unstable_transition_not_idle_acceptance(self):
        rows = samples()
        rows[0]['phase_stable'] = False
        self.assertFalse(self.passed(rows))

    def test_absent_samples(self):
        self.assertFalse(self.passed([]))

    def test_undersampled_idle(self):
        self.assertFalse(self.passed(samples()[1:]))

    def test_idle_cpu_ceiling(self):
        rows = samples()
        for row in rows:
            row['cpu_percent'] = 51
        self.assertFalse(self.passed(rows))

    def test_whole_stack_cpu_ceiling(self):
        rows = samples()
        rows.append(dict(rows[0], phase='churn-ten', cpu_percent=501))
        self.assertFalse(self.passed(rows))

    def test_memory_growth(self):
        rows = samples()
        for row in rows[5:]:
            row['memory_bytes'] += 65*1024**2
        self.assertFalse(self.passed(rows))

    def test_process_thread_fd_socket_growth(self):
        for field, increment in [('processes', 1), ('threads', 5), ('fds', 17), ('socket_descriptors', 17)]:
            with self.subTest(field=field):
                rows = samples()
                rows[-1]['census']['source'][field] += increment
                self.assertFalse(self.passed(rows))

    def test_time_wait_ceiling(self):
        rows = samples()
        rows[-1]['census']['source']['tcp_states']['06'] = 1025
        self.assertFalse(self.passed(rows))

    def test_memory_hard_limit(self):
        rows = samples()
        rows[-1]['containers'][0]['memory_usage_bytes'] = 1024**3+1
        self.assertFalse(self.passed(rows))

    def test_no_mutating_policy(self):
        before = copy.deepcopy(POLICY)
        controller.evaluate_samples(samples(), POLICY)
        self.assertEqual(POLICY, before)

    def test_detached_http_socket_is_shutdown_before_reader_join(self):
        closed = threading.Event()

        class Physical:
            def shutdown(self, _how):
                closed.set()

        class Response:
            status = 200
            first = True

            def read1(self, _size):
                if self.first:
                    self.first = False
                    return b'fixture-media'
                if not closed.wait(2):
                    raise TimeoutError('missing physical shutdown')
                raise OSError('planned socket shutdown')

        class Connection:
            sock = None

            def __init__(self, *_args, **_kwargs):
                pass

            def connect(self):
                self.sock = Physical()

            def request(self, *_args, **_kwargs):
                pass

            def getresponse(self):
                self.sock = None
                return Response()

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(controller.http.client, 'HTTPConnection', Connection):
                readers = controller.Readers('http://127.0.0.1:1', '/owned-fixture',
                    'not-a-real-token', 1, Path(temporary)/'round', POLICY['budgets'])
                rows = readers.run(0)
                self.assertTrue(closed.is_set())
                self.assertEqual(rows[0]['errors'], [])
                self.assertFalse(readers.threads[0].is_alive())
                self.assertEqual(rows[0]['requests'], 1)


if __name__ == '__main__':
    unittest.main()
