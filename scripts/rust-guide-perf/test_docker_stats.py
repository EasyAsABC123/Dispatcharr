import copy
import io
import json
import unittest
from unittest.mock import MagicMock, patch
import docker_stats


class DockerStats(unittest.TestCase):
    identifier = 'a'*64
    def value(self):
        return {'id': self.identifier, 'memory_stats': {'usage': 1000, 'limit': 2048,
                    'stats': {'inactive_file': 200}}, 'cpu_stats': {'cpu_usage': {'total_usage': 100}}}

    def test_linux_working_set_formula_and_missing_counters_fail(self):
        value = self.value()
        self.assertEqual(docker_stats.decode(value, self.identifier)['memory_bytes'], 800)
        value['memory_stats']['stats'] = {'total_inactive_file': 300}
        self.assertEqual(docker_stats.decode(value, self.identifier)['memory_bytes'], 700)
        value['memory_stats']['stats']['total_inactive_file'] = 1100
        self.assertEqual(docker_stats.decode(value, self.identifier)['memory_bytes'], 1000)
        for changed in [dict(value, id='other'), dict(value, cpu_stats={'cpu_usage': {}}),
                        dict(value, memory_stats={'usage': 1000, 'limit': 2048, 'stats': {}})]:
            with self.assertRaises(ValueError):
                docker_stats.decode(changed, self.identifier)

    def test_bounded_one_shot_request_and_limit_check(self):
        selected = {'baseline': {'id': self.identifier, 'limits': {'memory_bytes': 2048}}}
        with patch.dict('os.environ', {'DOCKER_HOST': 'unix:///tmp/owned-test.sock'}), \
             patch('docker_stats.UnixConnection') as factory:
            response = factory.return_value.getresponse.return_value
            response.status = 200
            response.read.return_value = json.dumps(self.value()).encode()
            reader = docker_stats.Stats(selected)
            self.assertEqual(reader.read(selected['baseline'])['memory_bytes'], 800)
            self.assertEqual(factory.return_value.request.call_args.args[1],
                             '/containers/'+self.identifier+'/stats?stream=false&one-shot=true')
            self.assertEqual(response.read.call_args.args, (docker_stats.MAX_REPLY+1,))
            response.read.return_value = b'x'*(docker_stats.MAX_REPLY+1)
            with self.assertRaises(ValueError):
                reader.read(selected['baseline'])
            response.read.return_value = json.dumps(self.value()).encode()
            selected['baseline']['limits']['memory_bytes'] = 1
            with self.assertRaises(ValueError):
                reader.read(selected['baseline'])
        with patch.dict('os.environ', {'DOCKER_HOST': 'tcp://production.invalid:2375'}):
            with self.assertRaises(ValueError):
                docker_stats.Stats(selected)

    def test_real_primed_cpu_deltas_missing_or_reset_counters_never_become_zero(self):
        selected = {'baseline': {'id': self.identifier, 'limits': {'memory_bytes': 2048}}}
        reader = docker_stats.Stats(selected)
        with self.assertRaises(ValueError):
            reader.sample()
        value = dict(id=self.identifier, memory_bytes=800, cpu_usage_ns=100, sampled_monotonic=1)
        with patch.object(reader, 'read', return_value=copy.deepcopy(value)):
            reader.prime()
        value.update(cpu_usage_ns=1000000100, sampled_monotonic=2)
        with patch.object(reader, 'read', return_value=copy.deepcopy(value)):
            self.assertEqual(reader.sample()['cpu_percent'], 100)
        value.update(cpu_usage_ns=0, sampled_monotonic=3)
        with patch.object(reader, 'read', return_value=value):
            with self.assertRaises(ValueError):
                reader.sample()


if __name__ == '__main__':
    unittest.main()
