import socket
import tempfile
from pathlib import Path
import unittest
from fixture_ports import write_port_overlay


class FixturePortTests(unittest.TestCase):
    def test_distinct_released_ports_and_override_cover_multiple_service_ports(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'ports.yml'
            result=write_port_overlay(path,{'baseline':[5656,5432,6379],'origin':[8080],'candidate':[9192]})
            ports=[row['host'] for rows in result['services'].values() for row in rows]
            self.assertEqual(len(ports),len(set(ports)))
            self.assertEqual(path.read_text().count('ports: !override'),3)
            for port in ports:
                self.assertTrue(61000<=port<=65000)
                if result['kernel_ephemeral_range']:
                    self.assertFalse(result['kernel_ephemeral_range'][0]<=port<=result['kernel_ephemeral_range'][1])
                with socket.socket() as probe:probe.bind(('127.0.0.1',port))

    def test_invalid_and_unbounded_mappings_are_rejected(self):
        for mapping in ({},{'bad:name':[5656]},{'baseline':[0]},{'baseline':[5656,5656]},{str(i):[1] for i in range(17)}):
            with self.assertRaises(ValueError):write_port_overlay(Path('/tmp/never-created-fixture-port-overlay'),mapping)


if __name__=='__main__':unittest.main()
