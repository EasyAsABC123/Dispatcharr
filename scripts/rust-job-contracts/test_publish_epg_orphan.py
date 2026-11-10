"""Tests for bounded public projection of actual orphan fixture evidence."""
import importlib.util
import unittest
from pathlib import Path
spec = importlib.util.spec_from_file_location('publish_orphan', Path(__file__).with_name('publish_epg_orphan.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class Projection(unittest.TestCase):
    def case(self):
        return dict(name='none', source_id=9, before={}, after={}, marker_attempts={},
                    envelope={'password':'must not publish'}, helper_return=False,
                    sql=[{'sql':'SELECT 1','time':'0.001'}],
                    metadata={'status':'SUCCESS','result':'EPG source 9 does not exist, task cleaned up'},
                    result_ttl=3598,raw_metadata_sha256='a'*64,postrun={'sql':[]})

    def test_worker_omits_private_envelope(self):
        result = module.project_case(self.case(), True)
        self.assertNotIn('envelope', result)
        self.assertEqual(result['postrun_sql_count'], 0)

    def test_worker_rejects_unexpected_result(self):
        value = self.case()
        value['metadata']['result'] = 'unrelated result'
        with self.assertRaises(ValueError): module.project_case(value, True)

    def test_sql_projection_omits_statement_and_timing(self):
        result = module.project_case(self.case())
        self.assertEqual(result['sql'][0]['verb'], 'SELECT')
        self.assertEqual(len(result['sql'][0]['sha256']), 64)
        self.assertNotIn('sql', result['sql'][0])

if __name__ == '__main__': unittest.main()
