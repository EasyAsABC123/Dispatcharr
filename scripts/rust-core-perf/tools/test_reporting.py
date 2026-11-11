import json
from pathlib import Path
import tempfile
import unittest
from m1_imports import load
sampling_status=load('reporting').sampling_status


class PartialEvidence(unittest.TestCase):
    def test_failure_partial_samples_are_collected(self):
        with tempfile.TemporaryDirectory()as directory:
            Path(directory,'failed-resources.json').write_text(json.dumps({'samples':[{},{}],'error_type':'ValueError'}))
            value=sampling_status(directory)
            self.assertIs(value['resource_samples_collected'],True)
            self.assertEqual(value['resource_sample_count'],2)

    def test_unreadable_is_unknown_not_false(self):
        with tempfile.TemporaryDirectory()as directory:
            Path(directory,'partial-resources.json').write_text('{')
            value=sampling_status(directory)
            self.assertIsNone(value['resource_samples_collected'])
            self.assertIsNone(value['resource_sample_count'])

    def test_known_samples_plus_unreadable_still_records_known_work(self):
        with tempfile.TemporaryDirectory()as directory:
            Path(directory,'a-resources.json').write_text(json.dumps({'samples':[{}]}))
            Path(directory,'b-resources.json').write_text('{')
            value=sampling_status(directory)
            self.assertIs(value['resource_samples_collected'],True)
            self.assertEqual(value['known_resource_sample_count'],1)
            self.assertIsNone(value['resource_sample_count'])

    def test_no_periodic_samples_does_not_count_lifecycle_as_samples(self):
        with tempfile.TemporaryDirectory()as directory:
            Path(directory,'setup-lifecycle.json').write_text('{}')
            value=sampling_status(directory)
            self.assertIs(value['resource_samples_collected'],False)
            self.assertEqual(value['resource_sample_count'],0)
            self.assertEqual(value['lifecycle_diagnostic_files'],1)


if __name__=='__main__':unittest.main()
