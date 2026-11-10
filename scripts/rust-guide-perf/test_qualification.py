import datetime
import json
import tempfile
import unittest
from pathlib import Path
import plan
import qualification
import test_acceptance


class Qualification(unittest.TestCase):
    def test_authorization_is_separate_fresh_and_binds_policy_manifest_boot(self):
        manifest = dict(image_id='sha256:fixture', passed=True,
                        source_tree_sha256=plan.TREE, source_archive_sha256=plan.ARCHIVE)
        value = dict(scope='guide-source-qualification', authorized=True,
                     candidate_image=manifest['image_id'],
                     candidate_manifest_canonical_sha256=plan.digest(manifest),
                     policy_sha256=plan.digest(plan.POLICY),
                     boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                     expires_utc=(datetime.datetime.now(datetime.timezone.utc)
                                  + datetime.timedelta(minutes=1)).isoformat())
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'quiet.json'
            path.write_text(json.dumps(value))
            self.assertEqual(len(qualification.authorize(path, manifest)), 64)
            for key, changed in [('scope', 'guide-resource-measurement'),
                                 ('boot_id', 'before-reboot'), ('authorized', 1),
                                 ('candidate_image', 'other'), ('policy_sha256', 'other'),
                                 ('expires_utc', '2000-01-01T00:00:00+00:00')]:
                with self.subTest(key=key):
                    path.write_text(json.dumps(dict(value, **{key: changed})))
                    with self.assertRaises(ValueError):
                        qualification.authorize(path, manifest)

    def test_source_only_row_excludes_candidate_and_missing_or_late_evidence(self):
        work = plan.POLICY['workloads'][0]
        phase = test_acceptance.Acceptance().phase()
        phase['samples'] = 15
        counters = dict(backend='source', cpu_percent_mean=100,
                        memory_fraction=.5, throttled_period_fraction=0)
        self.assertTrue(qualification.row(work, 10, phase, counters)['correct_responses'])
        for changed in [{'samples': 0}, {'errors': 1}, {'cache_state_verified': False}]:
            self.assertFalse(qualification.row(work, 10, dict(phase, **changed), counters)['correct_responses'])
        for changed in [{'backend': 'candidate'}, {'candidate_metrics': {}}]:
            with self.assertRaises(ValueError):
                qualification.row(work, 10, phase, dict(counters, **changed))


if __name__ == '__main__':
    unittest.main()
