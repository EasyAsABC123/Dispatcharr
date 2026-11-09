import json
from pathlib import Path
import tempfile
import unittest
from summarize_native_perf import summarize, container_metrics


def report(cpu,memory,error=0):
    return {'services':['baseline'],'measurement':{'summary':{
        'errors':error,'empty_responses':0,'viewers_without_data':0,'cpu_percent_mean':cpu,
        'memory_bytes_peak':memory,'bytes_per_second':100,'first_byte_ms_p95':10},
        'sample_errors':[],'resource_samples':[{'containers':[{'id':'abc','cpu_percent':cpu,'memory_bytes':memory}],
        'process_rss_diagnostic':{'observer_seconds':.01,'containers':[{'id':'abcdef','process_rss_bytes_sum_diagnostic':200,'processes':[{'name':'postgres','rss_bytes':200}]}]}}]*3}}


class SummaryTests(unittest.TestCase):
    def test_paired_medians_and_primary_memory_stay_separate_from_diagnostic_rss(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);run={'configuration':{'viewers':[1],'trials':3},'matrix_completed':True,
                'candidate_build_manifest_sha256':'fixture','candidate_image_source_bound':True}
            for trial in range(1,4):
                for backend,cpu,memory in [('baseline',10,100),('candidate',5,50)]:
                    (path/f'{backend}-same-channel-viewers-1-trial-{trial}.json').write_text(json.dumps(report(cpu,memory)))
            file=path/'run.json';file.write_text(json.dumps(run));result=summarize(file)
            self.assertTrue(result['measurement_quality_passed']);self.assertFalse(result['application_performance_verified'])
            self.assertEqual(result['counts'][0]['paired_ratio_medians']['memory_bytes_peak'],.5)
            self.assertEqual(result['counts'][0]['paired_ratio_medians']['cpu_percent_mean'],.5)
            (path/'candidate-same-channel-viewers-1-trial-2.json').write_text(json.dumps(report(5,50,error=1)))
            self.assertFalse(summarize(file)['measurement_quality_passed'])

    def test_missing_service_mapping_is_rejected(self):
        invalid=report(1,1)
        for sample in invalid['measurement']['resource_samples']:sample.pop('process_rss_diagnostic',None)
        with self.assertRaises(ValueError):container_metrics(invalid)


if __name__=='__main__':unittest.main()
