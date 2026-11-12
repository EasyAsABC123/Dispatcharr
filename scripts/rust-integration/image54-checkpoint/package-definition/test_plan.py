"""Finite local plan corruption/clock controls; no application dispatch."""
import copy,json,math,unittest
from pathlib import Path
from verify_plan import validate,guide_clock,fresh_reference
HERE=Path(__file__).parent
class Plan(unittest.TestCase):
    def test_retained(self):self.assertTrue(validate(deep=False)['passed'])
    def test_corruptions(self):
        mutations={
            'old791':lambda p:p['candidate'].update(runtime_inputs=791),
            'oldimage53':lambda p:p['candidate'].update(image_id='sha256:3f599a036bf0ba15f7d47f4f28016777fba9586cb328afd10a25cac16293f4f0'),
            'changedmap':lambda p:p['candidate'].update(runtime_map_sha256='0'*64),
            'wrongmanifest':lambda p:p['candidate'].update(manifest_sha256='0'*64),
            'wrongarchive':lambda p:p['candidate'].update(archive_sha256='0'*64),
            'dispatch':lambda p:p.update(dispatch_authorized=True),
            'performance':lambda p:p.update(performance_acceptance=True),
            'missingnode':lambda p:p['groups'].pop(),
            'duplicatenode':lambda p:p['groups'].__setitem__(1,copy.deepcopy(p['groups'][0])),
            'changeddependency':lambda p:p['groups'][14].update(requires=[]),
            'node-dispatch':lambda p:p['groups'][0].update(dispatch_authorized=True),
            'resource-action':lambda p:p['groups'][0]['argv'].append('--api-resource-matrix'),
            'oldoutput':lambda p:p['groups'][0].__setitem__('argv',[x.replace('image54-packaged-matrix-2','image54-packaged-matrix-1')for x in p['groups'][0]['argv']]),
            'max4':lambda p:p['scheduling'].update(maximum_ordinary_nodes=4),
            'heavyoverlap':lambda p:p['scheduling'].update(shared_or_retention_only_when_all_other_heavy_idle=False),
            'guideclock':lambda p:p['groups'][7]['pre_dispatch_clock_guard'].update(UTC_minute_range_inclusive=[0,59]),
            'refreshage':lambda p:p['groups'][14]['dispatch_chain'].update(maximum_reference_clock_age_seconds=901),
            'bodyoutcome-schedule':lambda p:p['fixture_coordination_barrier'].update(business_outcome_condition=True),
            'no-clearance':lambda p:p['fixture_coordination_barrier'].update(fresh_Root_finite_clearance_required=False),
            'changed-retention-cap':lambda p:p['groups'][-1]['argv'].__setitem__(p['groups'][-1]['argv'].index('4194304'),'8388608'),
        }
        for name,change in mutations.items():
            with self.subTest(name=name):
                p=json.loads((HERE/'plan.json').read_text());change(p)
                with self.assertRaises(ValueError):validate(p,deep=False)
    def test_clock_edges(self):
        for value in [0,1,35]:self.assertTrue(guide_clock(value))
        for value in [-1,36,59,60,True,0.0,'0',None]:self.assertFalse(guide_clock(value))
    def test_reference_age(self):
        for value in [0,899.5,900]:self.assertTrue(fresh_reference(value))
        for value in [-1,900.001,float('nan'),float('inf'),True,None]:self.assertFalse(fresh_reference(value))
if __name__=='__main__':unittest.main(verbosity=2)
