import copy
import unittest
from m0_accounting import Accounting, provider_capacity as capacity

BINDINGS={'baseline':dict(id='fixture-a',started_at='time-a',nano_cpus=0,memory_bytes=0),
          'origin':dict(id='fixture-b',started_at='time-b',nano_cpus=0,memory_bytes=0)}
def ledger(**kw):
    args=dict(scenario='existing-many',source_inputs={'run.json':'known-hash'},container_bindings=copy.deepcopy(BINDINGS),
        contract_ids=['route-c75171aee6eb93b36a3c:METHOD_UNRESOLVED'],observed_method='GET',
        declared_budgets={'absolute_memory':None},max_records=32,max_record_bytes=65536)
    args.update(kw)
    return Accounting(**args)
class Controls(unittest.TestCase):
    def test_finite_absent_unlimited_distinct(self):
        self.assertEqual([capacity(x)['kind'] for x in [None,0,10]],['absent','unlimited','finite'])
    def test_capacity_bool_or_negative_refused(self):
        for x in [True,-1]:
            with self.assertRaises(ValueError):capacity(x)
    def test_contract_method_separate_no_pass_invented(self):
        x=ledger().snapshot();self.assertTrue(x['header']['contract_ids'][0].endswith('METHOD_UNRESOLVED'))
        self.assertEqual(x['header']['observed_method'],'GET');self.assertFalse(x['fully_verified'])
        self.assertIsNone(x['header']['declared_budgets']['absolute_memory']);self.assertFalse(x['resource_acceptance'])
    def test_same_container_required_after_close(self):
        x=ledger();x.begin('active',monotonic=1,bindings=BINDINGS,state={})
        changed=copy.deepcopy(BINDINGS);changed['baseline']['started_at']='restarted'
        with self.assertRaises(ValueError):x.end('active',monotonic=2,bindings=changed,state={},readers_physically_closed=True)
    def test_wrong_epoch_refused(self):
        x=ledger();x.begin('active',monotonic=1,bindings=BINDINGS,state={})
        with self.assertRaises(ValueError):x.observation(phase='active',epoch=0,started_monotonic=1,completed_monotonic=2,bindings=BINDINGS,observer_record={},census_complete=True)
    def test_incomplete_record_remains_explicit(self):
        x=ledger();x.begin('active',monotonic=1,bindings=BINDINGS,state={})
        x.observation(phase='active',epoch=1,started_monotonic=1,completed_monotonic=2,bindings=BINDINGS,observer_record={'denied':1},census_complete=False)
        self.assertFalse(x.snapshot()['records'][1]['census_complete'])
    def test_no_implicit_reader_close(self):
        x=ledger();x.begin('active',monotonic=1,bindings=BINDINGS,state={});x.end('active',monotonic=2,bindings=BINDINGS,state={},readers_physically_closed=False)
        self.assertFalse(x.snapshot()['records'][-1]['readers_physically_closed'])
    def test_nested_phase_and_backwards_time_refused(self):
        x=ledger();x.begin('active',monotonic=2,bindings=BINDINGS,state={})
        with self.assertRaises(ValueError):x.begin('after',monotonic=3,bindings=BINDINGS,state={})
        with self.assertRaises(ValueError):x.end('active',monotonic=1,bindings=BINDINGS,state={},readers_physically_closed=True)
    def test_record_guards_explicit_not_source_budgets(self):
        x=ledger(max_records=1);x.begin('active',monotonic=1,bindings=BINDINGS,state={})
        with self.assertRaises(ValueError):x.end('active',monotonic=2,bindings=BINDINGS,state={},readers_physically_closed=True)
    def test_caller_mutation_cannot_change_bindings_or_budget(self):
        budget={'absolute_memory':None};x=ledger(declared_budgets=budget);budget['absolute_memory']=1
        self.assertIsNone(x.snapshot()['header']['declared_budgets']['absolute_memory'])
if __name__=='__main__':unittest.main()
