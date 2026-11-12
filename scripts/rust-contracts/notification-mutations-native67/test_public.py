"""Finite offline corruption controls; no application, Cargo, SQL or Docker work."""
import copy,json,unittest,tempfile
from pathlib import Path
import verify as V
import reconstruct as R
HERE=Path(__file__).resolve().parent
class Controls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        p=HERE/'observations';cls.values=[V.compressed(p/'mutations-projection.json.gz'),V.compressed(p/'legacy-projection.json.gz'),V.compressed(p/'Source48-projection.json.gz'),V.read(p/'execution-projection.json'),V.read(p/'mutations-cache-after.json'),V.read(p/'legacy-final-cache.json'),V.read(p/'command-timeline.json')]
    def reject(self,fn):
        values=copy.deepcopy(self.values);fn(*values)
        with self.assertRaises((ValueError,KeyError,TypeError,IndexError)):V.observations(*values)
    def test_all67_and_negative_controller_derive(self):
        r=V.verify();self.assertTrue(r['observation_qualified']);self.assertEqual(r['controller_actual_exit'],1);self.assertFalse(r['overall_fixture_passed'])
    def test_controller_pass_not_relabelled(self):self.reject(lambda m,l,s,e,a,b,t:e.update(passed=True))
    def test_controller_zero_not_relabelled(self):self.reject(lambda m,l,s,e,a,b,t:e.update(controller_exit=0))
    def test_Rust_exit_nonzero(self):self.reject(lambda m,l,s,e,a,b,t:e['native_processes']['mutations'].update(exit=1))
    def test_missing_primary_case(self):self.reject(lambda m,l,s,e,a,b,t:m['cases'].pop())
    def test_missing_order_control(self):self.reject(lambda m,l,s,e,a,b,t:m['extra_cases'].pop())
    def test_missing_legacy_case(self):self.reject(lambda m,l,s,e,a,b,t:l['cases'].pop())
    def test_missing_U_named_cache(self):self.reject(lambda m,l,s,e,a,b,t:m['cases'][8]['after_cache']['named']['entries'].pop('U'))
    def test_missing_H_named_cache(self):self.reject(lambda m,l,s,e,a,b,t:m['cases'][0]['before_cache']['named']['entries'].pop('H'))
    def test_V_value_changed(self):self.reject(lambda m,l,s,e,a,b,t:m['cases'][44]['after_cache']['named']['values'].update(V=None))
    def test_final_named_cache_mandatory(self):self.reject(lambda m,l,s,e,a,b,t:m['final_cache']['named'].update(entries={}))
    def test_wrong_sentinel(self):self.reject(lambda m,l,s,e,a,b,t:m['final_cache']['named']['sentinel'].update(raw_hex=b'wrong'.hex()))
    def test_omitted_primary_epoch_completion(self):self.reject(lambda m,l,s,e,a,b,t:a.update(passed=False))
    def test_removed_second_epoch_transition(self):self.reject(lambda m,l,s,e,a,b,t:next(r for r in t if r.get('epoch_transition_SET_owner')==e['databases']['legacy']).pop('epoch_transition_SET_owner'))
    def test_late_refusal_cannot_be_passed(self):self.reject(lambda m,l,s,e,a,b,t:next(r for r in t if r['command_file']=='command-025.json').update(exit=0))
    def test_late_refusal_cannot_be_retargeted(self):self.reject(lambda m,l,s,e,a,b,t:next(r for r in t if r['command_file']=='command-025.json')['collection'].update(database=e['databases']['legacy']))
    def test_drop_command_receipt(self):self.reject(lambda m,l,s,e,a,b,t:t.pop())
    def test_generated_clock_mapping_rewind(self):self.reject(lambda m,l,s,e,a,b,t:m['clock_mapping'][0].update(Native='2000-01-01T00:00:00Z'))
    def test_mutation_sequence_consumption(self):self.reject(lambda m,l,s,e,a,b,t:m['cases'][0]['after']['sequences']['core_systemnotification'].update(last_value=999))
    def test_credential_projection_field_cannot_be_dropped(self):self.reject(lambda m,l,s,e,a,b,t:m['cases'][0]['before']['tables']['accounts_user'][0].pop('api_key'))
    def test_credential_plain_value_not_allowed(self):self.reject(lambda m,l,s,e,a,b,t:m['cases'][0]['before']['tables']['accounts_user'][0].update(api_key='plain'))
    def test_raw_body_hash(self):self.reject(lambda m,l,s,e,a,b,t:m['cases'][0].update(body_sha256='0'*64))
    def test_source500_renderer_gap_remains(self):
        r=V.observations(*copy.deepcopy(self.values));self.assertFalse(r['renderer500_forms_physical_SQLcancel_Main_Source_credentials_graph_performance_qualified'])
    def test_missing_default_sequences_both_sides(self):
        p=HERE/'observations';e=V.read(p/'execution-projection.json');a=V.read(p/'default-before.json');b=V.read(p/'default-after.json');a['value']['sequences']={};b['value']['sequences']={}
        with self.assertRaises(ValueError):V.cleanup(e,a,b,V.read(p/'precleanup-PIDs.json'),V.read(p/'cleanup-PID-absence.json'),V.read(p/'Root-retained-observation-review.json'),V.read(p/'container-identity-projection.json'))
    def test_missing_PID_coverage(self):
        p=HERE/'observations'
        with self.assertRaises(ValueError):V.cleanup(V.read(p/'execution-projection.json'),V.read(p/'default-before.json'),V.read(p/'default-after.json'),[],[],V.read(p/'Root-retained-observation-review.json'),V.read(p/'container-identity-projection.json'))
    def test_compile_wait_required(self):
        b=V.read(HERE/'build/build.json');b['commands'][0]['wait_completed']=False
        with self.assertRaises(ValueError):V.builds(b,V.read(HERE/'source/executable-bindings.json'),V.read(HERE/'workspace/Root-independent-review.json'),V.sha(HERE/'source/full797-map.json'))
    def test_workspace_must_not_promote_controller(self):
        w=V.read(HERE/'workspace/Root-independent-review.json');w['Native67_controller_actual_exit_remains']=0
        with self.assertRaises(ValueError):V.builds(V.read(HERE/'build/build.json'),V.read(HERE/'source/executable-bindings.json'),w,V.sha(HERE/'source/full797-map.json'))
    def test_reconstruction_existing_output_rejected(self):
        with tempfile.TemporaryDirectory()as d:
            with self.assertRaises(ValueError):R.reconstruct(Path('.'),Path(d))
if __name__=='__main__':unittest.main()
