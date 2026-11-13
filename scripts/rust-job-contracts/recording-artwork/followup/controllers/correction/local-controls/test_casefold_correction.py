"""Refuse fake SQL proof and the formerly invalid exact-duplicate setup."""
import copy,json,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
from physical_contract import casefold_setup_proof

def example():
 return {'movie_id':1,'actual_duplicate_id':2,'actual_casefold_SQL':{'sql':'SELECT m.id,m.name FROM vod_movie m WHERE UPPER(m.name::text)=UPPER(%s) ORDER BY m.name LIMIT 2','parameters':['Owned Parented Artwork'],'Native_Store_predicate':'UPPER(m.name::text)=UPPER($1)','rows':[{'id':2,'name':'OWNED PARENTED ARTWORK'},{'id':1,'name':'Owned Parented Artwork'}]}}

class Correction(unittest.TestCase):
 def reject(self,change):
  value=example();change(value)
  with self.assertRaises((ValueError,KeyError,TypeError)):casefold_setup_proof(value)
 def test_valid_distinct_casefold_rows(self):self.assertTrue(casefold_setup_proof(example()))
 def test_old_exact_duplicate_is_rejected(self):self.reject(lambda v:v['actual_casefold_SQL']['rows'][0].update(name='Owned Parented Artwork'))
 def test_unrelated_title_rejected(self):self.reject(lambda v:v['actual_casefold_SQL']['rows'][0].update(name='Other Movie'))
 def test_wrong_predicate_rejected(self):self.reject(lambda v:v['actual_casefold_SQL'].update(sql='SELECT id,name FROM vod_movie LIMIT 2'))
 def test_wrong_parameter_rejected(self):self.reject(lambda v:v['actual_casefold_SQL'].update(parameters=['Other Movie']))
 def test_missing_actual_rows_rejected(self):self.reject(lambda v:v['actual_casefold_SQL'].pop('rows'))
 def test_same_ID_rejected(self):self.reject(lambda v:v['actual_casefold_SQL']['rows'][0].update(id=1))
 def test_bool_ID_rejected(self):self.reject(lambda v:v['actual_casefold_SQL']['rows'][0].update(id=True))
 def test_compiled_matching_predicate_bound(self):
  proof=json.loads((ROOT/'casefold-correction-proof.json').read_text());self.assertIn(proof['Native_matching_predicate'],(ROOT.parents[2]/'historical/15-recording_local_artwork.rs').read_text());self.assertIn("fields=['name', 'year', 'language']",proof['actual_pinned_constraint_declaration'])
 def test_real_guest_uses_case_distinct_name(self):
  text=(ROOT/'tools/guest_refusals.py').read_text();self.assertIn("Movie.objects.create(name='OWNED PARENTED ARTWORK'",text);self.assertIn("Movie.objects.filter(name__iexact='Owned Parented Artwork').count() == 2",text)

if __name__=='__main__':unittest.main()
