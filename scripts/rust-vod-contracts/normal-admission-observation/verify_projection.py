"""Optional exact projection audit against explicitly supplied private originals.
No private-path discovery/fallback, no requests, no secret fields printed.
"""
import argparse,copy,hashlib,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def without_headers(summary):
 result=copy.deepcopy(summary)
 for phase in result['backends'].values():
  for row in phase['probe_outcomes']:row.pop('headers',None)
 return result
def verify(raw_summary,raw_source,raw_native,seed):
 full=json.loads((HERE/'full-retained.json').read_text());projection=json.loads((HERE/'retained-input.json').read_text())
 if sha(raw_summary)!=full['raw_summary_sha256']:raise ValueError('ExactPrivateSummary')
 original=json.loads(raw_summary.read_text())
 if without_headers(original)!=full['summary']:raise ValueError('ExactSummaryProjection')
 for mode,path in [('source',raw_source),('native',raw_native)]:
  if sha(path)!=full['raw_phase_sha256'][mode]or json.loads(path.read_text())!=original['backends'][mode]:raise ValueError('ExactRawPhase '+mode)
 if sha(seed)!=projection['fixture_seed_binding']['private_seed_sha256']:raise ValueError('ExactIndependentSeed')
 data=json.loads(seed.read_text())
 if data['other_movie_uuid']!=projection['expected_content']or 'profile_connections:'+str(data['provider_profile_id'])!=projection['expected_profile']:raise ValueError('ExpectedOtherBinding')
 return {'exact_summary_projection':True,'exact_two_raw_phase_bindings':True,'independent_expected_seed_binding':True,'private_auth_fields_printed':False,'application_executed':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__)
 for name in ['raw-summary','raw-source','raw-native','seed']:p.add_argument('--'+name,type=Path,required=True)
 a=p.parse_args();print(json.dumps(verify(a.raw_summary,a.raw_source,a.raw_native,a.seed),indent=2))
