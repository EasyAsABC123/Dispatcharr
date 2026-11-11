"""Portable public attestation validator; not a private-raw replay."""
import hashlib,json
from pathlib import Path

def verify(root):
    d=json.loads((root/'scripts/rust-playback-baseline/verified-source-accounting-1.json').read_text())
    assert d['offline_controls']==27 and d['fully_verified'] is False
    assert d['resource_acceptance'] is False and d['Source_budget_acceptance'] is None
    assert len(d['functional_cases'])==3
    for x in d['runtime_files']:
        assert hashlib.sha256((root/x['path']).read_bytes()).hexdigest()==x['sha256'],x['path']
    for c in d['functional_cases']:
        assert c['passed'] is True
        assert c['states'][-1]['clients']==c['states'][-1]['origin_active']==c['states'][-1]['provider_slots']==0
        assert c['resource_errors']==[]
    m=d['many'];assert m['summary']['requests']==10 and m['summary']['errors']==m['summary']['empty_responses']==0
    assert m['existing_active_resource_sample_count']>=3 and not m['independent_cleanup']['containers']
    assert m['states'][-1]['clients']==m['states'][-1]['origin_active']==0
    assert d['limits']['absolute_CPU_memory_growth_ceilings'] is None
    return d
if __name__=='__main__':
    verify(Path(__file__).resolve().parents[2]);print('Public attestation/runtime hashes PASS; private raw replay not performed.')
