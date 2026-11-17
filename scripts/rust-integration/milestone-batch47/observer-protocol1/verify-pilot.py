"""Verify published preparation/pilot records only; no Docker/apps/media execution.
Original Dispatcharr authors retain all application and algorithm credit.
"""
import hashlib,json
from pathlib import Path
P=Path(__file__).resolve().parent
load=lambda name:json.loads((P/name).read_text())
sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
for row in load('index.json')['files']:
    path=P/row['path'];assert path.stat().st_size==row['bytes'];assert sha(path)==row['sha256'],row['path']
for row in load('provenance.json')['items']:assert sha(P/row['path'])==row['sha256'],row['path']
q=load('pilot/result.json');assert q['protocol_qualification_only']and q['all_phases_valid']
assert q['actual_new_phase_count']==2 and q['retained_phase_count']==0 and q['changed_inputs']==[]
assert q['cross_phase_useful_duration_equivalence']==[True,True]
assert [row['backend']for row in q['phases']]==['baseline','candidate']
assert all(row['actual_exit']==0 and row['valid']for row in q['phases'])
assert q['gain_accepted']is False and q['measured_pair_comparison_eligible']is False
config=load('pilot/config-review.json');assert config['passed']and config['qualification_sha256']==sha(P/'pilot/result.json')
assert config['candidate_image_id']==q['candidate_image_id']
assert config['Main_shutdown_seconds']=={'functional_explicit':45,'resource_environment_override_present':False,'resource_compiled_default':12}
assert config['proxy_settings_channel_shutdown_delay']=={'functional':5,'resource':2}
assert config['Tokio_threads']=={'functional':2,'resource':1}
assert config['fixture_seconds']=={'functional_declared':1800,'resource_actual':300}
assert load('pilot/config-execution.json')['actual_exit']==0
for backend in ('baseline','candidate'):
    r=load('pilot/'+backend+'-observer-receipt.json');i=r['identity']
    assert r['cleanup_passed']and r['normal_STOP']and r['actual_exit']==0
    assert r['forced']==[]and r['failure']is None and r['close_error']is None
    assert r['completed_requests']==62 and all(r['EOF'].values())and r['original_guest_generation_absent']
    assert i['ready']['script_sha256']==sha(P/'redis_observer.py')
    assert int(i['NSpid'][0])==i['hostPID']and int(i['NSpid'][-1])==i['guestPID']==i['ready']['guestPID']
    assert i['starttime']==i['ready']['starttime']and i['starttime'].isdigit()
    assert i['CID']in i['cgroup']and i['ready']['database']==0 and i['ready']['auth_source']=='inherited_REDISCLI_AUTH'
cleanup=load('pilot/Root-independent-cleanup.json');assert cleanup['all_passed']
for row in cleanup['checks']:
    if row['kind']=='exact-host-generation':
        assert row['passed']and row['original_starttime']is not None and row['current_starttime']!=row['original_starttime']
    else:assert row['actual_exit']==0 and not row['stdout'].strip()
peer=load('peer/pilot-review.json');assert peer['status']=='RECORDED_FRESH_SOURCE_NATIVE_PROTOCOL_PILOT_PEER_CLEAR'
assert peer['result']['sha256']==sha(P/'pilot/result.json')and peer['actual_new_phases']==2 and peer['retained_phases']==0
qualification=load('qualification.json');assert not qualification['performance_gain_accepted']and not qualification['full_milestone_accepted']
print(json.dumps({'published_pilot_records_verified':True,'fresh_Source_Native_phases':2,'retained_phases':0,'normal_observer_requests_each':62,'performance_gain_accepted':False,'workloads_started':False},indent=2))
