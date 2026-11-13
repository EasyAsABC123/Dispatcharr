"""Recompute separate published functional/Original-qualification prerequisite data."""
import hashlib
import json
from pathlib import Path
from acceptance import qualify

IMAGE='sha256:fc5ea2aba28f86a3ebf3672f6bf8d874a6f79b2d9dd4c8a1efb8993de680375a'
ORIGINAL='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'

def verify(root, functional=None, qualification=None):
    root=Path(root)
    functional=functional if functional is not None else json.loads((root/'functional.json').read_text())
    qualification=qualification if qualification is not None else json.loads((root/'qualification.json').read_text())
    plan=json.loads((root/'protocol.json').read_text())
    protocol_sha=hashlib.sha256((root/'protocol.json').read_bytes()).hexdigest()
    counts=[]
    for projected, action, expected in [(functional,'functional',[(n,b)for n in [1,10,100]for b in ['source','candidate']]),(qualification,'qualify',[(n,'source')for n in [1,10,100]])]:
        report=projected['report'];review=projected['Root_review']
        assert review['passed'] is True and review['report_sha256']==projected['raw_report_sha256']
        assert report['passed'] is True and report['performance_verified'] is False
        assert report['action']==action and report['candidate_image']==IMAGE
        assert report['source_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430' and report['protocol_sha256']==protocol_sha
        assert report['cleanup']=='owned project removed'
        assert [(p['count'],p['backend'])for p in report['phases']]==expected
        total=0
        for phase in report['phases']:
            n=phase['count'];rows=phase['viewers'];total+=n
            assert phase['passed'] is True and len(rows)==phase['requests']==n
            assert phase['authenticated_identity']=={'clients':n,'all_owned_user_id':True} and phase['continuous_connections'] is True
            assert phase['source_bound_image']==(ORIGINAL if phase['backend']=='source' else IMAGE)
            assert phase['trial']==1 and phase['cleanup_upstream_active']==0
            assert phase['primer_frames']>=50
            for row in rows:
                assert row['status']==200 and row['requests']==1 and not row['errors'] and row['unexpected_eof'] is False and row['post_window_barrier'] is True
                assert row['prefix_bytes']==2097152 and row['decoder_exit_code']==0 and row['decoder_frames']>=25
                for key in ['prefix_sha256','decoder_stdout_sha256','decoder_stderr_sha256']:
                    assert len(row[key])==64 and all(c in '0123456789abcdef'for c in row[key])
            if action=='functional':
                assert phase['resources'] is None and phase['warmup_seconds']==phase['measure_seconds']==5
            else:
                computed=qualify(phase,plan)
                assert computed['passed'] is True and computed==phase['qualification']
        assert total==(222 if action=='functional' else 111)
        cleanup=projected['Root_independent_cleanup_projection']
        assert review['independent_owned_cleanup_empty']is True and cleanup['passed']is True and cleanup['inventories']=={'containers':[],'networks':[],'volumes':[]}and cleanup['query_exits']==[0,0,0]
        assert len(cleanup['raw_receipt_sha256'])==64
        if action=='functional':assert review['continuous_authenticated_readers']==222 and review['no_resource_sampling'] is True
        else:assert review['Native_resource_measurements'] is False and review['windows']==3 and review['Source_continuous_authenticated_readers']==111
        counts.append(total)
    return {'passed':True,'functional_readers':counts[0],'Original_qualification_readers':counts[1],'Original_qualification_samples':sum(p['qualification']['metrics']['samples']for p in qualification['report']['phases']),'Native_prerequisite_resource_metrics':False}

if __name__=='__main__':print(json.dumps(verify(Path(__file__).resolve().parent)))
