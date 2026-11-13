"""Public input, prerequisite, authority and privacy checks; no private fallback."""
import hashlib,json,re
from pathlib import Path
from verify_projection import verify as numeric
from verify_prerequisites import verify as prerequisites
HERE=Path(__file__).resolve().parent
IMAGE='sha256:120df2f930a7f4d2738a04fbd793c16b230b7a80114f4d6b03f670244940a014'
REV='aca6d900ad13700ab86b2898134a61e4dba57bc1'
TREE='6457d75bad17874f039eaed58941e3a047fb124a841637ff1a28392db2472e5c'
BUILD='3e1cb5c613d280c9297d75e1a69998f22c858c8a3d158949d61377cd1ffd3e76'
SUPPORT='28122faf28a0acf1c136ca0d0831961e786cb686'
sha=lambda b:hashlib.sha256(b).hexdigest()
def privacy(value):
    if isinstance(value,dict):
        for k,v in value.items():
            assert k not in {'boot_id','nonce','pid','starttime','authorization','access_token','refresh_token','cookie','password','argv','command','cleanup_queries'}
            privacy(v)
    elif isinstance(value,list):
        for v in value:privacy(v)
    elif isinstance(value,str):
        assert not re.search(r'eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+',value)
        assert ('/home/'+'justin/') not in value and not value.startswith('Bearer ')
def verify(root=HERE,overrides=None):
    root=Path(root);overrides=overrides or {}
    def read(n):return overrides[n]if n in overrides else json.loads((root/n).read_text())
    app=read('application-inputs.json');execution=read('execution-inputs.json');projection=read('projection.json');review=read('parent-review.json');chain=read('cue-claim-chain.json');plan=read('plan.json')
    assert app['candidate_image']==IMAGE and app['git_revision']==REV and app['source_tree_sha256']==TREE and app['build_manifest_sha256']==BUILD
    sources=app['source_sha256'];assert len(sources)==app['application_input_count']==1291
    rust={k:v for k,v in sources.items()if k.startswith('rust/')};assert len(rust)==app['Rust_input_count']==794
    assert len([k for k in sources if k.startswith('frontend/')])==495
    assert sha(json.dumps(sources,sort_keys=True,separators=(',',':')).encode())==TREE
    assert sha((json.dumps(rust,indent=2)+'\n').encode())=='a65ca5bfb57097b387451a9cea6c0341c670823d64e541704a20f63a4969dc0d'
    assert app['source_archive_sha256']=='8addffe734f0326b99f9faeb7fac0af670129a482ecb591df2a63638446c74af'
    assert sources['LICENSE']=='8486a10c4393cee1c25392769ddd3b2d6c242d6ec7928e1414efff7dfb2f07ef'
    assert execution['support_revision']==execution['actual_executed_tool_base_commit']==SUPPORT
    assert execution['support_archive_sha256']=='4d33a8153050f80aa8c8ae7c3fbb7697b1c75071df071cbe856b126cec61c2d1'
    assert len(execution['support_sha256'])==execution['support_file_count']==1970
    assert len(execution['actual_executed_tools_sha256'])==execution['actual_executed_tool_count']==1978
    for name,digest in projection['executed_tools_sha256'].items():
        assert sha((root/name).read_bytes())==digest==execution['actual_executed_tools_sha256']['scripts/rust-live-perf/'+name]
    assert review['passed']and review['raw_live_report_sha256']==projection['raw_live_report_sha256']
    assert sha((root/'parent-review.json').read_bytes())==projection['parent_review_sha256']
    assert review['resource_samples_raw']==1080 and review['resource_samples_in_window']==1062
    assert sum(len(p['resources']['raw']['samples'])for p in projection['report']['phases'])==1080
    assert projection['verified']['samples']==1062
    assert review['exact_project_all_containers_networks_volumes_absent']and review['actual_wrapper_exit']==0
    assert review['gain_transfer_to_any_other_image_or_current806_recording_runtime']is False and review['full_milestone_or_rewrite_acceptance']is False
    stages=chain['stages'];assert len(stages)==3 and [r['action']for r in stages]==['functional','qualify','measure']
    for r in stages:
        assert r['no_retry']is True and r['candidate_manifest_sha256']==BUILD
        assert 0<r['authority_window_seconds']<=7200
        for k in ['cue_sha256','exclusive_claim_sha256','dispatch_plan_sha256','selection_sha256']:
            assert re.fullmatch('[0-9a-f]{64}',r[k])
    assert plan['worker_graph_activation']is False and plan['no_gain_transfer_to_any_other_image_or_source']is True
    assert plan['viewers']==[1,10,100]and plan['resource_phase_seconds']==[15,60]
    assert plan['pair_orders']==[['source','candidate'],['candidate','source'],['source','candidate']]
    for value in [app,execution,projection,review,chain,plan,read('functional.json'),read('qualification.json'),read('preserved-negatives.json')]:privacy(value)
    # Independently recompute the full numeric and prerequisite projections as well.
    numeric(root/'projection.json');prerequisites(root)
    return {'passed':True,'application_inputs':1291,'Rust_inputs':794,'support_files':1970,'actual_executed_tools':1978,'raw_samples':1080,'in_window_samples':1062,'privacy_checked':True,'private_fallback':False,'gain_transfer':False}
if __name__=='__main__':print(json.dumps(verify(),indent=2))
