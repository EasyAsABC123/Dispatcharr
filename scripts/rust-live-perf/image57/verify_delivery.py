"""Public input, prerequisite, authority and privacy checks; no private fallback."""
import hashlib,json,re
from pathlib import Path
from verify_projection import verify as numeric
from verify_prerequisites import verify as prerequisites
HERE=Path(__file__).resolve().parent
IMAGE='sha256:fc5ea2aba28f86a3ebf3672f6bf8d874a6f79b2d9dd4c8a1efb8993de680375a'
REV='d77f25fa94ee128c79ade9176d8eb4a9d23bf498'
TREE='bbaa9755b4426e256d1f875f66b5c2feb46a127fa6e5150c470f6f23a6456e9c'
BUILD='c1e096bdf7789c5fc2d0d9004ca6e3a68f663fe9100158b26fc621092082df0a'
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
    sources=app['source_sha256'];assert len(sources)==app['application_input_count']==1336
    rust={k:v for k,v in sources.items()if k.startswith('rust/')};assert len(rust)==app['Rust_input_count']==839
    assert len([k for k in sources if k.startswith('frontend/')])==495
    assert sha(json.dumps(sources,sort_keys=True,separators=(',',':')).encode())==TREE
    assert sha((json.dumps(rust,indent=2)+'\n').encode())=='d349ac8d72d5e77f41c75bde46855129c18a3c9f9d061c92873ff9a8d568f67a'
    assert app['source_archive_sha256']=='868d5e4c23b4c48b0f92619f92c104f1ecf6c372210c078a11cef5dd2e58902a'
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
    assert review['gain_transfer_to_any_other_image_or_source']is False and review['full_milestone_or_rewrite_acceptance']is False
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
    return {'passed':True,'application_inputs':1336,'Rust_inputs':839,'support_files':1970,'actual_executed_tools':1978,'raw_samples':1080,'in_window_samples':1062,'privacy_checked':True,'private_fallback':False,'gain_transfer':False}
if __name__=='__main__':print(json.dumps(verify(),indent=2))
