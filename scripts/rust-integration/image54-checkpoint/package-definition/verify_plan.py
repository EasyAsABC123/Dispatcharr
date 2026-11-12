"""Image54 package input/definition checks only; never dispatches workloads."""
import hashlib,io,json,os,subprocess,tarfile
from pathlib import Path
HERE=Path(__file__).resolve().parent
REV='044f3ad0fb3ee68ffb17b23bbb796ccdae954298'
IMAGE='sha256:8e3c133c0639d60057d8d05a5be2fce66e0f0968c35e29f674c348495a1a9ca0'
BUILD='cac7bcdcc60dff6f26e233e78c2737b7f5d9450cf54042719b8d5cd61cf5ff8b'
TREE='a8ab7ab0e400c1ebcdc1bc0e232434bb9408ba5165a048d1cbf2c0716f501256'
ARCHIVE='86fb4e93032cc1565ada0d0935e21805dc6e0352897f5eeed06b7a0748b3c880'
MAP='7acb3cf3c8777286cbfab1a8e338920ede7151d9ef173db5b259f77e3b6e802f'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def require(value,message):
    if not value:raise ValueError(message)
def guide_clock(minute):return type(minute)is int and 0<=minute<=35
def fresh_reference(age):return type(age)in (int,float)and 0<=age<=900
def validate(plan=None,deep=True):
    plan=plan if plan is not None else json.loads((HERE/'plan.json').read_text());old=json.loads((HERE/'definition-preimage.json').read_text());support=Path(plan['support_root']);c=plan['candidate']
    require(plan['dispatch_authorized']is False and plan['performance_acceptance']is False,'BlockedFunctionalOnly')
    require(c['image_id']==IMAGE and c['revision']==REV and c['manifest_sha256']==BUILD and c['tree_sha256']==TREE and c['archive_sha256']==ARCHIVE and c['runtime_map_sha256']==MAP and c['runtime_inputs']==792 and c['app_inputs']==1289,'Exact792Candidate')
    require(plan['support_revision']==old['support_revision']=='f0bfc2f200dcfd1f64ae32a9855ea250731ce67f','FrozenSupportRevision')
    require(plan['scheduling']==old['scheduling'],'UnchangedScheduling')
    require(plan['host_tooling']['preflight_before_any_matrix_output_state_claim_or_child']is True,'ToolingBeforeClaim')
    require(plan['fixture_coordination_barrier']['fresh_Root_finite_clearance_required']is True and plan['fixture_coordination_barrier']['business_outcome_condition']is False,'FreshOwnershipClearance')
    require(len(plan['groups'])==len(old['groups'])==28,'Exact28')
    output=HERE.parent/'image54-packaged-matrix-2'
    require(support==HERE.parent/'image54-package-plan-1/support' and c['manifest']==str(HERE.parent/'candidate-build-notification-mutations-image54-1/build.json'),'ExactContextPaths')
    def forward(value):return value.replace(old['support_root'],str(support)).replace(str(HERE.parent/'image54-packaged-matrix-1'),str(output)).replace(old['candidate']['manifest'],c['manifest']).replace(old['candidate']['image_id'],IMAGE)
    for row,before in zip(plan['groups'],old['groups']):
        expected={**before,'argv':[forward(v)for v in before['argv']],'cwd':str(support),'candidate_manifest_sha256':BUILD}
        require(row==expected,'ExactDefinition:'+row['name'])
        require(row['dispatch_authorized']is False and row['cwd']==str(support),'BlockedFrozenNode')
        require(Path(row['argv'][1]).is_file(),'FrozenEntrypoint')
        require(not any('--measure'==v or '--api-resource-matrix'==v for v in row['argv']),'NoResourceAction')
    if deep:
        from tooling_guard import assert_inputs
        assert_inputs(json.loads((HERE/'environment.json').read_text()),json.loads((HERE/'tooling-inputs.json').read_text()))
        meta=json.loads((HERE/'support-inputs.json').read_text());require(sha(HERE/'support-inputs.json')==plan['support_input_map_sha256'],'SupportManifest')
        require(len(meta['files'])==6148 and all(sha(support/k)==v for k,v in meta['files'].items()),'AllSupportBytes')
        env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};env.update(json.loads((HERE/'environment.json').read_text()))
        def git(*args):return subprocess.check_output(['git',*args],cwd=support,env=env)
        require(git('rev-parse','HEAD').decode().strip()==plan['support_revision'],'DetachedHead')
        require(not git('status','--porcelain','--untracked-files=no').strip(),'FrozenTrackedSupport')
        require(hashlib.sha256(git('archive','HEAD')).hexdigest()==meta['support_archive_sha256'],'NestedArchivePinned')
        b=json.loads(Path(c['manifest']).read_text());require(sha(c['manifest'])==BUILD and b['image_id']==IMAGE and b['git_revision']==REV and b['source_tree_sha256']==TREE,'ActualBuildManifest')
        require(sha(HERE/'binding/build.json')==BUILD,'SealedBuildSnapshot')
        require(sha(HERE/'binding/runtime792.json')==MAP,'RuntimeMap')
        m=json.loads((HERE/'binding/application1289.json').read_text());require(m==b['source_sha256']and len(m)==1289,'AllAppInputs')
        require({k:v for k,v in m.items()if k.startswith('rust/')}==json.loads((HERE/'binding/runtime792.json').read_text()),'All792Runtime')
        require(hashlib.sha256(json.dumps(m,sort_keys=True,separators=(',',':')).encode()).hexdigest()==TREE,'AppTree')
        archive=Path(c['manifest']).parent/'source.tar';require(sha(archive)==ARCHIVE,'ActualSourceArchive')
        actual={}
        with tarfile.open(archive)as t:
            for member in t:
                if member.isdir():continue
                require(member.isfile()and member.name not in actual,'ArchiveShape')
                actual[member.name]=hashlib.sha256(t.extractfile(member).read()).hexdigest()
        require(actual==m,'ArchiveInputEquality')
        credit=json.loads((HERE/'binding/Root-input-credit-review.json').read_text());require(credit['passed']is True and credit['manifest_sha256']==BUILD and credit['image_id']==IMAGE and credit['Original_credit_preserved']is True and credit['runtime_actual_uid']=='65532','RootBuildCredit')
        require(not output.exists(),'FreshOutputRoot')
    return {'passed':True,'required_nodes':28,'support_files':6148,'support_revision':plan['support_revision'],'Rust_inputs':792,'application_inputs':1289,'unchanged_definitions_timings_budgets':True,'all_dispatch_blocked':True,'Docker_Source_Main_Cargo_or_resource_executed':False}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
