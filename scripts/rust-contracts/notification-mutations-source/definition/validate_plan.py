#!/usr/bin/env python3
"""Read-only definition/closure checks. Never imports Django or executes a fixture."""
import hashlib,json,re
from pathlib import Path
ROOT=Path(__file__).resolve().parent
TABLES={'accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal'}
REV='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
def require(ok,label):
    if not ok:raise ValueError(label)
def validate(p):
    require(p['state']=='UNEXECUTED_DEFINITION_REVIEW_ONLY','state')
    require(p['fixture_requests_executed']==0 and p['execution_authorized'] is False and p['controller_present'] is False,'execution-forbidden')
    require(p['Native_or_TCP_or_credentials_claim'] is False,'scope-claim')
    require(p['Source_revision']==REV,'pinned-revision')
    require(p['Source_image'].endswith('@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'),'pinned-image')
    cases=p['cases'];require(len(cases)==p['fixed_request_count']==p['bounds']['requests']==48,'fixed48')
    require([c['index']for c in cases]==list(range(48)) and len({c['name']for c in cases})==48,'case-order-names')
    require(p['candidate_methods']=={'PUT':'update','PATCH':'partial_update','DELETE':'destroy'},'method-actions')
    require(set(p['candidate_contract_ids'])=={'route-06ad6f49ab1bfa0a7ea3:'+m for m in p['candidate_methods']},'detail-route-only')
    require(set(p['audit_tables'])==TABLES and len(p['audit_tables'])==11,'exact-eleven')
    for c in cases:
        require(c['method']in p['candidate_methods'],'method-scope')
        require(c['path']=='/api/core/notifications/{'+c['target']+'}/','correct-detail-path')
        require(c['prediction_unmeasured']is True,'prediction-not-measurement')
        require(c['actor']in p['seed']['users'],'actor')
        require(c['target']in p['seed']['notifications']or c['target']in ['missing','malformed'],'seed-target')
        require(c['predicted_status']in [200,204,400,401,403,404,500],'status-hypothesis')
        require(c.get('body_encoding','json')in ['json','raw-invalid-json'],'body-encoding')
    for method in p['candidate_methods']:
        for actor,status in [('anonymous',401),('standard',403),('adminDenied',403)]:
            found=[c for c in cases if c['method']==method and c['actor']==actor]
            require(len(found)==1 and found[0]['predicted_status']==status and found[0]['effect_class']=='none','all-method-role-boundaries')
    require(p['seed']['users']['admin']['user_level']==10 and p['seed']['users']['standard']['user_level']==1 and p['seed']['users']['adminDenied']['user_level']==10,'persisted-levels')
    require(all(u['is_active']is True for u in p['seed']['users'].values()if u),'force-auth-active-scope')
    require(p['seed']['users']['admin']['UI']=='127.0.0.1/32' and p['seed']['users']['adminDenied']['UI']=='192.0.2.0/24','network-boundary')
    require(len(p['seed']['notifications'])==9 and len(p['seed']['dismissals'])==5,'finite-seed')
    require(p['seed']['cache']['H/admin']is False and p['seed']['cache']['V/admin']is True and str(p['seed']['cache']['U/admin']).startswith('ABSENT'),'cache-truth-and-miss')
    require(p['database_guard']['regex']=='^rust_probe_notifmut_[0-9a-f]{10}$' and 'dispatcharr'in p['database_guard']['deny'],'owned-database')
    require(len(p['database_guard']['require'])>=5,'isolation-default-audit-guards')
    effects={c['effect_class']for c in cases};require({'none','save-required-only','save-no-fields','normalize','replace-json','hide-inactive','hide-expired','delete-cascade','delete-developer','poison-developer'}<=effects,'effect-boundaries')
    require(cases[-2]['effect_class']=='poison-developer' and cases[-1]['predicted_status']==500 and cases[-1]['effect_class']=='none','poison-last-preserve-negative')
    require(len(p['preexecution_gates'])>=6 and any('second exact seal review'in s for s in p['preexecution_gates']),'second-review-required')
    require(any('finally'in s for s in p['lifecycle']) and any('exactID'in s for s in p['lifecycle']),'audit-and-independent-cleanup')
    require(p['bounds']['source_seconds']<=240 and p['bounds']['report_bytes']<=134217728,'finite-capture')
    return {'definition_checks':'PASS','requests':48,'Source_executed':False,'Docker_executed':False,'Native_executed':False,'execution_authorized':False}
def verify_inventory(root,records):
    for row in records:
        path=Path(row['path']);require(not path.is_absolute() and '..'not in path.parts,'inventory-path')
        data=(root/path).read_bytes();require(len(data)==row['bytes'] and hashlib.sha256(data).hexdigest()==row['sha256'],'input-hash:'+str(path))
def main():
    p=json.loads((ROOT/'plan.json').read_text());result=validate(p)
    seal=json.loads((ROOT/'seal.json').read_text());verify_inventory(ROOT,seal['inputs'])
    require(seal['kind']=='definition-only-not-execution-seal','seal-kind')
    for x in json.loads((ROOT/'external-inputs.json').read_text()):
        d=Path(x['path']).read_bytes();require(len(d)==x['bytes']and hashlib.sha256(d).hexdigest()==x['sha256'],'external-archive')
    result['frozen_inputs']=len(seal['inputs']);result['external_archives']=1;result['actual_image_dependency_equality']='NOT_YET_VERIFIED'
    print(json.dumps(result,indent=2))
if __name__=='__main__':main()
