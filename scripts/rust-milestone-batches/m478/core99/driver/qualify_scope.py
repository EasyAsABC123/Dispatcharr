"""Recompute bounded Native41 scopes offline; retain strict failure literally."""
import argparse,hashlib,json
from pathlib import Path
from compare import validate,validate_http_sql
from gpg_trace import validate as validate_gpg,need
from origin_check import validate as validate_origin
HERE=Path(__file__).resolve().parent

def qualify(out):
    report=json.loads((out/'report.json').read_text());authority=json.loads((out/'inputs/authority.json').read_text());bound=json.loads((HERE/'source-authority.json').read_text());results={}
    for epoch in ['transport16','retained24','fresh404']:
        row=authority[epoch];need(row['source_sha256']==bound[epoch]['source_sha256'],'exact retained epoch authority');path=out/'inputs'/row['source_filename'];need(hashlib.sha256(path.read_bytes()).hexdigest()==row['source_sha256'],'exact raw reference bytes');source=json.loads(path.read_text());native=json.loads((out/(epoch+'-capture.json')).read_text());item={'HTTP_SQL':validate_http_sql(source,native),'GPG':validate_gpg(source,native,(out/(epoch+'-Main.logs')).read_bytes(),numeric_mode=report['candidate']['plugin_numeric_mode']),'Origin':validate_origin(out,epoch,authority),'strict_cache_passed':False};results[epoch]=item
        try:item['strict_qualification']=validate(source,native);item['strict_cache_passed']=True
        except ValueError as error:item['strict_failure']=str(error)
        physical=(out/(epoch+'-GPG-absence.stdout')).read_text().splitlines();namespace=physical[0];marker=physical.index('HOMES');need(physical[marker+1:]==[] and marker-1==len(item['GPG']['children']),'actual retained home/child closure observation')
        for child,line in zip(item['GPG']['children'],physical[1:marker]):
            need(child['PID_namespace']==namespace,'actual observed same Native PID namespace')
            need(line=='ABSENT '+str(child['pid']) or (line.split(' ',1)[0]==str(child['pid']) and line.rsplit(')',1)[1].split()[19]!=child['generation']),'retained actual GPG generation absent')
        need(report['cohorts'][epoch]['actual_Main_exit']==0,'actual recorded normal Main exit0')
        need(json.loads((out/(epoch+'-backend-after-Main.json')).read_text())==[],'actual post-Main backend absence')
    cleanup=json.loads((out/'independent-cleanup.json').read_text());need(cleanup['exact_owned_absence'] is True and cleanup['PID_namespace_checked'] is True,'retained independent fresh cleanup receipt')
    need((out/'default-before.json').read_bytes()==(out/'default-after.json').read_bytes(),'literal default62/59 unchanged')
    need(report['actual_controller_exit']==0 and report['normal_HTTP_SQL_GPG_scope_passed'],'ActualScopedNormalController0')
    numeric=json.loads((out/'fresh404-capture.json').read_text())['numeric_diagnostic']
    result={'candidate':report['candidate'],'actual_reference_cases':sum(v['HTTP_SQL']['actual_Main_API_cases'] for v in results.values()),'HTTP_SQL_GPG_normal_scope_passed':True,'cohorts':results,'strict_batch_passed':False,'actual_controller_exit':report['actual_controller_exit'],'prior_actual_framework_controller_exit2_preserved':True,'numeric_actual_status':numeric['status'],'numeric_domain_accepted':False,'adversarial_handler_drain_qualified':False,'retained_independent_cleanup':cleanup,'live_Docker_or_PID_queries_performed':False,'Source_API_runs':0,'full_M8_accepted':False,'performance_measured':False}
    from startup_review import qualify as qualify_startup
    result['startup_finite6']=qualify_startup(out)
    (out/'independent-normal-scope-review.json').write_text(json.dumps(result,indent=2)+'\n');return result
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();result=qualify(args.output);print(json.dumps({'normal_HTTP_SQL_GPG_scope_passed':True,'actual_reference_cases':result['actual_reference_cases'],'strict_batch_passed':False,'numeric_status':result['numeric_actual_status'],'Source_API_runs':0}));raise SystemExit(0)
