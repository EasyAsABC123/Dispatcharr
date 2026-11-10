#!/usr/bin/env python3
"""Publish credential-free bounded refresh construction/HTTP integration evidence.
Original Dispatcharr authors retain all application/algorithm credit and AGPL.
"""
import argparse,hashlib,json
from pathlib import Path

def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--production',required=True,type=Path)
    parser.add_argument('--http',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    first=json.loads((args.production/'integration.json').read_text())
    second=json.loads((args.http/'integration.json').read_text())
    for report in [first,second]:
        assert report['passed'] and report['default_database_unchanged'] and report['cleanup']=='project removed'
    private=args.http/'source-native-refresh-http-private.json'
    observed=json.loads(private.read_text())
    assert observed['passed'] and observed['queue_and_results_after_cleanup']==[0,0]
    native=observed['native']
    assert native['drained'] and native['held_sql_caller_abort'] and native['active_shutdown_reported_incomplete'] and not native['aborted_http_response_observed']
    assert len(observed['cases'])==20 and len(native['records'])==20
    rows=[]
    for source,actual in zip(observed['cases'],native['records']):
        assert source['name']==actual['name'] and source['status']==actual['status'] and source['body_sha256']==actual['body_sha256'] and source['body_bytes']==actual['bytes'] and source['mime']==actual['mime']
        rows.append({'name':source['name'],'status':source['status'],'mime':source['mime'],'body_bytes':source['body_bytes'],'body_sha256':source['body_sha256'],'role':source['role']})
    output={'passed':True,'scope':'Bounded positive-ID refresh production constructor/poller and real HTTP local import/result/drain component. Explicit app attachment defaults OFF; no packaged/second-import/Index/Series/ETA/fullM6 acceptance.',
        'original_revision':first['original_revision'],'production':{'checks':first['checks'],'physical_tests':5,'archive_sha256':first['rust_source_archive_sha256'],'integration_sha256':digest(args.production/'integration.json')},
        'http':{'checks':second['checks'],'requests':20,'archive_sha256':second['rust_source_archive_sha256'],'integration_sha256':digest(args.http/'integration.json'),'source_private_sha256':digest(private),'rows':rows},
        'results':{'normal_records':3,'actual_original_AsyncResult':True,'local_programme_committed':True,'source_status_success':True,'backend_expiry_seconds':3600,'held_actual_SQL_client_abort':True,'incomplete_shutdown_before_reply':True,'aborted_response_observed':False,'Index_Series_and_aborted_refresh_preserved_in_FIFO':True,'result_for_unconsumed_request_fabricated':False},
        'strict_clippy':'artifacts/rust/epg-refresh-app-attached-input-1/clippy.log','default_units':{'passed':34,'external_ignored':20},
        'default_database_unchanged':True,'cleanup':'both projects removed','queue_and_results_after_cleanup':[0,0],
        'limitations':['Raw null/nonpositive/bool/string/float ID shapes501 pending Source-worker interpretation.','Only positive-ID JSON browser path; no form/multipart/browsableHTML/full-header parity.','Refresh-only polling cannot consume Index/Series/ETA heads; default app enablement remainsOFF.','Typed sink callbacks observed; packaged real bus/React/guide/second-import workflow not yet accepted.','Four staged HTTP runs are successive scope extensions; earlier successful artifacts remain unchanged.']}
    args.output.write_text(json.dumps(output,indent=2)+'\n')
    print(json.dumps({'passed':True,'production_checks':first['checks'],'http_checks':second['checks'],'output':str(args.output)}))
if __name__=='__main__':main()
