"""Exact packaged unified HTTP observations, no resource/performance claim.
Original Dispatcharr authors/contributors retain credit and AGPL licensing.
"""
import hashlib,json

def probe(command,compose,evidence,original):
    completed=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','-e','VOD_UNIFIED_OBSERVATION=http','-e','VOD_UNIFIED_TARGET=candidate','baseline','/dispatcharrpy/bin/python','/vod-api-probe/unified_source.py'],timeout=300)
    (evidence/'packaged-unified-controller.log').write_text(completed.stderr)
    native=json.loads(completed.stdout.splitlines()[-1]);assert native['passed']and native['target']=='candidate'and native['exact_owned_rows_restored']and native['provider_trap_attempts']==[]
    (evidence/'packaged-unified-observations.json').write_text(json.dumps(native,indent=2,ensure_ascii=False)+'\n')
    assert len(native['records'])==len(original['records'])==189
    historical_full=next(row['expected']['json']['results']for row in original['records']if row['label']=='cross-kind LOWER(name),id tie'and row['path']=='/api/vod/all/?page_size=101')
    historical_rows={(row['content_type'],row['id']):row for row in historical_full}
    records=[];differences=[];unsupported=[];fresh_count=0
    for index,(source,row)in enumerate(zip(original['records'],native['records'])):
        assert all(source[key]==row[key]for key in ['path','role','method','label'])
        actual=row['expected'];expected=source['expected']
        if source['headers']['Accept']=='text/html':
            unsupported.append({'source_index':index,'path':source['path'],'role':source['role'],'Source_status':expected['status'],'native_status':actual['status'],'explicit501':actual['status']==501,'parity_pass':False});continue
        if source['label']=='cross-kind LOWER(name),id tie':
            fresh_count+=1;expected=row['fresh_Original_HTTP'];assert hashlib.sha256(row['fresh_Original_raw_body'].encode()).hexdigest()==expected['body_sha256']
            for key in ['count','next','previous']:assert expected['json'][key]==source['expected']['json'][key]
            for item in expected['json']['results']:assert item==historical_rows[item['content_type'],item['id']]
        keys=['status','body_bytes','body_sha256','content_type','selected_headers']
        same=all(expected[key]==actual[key]for key in keys)
        item={'source_index':index,'path':source['path'],'method':source['method'],'role':source['role'],'same':same,'status':actual['status'],'body_sha256':actual['body_sha256'],'fresh_predeclared_Source_tie_authority':source['label']=='cross-kind LOWER(name),id tie'}
        if not same:differences.append({'case':item,'Source':expected,'native':actual})
        records.append(item)
    passed=not differences and len(records)==188 and len(unsupported)==1 and all(v['explicit501']for v in unsupported)and fresh_count==6
    result={'passed':passed,'scope':'Actual packaged native vs actual Original HTTP, allselected headers, sharedmetadataeffects; SourceRawbody ties use fixed fresh Original authority with historical non-orderfields; nofullroute/browser/performance acceptance','compared':len(records),'records':records,'differences':differences,'explicit_browsable_HTML_not_parity_pass':unsupported,'fresh_predeclared_tie_authorities':fresh_count,'all_owned_rows_restored':native['exact_owned_rows_restored'],'provider_trap_attempts':native['provider_trap_attempts'],'Source_database':original['database_witness'],'native_controller_database':native['database_witness'],'native_fingerprint_matches_Original_before':native['fingerprints_before']==original['fingerprints_before'],'native_fingerprint_matches_Original_after':native['fingerprints_after']==original['fingerprints_after']}
    result['passed']=result['passed']and result['native_fingerprint_matches_Original_before']and result['native_fingerprint_matches_Original_after']
    (evidence/'packaged-unified-http.json').write_text(json.dumps(result,indent=2)+'\n');return result
