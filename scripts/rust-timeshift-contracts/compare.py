"""Compare source-bound capture slices without asserting application parity."""

def compare_sessions(source,candidate):
    differences=[]
    expected={event['case']:event for event in source['events']}
    actual={event['case']:event for event in candidate['events']}
    for case in sorted(expected.keys()|actual.keys()):
        if case not in expected or case not in actual:
            differences.append({'case':case,'field':'present'})
            continue
        for key in ['status','body','allow']:
            left,right=expected[case].get(key),actual[case].get(key)
            if key=='allow':
                left=sorted(part.strip() for part in left.split(',')) if left else left
                right=sorted(part.strip() for part in right.split(',')) if right else right
            if left!=right:
                differences.append({'case':case,'field':key,'source':left,'candidate':right})
    return {'captured_wire_matches':not differences,'differences':differences,'application_parity_verified':False,'performance_verified':False}


def compare_media(source,candidate):
    differences=[]
    expected={event['case']:event for event in source['events']}
    actual={event['case']:event for event in candidate['events']}
    stable_state=['api_exists','pool_exists','pool_busy','profile_connections','stats_client_count']
    for case in sorted(expected.keys()|actual.keys()):
        if case not in expected or case not in actual:
            differences.append({'case':case,'field':'present'})
            continue
        left,right=expected[case],actual[case]
        for key in ['status','bytes','sha256','headers','error_body']:
            if left.get(key)!=right.get(key):differences.append({'case':case,'field':key,'source':left.get(key),'candidate':right.get(key)})
        for phase in ['after_body','after_grace']:
            for key in stable_state:
                if left[phase].get(key)!=right[phase].get(key):differences.append({'case':case,'field':phase+'.'+key,'source':left[phase].get(key),'candidate':right[phase].get(key)})
            for key in ['api_ttl','pool_ttl']:
                # Timing differs across the standalone observer's Django boot.
                # Live TTLs remain bounded, and absence has exact Redis -2 TTL.
                value=right[phase][key]
                exists=right[phase]['api_exists' if key=='api_ttl' else 'pool_exists']
                maximum=600 if key=='api_ttl' else (600 if right[phase]['pool_busy']=='1' else 60)
                source_ttl=left[phase][key]
                if (exists and (not 0<=value<=maximum or source_ttl>=0 and abs(value-source_ttl)>30)) or (not exists and value!=-2):differences.append({'case':case,'field':phase+'.'+key,'candidate':value,'source':source_ttl,'expected':'bounded live TTL within30seconds observer jitter or exact absent -2'})
        for key in ['mode','fixture_sha256','fixture_bytes','bytes_per_second','requests']:
            if left['origin'].get(key)!=right['origin'].get(key):differences.append({'case':case,'field':'origin.'+key,'source':left['origin'].get(key),'candidate':right['origin'].get(key)})
    return {'captured_media_matches':not differences,'differences':differences,'normalizations':['elapsed wall time is measured separately','observer boot makes exact remaining TTL unstable; source policy/bounds and30second jitter ceiling retained'],'application_parity_verified':False,'performance_verified':False}
