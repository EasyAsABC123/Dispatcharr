"""Compare owned Origin useful work; preserve TLS request-header differences."""
import base64,hashlib,json
from pathlib import Path
from gpg_trace import need

def validate(output,epoch,authority):
    observed=json.loads((output/(epoch+'-Origin-observations.json')).read_text())
    if epoch=='transport16':
        wanted=[json.loads(line) for line in (output/'inputs/transport16-origin.jsonl').read_text().splitlines()]
        wanted=[row for row in wanted if row['kind']=='request'];actual=[row for row in observed if row['kind']=='request']
        need(len(actual)==len(wanted)==12,'exact retained TLS application request count12')
        equality=[]
        for a,b in zip(wanted,actual):
            need(a['target']==b['target'] and a['response_sha256']==b['response_sha256'] and a['response_b64']==b['response_b64'],'same TLS Origin target/response bytes')
            for row in [a,b]:need(hashlib.sha256(base64.b64decode(row['response_b64'],validate=True)).hexdigest()==row['response_sha256'],'actual Origin response digest')
            equality.append(a['raw_b64']==b['raw_b64'])
        return {'passed':True,'actual_application_requests':len(actual),'literal_response_bytes_equal':True,'actual_request_header_bytes_equal_by_request':equality,'TLS_header_byte_parity_qualified':False,'passive_whole_network_clean_claim':False}
    binding=authority[epoch];reference=output/'inputs'/binding['actual_Source_origin_filename'];need(hashlib.sha256(reference.read_bytes()).hexdigest()==binding['actual_Source_origin_sha256'],'exact observed unsigned Origin reference')
    wanted=[json.loads(line) for line in reference.read_text().splitlines()];regular=[row for row in observed if row['path']!='/numeric-float.json'];numeric=[row for row in observed if row['path']=='/numeric-float.json']
    need(len(regular)==len(wanted)==binding['actual_Source_origin_count'],'measured unsigned Origin count17or1')
    need(len(numeric)==(1 if epoch=='fresh404' else 0),'one declared numeric diagnostic only')
    for a,b in zip(wanted,regular):
        need(b['valid'] is True and b['wire_write_complete'] is True and b['peer'][0]=='11.253.250.3','actual owned Origin request')
        need(all(a[key]==b[key] for key in ['method','path','path_occurrence','status','content_type','body_hex','body_sha256']),'retained exact unsigned Origin request/reply')
        raw=bytes.fromhex(b['request_header_hex']);need(hashlib.sha256(raw).hexdigest()==b['request_header_sha256'],'literal actual request digest')
        lines=raw.decode('ascii').split('\r\n');need(lines[0]=='GET '+b['path']+' HTTP/1.1','actual GET version');headers=[line.split(': ',1) for line in lines[1:] if line]
        need(headers==[list(row) for row in a['headers']],'retained outgoing unsigned header order/values')
    if numeric:need(numeric[0]['valid'] is True and numeric[0]['status']==200,'numeric fixture actually served before capability refusal')
    return {'passed':True,'actual_unsigned_reference_requests':len(regular),'additional_numeric_requests':len(numeric),'retained_header_order_values_equal':True,'Source24_old_planned18_not_relabelled':True,'passive_whole_network_clean_claim':False}
