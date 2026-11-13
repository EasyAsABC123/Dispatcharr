"""Separate immutable Source HEAD1 authority; never dispatch or modify Source."""
import hashlib,json,math
from pathlib import Path
EXPECTED_CAPTURE_SHA='e9db2918f98adae5ae1766c4b327349f8b94b30c726a406e336cfbb86f1a2891'

def need(ok,label):
 if not ok:raise ValueError(label)
def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def validate_frozen_head_reference(root):
 root=Path(root);ref=json.loads((root/'Source-HEAD1-reference.json').read_text())
 need(ref['raw_sha256']==EXPECTED_CAPTURE_SHA,'ExactActualSourceHEADReference')
 for name,want in ref['retained_files'].items():
  path=Path(name);need(not path.is_absolute()and'..'not in path.parts,'SafeHEADReferencePath');need(digest(root/path)==want,'ExactRetainedHEADBytes')
 raw=root/'retained-head/Source-method-private.json';need(digest(raw)==EXPECTED_CAPTURE_SHA,'ExactActualSourceHEADCapture')
 value=json.loads(raw.read_text());summary=json.loads((root/'retained-head/summary-private.json').read_text());review=json.loads((root/'retained-head/agent-independent-review.json').read_text())
 need(len(value['cases'])==1 and value['Source_functions_replaced']is False and value['Native_requests']==0,'ExactlyOneSourceHEADNoNative')
 row=value['cases'][0];need({k:row[k]for k in ref['observed']}==ref['observed'],'LiteralHEADProjection')
 need(row['id']==1 and row['name']=='invalid_bearer_HEAD_valid_UUID'and row['method']=='HEAD'and row['status']==401 and row['body_bytes']==0 and row['body_hex']==''and row['body_sha256']==hashlib.sha256(b'').hexdigest(),'ActualHEAD401EmptyWire')
 headers={}
 for name,val in row['headers']:headers.setdefault(name.lower(),[]).append(val)
 for name,want in [('content-length','172'),('allow','OPTIONS, GET'),('vary','Accept, origin'),('content-type','application/json'),('www-authenticate','Bearer realm="api"')]:need(headers.get(name)==[want],'LiteralSourceHEADHeader:'+name)
 need('x-worker-id'not in headers,'NoSourceHEADSuccessWorkerHeader')
 need(row['before']==row['after'] and value['before']==value['after'],'OwnedHEADSQLUnchanged')
 for x in ['began_monotonic','completed_monotonic']:need(type(row[x])in(int,float)and math.isfinite(row[x]),'FiniteSourceHEADClock')
 need(0<=row['completed_monotonic']-row['began_monotonic']<=15 and row['absolute_observation']['timer_joined']is True and row['physical_close']['fd_after']==-1 and row['physical_close']['original_inode_absent']is True and row['physical_close']['timer_joined']is True,'ActualSourceHEADPhysicalClose')
 need(summary['passed']is True and summary['remaining']=={'containers':'','networks':'','volumes':''}and summary['default_before']==summary['default_after']and summary['default_all_public_tables_sequences_unchanged']is True,'SourceHEADDefaultAndOwnedCleanup')
 need(summary['origin_terminal']['rows']==[]and summary['origin_terminal']['invalid']==[],'SourceHEADNoProviderRequest')
 need(review['actual_host_exit']==0 and review['actual_capture_sha256']==EXPECTED_CAPTURE_SHA and review['actual_summary_sha256']==digest(root/'retained-head/summary-private.json')and review['owned_SQL_equal']is True and review['default_full62_and59_equal']is True,'IndependentHEADReceipt')
 return {'actual_Source_HEAD_requests':1,'Source_requests_rerun':False,'actual_capture_sha256':EXPECTED_CAPTURE_SHA,'status':401,'wire_body_bytes':0,'Content_Length':'172','Allow':'OPTIONS, GET','Vary':'Accept, origin','Source6_epoch_separate':True,'full_header_or_method_parity':False}
