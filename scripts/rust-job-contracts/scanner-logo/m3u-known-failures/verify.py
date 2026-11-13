#!/usr/bin/env python3
"""Offline raw/source/exit/scope checks. Does not relabel historical failures."""
import gzip,hashlib,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
def sha(data):return hashlib.sha256(data).hexdigest()
def check(value,name):
 if not value:raise ValueError(name)
def raw(root,row):
 compressed=(root/row['public_gzip']).read_bytes();check(sha(compressed)==row['public_gzip_sha256'],'CompressedEvidenceChanged');value=gzip.decompress(compressed);check(len(value)==row['raw_bytes']and sha(value)==row['raw_sha256'],'RawEvidenceChanged');return value
def verify(root=HERE):
 bindings=json.loads((root/'evidence-bindings.json').read_text());summary=json.loads((root/'summary.json').read_text());checks=[]
 for name,rows in bindings.items():
  for filename,row in rows.items():
   if 'public_gzip'in row:raw(root,row)
  check(rows['default_manifest_binding_only']['actual_bytes_equal']is True,'DefaultBytesNotEqual')
 paired=json.loads(raw(root,bindings['m3u-failure-native-paired-1']['source-logo-scanner-private.json']));check(paired['passed']is True and len(paired['cases'])==8 and len(paired['checks'])==189,'ActualPairedGuest');check(sum(len(v['native_ids'])for v in paired['cases'])==15 and all(v['source_fallback']is False for v in paired['cases']),'All15NativeNoFallback');check(summary['paired']['actual_host_exit']==1,'DoNotRelabelPairedHost1')
 failed=json.loads(raw(root,bindings['m3u-failure-native-physical-1']['source-logo-scanner-private.json']));check(failed['passed']is False and len(failed['checks'])==107 and failed['checks'][-1]['passed']is False,'PreservedPhysical1Negative');prefix=[v for v in failed['checks']if v['name'][:2]in [str(i).zfill(2)for i in range(8)]];check(len(prefix)==106 and all(v['passed']is True for v in prefix),'ActualFirst8PrefixOnly')
 final=json.loads(raw(root,bindings['m3u-failure-native-physical-remaining-3']['source-logo-scanner-private.json']));check(final['passed']is True and len(final['cases'])==2 and len(final['checks'])==21 and all(v['passed']is True for v in final['checks']),'FinalActual2Phases21');check(all(v['tail']for v in final['cases']),'LegitimateSingleTailsConsumed')
 for name in ['m3u-failure-native-physical-remaining-1','m3u-failure-native-physical-remaining-2']:
  failed=json.loads(raw(root,bindings[name]['source-logo-scanner-private.json']));check(failed['passed']is False,'HistoricalRemainingFailurePreserved')
 lease=json.loads(raw(root,bindings['m3u-failure-native-leases-1']['integration.json']));check(lease['passed']is True and len(lease['lease_controls'])==3 and all(v['actual_exit']==0 and v['process_settled']is True for v in lease['lease_controls']),'Actual3LeaseMethods')
 selection=json.loads((root/'runtime-selection.json').read_text())
 for row in selection['files']:check(sha((root/'historical'/row['path']).read_bytes())==row['sha256'],'HistoricalRuntimeChanged')
 check(summary['no_default_graph_or_Main_activation']is True and summary['no_new_resource_or_performance_claim']is True and summary['raw_Source_tracebacks_equal']is False,'NoScopePromotion')
 return {'passed':True,'scope':'Offline evidence/digest/exits/source checks only; not newapp/build acceptance','runtime_paths':len(selection['files']),'runs':len(bindings),'all_historical_negatives_preserved':True}
if __name__=='__main__':print(json.dumps(verify()))
