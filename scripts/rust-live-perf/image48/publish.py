#!/usr/bin/env python3
"""Publish reviewed complete live resource evidence; explicit inputs, no defaults.
Original Dispatcharr authors retain full application/algorithm credit/licenses.
"""
import argparse,hashlib,importlib.util,json,shutil
from pathlib import Path
spec=importlib.util.spec_from_file_location('_live_independent_verifier',Path(__file__).with_name('verify.py'));v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
def publish(directory,parent_review,destination):
 directory=Path(directory);destination=Path(destination)
 raw_path=directory/'live-functional.json';report=json.loads(raw_path.read_text());review=json.loads(Path(parent_review).read_text());tools=directory/'tools/scripts/rust-live-perf';plan=json.loads((tools/'protocol.json').read_text())
 v.require(review['passed']and review['raw_live_report_sha256']==v.sha(raw_path),'parent review binding')
 v.require(report['protocol_sha256']==v.sha(tools/'protocol.json'),'protocol binding')
 for name in ['run.py','workload.py','resources.py','acceptance.py','controller_contract.py','seed.py','protocol.json']:
  v.require(report['tools_sha256']['scripts/rust-live-perf/'+name]==v.sha(tools/name),'tool binding '+name)
 result=v.verify(report,plan)
 # Validate every retained capture and successful decoder transcript, outside timing.
 raw_hashes={}
 for phase in report['phases']:
  label=phase['backend']+'-'+str(phase['count'])+'-'+str(phase['trial']);p=directory/(label+'.json');v.require(json.loads(p.read_text())==phase,'phase file '+label);raw_hashes[p.name]=v.sha(p)
  for i,row in enumerate(phase['viewers']):
   prefix=directory/label/(str(i)+'.ts');v.require(prefix.stat().st_size==2097152 and v.sha(prefix)==row['prefix_sha256'],'prefix hash')
   for suffix,key in [('stdout','decoder_stdout_sha256'),('stderr','decoder_stderr_sha256')]:v.require(v.sha(directory/label/(str(i)+'.decode.'+suffix))==row[key],'decoder transcript')
 # Every key below is a numeric resource/fixture observation or a static identity.
 # Exclude host commands, cue boot IDs, copied data and complete arbitrary tool snapshots.
 selected={key:report[key]for key in ['passed','performance_verified','action','source_revision','candidate_image','protocol_sha256','cleanup','phases','pairs']}
 projection={'format_version':1,'scope':result['scope'],'credit':'Original Dispatcharr authors and contributors retain all application/algorithm credit and original licenses.','protocol':plan,'report':selected,'verified':result,'raw_live_report_sha256':v.sha(raw_path),'parent_review_sha256':v.sha(parent_review),'raw_phase_files_sha256':raw_hashes,'executed_tools_sha256':{name:v.sha(tools/name)for name in ['run.py','workload.py','resources.py','acceptance.py','controller_contract.py','seed.py','protocol.json']},'full_media_bytes_published':False,'raw_prefix_every_byte_decoder_verified':False}
 destination.mkdir(parents=True,exist_ok=False);(destination/'projection.json').write_text(json.dumps(projection,indent=2)+'\n');return projection
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input',type=Path,required=True);p.add_argument('--parent-review',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();r=publish(a.input,a.parent_review,a.output);print(json.dumps(r['verified']))
