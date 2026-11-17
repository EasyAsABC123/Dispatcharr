"""Own-input CUT proof inside the caller's existing baseline codec role.
No service creation, coordinator, global lock, app/Comskip or booking call.
"""
import hashlib,json,shutil,uuid
from pathlib import Path
HERE=Path(__file__).resolve().parent
SOURCE_IMAGE='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def load(p):return json.loads(Path(p).read_text())
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def beneath(path,root):
 path=Path(path);root=Path(root).resolve();assert not path.is_symlink()and not any(x in('..','.')for x in str(path).split('/'))
 resolved=path.resolve(strict=True);assert resolved.is_relative_to(root)and resolved.is_file();return resolved

def compare_in_existing_role(command,compose,host_data,*,original_input,actual_output,edl_text,edl_sha256):
 """command(argv, timeout) is the fixture's existing bounded command callback."""
 data=Path(host_data).resolve(strict=True);original=beneath(original_input,data);actual=beneath(actual_output,data);assert original!=actual
 assert type(edl_text)is str and len(edl_text.encode())<=1024*1024 and hashlib.sha256(edl_text.encode()).hexdigest()==edl_sha256
 calibration=load(HERE/'Source6-calibration.json');terminal=load(HERE/'Source6-terminal.json');cleanup=load(HERE/'Source6-Root-cleanup.json')
 assert calibration['passed']and calibration['mode']=='qualify-source6'and calibration['owned_cleanup']['passed']and calibration['codec_result']['all_streams_and_full_timing_exact']and cleanup['passed']
 assert terminal['actual_exit']==0 and terminal['stdout_EOF']and terminal['stderr_EOF']and terminal['process_group_settled']
 assert sha(HERE/'guest.py')==calibration['tool_inputs']['guest']['sha256']and sha(HERE/'decoder-authority.json')==calibration['tool_inputs']['decoder_authority']['sha256']
 cid=command(compose+['ps','-q','baseline'],timeout=15).strip();image=json.loads(command([compose[0],'inspect','--format','{{json .Image}}',cid],timeout=15));assert image==SOURCE_IMAGE
 nonce=uuid.uuid4().hex;root=data/('comskip-own-input-'+nonce);root.mkdir(mode=0o755);root.chmod(0o755);tools=root/'tools';tools.mkdir(mode=0o755);tools.chmod(0o755)
 for name in('guest.py','evaluate.py','decoder-authority.json'):
  source=HERE/name;target=tools/name;shutil.copyfile(source,target);assert sha(target)==sha(source);target.chmod(0o444)
 edl=root/'actual-generated.edl';edl.write_bytes(edl_text.encode());edl.chmod(0o444)
 guest_root='/shared/'+str(root.relative_to(data));work='/tmp/comskip-own-input-'+nonce;evidence=root/'evidence'
 argv=compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-I','-B',guest_root+'/tools/evaluate.py','--original','/shared/'+str(original.relative_to(data)),'--actual','/shared/'+str(actual.relative_to(data)),'--edl',guest_root+'/actual-generated.edl','--work',work,'--evidence',guest_root+'/evidence','--authority',guest_root+'/tools/decoder-authority.json','--seconds','300']
 error=None;raw=None;scratch_cleanup=None
 try:raw=command(argv,timeout=320)
 except BaseException as exc:error=type(exc).__name__
 finally:
  # Same already-owned role and command path, after the guest's owned children
  # settle. No nested container, scheduler or lock; keep bounded evidence files.
  result=load(evidence/'result.json')if(evidence/'result.json').is_file()else None
  children=load(evidence/'commands.json')if(evidence/'commands.json').is_file()else []
  settled=result is not None and result.get('all_input_handles_closed')is True and bool(children)and all(row.get('wait_completed')is True and row.get('original_generation_absent')is True and row.get('process_group_settled')is True and row.get('stderr_EOF')is True and(row.get('stdout_EOF')is True or row.get('stdout_direct_file_closed')is True)for row in children)
  if settled:
   script="import json,shutil;from pathlib import Path;p=Path("+repr(work)+");assert p.parent==Path('/tmp')and p.name.startswith('comskip-own-input-');shutil.rmtree(p,ignore_errors=True);print(json.dumps({'scratch_absent':not p.exists()}))"
   scratch_cleanup=json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-I','-B','-c',script],timeout=15))
  else:scratch_cleanup={'scratch_absent':False,'reason':'Physical child/input-handle settlement unproven; existing fixture container teardown required, no live scratch deletion'}
 proof={'passed':error is None and result is not None and result.get('passed')is True and scratch_cleanup['scratch_absent'],'delegate_error':error,'baseline_image':image,'baseline_CID':cid,'existing_role_only':True,'new_roles_or_containers_or_coordinators_or_global_locks':0,'Source6_calibration_sha256':sha(HERE/'Source6-calibration.json'),'calibrated_guest_bytes_exact':True,'entry_sha256':sha(HERE/'evaluate.py'),'actual_result':result,'actual_result_file':str(evidence/'result.json'),'actual_result_sha256':sha(evidence/'result.json')if result is not None else None,'full_transcript_files':{name:{'path':str(evidence/(name+'.framehash')),'sha256':sha(evidence/(name+'.framehash')),'bytes':(evidence/(name+'.framehash')).stat().st_size}for name in('reference','actual')if(evidence/(name+'.framehash')).exists()},'scratch_cleanup':scratch_cleanup,'application_or_Comskip_detector_or_booking_calls':0,'container_SHA_equivalence_claimed':False,'performance_claimed':False}
 (root/'proof.json').write_text(json.dumps(proof,indent=2)+'\n');(root/'proof.json').chmod(0o644);assert proof['passed'],'OwnInputCUTContentOrTimeMismatch:'+str(root/'proof.json');return proof
