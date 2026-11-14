"""Retained physical-only two-case acceptor; no new sockets or app execution."""
import gzip,hashlib,importlib.util,json,sys,tempfile
from pathlib import Path
import verify as V
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'portable'))
from validate_physical import validate_physical
need=V.need;H=V.H
def materialize(mode,output):
 need(H((ROOT/'physical-policy.json').read_bytes())=='afefd88ef92ca2997492711abe17d927e284de3cec28e484e1e4c56dbcc5eff9','ExactPhysicalPolicy');policy=json.loads((ROOT/'physical-policy.json').read_bytes());need(mode in policy['epochs'],'FinitePhysicalEpoch');records=policy['epochs'][mode]['files'];output=Path(output)
 for name,row in records.items():
  need(not Path(name).is_absolute()and'..'not in Path(name).parts,'SafeExactEvidencePath');compressed=(ROOT/row['file']).read_bytes();need(H(compressed)==row['selected_compressed_sha256'],'ExactPhysicalCompressed');raw=gzip.decompress(compressed);need(H(raw)==row['selected_decompressed_sha256']and len(raw)==row['selected_decompressed_bytes'],'ExactPhysicalReference');p=output/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
 return policy
def validate():
 results={}
 with tempfile.TemporaryDirectory(prefix='unsigned-physical-offline-')as tmp:
  for mode in ['headers-abort','body-deadline']:
   out=Path(tmp)/mode;materialize(mode,out);v=validate_physical(out,mode);need(v==json.loads((out/'physical-validation.json').read_bytes()),'ExactExecutedPhysicalAcceptorRecomputed');e=json.loads((out/'execution-report.json').read_bytes());need(e['actual_controller_exit']==e['actual_ELF_exit']==0 and e['passed']is True and e['actual_business_requests']==1 and e.get('error')is None and not e.get('cleanup_errors'),'ActualHostAndELF0');need((out/'default-before.json').read_bytes()==(out/'default-after.json').read_bytes(),'LiteralDefaultFull6259And11');need(json.loads((out/'owned-backends-after-ELF.json').read_bytes())==[],'AfterELFBackendAbsence');rows=e['processes'];need(len(rows)==42==e['actual_command_count']and 0<=e['cleanup_elapsed_seconds']<=90,'ActualFiniteCleanup')
   for i,row in enumerate(rows,1):need(json.loads((out/f'command-{i:03d}.json').read_bytes())==row and type(row['actual_exit'])is int and row['actual_exit']==0 and row['invalid']is None and row['physically_settled']is True and row['readers_settled']is True and row['writer_settled']is True,'ActualEveryWaitAndPipes0')
   results[mode]=v
 return {'passed_limited_physical2':True,'actual_cases':2,'source_inputs':882,'normal25_different_source881':True,'unqualified':'frontend FDloss/fate, SQL ErrorReady, four newfetch frames/fifth capacity, all duplicated socket descriptors/global TCP retirement, Main/fullM8/RSS/performance','results':results}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
