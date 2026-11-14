"""One Source reference cohort for the milestone batch, not per-helper admission.
Called only by Root's global dispatcher with its existing global job context.
"""
import argparse,hashlib,json,re,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent

def validate_inputs():
 mapping=json.loads((HERE/'inputs.json').read_text())
 for name,digest in mapping.items():
  p=HERE/name
  if not p.is_file()or p.is_symlink()or hashlib.sha256(p.read_bytes()).hexdigest()!=digest:raise ValueError('BatchInput:'+name)
 return mapping

def run(context):
 if context['batch']!='M2-M3-M5-wide-1'or context['node_id']!='source/cache-and-profile-contexts':raise ValueError('GlobalBatchNodeIdentity')
 if not re.fullmatch('[a-f0-9]{10}',context['nonce']):raise ValueError('GlobalOwnedNamespace')
 validate_inputs();repo=Path(context['repo']).resolve()
 if Path.cwd().resolve()!=repo:raise ValueError('ExactBoundRepoCwd')
 output=Path(context['output']).resolve()/'application'
 if output.exists()or output.is_symlink():raise ValueError('FreshApplicationOutputNoAdaptiveRetry')
 sys.path.insert(0,str(HERE/'source_workflows'))
 import run as source
 source.REPO=repo;source.ARCHIVE=repo/'artifacts/rust/m5-core-settings-source-executor-2/inputs/original-source.tar';source.OUTPUT=output
 plan=source.selfcheck();output.mkdir(mode=0o700)
 result=source.execute(plan,{'nonce':context['nonce']})
 if (output/'ts-cache-source-private.json').exists()and(output/'profile-arguments-owner-private.json').exists():
  from qualify_source import qualify
  qualification=qualify(output);(output/'Source-domain-qualification.json').write_text(json.dumps(qualification,indent=2)+'\n')
  if not qualification['capture_and_domain_valid']:result=1
 return {'actual_exit':result,'app_output':str(output),'scope':'Newcold/2.4sreply/expiryclass context plusStreamProfile18+owner6; no profileprocess/media/Main authority'}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--context',type=Path,required=True);a=p.parse_args();result=run(json.loads(a.context.read_text()));print(json.dumps(result));raise SystemExit(result['actual_exit'])
