"""Read-only Git verification of pinned context excerpts; no mutable fallback."""
import argparse,hashlib,json,subprocess
from pathlib import Path
HERE=Path(__file__).resolve().parent
def verify(repo):
 checks=[]
 for name in ['pinned-Original-context.json','pinned-native-context.json']:
  record=json.loads((HERE/name).read_text());path=record.get('path','rust/crates/vod-http/src/engine.rs');revision=record['revision'];data=subprocess.check_output(['git','-C',str(repo),'show',revision+':'+path]);digest=hashlib.sha256(data).hexdigest();expected=record.get('sha256',record.get('engine_sha256'))
  if digest!=expected:raise ValueError('ExactPinnedCode')
  blob=subprocess.check_output(['git','-C',str(repo),'rev-parse',revision+':'+path],text=True).strip()
  if blob!=record['git_blob']:raise ValueError('ExactGitBlob')
  lines=data.decode().splitlines()
  for part in record.get('excerpts',[record.get('excerpt')]):
   if '\n'.join(lines[part['start_line']-1:part['end_line']])!=part['text']:raise ValueError('ExactExcerpt')
  checks.append({'context':name,'git_blob':blob,'sha256':digest})
 return {'exact_immutable_code_contexts':checks,'runtime_application_executed':False,'dynamic_request_association_inferred':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);a=p.parse_args();print(json.dumps(verify(a.repo),indent=2))
