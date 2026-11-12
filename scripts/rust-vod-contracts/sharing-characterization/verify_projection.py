"""Optional exact private-source/retained-media audit; explicit paths only."""
import argparse,copy,hashlib,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def verify(summary,source,native,seed,media=None):
 data=json.loads((HERE/'retained.json').read_text());require=lambda ok,label:None if ok else(_ for _ in()).throw(ValueError(label))
 require(digest(summary)==data['raw_summary_sha256'],'ExactPrivateSummary');original=json.loads(summary.read_text());projected=copy.deepcopy(original)
 for phase in projected['backends'].values():
  for c in phase['clients']:c.pop('headers',None)
 require(projected==data['summary'],'ExactFieldProjection')
 for mode,path in [('source',source),('native',native)]:require(digest(path)==data['raw_phase_sha256'][mode]and json.loads(path.read_text())==original['backends'][mode],'ExactPrivatePhase '+mode)
 require(digest(seed)==data['seed_sha256'],'ExactPrivateSeed');raw=json.loads(seed.read_text());require({k:raw[k]for k in data['seed_non_auth_projection']}==data['seed_non_auth_projection'],'IndependentSeedProjection')
 if media is not None:
  for mode in ['source','native']:
   for role in ['first','second']:
    p=media/(mode+'-'+role+'.mp4');require({'bytes':p.stat().st_size,'sha256':digest(p)}==data['retained_files'][mode][role],'ActualRetainedMedia')
 return {'exact_summary_phase_and_seed_projection':True,'retained_media_hashes_verified':media is not None,'private_auth_printed':False,'application_executed':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__)
 for n in ['summary','source','native','seed']:p.add_argument('--'+n,type=Path,required=True)
 p.add_argument('--media',type=Path);a=p.parse_args();print(json.dumps(verify(a.summary,a.source,a.native,a.seed,a.media),indent=2))
