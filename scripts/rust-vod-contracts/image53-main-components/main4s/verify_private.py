"""Optional explicit retained authority verification. Never print private authentication data."""
import argparse,hashlib,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def verify(raw):
 data=json.loads((HERE/'retained.json').read_text());raw=Path(raw)
 for name,expected in data['raw_sha256'].items():
  if sha(raw/name)!=expected:raise ValueError('ExactRawAuthority '+name)
 summary=json.loads((raw/'summary.json').read_text())
 if summary!=data['summary']or data['omitted_fields']!=[]:raise ValueError('CompleteExactSummaryProjection')
 redactions=[]
 for r in summary['cases']:
  case=r['case'];base=raw/case
  if json.loads((base/'report.json').read_text())!=r:raise ValueError('ExactCaseReport')
  seed=json.loads((base/'pending-main-private.json').read_text());redactions.append({'private_path':case+'/pending-main-private.json:access_token','replacement':'not published','field_sha256':hashlib.sha256(seed['access_token'].encode()).hexdigest(),'raw_file_sha256':sha(base/'pending-main-private.json')})
  if{k:v for k,v in seed.items()if k!='access_token'}!=data['seed_non_auth_projection'][case]:raise ValueError('SeedNonAuthProjection')
  for mode in ['source','native']:
   if json.loads((base/('pending-main-'+mode+'-'+case+'-private.json')).read_text())!=r[mode]:raise ValueError('ExactPhaseProjection')
  if[json.loads(line)for line in(base/'Source-request-signals.jsonl').read_text().splitlines()]!=data['Source_signals'][case]:raise ValueError('ExactSignalJournal')
  owned=[]
  for p in sorted(base.glob('owned-*.json')):
   v=json.loads(p.read_text());redactions.append({'private_path':case+'/'+p.name+':nonce','replacement':'nonce_sha256','field_sha256':hashlib.sha256(v['nonce'].encode()).hexdigest(),'raw_file_sha256':sha(p)});owned.append({'file':p.name,'pid':v['pid'],'starttime':v['starttime'],'role':v['role'],'nonce_sha256':hashlib.sha256(v['nonce'].encode()).hexdigest()})
  if owned!=data['owned_process_record_projection'][case]:raise ValueError('OwnedPIDProjection')
  token=seed['access_token'].encode()
  for p in HERE.rglob('*'):
   if p.is_file()and'__pycache__'not in p.parts and token in p.read_bytes():raise ValueError('PrivateTokenPublished')
 if redactions!=data['private_seed_or_process_field_redactions']:raise ValueError('ExactRedactionMap')
 return {'all_raw_hashes_and_full_summary_phase_signal_PID_projections_exact':True,'header_maps_retained':4,'private_auth_printed':False,'private_access_tokens_absent':True,'application_executed':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--raw',required=True,type=Path);args=p.parse_args();print(json.dumps(verify(args.raw),indent=2))
