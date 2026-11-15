"""Read-only qualification of captured Native image7 TLS13, no app/Source replay.
Original Dispatcharr and dependency authors retain all application/algorithm credit.
"""
import argparse, hashlib, json
from pathlib import Path

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def qualify(directory):
 directory=Path(directory);r=json.loads((directory/'report.json').read_bytes());checks=[]
 def check(label,value):checks.append({'label':label,'passed':bool(value)})
 check('raw controller actual0',r['actual_controller_exit']==0)
 check('candidate fixed image7',r['candidate']['git']=='52e8ebc7e4ce8b92368ec94797d60c0e44678687' and r['candidate']['image']=='sha256:62e4fb5ab2b06f98a6fbf26301d1ab4b8f9cb4161f3a89427d7c64f9ab700cd0')
 check('actual13 distinct Main CIDs',len(r['native_cids'])==13 and len(set(r['native_cids']))==13)
 check('actual13 captured cells',len(r['cases'])==13 and all(v['state']=='captured' for v in r['cases']))
 definitions={v['name']:v for v in r['definition']['cases']};pg=0;expected=0
 for case in r['cases']:
  name=case['name'];spec=definitions[name];a=case['actual'];sessions=a['native_sessions'];ready=any(v['status']==200 for v in a['HTTP_observations'])
  physical=all(v['ssl'] is spec['expected_native_ssl'] and v['client_dn']==spec['expected_native_client_dn'] for v in sessions) and bool(sessions) if ready else not sessions
  if name=='unix_socket_ssl_true':physical &= all(v['client_addr'] is None for v in sessions)
  clean=case['actual_Main_removed'] and case['actual_Main_wait_exit']==spec['expected_Main_exit']
  match=ready==spec['expected_native_ready'] and physical and clean and case['sqlite_unchanged']
  for stream in ('stdout','stderr'):
   check(name+' actual '+stream+' byteshash',sha(directory/(name+'-Main.'+stream+'.log'))==case['Main_log_streams'][stream+'_sha256'])
  if spec['family']=='sqlite':
   log=(directory/(name+'-Main.stderr.log')).read_bytes()+(directory/(name+'-Main.stdout.log')).read_bytes()
   match &= b'SQLite backend is not yet implemented' in log and not sessions and not ready
   check('SQLite refusal is explicit gap not parity',r['counts']['SQLite_required_parity_matched'] is False)
  else:pg+=int(ready==spec['source_connected'] and physical and clean)
  expected+=int(match);check(name+' actual HTTP SQL wait unchangedSQLite',match)
 check('12 PostgreSQL cells matched Source',pg==12)
 check('13 expected Native behaviors',expected==13)
 check('default62tables59sequences RedisDB7 unchanged',(directory/'default-before.json').read_bytes()==(directory/'default-after.json').read_bytes() and r['default_unchanged'])
 check('17 independent absence inventories',len(r['independent_absence'])==17 and all(v['empty'] for v in r['independent_absence']))
 check('142 CLI generations actually absent',len(r['host_generations'])==142 and all(v['original_absent'] for v in r['host_generations']) and r['all_host_generations_absent'])
 check('actual exactcleanup',r['exact_cleanup'])
 check('no Source API or completed authority replay',all(r[k]==0 for k in ('Source_API_calls','Source_driver_calls','Source_application_imports','Source_replays')))
 check('no migration/performance overclaim',r['full_migration_accepted'] is False and r['performance_claims'] is False)
 return {'passed':all(v['passed'] for v in checks),'checks':checks,'PostgreSQL_Source_compatibility':{'matched':pg,'total':12},'expected_Native_behavior':{'matched':expected,'total':13},'SQLite_supported':False,'full_migration_accepted':False,'performance_claims':False,'report_sha256':sha(directory/'report.json'),'unmeasured':r['unmeasured_modes']}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('directory',type=Path);p.add_argument('--output',type=Path);a=p.parse_args();result=qualify(a.directory)
 if a.output:a.output.write_text(json.dumps(result,indent=2)+'\n')
 print(json.dumps(result));raise SystemExit(0 if result['passed'] else 1)
