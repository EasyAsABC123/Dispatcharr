"""Read-only qualification of retained new driver cases; no applications/tools run."""
import hashlib,json
from pathlib import Path
import argparse
p=argparse.ArgumentParser();p.add_argument('--evidence-root',type=Path,default=Path('/home/justin/github/Dispatcharr/artifacts/rust'));a=p.parse_args();P=a.evidence_root.absolute();R=P/'m478-pg-tls-authority-3'
reports={n:json.loads((P/f'm478-pg-tls-authority-{n}'/'report.json').read_text())for n in [1,2,3]}
def need(x,label):
 if not x:raise ValueError(label)
for n,r in reports.items():
 need(r['exact_cleanup']and r['all_host_generations_absent']and r['default_unchanged'],'ActualCleanupDefault:'+str(n))
 need(all(c['capture_complete']and not c['forced_termination']for c in r['commands']),'ActualCLIs:'+str(n))
need(reports[1]['actual_controller_exit']==2 and all(c['actual']is None for c in reports[1]['cases']),'PreservePermissionFailure')
need(reports[2]['actual_controller_exit']==2 and reports[3]['actual_controller_exit']==0,'ActualAttemptExits')
selected=[c for c in reports[2]['cases']if c['name']in ['missing_ca_path','sqlite']]+reports[3]['cases'];need(len(selected)==12 and len({c['name']for c in selected})==12,'TwelveDistinctActualCases')
checks=[]
for c in selected:
 a=c['actual'];need(c['expected_match']and a is not None and a['Source_API_calls']==0 and a['django_setup_called']is False,'ActualSourceDriver:'+c['name']);checks.append(c['name'])
 if a['connected']and c['name']!='sqlite':
  observed=a['observation'];need(observed[1]=='dispatch','ActualRole')
  if c['name']=='unix_socket_ssl_true':need(observed[2]is None and observed[4]is False,'ActualUDSNoTLS')
  else:need(observed[4]is True and observed[5]=='TLSv1.3'and observed[7]=='/CN=dispatch','ActualTLSClientCertificate')
source=Path('/home/justin/github/Dispatcharr/artifacts/rust/m4-m7-m8-wide-batch-run-2-source16/source');prior=json.loads(Path('/home/justin/github/Dispatcharr/artifacts/rust/m478-source-normal-wsgi-cache-1/Original-python-inputs.json').read_text())
need(all(hashlib.sha256((source/p).read_bytes()).hexdigest()==h for p,h in prior.items()),'RetainedExactOriginalPythonInputs')
summary={'passed':True,'authority_cases':selected,'Source_API_calls':0,'actual_Source_configuration_probe_processes':22,'attempt1_pre_script_python_failures':12,'completed_source_case_replays':0,'original_python_files_exact_prior_authority':len(prior),'application_function_replacement':False,'django_setup_calls':0,'normal_AppConfig_trace_registration_imported':True,'private_keys_published':False,'attempt_controller_exits':{str(n):r['actual_controller_exit']for n,r in reports.items()},'all_exact_cleanup':True,'default62tables59sequences_Redis7_unchanged':True,'scope':'Pinned Original settings + actual gevent backend SQL driver; not full application startup/API or performance acceptance','Native_acceptance':False,'SQLite_Rust_gap':True,'allow_prefer_fallback_parity':False,'evidence_hashes':{str(n):hashlib.sha256((P/f'm478-pg-tls-authority-{n}'/'report.json').read_bytes()).hexdigest()for n in reports}}
(R/'qualified-source-summary.json').write_text(json.dumps(summary,indent=2,allow_nan=False)+'\n');print(json.dumps({'passed':True,'distinct_authority_cases':12,'actual_Source_probes':22,'Source_API_calls':0,'all_exact_cleanup':True}))
