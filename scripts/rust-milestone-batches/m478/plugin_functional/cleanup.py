"""Fresh read-only owned-resource/PID verification; never starts an application."""
import argparse,importlib.util,json,os,re
from pathlib import Path
HERE=Path(__file__).resolve().parent;REPO=HERE.parents[3]
def need(value,label):
    if not value:raise ValueError(label)
def verify(output):
    report=json.loads((output/'report.json').read_text());project=report['project'];need(re.fullmatch('dispatcharr-plugin-functional-[a-f0-9]{10}',project),'exact owned project')
    path=REPO/'artifacts/rust/m4-m7-m8-wide-batch-1/prerequisites/assets/bounded_cli.py';spec=importlib.util.spec_from_file_location('bounded_cleanup',path);bounded=importlib.util.module_from_spec(spec);spec.loader.exec_module(bounded)
    rows=[]
    for kind in ['container','network','volume']:
        argv=['docker','--host','unix:///tmp/dispatcharr-docker.sock',kind,'ls']+(['--all','--no-trunc'] if kind=='container' else [])+['--filter','label=com.docker.compose.project='+project,'--format','{{.Name}}' if kind=='volume' else '{{.ID}}']
        raw,err,row=bounded.collect(argv,15,65536);rows.append(row);(output/('independent-'+kind+'.stdout')).write_bytes(raw);(output/('independent-'+kind+'.stderr')).write_bytes(err)
        need(row['actual_wait_exit']==row['actual_poll_exit']==0 and row['invalid'] is None and row['both_actual_EOF_observed'] is True and not raw.strip(),'fresh exact owned '+kind+' absence')
    cids=[]
    for file in [output/'service-inspect.json',*output.glob('*-Main-inspect.json')]:
        if not file.is_file():continue
        saved=json.loads(file.read_text());saved=saved if type(saved) is list else [saved]
        for container in saved:
            need(container['Config']['Labels'].get('com.docker.compose.project')==project and re.fullmatch('[a-f0-9]{64}',container['Id']),'actual saved owned CID');cids.append(container['Id'])
    for cid in cids:
        raw,err,row=bounded.collect(['docker','--host','unix:///tmp/dispatcharr-docker.sock','container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}'],15,65536);rows.append(row)
        need(row['actual_wait_exit']==row['actual_poll_exit']==0 and row['invalid'] is None and row['both_actual_EOF_observed'] is True and not raw.strip(),'fresh actual owned CID absence')
    namespace=os.readlink('/proc/self/ns/pid');known=report['commands']+([report['loopback_host_CLI']] if report.get('loopback_host_CLI') else [])
    if report.get('emergency_stop'):
        emergency=report['emergency_stop'];need(emergency.get('physically_settled') is True and emergency.get('pipes_settled') is True,'actual emergency-stop pipe settlement')
        known.append({'pid':emergency['pid'],'observed_starttime':emergency['starttime'],'PID_namespace':emergency['proc_PID_namespace']})
    for row in known+rows:
        need(row['PID_namespace']==namespace,'same actual host PID namespace')
        pid=row['pid'];start=row.get('observed_starttime',row.get('generation'));need(type(pid) is int and pid>0 and type(start) is str and start.isdecimal() and int(start)>0,'actual positive host generation')
        try:current=Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
        except FileNotFoundError:current=None
        need(current!=start,'actual known host generation absent')
    result={'exact_owned_absence':True,'PID_namespace_checked':True,'host_generations_absent':len(known)+len(rows),'fresh_project_queries':3,'fresh_CID_queries':len(cids),'normal_cleanup_report_valid':report.get('cleanup_verified') is True,'Source_API_calls':0,'Main_adversarial_handler_drain_qualified':False}
    (output/'independent-cleanup-receipts.json').write_text(json.dumps(rows,indent=2)+'\n');(output/'independent-cleanup.json').write_text(json.dumps(result,indent=2)+'\n');return result
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    try:print(json.dumps(verify(args.output)))
    except Exception as error:print(json.dumps({'exact_owned_absence':False,'error':{'type':type(error).__name__,'message':str(error)}}));raise SystemExit(2)
