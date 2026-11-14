"""Bounded exact-project recovery and independent absence proof; no app starts."""
import importlib.util,json,os,re,sys
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr');out=Path(sys.argv[1]).absolute();assert out.is_relative_to(ROOT/'artifacts/rust')and not any(p.is_symlink()for p in [out,*out.parents]);report=json.loads((out/'report.json').read_bytes());project=report['project'];assert re.fullmatch(r'dispatcharr-native-startup-[a-f0-9]{10}',project)
p=ROOT/'artifacts/rust/m4-m7-m8-wide-batch-1/prerequisites/assets/bounded_cli.py';s=importlib.util.spec_from_file_location('bounded',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);rows=[]
def command(argv):
 raw,err,r=m.collect(['docker','--host','unix:///tmp/dispatcharr-docker.sock',*argv],20,65536);rows.append(r);i=len(rows);(out/f'independent-cleanup-{i}.stdout').write_bytes(raw);(out/f'independent-cleanup-{i}.stderr').write_bytes(err);assert r['actual_wait_exit']==r['actual_poll_exit']==0 and r['invalid']is None and r['both_actual_EOF_observed']and not r['forced_termination'];return raw
# If the normal controller was interrupted, recover only its exact generated
# Compose project. A successful controller needs read-only verification only.
if report.get('exact_cleanup')is not True:command(['compose','-p',project,'-f',str(out/'compose.json'),'down','--volumes','--remove-orphans','--timeout','5'])
for kind in ['container','network','volume']:
 args=[kind,'ls']+(['--all','--no-trunc']if kind=='container'else[])+['--filter','label=com.docker.compose.project='+project,'--format','{{.Name}}'if kind=='volume'else'{{.ID}}'];assert not command(args).strip()
cids=[]
for name in ['actual-container.json','actual-Main.json']:
 if not(out/name).is_file():continue
 c=json.loads((out/name).read_bytes());assert c['Config']['Labels']['com.docker.compose.project']==project and re.fullmatch('[a-f0-9]{64}',c['Id']);cids.append(c['Id']);assert not command(['container','ls','--all','--no-trunc','--filter','id='+c['Id'],'--format','{{.ID}}']).strip()
known=[];namespace=os.readlink('/proc/self/ns/pid')
for r in report['commands']+rows:
 assert r['PID_namespace']==namespace;start=r['observed_starttime'];assert isinstance(start,str)and start.isdecimal()and int(start)>0
 try:current=Path('/proc',str(r['pid']),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:current=None
 known.append({'pid':r['pid'],'generation':start,'current':current,'absent':current!=start})
result={'passed':all(x['absent']for x in known),'fresh_queries':3+len(cids),'known_host_generations':known,'receipts':rows};(out/'independent-cleanup.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'passed':result['passed'],'fresh_queries':result['fresh_queries'],'host_generations_absent':len(known)}));assert result['passed']
