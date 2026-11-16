"""Artifact-only false-acceptance controls; no application, Docker or Source run."""
import ast,copy,hashlib,importlib.util,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
import argparse
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source-report',type=Path,required=True);p.add_argument('--dataset',type=Path,required=True);args=p.parse_args()
s=importlib.util.spec_from_file_location('effects',HERE/'effects.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
source=json.loads(args.source_report.read_text());dataset=json.loads(args.dataset.read_text());refs={r['name']:r for r in dataset['cases']};checks=[]
def check(name,v):checks.append({'name':name,'passed':bool(v)})
for p in HERE.glob('*.py'):ast.parse(p.read_text())
check('allPythonAST',True)
seed={'channel_ids':list(range(1,7)),'epg_id':1,'stream_id':1};bindings={'seed_channel_pairs':list(zip(range(1,7),range(1,7))),'epgs':{1:1}}
for case in source['cases']:
 def tables(raw):return {k:[json.loads(v)if isinstance(v,str)else v for v in vs]for k,vs in raw.items()}
 row={'before':{'tables':tables(case['before'])},'after':{'tables':tables(case['after'])}}
 ref=refs[case['name']];review=m.compare(row,ref,seed,bindings);check(case['name']+'-retainedSource-delta-self-consistency',review['passed'])
 corrupt=copy.deepcopy(row);corrupt['after']['tables'][m.CHANNEL][0]['user_level']=999
 bad=m.compare(corrupt,ref,seed,copy.deepcopy(bindings));check(case['name']+'-reject-unrelated-row-field-change',not bad['passed'])
 if case['name']=='normal_logos':
  corrupt=copy.deepcopy(row);corrupt['after']['tables'][m.LOGO][0]['name']='unqualified renamed logo';check('reject-logo-field-change-despite-same-count',not m.compare(corrupt,ref,seed,copy.deepcopy(bindings))['passed'])
result={'passed':all(c['passed']for c in checks),'checks':checks,'scope':'Artifact-only AST +row-effect validator false-acceptance controls on retained Source snapshots; no Native/Source runtime parity','Cargo':0,'Docker_builds':0,'application_runs':0};print(json.dumps(result,indent=2));raise SystemExit(0 if result['passed']else 1)
