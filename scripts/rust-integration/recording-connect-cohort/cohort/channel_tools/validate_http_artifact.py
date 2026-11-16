"""Pure retained-data predicate controls; not runtime or Source task evidence."""
import ast,base64,copy,importlib.util,json,uuid
from pathlib import Path
HERE=Path(__file__).resolve().parent
import argparse
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source-report',type=Path,required=True);args=p.parse_args()
spec=importlib.util.spec_from_file_location('channel_http_predicates',HERE/'http_cases.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
source=json.loads(args.source_report.read_bytes());checks=[]
def check(name,value):checks.append({'name':name,'passed':bool(value)})
old=source['seed_ids'];new=dict(channel=101,stream=202,source=303,epg=404);inspection={'Config':{'Hostname':'owned-native-control'}}
for row in source['cases']:
 body=m.rebind_body(row['definition']['body'],old,new)
 if isinstance(row['definition']['body'],dict):
  for key in ['starting_channel_number','channel_profile_ids']:
   check(row['definition']['name']+'-preserve-nonselector-'+key,body.get(key)==row['definition']['body'].get(key))
 for ref in row['new_family_frames']:
  f=copy.deepcopy(ref['frame']);ident=str(uuid.uuid4());wanted=m.expected_body(ref,old,new,row['definition']['route']);f['body']=base64.b64encode(json.dumps(wanted).encode()).decode();f['headers'].update(id=ident,root_id=ident,origin='gen1@owned-native-control',argsrepr=repr(tuple(wanted[0])),kwargsrepr=repr(wanted[1]));f['properties'].update(correlation_id=ident,reply_to=str(uuid.uuid3(uuid.NAMESPACE_OID,'opaque-control')),delivery_tag=str(uuid.uuid4()))
  representation=dict(argsrepr=repr(tuple(wanted[0])),kwargsrepr=repr(wanted[1]));review=m.frame_review({'frame':f},ref,old,new,row['definition']['route'],inspection,representation);check(row['definition']['name']+'-explicit-binding-only',review['passed'])
  bad=copy.deepcopy(f);bad['headers']['ignore_result']=True;check(row['definition']['name']+'-reject-header-policy-drift',not m.frame_review({'frame':bad},ref,old,new,row['definition']['route'],inspection,representation)['passed'])
  bad=copy.deepcopy(f);b=json.loads(base64.b64decode(bad['body']));b[2]['chain']=['invented'];bad['body']=base64.b64encode(json.dumps(b).encode()).decode();check(row['definition']['name']+'-reject-body-callback-drift',not m.frame_review({'frame':bad},ref,old,new,row['definition']['route'],inspection,representation)['passed'])
for p in HERE.glob('*.py'):ast.parse(p.read_text())
# Compile the actual dynamically-generated observer control program without any
# processes or IO, catching literal-backslash newline mistakes before dispatch.
module=ast.parse((HERE/'callback.py').read_text());run=next(n for n in module.body if isinstance(n,ast.FunctionDef)and n.name=='run');control=next(n for n in run.body if isinstance(n,ast.FunctionDef)and n.name=='control');ns={'uuid':uuid,'json':json,'case_path':'/owned/control','fixture':type('F',(),{'remaining':lambda self,n:n,'python':lambda self,script:(compile(script,'actual-control-program','exec')and '{}')})()};exec(compile(ast.fix_missing_locations(ast.Module(body=[control],type_ignores=[])),'control-builder','exec'),ns);ns['control']('frame_mark');check('actual-generated-control-program-compiles',True)
result={'passed':all(c['passed']for c in checks),'checks':checks,'scope':'Artifact-only Source98 producer rebind/frame false-acceptance controls and generated-program syntax; not an application or Source task-body run','application_runs':0,'Cargo':0,'Docker_builds':0};print(json.dumps(result,indent=2));raise SystemExit(0 if result['passed']else 1)
