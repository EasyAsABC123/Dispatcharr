#!/usr/bin/env python3
"""Capture actual original third-party regex preview/URL-transform contracts.

Full original Dispatcharr authorship remains upstream; no native preview or
builtin-re equivalence is claimed by this source-only corpus.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from shared_ts import BASELINE,IMAGE

def corpus():
 rows=[]
 def add(name,url,search,replace,role=10):rows.append({'case':name,'role':role,'input':{'type':'m3u_profile_test','url':url,'search':search,'replace':replace}})
 url='http://provider.invalid/live/user/pass/123.ts'
 common=[('', ''),('', 'x'),('live',''),('live','movie'),('absent','x'),('^http','https'),('(live)/(.*)','$1/changed/$2'),('(?<kind>live)','${kind}'),('(?<kind>live)','$<kind>'),('(?P<kind>live)',r'\g<kind>'),('live','$0'),('live',r'\g<0>'),('live','$01'),('live',r'\g<missing>'),('[','x'),('live','\\'),('(?<=/)live(?=/)','movie'),('(?<=https?://)provider','changed'),('(?i)LIVE','movie'),('(?V1)(?i)LIVE','movie'),(r'(?r)\w+','x')]
 for index,(search,replace) in enumerate(common):add('url-'+str(index),url,search,replace)
 patterns=[r'.',r'^',r'$',r'\b',r'\B',r'\w+',r'\W+',r'\d+',r'\s+',r'a*',r'a*?',r'(a+)+$',r'(a)\1',r'(a)?(?(1)b|c)',r'(?|([ab])|(cd))',r'(?V1)[[a-z]--[aeiou]]+',r'\p{Letter}+',r'\X',r'(?P<name>\w+)',r'(?<name>\w+)',r'(?P<paren>\((?:[^()]|(?&paren))*\))',r'(?:cat){e<=1}',r'(?iV1)strasse',r'(?w)\b\w+\b',r'(?a)\w+',r'(?s).+',r'(?m)^.+$']
 subjects=['aab cat cot cd','Straße STRASSE İstanbul ıiſKσςΣ','東京 é e\u0301 👩\u200d💻','a\nb\n','(a(b)c)','', '\ud800x\udfff']
 for index,search in enumerate(patterns):
  for subindex,subject in enumerate(subjects):add('feature-'+str(index)+'-'+str(subindex),subject,search,'X')
 for replacement in ['$1','$2','$10','$123',r'\1',r'\g<1>',r'\g<0>',r'\n',r'\q',r'\101','$<name>','\\$1','$$1']:
  add('replacement-'+str(len(rows)),'aba',r'(?P<name>a)(b)?',replacement)
 for value in [None,False,0,'',[],{},True,1,[1],{'x':1}]:
  for field in ['url','search','replace']:
   row={'type':'m3u_profile_test','url':'aba','search':'a','replace':'x'};row[field]=value
   rows.append({'case':'input-type-'+str(len(rows)),'input':row})
 for size in [8191,8192,8193]:add('url-limit-'+str(size),'a'*size,'a','x')
 for size in [255,256]:
  add('pattern-limit-'+str(size),'a'*300,'a'*size,'x');add('replacement-limit-'+str(size),'aaa','a','x'*size)
 add('unicode-character-limit','😀'*8193,'😀','X')
 add('surrogate-subject','\ud800x\udfff',r'.','Y')
 add('surrogate-pattern','\ud800x','\ud800','Y')
 add('timeout-nested-alternation','a'*8190+'!',r'(a|aa)+$','X')
 add('timeout-nested-quantifiers','a'*8190+'!',r'(a+)+$','X')
 add('standard-ignored',url,'live','movie',role=1);add('viewer-ignored',url,'live','movie',role=0)
 return rows

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--docker',default='docker');parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();docker=shutil.which(args.docker)
 if not docker:parser.error('Docker not found')
 out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
 if any(out.iterdir()):parser.error('output must be empty')
 for source,target in [('dispatcharr/consumers.py','consumers.py'),('apps/proxy/live_proxy/url_utils.py','url_utils.py')]:
  (out/target).write_bytes(subprocess.check_output(['git','show',BASELINE+':'+source],cwd=ROOT))
 shutil.copyfile(Path(__file__).with_name('preview_oracle.py'),out/'preview_oracle.py');shutil.copyfile(Path(__file__).resolve(),out/'captured-preview_source.py')
 request={'cases':corpus()};(out/'inputs.json').write_text(json.dumps(request,indent=2)+'\n')
 command=[docker,'run','--rm','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','-i','-v',str(out)+':/source:ro','--entrypoint','/dispatcharrpy/bin/python',IMAGE,'/source/preview_oracle.py']
 result=subprocess.run(command,input=json.dumps(request),capture_output=True,text=True,timeout=60)
 (out/'stderr.log').write_text(result.stderr)
 report={'scope':'actual original regex preview source-only behavior','passed':False,'native_preview_implemented':False,'application_performance_verified':False,'milestone_complete':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'exit_code':result.returncode,'command':command}
 if result.returncode==0:
  data=json.loads(result.stdout);(out/'source-preview.json').write_text(json.dumps(data,indent=2)+'\n')
  report.update({key:data[key] for key in ['case_count','python','regex_version','regex_default_version','regex_native_sha256','source_sha256','method_sha256']})
  assert all(not row['exception'] for row in data['cases'])
  assert all(not row['sent'] for row in data['cases'] if row['role']<10)
  report['passed']=True
 report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in [Path(__file__).resolve(),Path(__file__).with_name('preview_oracle.py')]}
 (out/'evidence.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));return 0 if report['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
