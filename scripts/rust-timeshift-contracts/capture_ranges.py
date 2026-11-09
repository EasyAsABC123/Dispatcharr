"""Execute immutable pure range helpers from the original Dispatcharr AST.
All original application credit belongs to upstream authors and contributors.
"""
import ast
import hashlib
import itertools
import json
from pathlib import Path
from types import SimpleNamespace
source=Path('/app/apps/timeshift/views.py').read_text()
names=['_parse_client_range','_parse_content_range_header','_extract_representation_length','_build_downstream_length_headers']
parsed=ast.parse(source)
functions=[node for node in parsed.body if isinstance(node,ast.FunctionDef) and node.name in names]
if len(functions)!=len(names):raise RuntimeError('immutable helper selection mismatch')
namespace={}
exec(compile(ast.Module(body=functions,type_ignores=[]),'<immutable-source-pure-range-helpers>','exec'),namespace)
cases=[]
for value in [None,'','bytes=0-','bytes=7-100','bytes=-256','bytes=9000-','bytes=+4-+9','bytes= 4 - 9 ','bytes=5-2','bytes=bad-','bytes=1-2,3-4','Bytes=1-2','bytes=1_0-2_0','bytes=1--2']:
 cases.append({'function':'parse_client_range','input':value,'output':namespace['_parse_client_range'](value)})
for value in [None,'','bytes 7-100/500','bytes 7-100/*','bytes */500','bytes -100/500','bytes 7-/500','bytes bad-9/500','bytes 9-7/500','bytes 0-9/0']:
 cases.append({'function':'parse_content_range','input':value,'output':namespace['_parse_content_range_header'](value)})
for cr,length in [(None,None),(None,'500'),(None,'bad'),('bytes 7-100/500','94'),('bytes 7-100/*','94'),('bytes */500','0')]:
 cases.append({'function':'representation_length','input':[cr,length],'output':namespace['_extract_representation_length'](SimpleNamespace(headers={'Content-Range':cr,'Content-Length':length}))})
for range_header,representation,status,streaming,upstream in itertools.product([None,'','bytes=7-100','bytes=-256','bytes=490-','bytes=bad-'],[None,0,500],[200,206],[False,True],[(None,None),('bytes 7-100/500','94'),('bytes 7-100/*',None)]):
 args=dict(range_header=range_header,representation_length=representation,status_code=status,streaming=streaming,upstream_content_range=upstream[0],upstream_content_length=upstream[1])
 cases.append({'function':'downstream_headers','input':args,'output':namespace['_build_downstream_length_headers'](**args)})
print(json.dumps({'format_version':1,'scope':'immutable original pure Timeshift Range and length-header helpers','capture_complete':True,'source_sha256':{'apps/timeshift/views.py':hashlib.sha256(source.encode()).hexdigest()},'vector_count':len(cases),'cases':cases},indent=2))
