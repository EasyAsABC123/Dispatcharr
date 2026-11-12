import ast,hashlib,json,math,struct,sys
from pathlib import Path
raw=Path('/original-app-data.py').read_bytes();tree=ast.parse(raw);names={'AppDataError','json_byte_size'};nodes=[n for n in tree.body if isinstance(n,(ast.ClassDef,ast.FunctionDef))and n.name in names];assert len(nodes)==2
scope={'json':json,'CODE_VALUE_NOT_SERIALIZABLE':'value_not_serializable'};exec(compile(ast.Module(body=nodes,type_ignores=[]),'/original-app-data.py','exec'),scope)
r=json.load(sys.stdin);rows=[]
for bits in r['bits']:
 v=struct.unpack('>d',bytes.fromhex(bits))[0];assert math.isfinite(v);payload={'float':v,'label':'é𝄞'};compact=json.dumps(payload,ensure_ascii=False,allow_nan=False,separators=(',',':'));size=scope['json_byte_size'](payload);assert size==len(compact.encode())
 rows.append({'bits':bits,'repr':repr(v),'repr_length':len(repr(v)),'compact':compact,'compact_size':size})
print(json.dumps({'passed':True,'scope':'Actual pinned Original extracted json_byte_size + finite repr/UTF8 JSON only; no module/endpoint/ORM execution','python':sys.version,'original_module_sha256':hashlib.sha256(raw).hexdigest(),'rows':rows}))
