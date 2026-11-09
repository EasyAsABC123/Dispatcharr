"""Execute immutable original pure busy Range controls, with upstream credit."""
import ast
import hashlib
import itertools
import json
from pathlib import Path

source=Path('/app/apps/timeshift/views.py').read_text()
stats=Path('/app/apps/timeshift/stats.py').read_text()
names=['_parse_client_range','_parse_range_start','_is_near_eof_probe',
       '_should_displace_busy_playback','_should_displace_busy_pool',
       '_is_full_restart_range','_should_preempt_plain_reconnect','_cap_open_ended_range']
tree=ast.parse(source)
selected=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in names]
if len(selected)!=len(names):raise RuntimeError('immutable busy helper selection mismatch')
def constant(tree,name):
    for node in tree.body:
        if isinstance(node,ast.Assign) and any(isinstance(target,ast.Name) and target.id==name for target in node.targets):
            return ast.literal_eval(node.value)
    raise RuntimeError('source constant missing')
namespace={'_EOF_PROBE_TAIL_BYTES':constant(ast.parse(stats),'EOF_PROBE_TAIL_BYTES'),
           '_EOF_PROBE_UNKNOWN_LENGTH_MIN':constant(tree,'_EOF_PROBE_UNKNOWN_LENGTH_MIN')}
exec(compile(ast.Module(body=selected,type_ignores=[]),'<immutable-source-busy-controls>','exec'),namespace)
cases=[]
ranges=[None,'','bytes=0-','bytes=0-255','bytes=7-','bytes=7-100','bytes=-256','bytes=14999900-',
        'bytes=100000000-','bytes=bad-','bytes=1-2,3-4','Bytes=0-','bytes= 4 - 9 ','bytes=5-2']
for header,length,serving in itertools.product(ranges,[None,0,1000,15001836], [None,'none','start','range']):
    args={'range_header':header,'content_length':length,'busy_serving_range':serving}
    cases.append({'function':'displace','input':args,'output':namespace['_should_displace_busy_playback'](**args)})
for header,exists,busy,media in itertools.product(ranges,[False,True],[False,True],[None,'same','different']):
    args={'range_header':header,'pool_exists':exists,'pool_busy':busy,'pool_media_id':media,'media_id':'same'}
    cases.append({'function':'preempt_plain','input':args,'output':namespace['_should_preempt_plain_reconnect'](**args)})
for header,span in itertools.product(ranges,[None,0,1,1880000]):
    cases.append({'function':'cap_open_range','input':[header,span],'output':namespace['_cap_open_ended_range'](header,span)})
print(json.dumps({'format_version':1,'scope':'immutable original pure busy archive Range controls',
    'capture_complete':True,'vector_count':len(cases),'cases':cases,
    'source_sha256':{'apps/timeshift/views.py':hashlib.sha256(source.encode()).hexdigest(),
                     'apps/timeshift/stats.py':hashlib.sha256(stats.encode()).hexdigest()}},indent=2))
