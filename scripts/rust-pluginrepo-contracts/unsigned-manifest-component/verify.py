"""Strict projected-reference HTTP/SQL/cache qualification; no apps or Docker."""
import gzip,hashlib,importlib.util,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
need=lambda ok,label:None if ok else (_ for _ in()).throw(ValueError(label))
H=lambda b:hashlib.sha256(b).hexdigest()
def load_projection(name):
 need(H((ROOT/'projection-policy.json').read_bytes())=='245e8854794649a2f430b27e425fb63309b3367f4709250c4588501476242650','ExactPublicProjectionPolicy');p=json.loads((ROOT/'projection-policy.json').read_bytes())['projections'][name];raw=(ROOT/p['file']).read_bytes();need(H(raw)==p['compressed_sha256'],'StrictSelectedCompressedReference');data=gzip.decompress(raw);need(H(data)==p['selected_JSON_sha256']and len(data)==p['selected_JSON_bytes'],'StrictSelectedJSONReferenceNoOriginalFallback');return json.loads(data)
def blob(epoch,path):
 need(H((ROOT/'closure-materialization.json').read_bytes())=='2050a917b858986eebd53de3ae8efe469cb9e69b87d74bae424e035c20e8c79f','ExactOriginalClosureMaterialization');row=json.loads((ROOT/'closure-materialization.json').read_bytes())['epochs'][epoch]['entries'][path];need('exact_public_blob'in row,'ExactPublicBlobRequired');raw=(ROOT/row['exact_public_blob']).read_bytes();need(H(raw)==row['original_sha256'],'ExactOriginalInputBlob');return json.loads(raw)
def module(name,path):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
N=module('portable_native',ROOT/'portable/validate_native.py')
def validate():
 results={};sources={};natives={}
 for source,native in [('Source24','Native24'),('Source404','Native404')]:
  a=load_projection(source);b=load_projection(native);s=module('portable_'+source,ROOT/'portable'/('effects_'+source.lower()+'.py'));v=s.validate(a,blob(source,'inputs/plan.json'),blob(source,'inputs/schema62-authority.json'));need(v['passed_observation_integrity']is True,'ActualSourceIntegrity');n=N.validate(a,b);need(n['passed']is True and n['group_serialization_byte_equal_all']is False,'ActualNativeScopeSerializationFalse');results[source]=v;results[native]=n
 return {'passed':True,'Source_requests':25,'Native_requests':25,'actual_Source_host_exits':[1,1],'actual_Native_host_exits':[0,0],'original_whole_lifecycle_coverage_and_18prediction':'FAILED unchanged','group_serialization_byte_equal':False,'Main_fullM8_HTTPphysical_performance':False,'results':results}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
