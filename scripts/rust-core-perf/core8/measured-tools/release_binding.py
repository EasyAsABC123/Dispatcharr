"""Data-only optimized ELF/source identity validation before any qualification."""
import hashlib
from .policy import digest
BASE='45ea3ad2563b7db8d9221c09993944bf71b985ab'
CORE='f16a8b1317452a9a40cee8dc1f7f75d2149a70ea47643f831cb1088be6afb60e'
def validate(record,binary):
 if record.get('passed')is not True or record.get('production_base')!=BASE or record.get('production_map')!=CORE:raise ValueError('Exact immutable Core8 production base required')
 if hashlib.sha256(binary.read_bytes()).hexdigest()!=record['binary_sha256']:raise ValueError('Optimized ELF binding changed')
 profile=record['profile']
 if profile['opt_level']!='3' or profile['debuginfo']!=0 or profile['debug_assertions']is not False or profile['overflow_checks']is not False or profile['test']is not True:raise ValueError('Measured optimized test profile required')
 inputs=record['source_map']
 if not inputs or any(not k.startswith('rust/')or '..'in k.split('/')or len(v)!=64 or any(c not in '0123456789abcdef'for c in v)for k,v in inputs.items()):raise ValueError('Complete regular Rust source map required')
 if digest(inputs)!=record['source_map_canonical_sha256']:raise ValueError('Source map seal changed')
 return {'production_base_commit':BASE,'production_source_map_canonical_sha256':CORE,'native_source_map_canonical_sha256':record['source_map_canonical_sha256']}
