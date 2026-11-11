"""Core8 publication schema/identities, no metrics or workloads.
Original Dispatcharr authors retain application/algorithm credit/licenses.
"""
import hashlib
import json
from pathlib import Path
BASE='45ea3ad2563b7db8d9221c09993944bf71b985ab'
PRODUCTION='f16a8b1317452a9a40cee8dc1f7f75d2149a70ea47643f831cb1088be6afb60e'
BENCHMARK='d27a4c69776f27682700cae01156b0faced7f2e6a5827acd961304c9ca1e35c6'
ELF='32ddd73d70d6198a41f6fc67babc0d837cc71d2e448885e7f0aaed471b1dd947'
RELEASE='7fca498d8913b17a5c4261cb4dad0bda5c2535b07cbe4fc1a7b9a5ed3ec0dd20'
POLICY='330058287fc2b47020b754ccafccce07f114c1ae9982756b95dae3ee7d304cf3'
OVERLAYS={'rust/crates/stream-http/src/engine.rs','rust/crates/stream-http/src/core_perf_peer.rs'}
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def require(value,message):
    if not value:raise ValueError(message)
def identity(ready,release):
    require(ready.get('production_base_commit')==release.get('production_base')==BASE,'Production base changed')
    require(ready.get('production_source_map_canonical_sha256')==release.get('production_map')==PRODUCTION,'Production map changed')
    require(ready.get('native_source_map_canonical_sha256')==release.get('source_map_canonical_sha256')==BENCHMARK,'Benchmark map changed')
    require(release['source_map']==ready['source_map'] and digest(release['source_map'])==BENCHMARK,'Whole benchmark map changed')
    require(ready['native_binary']['sha256']==release['binary_sha256']==ELF,'Measured ELF changed')
    require(ready['release_record']['sha256']==RELEASE and ready['policy_canonical_sha256']==POLICY,'Release/policy changed')
    profile=release['profile']
    require(profile.get('opt_level')=='3' and type(profile.get('debuginfo'))is int and profile['debuginfo']==0
            and profile.get('debug_assertions')is False and profile.get('overflow_checks')is False and profile.get('test')is True,'Optimized profile changed')
    return True

def binding(fixture,report):
    fields={'production_base_commit':BASE,'production_source_map_canonical_sha256':PRODUCTION,
            'native_source_map_canonical_sha256':BENCHMARK,'native_release_record_sha256':RELEASE,
            'native_binary_sha256':ELF}
    for key,value in fields.items():
        require(fixture.get(key)==report.get(key)==value,'Source seal release field mismatch: '+key)

def parent_review(review,experiment_sha256):
    require(review.get('passed')is True and review.get('experiment_report_sha256')==experiment_sha256
            and review.get('native_binary_sha256')==ELF
            and review.get('native_source_map_sha256')==BENCHMARK,'Independent complete-run parent review required')

def tool_map(ready):
    entries=ready['tools_sha256']
    require(all(name.startswith('tools/') and '..'not in Path(name).parts for name in entries),'Ready tool path changed')
    return {name[len('tools/'):]:value for name,value in entries.items()}
