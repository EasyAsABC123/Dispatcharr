"""Read-only projected-only Native19 validation. No app/Source/binary execution."""
import hashlib
import json
import io
import tarfile
from pathlib import Path
from validate_native import validate as validate_native

ROOT=Path(__file__).resolve().parent
EXPECTED_PROJECTION='17042484ddd12b3e73aeadbbe5857427d305b2868ae43d42f80b83c3ba60a8bb'
EXPECTED_ORIGINAL_RAW='c5ccd8e45a5e4b1f98368b68fdb6d2eb9b0b889d98e38a23d4c91cb639d339fe'

def load_native_projection():
    ref=json.loads((ROOT/'reference.json').read_text())
    if ref['input_mode']!='projected-only-new-reference-no-raw-fallback':raise ValueError('ExactlyProjectedReferenceMode')
    if ref['projection_sha256']!=EXPECTED_PROJECTION or ref['original_raw_sha256']!=EXPECTED_ORIGINAL_RAW:raise ValueError('ExactlyOneOriginalToProjectedBinding')
    policy=(ROOT/'projection-policy.json').read_bytes()
    if hashlib.sha256(policy).hexdigest()!=ref['projection_policy_sha256']:raise ValueError('ExactProjectionPolicy')
    metadata=json.loads(policy)
    if metadata['original_raw_sha256']!=EXPECTED_ORIGINAL_RAW or metadata['projection_sha256']!=EXPECTED_PROJECTION:raise ValueError('PolicyOriginalAndProjectedIdentity')
    raw=(ROOT/'Native19-projection.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=ref['projection_sha256']:raise ValueError('StrictProjectedSHAOnly')
    value=json.loads(raw)
    if value.get('_synthetic_only') is True:raise ValueError('ActualReferenceCannotBeSynthetic')
    return value

def check_packet():
    inventory=json.loads((ROOT/'packet-inventory.json').read_text())
    actual={str(p.relative_to(ROOT)) for p in ROOT.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.name!='packet-inventory.json'}
    if actual!=set(inventory['files']):raise ValueError('ExactPacketFileSet')
    for name,row in inventory['files'].items():
        p=ROOT/name
        if p.stat().st_size!=row['bytes'] or hashlib.sha256(p.read_bytes()).hexdigest()!=row['sha256']:raise ValueError('ExactPacketBlob:'+name)
    lifecycle=json.loads((ROOT/'lifecycle-inventory.json').read_text());data=(ROOT/'lifecycle.tar.gz').read_bytes()
    if hashlib.sha256(data).hexdigest()!=lifecycle['archive_sha256']:raise ValueError('LifecycleArchiveSHA')
    expected={row['path']:row for row in lifecycle['members']}
    with tarfile.open(fileobj=io.BytesIO(data),mode='r:gz') as tar:
        members=tar.getmembers()
        if len(members)!=len(expected) or {m.name for m in members}!=set(expected):raise ValueError('ExactLifecycleMemberSet')
        for member in members:
            if not member.isfile():raise ValueError('OnlyLifecycleRegularFiles')
            raw=tar.extractfile(member).read();row=expected[member.name]
            if len(raw)!=row['bytes'] or hashlib.sha256(raw).hexdigest()!=row['sha256']:raise ValueError('ExactLifecycleMemberBytes')
    return {'packet_files':len(actual),'lifecycle_members':len(expected)}

def validate():
    integrity=check_packet();value=load_native_projection();result=validate_native(value)
    return {**result,**integrity,'reference_mode':'new-projected-only-input-binding','new_application_runs':0}

if __name__=='__main__':print(json.dumps(validate(),sort_keys=True))
