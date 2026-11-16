"""Declared retained Original file+SQL read fixture. No app on import.

No recording is produced again; actual Original playback/FD/TERM is exercised
only when Root invokes run inside its exclusive continuation lease.
"""
import hashlib
import json
import os
import shutil
import uuid
from pathlib import Path


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def run(fixture, authority_path):
    if fixture.backend != 'Original':
        raise ValueError('RetainedOriginalReadFixtureOnly')
    authority_path = Path(authority_path)
    authority = json.loads(authority_path.read_text())
    capture = Path(authority['captured_snapshot']['path'])
    if sha(capture) != authority['captured_snapshot']['sha256']:
        raise ValueError('ExactRetainedOriginalSnapshot')
    captured = json.loads(capture.read_text())
    row = next(r for r in captured['actual_rows'] if r['id'] == authority['recording_id'])
    media = Path(authority['media']['path'])
    if (sha(media) != authority['media']['sha256'] or media.stat().st_size != authority['media']['bytes']
            or row['custom_properties'].get('status') != 'completed'):
        raise ValueError('ExactRetainedCompletedOriginalMedia')
    # Only actual Source uWSGI is started. No Celery/DVR/Beat startup/recovery
    # can mutate this declared read-only recording fixture.
    if fixture.roles.keys() != {'web'}:
        raise ValueError('ReadFixtureMustNotStartBusinessConsumers')
    if row['channel_id'] != fixture.channel_id:
        raise ValueError('CapturedSourceChannelIdentity')
    destination = fixture.case_data / Path(row['custom_properties']['file_path']).relative_to('/data')
    relative=destination.relative_to(fixture.case_data)
    guest='/shared/'+fixture.case+'/'+relative.as_posix()
    fixture.python("from pathlib import Path;p=Path("+repr(guest)+");p.parent.mkdir(parents=True,exist_ok=True);[q.chmod(0o755)for q in p.parents if str(q).startswith('/shared/'+"+repr(fixture.case)+")]")
    # Docker archive extraction cannot lchown a prior rootless subordinate
    # host UID as a new guest UID. Copy bytes without ownership metadata first.
    stage=fixture.output/('retained-media-controller-'+uuid.uuid4().hex+'.mkv')
    try:
        shutil.copyfile(media,stage)
        metadata=stage.stat()
        if metadata.st_uid!=os.getuid()or metadata.st_gid!=os.getgid()or metadata.st_size!=authority['media']['bytes']or sha(stage)!=authority['media']['sha256']:
            raise ValueError('ControllerOwnedByteExactMediaStage')
        receipt={'stage_path':str(stage),'bytes':metadata.st_size,'sha256':sha(stage),'UID':metadata.st_uid,'GID':metadata.st_gid,'controller_UID':os.getuid(),'controller_GID':os.getgid(),'original_UID':media.stat().st_uid,'original_GID':media.stat().st_gid,'original_unchanged':sha(media)==authority['media']['sha256'],'ownership_metadata_preserved':False}
        (fixture.output/'retained-source-owner-staging.json').write_text(json.dumps(receipt,indent=2)+'\n')
        fixture.command([fixture.docker,'cp',str(stage),fixture.project+'-baseline-1:'+guest],seconds=60)
    finally:
        stage.unlink(missing_ok=True)

    copied=json.loads(fixture.python("import json,hashlib;from pathlib import Path;p=Path("+repr(guest)+");p.chmod(0o444);h=hashlib.sha256();f=p.open('rb');[h.update(v)for v in iter(lambda:f.read(1048576),b'')];print(json.dumps({'bytes':p.stat().st_size,'sha256':h.hexdigest()}))"))
    if copied!={'bytes':authority['media']['bytes'],'sha256':authority['media']['sha256']}:
        raise ValueError('GuestByteExactOriginalReadFixtureCopy')
    if sha(destination) != authority['media']['sha256']:
        raise ValueError('ByteExactOriginalReadFixtureCopy')
    # Literal actual captured row, bulk insertion only: no post_save signal,
    # booking/task/publication replay, fabricated media or row normalization.
    fixture.sql('row=' + repr(row) + ';Recording.objects.bulk_create([Recording(**row)]);'
                'print(json.dumps({"actual_inserted_rows":Recording.objects.count()}))')
    result = fixture.slow_reader_cases(row['id'])
    result['retained_Original_read_fixture'] = {
        'authority_path': str(authority_path), 'authority_sha256': sha(authority_path),
        'media_sha256': sha(destination), 'captured_row_preserved': True,
        'new_recording_production': False, 'consumer_startup': False}
    if sha(media) != authority['media']['sha256'] or sha(destination) != authority['media']['sha256']:
        raise ValueError('RetainedOriginalMediaWasModified')
    return result
