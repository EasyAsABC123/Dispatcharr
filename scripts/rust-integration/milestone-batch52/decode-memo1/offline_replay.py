"""Existing qualify admission for completed captures; no app/process on import.
Caller supplies the existing supervised command callback and owned decoder-only
compose role. Root grants actual replay separately. No measurement is repeated.
"""
import hashlib
import json
import re
from pathlib import Path
import sys
sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / 'overlay'))


def copy_verified(source, target, expected_sha=None, expected_bytes=None):
    if source.is_symlink() or not source.is_file():
        raise ValueError('RetainedRegularInputRequired')
    target.parent.mkdir(parents=True, exist_ok=True)
    before = source.stat()
    digest = hashlib.sha256()
    count = 0
    with source.open('rb') as raw, target.open('xb') as staged:
        for block in iter(lambda: raw.read(65536), b''):
            staged.write(block)
            digest.update(block)
            count += len(block)
    after = source.stat()
    generation = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if generation(before) != generation(after):
        raise ValueError('RetainedInputChangedDuringCopy')
    sha = digest.hexdigest()
    if (expected_sha is not None and sha != expected_sha) or (expected_bytes is not None and count != expected_bytes):
        raise ValueError('RetainedInputProofMismatch')
    return {'original': str(source), 'staged': str(target), 'sha256': sha, 'bytes': count,
            'original_generation': list(generation(after)), 'method': 'independent-streamed-copy-no-hardlinks'}


def prepare(completed_phase, separate_output):
    source, output = Path(completed_phase).resolve(), Path(separate_output).resolve()
    if output == source or source in output.parents or output in source.parents:
        raise ValueError('SeparateOutputRequired')
    if output.exists() and any(output.iterdir()):
        raise ValueError('EmptyOutputRequired')
    output.mkdir(parents=True, exist_ok=True)
    terminal = json.loads((source.parent / 'Root-terminal.json').read_text())
    cleanup = json.loads((source.parent / 'Root-independent-cleanup.json').read_text())
    if terminal.get('tool_terminal_consumed') is not True or terminal.get('actual_exit') != 0 or cleanup.get('passed') is not True:
        raise ValueError('ConsumedCompletedAuthorityAndCleanupRequired')
    old = json.loads((source / 'useful-media-complete.json').read_text())
    report = json.loads((source / 'shared-ts.json').read_text())
    if old['passed'] is not True or report['passed'] is not True or len(old['readers']) != 100:
        raise ValueError('CompletedPassed100CaptureRequired')
    refs = []
    for row in old['readers']:
        raw = Path(row['raw_path']).resolve()
        raw.relative_to(source)
        refs.append(copy_verified(raw, output / raw.relative_to(source),
                                  row['full_raw_decode']['media_sha256'], row['certificate']['raw_bytes']))
        ledger = raw.with_suffix('.arrival.jsonl')
        refs.append(copy_verified(ledger, output / ledger.relative_to(source), row['ledger_sha256']))
    for name in ('resource-samples.json', 'reference-fixture.framehash', 'reference-fixture.media-index.json',
                 'shared-ts.json', 'useful-media-complete.json'):
        destination = output / ('retained-original-useful-media-complete.json' if name == 'useful-media-complete.json' else name)
        refs.append(copy_verified(source / name, destination))
    plan = {'scope': 'offline-completed100-decoder-qualification-only', 'new_live_connections': 0,
            'new_measurement_phases': 0, 'new_gain_claim': False, 'source_phase': str(source),
            'staged_inputs': refs, 'originals_mutated': False}
    (output / 'offline-replay-inputs.json').write_text(json.dumps(plan, indent=2) + '\n')
    return plan


def replay(command, compose, separate_output):
    # Caller adds existing unchanged complete_units/certified_media/window_offsets
    # directory to sys.path, binds their hashes, and owns the pinned baseline role.
    from complete_media import qualify
    from decode_memo import outcome
    output = Path(separate_output).resolve()
    plan = json.loads((output / 'offline-replay-inputs.json').read_text())
    if plan['scope'] != 'offline-completed100-decoder-qualification-only':
        raise ValueError('OfflineQualificationAdmissionRequired')
    report = json.loads((output / 'shared-ts.json').read_text())
    reference = json.loads((output / 'reference-fixture.media-index.json').read_text())
    old = json.loads((output / 'retained-original-useful-media-complete.json').read_text())
    qualify(command, compose, output, report, reference)
    # Compare persisted JSON wire evidence to persisted JSON evidence. certify
    # uses integer PID dictionary keys internally; JSON object keys are strings.
    # No semantic field or value is omitted by reading each actual saved report.
    new = json.loads((output / 'useful-media-complete.json').read_text())
    # No new resource arithmetic, applications, network readers or phase timing.
    for key in old:
        if key != 'readers' and new[key] != old[key]:
            raise ValueError('RootQualificationChanged:' + key)
    if len(new['readers']) != len(old['readers']):
        raise ValueError('ReaderCountChanged')
    physical = reused = 0
    physical_diagnostics = []
    for before, after in zip(old['readers'], new['readers']):
        exempt = {'raw_path', 'full_raw_decode', 'certified_decode'}
        if {k:v for k,v in before.items() if k not in exempt} != {k:v for k,v in after.items() if k not in exempt}:
            raise ValueError('PerReaderSemanticProofChanged:' + before['reader'])
        for field in ('full_raw_decode', 'certified_decode'):
            current = after[field]
            resolved = outcome(current)
            for key in ('actual_exit', 'failure', 'stdout_sha256', 'stderr_bytes', 'media_sha256'):
                if resolved[key] != before[field][key]:
                    raise ValueError('DecodedContentOrOutcomeChanged:' + before['reader'] + ':' + key)
            if resolved['stderr_sha256'] != before[field]['stderr_sha256']:
                old_path = before[field]['command_receipt']['stderr']
                new_path = resolved['command_receipt']['stderr']
                old_text, new_text = Path(old_path).read_text(), Path(new_path).read_text()
                # Diagnostic classification only. Literal bytes/hashes survive;
                # original qualification gates still check count + outer cuts.
                address = r'(?<= @ )0x[0-9a-f]+(?=\])'
                old_lines = sorted(re.sub(address, '<actual-process-pointer>', old_text).splitlines())
                new_lines = sorted(re.sub(address, '<actual-process-pointer>', new_text).splitlines())
                if old_lines != new_lines:
                    raise ValueError('UnreviewedPhysicalWarningTextChange')
                physical_diagnostics.append({'reader': before['reader'], 'kind': field,
                    'retained_stderr_path': old_path, 'new_stderr_path': new_path,
                    'retained_stderr_sha256': before[field]['stderr_sha256'],
                    'new_stderr_sha256': resolved['stderr_sha256'],
                    'same_exact_warning_text_multiset': True,
                    'difference_scope': 'actual FFmpeg process-pointer addresses and independent warning line order only',
                    'literal_stderr_artifacts_preserved': True})
            if current.get('kind') == 'reused_decode':
                reused += 1
            else:
                physical += 1
    result = {'passed': new['passed'], 'scope': plan['scope'], 'readers_compared': len(new['readers']),
              'all_per_reader_semantic_proofs_exact': True, 'actual_physical_decodes': physical,
              'honest_reused_decode_references': reused, 'live_connections_started': 0,
              'measurement_replayed': False, 'old_gain_or_campaign_changed': False,
              'physical_stderr_provenance_differences': physical_diagnostics}
    (output / 'offline-replay-comparison.json').write_text(json.dumps(result, indent=2) + '\n')
    return result
