"""Off-window whole-raw/derivative decoding and fixed-window qualification.

Uses existing command receipts and pinned fixture decoder, never starts an app.
Original Dispatcharr and FFmpeg authors retain all application/codec credit.
"""
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import subprocess
import math
import re
from decode_memo import DecodeMemo, outcome
from certified_media import certify
from window_offsets import offsets


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def frame_rows(text):
    bases = {}
    rows = {}
    for line in text.splitlines():
        if line.startswith('#tb '):
            key, value = line[4:].split(':', 1)
            bases[int(key)] = Fraction(value.strip())
        elif line and not line.startswith('#'):
            stream, _, pts, duration, size, digest = [v.strip() for v in line.split(',')]
            key = int(stream), int(pts)
            if key in rows:
                raise ValueError('DuplicateDecodedFramePTS')
            rows[key] = (int(duration), int(size), digest)
    if bases != {0: Fraction(1, 25), 1: Fraction(1, 48000)}:
        raise ValueError('ExactPinnedDecoderTimebasesRequired')
    return rows


def decode_argv(compose, raw, output):
    local = '/evidence/' + str(raw.relative_to(output))
    argv = compose + ['exec', '-T', 'baseline', 'ffmpeg', '-hide_banner', '-loglevel', 'error',
                     '-threads', '1', '-filter_threads', '1', '-filter_complex_threads', '1', '-copyts', '-i', local, '-map', '0:v:0', '-map', '0:a:0',
                     '-fps_mode:v', 'passthrough', '-c:v', 'rawvideo', '-threads:v', '1', '-threads:a', '1', '-pix_fmt', 'yuv420p',
                     '-c:a', 'pcm_s16le', '-f', 'framehash', '-hash', 'sha256', 'pipe:1']
    return argv


def physical_decode(command, compose, raw, output):
    argv = decode_argv(compose, raw, output)
    actual_exit, stdout, stderr = None, '', ''
    failure = None
    try:
        stdout = command(argv, timeout=120)
        actual_exit = 0
    except subprocess.CalledProcessError as error:
        actual_exit, stdout, stderr = error.returncode, error.output or '', error.stderr or ''
    except Exception as error:
        failure = type(error).__name__
    if not stderr:
        stderr = getattr(command, 'last_stderr', '')
    if isinstance(stdout, bytes):
        stdout = stdout.decode('utf8', errors='strict')
    if isinstance(stderr, bytes):
        stderr = stderr.decode('utf8', errors='strict')
    transcript = raw.with_suffix(raw.suffix + '.framehash')
    transcript.write_text(stdout)
    receipt = {'actual_exit': actual_exit, 'failure': failure,
               'stdout_path': str(transcript), 'stdout_sha256': sha(transcript),
               'stderr_bytes': len(stderr.encode()), 'stderr_sha256': hashlib.sha256(stderr.encode()).hexdigest(),
               'command_receipt': getattr(command, 'last_result', None), 'media_sha256': sha(raw), 'media_bytes': raw.stat().st_size}
    raw.with_suffix(raw.suffix + '.decode.json').write_text(json.dumps(receipt, indent=2) + '\n')
    return receipt, stdout


def decoder_identity(command, compose, report):
    cid = command(compose + ['ps', '-q', 'baseline'], timeout=15).strip()
    if not re.fullmatch(r'[0-9a-f]{64}', cid):
        raise ValueError('ActualDecoderContainerRequired')
    image = json.loads(command([compose[0], 'inspect', '--format', '{{json .Image}}', cid], timeout=15))
    expected = report['baseline_image'].split('@')[-1]
    if image != expected or not re.fullmatch(r'sha256:[0-9a-f]{64}', image):
        raise ValueError('PinnedActualDecoderImageRequired')
    script = "import hashlib,json,shutil;from pathlib import Path;p=Path(shutil.which('ffmpeg')).resolve();h=hashlib.sha256();f=p.open('rb');[h.update(b)for b in iter(lambda:f.read(65536),b'')];print(json.dumps({'path':str(p),'sha256':h.hexdigest()}))"
    tool = json.loads(command(compose + ['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '-c', script], timeout=15))
    if not re.fullmatch(r'[0-9a-f]{64}', tool['sha256']) or not tool['path'].startswith('/'):
        raise ValueError('ActualDecoderExecutableRequired')
    return {'image_id': image, 'CID': cid, 'ffmpeg_path': tool['path'],
            'ffmpeg_sha256': tool['sha256'], 'argv_environment': compose + ['exec', '-T', 'baseline'],
            'scope': 'same owned immutable baseline container; no per-call environment overrides'}


def decode(command, compose, raw, output, memo=None):
    if memo is None:
        return physical_decode(command, compose, raw, output)
    return memo.decode(raw, decode_argv(compose, raw, output),
                       lambda: physical_decode(command, compose, raw, output))


def reference_ordinals(reference_frames):
    """Exact Source reference order, not a timestamp tolerance or retiming.

VLC's AAC samples still have duration1024; its actual muxed PTS sequence rounds
through microseconds and produces1023/1024/1025 sample-PTS differences. Every
candidate PTS/content/size must match the independent Source entry exactly.
"""
    counts = {}
    positions = {}
    for key in reference_frames:
        sid = key[0]
        positions[key] = counts.get(sid, 0)
        counts[sid] = positions[key] + 1
    return positions


def selected_content(certificate, decoded, reference_frames, reference_packets, window_seconds=60):
    previous = {}
    positions = reference_ordinals(reference_frames)
    streams = {}
    for kind, sid, rate, duration in [('video', 0, 25, Fraction(1, 25)),
                                      ('audio', 1, 48000, Fraction(1024, 48000))]:
        chosen = [unit for group in certificate['window']['complete_groups']
                  if group.get('kind') == kind for unit in group['units']]
        if not chosen:
            raise ValueError('CompleteSelectedAVUnitsRequired')
        encoded = 0
        for unit in chosen:
            key = sid, (unit['pts90k'] * rate + 45000) // 90000
            ordinal = positions.get(key)
            if ordinal is None or (sid in previous and ordinal != previous[sid] + 1):
                raise ValueError('MissingDuplicateOrReorderedInteriorUnit')
            previous[sid] = ordinal
            if key not in decoded or key not in reference_frames or decoded[key] != reference_frames[key]:
                raise ValueError('InteriorDecodedContentMismatch')
            packet_key = sid, unit['pts90k']
            if packet_key not in reference_packets or reference_packets[packet_key] != unit['elementary_bytes']:
                raise ValueError('IndependentEncodedUnitMismatch')
            encoded += unit['elementary_bytes']
        first = Fraction(chosen[0]['pts90k'], 90000)
        last = Fraction(chosen[-1]['pts90k'], 90000) + duration
        # Use the independent fixed-profile content in the full sixty seconds
        # beginning at the selected content anchor, never candidate calibration.
        expected = sum(size for (stream, pts), size in reference_packets.items()
                       if stream == sid and first <= Fraction(pts, 90000)
                       and Fraction(pts, 90000) + duration <= first + window_seconds)
        if not expected:
            raise ValueError('IndependentOutputRateAuthorityMissing')
        rate_fraction = Fraction(encoded, expected)
        if not Fraction(9, 10) <= rate_fraction <= Fraction(11, 10):
            raise ValueError('UnchangedPerViewerUsefulRateBudget')
        streams[str(sid)] = {'complete_units': len(chosen), 'encoded_useful_bytes': encoded,
                            'useful_duration_seconds': float(last - first),
                            'first_pts90k': chosen[0]['pts90k'], 'last_pts90k': chosen[-1]['pts90k'],
                            'independent_expected_encoded_bytes_60s': expected,
                            'useful_rate_fraction': float(rate_fraction)}
    return streams


def context_content(certificate, decoded, reference_frames, reference_packets):
    """Reject internal damage anywhere in retained complete decode context.

AAC-LC's first decoded overlap block can depend on missing preceding context.
Only that first1024-sample context unit, strictly before W0, may differ in PCM
hash; its exact PTS/size/duration and encoded unit size remain required. It never
counts as measured useful work. All later context units require exact content.
"""
    previous = {}
    positions = reference_ordinals(reference_frames)
    compared = {'video': 0, 'audio': 0}
    prefix = []
    for group in certificate['context_groups']:
        kind = group['kind']
        sid, rate = (0, 25) if kind == 'video' else (1, 48000)
        for unit in group['units']:
            key = sid, (unit['pts90k'] * rate + 45000) // 90000
            first = sid not in previous
            ordinal = positions.get(key)
            if ordinal is None or (not first and ordinal != previous[sid] + 1):
                raise ValueError('NoncontiguousInternalDecodeContext')
            previous[sid] = ordinal
            if key not in decoded or key not in reference_frames:
                raise ValueError('MissingInternalContextFrame')
            if reference_packets.get((sid, unit['pts90k'])) != unit['elementary_bytes']:
                raise ValueError('AlteredInternalEncodedContext')
            if decoded[key] != reference_frames[key]:
                allowed = (kind == 'audio' and first and group['closure_proof_end'] <= certificate['selected_window'][0]
                           and decoded[key][:2] == reference_frames[key][:2])
                if not allowed:
                    raise ValueError('InternalContextContentMismatch')
                prefix.append({'PTS': key[1], 'timebase': '1/48000', 'samples': 1024,
                               'actual': decoded[key], 'reference': reference_frames[key],
                               'scope': 'first AAC-LC decoder overlap unit outside window only'})
            compared[kind] += 1
    return {'compared_complete_context_units': compared, 'outer_AAC_priming_difference': prefix}


def qualify(command, compose, output, report, reference):
    result = {'protocol': 'complete-unit fixed60s plus bounded5s drain', 'passed': False,
              'old_v2_requalification': False, 'readers': [], 'gain_acceptance': False}
    reference_frames = frame_rows((output / 'reference-fixture.framehash').read_text())
    packets = {}
    for packet in reference['packets']:
        if 'pts_time' not in packet:
            raise ValueError('IndependentPacketPTSRequired')
        pts = round(Fraction(packet['pts_time']) * 90000)
        key = packet['stream_index'], pts
        if key in packets:
            raise ValueError('IndependentPacketAmbiguity')
        packets[key] = int(packet['size'])
    resource_path = output / 'resource-samples.json'
    sample = json.loads(resource_path.read_text()) if resource_path.exists() else {}
    fixed = sample.get('fixed_window_monotonic')
    bound = reference['one_batch_max_PTS_seconds']
    result['one_batch_PTS_seconds'] = bound
    groups={};max_pes_span=0.0
    for packet in reference['packets']:
        sid=packet['stream_index'];pos=packet.get('pos')
        if pos is not None and sid in groups and groups[sid]['pos']!=pos:
            current=groups.pop(sid);max_pes_span=max(max_pes_span,current['last']-current['first'])
        current=groups.setdefault(sid,{'pos':pos,'first':float(packet['pts_time']),'last':float(packet['pts_time'])})
        current['last']=max(current['last'],float(packet['pts_time'])+float(packet.get('duration_time',0)))
    max_pes_span=max([max_pes_span]+[g['last']-g['first']for g in groups.values()])
    result['independent_max_PES_media_span_seconds']=max_pes_span
    result['fixed_drain_derivation_seconds']=math.ceil(2*bound+max_pes_span)
    if result['fixed_drain_derivation_seconds']>5:result['reference_bound_failed']=True
    if bound > 2.0480000001:
        result['reference_bound_failed'] = True
    roots = [output / (case['backend'] + '-' + case['case']) for case in report.get('cases', [])]
    raw_paths = [p for directory in roots for p in sorted(directory.glob('*.ts'))
                 if '.prefix.' not in p.name and '.certified.' not in p.name]
    identity = decoder_identity(command, compose, report)
    result['decoder_identity'] = identity
    result['decode_reuse_protocol'] = 'phase-local-exact-content-v1'
    memo = DecodeMemo(output, identity, max_entries=2 * len(raw_paths))
    for raw in raw_paths:
        row = {'reader': raw.stem, 'raw_path': str(raw), 'passed': False}
        result['readers'].append(row)
        # Full raw decode precedes every assertion, even invalid window/ledger.
        raw_receipt, raw_text = decode(command, compose, raw, output, memo)
        row['full_raw_decode'] = raw_receipt
        try:
            if fixed is None or len(fixed) != 2 or abs(fixed[1] - fixed[0] - 60) > 1e-6:
                raise ValueError('Fixed60SecondWindowMissing')
            ledger = raw.with_suffix('.arrival.jsonl')
            if ledger.stat().st_size > 8 * 1024 * 1024:
                raise ValueError('ArrivalLedger8MiBBudget')
            reads = [json.loads(line) for line in ledger.read_text().splitlines()]
            if len(reads) > 16384:
                raise ValueError('ArrivalLedgerEntryBudget')
            with raw.open('rb') as data:
                for entry in reads:
                    if data.tell() != entry['start_offset']:
                        raise ValueError('ArrivalLedgerRawOffsetMismatch')
                    chunk = data.read(entry['end_offset'] - entry['start_offset'])
                    if hashlib.sha256(chunk).hexdigest() != entry['sha256']:
                        raise ValueError('ArrivalLedgerRawHashMismatch')
                if data.read(1):
                    raise ValueError('RawBytesOutsideArrivalLedger')
            start, end = offsets(reads, *fixed)
            observed_drain = [r['end_offset'] for r in reads if r['end_monotonic'] <= fixed[1] + 5]
            if not observed_drain:
                raise ValueError('BoundedDrainObservationMissing')
            derivative = raw.with_suffix('.certified.ts')
            certificate = certify(raw, derivative, start, end, observed_drain[-1])
            if certificate['raw_sha256'] != raw_receipt['media_sha256'] or certificate['raw_bytes'] != raw_receipt['media_bytes']:
                raise ValueError('RawCertificateDecodeCustodyMismatch')
            compact = {k: v for k, v in certificate.items() if k not in ('window', 'context_groups')}
            raw.with_suffix('.certificate.json').write_text(json.dumps(compact, indent=2) + '\n')
            row['certificate'] = compact
            derived_receipt, derived_text = decode(command, compose, derivative, output, memo)
            row['certified_decode'] = derived_receipt
            if certificate['derivative_sha256'] != derived_receipt['media_sha256'] or certificate['derivative_bytes'] != derived_receipt['media_bytes']:
                raise ValueError('DerivativeCertificateDecodeCustodyMismatch')
            derived_result = outcome(derived_receipt)
            raw_result = outcome(raw_receipt)
            if derived_result['actual_exit'] != 0 or derived_result['failure'] or derived_result['stderr_bytes']:
                raise ValueError('CertifiedDerivativeDecoderNotClean')
            if raw_result['failure'] or raw_result['actual_exit'] not in (0, 1):
                raise ValueError('RawDecoderInfrastructureFailure')
            if (raw_result['actual_exit'] or raw_result['stderr_bytes']) and not certificate['removed_ranges']:
                raise ValueError('RawDecodeErrorWithoutCertifiedOuterCut')
            row['streams'] = selected_content(certificate, frame_rows(derived_text), reference_frames, packets)
            # The raw decoder must also establish all selected units; the clean
            # derivative cannot hide a lost measurement or replace its output.
            selected_content(certificate, frame_rows(raw_text), reference_frames, packets)
            row['decode_context'] = context_content(certificate, frame_rows(derived_text), reference_frames, packets)
            if any(abs(stream['useful_duration_seconds'] - 60) > bound for stream in row['streams'].values()):
                raise ValueError('SingleBatchWallMediaSpanBudget')
            row['window_bytes'] = end - start
            row['window_encoded_useful_bytes'] = sum(stream['encoded_useful_bytes'] for stream in row['streams'].values())
            row['ledger_sha256'] = sha(ledger)
            row['passed'] = True
        except Exception as error:
            row['error'] = {'type': type(error).__name__, 'message': str(error)}
        (output / 'useful-media-complete.json').write_text(json.dumps(result, indent=2) + '\n')
    expected = report.get('cases', [{}])[0].get('viewer_count', 0) if report.get('cases') else 0
    result['passed'] = (not result.get('reference_bound_failed') and expected > 0
                        and len(result['readers']) == expected and all(r['passed'] for r in result['readers'])
                        and sample.get('live_state_passed') is True and not report.get('scheduled_work_during_window'))
    (output / 'useful-media-complete.json').write_text(json.dumps(result, indent=2) + '\n')
    return result
