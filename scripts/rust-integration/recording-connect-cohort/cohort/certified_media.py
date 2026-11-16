"""Fixed-profile outer-cut certificates. No subprocess/application on import.

Only actual per-PID prefix before an independently decodable IDR and unfinished
outer PES after the measurement window can be removed. Interior bytes survive.
"""
import hashlib
from pathlib import Path
from complete_units import Index, window


def certify(raw, derivative, start, end, drain_offset):
    raw, derivative = Path(raw), Path(derivative)
    full = Index().read(raw)
    drained = Index().read(raw, limit=drain_offset)
    selected = window(drained, start, end)
    if not selected['closure_proven']:
        raise ValueError('WindowPESMissingBoundedDrainClosure')
    if set(drained['pid_kinds'].values()) != {'video', 'audio'}:
        raise ValueError('ExactVideoAudioPIDPairRequired')
    psi=full.get('verified_PSI',{})
    if not psi.get('PAT_count')or not psi.get('PMT_count')or psi.get('first_PMT',start+1)>start:
        raise ValueError('ActualCRCVerifiedProgramMapBeforeWindowRequired')
    if any(pid not in full['pid_kinds'] and pid!=psi['PMT_PID'] for pid in full['prefix_unassigned_packets']):
        raise ValueError('UnknownUnassignedPID')
    lower = {}
    upper = {}
    for pid, kind in full['pid_kinds'].items():
        groups = [g for g in full['groups'] if g['pid'] == pid]
        first = next((g for g in groups if g.get('kind') == kind and g['complete']
                      and (kind != 'video' or g.get('random_access'))), None)
        if first is None or first['closure_proof_end'] > start:
            raise ValueError('DecodableContextNotBeforeWindow')
        lower[pid] = first['start']
        unfinished = [g for g in groups if not g['complete']]
        if len(unfinished) > 1 or any(g['start'] < end for g in unfinished):
            raise ValueError('UnfinishedPESInsideOrTouchingWindow')
        upper[pid] = unfinished[0]['start'] if unfinished else full['raw_bytes']
    # No transport/parser failure may be excused by a generic EOF assertion.
    # A trailing partial TS packet outside W is the only parser-error exception.
    for entry in full['errors']:
        if entry['type'] != 'TerminalPartialTSPacket' or entry['offset'] < end:
            raise ValueError('InternalTransportOrCodecDamage:' + entry['type'])
    if full['error_count'] > len(full['errors']):
        raise ValueError('TransportErrorEvidenceTruncated')
    removed = []
    kept_hash = hashlib.sha256()
    with raw.open('rb') as source, derivative.open('xb') as target:
        pos = 0
        while True:
            packet = source.read(188)
            if not packet:
                break
            pid = ((packet[1] & 31) << 8 | packet[2]) if len(packet) == 188 else None
            drop = len(packet) != 188 or (pid in lower and not lower[pid] <= pos < upper[pid])
            if drop:
                if not (pos + len(packet) <= start or pos >= end):
                    raise ValueError('CertificateWouldRemoveWindowBytes')
                reason = 'terminal_partial_TS' if pid is None else 'outer_prefix' if pos < lower[pid] else 'outer_unclosed_PES'
                if removed and removed[-1]['end'] == pos and removed[-1]['pid'] == pid and removed[-1]['reason'] == reason:
                    removed[-1]['end'] += len(packet)
                else:
                    removed.append({'start': pos, 'end': pos + len(packet), 'pid': pid, 'reason': reason})
            else:
                target.write(packet)
                kept_hash.update(packet)
            pos += len(packet)
    return {'raw_sha256': full['raw_sha256'], 'raw_bytes': full['raw_bytes'],
            'derivative_sha256': kept_hash.hexdigest(), 'derivative_bytes': derivative.stat().st_size,
            'drain_offset': drain_offset, 'selected_window': [start, end],
            'removed_ranges': removed, 'PID_lower': lower, 'PID_upper': upper,
            'closure_proven': True, 'window': selected, 'context_groups':[g for g in full['groups']if g['complete']and g['start']>=lower[g['pid']]and g['last_packet_end']<=upper[g['pid']]]}
