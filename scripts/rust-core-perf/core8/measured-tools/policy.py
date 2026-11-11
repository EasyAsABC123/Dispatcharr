"""Private M1 core plan validation, work matching and fail-closed metric checks.
Original Dispatcharr authors retain application/algorithm credit and licenses.
"""
import hashlib
import json
import math


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def number(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError('Missing/negative/nonfinite metric')
    return value


def validate_plan(plan):
    fixed = {'packet_bytes': 188, 'packets_per_chunk': 1361, 'chunk_bytes': 255868,
             'input_frame_bytes': 8192, 'retention_seconds': 60,
             'native_authoritative_history_bytes': 128 * 1024 * 1024,
             'native_max_chunks': 1024, 'native_local_cache_bytes': 4 * 1024 * 1024,
             'viewers': [1, 10, 100], 'source_only_period_candidates_ms': [250, 500, 1000],
             'warmup_seconds': 15, 'measure_seconds': 60, 'pairs': 3, 'minimum_samples': 50}
    if any(plan.get(key) != value for key, value in fixed.items()):
        raise ValueError('Core workload/pacing/bounds changed')
    if plan['budgets'] != {'max_native_source_cpu_ratio': .9,
                          'max_native_source_peak_working_memory_ratio': .95,
                          'min_successful_byte_ratio': .98}:
        raise ValueError('Core budgets changed')
    if plan.get('measurement_minimum_pacing_fraction') != .95 or plan['qualification_limits']['minimum_pacing_fraction'] != .95:
        raise ValueError('Predeclared pacing floor changed')
    if plan['orders'] != ['source,native', 'native,source', 'source,native']:
        raise ValueError('Pair order changed')
    for key in ['backend_limits', 'infrastructure_limits']:
        if plan[key] != {'cpus': 2, 'memory_bytes': 512 * 1024 * 1024}:
            raise ValueError('Finite identical limits required')


def validate_work(value, backend, viewers, period_ms, warmup, measure, require_history=False):
    total_ms = (warmup + measure) * 1000
    if total_ms % period_ms:
        raise ValueError('Integral chunk schedule required')
    chunks = total_ms // period_ms
    if value.get('passed') is not True or value.get('backend') != backend:
        raise ValueError('Core worker failed/backend mismatch')
    if value.get('viewers') != viewers or value.get('period_ms') != period_ms:
        raise ValueError('Core actor/workload mismatch')
    if value.get('produced') != chunks or value.get('chunk_bytes') != 255868:
        raise ValueError('Incomplete producer/packet geometry')
    if value.get('final_full_bytes_per_viewer') != chunks * 255868:
        raise ValueError('Full-byte count mismatch')
    if value.get('drain_release_seen') is not True:
        raise ValueError('Core peer omitted controlled post-window drain barrier')
    rows = value.get('readers')
    if not isinstance(rows, list) or len(rows) != viewers:
        raise ValueError('Reader cohort missing')
    classes = {'fast': {'readers': 0, 'measured_bytes': 0}, 'slow': {'readers': 0, 'measured_bytes': 0}}
    for index, row in enumerate(rows):
        if row.get('delivered') != chunks or row.get('bytes') != chunks * 255868 or row.get('errors', []):
            raise ValueError('Incomplete/full-byte/sequence reader work')
        measured = row.get('measured')
        if type(measured) is not int or not 0 < measured <= chunks:
            raise ValueError('Measured work missing/out of bounds')
        slow = viewers > 1 and index % 10 == 0
        kind = 'slow' if slow else 'fast'
        classes[kind]['readers'] += 1
        classes[kind]['measured_bytes'] += measured * 255868
        if require_history and slow and row.get('historical_reads', 0) <= 0:
            raise ValueError('Functional slow reader never exercised actual History')
    late = number(value.get('producer_late'))
    if late > chunks:
        raise ValueError('Invalid producer lateness count')
    return {'chunks': chunks, 'total_full_bytes': chunks * viewers * 255868,
            'pacing_fraction': 1 - late / chunks, 'classes': classes}


def select_common_period(plan, qualification):
    """All nine Source rows must exist before selecting one common pacing."""
    expected = {(v, p) for v in plan['viewers'] for p in plan['source_only_period_candidates_ms']}
    if len(qualification) != len(expected) or {(v.get('viewers'), v.get('period_ms')) for v in qualification} != expected:
        raise ValueError('Incomplete common Source-only qualification')
    limits = plan['qualification_limits']
    good = {}
    for row in qualification:
        if row.get('backend') != 'source' or any(key in row for key in ['native', 'native_metrics', 'candidate_metrics']):
            raise ValueError('Native data cannot choose common pacing')
        for key in ['pacing_fraction', 'core_cpu_percent', 'core_memory_fraction', 'core_throttled_period_fraction']:
            number(row.get(key))
        if row['core_memory_fraction'] > 1 or row['core_throttled_period_fraction'] > 1:
            raise ValueError('Invalid resource fraction')
        good[(row['viewers'], row['period_ms'])] = (
            row.get('passed') is True and row.get('errors') == 0
            and row['pacing_fraction'] >= limits['minimum_pacing_fraction']
            and row['core_cpu_percent'] <= limits['maximum_core_cpu_percent']
            and row['core_memory_fraction'] <= limits['maximum_core_memory_fraction']
            and row['core_throttled_period_fraction'] <= limits['maximum_core_throttled_period_fraction'])
    feasible = [p for p in plan['source_only_period_candidates_ms'] if all(good[(v, p)] for v in plan['viewers'])]
    if not feasible:
        raise ValueError('No common Source-feasible producer period')
    return min(feasible)
