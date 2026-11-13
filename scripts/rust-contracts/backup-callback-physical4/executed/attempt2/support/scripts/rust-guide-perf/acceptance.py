"""Pure predeclared stored-guide acceptance; importing never runs workloads.
Original Dispatcharr authors retain full application credit and AGPL licensing.
"""
import math
from plan import POLICY


def number(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError('Finite nonnegative measurement required')
    return value


def phase(value):
    """Validate an orchestrator-normalized phase, retaining all failures."""
    required = {'cpu_percent_mean', 'peak_working_memory_bytes', 'ttfb_p95_ms',
                'completion_p95_ms', 'throughput_per_second', 'pacing_fraction',
                'samples', 'errors', 'sample_errors', 'late_completed', 'oom_kills',
                'counter_resets', 'correct_responses', 'cache_state_verified',
                'fixture_unchanged', 'limits_verified'}
    if set(value) != required:
        raise ValueError('Incomplete or unknown normalized phase fields')
    for key in required - {'correct_responses', 'cache_state_verified',
                           'fixture_unchanged', 'limits_verified'}:
        number(value[key])
    for key in ['samples', 'errors', 'sample_errors', 'late_completed',
                'oom_kills', 'counter_resets']:
        if type(value[key]) is not int:
            raise ValueError('Integer measurement count required')
    if value['pacing_fraction'] > 1:
        raise ValueError('Pacing fraction exceeds one')
    failures = []
    for key in ['correct_responses', 'cache_state_verified', 'fixture_unchanged',
                'limits_verified']:
        if value[key] is not True:
            failures.append(key)
    for key in ['errors', 'sample_errors', 'oom_kills', 'counter_resets']:
        if value[key] != 0:
            failures.append(key)
    if value['samples'] < POLICY['budgets']['minimum_samples']:
        failures.append('insufficient_samples')
    if value['pacing_fraction'] < POLICY['budgets']['min_pacing_fraction']:
        failures.append('pacing_fraction')
    if value['peak_working_memory_bytes'] == 0:
        failures.append('empty_memory_observation')
    if value['throughput_per_second'] == 0:
        failures.append('empty_throughput')
    return failures


def compare(source, candidate):
    """Both phases must be valid; zero baselines never invent a ratio."""
    failures = [f'{side}:{reason}' for side, value in
                [('source', source), ('candidate', candidate)] for reason in phase(value)]
    budget = POLICY['budgets']
    ratios = {}
    for metric, maximum in [('cpu_percent_mean', budget['max_cpu_ratio']),
                            ('peak_working_memory_bytes', budget['max_memory_ratio']),
                            ('completion_p95_ms', budget['max_completion_p95_ratio'])]:
        if source[metric] == 0:
            failures.append(f'zero_source:{metric}')
            ratios[metric] = None
        else:
            ratios[metric] = candidate[metric] / source[metric]
            if ratios[metric] > maximum:
                failures.append(f'regression:{metric}')
    ceiling = max(source['ttfb_p95_ms'], budget['ttfb_floor_ms']) * budget['max_ttfb_p95_ratio']
    if candidate['ttfb_p95_ms'] > ceiling:
        failures.append('regression:ttfb_p95_ms')
    if source['throughput_per_second'] == 0:
        ratios['throughput_per_second'] = None
    else:
        ratios['throughput_per_second'] = candidate['throughput_per_second'] / source['throughput_per_second']
        if ratios['throughput_per_second'] < budget['min_throughput_ratio']:
            failures.append('regression:throughput_per_second')
    return {'passed': not failures, 'failures': failures, 'ratios': ratios,
            'ttfb_p95_ceiling_ms': ceiling}


def matrix(rows):
    """Exactly three pairs per fixed workload; no dropped failed trials."""
    expected = {(work['name'], trial) for work in POLICY['workloads']
                for trial in range(1, POLICY['trials'] + 1)}
    seen = set()
    results = []
    for row in rows:
        if set(row) != {'workload', 'trial', 'order', 'source', 'candidate'}:
            raise ValueError('Invalid matrix row')
        key = (row['workload'], row['trial'])
        if type(row['trial']) is not int or key not in expected or key in seen:
            raise ValueError('Missing, repeated or undeclared matrix pair')
        if row['order'] != POLICY['orders'][row['trial'] - 1]:
            raise ValueError('Paired order differs from frozen policy')
        seen.add(key)
        results.append({'workload': key[0], 'trial': key[1],
                        **compare(row['source'], row['candidate'])})
    if seen != expected:
        raise ValueError('Incomplete matrix; retain partial result without acceptance')
    return {'passed': all(row['passed'] for row in results), 'pairs': results,
            'scope': POLICY['scope']}
