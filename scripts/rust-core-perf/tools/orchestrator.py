"""Private Source-only qualification then separately cued9-pair core matrix.
UNEXECUTED without a fresh parent quiet cue. No thresholds adapt to results.
"""
import json
import hashlib
from pathlib import Path
import time
from .metrics import authorize, observer, window, summarize, lifecycle
from .policy import digest, select_common_period, validate_work


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def pair(plan, source, native):
    if source['backend'] != 'source' or native['backend'] != 'native' or source['viewers'] != native['viewers']:
        raise ValueError('Pair actor/cohort mismatch')
    ratios = {}
    for label in ['fast', 'slow']:
        a = source['work']['classes'][label]
        b = native['work']['classes'][label]
        if a['readers'] != b['readers']:
            raise ValueError('Slow/fast class membership mismatch')
        if a['readers']:
            ratios[label] = min(a['measured_bytes'], b['measured_bytes']) / max(a['measured_bytes'], b['measured_bytes'])
    cpu = native['metrics']['cpu_percent_mean'] / source['metrics']['cpu_percent_mean']
    memory = native['metrics']['peak_working_memory_bytes'] / source['metrics']['peak_working_memory_bytes']
    failed = []
    if cpu > plan['budgets']['max_native_source_cpu_ratio']:
        failed.append('whole_core_infrastructure_cpu')
    if memory > plan['budgets']['max_native_source_peak_working_memory_ratio']:
        failed.append('whole_core_infrastructure_peak_memory')
    if any(value < plan['budgets']['min_successful_byte_ratio'] for value in ratios.values()):
        failed.append('measured_per_class_byte_work')
    if source['work']['total_full_bytes'] != native['work']['total_full_bytes']:
        failed.append('final_full_byte_work')
    return {'passed': not failed, 'failed': failed, 'cpu_ratio': cpu, 'memory_ratio': memory,
            'measured_class_byte_ratios': ratios, 'viewers': source['viewers']}


def experiment(runtime, plan, support, binary_sha256, source_cue, measurement_cue, fixture_binding):
    out = runtime.output
    report = {'passed': False, 'performance_verified': False, 'qualification': [], 'phases': [], 'pairs': []}
    smoke_done = False

    def phase(backend, viewers, period, label):
        nonlocal smoke_done
        if hashlib.sha256((out/'native-core-peer').read_bytes()).hexdigest() != binary_sha256:
            raise ValueError('Release ELF changed after qualification/seal')
        for name, expected in fixture_binding['tools_sha256'].items():
            if hashlib.sha256((out/'core-tools'/name).read_bytes()).hexdigest() != expected:
                raise ValueError('Frozen core tool changed')
        for name, expected in fixture_binding.get('support_sha256', {}).items():
            if hashlib.sha256((Path(support)/name).read_bytes()).hexdigest() != expected:
                raise ValueError('Frozen shared observer/fixture tool changed')
        lifecycle({'infrastructure': runtime.inspected('baseline')}, support, out/(label+'-between-phase-lifecycle.json'))
        state = runtime.start(backend, viewers, period, plan, label)
        lifecycle(state['selected'], support, out/(label+'-setup-lifecycle.json'))
        observed = observer(state['selected'], support)
        if not smoke_done:
            smoke = observed.smoke()
            dump(out / 'observer-functional-smoke.json', smoke)
            smoke_done = True
        raw = window(observed, state, plan, out / (label + '-resources.json'))
        value = runtime.finish(state, before_remove=lambda:lifecycle(state['selected'], support, out/(label+'-drain-lifecycle.json')))
        work = validate_work(value, backend, viewers, period, plan['warmup_seconds'], plan['measure_seconds'], require_history=(backend == 'native'))
        metrics = summarize(raw, plan)
        result = {'backend': backend, 'viewers': viewers, 'period_ms': period, 'work': work,
                  'metrics': metrics, 'selected': state['selected'], 'label': label}
        dump(out / (label + '-normalized.json'), result)
        report['phases'].append(result)
        return result

    try:
        for viewers in plan['viewers']:
            for period in plan['source_only_period_candidates_ms']:
                authorize(source_cue, 'm1-core-source-qualification', plan, binary_sha256)
                value = phase('source', viewers, period, f'qual-source-{viewers}-{period}')
                row = dict(backend='source', viewers=viewers, period_ms=period, passed=True, errors=0,
                           pacing_fraction=value['work']['pacing_fraction'],
                           **{key: value['metrics'][key] for key in ['core_cpu_percent', 'core_memory_fraction', 'core_throttled_period_fraction']})
                report['qualification'].append(row)
                dump(out / 'source-qualification.json', report['qualification'])
        period = select_common_period(plan, report['qualification'])
        frozen = {'policy': plan, 'policy_sha256': digest(plan), 'qualification_sha256': digest(report['qualification']),
                  'selected_common_period_ms': period, 'native_binary_sha256': binary_sha256,
                  'fixture_binding': fixture_binding, 'measurements_started': False}
        frozen['plan_sha256'] = digest(frozen)
        dump(out / 'frozen-plan.json', frozen)
        print(json.dumps({'source_seal_ready': frozen['plan_sha256'], 'selected_common_period_ms': period}), flush=True)
        # Parent coordination only; elapsed time never constitutes authorization.
        deadline = time.monotonic() + 20 * 60
        while not Path(measurement_cue).is_file():
            if time.monotonic() > deadline:
                raise TimeoutError('Fresh parent measurement quiet cue absent')
            time.sleep(1)
        for viewers in plan['viewers']:
            for trial, order in enumerate(plan['orders'], 1):
                current = json.loads((out/'frozen-plan.json').read_text())
                unsigned = dict(current)
                claimed = unsigned.pop('plan_sha256', None)
                if current != frozen or claimed != digest(unsigned):
                    raise ValueError('Frozen Source plan changed')
                authorize(measurement_cue, 'm1-core-resource-measurement', plan, binary_sha256, frozen['plan_sha256'])
                values = {}
                for backend in order.split(','):
                    authorize(measurement_cue, 'm1-core-resource-measurement', plan, binary_sha256, frozen['plan_sha256'])
                    values[backend] = phase(backend, viewers, period, f'measure-{viewers}-{trial}-{backend}')
                    if values[backend]['work']['pacing_fraction'] < plan['measurement_minimum_pacing_fraction']:
                        raise ValueError('Measured producer missed unchanged declared pacing floor')
                result = pair(plan, values['source'], values['native'])
                result.update(trial=trial, order=order)
                report['pairs'].append(result)
                dump(out / 'matrix-progress.json', report)
                print(json.dumps({'pair_completed': result}), flush=True)
                if not result['passed']:
                    raise ValueError('Predeclared core budget/work failure; stop without tuning')
        report.update(passed=True, performance_verified=True, plan_sha256=frozen['plan_sha256'])
        return report
    except (Exception, KeyboardInterrupt) as error:
        report['error_type'] = type(error).__name__
        report['failure_reason'] = str(error)[:1024]
        report['manual_stop'] = isinstance(error, KeyboardInterrupt)
        raise
    finally:
        dump(out / 'experiment.json', report)
