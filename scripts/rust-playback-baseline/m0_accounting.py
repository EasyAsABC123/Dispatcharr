"""Private accounting seam; never starts probes/threads or claims Source acceptance.
Original Dispatcharr authors retain application/algorithm credit and licenses.
"""
import json
import math


class Accounting:
    """Accept observations from the scenario's existing observer and owned barriers.

    Storage guards are caller-declared instrumentation bounds, not new Source
    CPU/memory/growth budgets. The caller owns observation and physical close.
    """
    def __init__(self, *, scenario, source_inputs, container_bindings, contract_ids,
                 observed_method, declared_budgets, max_records, max_record_bytes):
        if type(max_records) is not int or max_records < 1:
            raise ValueError('Explicit record-count guard required')
        if type(max_record_bytes) is not int or max_record_bytes < 1:
            raise ValueError('Explicit record-byte guard required')
        if not container_bindings or any(not v.get('id') or not v.get('started_at') for v in container_bindings.values()):
            raise ValueError('Container identity and StartedAt required')
        self.max_records, self.max_record_bytes = max_records, max_record_bytes
        self.header = self._copy(dict(scenario=scenario, source_inputs=source_inputs,
            container_bindings=container_bindings, contract_ids=contract_ids,
            observed_method=observed_method, declared_budgets=declared_budgets))
        self.records = []
        self.active_phase = None
        self.last_time = None
        self.epoch = 0

    def _copy(self, value):
        encoded = json.dumps(value, sort_keys=True, allow_nan=False).encode()
        if len(encoded) > self.max_record_bytes:
            raise ValueError('Accounting instrumentation record-byte guard')
        return json.loads(encoded)

    def _append(self, value):
        if len(self.records) >= self.max_records:
            raise ValueError('Accounting instrumentation record-count guard')
        self.records.append(self._copy(value))

    def _time(self, value):
        if type(value) not in (float, int) or not math.isfinite(value) or value < 0:
            raise ValueError('Finite monotonic time required')
        if self.last_time is not None and value < self.last_time:
            raise ValueError('Accounting clock moved backwards')
        self.last_time = value

    def _identity(self, bindings):
        if bindings != self.header['container_bindings']:
            raise ValueError('Same-run container identity/StartedAt/limits changed')

    def begin(self, phase, *, monotonic, bindings, state):
        if self.active_phase is not None:
            raise ValueError('Previous accounting phase still open')
        self._identity(bindings)
        self._time(monotonic)
        self.epoch += 1
        self._append(dict(kind='begin', phase=phase, epoch=self.epoch,
                          monotonic=monotonic, state=state))
        self.active_phase = phase

    def observation(self, *, phase, epoch, started_monotonic, completed_monotonic,
                    bindings, observer_record, census_complete):
        self._identity(bindings)
        if phase != self.active_phase or epoch != self.epoch:
            raise ValueError('Wrong accounting phase/epoch')
        if completed_monotonic < started_monotonic:
            raise ValueError('Observation completion precedes start')
        self._time(completed_monotonic)
        self._append(dict(kind='observation', phase=phase, epoch=epoch,
            started_monotonic=started_monotonic, completed_monotonic=completed_monotonic,
            census_complete=bool(census_complete), observer_record=observer_record))

    def end(self, phase, *, monotonic, bindings, state, readers_physically_closed):
        self._identity(bindings)
        if phase != self.active_phase:
            raise ValueError('Wrong phase completion')
        self._time(monotonic)
        if type(readers_physically_closed) is not bool:
            raise ValueError('Explicit close-barrier observation required')
        self._append(dict(kind='end', phase=phase, epoch=self.epoch,
            monotonic=monotonic, state=state, readers_physically_closed=readers_physically_closed))
        self.active_phase = None

    def snapshot(self):
        return self._copy(dict(header=self.header, records=self.records,
            phase_open=self.active_phase is not None, resource_acceptance=False,
            Source_budget_acceptance=None, fully_verified=False,
            note='Recorded observations only; apply separately sealed scenario budgets.'))


def provider_capacity(value):
    """Do not confuse absent, unlimited provider caps and finite configured caps."""
    if value is None:
        return dict(kind='absent', value=None)
    if type(value) is not int or value < 0:
        raise ValueError('Invalid configured capacity')
    return dict(kind='unlimited' if value == 0 else 'finite', value=value)
