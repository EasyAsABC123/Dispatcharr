"""Native-only preACK refusal contract; never a Source error-result oracle.

Guest integration must use fresh actual Original Refresh/Series metadata and
unmodified raw child frames. No process or fixture is started by this module.
"""
import base64
import json
from contract import SNAPSHOT_FIELDS

REASONS = {
    'missing-recording': 'recording snapshot',
    'duplicate-casefold-movie': 'missing/ambiguous local Movie logo',
    'oversized-recording-properties': 'recording snapshot',
    'nonlocal-logo': 'nonlocal/unqualified logo',
}


def same(a, b):
    if type(a) is not type(b):
        return False
    if type(a) is dict:
        return set(a) == set(b) and all(same(a[k], b[k]) for k in a)
    if type(a) is list:
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    return a == b


def require(value, label):
    if value is not True:
        raise ValueError(label)


def raw_child_context(snapshot, parent_task, root_task, artwork_queue):
    """Derive identities from complete actual metadata plus exact retained FIFO.

    A prior Original worker SUCCESS is an input receipt, not an assumption that
    a random caller can authorize inherited lineage in production.
    """
    for task in (root_task, parent_task):
        require(task['result']['metadata']['status'] == 'SUCCESS', 'ActualParentSuccess')
        require(task['result']['metadata']['task_id'] == task['id'], 'ActualParentIdentity')
        require(type(task['result']['raw_base64']) is str, 'ActualRawParentMetadata')
    require(parent_task['result']['metadata']['parent_id'] == root_task['id'], 'ActualParentRoot')
    queue = snapshot['queues'][artwork_queue]
    require(type(queue) is list and len(queue) == 2, 'ExactlyTwoRetainedArtworkChildren')
    raw = base64.b64decode(queue[-1], validate=True)
    frame = json.loads(raw)
    require(frame['headers']['root_id'] == root_task['id'], 'ActualInheritedRoot')
    require(frame['headers']['parent_id'] == parent_task['id'], 'ActualSeriesParent')
    require(frame['headers']['task'] == 'apps.channels.tasks.prefetch_recording_artwork', 'ActualArtworkTask')
    body = json.loads(base64.b64decode(frame['body'], validate=True))
    require(type(body[0]) is list and len(body[0]) == 1 and type(body[0][0]) is int and body[0][0] > 0, 'ActualRecordingArgument')
    ids = [entry[0][0] for entry in parent_task['result']['metadata']['children']]
    require(len(ids) == 2 and len(set(ids)) == 2 and frame['headers']['id'] == ids[0], 'ActualFirstPublication')
    return {'frame_base64': queue[-1], 'task_id': ids[0], 'other_id': ids[1], 'recording_id': body[0][0]}


def validate(mode, before, after, response, process, artwork_queue, context):
    require(mode in REASONS, 'FiniteRefusalMode')
    require(type(response['ok']) is bool and response['ok'] is False, 'ActualNativeOperationalRefusal')
    # Exact native debug spelling is deliberately scoped to the existing same
    # source7 peer. No unknown domain error or successful no-op qualifies.
    expected = 'Err(Domain(Sql(Unsupported(' + json.dumps(REASONS[mode]) + '))))'
    require(response['result'] == expected, 'ExactKnownPreACKUnsupportedReason')
    require(response['typed_result'] is None, 'NoInventedNormalResult')
    require(response['caller_released'] is True and type(response['available_slots']) is int and response['available_slots'] == 1, 'CallerAndAdmissionPhysicallyReleased')
    require(process['exit'] == 0 and process['settled'] is True and process['pipes_settled'] is True and process['writer_settled'] is True and process['collector_invalid'] is None, 'ActualPeerWait0AndPipes')
    require(set(before) == set(after) == SNAPSHOT_FIELDS, 'EverySnapshotFieldPresent')
    # Full SQL/effects/file/queue fields stay exact. Monotonic Redis TTL and the
    # observation wall clock are checked separately, never discarded silently.
    allowed = {'utc', 'broker', 'cache', 'backend', 'result_backend_raw'}
    for name in before:
        if name not in allowed:
            require(same(before[name], after[name]), 'NoEffect:' + name)
    require(before['queues'][artwork_queue] == after['queues'][artwork_queue], 'OriginalRawFIFOUnchangedBeforeACK')
    require(context['frame_base64'] in after['queues'][artwork_queue], 'ExactCandidateRetained')
    for name in ('broker', 'cache', 'result_backend_raw'):
        a, b = before[name], after[name]
        require(len(a) == len(b), 'ExactRedisKeyCount:' + name)
        key = lambda row: row['key_base64']
        # SCAN ordering is diagnostic; compare exact keyed rows, retain original
        # arrays and timestamps in reports. This is not business row sorting.
        old, new = {key(row): row for row in a}, {key(row): row for row in b}
        require(len(old) == len(a) and len(new) == len(b) and set(old) == set(new), 'ExactUniqueRedisKeys:' + name)
        for identity, value in old.items():
            actual = new[identity]
            require(set(value) == set(actual), 'ExactRedisFields:' + name)
            for field in value:
                if field != 'ttl':
                    require(same(value[field], actual[field]), 'ExactRedisBytes:' + name + ':' + field)
            if value['ttl'] == -1:
                require(actual['ttl'] == -1, 'PersistentTTLUnchanged')
            else:
                require(type(value['ttl']) is int and type(actual['ttl']) is int and 1 <= actual['ttl'] <= value['ttl'], 'ExistingFiniteTTLMonotonic')
    require(set(before['backend']) == set(after['backend']), 'AllKnownBackendIDsPresent')
    for ident, row in before['backend'].items():
        new = after['backend'][ident]
        require(set(row) == set(new), 'EveryBackendFieldPresent')
        for field in row:
            if field != 'ttl':
                require(same(row[field], new[field]), 'ExactBackendField:' + field)
        if row['raw_base64'] is None:
            require(row['ttl'] == new['ttl'] == -2, 'AbsentMetadataRemainsAbsent')
        else:
            require(1 <= new['ttl'] <= row['ttl'], 'ExistingBackendTTLMonotonic')
    from datetime import datetime
    require(datetime.fromisoformat(before['utc']) <= datetime.fromisoformat(response['began_utc']) <= datetime.fromisoformat(response['finished_utc']) <= datetime.fromisoformat(after['utc']), 'ActualOwnCommandUTCWindow')
    return {'passed': True, 'mode': mode, 'Native_preACK_refusal_verified': True, 'Source_failure_result_parity_verified': False, 'all_raw_fields_retained': True}
