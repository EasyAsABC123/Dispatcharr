"""Unexecuted strict two-case checks; actual fixture collection is separately gated.
Original Dispatcharr authors retain application and algorithm credit.
"""
from datetime import datetime

GROUPS = 'apps.m3u.tasks.refresh_m3u_groups'
SINGLE = 'apps.m3u.tasks.refresh_single_m3u_account'

def require(condition, label):
    if not condition:
        raise AssertionError(label)

def instant(value):
    require(type(value) is str, 'TimestampString')
    parsed = datetime.fromisoformat(value)
    require(parsed.tzinfo is not None, 'AwareTimestamp')
    return parsed

def child_entry(case, task):
    publications = [v for v in case['publications'] if v['task'] == task]
    require(len(publications) == 1, 'ExactOneChildFamily:' + task)
    publication = publications[0]
    ident = publication['headers']['id']
    require(ident in case['native_ids'], 'ActualNativeExecution:' + task)
    require(case['source_fallback'] is False, 'NoOriginalFallback')
    metadata = case['results'][ident]['metadata']
    require(metadata['task_id'] == ident, 'TaskIdentity')
    require(metadata['parent_id'] == case['parent_id'], 'ResultParent')
    require(publication['headers']['root_id'] == case['parent_id'], 'RootIdentity')
    require(metadata['children'] == [], 'EmptyChildMetadata')
    require(3500 <= case['results'][ident]['ttl'] <= 3600, 'UnchangedResultTTL')
    return ident, metadata

def known_results(reference, actual):
    require(reference['name'] == actual['name'], 'CaseIdentity')
    require(set(actual['native_ids']) == {
        v['headers']['id'] for v in actual['publications'] if v['task'] in (GROUPS, SINGLE)
    }, 'BothActualNativeChildren')
    group_id, groups = child_entry(actual, GROUPS)
    _, single = child_entry(actual, SINGLE)
    original_group = next(v for v in reference['postruns'] if v['task'] == GROUPS)
    original_single = next(v for v in reference['postruns'] if v['task'] == SINGLE)
    expected = reference['results'][original_group['id']]['metadata']
    require(set(groups) == set(expected), 'ExactFailureMetadataFields')
    require(groups['status'] == 'FAILURE', 'GroupsActualFailure')
    require(groups['result'] == expected['result'], 'ExactModuleClassTypedArgs')
    require(single['status'] == 'SUCCESS' and single['traceback'] is None, 'SingleActualNormalReturn')
    require(type(single['result']) is str and single['result'] == original_single['result'], 'SingleExactStringNotNull')
    require(type(groups['traceback']) is str and groups['traceback'].startswith('Native Dispatcharr M3U groups diagnostic'), 'TruthfulNativeDiagnostic')
    require('Traceback (most recent call last)' not in groups['traceback'], 'NoInventedPythonFrames')
    require(groups['traceback'] != expected['traceback'], 'TracebackDifferenceExplicit')
    window = actual['command_windows'][group_id]
    require(instant(window['began_utc']) <= instant(groups['date_done']) <= instant(window['finished_utc']), 'NativeCompletionInActualCommandInterval')
    # Source4 did not retain equivalent command-clock receipts; do not invent them.
    require(actual['source_command_intervals'] is None, 'HistoricalSourceIntervalUnavailable')
    parent_done = actual['results'][actual['parent_id']]['metadata']['date_done']
    require(instant(parent_done) <= instant(groups['date_done']) <= instant(single['date_done']), 'ObservedTaskResultOrder')
    return {'typed_results': True, 'source_traceback_equal': False,
            'source_command_intervals_qualified': False, 'native_command_interval_qualified': True}

def residual_state(reference, actual):
    source_account = reference['after']['accounts'][0]
    account = actual['after']['accounts'][0]
    for key in ('status', 'last_message', 'locked'):
        require(key in source_account and key in account and account[key] == source_account[key], 'AccountField:' + key)
    key = 'task_lock_refresh_m3u_account_groups_' + str(account['id'])
    require(key in actual['after']['locks'], 'ResidualGroupsKey')
    lock = actual['after']['locks'][key]
    require(type(lock['raw_base64']) is str and lock['raw_base64'], 'RetainedRawToken')
    require(1 <= lock['ttl'] <= 300, 'ResidualFiniteTTL')
    # Raw unique owner token and stopped renewal are deliberate declared differences.
    require(actual['lease_differences'] == {
        'raw_token_equal': False, 'native_renewal_stopped': True,
        'Source_renewal_after_120s_qualified': False,
    }, 'LeaseDifferenceNotHidden')
    require(actual['all_owned_sql_and_redis_physically_settled'] is True, 'PhysicalSettlement')
    return {'residual_key_observed': True, 'full_lease_parity': False}

def preserved_temporal_fields(reference, actual):
    """Check changed/unchanged relations; never remove timestamp fields for equality.

    Full rows remain in reports. Generated Native times need actual command-window
    receipts at collection time. Source clock receipts absent in historical4 are
    explicitly unavailable, not made from the values under test.
    """
    checked = 0
    for table in ('accounts', 'profiles', 'streams', 'all_owned_streams', 'groups', 'group_links', 'system_events'):
        original_before = {r['id']: r for r in reference['before'][table]}
        original_after = {r['id']: r for r in reference['after'][table]}
        native_before = {r['id']: r for r in actual['before'][table]}
        native_after = {r['id']: r for r in actual['after'][table]}
        # The reviewed replay keeps Source fixture IDs, rather than outcome-driven rebinding.
        require(set(original_after) == set(native_after), 'ExactSeededRowIdentity:' + table)
        for ident, row in original_after.items():
            require(set(row) == set(native_after[ident]), 'NoDroppedFields:' + table)
            temporal = ('created_at', 'updated_at', 'last_seen', 'timestamp')
            for key in row:
                if key not in temporal:
                    require(row[key] == native_after[ident][key], 'StaticField:' + table + ':' + key)
            for key in temporal:
                if key not in row:
                    continue
                expected, observed = row[key], native_after[ident][key]
                require((expected is None) == (observed is None), 'TemporalNull:' + table + key)
                if expected is not None:
                    instant(expected); instant(observed)
                    if ident in original_before and ident in native_before:
                        was_changed = original_before[ident].get(key) != expected
                        require((native_before[ident].get(key) != observed) == was_changed, 'TemporalEffect:' + table + key)
                        if not was_changed:
                            require(observed == native_before[ident][key], 'UnchangedNativeTimestamp')
                            checked += 1
                            continue
                    window = actual['temporal_windows'][table][str(ident)][key]
                    require(instant(window['began_utc']) <= instant(observed) <= instant(window['finished_utc']), 'ActualTemporalWindow')
                checked += 1
    return checked
