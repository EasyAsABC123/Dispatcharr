"""Exercise the stock Connect create, edit, toggle and delete HTTP sequence.

Original Dispatcharr authors retain full application and algorithm credit.
"""

PREFIX = 'rust-probe-connect-lifecycle-'
ROUTE = '/api/connect/integrations/'
RECEIVER = 'http://origin:8080/owned-connect-create-webhook'
# The current React form sends every key in SUBSCRIPTION_EVENTS, in this order.
EVENTS = (
    'channel_start', 'channel_stop', 'channel_reconnect', 'channel_error', 'channel_buffering',
    'channel_failover', 'stream_switch', 'recording_start', 'recording_end', 'epg_refresh',
    'epg_error', 'm3u_refresh', 'm3u_error', 'client_connect', 'client_disconnect', 'login_failed',
    'epg_blocked', 'm3u_blocked', 'vod_start', 'vod_stop',
)


def next_id(state, key):
    value, called = state['sequences'][key]
    return value + int(called)


def with_rows(state, key, rows, sequence=None):
    result = {**state, key: rows}
    if sequence is not None:
        result['sequences'] = {**state['sequences'], key: [sequence, True]}
    return result


def by_id(rows, item_id):
    return [row for row in rows if row['id'] == item_id]


def without_id(rows, item_id):
    return [row for row in rows if row['id'] != item_id]


def run(http, baseline, candidate, accounts, record, fixture, digest):
    def snapshot():
        return fixture({'action': 'connect_lifecycle_snapshot'})

    initial = snapshot()
    sibling = fixture({'action': 'connect_lifecycle_seed_sibling'})
    state = snapshot()
    expected = initial
    for table, key in (('integrations', 'integration'), ('subscriptions', 'subscription'), ('logs', 'log')):
        item_id = next_id(expected, table)
        rows = by_id(state[table], sibling[key])
        if sibling[key] != item_id or len(rows) != 1:
            raise RuntimeError('Connect lifecycle sibling allocation differed from the fixture sequence')
        expected = with_rows(expected, table, expected[table] + rows, item_id)
    if state != expected:
        raise RuntimeError('Connect lifecycle sibling setup changed another fixture row')
    suites = []
    for backend, target in (('source', baseline), ('native', candidate)):
        results, state = exercise(http, target, accounts['admin']['access'], record, fixture,
                                  digest, backend, state, snapshot)
        suites.append(results)

    conserved = all(state[table] == expected[table] for table in ('integrations', 'subscriptions', 'logs'))
    comparison = suites[0] == suites[1] and conserved
    record('connect-lifecycle:paired', comparison, {
        'equivalent_ordered_http_results': suites[0] == suites[1],
        'unrelated_integration_subscription_and_delivery_rows_preserved': conserved,
        'steps_per_backend': len(suites[0]),
    })


def exercise(http, endpoint, token, record, fixture, digest, backend, initial, snapshot):
    name = PREFIX + backend
    desired = {'name': name, 'type': 'webhook', 'config': {'url': RECEIVER}, 'enabled': True}
    outcomes = []
    count_before = fixture({'action': 'connect_create_receiver_count'})['requests']

    def send(method, route, body=None):
        return http(endpoint + route, body=body, token=token, method=method, headers={'Accept': 'application/json'})

    def normalize_detail(response):
        status, body = response
        if not isinstance(body, dict) or not isinstance(body.get('subscriptions'), list):
            return response
        rows = body['subscriptions']
        if any(not isinstance(row, dict) or type(row.get('id')) is not int for row in rows):
            return response
        return status, {**body, 'subscriptions': sorted(rows, key=lambda row: row['id'])}

    def verify(label, observed, required, state, expected):
        exact_response = normalize_detail(observed) == normalize_detail(required)
        exact_state = state == expected
        passed = exact_response and exact_state
        record(f'connect-lifecycle-{backend}:{label}', passed, {
            'actual_status': observed[0],
            'actual_body_sha256': digest(observed[1]),
            'exact_http_response': exact_response,
            'exact_rows_and_sequences': exact_state,
        })
        if not passed:
            raise RuntimeError('Connect lifecycle prerequisite failed')
        outcomes.append((label, observed[0]))

    integration_id = next_id(initial, 'integrations')
    detail = f'{ROUTE}{integration_id}/'
    subscriptions_route = detail + 'subscriptions/set/'
    created = send('POST', ROUTE, desired)
    state = snapshot()
    rows = by_id(state['integrations'], integration_id)
    stamp = rows[0]['created_at'] if rows else None
    row = {**desired, 'id': integration_id, 'created_at': stamp}
    verify('create', created, (201, {**row, 'subscriptions': []}), state,
           with_rows(initial, 'integrations', initial['integrations'] + [row], integration_id))

    selected = [
        {'event': event, 'enabled': event in {'recording_end', 'epg_blocked'},
         'payload_template': '{{ event }}' if event == 'recording_end' else None}
        for event in EVENTS
    ]
    first_id = next_id(state, 'subscriptions')
    subscriptions = [{**item, 'id': first_id + offset, 'integration': integration_id}
                     for offset, item in enumerate(selected)]
    result = send('PUT', subscriptions_route, selected)
    after = snapshot()
    verify('set', result, (200, subscriptions), after,
           with_rows(state, 'subscriptions', state['subscriptions'] + subscriptions, subscriptions[-1]['id']))
    state = after

    seeded = fixture({'action': 'connect_lifecycle_seed_logs', 'integration': integration_id, 'name': name})
    after = snapshot()
    legacy_id = next_id(state, 'subscriptions')
    legacy = {'id': legacy_id, 'integration': integration_id, 'event': 'download_complete',
              'enabled': False, 'payload_template': None}
    logs = [log for log in after['logs'] if log['id'] in seeded['logs']]
    log_id = next_id(state, 'logs')
    recording = subscriptions[EVENTS.index('recording_end')]
    seeded_state = with_rows(state, 'subscriptions', state['subscriptions'] + [legacy], legacy_id)
    seeded_state = with_rows(seeded_state, 'logs', state['logs'] + logs, log_id + 1)
    if (seeded['subscription'] != legacy_id or seeded['logs'] != [log_id, log_id + 1]
            or [log['subscription_id'] for log in logs] != [recording['id'], legacy_id]
            or after != seeded_state):
        raise RuntimeError('Connect lifecycle log setup affected unexpected SQL rows or sequences')
    retained_log, stale_log = logs
    state = after

    edited = {**desired, 'name': name + '-edited', 'config': {'url': RECEIVER, 'headers': {'X-Owned-Probe': 'edited'}}}
    row = {**row, **edited}
    result = send('PUT', detail, edited)
    after = snapshot()
    replaced = [row if item['id'] == integration_id else item for item in state['integrations']]
    verify('edit', result, (200, {**row, 'subscriptions': subscriptions + [legacy]}), after,
           with_rows(state, 'integrations', replaced))
    state = after

    changed = [{**item, 'payload_template': '{{ integration }}'} if item['event'] == 'recording_end'
               else item for item in selected]
    subscriptions = [{**row, **values} for row, values in zip(subscriptions, changed, strict=True)]
    result = send('PUT', subscriptions_route, changed)
    after = snapshot()
    expected = with_rows(state, 'subscriptions', initial['subscriptions'] + subscriptions)
    expected = with_rows(expected, 'logs', without_id(state['logs'], stale_log['id']))
    verify('edit-subscriptions', result, (200, subscriptions), after, expected)
    state = after

    row = {**row, 'enabled': False}
    result = send('PUT', detail, {**row, 'subscriptions': subscriptions})
    after = snapshot()
    replaced = [row if item['id'] == integration_id else item for item in state['integrations']]
    verify('toggle', result, (200, {**row, 'subscriptions': subscriptions}), after,
           with_rows(state, 'integrations', replaced))
    state = after

    result = send('DELETE', detail)
    after = snapshot()
    expected = with_rows(state, 'integrations', without_id(state['integrations'], integration_id))
    expected = with_rows(expected, 'subscriptions', initial['subscriptions'])
    expected = with_rows(expected, 'logs', without_id(expected['logs'], retained_log['id']))
    verify('delete', result, (204, {'non_json': True, 'length': 0}), after, expected)
    count_after = fixture({'action': 'connect_create_receiver_count'})['requests']
    record(f'connect-lifecycle-{backend}:receiver', count_before == count_after == 0, {
        'owned_receiver_before': count_before, 'owned_receiver_after': count_after,
    })
    return outcomes, after
