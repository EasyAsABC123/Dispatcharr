"""Paired stock JSON Connect create using the owned integration fixture.

Original Dispatcharr authors retain all application and algorithm credit.
"""
from datetime import datetime, timezone


def run(http, baseline, candidate, accounts, record, fixture, digest):
    initial = fixture({'action': 'connect_create_snapshot'})
    initial_rows = initial['integrations']
    if any(row['name'].startswith('rust-probe-connect-create-') for row in initial_rows):
        raise RuntimeError('Connect create fixture already ran in this project')
    exercise(http, baseline, accounts, record, fixture, digest, 'source', initial)
    original = fixture({'action': 'connect_create_snapshot'})
    exercise(http, candidate, accounts, record, fixture, digest, 'native', original)
    final = fixture({'action': 'connect_create_snapshot'})
    rows = final['integrations'][len(initial_rows):]

    def content(row):
        return {key: value for key, value in row.items() if key not in ('id', 'created_at')}
    matched = (len(rows) == 6 and final['integrations'][:len(initial_rows)] == initial_rows
               and all(final[key] == initial[key] for key in ('subscriptions', 'delivery_logs'))
               and [content(row) for row in rows[:3]] == [content(row) for row in rows[3:]])
    record('connect-create-paired-state', matched,
           {'exact_paired_stored_content': matched, 'stored_integrations': len(rows),
            'subscriptions_unchanged': final['subscriptions'] == initial['subscriptions'],
            'delivery_logs_unchanged': final['delivery_logs'] == initial['delivery_logs']})


def exercise(http, target, accounts, record, fixture, digest, backend, initial):
    """Check one backend against the captured Original create contract."""
    route = '/api/connect/integrations/'
    admin = accounts['admin']['access']
    prefix = 'rust-probe-connect-create-'
    webhook = {'name': prefix+'webhook', 'type': 'webhook',
               'config': {'url': 'http://origin:8080/owned-connect-create-webhook',
                          'headers': {'X-Owned-Probe': 'literal'}}, 'enabled': True}
    def snapshot():
        return fixture({'action': 'connect_create_snapshot'})

    outbound_before = fixture({'action': 'connect_create_receiver_count'})['requests']
    issued = []

    def post(data, token=admin):
        issued.append(('POST', route))
        return http(target+route, body=data, token=token, method='POST', headers={'Accept': 'application/json'})

    def malformed(token):
        return post(b'{', token)

    def rejected(label, request, status, expected):
        before = snapshot()
        observed = request()
        unchanged = snapshot() == before
        record(f'connect-create-{backend}:'+label, observed == (status, expected) and unchanged,
               {'actual_status': observed[0], 'actual_body_sha256': digest(observed[1]),
                'database_rows_and_sequence_unchanged': unchanged})

    denied = {'detail': 'You do not have permission to perform this action.'}
    rejected('anonymous', lambda: post(webhook, None), 401, {'detail': 'Authentication credentials were not provided.'})
    rejected('standard', lambda: post(webhook, accounts['standard']['access']), 403, denied)
    rejected('viewer', lambda: post(webhook, accounts['viewer']['access']), 403, denied)
    rejected('inactive-admin', lambda: post(webhook, accounts['inactive']['access']), 401,
             {'detail': 'User is inactive', 'code': 'user_inactive'})
    rejected('role-before-json-parser', lambda: malformed(accounts['standard']['access']), 403, denied)
    rejected('admin-malformed-json', lambda: malformed(admin), 400,
             {'detail':'JSON parse error - Expecting property name enclosed in double quotes: line 1 column 2 (char 1)'})
    rejected('required-fields', lambda: post({}), 400,
             {'name': ['This field is required.'], 'type': ['This field is required.']})
    rejected('invalid-field-aggregation', lambda: post({'name': ' ', 'type': 'camel', 'config': None, 'enabled': 'maybe'}), 400,
             {'name': ['This field may not be blank.'], 'type': ['"camel" is not a valid choice.'],
              'config': ['This field may not be null.'], 'enabled': ['Must be a valid boolean.']})
    rejected('top-level-array', lambda: post([]), 400,
             {'non_field_errors': ['Invalid data. Expected a dictionary, but got list.']})
    for label, data in [('missing-webhook-url', {key: value for key, value in webhook.items() if key != 'config'}),
                        ('non-string-webhook-url', {**webhook, 'config': {'url': 17}})]:
        rejected(label, lambda data=data: post(data), 400, {'config': ["Webhook config must include a 'url' string"]})
    rejected('unsupported-model-type', lambda: post({**webhook, 'type': 'api'}), 400,
             {'type': ['Unsupported integration type']})
    missing_path = '/data/rust-probe-connect-create-missing-path'
    rejected('missing-local-script', lambda: post({**webhook, 'type': 'script', 'config': {'path': missing_path}}), 400,
             {'config': ['Script path does not exist: '+missing_path]})

    def created(label, payload, wanted):
        before = snapshot()
        started = datetime.now(timezone.utc)
        status, value = post(payload)
        completed = datetime.now(timezone.utc)
        after = snapshot()
        expected_id = before['sequence'][0] + int(before['sequence'][1])
        try:
            stamp = datetime.fromisoformat(value['created_at'])
            window = started <= stamp <= completed
            rendered = value == {**wanted, 'id': expected_id, 'created_at': value['created_at'], 'subscriptions': []}
            row = {key: field for key, field in value.items() if key != 'subscriptions'}
        except (KeyError, TypeError, ValueError):
            window, rendered, row = False, False, None
        effects = (row is not None and after['integrations'] == before['integrations']+[row]
                   and after['sequence'] == [expected_id, True]
                   and after['subscriptions'] == before['subscriptions'] == initial['subscriptions']
                   and after['delivery_logs'] == before['delivery_logs'] == initial['delivery_logs'])
        record(f'connect-create-{backend}:'+label, status == 201 and window and rendered and effects,
               {'actual_status': status, 'actual_body_sha256': digest(value), 'utc_creation_in_request_window': window,
                'exactly_one_integration_and_sequence_advance_without_changing_subscriptions_or_deliveries': effects})

    created('stock-webhook', {**webhook, 'name': ' '+webhook['name']+' ',
                             'id': 7, 'created_at': 'ignored', 'subscriptions': [{'event': 'channel_start'}]}, webhook)
    optional = {key: value for key, value in webhook.items() if key != 'enabled'}
    created('duplicate-name-and-default-enabled', optional, webhook)
    script = {'name': prefix+'script', 'type': 'script', 'config': {'path': '/data'}, 'enabled': False}
    created('existing-directory-script-path', script, script)
    outbound_after = fixture({'action': 'connect_create_receiver_count'})['requests']
    creation_only = issued == [('POST', route)] * 16
    record(f'connect-create-{backend}-no-dispatch', creation_only and outbound_before == outbound_after == 0,
           {'issued_only_collection_post': creation_only, 'creation_post_count': len(issued),
            'actual_owned_webhook_requests_before': outbound_before,
            'actual_owned_webhook_requests_after': outbound_after})
