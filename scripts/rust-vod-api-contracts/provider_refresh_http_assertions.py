"""UNEXECUTED draft: narrow Source-derived refresh HTTP/effects comparisons.
Original Dispatcharr authors retain application/algorithm credit. Raw responses
remain authoritative; only explicitly SQL-bound new UUID/changed clock spans may
be replaced when deriving a secondary comparison template. No whitespace/order
normalization and no Main/package/performance acceptance.
"""
import base64
import datetime
import json
import uuid

CLOCK_FIELDS = {'created_at', 'updated_at', 'last_advanced_refresh',
                'last_episode_refresh', 'last_seen', 'last_refreshed'}


def compare_effects(source, native, source_before, source_interval, native_interval, encoder, controlled_equal_paths=()):
    """Compare complete ordered rows; never invent an ID/field sort."""
    failures, clock_observations, entropy = [], [], []
    if list(source) != list(native):
        failures.append({'kind': 'table_order_or_set'})
    for table, expected in source.items():
        actual = native.get(table, [])
        if [row['id'] for row in expected] != [row['id'] for row in actual]:
            failures.append({'table': table, 'kind': 'ordered_ID_set'})
            continue
        old = {row['id']: row for row in source_before[table]}
        for left, right in zip(expected, actual):
            if list(left) != list(right):
                failures.append({'table': table, 'id': left['id'], 'kind': 'field_order_or_set'})
            for field, value in left.items():
                before = old.get(left['id'], {}).get(field)
                if (table, left['id'], field) in controlled_equal_paths:
                    if json.dumps(value, cls=encoder, separators=(',', ':')) != json.dumps(right.get(field), cls=encoder, separators=(',', ':')):
                        failures.append({'table': table, 'id': left['id'], 'field': field, 'kind': 'strict_predeclared_controlled_input'})
                elif field in CLOCK_FIELDS and value != before:
                    a = isinstance(value, datetime.datetime) and source_interval[0] <= value <= source_interval[1]
                    other = right.get(field)
                    b = isinstance(other, datetime.datetime) and native_interval[0] <= other <= native_interval[1] and other != before
                    clock_observations.append({'table': table, 'id': left['id'], 'field': field,
                                               'Source_raw': value, 'native_raw': other,
                                               'Source_own_interval': a, 'native_own_interval': b})
                    if not (a and b):
                        failures.append({'table': table, 'id': left['id'], 'field': field, 'kind': 'causal_clock'})
                elif table == 'vod_episode' and field == 'uuid' and left['id'] not in old:
                    try:
                        valid = uuid.UUID(str(value)).version == uuid.UUID(str(right[field])).version == 4
                    except (ValueError, KeyError, TypeError):
                        valid = False
                    entropy.append({'episode_id': left['id'], 'Source_raw': str(value),
                                    'native_raw': str(right.get(field)), 'new_UUIDv4': valid})
                    if not valid:
                        failures.append({'table': table, 'id': left['id'], 'field': field, 'kind': 'new_UUIDv4'})
                elif json.dumps(value, cls=encoder, separators=(',', ':')) != json.dumps(right.get(field), cls=encoder, separators=(',', ':')):
                    failures.append({'table': table, 'id': left['id'], 'field': field, 'kind': 'value'})
    return {'differences': failures, 'own_interval_clocks': clock_observations,
            'new_episode_UUIDv4': entropy}


def _spans(text):
    """Map exact JSON value spans without reformatting the response text."""
    decoder, found = json.JSONDecoder(), {}

    def space(at):
        while at < len(text) and text[at] in ' \r\n\t':
            at += 1
        return at

    def walk(at, path):
        at = space(at)
        start = at
        if text[at] == '{':
            at = space(at + 1)
            if text[at] != '}':
                while True:
                    key, at = decoder.raw_decode(text, at)
                    assert isinstance(key, str)
                    at = space(at)
                    assert text[at] == ':'
                    at = space(walk(at + 1, path + (key,)))
                    if text[at] == '}':
                        break
                    assert text[at] == ','
                    at = space(at + 1)
            end = at + 1
        elif text[at] == '[':
            at, index = space(at + 1), 0
            if text[at] != ']':
                while True:
                    at = space(walk(at, path + (index,)))
                    index += 1
                    if text[at] == ']':
                        break
                    assert text[at] == ','
                    at = space(at + 1)
            end = at + 1
        else:
            _, end = decoder.raw_decode(text, at)
        found[path] = (start, end)
        return end

    end = space(walk(0, ()))
    assert end == len(text)
    return found


def compare_reply(source, native, kind, parent_id, source_before, source_after, native_after):
    """Raw equality first; secondary template only for declared SQL-bound fields.

    Content-Length remains strict across sides. Both sides must additionally
    report their own byte length. Any clock-width delta remains an explicit
    failure until separately characterized; no automatic header waiver.
    """
    failures, exceptions = [], []
    if source['status'] != native['status']:
        failures.append({'kind': 'status'})
    if source['headers'] != native['headers']:
        failures.append({'kind': 'headers'})
    a, b = base64.b64decode(source['body_base64']), base64.b64decode(native['body_base64'])
    for label, record, raw in [('Source', source, a), ('native', native, b)]:
        length = record['headers'].get('Content-Length')
        # HEAD has no response body, although its representation length is sent.
        if record.get('method', 'GET') != 'HEAD' and length is not None and int(length) != len(raw):
            failures.append({'target': label, 'kind': 'own_Content_Length'})
    if a == b:
        return {'passed': not failures, 'raw_body_equal': True,
                'differences': failures, 'SQL_bound_exceptions': exceptions}
    try:
        text_a, text_b = a.decode('utf-8'), b.decode('utf-8')
        left, right = json.loads(text_a), json.loads(text_b)
        spans_a, spans_b = _spans(text_a), _spans(text_b)
    except (UnicodeError, ValueError, IndexError, AssertionError):
        return {'passed': False, 'raw_body_equal': False,
                'differences': failures + [{'kind': 'raw_nonJSON_body'}], 'SQL_bound_exceptions': []}
    old_episodes = {row['id'] for row in source_before['vod_episode']}
    source_episodes = {row['id']: row for row in source_after['vod_episode']}
    native_episodes = {row['id']: row for row in native_after['vod_episode']}
    allowed = set()

    def walk(x, y, path):
        if type(x) is not type(y):
            failures.append({'path': path, 'kind': 'JSON_type'})
            return
        if isinstance(x, dict):
            if list(x) != list(y):
                failures.append({'path': path, 'kind': 'ordered_JSON_keys'})
                return
            for key in x:
                next_path = path + (key,)
                if key == 'uuid' and 'episodes' in path and x.get('id') not in old_episodes:
                    episode_id = x.get('id')
                    sa, nb = source_episodes.get(episode_id), native_episodes.get(episode_id)
                    valid = bool(sa and nb and x[key] == str(sa['uuid']) and y[key] == str(nb['uuid'])
                                 and uuid.UUID(x[key]).version == uuid.UUID(y[key]).version == 4)
                    if valid:
                        allowed.add(next_path)
                        exceptions.append({'path': next_path, 'kind': 'new_episode_UUIDv4',
                                           'episode_id': episode_id, 'Source_raw': x[key], 'native_raw': y[key],
                                           'each_equals_own_committed_SQL': True})
                        continue
                if key == 'last_refreshed' and kind == 'series' and not path and x[key] != y[key]:
                    # Pinned api_views.py638 returns series.updated_at here.
                    table, clock_field = 'vod_series', 'updated_at'
                    before = next(row for row in source_before[table] if row['id'] == parent_id)
                    sa = next(row for row in source_after[table] if row['id'] == parent_id)
                    nb = next(row for row in native_after[table] if row['id'] == parent_id)
                    if sa.get(clock_field) != before.get(clock_field) and isinstance(sa.get(clock_field), datetime.datetime) and isinstance(nb.get(clock_field), datetime.datetime):
                        from rest_framework.fields import DateTimeField
                        format_clock = DateTimeField().to_representation
                        if x[key] == format_clock(sa[clock_field]) and y[key] == format_clock(nb[clock_field]):
                            allowed.add(next_path)
                            exceptions.append({'path': next_path, 'kind': 'changed_SQL_clock',
                                               'table': table, 'field': clock_field, 'id': parent_id,
                                               'Source_raw': x[key], 'native_raw': y[key],
                                               'each_equals_own_SQL_using_Source_renderer': True})
                            continue
                walk(x[key], y[key], next_path)
        elif isinstance(x, list):
            if len(x) != len(y):
                failures.append({'path': path, 'kind': 'ordered_JSON_length'})
                return
            for index, (a_item, b_item) in enumerate(zip(x, y)):
                walk(a_item, b_item, path + (index,))
        elif json.dumps(x, separators=(',', ':')) != json.dumps(y, separators=(',', ':')):
            failures.append({'path': path, 'kind': 'JSON_scalar'})

    walk(left, right, ())

    def template(text, spans):
        edits = sorted((spans[path][0], spans[path][1], json.dumps('__SQL_BOUND_' + repr(path))) for path in allowed)
        result, at = [], 0
        for start, end, replacement in edits:
            result.extend([text[at:start], replacement])
            at = end
        result.append(text[at:])
        return ''.join(result)

    if template(text_a, spans_a) != template(text_b, spans_b):
        failures.append({'kind': 'raw_template_outside_exact_SQL_bound_value_spans'})
    return {'passed': not failures, 'raw_body_equal': False,
            'differences': failures, 'SQL_bound_exceptions': exceptions,
            'comparison_scope': 'No whitespace/order normalization; exact raw template outside named SQL-bound value spans'}
