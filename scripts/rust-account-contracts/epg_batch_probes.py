"""Observe stock React batch guide assignment in the packaged backends."""

from copy import deepcopy
from uuid import UUID

ROUTE = '/api/channels/channels/batch-set-epg/'
MISSING_ID = 2147483646


def association(channel, guide):
    return {'channel_id': channel, 'epg_data_id': guide}


def run(http, baseline, candidate, accounts, record, fixture, digest):
    owned = fixture({'action': 'epg_batch_seed'})
    for backend, origin in (('source', baseline), ('native', candidate)):
        exercise(http, origin, accounts, record, fixture, digest, owned, backend)


def exercise(http, origin, accounts, record, fixture, digest, owned, backend):
    channels = owned[backend]
    first, second, third = channels[:3]
    dummy = owned['dummy']
    admin = accounts['admin']['access']

    def post(body, token=admin, path=ROUTE):
        return http(origin + path, body=body, token=token, method='POST',
                    headers={'Accept': 'application/json', 'Content-Type': 'application/json'})

    def snapshot():
        return fixture({'action': 'epg_batch_snapshot'})

    def present(name, result, response_ok, effects):
        passed = response_ok and effects
        record(f'channel-batch-epg-{backend}:{name}', passed,
               {'actual_status': result[0], 'actual_body_sha256': digest(result[1]),
                'sql_cache_broker_match': effects})
        return passed

    def no_effect(name, body, status, expected, token=admin):
        before = snapshot()
        result = post(body, token)
        matches = result[0] == status and (expected is None or result[1] == expected)
        return present(name, result, matches, snapshot() == before)

    def mutate(name, associations, changes, path=ROUTE, published=None):
        before = snapshot()
        result = post({'associations': associations}, path=path)
        after = snapshot()
        expected = deepcopy(before)
        for row in expected['tables']['channel']:
            if row['id'] in changes:
                row['epg_data_id'] = changes[row['id']]
        queue_match = published is None
        if published is not None:
            jobs = after['jobs']
            queue = after['queue_frame_digests']
            if (len(jobs) == len(before['jobs']) + 1 and jobs[1:] == before['jobs']
                    and len(queue) == len(before['queue_frame_digests']) + 1
                    and queue[1:] == before['queue_frame_digests']):
                try:
                    job_id = UUID(jobs[0]['id'])
                except (KeyError, TypeError, ValueError):
                    job_id = None
                if (job_id is not None and all(jobs[0]['id'] != job['id'] for job in before['jobs'])
                        and jobs[0]['args'] == [published] and jobs[0]['kwargs'] == {}):
                    expected['jobs'] = jobs
                    expected['queue_frame_digests'] = queue
                    queue_match = True
        expected_body = {'success': True, 'channels_updated': len(changes),
                         'programs_refreshed': int(published is not None)}
        return present(name, result, result == (200, expected_body), after == expected and queue_match)

    required = {'error': 'associations list is required'}
    denied = {'detail': 'You do not have permission to perform this action.'}
    valid = {'associations': [association(first, dummy)]}
    no_effect('anonymous-before-parse', b'{', 401,
              {'detail': 'Authentication credentials were not provided.'}, None)
    no_effect('standard', valid, 403, denied, accounts['standard']['access'])
    no_effect('viewer', valid, 403, denied, accounts['viewer']['access'])
    no_effect('inactive-admin', valid, 401, {'detail': 'User is inactive', 'code': 'user_inactive'},
              accounts['inactive']['access'])
    no_effect('malformed-json', b'{', 400,
              {'detail': 'JSON parse error - Expecting property name enclosed in double quotes: line 1 column 2 (char 1)'})
    no_effect('missing-associations', {}, 400, required)
    no_effect('empty-associations', {'associations': []}, 400, required)
    no_effect('null-associations', {'associations': None}, 400, required)
    mutate('ignore-unknown', [association(MISSING_ID, dummy)], {})
    if not mutate('assign-two-with-unknown', [association(first, dummy), association(MISSING_ID, dummy),
                                              association(second, dummy)], {first: dummy, second: dummy}):
        raise RuntimeError('Batch EPG assignment prerequisite failed')
    mutate('repeat-assignment', [association(first, dummy), association(second, dummy)], {})
    mutate('last-association-wins', [association(first, dummy), association(first, None)], {first: None})
    mutate('missing-guide-clears', [{'channel_id': second}], {second: None})
    mutate('collection-ignores-filter', [association(third, dummy)], {third: dummy},
           path=ROUTE + f'?channel_profile_id={owned["profile"]}')
    if not mutate('assign-across-original-batches', [association(channel, dummy) for channel in channels],
                  dict.fromkeys((channel for channel in channels if channel != third), dummy)):
        raise RuntimeError('Larger Batch EPG assignment prerequisite failed')
    if not mutate('clear-across-original-batches', [association(channel, None) for channel in channels],
                  dict.fromkeys(channels)):
        raise RuntimeError('Larger Batch EPG clear prerequisite failed')
    with_xmltv = [association(first, dummy), association(second, owned['xmltv'])]
    if backend == 'source':
        mutate('source-xmltv-publication', with_xmltv, {first: dummy, second: owned['xmltv']},
               published=owned['xmltv'])
    else:
        no_effect('publisher-absent-atomic-guard', {'associations': with_xmltv}, 501, None)
