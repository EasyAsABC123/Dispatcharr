"""Finite actual Native preACK refusal acceptor; no Source error projection."""
from refusal_contract import REASONS, raw_child_context, validate as refusal, require
from seed_spec import REFRESH, SERIES, QUEUES

def casefold_setup_proof(mutation):
    value = mutation['actual_casefold_SQL']
    require(value['sql'] == 'SELECT m.id,m.name FROM vod_movie m WHERE UPPER(m.name::text)=UPPER(%s) ORDER BY m.name LIMIT 2' and value['parameters'] == ['Owned Parented Artwork'] and value['Native_Store_predicate'] == 'UPPER(m.name::text)=UPPER($1)', 'ExactActualStoreCasefoldPredicate')
    rows = value['rows']
    require(type(rows) is list and len(rows) == 2 and all(set(row) == {'id', 'name'} and type(row['id']) is int and row['id'] > 0 and type(row['name']) is str for row in rows), 'TwoActualTypedCasefoldRows')
    require({row['id'] for row in rows} == {mutation['movie_id'], mutation['actual_duplicate_id']} and mutation['movie_id'] != mutation['actual_duplicate_id'], 'ActualDistinctOwnedMovieIDs')
    require({row['name'] for row in rows} == {'Owned Parented Artwork', 'OWNED PARENTED ARTWORK'}, 'ValidUniqueDistinctASCIINameSpellings')
    return True

def validate(code, report, reference):
    require(type(code) is int and code == 0, 'ActualGuest0')
    require(report['passed'] is True and report['Native_verified'] is True and report['Source_verified'] is False and report['performance_verified'] is False, 'NativeOnlyRefusalScope')
    require(len(report['cases']) == 3, 'TwoActualOriginalParentsOneRefusal')
    first, parent, case = report['cases']
    require(case['name'] in REASONS and report['predeclared_cases'] == [first['name'], parent['name'], case['name']], 'ExactPredeclaredMode')
    require(first['role'] == 'refresh' and parent['role'] == 'series' and len(first['tasks']) == len(parent['tasks']) == 1, 'ActualOriginalParentRoles')
    require([p['task'] for p in report['all_prerun']] == [REFRESH, SERIES], 'OnlyOriginalParentsConsumed')
    require(len(report['worker_exit_receipts']) == 2, 'TwoActualSourceWorkerReceipts')
    for worker in report['worker_exit_receipts']:
        require(worker['actual_exit'] == 0 and worker['actual_wait_completed'] is True and worker['pipes_settled'] is True and worker['all_observed_owned_PIDs_absent'] is True and worker['forced'] is False and worker['invalid'] is None, 'ActualSourceParentWorkerSettlement')
    require(all(c['passed'] is True for c in report['checks']) and report['finally_errors'] == [], 'NoHiddenSetupOrFinallyFailure')
    require(report['all_HTTP_attempts'] == [], 'NoProviderOrOwnedURLNetworkContact')
    require(case['Native_refusal_qualified'] is True and case['role'] == 'Native-only-preACK-refusal' and case['tasks'] == [], 'NoInventedSourceNativeTaskPrerun')
    execution = case['execution']
    require(execution['passed'] is True and execution['finally_errors'] == [] and execution['case_elapsed_seconds'] <= 90, 'ActualRefusalCaseSettlement')
    context = raw_child_context(execution['before'], parent['tasks'][0], first['tasks'][0], QUEUES['artwork'])
    require(context == execution['context'], 'ActualOriginalContextNotBoolAuthority')
    derived = refusal(case['name'], execution['before'], execution['after'], execution['response'], execution['native_process'], QUEUES['artwork'], context)
    require(execution['derived_validation'] == derived, 'RecomputedAllRefusalEffects')
    require(execution['before']['queues']['celery'] == execution['after']['queues']['celery'] == report['final']['queues']['celery'], 'DefaultDescendantsNeverDeletedOrConsumed')
    require(execution['before']['queues'][QUEUES['artwork']] == report['final']['queues'][QUEUES['artwork']], 'BothRawArtworkChildrenRemainUntouched')
    mutation = case['fixture_mutation']
    require(mutation['mode'] == case['name'] and mutation['database'] == report['actual_database'] and mutation['recording_id'] == context['recording_id'] and mutation['Native_task_started'] is False, 'ExplicitOwnedMutationBeforeNative')
    if case['name'] == 'duplicate-casefold-movie':
        casefold_setup_proof(mutation)
    return {'passed': True, 'mode': case['name'], 'Original_setup_tasks': 2, 'Native_preACK_domain_refusal_verified': True, 'raw_children_retained': 2, 'Native_terminal_metadata_created': False, 'Source_failure_result_parity_verified': False, 'Main_default_graph_verified': False, 'performance_verified': False}
