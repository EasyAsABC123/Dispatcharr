"""Selected scalar/body/audit contracts only, never full application parity.
Original Dispatcharr authors retain full application and algorithm credit.
"""
def comparison(source, native):
    def state(report, name, predicate=lambda row: True):
        return next((row for row in reversed(report['observations']) if row['name']==name and predicate(row)), None)
    def number(value):
        return int(value or 0)
    def scalar(report, row):
        if row is None:
            return None
        meta=row.get('metadata', {})
        origin=row.get('origin_vod', {})
        return {'metadata_present':bool(meta),
                'active':number(meta.get('active_streams')),
                'requests':number(meta.get('request_count')),
                'provider':number(row.get('provider_count')),
                'origin_requests':number(origin.get('requests'))-number(report['origin_fixture'].get('requests'))}
    selected={
        'after_loss':'old-body-after-loss-sampling',
        'reopen':'same-id-reopen-before-close',
        'additional_active_entry':'third-current-entry-open-old-and-younger-held',
        'old_progress':'old-body-beyond100th-yield-with-younger-sibling-held',
        'younger_after_old_close':'younger-beyond100th-yield-after-old-client-closed',
        'retired_reopen':'metadata-retired-reopen-while-younger-body-held',
        'younger_after_retired_reopen':'younger-extra-read-after-retired-public-reopen',
    }
    expected = {
        'after_loss': (False,0,0,0,1), 'reopen': (True,1,1,1,2),
        'additional_active_entry': (True,2,2,1,3), 'old_progress': (True,1,2,1,3),
        'younger_after_old_close': (False,0,0,0,3), 'retired_reopen': (True,1,1,1,4),
        'younger_after_retired_reopen': (True,1,1,1,4),
    }
    checks = {}
    for name,label in selected.items():
        wanted = dict(zip(['metadata_present','active','requests','provider','origin_requests'], expected[name]))
        checks[name] = all(scalar(report,state(report,label)) == wanted for report in [source,native])
    for key in ['same_id_reopen','additional_current_entry','retired_public_reopen']:
        checks[key+'_wire']=all(source.get(key,{}).get(field)==native.get(key,{}).get(field) for field in ['status','first_bytes','first_sha256']) and source.get(key,{}).get('status')==200
    checks['old_body']=all(source['old_body'].get(field)==native['old_body'].get(field)for field in ['bytes','sha256','ended','error'])and not native['old_body']['ended']and native['old_body']['error']is None
    checks['old_progress_bytes']=all(report.get('old_progress_after_reopen',{}).get('bytes_read')==827392 for report in [source,native])
    checks['delegated_progress_value']=all(number((state(report,selected['old_progress'])or{}).get('metadata',{}).get('bytes_sent'))==819200 for report in [source,native])
    checks['third_close_count']=all(report.get('third_close_returned_public_count_to_one')is True for report in [source,native])
    checks['old_close_public_effect']=all(report.get('old_close_public_effect_observed')is True for report in [source,native])
    checks['young_continuity']=all(source.get('younger_after_old_close',{}).get(field)==native.get('younger_after_old_close',{}).get(field)for field in ['chunks_read','bytes_read','ended','error'])and native.get('younger_after_old_close',{}).get('bytes_read')==843776 and not native.get('younger_after_old_close',{}).get('ended')
    checks['young_extra_bytes']=source.get('younger_after_retired_reopen')==native.get('younger_after_retired_reopen')and native.get('younger_after_retired_reopen',{}).get('bytes')==8192
    checks['prefixes']=all(report.get('body_prefix_matches_origin')and report.get('reopened_body_starts_at_prefix')and report.get('old_body_after_reopen_matches_offset')for report in [source,native])
    checks['audit_event_order']=[row['event_type']for row in source['events']]==[row['event_type']for row in native['events']]
    if source.get('viewer_auth')or native.get('viewer_auth'):
        checks['authenticated_capped_viewer']=source.get('viewer_auth')==native.get('viewer_auth')and all(report.get('authenticated_viewer_observed')is True for report in [source,native])
        stripped=source.get('authorization_omitted_after_loss') is True
        if stripped or native.get('authorization_omitted_after_loss'):
            checks['headerless_after_loss_subject']=stripped and native.get('authorization_omitted_after_loss') is True
        wanted='unknown'if stripped else str(source.get('viewer_auth',{}).get('user_id'))
        checks['current_hash_user_identity']=all((state(report,label)or{}).get('metadata',{}).get('user_id')==wanted for report in [source,native]for name,label in selected.items()if name not in ['after_loss','younger_after_old_close'])
    return {'checks':checks,'passed':all(checks.values()),
            'scope':'Selected actualHTTP all-missing-restart scalar/body/audit-type schedule; no complete metadata DTO/event IP/timestamps, WebSocket, partial-loss, default application or resource acceptance.',
            'counter_note':'Missing provider counter and zero both represent zero, as Source public counter readers do; origin request counts are relative to each capture baseline.'}
