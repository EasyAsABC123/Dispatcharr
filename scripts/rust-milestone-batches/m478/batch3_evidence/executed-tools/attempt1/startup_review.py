"""Offline Source-normal-context and finite-signature qualification, no apps."""
import json
from gpg_trace import need

def qualify(out):
    x=json.loads((out/'native-startup.json').read_bytes());r=json.loads((out/'startup-finite-result.json').read_bytes());s={v['label']:v for v in x['steps']};q={v['label']:v for v in x['requests']}
    checks={
        'cache4_plus_finite2_complete':x['capture_complete']and x['actual_HTTP_calls']==6,
        'no_Original_application_runs':x['Original_application_runs']==0,
        'TCP_only_readiness':x['HTTP_readiness_requests']==0 and all(not v['HTTP_request_issued']and v['socket_closed']for v in x['TCP_readiness']),
        'group_absent_before_Main':not s['before-Main-start']['system_cache']['present'],
        'group_warm_before_first_HTTP':s['after-Main-start-before-HTTP']['system_cache'].get('typed_value')=={'time_zone':'UTC'},
        'first_HTTP_group_typed_UTC':s['first-request']['system_cache'].get('typed_value')=={'time_zone':'UTC'},
        'actual_setter_invalidates':not s['actual-settings-setter']['system_cache']['present'],
        'setter_and_immediate_window_under5s':q['immediate-after-setter']['completed_monotonic']-q['actual-settings-setter']['began_monotonic']<5,
        'immediate_GET_leaves_group_absent':not s['immediate-after-setter']['system_cache']['present'],
        'actual_expired_age_lower_bound':q['expired-after-setter']['began_monotonic']-q['actual-settings-setter']['completed_monotonic']>=5.05,
        'expired_GET_repopulates_typed_UTC':s['expired-after-setter']['system_cache'].get('typed_value')=={'time_zone':'UTC'},
        'all6_HTTP200':all(v['status']==200 for v in q.values()),
        'all6_HTTP_socket_timer_closed':all(v['physical_close']['original_inode_absent']and v['physical_close']['timer_joined']for v in q.values()),
        'default6259_unchanged':x['default_unchanged']and x['default_before']==x['default_after'],
        'normal_Main_exit0_backends_absent':r['actual_Main_exit']==0 and r['backends_after_Main']==[],
        'finite_valid_signature_true':q['finite-signed-valid']['json']['signature_verified']is True,
        'finite_changed_signature_false':q['finite-signed-changed']['json']['signature_verified']is False,
        'actual_GPG_wait_EOF_homes':r['GPG']['passed']and r['GPG']['actual_children']==4 and r['GPG']['actual_homes_removed']==2,
        'actual_GPG_live_namespace_absence':r['live_namespace_child_home_absence'],
        'owned_finite_Origin_joined':x['finite_origin_joined']and len(x['finite_origin_events'])==2,
    }
    result={'passed':all(checks.values()),'checks':checks,'Original_functions_replaced':False,'Source41_replays':0,'cache_authority':'actual Original normal WSGI startup15 checks; Source checked=0/zoneNone before first HTTP','Source_numeric_authority':'Python builtin Original canonical expression plus actual independent tool GPG waits0/1; no Source API execution','pickle_scope':'typed primitive interoperability; raw serialization bytes are not required identical','full_M8_acceptance':False,'adversarial_Plugin_actor_drain_qualified':False,'performance_claims':False}
    (out/'startup-finite-review.json').write_text(json.dumps(result,indent=2)+'\n');need(result['passed'],'ActualStartupFiniteScopeAllChecks');return result
