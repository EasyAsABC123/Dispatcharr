"""Read captured normal context; never runs an application or repairs observations."""
import json,sys
from pathlib import Path
p=Path(sys.argv[1]);x=json.loads((p/'normal-startup.json').read_bytes());report=json.loads((p/'report.json').read_bytes())
steps={r['label']:r for r in x['steps']};requests={r['label']:r for r in x['requests']}
checks={
 'complete_four_new_HTTP_calls':x['capture_complete']and x['actual_HTTP_calls']==4 and len(requests)==4,
 'actual_normal_startup_branch':x['actual_should_skip_initialization']is False and x['actual_process_role']=='django',
 'initial_cache_absent':not steps['before-Original-import']['system_cache']['present'],
 'normal_startup_populates_group':steps['after-normal-startup-before-HTTP']['system_cache'].get('typed_value')=={'time_zone':'UTC'},
 'first_HTTP_has_group':steps['first-request']['system_cache'].get('typed_value')=={'time_zone':'UTC'},
 'actual_HTTP_setter_invalidates':not steps['actual-settings-setter']['system_cache']['present'],
 'immediate_request_inside_debounce':requests['immediate-after-setter']['before_display_timezone']['age_seconds']<5 and not steps['immediate-after-setter']['system_cache']['present'],
 'expired_request_observes_real_clock':requests['expired-after-setter']['before_display_timezone']['age_seconds']>=5.05,
 'expired_request_repopulates':steps['expired-after-setter']['system_cache'].get('typed_value')=={'time_zone':'UTC'},
 'HTTP_statuses':all(r['status']==200 for r in requests.values()),
 'all_HTTP_socket_timer_closes':all(r['physical_close']['original_inode_absent']and r['physical_close']['timer_joined']for r in requests.values()),
 'unchanged_default6259':x['default_unchanged']and x['default_before']==x['default_after'],
 'normal_WSGI_joined':x['WSGI_thread_joined'],
 'actual_controller_exit0':report['actual_controller_exit']==0,
 'exact_cleanup':report.get('exact_cleanup')is True and report.get('all_host_generations_absent')is True,
}
r={'passed':all(checks.values()),'checks':checks,'scope':'Original standard WSGI startup plus four real HTTP requests, role django; explicit plugin discovery optout; no worker/beat; not full uWSGI/media runtime','strict_Native41_cache_result_unchanged':2,'Source41_replays':0,'full_M8_acceptance':False,'performance_claims':False}
(p/'cache-context-review.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r));sys.exit(0 if r['passed']else 2)
