"""Evidence completeness and normal scenario gates, never dispatch predicates."""
def live(row):
 return isinstance(row,dict)and row.get('eof')is False and row.get('invalid')is None and row.get('reader_done')is False and type(row.get('socket_fd'))is int and row['socket_fd']>=0

def phase(report):
 outcomes=report.get('probe_outcomes',[])
 required=['before_stop','fixed_probe1_boundary','after_probe1','after_probe2','after_decode']
 checkpoints=report.get('checkpoints',[])
 coverage=all(any(r.get('label')==label and not r.get('observation_error')for r in checkpoints)for label in required)
 readers=report.get('readers_before_teardown',{})
 late_valid=bool(readers)and all(not r.get('invalid')for r in readers.values())and all(not r.get('snapshot',{}).get('invalid')for r in report.get('client_closes',[]))
 literal=(len(outcomes)==2 and [r.get('role')for r in outcomes]==['probe1','probe2']and all(type(r.get('status'))is int or isinstance(r.get('transport_error'),str)for r in outcomes))
 capture=literal and all(not r.get('physical_close_error')for r in outcomes)and coverage and late_valid and not any(report.get(k)for k in ['error_type','close_errors','terminal_observation_error','phase_deadline_exceeded'])
 other=all(live(report.get(k))for k in ['other_at_probe1_dispatch','other_at_probe2_dispatch','other_before_teardown'])
 first=outcomes[0]if outcomes else{}
 probe1=first.get('status')!=200 or live(report.get('probe1_at_probe2_dispatch'))
 grant=report.get('mode')!='native' or (isinstance(report.get('native_other_grant_preserved'),dict)and report['native_other_grant_preserved'].get('preserved')is True)
 body=all(not r.get('observation_error')and(r.get('status')!=200 or r.get('prefix_matches_fixture')is True)for r in outcomes)
 normal=capture and body and other and probe1 and grant and report.get('admin_stop',{}).get('status')==200 and report.get('early_stop_literal')=='true'
 return {'HTTP_capture_valid':capture,'scenario_ready':normal,'other_continuity_valid':other,'admitted_probe1_continuity_valid':probe1,'native_other_ownership_valid':grant,'required_checkpoint_coverage':coverage,'reader_late_valid':late_valid,'bounded_unexpected_outcomes_retained':literal,'no_reproduction':capture and not normal,'capture_incomplete':not capture,'parity_acceptance':False}

def pair(report):
 backends=report.get('backends',{})
 complete=report.get('HTTP_pair_complete')is True and set(backends)=={'source','native'}and all(r.get('HTTP_capture_valid')is True for r in backends.values())and report.get('owned_cleanup_valid')is True and report.get('default_named_tables_sequences_unchanged')is True
 normal=complete and all(r.get('scenario_ready')is True for r in backends.values())and backends['source'].get('shared_response_finalized_by_probe1')is True
 return {'observation_complete':complete,'normal_scenario_ready':normal,'normal_scenario_qualified':normal,'no_reproduction':complete and not normal,'full_M4_acceptance':False}
