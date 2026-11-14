"""Actual failed-process classification then pure retained effect checks only."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent/'tools'))
from physical_contract import validate_effects
EXPECTED='ActualStartedEventPersistsAcrossOperationalErrorNoRollbackFabrication'
def review(host,raw):
 if not(host.get('actual_host_exit')==1 and type(host.get('actual_host_exit'))is int and host.get('actual_controller_exit')==1 and type(host.get('actual_controller_exit'))is int and host.get('actual_controller_settled')is True and host.get('passed')is False):raise ValueError('ActualHost1Guest1SettledRequired')
 if not(raw.get('passed')is False and raw.get('error_type')=='ValueError'and raw.get('error')==EXPECTED):raise ValueError('ExactKnownExecutedComparatorErrorRequired')
 case=raw['physical']
 if not(case.get('mode')=='sql-update-error-ready'and case.get('capture_complete')is True and case.get('cleanup_errors')==[]and type(case['peer_collection'].get('actual_exit'))is int and case['peer_collection']['actual_exit']==0):raise ValueError('ActualUnderlyingPeer0CompleteRequired')
 effects=validate_effects(raw,'sql-update-error-ready')
 return {'body_effects_passed':True,'scope':'pure retained body/effect/ownership predicates only; no actual process-success claim or exit substitution','actual_host_exit_preserved':1,'actual_guest_exit_preserved':1,'actual_Native_peer_exit':0,'hypothetical_exit_argument_used':False,'effects':effects}
