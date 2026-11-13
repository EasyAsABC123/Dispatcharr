"""Owned cfg(test) fixture wiring only; no connections or process on import.
Preserve Jobs::connect_fixture same physical broker/result address requirement.
"""
from urllib.parse import urlsplit,unquote
HOST='127.0.0.1';PORT=61603;PASSWORD='playback-fixture-only'
def route_reply_gate(config,target):
 result=dict(config)
 if target in ('broker_url','result_url'):
  result['broker_url']=f'redis://:{PASSWORD}@{HOST}:{PORT}/8'
  result['result_url']=f'redis://:{PASSWORD}@{HOST}:{PORT}/9'
 elif target in ('cache_url','events_url'):result[target]=f'redis://:{PASSWORD}@{HOST}:{PORT}/0'
 else:raise ValueError('ExactConfiguredGateTarget')
 return result
def inspect_config(config):
 broker,result=(urlsplit(config[name])for name in ('broker_url','result_url'))
 for url,db in ((broker,8),(result,9)):
  if url.scheme!='redis'or url.hostname not in ('localhost','127.0.0.1','::1')or(url.port or 6379)not in (6379,PORT)or url.path!='/'+str(db)or unquote(url.password or'')!=PASSWORD:raise ValueError('OwnedFixtureEndpointAndDB')
 same=(broker.hostname,broker.port or 6379)==(result.hostname,result.port or 6379)
 if not same:raise ValueError('FixtureBrokerResultSamePhysicalAddress')
 return {'broker_result_same_address':True,'separate_broker_result_databases':[8,9],'credentials_recorded':False,'physical_host':broker.hostname,'physical_port':broker.port or 6379}
