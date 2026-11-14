"""Every owned observer retirement step runs, even after capture failure."""
import sys
from capture_common import exception_record

def retire(probe,snapshot):
 result={};errors=[]
 for key,fn in [('terminal_before_receiver_cleanup',snapshot),('receiver_cleanup',probe.discard),('terminal_Redis_unconsumed',probe.raw),('observer_clients_closed',probe.close_clients)]:
  try:result[key]=fn()
  except BaseException:errors.append({'step':key,'exception':exception_record(sys.exc_info())})
 return result,errors
