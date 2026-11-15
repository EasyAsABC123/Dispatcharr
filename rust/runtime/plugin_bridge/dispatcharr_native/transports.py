"""Checked production adapter installation; missing ports fail BEFORE setup.
No fake broker/cache/event client or canned success is substituted.
"""
import importlib,importlib.util
from .wire import CompatibilityUnavailable
REQUIRED={'sql':'dispatcharr_native.sql_transport','cache':'dispatcharr_native.redis_transport','jobs':'dispatcharr_native.kombu_transport','events':'dispatcharr_native.channels_transport','process':'dispatcharr_native.process_transport'}
def install_all():
 # Check all actual adapters exist before importing any Source clients. Install
 # process ownership and cache transport BEFORE Celery/Channels import aliases.
 missing=[name for name,module in REQUIRED.items()if importlib.util.find_spec(module)is None]
 if missing:raise CompatibilityUnavailable('Required production native adapters are unimplemented: '+','.join(missing))
 for name in('process','sql','cache','jobs','events'):
  importlib.import_module(REQUIRED[name]).install()
