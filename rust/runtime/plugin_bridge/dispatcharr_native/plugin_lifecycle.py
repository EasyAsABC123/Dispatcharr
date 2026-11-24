"""Django thread/atomic context for native-orchestrated plugin lifecycle.
No Original API view/task is invoked. Rust selects the Source effect ordering;
these narrow helpers preserve actual model/compiler/signals and user hooks.
Original Dispatcharr/Django/plugin authors retain full implementation credit.
"""
import base64,contextvars,logging,shutil
from pathlib import Path
from .wire import current,CompatibilityUnavailable
_enabled=contextvars.ContextVar('native_enabled_config',default=None)

def nested(method,args):
 from apps.plugins.loader import PluginManager
 from apps.plugins.models import PluginConfig
 pm=PluginManager.get()
 if method=='select_enabled_config':
  try:cfg=PluginConfig.objects.select_for_update().get(key=args['key'])
  except PluginConfig.DoesNotExist:return{'found':False}
  _enabled.set(cfg);return{'found':True,'enabled':cfg.enabled,'ever_enabled':cfg.ever_enabled}
 cfg=_enabled.get()
 if cfg is None:raise CompatibilityUnavailable('Actual row-locked plugin lifecycle context missing')
 if method=='suspend_enabled_schedules':return pm.suspend_schedules(cfg)
 if method=='stop_disabled_plugin_caught':
  if args['key']!=cfg.key:raise CompatibilityUnavailable('Actual locked plugin identity mismatch')
  try:pm.stop_plugin(cfg.key,reason='disable')
  except Exception:
   logging.getLogger(__name__).exception("Failed to stop plugin '%s' on disable",cfg.key)
  return None
 if method=='save_enabled_config':
  cfg.enabled=args['enabled']
  if cfg.enabled and not cfg.ever_enabled:cfg.ever_enabled=True
  cfg.save(update_fields=['enabled','ever_enabled','updated_at']);return{'ever_enabled':cfg.ever_enabled}
 if method=='restore_enabled_schedules':return pm.restore_schedules(cfg)
 raise CompatibilityUnavailable('Unimplemented native lifecycle reentry')

def rendered(status,body):
 from rest_framework.renderers import JSONRenderer
 try:raw=JSONRenderer().render(body)
 except Exception as error:return{'source_rendering_failed':True,'actual_exception_class':type(error).__name__}
 return{'status':status,'body_base64':base64.b64encode(raw).decode('ascii')}

def change_enabled(value):
 from django.db import transaction
 from apps.plugins.loader import PluginManager
 from apps.plugins.models import PluginConfig
 pm=PluginManager.get();key=value['key'];enabled=value['enabled']
 try:
  pm.refresh_task_ownership()
  try:
   token=_enabled.set(None)
   try:
    with transaction.atomic():
     actual=current().call('plugin_control','enabled',{'key':key,'enabled':enabled})
     if actual['found']is False:raise PluginConfig.DoesNotExist()
   finally:_enabled.reset(token)
  except PluginConfig.DoesNotExist:raise
  except Exception as error:
   logging.getLogger(__name__).exception("Failed to change enabled state of plugin '%s'",key)
   return rendered(500,{'success':False,'error':f'Failed to change plugin enabled state: {error}'})
  pm.discover_plugins(force_reload=True)
  from .task_registry import seal
  seal()
  plugin_entry=None
  try:plugin_entry=next((p for p in pm.list_plugins()if p.get('key')==key),None)
  except Exception:pass
  body={'success':True,'enabled':actual['enabled'],'ever_enabled':actual['ever_enabled']}
  if plugin_entry:
   from urllib.parse import urljoin
   if plugin_entry.get('logo_url'):plugin_entry['logo_url']=urljoin(value['origin'],plugin_entry['logo_url'])
   body['plugin']=plugin_entry
  return rendered(200,body)
 except PluginConfig.DoesNotExist:return rendered(404,{'success':False,'error':'Plugin not found'})

def delete_enabled(value):
 """Staged Action-host entry. Only the reserved Action-host dispatch supplies admission."""
 from django.db import transaction
 from apps.plugins.loader import PluginManager
 from apps.plugins.models import PluginConfig
 from .task_registry import seal
 from .loaded_module_witness import observe

 key=value['key']
 if not isinstance(key,str) or not key or len(key)>512 or key in ('.','..') or '/' in key or chr(92) in key:
  raise CompatibilityUnavailable('Invalid native delete identity')
 if set(value)!={'key','load_sequence','mutation_lock_held'} or value['mutation_lock_held'] is not True:
  raise CompatibilityUnavailable('Owned delete mutation boundary unavailable')
 pm=PluginManager.get()
 pm.refresh_task_ownership()
 if type(value['load_sequence']) is not int or observe(pm,key)['load_sequence']!=value['load_sequence']:
  raise CompatibilityUnavailable('Owned loaded plugin changed before stop')
 root=Path(pm.plugins_dir).resolve(strict=True)
 target=Path(pm.get_plugin_path(key))
 if target.is_symlink() or target.resolve(strict=True)!=root/key or not target.is_dir():
  raise CompatibilityUnavailable('Invalid owned plugin directory')
 identity=target.stat()
 with transaction.atomic(durable=True):
  cfg=PluginConfig.objects.select_for_update().get(key=key)
  if not cfg.enabled or not cfg.ever_enabled or cfg.source_repo_id is not None or cfg.slug:
   raise CompatibilityUnavailable('Only the owned enabled unmanaged lifecycle is staged')
  if cfg.owned_tasks!=[] or cfg.suspended_schedules!=[] or pm._owned_task_names(cfg,path=str(target)):
   raise CompatibilityUnavailable('Plugin task ownership requires a separate drain')
  if pm.stop_plugin(key,reason='delete') is not True:
   raise CompatibilityUnavailable('Actual delete stop did not complete')
  cfg.refresh_from_db(fields=['owned_tasks','suspended_schedules'])
  if cfg.owned_tasks!=[] or cfg.suspended_schedules!=[] or pm._owned_task_names(cfg,path=str(target)):
   raise CompatibilityUnavailable('Stop changed exact task ownership')
  # Do not restore the files after a later database failure, matching Original.
  if target.is_symlink() or target.resolve(strict=True)!=root/key:
   raise CompatibilityUnavailable('Owned delete directory changed during stop')
  settled=target.stat()
  if (settled.st_dev,settled.st_ino)!=(identity.st_dev,identity.st_ino):
   raise CompatibilityUnavailable('Owned delete directory identity changed during stop')
  shutil.rmtree(target)
  if pm.delete_plugin_periodic_tasks(key,path=str(target))!=0:
   raise CompatibilityUnavailable('Unadmitted plugin task deletion')
  cfg.delete()
 pm.discover_plugins(force_reload=True)
 if pm.get_plugin(key) is not None:
  raise CompatibilityUnavailable('Deleted plugin remains loaded')
 seal()
 return {'key':key,'stopped':True,'committed':True,'registry_sealed':True}

def list_plugins(value):
 from apps.plugins.loader import PluginManager
 from urllib.parse import urljoin
 pm=PluginManager.get();pm.discover_plugins(sync_db=False,use_cache=True)
 from .task_registry import seal
 seal()
 plugins=pm.list_plugins()
 for plugin in plugins:
  if plugin.get('logo_url'):plugin['logo_url']=urljoin(value['origin'],plugin['logo_url'])
 return rendered(200,{'plugins':plugins})

def plugin_logo(value):
 # Source PluginLogoAPIView: cached discovery defaults sync_db=True and may
 # import enabled modules. Do not replace it with static manifest metadata.
 import os
 from apps.plugins.loader import PluginManager
 pm=PluginManager.get();pm.discover_plugins(use_cache=True)
 from .task_registry import seal
 seal()
 key=value['key'];logo_path=os.path.join(pm.plugins_dir,key,'logo.png')
 lp=pm.get_plugin(key)
 if lp and getattr(lp,'path',None):logo_path=os.path.join(lp.path,'logo.png')
 abs_plugins=os.path.abspath(pm.plugins_dir)+os.sep
 abs_target=os.path.abspath(logo_path)
 if not abs_target.startswith(abs_plugins):return{'status':400}
 if not os.path.isfile(logo_path):return{'status':404}
 return{'status':200,'path':abs_target,'plugins_root':os.path.abspath(pm.plugins_dir)}

def stop_all(value):
 from apps.plugins.loader import PluginManager
 return PluginManager.get().stop_all_plugins(reason=value['reason'])
