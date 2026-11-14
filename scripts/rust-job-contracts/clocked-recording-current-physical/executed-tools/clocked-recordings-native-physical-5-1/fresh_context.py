"""Read-only copied-fixture guards; no Source imports/config mutations/hooks."""
import os
from pathlib import Path
def empty_plugin_directory(configured,owned=Path('/data/plugins')):
 owned=Path(owned)
 if type(configured)is not str or configured!=str(owned):raise ValueError('ExactOwnedConfiguredPluginDirectory')
 if owned.is_symlink()or owned.parent.is_symlink():raise ValueError('OwnedPluginDirectoryNoSymlink')
 entries=[]
 if owned.exists():
  if not owned.is_dir():raise ValueError('OwnedPluginDirectoryType')
  with os.scandir(owned)as iterator:
   for entry in iterator:
    entries.append(entry.name)
    if len(entries)>64:raise ValueError('OwnedPluginEntryCapacity')
 if entries:raise ValueError('FreshConfiguredPluginDirectoryEmpty')
 return {'configured':configured,'exists':owned.exists(),'entries':entries,'empty':True,'no_suppression_or_deletion':True}
def empty_effect_context(context):
 if type(context)is not dict or set(context)!={'integrations','subscriptions','delivery_logs','plugin_configs'}:raise ValueError('ExactFreshEffectContext')
 if any(type(rows)is not list or rows!=[]for rows in context.values()):raise ValueError('FreshIntegrationAndPluginRowsEmpty')
 return True
