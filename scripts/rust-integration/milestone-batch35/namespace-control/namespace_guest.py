"""Actual installed namespace/AppConfig packaging control; no Django setup.
Original Dispatcharr, Django and CPython authors retain all credit/licenses.
"""
import hashlib,importlib,importlib.util,json,os,stat,sys
from pathlib import Path
PREFIX=Path('/opt/dispatcharr-python')
APPLICATION=PREFIX/'application'
NAMESPACE=APPLICATION/'apps/api'
CONFIG_SHA='d59871b783abc119ce9ac4ff07f06eac89b3de88bcdf7d13246d2b451279f1b4'
assert os.getuid()==65532 and os.getgid()==65532
assert sys.flags.isolated and sys.dont_write_bytecode
# Actual installed filesystem/import behavior is checked before manifest flags.
assert NAMESPACE.is_dir()and not NAMESPACE.is_symlink()
assert stat.S_IMODE(NAMESPACE.stat().st_mode)==0o755
assert list(NAMESPACE.iterdir())==[]
assert str(APPLICATION)not in sys.path
sys.path.insert(0,str(APPLICATION))
spec=importlib.util.find_spec('apps.api')
assert spec is not None and spec.origin is None and not spec.has_location
locations=list(spec.submodule_search_locations)
assert len(locations)==1 and Path(locations[0]).resolve()==NAMESPACE
module=importlib.import_module('apps.api')
assert module.__name__=='apps.api'and getattr(module,'__file__',None)is None
assert list(module.__path__)==locations
for name in('apps','models','urls','schema_views'):
 assert importlib.util.find_spec('apps.api.'+name)is None
import django
from django.apps import AppConfig,apps
from django.conf import settings
assert django.VERSION==(6,0,9,'final',0)
config_module=importlib.import_module('django.apps.config')
config_path=Path(config_module.__file__).resolve()
assert config_path==PREFIX/'lib/python3.13/dist-packages/django/apps/config.py'
assert hashlib.sha256(config_path.read_bytes()).hexdigest()==CONFIG_SHA
assert not settings.configured and not apps.ready and not apps.apps_ready and not apps.models_ready
assert not apps.app_configs and not apps.all_models
config=AppConfig.create('apps.api')
assert type(config)is AppConfig and type(config).ready is AppConfig.ready
assert config.name=='apps.api'and config.label=='api'and config.module is module
assert Path(config.path).resolve()==NAMESPACE
assert config.apps is None and config.models is None and config.models_module is None
# Never call setup(), populate(), import_models(), ready() or default_auto_field.
assert not settings.configured and not apps.ready and not apps.apps_ready and not apps.models_ready
assert not apps.app_configs and not apps.all_models
assert not any(n=='django.db'or n.startswith('django.db.')for n in sys.modules)
assert 'dispatcharr.settings'not in sys.modules and 'dispatcharr_native.settings'not in sys.modules
assert {n for n in sys.modules if n=='apps'or n.startswith('apps.')}=={'apps','apps.api'}
assert list(NAMESPACE.iterdir())==[]
manifest=json.loads(Path('/usr/share/doc/dispatcharr/python-host/source-manifest.json').read_text())
assert manifest['original_archive_sha256']=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
assert manifest['archive_sha256']=='cfab2dd784ec1b22324b9909b1b52821d71ba09788d6275e673efa2a80408a80'
assert len(manifest['retained'])==328
assert not any(row['path'].startswith('apps/api/')for row in manifest['retained'])
assert len(manifest['namespace_directories'])==1
row=manifest['namespace_directories'][0]
assert row['path']=='apps/api'and row['installed_app']=='apps.api'
assert {item['path']for item in row['source_members']}=={'apps/api/schema_views.py','apps/api/urls.py'}
print(json.dumps({'passed':True,'actual_UID':os.getuid(),'Django':'6.0.9','Django_AppConfig_sha256':CONFIG_SHA,'namespace_imported':'apps.api','AppConfig_class':'django.apps.config.AppConfig','app_name':config.name,'app_label':config.label,'namespace_paths':locations,'namespace_mode':'0755','namespace_empty_before_and_after':True,'no_Source_init_AppConfig_models_ready_or_HTTP_code':True,'settings_unconfigured':True,'registry_unpopulated':True,'Django_DB_not_imported':True,'Source_business_calls':0,'app_started':False,'performance_verified':False},sort_keys=True))
