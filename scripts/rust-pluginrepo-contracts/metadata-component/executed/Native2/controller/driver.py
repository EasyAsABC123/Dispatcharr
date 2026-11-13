"""Unexecuted raw storage audit/import entrypoint. No Django/application imports."""
import json,os,re,sys
from pathlib import Path
from native_state import default,import_existing,state,need
ROOT=Path('/native-root');OUT=Path('/evidence')
def main():
 need(os.environ.get('PLUGIN_REPO_DRIVER')=='owned-native-pluginrepo-only','ExplicitNativeDriver');action=os.environ['PLUGIN_REPO_ACTION'];label=os.environ['PLUGIN_REPO_LABEL'];need(re.fullmatch('[a-z-]{1,48}',label)is not None,'FiniteOutputLabel');database=os.environ['PLUGIN_REPO_DATABASE'];schema=json.loads((ROOT/'inputs/schema62-authority.json').read_bytes())
 if action=='default':value=default(database,schema)
 elif action=='import':value=import_existing(database,json.loads((ROOT/'inputs/Source2-private.json').read_bytes()),schema)
 elif action=='state':value=state(database,schema)
 else:raise ValueError('KnownRawDriverAction')
 need(not any(n=='django'or n.startswith(('django.','apps.','dispatcharr.'))for n in sys.modules),'NoDjangoOrSourceApplicationLoaded');path=OUT/(label+'.json');need(not path.exists(),'FreshDriverOutput');raw=json.dumps(value,indent=2,allow_nan=False).encode();need(len(raw)<=16777216,'BoundedDriverOutput');path.write_bytes(raw);print(json.dumps({'passed':True,'output':str(path),'action':action,'Django_or_Source_application_loaded':False}))
if __name__=='__main__':main()
