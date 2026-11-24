"""Separate actual Core task context, always run for both backend fixtures.
Original Dispatcharr/dependency authors retain application/algorithm credit.
No task producer/function is replaced; no application starts on import.
"""
import hashlib
import importlib.util
import json
from pathlib import Path


def prepare_context(fixture, output):
    if fixture.case != 'core-produced':
        raise ValueError('DistinctCoreContextRequired')
    directory = Path(output)/'m6-core-produced'
    directory.mkdir(parents=True, exist_ok=False)
    observed = fixture.sql("""
from core.models import CoreSettings
from apps.channels.models import RecurringRecordingRule
from django_celery_beat.models import PeriodicTask,PeriodicTasks
PeriodicTask.objects.all().update(enabled=False)
PeriodicTasks.update_changed()
RecurringRecordingRule.objects.all().update(enabled=False)
for key,changes in [('system_settings',{'time_zone':'UTC'}),('stream_settings',{'m3u_hash_key':'url'}),('dvr_settings',{'pre_offset_minutes':0,'post_offset_minutes':0})]:
 row,_=CoreSettings.objects.get_or_create(key=key,defaults={'name':key,'value':{}})
 value=row.value.copy()if isinstance(row.value,dict)else{}
 value.update(changes)
 CoreSettings.objects.filter(pk=row.pk).update(value=value)
print(json.dumps({'settings':list(CoreSettings.objects.filter(key__in=['system_settings','stream_settings','dvr_settings']).values('key','value')),'enabled_beat_rows':PeriodicTask.objects.filter(enabled=True).count()}))
""")
    (directory/'before-backend-context.json').write_text(json.dumps(observed,indent=2)+'\n')


def run(fixture, output):
    if fixture.case != 'core-produced':
        raise ValueError('DistinctCoreContextRequired')
    if fixture.binding.get('core_offset_iso_pair') is True:
        path = Path(fixture.binding['core_offset_iso_case'])
        if hashlib.sha256(path.read_bytes()).hexdigest() != fixture.binding['core_offset_iso_case_sha256']:
            raise ValueError('ExactCoreOffsetIsoCallbackRequired')
        spec = importlib.util.spec_from_file_location('bound_core_offset_iso_case', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.run(fixture)
    path=Path(fixture.binding['core_scale_cases'])
    if hashlib.sha256(path.read_bytes()).hexdigest()!=fixture.binding['core_scale_cases_sha256']:
        raise ValueError('ExactCoreScaleCallbackRequired')
    spec=importlib.util.spec_from_file_location('bound_core_scale_cases',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module.run(fixture)
