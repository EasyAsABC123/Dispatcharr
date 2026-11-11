"""Read retained diagnostics truthfully, including incomplete/failed windows."""
import json
from pathlib import Path


def sampling_status(output):
    root=Path(output)
    known=0
    unreadable=[]
    windows=sorted(root.glob('*-resources.json'))
    for path in windows:
        try:
            if path.stat().st_size>16*1024*1024:
                raise ValueError('Bounded diagnostics required')
            value=json.loads(path.read_text())
            samples=value['samples']
            if not isinstance(samples,list)or any(not isinstance(row,dict)for row in samples):
                raise ValueError('Sample array unavailable')
            known+=len(samples)
        except(OSError,ValueError,KeyError,TypeError):
            unreadable.append(path.name)
    collected=True if known else None if unreadable else False
    return {'resource_samples_collected':collected,
            'resource_sample_count':None if unreadable else known,
            'known_resource_sample_count':known,
            'retained_resource_windows':len(windows),
            'unreadable_resource_diagnostics':unreadable,
            'lifecycle_diagnostic_files':len(list(root.glob('*-lifecycle.json'))),
            'observer_smoke_diagnostic_retained':(root/'observer-functional-smoke.json').is_file()}
