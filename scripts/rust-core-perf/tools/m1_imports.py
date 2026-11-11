"""Explicit private package loader, independent of shared helper sys.path/cache."""
import importlib.util
from pathlib import Path
import sys
import types

HERE=Path(__file__).resolve().parent
PACKAGE='dispatcharr_m1_core_private'


def load(name):
    if name not in {'runtime','policy','metrics','orchestrator','reporting'}:
        raise ValueError('Known private M1 module required')
    package=sys.modules.get(PACKAGE)
    if package is None:
        package=types.ModuleType(PACKAGE)
        package.__path__=[str(HERE)]
        package.__package__=PACKAGE
        sys.modules[PACKAGE]=package
    elif list(getattr(package,'__path__',[]))!=[str(HERE)]:
        raise ValueError('Private M1 package identity changed')
    qualified=PACKAGE+'.'+name
    path=HERE/(name+'.py')
    cached=sys.modules.get(qualified)
    if cached is not None:
        if Path(cached.__file__).resolve()!=path:
            raise ValueError('Private M1 module identity changed')
        return cached
    spec=importlib.util.spec_from_file_location(qualified,path)
    module=importlib.util.module_from_spec(spec)
    sys.modules[qualified]=module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(qualified,None)
        raise
    return module
