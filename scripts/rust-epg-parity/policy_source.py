"""Capture original guide-history policy using unchanged classmethod AST."""
import argparse
import ast
from collections import namedtuple
import hashlib
import json
from pathlib import Path
import random


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    models=Path('/source/core/models.py').read_bytes()
    utilities=Path('/source/core/utils.py').read_bytes()
    scope={'EpgHistoryPolicy':namedtuple('EpgHistoryPolicy','min_days max_days default_prev_days default_days'),
           'EPG_HISTORY_MIN_DAYS_DEFAULT':1,'EPG_HISTORY_MAX_DAYS_DEFAULT':30,'EPG_DAYS_LIMIT':365}
    node=next(node for node in ast.parse(utilities).body if isinstance(node,ast.FunctionDef) and node.name=='clamp_int')
    exec(compile(ast.Module(body=[node],type_ignores=[]),'<original clamp_int>','exec'),scope)
    cls=next(node for node in ast.parse(models).body if isinstance(node,ast.ClassDef) and node.name=='CoreSettings')
    node=next(node for node in cls.body if isinstance(node,ast.FunctionDef) and node.name=='get_epg_history_policy')
    exec(compile(ast.Module(body=[node],type_ignores=[]),'<original EPG policy>','exec'),scope)
    class Settings:
        @classmethod
        def get_epg_settings(cls):
            return cls.value
    Settings.get_epg_history_policy=scope['get_epg_history_policy']
    keys=['epg_history_min_days','epg_history_max_days','epg_default_prev_days','epg_default_days']
    values=[None,False,True,0,1,-1,30,365,366,10**200,-10**200,0.9,-1.9,365.9,1e200,
            '', ' ', '3', '+3', '-99', '1_2', '12.0', '0x10', '١٢', '１２', '\u00851', '\x1c1',
            '9'*4301, [], {}, [1]]
    inputs=[{}]+[{key:value} for key in keys for value in values]
    rng=random.Random(20261115)
    inputs.extend({key:rng.choice(values) for key in rng.sample(keys,rng.randrange(5))} for _ in range(1000))
    cases=[]
    for value in inputs:
        Settings.value=value
        try:
            expected={'ok':True,'value':dict(Settings.get_epg_history_policy()._asdict())}
        except Exception as error:
            expected={'ok':False,'error':type(error).__name__}
        cases.append({'input':value,'expected':expected})
    args.output.write_text(json.dumps({'scope':'original-EPG-history-policy','cases':cases,
        'source_sha256':{'core/models.py':hashlib.sha256(models).hexdigest(),'core/utils.py':hashlib.sha256(utilities).hexdigest()},
        'tool_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},indent=2)+'\n')
    print(json.dumps({'captured':len(cases)}))


if __name__=='__main__':
    main()
