"""Add retained Original model and command-help observations to the register."""

from hashlib import sha256
import json

INPUT = 'scripts/rust-contracts/retained-model-command-observations.json'
INPUT_SHA256 = 'af292915f47ba5c3381bbb4213b04986fd812ae3a322effefc137b0613222437'
ATTACHMENTS = 'additional_executable_Source_contracts'
MODELS = {
    'candidate-04d0067947b1e7b35bef': 3,
    'candidate-8f35ebf6514bac73716d': 7,
}
COMMANDS = {
    'candidate-28156bb1f8d7874a97af': 'dropdb',
    'candidate-8738db7f5c7a632f8932': 'proxy',
    'candidate-d379ec88e7a6c81f1ca6': 'kill_processes',
}


def attach(root, report):
    path = root / INPUT
    if not path.exists():
        return report
    data = path.read_bytes()
    if sha256(data).hexdigest() != INPUT_SHA256:
        raise ValueError('retained Original model and command observations changed')
    packet = json.loads(data)
    if packet['Original_revision'] != report['baseline_ref'] or packet['unique_new_Original_executions'] != 0:
        raise ValueError('retained Original observation authority differs')
    candidates = {item['candidate_id']: item for item in report['non_HTTP_candidates']}
    contracts = {item['candidate_id']: item for item in packet['contracts']}
    if len(contracts) != len(packet['contracts']) or set(contracts) != MODELS.keys() | COMMANDS.keys():
        raise ValueError('retained Original observation roster differs')
    for identity, contract in contracts.items():
        candidate = candidates.get(identity)
        if (candidate is None or candidate['source'] != contract['source']
                or ATTACHMENTS in candidate or contract['fully_verified'] is not False):
            raise ValueError(f'cannot attach retained Original observation for {identity}')
        cases = contract['cases']
        if identity in MODELS:
            if len(cases) != MODELS[identity] or any(case['historical_result'] != 'ok' for case in cases):
                raise ValueError(f'retained model result differs for {identity}')
        elif (len(cases) != 1 or cases[0]['argv'] != [COMMANDS[identity], '--help']
              or cases[0]['exit_code'] != 0 or contract['management_handle_execution_verified'] is not False):
            raise ValueError(f'retained command help scope differs for {identity}')
    for identity, contract in contracts.items():
        candidates[identity][ATTACHMENTS] = [contract]
    report['input_sha256'][INPUT] = INPUT_SHA256
    return report
