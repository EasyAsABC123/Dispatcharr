"""Typed data-only Source-observed normal result contracts; no Python imports."""
import json
from pathlib import Path
CONTRACTS=json.loads(Path(__file__).with_name('Source-result-contracts.json').read_text())
NORMAL_SINGLE=CONTRACTS['normal_single']['metadata']['result']
BATCH_FAILED=CONTRACTS['caught_mutation_failure']['metadata']['result']
def exact_success(raw,expected):
 if not isinstance(raw,(str,bytes,bytearray)):return False
 value=json.loads(raw)
 return value.get('status')=='SUCCESS' and type(value.get('result'))is str and value['result']==expected and value.get('traceback')is None and value.get('children')==[]
