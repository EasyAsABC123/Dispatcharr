#!/usr/bin/env python3
"""Small read-only public dependency/tool/data closure; no private paths used."""
import hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
def check(root=ROOT):
 sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
 binding=json.loads((root/'source-binding.json').read_text());mapping=root.parent/'source-inputs.json'
 if binding['runtime_changed']is not False or sha(mapping)!=binding['full_source_map_sha256']or len(json.loads(mapping.read_text()))!=839:raise ValueError('Committed exact source839 dependency')
 count=0
 for kind in('physical','refusal','correction'):
  bundle=root/'controllers'/kind;tools=json.loads((bundle/'executed-tool-map.json').read_text())
  if any(sha(bundle/'tools'/name)!=value for name,value in tools.items()):raise ValueError('Exact executed controller '+kind)
  count+=len(tools)
 from review import dataset_rows
 rows=dataset_rows(root)
 if count!=56 or len(rows)!=12 or sum(row['passed']for row in rows)!=11:raise ValueError('Finite actual tool/case closure')
 return {'passed':True,'source_inputs':839,'executed_tool_positions':56,'retained_attempts':12,'passing_attempts':11,'new_app_or_build':False,'private_fallback':False}
if __name__=='__main__':print(json.dumps(check()))
