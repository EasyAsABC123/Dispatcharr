"""Portable sanitized CLI attestation checks; no Docker/replay/private fallback."""
import argparse,hashlib,json,re
from pathlib import Path
HERE=Path(__file__).resolve().parent
def verify(root=HERE,deep_repo=None):
 root=Path(root);binding=json.loads((root/'image-binding.json').read_text());safe=json.loads((root/'safe-operator17.json').read_text());create=json.loads((root/'createsuperuser22.json').read_text())
 assert binding['image_id']=='sha256:909d506f1c1deb29c60bd360da995eef9d5ff5c7ffa0ea3799bca11f7c93b5d0'and binding['source_revision']=='5afbb78ee8be031474b41278479fcdf623f2ddce'and binding['build_manifest_sha256']=='2dcf19e84324aa77d56a8df034729dd3d8a07ba7743d09d8f3fd7ebc9ca89957'
 for item in [safe,create]:
  assert item['passed']is True and item['process_exit']==0 and item['image_id']==binding['image_id']and item['build_manifest_sha256']==binding['build_manifest_sha256'];assert item['own_cleanup_empty']and item['cleanup']['all_empty']and all(v['empty']for v in item['cleanup']['inventories'].values())
 assert safe['case_count']==len(safe['cases'])==17 and len({c['id']for c in safe['cases']})==17
 assert all(type(v)is bool and v for c in safe['cases']for v in c['checks'].values());assert safe['Original_source_case_binding_sha256']=='1043832c1cfa47bec05bf0a23500aa7d1e7b7c80c98ceefb1868097c9fe10615';assert safe['native_help_and_stderr_exact']is False and safe['full_django_cli_verified']is False
 assert create['case_count']==len(create['cases'])==22 and len({c['id']for c in create['cases']})==22 and create['check_count']==len(create['checks'])==95
 assert all(type(c['passed'])is bool and c['passed']for c in create['checks']);assert create['default_database_unchanged']is True and create['runtime_UID65532_readonly_capdropALL_inspected']is True
 assert create['strict_comparison']['passed']is True and create['strict_comparison']['strict_typed_comparisons']==154 and create['strict_comparison']['actual_creation_clock_slots_per_backend']==10
 for case in create['cases']:
  assert not({'stdout','stderr','password'}&set(case))and all(re.fullmatch('[a-f0-9]{64}',case[k])for k in ['stdout_sha256','stderr_sha256'])
 assert create['all44_private_transcript_hashes_verified']is True and not binding['performance_verified']and not binding['full_M8']
 count=0
 if deep_repo is not None:
  for entry in json.loads((root/'raw-hashes.json').read_text())['private_deep_evidence_only']:
   p=Path(deep_repo)/entry['path']
   if not p.is_file()or hashlib.sha256(p.read_bytes()).hexdigest()!=entry['sha256']:raise ValueError('DeepRawAbsentOrHash')
   count+=1
 return {'passed':True,'mode':'public sanitized attestation, NOT live replay','safe17':17,'create22':22,'checks':95,'typed':154,'deep_raw_verified':count,'full_M8':False,'performance':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--deep-repo',type=Path);a=p.parse_args();print(json.dumps(verify(deep_repo=a.deep_repo),indent=2))
