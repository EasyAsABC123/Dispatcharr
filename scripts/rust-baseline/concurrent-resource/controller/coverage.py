"""Canonical denied-proc merge plus explicit coverage/identity/error guards.
Original authors retain credit. No probe, thread or subprocess starts on import.
"""
from canonical_merge import merge_census as canonical_merge
SERVICES={'baseline','origin','source','worker'}
def merge_census(inventory,parts,ending=None):
 ending=inventory if ending is None else ending
 value=canonical_merge(inventory,parts,ending)
 records=[inventory,*parts,ending]
 value['coverage_complete']=all(x.get('coverage_complete')is True for x in records)
 identities=[tuple(x['collector_identity'])for x in parts]
 expected=[tuple(x)for x in inventory['identities']]
 value['identities_match']=len(identities)==len(set(identities)) and set(identities)==set(expected)=={tuple(x)for x in ending['identities']}
 value['complete']=bool(value['complete']and value['coverage_complete']and value['identities_match']and not any(x.get('error_type')for x in records))
 return value

def complete_service(value):
 try:
  merged=merge_census(value['inventory_start'],value['per_uid'],value['inventory_end'])
  return value.get('complete')is True and merged['complete']is True and merged['processes']>0 and not value.get('error_type')
 except (KeyError,TypeError,ValueError):return False

def eligible_census_row(row):
 return row.get('phase_stable')is True and row.get('complete')is True and set(row.get('census',{}))==SERVICES and all(complete_service(v)for v in row['census'].values())
