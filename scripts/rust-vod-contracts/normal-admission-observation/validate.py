"""Offline only: check independent native lease/metadata namespaces and equality.
Never dispatches, changes prior reports, or changes Source/Native behavior.
"""
import argparse,json,math,re,uuid
from pathlib import Path

def token(value,prefix):
 if type(value)is not str or not value.startswith(prefix):raise ValueError('TypedTokenPrefix')
 suffix=value[len(prefix):]
 try:parsed=uuid.UUID(suffix)
 except (ValueError,AttributeError):raise ValueError('TypedTokenUUID')
 if str(parsed)!=suffix or parsed.version!=4 or parsed.variant!=uuid.RFC_4122:raise ValueError('CanonicalV4Token')
 return value

def preserved(before,after,profile,content):
 if type(profile)is not str or not re.fullmatch('profile_connections:[1-9][0-9]{0,18}',profile):raise ValueError('ExpectedProfile')
 for row in [before,after]:
  if type(row)is not dict:raise ValueError('OtherSnapshotMissing')
  lease=token(row.get('owner'),'rust:');ledger=row.get('ledger');metadata=row.get('metadata')
  if row.get('owner_confirm')!=lease or type(ledger)is not dict or type(metadata)is not dict:raise ValueError('OtherOwnerOrStructure')
  if ledger.get('token')!=lease or ledger.get('owner')!='rust:coordination:vod:session:fixture-normal-other:owner' or ledger.get('profile')!=profile or ledger.get('reserved')!='1':raise ValueError('OtherReservedLease')
  fence=ledger.get('fence')
  if type(fence)is not str or not re.fullmatch('[1-9][0-9]{0,18}',fence)or int(fence)>9223372036854775807:raise ValueError('OtherPositiveFence')
  token(metadata.get('rust_owner_token'),'rust-vod-metadata:')
  if metadata.get('content_uuid')!=content or metadata.get('active_streams')!='1' or metadata.get('m3u_profile_id')!=profile.split(':')[1]:raise ValueError('OtherMetadataBinding')
  created=metadata.get('created_at')
  if type(created)is not str or len(created)>64:raise ValueError('MetadataCreatedScalar')
  try:number=float(created)
  except ValueError:raise ValueError('MetadataCreatedScalar')
  if not math.isfinite(number)or number<=0:raise ValueError('MetadataCreatedScalar')
 if before!=after:raise ValueError('OtherSnapshotChanged')
 return {'preserved':True,'independent_typed_token_namespaces':True,'positive_fence':True,'reserved':True,'complete_before_after_equality':True,'counter_equality_claimed':False}

def reanalyze(data):
 result=preserved(data['native_other_before'],data['native_other_after'],data['expected_profile'],data['expected_content'])
 modes=data['backends'];requirements=['HTTP_capture_valid','other_continuity_valid','admitted_probe1_continuity_valid','required_checkpoint_coverage','reader_late_valid']
 other=all(all(modes[mode]['actual_flags'].get(key)is True for key in requirements)for mode in ['source','native'])
 summary=data['actual_summary_flags'];qualified=result['preserved']and other and modes['source']['actual_flags']['scenario_ready']is True and modes['source']['actual_flags']['shared_response_finalized_by_probe1']is True and summary['observation_complete']is True and summary['owned_cleanup_valid']is True and summary['default_named_tables_sequences_unchanged']is True
 return {'scope':'NEW offline corrected-schema calculation on retained input only','actual_exit_code_preserved':data['actual_exit_code'],'actual_summary_qualification_preserved':summary['normal_scenario_qualified'],'actual_summary_sha256':data['actual_summary_sha256'],'corrected_other_ownership':result,'offline_normal_scenario_qualified':qualified,'actual_execution_reclassified':False,'new_Source_or_Native_execution':False,'statuses':{mode:[p['status']for p in modes[mode]['probes']]for mode in modes},'full_M4_acceptance':False,'resource_or_performance_claim':False}
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('input',type=Path);args=parser.parse_args();print(json.dumps(reanalyze(json.loads(args.input.read_text())),indent=2))
