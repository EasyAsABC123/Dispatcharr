"""Pure bounded private-other ownership witness; not a dispatch predicate."""
import re
def preserved(before,after,profile,content):
 for row in[before,after]:
  if not isinstance(row,dict):raise ValueError('OtherGrantMissing')
  token=row.get('owner');ledger=row.get('ledger',{});meta=row.get('metadata',{});fence=ledger.get('fence')
  if type(token)is not str or not 1<=len(token)<=256 or row.get('owner_confirm')!=token:raise ValueError('OtherOwnerToken')
  if type(fence)is not str or not re.fullmatch('[1-9][0-9]{0,18}',fence)or int(fence)>9223372036854775807:raise ValueError('OtherPositiveFence')
  if ledger.get('token')!=token or ledger.get('owner')!='rust:coordination:vod:session:fixture-normal-other:owner'or ledger.get('profile')!=profile or ledger.get('reserved')!='1':raise ValueError('OtherReservedGrant')
  if meta.get('rust_owner_token')!=token or meta.get('content_uuid')!=content or meta.get('active_streams')!='1':raise ValueError('OtherMetadataIdentity')
 if before!=after:raise ValueError('OtherGrantChanged')
 return{'preserved':True,'positive_fence':True,'reserved':True,'profile':profile,'Source_counter_equality_inferred':False}
