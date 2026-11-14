"""Exact default SQL/cache preservation; no causal expiry inference."""
def need(v,k):
 if not v:raise ValueError(k)
def compare(before,after):
 need(after['default_full6259']==before['default_full6259']and after['default_named11']==before['default_named11'],'DefaultFull6259And11Unchanged')
 def stable(row):return [{k:v for k,v in item.items()if k!='pttl'}for item in row['rows']]
 need(after['default_cache_DB7']['complete']is True and before['default_cache_DB7']['complete']is True and stable(after['default_cache_DB7'])==stable(before['default_cache_DB7']),'DefaultCacheDB7UnchangedExceptObservedTTL')
 for old,new in zip(before['default_cache_DB7']['rows'],after['default_cache_DB7']['rows']):
  a,b=old['pttl'],new['pttl'];need(type(a)is int and type(b)is int and((a==-1 and b==-1)or(a>=0 and 0<=b<=a)),'DefaultCacheTTLNotRefreshedOrChangedToPersistent')
 return {'default_SQL_full6259_and11_equal':True,'default_cache_DB7_keys_type_bytes_equal':True,'PTTL_exact_equality_not_required':True,'PTTL_refresh_or_persistence_change_refused':True,'no_expiry_or_source_write_causality_assumed':True}
