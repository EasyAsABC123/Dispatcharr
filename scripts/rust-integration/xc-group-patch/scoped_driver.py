"""Keep playback-only comparison out of unrelated single-context cohort reports."""
def patch_run(source):
    before = ''' """Compare actual fresh Source transport, never invent HTTP416 or retry."""
 observed={}
'''
    after = ''' """Compare actual fresh Source transport, never invent HTTP416 or retry."""
 if 'retained_Source_playback' not in binding:
  return {'status':'not_selected','reason':'No paired playback reference was bound for this cohort'}
 observed={}
'''
    if source.count(before) != 1:
        raise ValueError("exact backed cohort runner changed")
    return source.replace(before, after)
