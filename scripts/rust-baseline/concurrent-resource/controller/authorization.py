"""Pure Root review/cue binding. Passing this does not remove entry hardblocks."""
import math
SUPPORT='21a152a27a5800592c66e412a7fac67509a5d485'
ORIGINAL='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def bindings(policy):return {'support_git_revision':SUPPORT,'original_git_revision':ORIGINAL,'original_archive_sha256':ARCHIVE,'original_image':IMAGE,'service_caps':policy['caps']}
def validate(review,cue,policy,policy_sha256,controller_sha256,repetition,boot_id,now):
 for value in [review,cue]:
  if value.get('scope')!='m0-concurrent-import-playback-source-resource'or value.get('policy_sha256')!=policy_sha256 or value.get('controller_sha256')!=controller_sha256 or value.get('immutable_bindings')!=bindings(policy):raise ValueError('ReviewedInputBinding')
 if review.get('approved')is not True or cue.get('heavy_idle_verified')is not True:raise ValueError('MissingFreshQuietAuthorization')
 if not isinstance(boot_id,str)or not boot_id or cue.get('boot_id')!=boot_id:raise ValueError('QuietBootBinding')
 issued,expires=cue.get('issued_unix'),cue.get('expires_unix');limits=policy['resource_draft']['authorization']
 if not all(type(x)in(int,float)and math.isfinite(x)for x in[issued,expires,now]):raise ValueError('QuietTimeFormat')
 if issued>now or expires<=now or expires<=issued or now-issued>limits['maximum_cue_age_at_start_seconds']or expires-issued>limits['maximum_cue_lifetime_seconds']:raise ValueError('QuietTimeBinding')
 reps=cue.get('authorized_repetitions')
 if type(repetition)is not int or not isinstance(reps,list)or not reps or any(type(x)is not int or x not in[1,2,3]for x in reps)or len(set(reps))!=len(reps)or repetition not in reps:raise ValueError('QuietRepetitionBinding')
 return {'validated':True,'issued_unix':issued,'expires_unix':expires,'requested_repetition':repetition,'immutable_bindings':bindings(policy)}
