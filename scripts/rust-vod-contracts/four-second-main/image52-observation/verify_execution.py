"""Read-only public metadata/hash verification; no cue activation or fixture dispatch."""
import hashlib,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def verify():
 p=json.loads((HERE/'provenance.json').read_text());cue=json.loads((HERE/'actual-cue.json').read_text());claim=json.loads((HERE/'actual-claim.json').read_text());data=json.loads((HERE/'retained.json').read_text())
 checks={'blocked-selection.json':'blocked_selection_sha256','executed-launch_once.py':'actual_executed_launcher_sha256','retained.json':'retained_projection_sha256','coverage-authority.json':'coverage_authority_sha256','actual-cue.json':'actual_cue_sha256','actual-claim.json':'actual_claim_sha256','independent-review.json':'independent_review_sha256','Root-independent-cleanup-review.json':'Root_cleanup_sha256','Original-LICENSE':'Original_license_sha256'}
 for name,key in checks.items():
  if sha(HERE/name)!=p[key]:raise ValueError('ExactPublicInput '+name)
 if cue['selection_sha256']!=p['blocked_selection_sha256']or claim['selection_sha256']!=cue['selection_sha256']or claim['cue_sha256']!=p['actual_cue_sha256']:raise ValueError('CueClaimSelection')
 if not cue['issued_epoch']<=claim['claimed_epoch']<cue['expires_epoch']or cue['boot_id']!=claim['boot_id']or claim['no_retry']is not True:raise ValueError('HistoricalClaimClock')
 if cue['Root_quiet_preflight_sha256']!=p['Root_quiet_preflight_raw_sha256']:raise ValueError('QuietPreflightHash')
 binding=p['candidate']
 expected=HERE/'blocked-inputs/tools/expected-runtime.json';mapping=json.loads(expected.read_text())
 if len(mapping)!=788 or not all(k.startswith('rust/')and len(v)==64 for k,v in mapping.items())or sha(expected)!=binding['Root_reviewed_input_map_sha256']:raise ValueError('Exact788PublicInputMap')
 for key in ['candidate_image','candidate_tag','build_manifest_sha256','git_revision','source_tree_sha256','source_archive_sha256','Root_reviewed_input_map_sha256','Root_reviewed_Rust_input_count']:
  if cue[key]!=binding[key]:raise ValueError('ExactCandidate '+key)
 if data['raw_sha256']['summary.json']!=p['raw_summary_sha256']or data['actual_exit_code']!=0:raise ValueError('ActualSummary')
 plan=HERE/'blocked-inputs/execution-plan.json';policy=HERE/'blocked-inputs/policy.json';bound=HERE/'blocked-inputs/candidate-binding.json'
 for file,key in [(plan,'plan_sha256'),(policy,'policy_sha256'),(bound,'binding_sha256')]:
  if sha(file)!=cue[key]:raise ValueError('ExecutedMetadata '+key)
 return {'all_public_hashes_cue_claim_identity_time_exact':True,'Root_quiet_preflight_hash_only_not_hostargv_published':True,'actual_execution_authorized_by_consumed_historical_cue_only':True,'fresh_authorization_granted':False,'application_executed':False,'raw_auth_private':True}
if __name__=='__main__':print(json.dumps(verify(),indent=2))
