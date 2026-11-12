# Proposed once-only functional observation — approval required

No execution cue currently authorizes this revision. The entry point remains blocked.
Root should review selection.json, policy.json, image50-readonly-preflight.json and
41 offline controls before issuing a new cue for exactly success and busy.

After approval, create a separate execution12 copy. Change only the unconditional
block into validation of the fresh cue's exact selection/policy/manifest/archive,
image ID/source tree, boot ID, expiry, two cases and fresh output binding, followed
by execute(args, repo, out). Retain the activation diff and cue. Do not precreate
output, retry either case, widen timing or change application budgets.

Proposed command after that guarded activation (not executable draft11):

```sh
python3 artifacts/rust/m4-vod-four-second-observation-execution-12/tools/run.py \
  --repo /home/justin/github/Dispatcharr \
  --candidate-image dispatcharr-rust-app:retained-helper-image50 \
  --build-manifest artifacts/rust/candidate-build-retained-helper-image50-1/build.json \
  --source-archive artifacts/rust/candidate-build-retained-helper-image50-1/source.tar \
  --output artifacts/rust/m4-vod-four-second-main-image50-1
```

The candidate is immutable image50/208598ee, not a rebuilt or overlaid application.
Read-only archive/runtime input checks already pass; actual Docker identity is
rechecked by the activated controller. Owned origin starts and qualifies before
Source playback. Each case has independent template/Source/native databases and
Redis instances. The default named nine-table/sequence audit remains separate.

Capture actual physical reply at 4 seconds (maximum 4.35), the absolute 3.1-second
checkpoint, before/after release, header/outcome in the request thread, explicit
consumer close and 1.5/3/6/15-second post-close snapshots. Snapshot intervals are
non-atomic and recorded. Native may return 503 at outer 3 seconds; that is a possible
valid observation and negative parity result, never an accepted success. Source
200/429 hypotheses are checked separately. No changed global 2s/outer 3s budget.

Inspect Source request_started/request_finished correlation after Source phase and
again after Native phase before stopping Source. A request_finished observation
proves response-close signaling after closer attempts, not successful generator
finally (exceptions can be swallowed), normal generator EOF, TCP acknowledgement,
old idle-timer execution or daemon completion. Literal metadata and any independent
callback evidence must establish those separate claims. The real Django signal
path remains unexecuted until this authorized run; pure controls do not replace it.

Preserve both raw and sanitized reports, terminal proxy replies/status, signal rows
and bounded memory status, PID/pipe cleanup and project inventories. Missing or
invalid coverage makes observation invalid. A process exit 1 can represent valid
negative parity; report observation_valid, parity_passed and source reference
expectation independently. Independent cleanup must verify only exact owned
projects. No performance, resource, default-recovery or full M4 acceptance claim.
