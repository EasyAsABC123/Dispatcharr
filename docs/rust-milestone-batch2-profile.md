# Current image: shared custom-input media

The `e88cc73f…ece65` image passes the scoped custom-input/shared-stream cohort:
33 assertions across shared demand, stop and owner-crash cases. Two viewers
across Rust replicas share **one real FFmpeg process, one upstream connection
and one provider slot** in each case. All six captured streams decode74 video
frames with both audio and video present.

Reader loss retains the process/provider owner until retirement. Observed normal,
global-stop and owner-crash retirement times are5.47s,0.51s and28.50s respectively.
Exactly32 candidates work;33 return the expected503 JSON error before opening
an upstream or reserving a provider.

The [publication summary](../scripts/rust-milestone-batches/m235/profile-sharedTS/publication-summary.json)
and [exact driver manifest](../scripts/rust-milestone-batches/m235/profile-sharedTS/manifest.json)
bind the executed image, tools, raw capture hashes and cleanup. Execution,
independent cleanup and evaluation exit0, with process groups settled and pipe
EOFs. Fresh Docker project and candidate-container absence checks pass; no
unobserved host PID-generation witness is claimed.

The broader matrix node remains failed because `full_required_workflow` is false.
Source media/process authority, VLC, active OutputProfiles, provider transforms,
credential pooling and non-TS media remain unqualified. Source18 model arguments
were reused without replay. This is functional evidence, not a performance gain
or completed M3 milestone. Original Dispatcharr authors retain all application
and algorithm credit; dependency licenses remain preserved.
